use std::{collections::HashMap, io::Cursor, time::Duration};

use anyhow::anyhow;
use async_trait::async_trait;
use beam_model_rs::v1::{ApiServiceDescriptor, DelayedBundleApplication, ProcessBundleResponse};
use log::{error, info, warn};
use petgraph::{Direction, algo::toposort, graph::NodeIndex};

use crate::{
    engine::{
        executor::{self, Executor, StageExecutor},
        harness::{
            control::ControlResponse,
            data::{DataKey, ElementStreamPayload},
        },
        runtime::{BundleRuntime, stage_sink_transform_id, stage_source_transform_id},
    },
    fusion::{
        pipeline::{ConsumerMetaData, ExecutableNode},
        stage::{ExecutableStage, SplittableStage},
    },
    jobservice::urns,
};

/// Executor for a `SplittableStage` node
pub struct SplittableStageExecutor {
    runtime: BundleRuntime,
}

impl SplittableStageExecutor {
    pub fn new(runtime: BundleRuntime) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl Executor for SplittableStageExecutor {
    async fn execute(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
    ) -> anyhow::Result<ControlResponse> {
        let ExecutableNode::Splittable(stage) = node else {
            return Err(anyhow!(
                "SplittableStageExecutor received a non-splittable node"
            ));
        };

        self.execute_splittable_stage(&stage, input_edge_metadata, output_edge_metadata)
            .await
    }
}

impl SplittableStageExecutor {
    async fn execute_splittable_stage(
        &mut self,
        stage: &SplittableStage,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
    ) -> anyhow::Result<ControlResponse> {
        let sdf_nodes = stage.graph();

        let sorted: Vec<NodeIndex> = toposort(sdf_nodes, None).map_err(|_| {
            anyhow!(
                "splittable stage {} has a cycle in its nested graph",
                stage.id()
            )
        })?;

        let mut last_response = ControlResponse::BundleDone;

        for idx in sorted {
            let node = sdf_nodes[idx].clone();

            let node_input = sdf_nodes
                .edges_directed(idx, Direction::Incoming)
                .next()
                .map(|edge| edge.weight().clone())
                .or_else(|| input_edge_metadata.clone());

            let node_output = sdf_nodes
                .edges_directed(idx, Direction::Outgoing)
                .next()
                .map(|edge| edge.weight().clone())
                .or_else(|| output_edge_metadata.clone());

            let ExecutableNode::Worker(stage) = &node else {
                return Err(anyhow!(
                    "SplittableStage nested graph member {} is not a Worker node",
                    node.id()
                ));
            };

            last_response = if stage_has_sdf_process_transform(stage) {
                self.execute_sdf_process_stage(stage, node_input, node_output)
                    .await?
            } else {
                // Ordinary bundle (pair/split, or plain ParDos fused alongside
                // them) — reuse StageExecutor unchanged.
                let mut executor = StageExecutor::new(self.runtime.clone());
                executor
                    .execute_node(node, node_input, node_output, None)
                    .await?
            };
        }

        Ok(last_response)
    }

    /// Register the process stage once, seed it from the store (same path as
    /// an ordinary stage's input), then loop on `residual_roots` returned by
    /// the SDK — resending each residual batch as the next bundle's input
    /// against the same registered descriptor, until none remain.
    ///
    /// No runner-initiated dynamic splitting here, only self-checkpointing
    /// residuals, per the stated goal.
    async fn execute_sdf_process_stage(
        &mut self,
        executable_stage: &ExecutableStage,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
    ) -> anyhow::Result<ControlResponse> {
        let descriptor_id = executable_stage.id().to_string();

        let bundle_status = self.runtime.register_bundle(executable_stage).await?;
        if !matches!(bundle_status, ControlResponse::BundleRegistered) {
            return Err(anyhow!(
                "failed to register SDF process bundle {}",
                descriptor_id
            ));
        }
        info!("SDF process bundle {} registered at worker", descriptor_id);

        let source_transform_id = stage_source_transform_id(executable_stage);
        let mut pending_residuals: Option<Vec<DelayedBundleApplication>> = None;
        let mut final_response: Option<ProcessBundleResponse> = None;
        let mut iteration = 0u32;

        loop {
            let (instruction_id, bundle_response_rx) = self
                .runtime
                .control()
                .send_process_bundle_request(&descriptor_id)
                .await?;

            info!(
                "SDF stage {} bundle iteration {} instruction_id={}",
                descriptor_id, iteration, instruction_id
            );

            // input: seed from store on the first pass, residual bytes
            // resent verbatim on subsequent passes
            match pending_residuals.take() {
                None => {
                    let input_coder_id = executable_stage.input_pcol().node().coder_id.clone();
                    let input_runtime = self.runtime.clone();
                    let input_instruction_id = instruction_id.clone();
                    let input_pcollection_id = executable_stage.input_pcol().id().clone();
                    let input_consumer_transform_id = source_transform_id.clone();
                    let instr_log = instruction_id.clone();

                    tokio::spawn(async move {
                        if let Err(err) = input_runtime
                            .process_input_elements(
                                input_instruction_id,
                                input_consumer_transform_id,
                                input_pcollection_id,
                                input_coder_id,
                                None,
                            )
                            .await
                        {
                            error!(
                                "failed to send SDF seed restrictions for instruction {}: {}",
                                instr_log, err
                            );
                        }
                    });
                }
                Some(residuals) => {
                    self.send_residual_elements(&instruction_id, &source_transform_id, residuals)
                        .await?;
                }
            }

            // output: same drain-while-awaiting-response pattern as
            // StageExecutor::execute_node, reused verbatim
            let control = self.runtime.control().clone();
            let bundle_response_future =
                control.recv_process_bundle_response(&instruction_id, bundle_response_rx);

            let control_response = if let Some(output_meta_data) = output_edge_metadata.clone() {
                let data_key = DataKey {
                    instruction_id: instruction_id.clone(),
                    transform_id: stage_sink_transform_id(
                        executable_stage,
                        &output_meta_data.produced_pcol_id,
                    ),
                };
                let receiver = self.runtime.data_receiver(data_key);
                let output_runtime = self.runtime.clone();

                let mut decode_task = tokio::spawn(async move {
                    output_runtime
                        .process_output_elements(receiver, output_meta_data)
                        .await
                });

                let timeout_id = instruction_id.clone();
                tokio::time::timeout(Duration::from_secs(60), async {
                    tokio::pin!(bundle_response_future);
                    tokio::select! {
                        bundle_response = &mut bundle_response_future => {
                            match bundle_response {
                                Ok(response) => {
                                    decode_task.await.map_err(|err| {
                                        anyhow!("output decode task failed: {}", err)
                                    })??;
                                    Ok(response)
                                }
                                Err(err) => {
                                    decode_task.abort();
                                    Err(err)
                                }
                            }
                        }
                        decode_result = &mut decode_task => {
                            decode_result.map_err(|err| {
                                anyhow!("output decode task failed: {}", err)
                            })??;
                            bundle_response_future.await
                        }
                    }
                })
                .await
                .map_err(|_| {
                    anyhow!(
                        "timed out waiting for SDF bundle {} output data and control response",
                        timeout_id
                    )
                })??
            } else {
                let timeout_id = instruction_id.clone();
                tokio::time::timeout(Duration::from_secs(60), bundle_response_future)
                    .await
                    .map_err(|_| {
                        anyhow!(
                            "timed out waiting for SDF bundle {} control response",
                            timeout_id
                        )
                    })??
            };

            let response = match control_response {
                ControlResponse::ProcessBundleSuccess(response) => response,
                ControlResponse::ProcessBundleError(err) => {
                    return Err(anyhow!("SDF process bundle failed: {}", err));
                }
                other => {
                    return Err(anyhow!(
                        "unexpected control response for SDF process bundle: {:?}",
                        std::mem::discriminant(&other)
                    ));
                }
            };

            let residuals = response.residual_roots.clone();
            info!(
                "SDF stage {} bundle iteration {} complete, {} residual roots",
                descriptor_id,
                iteration,
                residuals.len()
            );

            iteration += 1;
            final_response = Some(response);

            if residuals.is_empty() {
                break;
            }
            pending_residuals = Some(residuals);
        }

        // final_response is always Some the loop only exits after at least
        // one successful iteration.
        Ok(ControlResponse::ProcessBundleSuccess(
            final_response
                .expect("SDF process loop must complete at least one bundle before exiting"),
        ))
    }

    /// Resend residual roots directly on the data channel as the next
    /// bundle's input. Each `DelayedBundleApplication.application.element` is
    /// already a fully wire-encoded WindowedValue (the SDK produced it), so
    /// this skips the store-scan/coder-encode path `process_input_elements`
    /// uses for the seed case entirely — it's a different data source, not a
    /// different protocol.
    async fn send_residual_elements(
        &self,
        instruction_id: &str,
        source_transform_id: &str,
        residuals: Vec<DelayedBundleApplication>,
    ) -> anyhow::Result<()> {
        let mut encoded = Vec::new();

        for residual in residuals {
            let Some(application) = residual.application else {
                warn!("residual root missing application, skipping");
                continue;
            };
            encoded.extend_from_slice(&application.element);
        }

        let elements = beam_model_rs::v1::Elements {
            data: vec![beam_model_rs::v1::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: source_transform_id.to_string(),
                data: encoded,
                is_last: true,
            }],
            timers: Vec::new(),
        };

        self.runtime.data().send_elements(elements).await
    }
}

/// True when `stage` contains the terminal SDF process transform — the only
/// member needing restriction-lifecycle handling. A stage can contain plain
/// ParDos fused alongside the process transform (e.g. a trailing elementwise
/// Map); those run inside the same bundle and don't change which stage this
/// is, only its output wire shape.
fn stage_has_sdf_process_transform(stage: &ExecutableStage) -> bool {
    stage.transforms().iter().any(|t| {
        t.node()
            .spec
            .as_ref()
            .map(|s| {
                matches!(
                    s.urn.as_str(),
                    urns::beam_urns::SPLITTABLE_PROCESS_KEYED_URN
                        | urns::beam_urns::SPLITTABLE_PROCESS_ELEMENTS_URN
                        | urns::beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN
                )
            })
            .unwrap_or(false)
    })
}
