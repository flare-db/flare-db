use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use beam_model_rs::v1::{DelayedBundleApplication, ProcessBundleResponse};
use log::{error, info, warn};

use crate::{
    engine::{
        executor::Executor,
        harness::{
            control::ControlResponse,
            data::{DataKey, ElementStreamPayload},
        },
        runtime::{BundleRuntime, stage_sink_transform_id, stage_source_transform_id},
    },
    fusion::{
        pipeline::{ConsumerMetaData, ExecutableNode},
        stage::SplittableStage,
    },
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
        _input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
    ) -> anyhow::Result<ControlResponse> {
        let plan = stage.plan();

        // 1. Execute the initialization stage
        let init_descriptor_id = plan.initialization_stage.id().to_string();
        let init_bundle_status = self
            .runtime
            .register_bundle(&plan.initialization_stage)
            .await?;
        if !matches!(init_bundle_status, ControlResponse::BundleRegistered) {
            return Err(anyhow!(
                "failed to register SDF initialization bundle {}",
                init_descriptor_id
            ));
        }
        info!(
            "SDF initialization bundle {} registered",
            init_descriptor_id
        );

        let (init_instruction_id, init_bundle_response_rx) = self
            .runtime
            .control()
            .send_process_bundle_request(&init_descriptor_id)
            .await?;

        info!(
            "SDF initialization stage {} instruction_id={}",
            init_descriptor_id, init_instruction_id
        );

        let init_coder_id = plan
            .initialization_stage
            .input_pcol()
            .node()
            .coder_id
            .clone();
        let init_runtime = self.runtime.clone();
        let init_instruction_id_clone = init_instruction_id.clone();
        let init_pcollection_id = plan.initialization_stage.input_pcol().id().clone();
        let init_consumer_transform_id = stage_source_transform_id(&plan.initialization_stage);

        tokio::spawn(async move {
            // Send Splittable stage's input elemnts to worker
            if let Err(err) = init_runtime
                .process_input_elements(
                    init_instruction_id_clone.clone(),
                    init_consumer_transform_id,
                    init_pcollection_id,
                    init_coder_id,
                    None,
                )
                .await
            {
                error!(
                    "failed to send SDF seed restrictions for instruction {}: {}",
                    init_instruction_id_clone, err
                );
            }
        });

        // Collect initialization output elements as raw wire bytes in memory
        let init_output_pcols = plan.initialization_stage.output_pcols();
        let init_output_pcol = init_output_pcols
            .iter()
            .next()
            .ok_or_else(|| anyhow!("SDF initialization stage missing output PCollection"))?;
        let init_output_pcol_id = init_output_pcol.id().clone();
        let init_sink_transform_id =
            stage_sink_transform_id(&plan.initialization_stage, &init_output_pcol_id);

        let init_data_key = DataKey {
            instruction_id: init_instruction_id.clone(),
            transform_id: init_sink_transform_id,
        };
        let init_receiver = self.runtime.data_receiver(init_data_key);

        let init_receiver_dup = init_receiver.clone();
        let mut collect_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            let mut receiver_lock = init_receiver_dup.lock().await;
            while let Some(payload) = receiver_lock.recv().await {
                match payload {
                    ElementStreamPayload::Data(data_chunk) => {
                        bytes.extend_from_slice(&data_chunk.data.data);
                        if data_chunk.data.is_last {
                            break;
                        }
                    }
                    ElementStreamPayload::Timers(_) => {}
                }
            }
            bytes
        });

        let init_control = self.runtime.control().clone();
        let init_response_future = init_control
            .recv_process_bundle_response(&init_instruction_id, init_bundle_response_rx);

        let init_control_response = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::pin!(init_response_future);
            tokio::select! {
                bundle_response = &mut init_response_future => {
                    match bundle_response {
                        Ok(response) => {
                            let bytes = collect_task.await.map_err(|err| {
                                anyhow!("initialization collect task failed: {}", err)
                            })?;
                            Ok((response, bytes))
                        }
                        Err(err) => {
                            collect_task.abort();
                            Err(err)
                        }
                    }
                }
                collect_result = &mut collect_task => {
                    let bytes = collect_result.map_err(|err| {
                        anyhow!("initialization collect task failed: {}", err)
                    })?;
                    let response = init_response_future.await?;
                    Ok((response, bytes))
                }
            }
        })
        .await
        .map_err(|_| {
            anyhow!("timed out waiting for SDF initialization stage control response/data")
        })??;

        let captured_bytes = match init_control_response {
            (ControlResponse::ProcessBundleSuccess(_), bytes) => bytes,
            (ControlResponse::ProcessBundleError(err), _) => {
                return Err(anyhow!("SDF initialization stage failed: {}", err));
            }
            (other, _) => {
                return Err(anyhow!(
                    "unexpected control response for SDF initialization stage: {:?}",
                    std::mem::discriminant(&other)
                ));
            }
        };

        // 2. Register process stage
        let process_descriptor_id = plan.process_stage.id().to_string();
        let process_bundle_status = self.runtime.register_bundle(&plan.process_stage).await?;
        if !matches!(process_bundle_status, ControlResponse::BundleRegistered) {
            return Err(anyhow!(
                "failed to register SDF process bundle {}",
                process_descriptor_id
            ));
        }
        info!("SDF process bundle {} registered", process_descriptor_id);

        let process_source_transform_id = stage_source_transform_id(&plan.process_stage);
        let mut pending_residuals: Option<Vec<DelayedBundleApplication>> = None;
        #[allow(unused_assignments)]
        let mut final_response: Option<ProcessBundleResponse> = None;
        let mut iteration = 0u32;

        loop {
            let (instruction_id, bundle_response_rx) = self
                .runtime
                .control()
                .send_process_bundle_request(&process_descriptor_id)
                .await?;

            info!(
                "SDF process stage {} bundle iteration {} instruction_id={}",
                process_descriptor_id, iteration, instruction_id
            );

            // Send raw bytes to process stage: captured_bytes on first iteration, residual_roots on subsequent iterations
            match pending_residuals.take() {
                None => {
                    self.send_raw_elements(
                        &instruction_id,
                        &process_source_transform_id,
                        captured_bytes.clone(),
                    )
                    .await?;
                }
                Some(residuals) => {
                    let mut encoded = Vec::new();
                    for residual in residuals {
                        let Some(application) = residual.application else {
                            warn!("residual root missing application, skipping");
                            continue;
                        };
                        encoded.extend_from_slice(&application.element);
                    }
                    self.send_raw_elements(&instruction_id, &process_source_transform_id, encoded)
                        .await?;
                }
            }

            let control = self.runtime.control().clone();
            let bundle_response_future =
                control.recv_process_bundle_response(&instruction_id, bundle_response_rx);

            let control_response = if let Some(output_meta_data) = output_edge_metadata.clone() {
                let data_key = DataKey {
                    instruction_id: instruction_id.clone(),
                    transform_id: stage_sink_transform_id(
                        &plan.process_stage,
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
                        "timed out waiting for SDF process bundle {} output data and control response",
                        timeout_id
                    )
                })??
            } else {
                let timeout_id = instruction_id.clone();
                tokio::time::timeout(Duration::from_secs(60), bundle_response_future)
                    .await
                    .map_err(|_| {
                        anyhow!(
                            "timed out waiting for SDF process bundle {} control response",
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
                "SDF process stage {} bundle iteration {} complete, {} residual roots",
                process_descriptor_id,
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

        Ok(ControlResponse::ProcessBundleSuccess(
            final_response
                .expect("SDF process loop must complete at least one bundle before exiting"),
        ))
    }

    async fn send_raw_elements(
        &self,
        instruction_id: &str,
        source_transform_id: &str,
        data: Vec<u8>,
    ) -> anyhow::Result<()> {
        let elements = beam_model_rs::v1::Elements {
            data: vec![beam_model_rs::v1::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: source_transform_id.to_string(),
                data,
                is_last: true,
            }],
            timers: Vec::new(),
        };

        self.runtime.data().send_elements(elements).await
    }
}
