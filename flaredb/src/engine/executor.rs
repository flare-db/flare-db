use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use beam_model_rs::v1::{ApiServiceDescriptor, ProcessBundleDescriptor};
use log::{error, info, warn};

use crate::{
    engine::timer::TimerEntry,
    engine::{
        harness::{
            control::ControlResponse,
            data::{DataKey, TimersKey},
        },
        runtime::{
            BundleRuntime, runner_consumer_transform_id, runner_output_pcollection_id,
            stage_sink_transform_id, stage_source_transform_id, stage_timer_endpoints,
        },
    },
    fusion::pipeline::{ConsumerMetaData, ExecutableNode},
    transforms::ExecutionContext,
};

/// Executor for worker and runner stage nodes.
///
/// Drives bundle execution end-to-end: for SDK worker stages it registers the
/// bundle and pumps encoded elements over the data channel; for
/// runner-implemented transforms it executes them directly against the element
/// store. A node may consume multiple input PCollections (fan-in), each
/// described by its own [`ConsumerMetaData`].
pub struct StageExecutor {
    runtime: BundleRuntime,
}

impl StageExecutor {
    pub fn new(runtime: BundleRuntime) -> Self {
        Self { runtime }
    }
}

#[async_trait]
pub trait Executor {
    async fn execute(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Vec<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
        input_watermark: i64,
    ) -> anyhow::Result<ControlResponse>;
}

#[async_trait]
impl Executor for StageExecutor {
    async fn execute(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Vec<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
        input_watermark: i64,
    ) -> anyhow::Result<ControlResponse> {
        self.execute_node(
            node,
            input_edge_metadata,
            output_edge_metadata,
            None,
            Vec::new(),
            input_watermark,
        )
        .await
    }
}

impl StageExecutor {
    /// Execute a node, delivering `timers` to its `@OnTimer` if it is an SDK stage.
    pub async fn execute_with_timers(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Vec<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
        timers: Vec<TimerEntry>,
        input_watermark: i64,
    ) -> anyhow::Result<ControlResponse> {
        self.execute_node(
            node,
            input_edge_metadata,
            output_edge_metadata,
            None,
            timers,
            input_watermark,
        )
        .await
    }
}

impl StageExecutor {
    /// Wait for a stage's timer-output tasks to finish, bounded so a stage that
    /// never closes its timer endpoints cannot hang the bundle forever.
    async fn finish_bundle(
        &self,
        response: ControlResponse,
        timer_handles: Vec<tokio::task::JoinHandle<()>>,
    ) -> ControlResponse {
        for handle in timer_handles {
            if tokio::time::timeout(Duration::from_secs(30), handle)
                .await
                .is_err()
            {
                warn!("timed out waiting for timer output; some timers may be lost");
            }
        }
        response
    }

    /// Execute a worker or runner stage node.
    ///
    /// `input_edge_metadata` carries one entry per incoming PCollection: a
    /// single entry for most transforms, several for fan-in transforms such as
    /// `Flatten`.
    pub async fn execute_node(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Vec<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
        _instruction_id: Option<String>,
        timers: Vec<TimerEntry>,
        input_watermark: i64,
    ) -> anyhow::Result<ControlResponse> {
        match node {
            ExecutableNode::Worker(executable_stage) => {
                info!("Executing worker node");
                let descriptor_id = executable_stage.id().to_string();
                let bundle_status = self.runtime.register_bundle(&executable_stage).await;
                info!(
                    "executable_stage input id: {:?}",
                    executable_stage.input_pcol()
                );

                match bundle_status {
                    Ok(response) => {
                        if matches!(response, ControlResponse::BundleRegistered) {
                            info!("Bundle registered at worker");

                            let (instruction_id, bundle_response_rx) = self
                                .runtime
                                .control()
                                .send_process_bundle_request(&descriptor_id)
                                .await?;

                            info!("Process instruction id {}", instruction_id);

                            // Spawn background task to send input elements to worker.
                            if !input_edge_metadata.is_empty() {
                                info!("Input edge metadata: {:?}", input_edge_metadata);
                            }
                            let output_meta_data = output_edge_metadata;
                            if let Some(meta_data) = &output_meta_data {
                                info!("Output edge metadata: {:?}", meta_data);
                            }

                            let instruction_id_log = instruction_id.clone();

                            let input_coder_id =
                                executable_stage.input_pcol().node().coder_id.clone();

                            let input_runtime = self.runtime.clone();
                            let input_instruction_id = instruction_id.clone();
                            let input_pcollection_id = executable_stage.input_pcol().id().clone();
                            let input_consumer_transform_id =
                                stage_source_transform_id(&executable_stage);
                            let input_timer_endpoints = stage_timer_endpoints(&executable_stage);

                            let timer_only = !timers.is_empty();
                            if timer_only || !input_timer_endpoints.is_empty() {
                                info!(
                                    "stage timer endpoints: {:?}; delivering {} fired timer(s) (timer_only={})",
                                    input_timer_endpoints,
                                    timers.len(),
                                    timer_only
                                );
                            }
                            // Deliver any fired timers first, then start the input
                            // pump (whose terminators close the endpoints), then
                            // consume the stage's own timer output.
                            self.runtime
                                .send_fired_timers(&instruction_id, &timers)
                                .await?;
                            let mut timer_handles = Vec::new();
                            for (transform_id, timer_family_id) in
                                input_timer_endpoints.iter().cloned()
                            {
                                let receiver = self.runtime.data().get_timer_receiver(TimersKey {
                                    instruction_id: instruction_id.clone(),
                                    transform_id: transform_id.clone(),
                                    timer_family_id: timer_family_id.clone(),
                                });
                                let timer_runtime = self.runtime.clone();
                                timer_handles.push(tokio::spawn(async move {
                                    if let Err(err) = timer_runtime
                                        .process_timer_elements(
                                            transform_id,
                                            timer_family_id,
                                            receiver,
                                        )
                                        .await
                                    {
                                        warn!("timer processing failed: {}", err);
                                    }
                                }));
                            }

                            tokio::spawn(async move {
                                let result = if timer_only {
                                    // A timer-only bundle must not re-deliver the
                                    // stage's input data.
                                    input_runtime
                                        .terminate_bundle_input(
                                            input_instruction_id,
                                            input_consumer_transform_id,
                                            input_timer_endpoints,
                                        )
                                        .await
                                } else {
                                    input_runtime
                                        .process_input_elements(
                                            input_instruction_id,
                                            input_consumer_transform_id,
                                            input_pcollection_id,
                                            input_coder_id,
                                            None,
                                            input_timer_endpoints,
                                        )
                                        .await
                                };
                                if let Err(err) = result {
                                    error!(
                                        "Failed to send input for instruction {}: {}",
                                        instruction_id_log, err
                                    );
                                }
                            });

                            let control = self.runtime.control().clone();
                            let bundle_response_future = control
                                .recv_process_bundle_response(&instruction_id, bundle_response_rx);

                            if let Some(output_meta_data) = output_meta_data {
                                let data_key = DataKey {
                                    instruction_id: instruction_id.clone(),
                                    transform_id: stage_sink_transform_id(
                                        &executable_stage,
                                        &output_meta_data.produced_pcol_id,
                                    ),
                                };
                                // pass data_key to get receiver
                                info!("Data Key: {:?}", data_key);
                                let receiver = self.runtime.data_receiver(data_key);

                                let output_runtime = self.runtime.clone();

                                let mut decode_task = tokio::spawn(async move {
                                    output_runtime
                                        .process_output_elements(receiver, output_meta_data)
                                        .await
                                });

                                let timeout_id = instruction_id.clone();
                                let proces_bundle_response = tokio::time::timeout(
                                    Duration::from_secs(60),
                                    async {
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
                                    },
                                )
                                .await
                                .map_err(|_| {
                                    anyhow!(
                                        "timed out waiting for SDK bundle {} output data and control response",
                                        timeout_id
                                    )
                                })??;

                                return Ok(self
                                    .finish_bundle(proces_bundle_response, timer_handles)
                                    .await);
                            }

                            let timeout_id = instruction_id.clone();
                            let proces_bundle_response = tokio::time::timeout(
                                Duration::from_secs(60),
                                bundle_response_future,
                            )
                            .await
                            .map_err(|_| {
                                anyhow!(
                                    "timed out waiting for SDK bundle {} control response",
                                    timeout_id
                                )
                            })??;
                            return Ok(self
                                .finish_bundle(proces_bundle_response, timer_handles)
                                .await);
                        } else {
                            Ok(ControlResponse::ProcessBundleError(
                                "Error wile registring bundle".to_string(),
                            ))
                        }
                    }
                    Err(err) => {
                        return Err(anyhow!("Error while processing bundle {}", err));
                    }
                }
            }
            ExecutableNode::Runner(runner_transform) => {
                info!("Executing runner node");

                let output_metadata = output_edge_metadata.as_ref();

                // A runner transform may consume several PCollections (e.g. a
                // Flatten). Map each incoming edge to its input PCollection id;
                // each edge's `coder_id`/`component_coder` describe how
                // the producing stage encoded those elements.
                let input_pcollection_ids: Vec<String> = input_edge_metadata
                    .iter()
                    .map(|meta| meta.produced_pcol_id.clone())
                    .collect();
                let output_pcollection_id =
                    runner_output_pcollection_id(&runner_transform, output_metadata);
                let consumer_transfrom_id =
                    runner_consumer_transform_id(input_edge_metadata.first(), output_metadata);

                info!("Runner node input metadata: {:?}", input_edge_metadata);
                info!("Runner node output metadata: {:?}", output_edge_metadata);

                let endpoint = ApiServiceDescriptor {
                    url: crate::DEFAULT_API_SERVICE_URL.to_string(),
                    ..Default::default()
                };

                let descriptor = ProcessBundleDescriptor {
                    id: runner_transform.id(),
                    transforms: runner_transform.transfrom_spec(),
                    pcollections: runner_transform.pcollections(self.runtime.pipeline_components()),
                    windowing_strategies: runner_transform.windowing_strategies(),
                    coders: runner_transform.coders(),
                    environments: runner_transform.environments(),
                    state_api_service_descriptor: Some(endpoint.clone()),
                    timer_api_service_descriptor: Some(endpoint),
                };

                let bundle_status = self.runtime.control().register_bundle(descriptor).await;

                match bundle_status {
                    Ok(response) => {
                        if matches!(response, ControlResponse::BundleRegistered) {
                            info!("Runer bundle registred at worker");
                            // Resolve the primary input's windowing strategy so a
                            // runner-native transform can read its trigger.
                            let components = self.runtime.pipeline_components();
                            let windowing_strategy = input_pcollection_ids
                                .first()
                                .and_then(|id| components.pcollections.get(id))
                                .and_then(|pcol| {
                                    components
                                        .windowing_strategies
                                        .get(&pcol.windowing_strategy_id)
                                        .cloned()
                                });

                            let ctx = ExecutionContext {
                                store: self.runtime.store().clone(),
                                input_pcollection_ids,
                                output_pcollection_id,
                                consumer_transfrom_id,
                                stage_id: runner_transform.id(),
                                windowing_strategy,
                                processing_time: self.runtime.timer_service().now(),
                                input_watermark,
                            };

                            runner_transform.execute(ctx).await?;
                        } else {
                        }
                    }
                    Err(err) => {
                        return Err(anyhow!("Error while processing bundle {}", err));
                    }
                };

                Ok(ControlResponse::BundleDone)
            }
            ExecutableNode::Splittable(_) => Err(anyhow!(
                "splittable-stage execution is not implemented; this node must be handled by SplittableStageExecutor"
            )),
        }
    }
}
