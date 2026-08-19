use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use beam_model_rs::v1::{ApiServiceDescriptor, ProcessBundleDescriptor};
use log::{error, info};

use crate::{
    engine::{
        harness::{control::ControlResponse, data::DataKey},
        runtime::{
            BundleRuntime, metadata_pcollection_id, runner_consumer_transform_id,
            runner_output_pcollection_id, stage_sink_transform_id, stage_source_transform_id,
        },
    },
    fusion::pipeline::{ConsumerMetaData, ExecutableNode},
    transforms::ExecutionContext,
};

/// Executor managing worker communication and bundle processing.
/// Owns control and data channels, coordinates input/output element flow.
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
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
    ) -> anyhow::Result<ControlResponse>;
}

#[async_trait]
impl Executor for StageExecutor {
    async fn execute(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
    ) -> anyhow::Result<ControlResponse> {
        self.execute_node(node, input_edge_metadata, output_edge_metadata, None)
            .await
    }
}

impl StageExecutor {
    /// Execute a worker or runner stage node.
    pub async fn execute_node(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
        _instruction_id: Option<String>,
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
                            if let Some(meta_data) = &input_edge_metadata {
                                info!("Input edge metadata: {:?}", meta_data.clone());
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
                                        "Failed to send input elements for instruction {}: {}",
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

                                return Ok(proces_bundle_response);
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
                            return Ok(proces_bundle_response);
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

                let input_metadata = input_edge_metadata.as_ref();
                let output_metadata = output_edge_metadata.as_ref();

                let input_pcollection_id = metadata_pcollection_id(input_metadata);
                let output_pcollection_id =
                    runner_output_pcollection_id(&runner_transform, output_metadata);
                let consumer_transfrom_id =
                    runner_consumer_transform_id(input_metadata, output_metadata);

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
                            let ctx = ExecutionContext {
                                store: self.runtime.store().clone(),
                                input_pcollection_id,
                                output_pcollection_id,
                                consumer_transfrom_id,
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
