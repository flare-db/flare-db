use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::anyhow;
use beam_model_rs::v1::{DelayedBundleApplication, ProcessBundleResponse};
use log::{error, info};

use crate::{
    engine::{
        harness::{
            control::ControlResponse,
            data::{DataKey, ElementStreamPayload},
        },
        runtime::{
            BundleRuntime, stage_sink_transform_id, stage_source_transform_id,
            stage_timer_endpoints,
        },
        watermark::{MIN_TIMESTAMP, Timestamp},
    },
    fusion::{
        pipeline::ConsumerMetaData,
        stage::{ExecutableStage, SplittableStage},
    },
};

/// Per-splittable-stage execution state carried across scheduling turns.
///
/// A splittable (SDF) stage does not finish in a single bundle: the SDK returns
/// the unprocessed remainder of each element as a *residual*, which the runner
/// must feed back later. FlareDB carries the residual element bytes here between
/// turns and re-arms the stage until no residuals remain. While residuals are
/// outstanding the stage's output watermark is held at their reported
/// `output_watermarks`, so consumers do not treat earlier event-time work as done.
#[derive(Default)]
pub struct SdfStageState {
    /// The initialization bundle has produced and delivered its seed elements.
    initialized: bool,
    /// The initialization and process bundles are registered with the worker.
    /// Registration persists on the harness connection, so it happens once.
    registered: bool,
    /// Encoded residual elements from the last process bundle, to re-feed next.
    residuals: Vec<Vec<u8>>,
}

impl SdfStageState {
    /// Whether the stage still owes deferred (residual) work.
    pub fn has_pending_work(&self) -> bool {
        !self.residuals.is_empty()
    }
}

/// Executor for a `SplittableStage` node
pub struct SplittableStageExecutor {
    runtime: BundleRuntime,
}

impl SplittableStageExecutor {
    pub fn new(runtime: BundleRuntime) -> Self {
        Self { runtime }
    }

    /// Run one scheduling turn of a splittable (SDF) stage.
    ///
    /// On the first turn this runs the initialization bundle to obtain the seed
    /// elements, then runs exactly one process bundle. On later turns it re-feeds
    /// the residual elements returned by the previous process bundle. The caller
    /// re-arms the stage while [`SdfStageState::has_pending_work`] holds, and
    /// applies the returned residual `output_watermarks` as output holds until
    /// then.
    pub async fn execute_turn(
        &mut self,
        stage: &SplittableStage,
        output_edge_metadata: Option<ConsumerMetaData>,
        state: &mut SdfStageState,
    ) -> anyhow::Result<(ProcessBundleResponse, Vec<Timestamp>)> {
        let plan = stage.plan();

        // Seed: freshly captured initialization bytes on the first turn,
        // otherwise the residual elements from the previous process bundle.
        let seed = if state.initialized {
            std::mem::take(&mut state.residuals).concat()
        } else {
            let seed = self
                .run_initialization_stage(&plan.initialization_stage)
                .await?;
            state.initialized = true;
            seed
        };

        let register = !state.registered;
        let response = self
            .run_process_stage(&plan.process_stage, output_edge_metadata, seed, register)
            .await?;
        state.registered = true;

        state.residuals = residual_elements(&response.residual_roots);
        let holds = residual_output_holds(stage, &response.residual_roots);
        Ok((response, holds))
    }

    /// Run the SDF initialization stage and return its captured seed bytes.
    async fn run_initialization_stage(
        &mut self,
        stage: &ExecutableStage,
    ) -> anyhow::Result<Vec<u8>> {
        let descriptor_id = stage.id().to_string();
        let bundle_status = self.runtime.register_bundle(stage).await?;
        if !matches!(bundle_status, ControlResponse::BundleRegistered) {
            return Err(anyhow!(
                "failed to register SDF initialization bundle {}",
                descriptor_id
            ));
        }
        info!("SDF initialization bundle {} registered", descriptor_id);

        let (instruction_id, bundle_response_rx) = self
            .runtime
            .control()
            .send_process_bundle_request(&descriptor_id)
            .await?;

        info!(
            "SDF initialization stage {} instruction_id={}",
            descriptor_id, instruction_id
        );

        let coder_id = stage.input_pcol().node().coder_id.clone();
        let runtime = self.runtime.clone();
        let instruction_id_clone = instruction_id.clone();
        let pcollection_id = stage.input_pcol().id().clone();
        let consumer_transform_id = stage_source_transform_id(stage);
        let timer_endpoints = stage_timer_endpoints(stage);

        tokio::spawn(async move {
            // Send the SDF's seed restrictions to the worker.
            if let Err(err) = runtime
                .process_input_elements(
                    instruction_id_clone.clone(),
                    consumer_transform_id,
                    pcollection_id,
                    coder_id,
                    None,
                    timer_endpoints,
                )
                .await
            {
                error!(
                    "failed to send SDF seed restrictions for instruction {}: {}",
                    instruction_id_clone, err
                );
            }
        });

        // Collect initialization output elements as raw wire bytes in memory.
        let output_pcol = stage
            .output_pcols()
            .iter()
            .next()
            .ok_or_else(|| anyhow!("SDF initialization stage missing output PCollection"))?
            .clone();
        let output_pcol_id = output_pcol.id().clone();
        let sink_transform_id = stage_sink_transform_id(stage, &output_pcol_id);

        let data_key = DataKey {
            instruction_id: instruction_id.clone(),
            transform_id: sink_transform_id,
        };
        let receiver = self.runtime.data_receiver(data_key);

        let receiver_dup = receiver.clone();
        let mut collect_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            let mut receiver_lock = receiver_dup.lock().await;
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

        let control = self.runtime.control().clone();
        let response_future =
            control.recv_process_bundle_response(&instruction_id, bundle_response_rx);

        let control_response = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::pin!(response_future);
            tokio::select! {
                bundle_response = &mut response_future => {
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
                    let response = response_future.await?;
                    Ok((response, bytes))
                }
            }
        })
        .await
        .map_err(|_| {
            anyhow!("timed out waiting for SDF initialization stage control response/data")
        })??;

        match control_response {
            (ControlResponse::ProcessBundleSuccess(_), bytes) => Ok(bytes),
            (ControlResponse::ProcessBundleError(err), _) => {
                Err(anyhow!("SDF initialization stage failed: {}", err))
            }
            (other, _) => Err(anyhow!(
                "unexpected control response for SDF initialization stage: {:?}",
                std::mem::discriminant(&other)
            )),
        }
    }

    /// Run exactly one SDF process bundle seeded with `seed` bytes and return its
    /// response. `register` is `true` only the first time (the descriptor persists
    /// on the worker across turns).
    async fn run_process_stage(
        &mut self,
        stage: &ExecutableStage,
        output_edge_metadata: Option<ConsumerMetaData>,
        seed: Vec<u8>,
        register: bool,
    ) -> anyhow::Result<ProcessBundleResponse> {
        let descriptor_id = stage.id().to_string();
        if register {
            let bundle_status = self.runtime.register_bundle(stage).await?;
            if !matches!(bundle_status, ControlResponse::BundleRegistered) {
                return Err(anyhow!(
                    "failed to register SDF process bundle {}",
                    descriptor_id
                ));
            }
            info!("SDF process bundle {} registered", descriptor_id);
        }

        let (instruction_id, bundle_response_rx) = self
            .runtime
            .control()
            .send_process_bundle_request(&descriptor_id)
            .await?;
        info!(
            "SDF process stage {} instruction_id={}",
            descriptor_id, instruction_id
        );

        let source_transform_id = stage_source_transform_id(stage);
        let timer_endpoints = stage_timer_endpoints(stage);
        self.send_raw_elements(
            &instruction_id,
            &source_transform_id,
            seed,
            &timer_endpoints,
        )
        .await?;

        let control = self.runtime.control().clone();
        let response_future =
            control.recv_process_bundle_response(&instruction_id, bundle_response_rx);

        let control_response = if let Some(output_meta_data) = output_edge_metadata {
            let data_key = DataKey {
                instruction_id: instruction_id.clone(),
                transform_id: stage_sink_transform_id(stage, &output_meta_data.produced_pcol_id),
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
                tokio::pin!(response_future);
                tokio::select! {
                    bundle_response = &mut response_future => {
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
                        response_future.await
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
            tokio::time::timeout(Duration::from_secs(60), response_future)
                .await
                .map_err(|_| {
                    anyhow!(
                        "timed out waiting for SDF process bundle {} control response",
                        timeout_id
                    )
                })??
        };

        match control_response {
            ControlResponse::ProcessBundleSuccess(response) => Ok(response),
            ControlResponse::ProcessBundleError(err) => {
                Err(anyhow!("SDF process bundle failed: {}", err))
            }
            other => Err(anyhow!(
                "unexpected control response for SDF process bundle: {:?}",
                std::mem::discriminant(&other)
            )),
        }
    }

    /// Send raw Beam-encoded bytes to an SDF stage's source transform.
    ///
    /// `timer_endpoints` carries the stage's inbound timer endpoints; each is
    /// terminated with an empty `Elements.Timers { is_last = true }` so the
    /// harness's `awaitCompletion` (which waits for data *and* timer endpoints)
    /// can return.
    async fn send_raw_elements(
        &self,
        instruction_id: &str,
        source_transform_id: &str,
        data: Vec<u8>,
        timer_endpoints: &[(String, String)],
    ) -> anyhow::Result<()> {
        info!(
            "Sending SDF raw elements: instruction_id={}, transform_id={}, bytes={}",
            instruction_id,
            source_transform_id,
            data.len()
        );

        let mut messages: Vec<beam_model_rs::v1::elements::Data> = Vec::new();
        if !data.is_empty() {
            messages.push(beam_model_rs::v1::elements::Data {
                instruction_id: instruction_id.to_string(),
                transform_id: source_transform_id.to_string(),
                data,
                is_last: false,
            });
        }
        messages.push(beam_model_rs::v1::elements::Data {
            instruction_id: instruction_id.to_string(),
            transform_id: source_transform_id.to_string(),
            data: Vec::new(),
            is_last: true,
        });

        let timers: Vec<beam_model_rs::v1::elements::Timers> = timer_endpoints
            .iter()
            .map(|(transform_id, timer_family_id)| {
                info!(
                    "Terminating SDF inbound timer endpoint: instruction_id={}, transform_id={}, timer_family_id={}",
                    instruction_id, transform_id, timer_family_id
                );
                beam_model_rs::v1::elements::Timers {
                    instruction_id: instruction_id.to_string(),
                    transform_id: transform_id.clone(),
                    timer_family_id: timer_family_id.clone(),
                    timers: Vec::new(),
                    is_last: true,
                }
            })
            .collect();

        let elements = beam_model_rs::v1::Elements {
            data: messages,
            timers,
        };

        self.runtime.data().send_elements(elements).await
    }
}

/// Encoded element bytes for each residual application, to re-feed next turn.
fn residual_elements(residuals: &[DelayedBundleApplication]) -> Vec<Vec<u8>> {
    residuals
        .iter()
        .filter_map(|residual| residual.application.as_ref())
        .map(|application| application.element.clone())
        .collect()
}

/// The output-watermark holds a stage must carry while its residuals are pending.
///
/// A residual's `output_watermarks` is a lower bound on the timestamps of elements
/// the owning PTransform will still produce when the residual runs. Only entries
/// naming an output PCollection that *leaves* the stage are considered — internal
/// PCollections are not tracked by the runner — and a `MIN_TIMESTAMP` or absent
/// entry imposes no hold (a hold at `MIN_TIMESTAMP` would stall the stage forever).
fn residual_output_holds(
    stage: &SplittableStage,
    residuals: &[DelayedBundleApplication],
) -> Vec<Timestamp> {
    if residuals.is_empty() {
        return Vec::new();
    }

    let boundary: HashSet<&str> = stage.output_pcols().iter().map(String::as_str).collect();
    let outputs_by_transform: HashMap<String, HashMap<String, String>> = stage
        .plan()
        .process_stage
        .transforms()
        .iter()
        .map(|transform| (transform.id().clone(), transform.node().outputs.clone()))
        .collect();

    let mut holds = Vec::new();
    for residual in residuals {
        let Some(application) = &residual.application else {
            continue;
        };
        let Some(outputs) = outputs_by_transform.get(&application.transform_id) else {
            continue;
        };
        for (local_name, watermark) in &application.output_watermarks {
            let Some(pcollection) = outputs.get(local_name) else {
                continue;
            };
            if !boundary.contains(pcollection.as_str()) {
                continue;
            }
            let hold = watermark
                .seconds
                .saturating_mul(1_000)
                .saturating_add(i64::from(watermark.nanos) / 1_000_000);
            if hold > MIN_TIMESTAMP {
                holds.push(hold);
            }
        }
    }
    holds
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_model_rs::v1::executable_stage_payload::WireCoderSetting;
    use beam_model_rs::v1::{BundleApplication, Components, Environment, PCollection, PTransform};
    use indexmap::IndexSet;
    use petgraph::Graph;

    use crate::fusion::pipeline::{PCollectionNode, PTransformNode};
    use crate::fusion::stage::{ExecutableStage, SplittableExecutionPlan, SplittableProcessKind};

    /// A splittable stage whose process stage has one transform with the given
    /// local-name -> PCollection-id outputs, and the listed boundary outputs.
    fn splittable_stage(
        boundary: &[&str],
        transform_id: &str,
        outputs: &[(&str, &str)],
    ) -> SplittableStage {
        let mut components = Components::default();
        let mut transform = PTransform {
            unique_name: transform_id.to_string(),
            ..Default::default()
        };
        for (local, pcollection) in outputs {
            transform
                .outputs
                .insert(local.to_string(), pcollection.to_string());
        }
        components
            .transforms
            .insert(transform_id.to_string(), transform.clone());

        let stage = ExecutableStage::from(
            components,
            Environment {
                urn: "test-env".to_string(),
                ..Default::default()
            },
            HashSet::<WireCoderSetting>::new(),
            PCollectionNode {
                id: "stage-in".to_string(),
                collection: PCollection {
                    unique_name: "stage-in".to_string(),
                    ..Default::default()
                },
            },
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::from([PTransformNode {
                id: transform_id.to_string(),
                transform,
            }]),
        );

        let output_pcols: HashSet<String> = boundary.iter().map(|p| p.to_string()).collect();
        SplittableStage::new(
            "splittable:test".to_string(),
            Graph::new(),
            output_pcols,
            SplittableExecutionPlan {
                initialization_stage: stage.clone(),
                process_stage: stage,
                process_kind: SplittableProcessKind::ProcessElements,
            },
        )
    }

    fn residual(transform_id: &str, local_output: &str, millis: i64) -> DelayedBundleApplication {
        DelayedBundleApplication {
            application: Some(BundleApplication {
                transform_id: transform_id.to_string(),
                output_watermarks: HashMap::from([(
                    local_output.to_string(),
                    prost_types::Timestamp {
                        seconds: millis.div_euclid(1_000),
                        nanos: (millis.rem_euclid(1_000) * 1_000_000) as i32,
                    },
                )]),
                ..Default::default()
            }),
            requested_time_delay: None,
        }
    }

    #[test]
    fn residual_holds_only_boundary_output_watermarks() {
        let stage = splittable_stage(
            &["pcol-out"],
            "process",
            &[("out", "pcol-out"), ("internal", "pcol-internal")],
        );

        // A boundary output reported at 5000ms becomes a single hold at 5000.
        assert_eq!(
            residual_output_holds(&stage, &[residual("process", "out", 5_000)]),
            vec![5_000]
        );

        // An internal output is not tracked by the runner: no hold.
        assert!(
            residual_output_holds(&stage, &[residual("process", "internal", 5_000)]).is_empty()
        );

        // A local output name that is not declared on the transform: no hold.
        assert!(residual_output_holds(&stage, &[residual("process", "missing", 5_000)]).is_empty());

        // A transform not present in the process stage: no hold.
        assert!(residual_output_holds(&stage, &[residual("elsewhere", "out", 5_000)]).is_empty());

        // No residuals: no hold.
        assert!(residual_output_holds(&stage, &[]).is_empty());
    }

    #[test]
    fn residual_holds_accumulate_across_residuals() {
        let stage = splittable_stage(&["pcol-out"], "process", &[("out", "pcol-out")]);

        // Each residual contributes its own multiset entry; the manager takes the
        // minimum, so a lower bound from any residual holds the stage.
        let holds = residual_output_holds(
            &stage,
            &[
                residual("process", "out", 5_000),
                residual("process", "out", 2_000),
            ],
        );
        assert_eq!(holds, vec![5_000, 2_000]);
    }

    #[test]
    fn residual_elements_collect_each_application_body() {
        let mut first = residual("process", "out", 5_000);
        first.application.as_mut().unwrap().element = b"first".to_vec();
        let mut second = residual("process", "out", 5_000);
        second.application.as_mut().unwrap().element = b"second".to_vec();
        // A residual with no application is skipped rather than panicking.
        let empty = DelayedBundleApplication::default();

        let elements = residual_elements(&[first, empty, second]);
        assert_eq!(elements, vec![b"first".to_vec(), b"second".to_vec()]);
    }
}
