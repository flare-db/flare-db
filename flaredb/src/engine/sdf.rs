use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use anyhow::anyhow;
use beam_model_rs::v1::process_bundle_split_request::DesiredSplit;
use beam_model_rs::v1::{
    BundleApplication, DelayedBundleApplication, ProcessBundleProgressResponse,
    ProcessBundleResponse,
};
use log::{error, info, warn};

use crate::coders::primitives::{VarIntCoder, WindowedValueCoder};
use crate::coders::{BeamCoder, StandardBeamCoders};
use crate::engine::harness::control::ControlChannel;
use crate::fusion::stage::SplittableProcessKind;
use crate::jobservice::urns::beam_urns;
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

/// The minimum duration between two consecutive `progress + split_evaluation` runs.
const MIN_PROGRESS_TICK: Duration = Duration::from_millis(100);
/// The maximum duration between two consecutive `progress + split_evaluation` runs.
const MAX_PROGRESS_TICK: Duration = Duration::from_secs(30);
/// Fraction of remaining work SDK keeps after a split.
const SPLIT_FRACTION_OF_REMAINDER: f64 = 0.5;

/// One deferred bundle's worth of SDF work.
///
/// `seed` is the encoded element bytes to feed the process stage for this bundle
/// (a residual's `element`, or the initialization seed for the first bundle).
/// `holds` is the output-watermark clamp the deferred work imposes: while the item
/// is queued or in flight, the stage's output watermark must not advance past it.
#[derive(Clone, Debug, Default)]
pub struct SdfWorkItem {
    pub seed: Vec<u8>,
    pub holds: Vec<Timestamp>,
}

/// What one executed SDF bundle reports back to the dispatcher.
#[derive(Debug, Default)]
pub struct SdfBundleOutcome {
    /// The initialization bundle has run (carried forward across bundles).
    pub initialized: bool,
    /// The descriptors are registered with the worker (carried forward).
    pub registered: bool,
    /// Output-watermark holds of the work item this bundle consumed (to release).
    pub consumed_holds: Vec<Timestamp>,
    /// Residual work items this bundle produced, to be scheduled later.
    pub items: Vec<SdfWorkItem>,
    /// Duration between two consecutive `progress + split_evaluation` runs, after this bundle.
    pub progress_tick: Duration,
}

/// `Progress + split_evaluation` state for a single running bundle.
struct SplitMonitor {
    /// `beam:metric:data_channel:read_index:v1` at the previous tick,
    prev_index: i64,
    /// Sum of `beam:metric:element_count:v1` at the previous tick.
    prev_total_count: i64,
    /// Whether a split request should be sent for this bundle.
    unsplit: bool,
    /// Upper bound on remaining work.
    estimated_input_elements: i64,
    /// Duration between two consecutive `progress + split_evaluation` runs, for this bundle.
    tick: Duration,
    /// Lowest whole input element index already handed back as a residual.
    handed_back_from: Option<i64>,
    /// Whether at least one `progress + split_evaluation` run has happened for this bundle.
    ticked: bool,
}

impl SplitMonitor {
    fn new(estimated_input_elements: i64, progress_tick: Option<Duration>) -> Self {
        Self {
            // Starts at -2 so the first tick only record numbers.
            prev_index: -2,
            prev_total_count: -2,
            unsplit: true,
            estimated_input_elements,
            tick: clamp_progress_tick(progress_tick.unwrap_or(MIN_PROGRESS_TICK)),
            handed_back_from: None,
            ticked: false,
        }
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

    /// Execute exactly one bundle of a splittable (SDF) stage.
    ///
    /// `seed` is the work item to process, or `None` for the first
    /// (initialization) bundle. `initialized`/`registered` are the stage's flags
    /// from prior bundles; the outcome carries them forward along with the residual
    /// work items this bundle produced and the holds of the item it consumed. The
    /// caller schedules one bundle per work item and applies the item holds as
    /// output-watermark clamps until the item completes.
    pub async fn execute_bundle(
        &mut self,
        stage: &SplittableStage,
        output_edge_metadata: Option<ConsumerMetaData>,
        seed: Option<SdfWorkItem>,
        initialized: bool,
        registered: bool,
        progress_tick: Option<Duration>,
    ) -> anyhow::Result<(ProcessBundleResponse, SdfBundleOutcome)> {
        let plan = stage.plan();

        let (seed_bytes, consumed_holds) = match seed {
            Some(item) => (Some(item.seed), item.holds),
            None => (None, Vec::new()),
        };

        // The first bundle runs the initialization stage to capture its seed;
        // later bundles are seeded by their work item.
        let bytes = if initialized {
            seed_bytes.unwrap_or_default()
        } else {
            self.run_initialization_stage(&plan.initialization_stage)
                .await?
        };

        // Dynamic split only applies to restriction-based SDF form.
        let allow_split = matches!(
            plan.process_kind,
            SplittableProcessKind::ProcessSizedElementsAndRestrictions
        );

        let (response, split_residual_roots, channel_items, progress_tick) = self
            .run_process_stage(
                &plan.process_stage,
                output_edge_metadata,
                bytes,
                !registered,
                allow_split,
                progress_tick,
            )
            .await?;

        let mut all_residual_roots = response.residual_roots.clone();
        all_residual_roots.extend(split_residual_roots);
        let mut items = residual_items(stage, &all_residual_roots);
        items.extend(channel_items);

        Ok((
            response,
            SdfBundleOutcome {
                initialized: true,
                registered: true,
                consumed_holds,
                items,
                progress_tick,
            },
        ))
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

        let control_response = crate::engine::liveness::idle_guard(
            async {
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
            },
            Duration::from_secs(60),
            "waiting for SDF initialization stage control response/data",
        )
        .await?;

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
        allow_split: bool,
        progress_tick: Option<Duration>,
    ) -> anyhow::Result<(
        ProcessBundleResponse,
        Vec<DelayedBundleApplication>,
        Vec<SdfWorkItem>,
        Duration,
    )> {
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

        let input_pcol = stage.input_pcol();
        let input_spans = self
            .decode_windowed_value_spans(
                input_pcol.id().as_str(),
                input_pcol.node().coder_id.as_str(),
                &seed,
            )
            .unwrap_or_else(|err| {
                warn!(
                    "failed to decode SDF process input for counting/split ({}); \
                    whole-element split re-injection disabled for this bundle",
                    err
                );
                Vec::new()
            });
        let estimated_input_elements = if !input_spans.is_empty() {
            input_spans.len() as i64
        } else if seed.is_empty() {
            0
        } else {
            1
        };

        let (instruction_id, bundle_response_rx) = self
            .runtime
            .control()
            .send_process_bundle_request(&descriptor_id)
            .await?;
        info!(
            "SDF process stage {} instruction_id={}, estimated_input_elements={}",
            descriptor_id, instruction_id, estimated_input_elements
        );

        let source_transform_id = stage_source_transform_id(stage);
        let timer_endpoints = stage_timer_endpoints(stage);
        self.send_raw_elements(
            &instruction_id,
            &source_transform_id,
            seed.clone(),
            &timer_endpoints,
        )
        .await?;

        let control = self.runtime.control().clone();
        let response_future =
            control.recv_process_bundle_response(&instruction_id, bundle_response_rx);

        let mut split_residual_roots: Vec<DelayedBundleApplication> = Vec::new();
        let mut channel_items: Vec<SdfWorkItem> = Vec::new();
        let mut monitor = SplitMonitor::new(estimated_input_elements, progress_tick);
        let monitor_enabled = allow_split && estimated_input_elements > 0;

        let process_future = async {
            tokio::pin!(response_future);

            let response = loop {
                let ticker = tokio::time::sleep(monitor.tick);
                tokio::pin!(ticker);

                tokio::select! {
                    biased;
                    bundle_response = &mut response_future => break bundle_response,
                    _ = &mut ticker, if monitor_enabled => {
                        if let Err(err) = poll_progress_and_maybe_split(
                            &control,
                            &instruction_id,
                            &source_transform_id,
                            &mut monitor,
                            &seed,
                            &input_spans,
                            &mut split_residual_roots,
                            &mut channel_items
                        ).await {
                            // Progresss error stops further splitting.
                            warn!(
                                "SDF `progress + split_evaluation` run failed for bundle {}, \
                                disabling further splits for this bundle: {}",
                                instruction_id,
                                err
                            );
                            monitor.unsplit = false;
                        }
                    }
                }
            }?;

            Ok::<_, anyhow::Error>(response)
        };

        let control_response = if let Some(output_meta_data) = output_edge_metadata {
            let data_key = DataKey {
                instruction_id: instruction_id.clone(),
                transform_id: stage_sink_transform_id(stage, &output_meta_data.produced_pcol_id),
            };
            let receiver = self.runtime.data_receiver(data_key);
            let output_runtime = self.runtime.clone();

            let decode_task = tokio::spawn(async move {
                output_runtime
                    .process_output_elements(receiver, output_meta_data)
                    .await
            });

            let timeout_id = instruction_id.clone();
            crate::engine::liveness::idle_guard(
                async {
                    match process_future.await {
                        Ok(response) => {
                            decode_task
                                .await
                                .map_err(|err| anyhow!("output decode task failed: {}", err))??;
                            Ok(response)
                        }
                        Err(err) => {
                            decode_task.abort();
                            Err(err)
                        }
                    }
                },
                Duration::from_secs(60),
                &format!(
                    "waiting for SDF process bundle {} output data and control response",
                    timeout_id
                ),
            )
            .await?
        } else {
            let timeout_id = instruction_id.clone();
            crate::engine::liveness::idle_guard(
                process_future,
                Duration::from_secs(60),
                &format!(
                    "waiting for SDF process bundle {} control response",
                    timeout_id
                ),
            )
            .await?
        };

        match control_response {
            ControlResponse::ProcessBundleSuccess(response) => {
                // Carry the stage-shared progress tick forward. If this bundle finished without
                // ever ticking, shrink the tick so that a genuinely huge residual doesn't wait
                // too long before being split.
                //
                // The tick shrinks much more slowly than it grows. This balances avoiding
                // over-splitting against still allowing genuine splits.
                let next_tick = if monitor.ticked {
                    monitor.tick
                } else {
                    clamp_progress_tick(monitor.tick.saturating_sub(MIN_PROGRESS_TICK))
                };
                Ok((response, split_residual_roots, channel_items, next_tick))
            }
            ControlResponse::ProcessBundleError(err) => {
                Err(anyhow!("SDF process bundle failed: {}", err))
            }
            other => Err(anyhow!(
                "unexpected control response for SDF process bundle: {:?}",
                std::mem::discriminant(&other)
            )),
        }
    }

    /// Decode the byte span of each `WindowedValue` in an SDF process input.
    fn decode_windowed_value_spans(
        &mut self,
        pcol_id: &str,
        coder_id: &str,
        bytes: &[u8],
    ) -> anyhow::Result<Vec<(usize, usize)>> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        let element_coder = StandardBeamCoders::from_urn(
            coder_id,
            None,
            Some(self.runtime.pipeline_coders().as_ref()),
        );
        let window_coder = self.runtime.window_coder_for_pcollection(pcol_id);
        let windowed_value_coder =
            WindowedValueCoder::with_window_coder(element_coder, window_coder);

        let mut cursor = Cursor::new(bytes);
        let mut spans = Vec::new();
        while (cursor.position() as usize) < bytes.len() {
            let start = cursor.position() as usize;
            let decoded = catch_unwind(AssertUnwindSafe(|| {
                windowed_value_coder.decode(&mut cursor)
            }));
            match decoded {
                Ok(Ok(_)) => spans.push((start, cursor.position() as usize)),
                Ok(Err(err)) => {
                    return Err(anyhow!("failed to decode SDF input element: {:?}", err));
                }
                Err(_) => return Err(anyhow!("panic while decoding SDF input element")),
            }
        }

        Ok(spans)
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

/// Read progress and if bundle looks stalled, ask the SDK to split it.
async fn poll_progress_and_maybe_split(
    control: &ControlChannel,
    bundle_instruction_id: &str,
    input_transform_id: &str,
    monitor: &mut SplitMonitor,
    input_bytes: &[u8],
    input_spans: &[(usize, usize)],
    split_residual_roots: &mut Vec<DelayedBundleApplication>,
    channel_items: &mut Vec<SdfWorkItem>,
) -> anyhow::Result<()> {
    monitor.ticked = true;

    let progress = control
        .send_process_bundle_progress_request(bundle_instruction_id)
        .await?;
    let (index, total_count) = extract_progress_metrics(&progress);

    let slow = monitor.prev_index == index && monitor.prev_total_count == total_count;
    let should_try_split = slow && monitor.unsplit && monitor.estimated_input_elements > 0;
    if !should_try_split {
        monitor.prev_index = index;
        monitor.prev_total_count = total_count;
        return Ok(());
    }

    let mut desired_splits = HashMap::new();
    desired_splits.insert(
        input_transform_id.to_string(),
        DesiredSplit {
            fraction_of_remainder: SPLIT_FRACTION_OF_REMAINDER,
            allowed_split_points: Vec::new(),
            estimated_input_elements: monitor.estimated_input_elements,
        },
    );

    let split = control
        .send_process_bundle_split_request(bundle_instruction_id, desired_splits)
        .await?;

    if split.channel_splits.is_empty() {
        // SDK cannot split (nothing left, or unsupported)
        monitor.unsplit = false;
        return Ok(());
    }

    // Intra-element remainders (of one or more elements that are partially
    // processed): process in future.
    split_residual_roots.extend(split.residual_roots);

    // Whole elements that are entirely unprocessed: process in future.
    for channel_split in &split.channel_splits {
        if let Some((start, end, count)) = channel_split_range(
            channel_split.first_residual_element,
            monitor.handed_back_from,
            &input_spans,
        ) {
            channel_items.push(SdfWorkItem {
                seed: input_bytes[start..end].to_vec(),
                holds: Vec::new(),
            });
            monitor.handed_back_from = Some(channel_split.first_residual_element);
            debug_assert!(count > 0);
        }
    }

    if let Some(channel_split) = split.channel_splits.first() {
        if monitor.estimated_input_elements >= channel_split.first_residual_element {
            monitor.estimated_input_elements = channel_split.first_residual_element;
        }
    }
    // Increase interval to avoid over-split.
    monitor.tick = clamp_progress_tick(monitor.tick * 4);
    Ok(())
}

/// The residual work items a process bundle produced.
///
/// One item per residual application: the encoded element bytes to re-feed, plus
/// the output-watermark holds that deferred work imposes.
fn residual_items(
    stage: &SplittableStage,
    residuals: &[DelayedBundleApplication],
) -> Vec<SdfWorkItem> {
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

    residuals
        .iter()
        .filter_map(|residual| residual.application.as_ref())
        .map(|application| SdfWorkItem {
            seed: application.element.clone(),
            holds: residual_item_holds(application, &outputs_by_transform, &boundary),
        })
        .collect()
}

/// The output-watermark holds a single residual imposes on its stage's output.
///
/// A residual's `output_watermarks` is a lower bound on the timestamps of elements
/// the owning PTransform will still produce when the residual runs. Only entries
/// naming an output PCollection that *leaves* the stage are considered — internal
/// PCollections are not tracked by the runner — and a `MIN_TIMESTAMP` or absent
/// entry imposes no hold (a hold at `MIN_TIMESTAMP` would stall the stage forever).
fn residual_item_holds(
    application: &BundleApplication,
    outputs_by_transform: &HashMap<String, HashMap<String, String>>,
    boundary: &HashSet<&str>,
) -> Vec<Timestamp> {
    let Some(outputs) = outputs_by_transform.get(&application.transform_id) else {
        return Vec::new();
    };

    let mut holds = Vec::new();
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
    holds
}

fn clamp_progress_tick(tick: Duration) -> Duration {
    tick.max(MIN_PROGRESS_TICK).min(MAX_PROGRESS_TICK)
}

/// Decode one beam varint (`beam:coder:varint:v1`) payload.
fn decode_varint_payload(payload: &[u8]) -> Option<i64> {
    let mut buf = payload;
    match catch_unwind(AssertUnwindSafe(|| VarIntCoder.decode(&mut buf))) {
        Ok(Ok(decoded)) => Some(decoded),
        _ => None,
    }
}

/// Extract progress metrics: data-channel read index + sum of emitted
/// element counts across all PCollections.
fn extract_progress_metrics(resp: &ProcessBundleProgressResponse) -> (i64, i64) {
    let mut index = 0i64;
    let mut total_count = 0i64;

    for info in &resp.monitoring_infos {
        match info.urn.as_str() {
            beam_urns::DATA_CHANNEL_READ_INDEX_METRIC => {
                if let Some(value) = decode_varint_payload(&info.payload) {
                    index = index.max(value);
                }
            }
            beam_urns::ELEMENT_COUNT_METRIC => {
                if let Some(value) = decode_varint_payload(&info.payload) {
                    total_count += value;
                }
            }
            _ => {}
        }
    }
    (index, total_count)
}

/// The element byte range(and element count) a channel split hands back.
fn channel_split_range(
    first_residual_element: i64,
    handed_back_from: Option<i64>,
    input_spans: &[(usize, usize)],
) -> Option<(usize, usize, i64)> {
    let first = first_residual_element as usize;
    let upper = handed_back_from
        .map(|value| value as usize)
        .unwrap_or(input_spans.len());

    if first < upper {
        let start = input_spans[first].0;
        let end = input_spans[upper - 1].1;
        let count = (upper - first) as i64;
        return Some((start, end, count));
    }
    None
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
    fn residual_items_only_hold_boundary_output_watermarks() {
        let stage = splittable_stage(
            &["pcol-out"],
            "process",
            &[("out", "pcol-out"), ("internal", "pcol-internal")],
        );

        // A boundary output reported at 5000ms becomes a hold at 5000.
        let items = residual_items(&stage, &[residual("process", "out", 5_000)]);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].holds, vec![5_000]);

        // An internal output is not tracked by the runner: the item still exists,
        // but imposes no hold.
        let items = residual_items(&stage, &[residual("process", "internal", 5_000)]);
        assert_eq!(items.len(), 1);
        assert!(items[0].holds.is_empty());

        // A local output name not declared on the transform, or a transform not in
        // the process stage, impose no hold.
        assert!(
            residual_items(&stage, &[residual("process", "missing", 5_000)])[0]
                .holds
                .is_empty()
        );
        assert!(
            residual_items(&stage, &[residual("elsewhere", "out", 5_000)])[0]
                .holds
                .is_empty()
        );

        // No residuals: no items.
        assert!(residual_items(&stage, &[]).is_empty());
    }

    #[test]
    fn residual_items_carry_seed_and_holds_per_residual() {
        let stage = splittable_stage(&["pcol-out"], "process", &[("out", "pcol-out")]);

        let mut first = residual("process", "out", 5_000);
        first.application.as_mut().unwrap().element = b"first".to_vec();
        let mut second = residual("process", "out", 2_000);
        second.application.as_mut().unwrap().element = b"second".to_vec();
        // A residual with no application is skipped rather than panicking.
        let empty = DelayedBundleApplication::default();

        let items = residual_items(&stage, &[first, empty, second]);
        assert_eq!(items.len(), 2, "one item per residual application");
        assert_eq!(items[0].seed, b"first".to_vec());
        assert_eq!(items[0].holds, vec![5_000]);
        assert_eq!(items[1].seed, b"second".to_vec());
        assert_eq!(items[1].holds, vec![2_000]);
    }
}
