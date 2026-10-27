use std::collections::{HashMap, HashSet};

use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};

use std::time::{Duration, Instant};

use anyhow::anyhow;
use beam_model_rs::v1::process_bundle_split_request::DesiredSplit;
use beam_model_rs::v1::{
    BundleApplication, DelayedBundleApplication, ProcessBundleProgressResponse,
    ProcessBundleResponse,
};
use bytes::Buf;
use log::{debug, error, info, warn};
use tokio::sync::mpsc::UnboundedSender;

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
/// Default fraction of remaining work the SDK keeps after a split.
const DEFAULT_SPLIT_FRACTION_OF_REMAINDER: f64 = 0.5;
/// Default elapsed time after which a still-progressing bundle is split.
const DEFAULT_SPLIT_AFTER: Duration = Duration::from_secs(2);
/// Default no-progress window before the fallback stall detector asks to split.
///
/// SDK output counters can be buffered for seconds, so this is intentionally
/// longer than the time-based trigger. SDK work-progress metrics take precedence
/// whenever the harness reports them.
const DEFAULT_STALL_AFTER: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
struct SdfSplitConfig {
    split_after: Duration,
    stall_after: Duration,
    fraction_of_remainder: f64,
}

impl Default for SdfSplitConfig {
    fn default() -> Self {
        Self {
            split_after: DEFAULT_SPLIT_AFTER,
            stall_after: DEFAULT_STALL_AFTER,
            fraction_of_remainder: DEFAULT_SPLIT_FRACTION_OF_REMAINDER,
        }
    }
}

impl SdfSplitConfig {
    fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            split_after: duration_from_seconds_env(
                "FLAREDB_SDF_SPLIT_AFTER_SECS",
                defaults.split_after,
            ),
            stall_after: duration_from_seconds_env(
                "FLAREDB_SDF_STALL_AFTER_SECS",
                defaults.stall_after,
            ),
            fraction_of_remainder: split_fraction_from_env(
                "FLAREDB_SDF_SPLIT_FRACTION",
                defaults.fraction_of_remainder,
            ),
        }
    }
}

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

/// A split residual routed to the dispatcher while its parent bundle is still
/// running.
pub type SdfWorkItemSender = UnboundedSender<(String, SdfWorkItem)>;

/// The byte span of one windowed input element plus the event-time metadata needed
/// to hold back downstream watermarks while that element is deferred.
#[derive(Clone, Debug)]
struct SdfInputSpan {
    start: usize,
    end: usize,
    timestamp_millis: i64,
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
    /// Residual work items reported with the final bundle response. Split
    /// residuals are sent directly to the dispatcher as soon as they are
    /// produced rather than waiting for this outcome.
    pub items: Vec<SdfWorkItem>,
    /// Duration between two consecutive `progress + split_evaluation` runs, after this bundle.
    pub progress_tick: Duration,
}

/// `Progress + split_evaluation` state for a single running bundle.
struct SplitMonitor {
    /// `beam:metric:data_channel:read_index:v1` at the previous tick,
    prev_index: i64,
    /// Sum of `beam:metric:element_count:v1` across every fused transform
    /// output at the previous tick.
    prev_output_element_count_sum: i64,
    /// SDK-reported completion fraction for the active element.
    prev_work_completed: Option<f32>,
    /// SDK-reported remaining fraction for the active element.
    prev_work_remaining: Option<f32>,
    /// Whether splitting is still allowed for this bundle;
    /// cleared once the SDK declines a split or progress polling fails.
    can_split: bool,
    /// Upper bound on remaining work.
    estimated_input_elements: i64,
    /// Duration between two consecutive `progress + split_evaluation` runs, for this bundle.
    tick: Duration,
    /// Lowest whole input element index already handed back as a residual.
    handed_back_from: Option<i64>,
    /// Whether at least one `progress + split_evaluation` run has happened for this bundle.
    ticked: bool,
    /// Number of progress polls made for this bundle.
    polls: u64,
    /// Number of `ProcessBundleSplit` requests sent for this bundle.
    split_requests: u64,
    /// Number of split responses that actually produced new work items.
    splits: u64,
    /// Wall-clock start used by the progress/time-based split policy.
    started: Instant,
    /// Most recent successful split, used to rate-limit time-based splits.
    last_split: Option<Instant>,
    config: SdfSplitConfig,
}

impl SplitMonitor {
    #[cfg(test)]
    fn new(estimated_input_elements: i64, progress_tick: Option<Duration>) -> Self {
        Self::with_config(
            estimated_input_elements,
            progress_tick,
            SdfSplitConfig::default(),
        )
    }

    fn with_config(
        estimated_input_elements: i64,
        progress_tick: Option<Duration>,
        config: SdfSplitConfig,
    ) -> Self {
        Self {
            // Starts at -2 so the first tick only record numbers.
            prev_index: -2,
            prev_output_element_count_sum: -2,
            prev_work_completed: None,
            prev_work_remaining: None,
            can_split: true,
            estimated_input_elements,
            tick: clamp_progress_tick(progress_tick.unwrap_or(MIN_PROGRESS_TICK)),
            handed_back_from: None,
            ticked: false,
            polls: 0,
            split_requests: 0,
            splits: 0,
            started: Instant::now(),
            last_split: None,
            config,
        }
    }
}

/// A snapshot of one `ProcessBundleProgressResponse`.
///
/// `read_index` is the input-element index of the SDK data-channel processor. It
/// remains `0` while the first input element is active, even if that one
/// restriction emits millions of outputs. `output_element_count_sum` is the sum
/// of `beam:metric:element_count:v1` across every PCollection produced by the
/// fused transforms, so it counts transform outputs rather than source records.
#[derive(Debug, Default)]
struct ProgressMetrics {
    read_index: i64,
    output_element_count_sum: i64,
    work_completed: Option<f32>,
    work_remaining: Option<f32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SplitDecision {
    Poll,
    SkipDisabled,
    SkipNoInput,
    SkipNoRemainingWork,
    WaitForProgress,
    RequestTime,
    RequestStall,
}

impl SplitDecision {
    fn action(self) -> &'static str {
        match self {
            Self::Poll => "poll",
            Self::SkipDisabled => "skip-splitting-disabled",
            Self::SkipNoInput => "skip-no-input-elements",
            Self::SkipNoRemainingWork => "skip-no-remaining-work",
            Self::WaitForProgress => "wait-for-progress",
            Self::RequestTime => "request-split-time",
            Self::RequestStall => "request-split-stall",
        }
    }

    fn requests_split(self) -> bool {
        matches!(self, Self::RequestTime | Self::RequestStall)
    }
}

impl SplitMonitor {
    fn decision(&self, metrics: &ProgressMetrics, now: Instant) -> SplitDecision {
        if !self.can_split {
            return SplitDecision::SkipDisabled;
        }
        if self.estimated_input_elements <= 0 {
            return SplitDecision::SkipNoInput;
        }
        if metrics
            .work_remaining
            .is_some_and(|remaining| remaining <= 0.0)
        {
            return SplitDecision::SkipNoRemainingWork;
        }

        let has_baseline = self.prev_index != -2;
        if !has_baseline {
            return SplitDecision::Poll;
        }

        let counters_changed = metrics.read_index != self.prev_index
            || metrics.output_element_count_sum != self.prev_output_element_count_sum;
        let work_changed = !same_float(self.prev_work_completed, metrics.work_completed)
            || !same_float(self.prev_work_remaining, metrics.work_remaining);
        let observed_progress = counters_changed || work_changed;
        let elapsed = now.duration_since(self.started);

        if observed_progress {
            let time_split_due = match self.last_split {
                Some(last_split) => now.duration_since(last_split) >= self.config.split_after,
                None => elapsed >= self.config.split_after,
            };
            if time_split_due {
                SplitDecision::RequestTime
            } else {
                SplitDecision::Poll
            }
        } else if elapsed >= self.config.stall_after {
            SplitDecision::RequestStall
        } else {
            SplitDecision::WaitForProgress
        }
    }

    fn record_poll(&mut self, metrics: &ProgressMetrics) {
        self.prev_index = metrics.read_index;
        self.prev_output_element_count_sum = metrics.output_element_count_sum;
        self.prev_work_completed = metrics.work_completed;
        self.prev_work_remaining = metrics.work_remaining;
    }

    fn record_split(&mut self, now: Instant) {
        self.last_split = Some(now);
    }
}

fn same_float(left: Option<f32>, right: Option<f32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.to_bits() == right.to_bits(),
        (None, None) => true,
        _ => false,
    }
}

fn output_endpoints(
    stage: &ExecutableStage,
    instruction_id: &str,
    output_edge_metadata: Vec<ConsumerMetaData>,
) -> Vec<(DataKey, ConsumerMetaData)> {
    output_edge_metadata
        .into_iter()
        .map(|metadata| {
            let data_key = DataKey {
                instruction_id: instruction_id.to_string(),
                transform_id: stage_sink_transform_id(stage, &metadata.produced_pcol_id),
            };
            (data_key, metadata)
        })
        .collect()
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
    /// from prior bundles and `progress_tick` is its progress-poll interval
    /// (`None` for the first); the outcome carries them forward along with the
    /// residual work items this bundle produced and the holds of the item it
    /// consumed. The caller schedules one bundle per work item and applies the item
    /// holds as output-watermark clamps until the item completes.
    pub async fn execute_bundle(
        &mut self,
        stage: &SplittableStage,
        output_edge_metadata: Vec<ConsumerMetaData>,
        seed: Option<SdfWorkItem>,
        initialized: bool,
        registered: bool,
        progress_tick: Option<Duration>,
        immediate_items: SdfWorkItemSender,
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
            let mut initialization_bytes = None;
            for stage in &plan.initialization_stages {
                initialization_bytes = Some(
                    self.run_initialization_stage(stage, initialization_bytes)
                        .await?,
                );
            }
            initialization_bytes.unwrap_or_default()
        };

        // Dynamic split only applies to restriction-based SDF form.
        let allow_split = matches!(
            plan.process_kind,
            SplittableProcessKind::ProcessSizedElementsAndRestrictions
        );

        let (response, progress_tick) = self
            .run_process_stage(
                &plan.process_stage,
                stage,
                output_edge_metadata,
                bytes,
                !registered,
                allow_split,
                progress_tick,
                immediate_items,
            )
            .await?;

        let items = residual_items(stage, &response.residual_roots);

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
        input: Option<Vec<u8>>,
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

        let timer_endpoints = stage_timer_endpoints(stage);
        let source_transform_id = stage_source_transform_id(stage);

        if let Some(input) = input {
            // A chained initialization step consumes the prior stage's raw
            // WindowedValue bytes without re-encoding them.
            self.send_raw_elements(
                &instruction_id,
                &source_transform_id,
                input,
                &timer_endpoints,
            )
            .await?;
        } else {
            let coder_id = stage.input_pcol().node().coder_id.clone();
            let runtime = self.runtime.clone();
            let instruction_id_clone = instruction_id.clone();
            let pcollection_id = stage.input_pcol().id().clone();
            let consumer_transform_id = source_transform_id;

            tokio::spawn(async move {
                // Send the first initialization stage's input from the
                // materialized PCollection.
                if let Err(err) = runtime
                    .process_input_elements(
                        instruction_id_clone.clone(),
                        consumer_transform_id,
                        pcollection_id,
                        coder_id,
                        None,
                        timer_endpoints,
                        Vec::new(),
                        false,
                    )
                    .await
                {
                    error!(
                        "failed to send SDF seed restrictions for instruction {}: {}",
                        instruction_id_clone, err
                    );
                }
            });
        }

        // Collect initialization output elements as raw wire bytes in memory.
        // Each initialization stage has exactly one output; anything else
        // indicates incorrect fusion of an unrelated transform into the SDF
        // initialization chain.
        let outputs = stage.output_pcols();
        if outputs.len() != 1 {
            return Err(anyhow!(
                "SDF initialization stage has {} output PCollections, expected exactly 1 \
                 (PairWithRestriction → SplitAndSize); this indicates the initialization \
                 stage was fused with an unrelated transform",
                outputs.len()
            ));
        }
        let output_pcol = outputs
            .iter()
            .next()
            .expect("initialization stage has exactly one output")
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

        let control_response = crate::engine::guard::idle_guard(
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
            crate::engine::guard::worker_idle_timeout(),
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
    /// on the worker across turns). `allow_split` enables dynamic split for this
    /// stage, and `progress_tick` is its progress-poll interval from prior bundles
    /// (`None` for the first). Split residuals are sent through
    /// `immediate_items`; the returned tuple carries the final response and the
    /// progress-poll interval to use after this bundle.
    async fn run_process_stage(
        &mut self,
        stage: &ExecutableStage,
        splittable_stage: &SplittableStage,
        output_edge_metadata: Vec<ConsumerMetaData>,
        seed: Vec<u8>,
        register: bool,
        allow_split: bool,
        progress_tick: Option<Duration>,
        immediate_items: SdfWorkItemSender,
    ) -> anyhow::Result<(ProcessBundleResponse, Duration)> {
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
        let input_pcol_id = input_pcol.id().clone();
        let input_coder_id = input_pcol.node().coder_id.clone();

        // Decode the input spans up front so `estimated_input_elements` is the
        // exact number of elements in this bundle rather than a placeholder. The
        // split loop reuses these spans instead of re-decoding. On a decode error
        // we leave the spans unset (so `poll_progress_and_maybe_split` can retry)
        // and fall back to a single-element estimate.
        let mut input_spans: Option<Vec<SdfInputSpan>> = if seed.is_empty() {
            Some(Vec::new())
        } else {
            match decode_windowed_value_spans(&self.runtime, &input_pcol_id, &input_coder_id, &seed)
            {
                Ok(spans) => Some(spans),
                Err(err) => {
                    warn!(
                        "failed to decode SDF process input spans up front ({}); \
                         falling back to a single-element estimate",
                        err
                    );
                    None
                }
            }
        };
        let estimated_input_elements = match &input_spans {
            Some(spans) if !spans.is_empty() => spans.len() as i64,
            _ => {
                if seed.is_empty() {
                    0
                } else {
                    1
                }
            }
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
        let input_runtime = self.runtime.clone();
        let response_future =
            control.recv_process_bundle_response(&instruction_id, bundle_response_rx);

        let mut split_residual_roots: Vec<DelayedBundleApplication> = Vec::new();
        let mut channel_items: Vec<SdfWorkItem> = Vec::new();
        let split_config = SdfSplitConfig::from_env();
        info!(
            "SDF split policy: split_after_ms={}, stall_after_ms={}, fraction={}",
            split_config.split_after.as_millis(),
            split_config.stall_after.as_millis(),
            split_config.fraction_of_remainder
        );
        let mut monitor =
            SplitMonitor::with_config(estimated_input_elements, progress_tick, split_config);
        let monitor_enabled = allow_split && estimated_input_elements > 0;

        let control_response = {
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
                                &input_runtime,
                                &input_pcol_id,
                                &input_coder_id,
                                &mut input_spans,
                                splittable_stage,
                                &mut split_residual_roots,
                                &mut channel_items,
                                &immediate_items,
                            ).await {
                                // Progress error stops further splitting.
                                warn!(
                                    "SDF `progress + split_evaluation` run failed for bundle {}, \
                                    disabling further splits for this bundle: {}",
                                    instruction_id,
                                    err
                                );
                                monitor.can_split = false;
                            }
                        }
                    }
                }?;

                Ok::<_, anyhow::Error>(response)
            };

            if !output_edge_metadata.is_empty() {
                let endpoints = output_endpoints(stage, &instruction_id, output_edge_metadata);
                let mut decode_tasks = Vec::with_capacity(endpoints.len());
                for (data_key, output_meta_data) in endpoints {
                    let receiver = self.runtime.data_receiver(data_key);
                    let output_runtime = self.runtime.clone();

                    decode_tasks.push(tokio::spawn(async move {
                        output_runtime
                            .process_output_elements(receiver, output_meta_data)
                            .await
                    }));
                }

                let decode_task = tokio::spawn(async move {
                    for task in decode_tasks {
                        task.await
                            .map_err(|err| anyhow!("output decode task panicked: {err}"))??;
                    }
                    Ok::<(), anyhow::Error>(())
                });

                let timeout_id = instruction_id.clone();
                crate::engine::guard::idle_guard(
                    async {
                        match process_future.await {
                            Ok(response) => {
                                decode_task.await.map_err(|err| {
                                    anyhow!("output decode tasks failed: {}", err)
                                })??;
                                Ok(response)
                            }
                            Err(err) => {
                                decode_task.abort();
                                Err(err)
                            }
                        }
                    },
                    crate::engine::guard::worker_idle_timeout(),
                    &format!(
                        "waiting for SDF process bundle {} output data and control response",
                        timeout_id
                    ),
                )
                .await?
            } else {
                let timeout_id = instruction_id.clone();
                crate::engine::guard::idle_guard(
                    process_future,
                    crate::engine::guard::worker_idle_timeout(),
                    &format!(
                        "waiting for SDF process bundle {} control response",
                        timeout_id
                    ),
                )
                .await?
            }
        };

        match control_response {
            ControlResponse::ProcessBundleSuccess(response) => {
                let next_tick = next_shared_tick(monitor.ticked, monitor.tick);
                info!(
                    "SDF bundle summary: instruction_id={instruction_id}, \
                     estimated_input_elements={estimated_input_elements}, polls={}, \
                     split_requests={}, splits={}, residual_roots={}, channel_items={}, \
                     next_tick_ms={}",
                    monitor.polls,
                    monitor.split_requests,
                    monitor.splits,
                    split_residual_roots.len(),
                    channel_items.len(),
                    next_tick.as_millis()
                );
                Ok((response, next_tick))
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

/// Decode the byte span and event timestamp of each `WindowedValue` in an SDF
/// process input.
fn decode_windowed_value_spans(
    runtime: &BundleRuntime,
    pcol_id: &str,
    coder_id: &str,
    bytes: &[u8],
) -> anyhow::Result<Vec<SdfInputSpan>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let element_coder =
        StandardBeamCoders::from_urn(coder_id, None, Some(runtime.pipeline_coders().as_ref()));
    let window_coder = runtime.window_coder_for_pcollection(pcol_id);
    let windowed_value_coder = WindowedValueCoder::with_window_coder(element_coder, window_coder);

    let mut cursor = Cursor::new(bytes);
    let mut spans = Vec::new();
    while (cursor.position() as usize) < bytes.len() {
        let start = cursor.position() as usize;
        let decoded = catch_unwind(AssertUnwindSafe(|| {
            windowed_value_coder.decode(&mut cursor)
        }));
        match decoded {
            Ok(Ok(value)) => spans.push(SdfInputSpan {
                start,
                end: cursor.position() as usize,
                timestamp_millis: value.timestamp_millis,
            }),
            Ok(Err(err)) => {
                return Err(anyhow!("failed to decode SDF input element: {:?}", err));
            }
            Err(_) => return Err(anyhow!("panic while decoding SDF input element")),
        }
    }

    Ok(spans)
}

/// Read progress and if bundle looks stalled, ask the SDK to split it.
#[allow(clippy::too_many_arguments)]
async fn poll_progress_and_maybe_split(
    control: &ControlChannel,
    bundle_instruction_id: &str,
    input_transform_id: &str,
    monitor: &mut SplitMonitor,
    input_bytes: &[u8],
    runtime: &BundleRuntime,
    input_pcol_id: &str,
    input_coder_id: &str,
    input_spans: &mut Option<Vec<SdfInputSpan>>,
    splittable_stage: &SplittableStage,
    split_residual_roots: &mut Vec<DelayedBundleApplication>,
    channel_items: &mut Vec<SdfWorkItem>,
    immediate_items: &SdfWorkItemSender,
) -> anyhow::Result<()> {
    monitor.ticked = true;
    monitor.polls += 1;

    let progress = control
        .send_process_bundle_progress_request(bundle_instruction_id)
        .await?;
    let metrics = extract_progress_metrics(&progress);
    let now = Instant::now();
    let first_poll = monitor.prev_index == -2;
    let decision = monitor.decision(&metrics, now);
    monitor.record_poll(&metrics);
    let action = decision.action();
    let poll_message = format!(
        "SDF progress: instruction_id={bundle_instruction_id}, read_index={}, \
         output_element_count_sum={}, work_completed={:?}, work_remaining={:?}, \
         action={action}, polls={}, can_split={}, estimated_input_elements={}, \
         tick_ms={}",
        metrics.read_index,
        metrics.output_element_count_sum,
        metrics.work_completed,
        metrics.work_remaining,
        monitor.polls,
        monitor.can_split,
        monitor.estimated_input_elements,
        monitor.tick.as_millis()
    );
    // Keep routine polls at DEBUG. The first sample and actual split decisions
    // are operator-visible without enabling a log firehose.
    if first_poll || decision.requests_split() {
        info!("{poll_message}");
    } else {
        debug!("{poll_message}");
    }

    if !decision.requests_split() {
        return Ok(());
    }

    if input_spans.is_none() {
        let spans =
            decode_windowed_value_spans(runtime, input_pcol_id, input_coder_id, input_bytes)
                .unwrap_or_else(|err| {
                    warn!(
                        "failed to decode SDF process input for counting/split ({}); \
                    whole-element split re-injection disabled for this bundle",
                        err
                    );
                    Vec::new()
                });
        if !spans.is_empty() {
            monitor.estimated_input_elements = spans.len() as i64;
        }
        *input_spans = Some(spans);
    }
    let input_spans = input_spans.as_deref().unwrap_or_default();

    let mut desired_splits = HashMap::new();
    desired_splits.insert(
        input_transform_id.to_string(),
        DesiredSplit {
            fraction_of_remainder: monitor.config.fraction_of_remainder,
            allowed_split_points: Vec::new(),
            estimated_input_elements: monitor.estimated_input_elements,
        },
    );

    monitor.split_requests += 1;
    let split = control
        .send_process_bundle_split_request(bundle_instruction_id, desired_splits)
        .await?;
    let channel_splits = split.channel_splits.len();
    let reported_roots = split.residual_roots.len();
    let residual_roots_before = split_residual_roots.len();
    let channel_items_before = channel_items.len();

    if split.channel_splits.is_empty() {
        // SDK cannot split (nothing left, or unsupported). Note that any
        // `residual_roots` in this response are not scheduled.
        monitor.can_split = false;
        info!(
            "SDF split response: instruction_id={bundle_instruction_id}, \
             residual_roots={reported_roots}, channel_splits=0, action=disable-splitting, \
             split_requests={}, splits={}",
            monitor.split_requests, monitor.splits
        );
        return Ok(());
    }

    if channel_splits != 1 {
        warn!(
            "SDF split returned {} channel splits; expected exactly one for the single input PCollection",
            channel_splits
        );
    }

    // Intra-element remainders (of one or more elements that are partially
    // processed): process in future.
    for item in residual_items(splittable_stage, &split.residual_roots) {
        immediate_items
            .send((splittable_stage.id().to_string(), item))
            .map_err(|_| anyhow!("SDF residual dispatcher closed"))?;
    }
    split_residual_roots.extend(split.residual_roots);

    // Whole elements that are entirely unprocessed: process in future.
    let input_element_count = input_spans.len() as i64;
    for channel_split in &split.channel_splits {
        let first = channel_split.first_residual_element;

        // A boundary at (or before the start of) the decoded input is the normal
        // case for an intra-element split: no whole elements remain, and the
        // residual is carried by `residual_roots`. Only a boundary outside
        // [0, len] or one that moves past an already-handed-back boundary is
        // invalid.
        if first < 0 || first > input_element_count {
            warn!(
                "ignoring out-of-range channel split: first_residual_element={}, \
                 decoded_input_elements={}",
                first, input_element_count
            );
            continue;
        }
        if let Some(previous) = monitor.handed_back_from
            && first > previous
        {
            warn!(
                "ignoring regressed channel split: first_residual_element={} is after \
                 already-handed-back boundary {}",
                first, previous
            );
            continue;
        }

        debug!(
            "SDF channel split: first_residual_element={}, handed_back_from={:?}, \
             decoded_input_elements={}",
            first, monitor.handed_back_from, input_element_count
        );

        if let Some((start, end, count)) =
            channel_split_range(first, monitor.handed_back_from, input_spans)
        {
            let timestamps = channel_item_timestamps(input_spans, monitor.handed_back_from, first);
            let item = SdfWorkItem {
                seed: input_bytes[start..end].to_vec(),
                holds: channel_item_holds(splittable_stage, &timestamps),
            };
            immediate_items
                .send((splittable_stage.id().to_string(), item.clone()))
                .map_err(|_| anyhow!("SDF residual dispatcher closed"))?;
            channel_items.push(item);
            monitor.handed_back_from = Some(first);
            debug_assert!(count > 0);
        }
        // Otherwise `first == input_element_count` (no whole elements) or
        // `first == handed_back_from` (nothing newly relinquished): no item.
    }

    if let Some(channel_split) = split.channel_splits.first()
        && monitor.estimated_input_elements >= channel_split.first_residual_element
    {
        monitor.estimated_input_elements = channel_split.first_residual_element;
    }

    // Increase interval to avoid over-split.
    monitor.tick = clamp_progress_tick(monitor.tick * 4);

    let new_residual_roots = split_residual_roots.len() - residual_roots_before;
    let new_channel_items = channel_items.len() - channel_items_before;
    if new_residual_roots + new_channel_items > 0 {
        monitor.splits += 1;
        monitor.record_split(now);
    }
    info!(
        "SDF split response: instruction_id={bundle_instruction_id}, \
         residual_roots={new_residual_roots}, channel_splits={channel_splits}, \
         channel_items={new_channel_items}, new_work_items={}, \
         split_requests={}, splits={}, next_tick_ms={}",
        new_residual_roots + new_channel_items,
        monitor.split_requests,
        monitor.splits,
        monitor.tick.as_millis()
    );
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
        .filter_map(|application| {
            if application.element.is_empty() {
                // An empty residual element would re-run the bundle with no input
                // and can loop forever; drop it rather than schedule a no-op.
                warn!(
                    "ignoring SDF residual for transform '{}': empty residual element",
                    application.transform_id
                );
                None
            } else {
                Some(application)
            }
        })
        .map(|application| {
            let holds = residual_item_holds(application, &outputs_by_transform, &boundary);
            debug!(
                "SDF residual root: transform='{}', output_watermarks={:?}, holds={:?}",
                application.transform_id, application.output_watermarks, holds
            );
            SdfWorkItem {
                seed: application.element.clone(),
                holds,
            }
        })
        .collect()
}

/// The output-watermark holds a single residual imposes on its stage's output.
///
/// A residual's `output_watermarks` is a lower bound on the timestamps of elements
/// the owning PTransform will still produce when the residual runs. The Fn API
/// specifies `MIN_TIMESTAMP` for an output the `RestrictionTracker` did not
/// report (`beam_fn_api.proto`, `BundleApplication.output_watermarks`), so every
/// boundary output of the residual's transform imposes **some** hold:
///
/// - a boundary output with a reported watermark holds at that watermark,
/// - a boundary output with no reported watermark (the entry is absent, or the
///   residual names a different local output) holds at [`MIN_TIMESTAMP`].
///
/// The second case is what covers intra-element residuals: the SDK's residual
/// root often reports an empty map, and ignoring it let the stage's output
/// watermark advance to `+inf` while deferred work was still outstanding. The
/// hold is released when the residual bundle that consumed the item completes.
///
/// Only outputs that leave the stage are tracked; internal PCollections are
/// consumed inside the fused stage and do not map to a watermark. Because a
/// residual can only influence its own stage, and SDF stages are isolated from
/// unrelated transforms by the fuser, defaulting *every* boundary output to
/// `MIN_TIMESTAMP` is conservative but sound.
fn residual_item_holds(
    application: &BundleApplication,
    outputs_by_transform: &HashMap<String, HashMap<String, String>>,
    boundary: &HashSet<&str>,
) -> Vec<Timestamp> {
    let Some(outputs) = outputs_by_transform.get(&application.transform_id) else {
        warn!(
            "SDF residual names an unknown transform '{}'; its output watermarks \
             will not hold the stage watermark",
            application.transform_id
        );
        return Vec::new();
    };

    // Resolve the boundary outputs this residual directly reports a watermark
    // for. Local outputs that are internal, unknown, or absent fall through to
    // the `MIN_TIMESTAMP` default below.
    let mut reported: HashMap<&str, Timestamp> = HashMap::new();
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
        reported
            .entry(pcollection.as_str())
            .and_modify(|current| *current = (*current).min(hold))
            .or_insert(hold);
    }

    boundary
        .iter()
        .map(|pcollection| reported.get(pcollection).copied().unwrap_or(MIN_TIMESTAMP))
        .collect()
}

/// Extract the event timestamps of the input elements a whole-element channel
/// split hands back.
fn channel_item_timestamps(
    input_spans: &[SdfInputSpan],
    handed_back_from: Option<i64>,
    first_residual_element: i64,
) -> Vec<i64> {
    let first = first_residual_element as usize;
    let upper = handed_back_from
        .map(|value| value as usize)
        .unwrap_or(input_spans.len());

    if first >= upper {
        return Vec::new();
    }

    input_spans[first..upper]
        .iter()
        .map(|span| span.timestamp_millis)
        .collect()
}

/// Output-watermark holds for whole-element SDF residuals.
///
/// Unlike SDK residual roots, channel splits do not carry `output_watermarks`.
/// The earliest event time of the elements being handed back is therefore used as
/// the lower bound, and one hold is emitted per boundary output PCollection to
/// match [`residual_item_holds`]. A hold at or below `MIN_TIMESTAMP` is omitted so
/// global-window/batch work is not stalled forever.
fn channel_item_holds(splittable_stage: &SplittableStage, timestamps: &[i64]) -> Vec<Timestamp> {
    let boundary: HashSet<&str> = splittable_stage
        .output_pcols()
        .iter()
        .map(String::as_str)
        .collect();

    if boundary.is_empty() {
        debug!(
            "SDF channel item: no boundary output PCollections; timestamps={:?}; no hold",
            timestamps
        );
        return Vec::new();
    }

    let Some(min_timestamp) = timestamps.iter().copied().min() else {
        debug!(
            "SDF channel item: no timestamps; boundary_outputs={}; no hold",
            boundary.len()
        );
        return Vec::new();
    };
    if min_timestamp <= MIN_TIMESTAMP {
        debug!(
            "SDF channel item: min timestamp {} <= MIN_TIMESTAMP; timestamps={:?}; no hold",
            min_timestamp, timestamps
        );
        return Vec::new();
    }

    debug!(
        "SDF channel item: hold={} for {} boundary output(s); timestamps={:?}",
        min_timestamp,
        boundary.len(),
        timestamps
    );
    boundary.iter().map(|_| min_timestamp).collect()
}

fn clamp_progress_tick(tick: Duration) -> Duration {
    tick.max(MIN_PROGRESS_TICK).min(MAX_PROGRESS_TICK)
}

fn duration_from_seconds_env(name: &str, default: Duration) -> Duration {
    duration_from_seconds_value(std::env::var(name).ok().as_deref(), default)
}

fn duration_from_seconds_value(configured: Option<&str>, default: Duration) -> Duration {
    configured
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or(default)
}

fn split_fraction_from_env(name: &str, default: f64) -> f64 {
    split_fraction_from_value(std::env::var(name).ok().as_deref(), default, name)
}

fn split_fraction_from_value(configured: Option<&str>, default: f64, name: &str) -> f64 {
    let Some(configured) = configured.and_then(|value| value.trim().parse::<f64>().ok()) else {
        return default;
    };
    if configured.is_finite() && configured > 0.0 && configured < 1.0 {
        configured
    } else {
        warn!("ignoring invalid {name}={configured}; expected a fraction between 0 and 1");
        default
    }
}

/// The stage-shared progress tick to carry forward after a bundle.
/// A bundle that polled at least once keeps its current tick.
/// A bundle that finished before its first poll shrinks the tick
/// by one MIN_PROGRESS_TICK (floored), so short bundles pull the
/// wait back down. Shrinking is far slower than the ×4 growth on each
/// split, balancing over-splitting against still allowing genuine splits.
fn next_shared_tick(ticked: bool, tick: Duration) -> Duration {
    if ticked {
        tick
    } else {
        clamp_progress_tick(tick.saturating_sub(MIN_PROGRESS_TICK))
    }
}

/// Decode one beam varint (`beam:coder:varint:v1`) payload.
fn decode_varint_payload(payload: &[u8]) -> Option<i64> {
    let mut buf = payload;
    match catch_unwind(AssertUnwindSafe(|| VarIntCoder.decode(&mut buf))) {
        Ok(Ok(decoded)) => Some(decoded),
        _ => None,
    }
}

/// Extract data-channel position, transform-output counts, and SDK work progress.
fn extract_progress_metrics(resp: &ProcessBundleProgressResponse) -> ProgressMetrics {
    let mut metrics = ProgressMetrics::default();

    for info in &resp.monitoring_infos {
        match info.urn.as_str() {
            beam_urns::DATA_CHANNEL_READ_INDEX_METRIC => {
                if let Some(value) = decode_varint_payload(&info.payload) {
                    metrics.read_index = metrics.read_index.max(value);
                }
            }
            beam_urns::ELEMENT_COUNT_METRIC => {
                if let Some(value) = decode_varint_payload(&info.payload) {
                    metrics.output_element_count_sum += value;
                }
            }
            beam_urns::WORK_COMPLETED_METRIC => {
                if let Some(value) = decode_progress_payload(&info.payload) {
                    metrics.work_completed = Some(
                        metrics
                            .work_completed
                            .map_or(value, |current| current.max(value)),
                    );
                }
            }
            beam_urns::WORK_REMAINING_METRIC => {
                if let Some(value) = decode_progress_payload(&info.payload) {
                    metrics.work_remaining = Some(
                        metrics
                            .work_remaining
                            .map_or(value, |current| current.min(value)),
                    );
                }
            }
            _ => {}
        }
    }
    metrics
}

/// Decode `beam:metrics:progress:v1` payloads, currently
/// `IterableCoder(FloatCoder)` with one value for the active element.
fn decode_progress_payload(payload: &[u8]) -> Option<f32> {
    let mut buf = payload;
    if buf.remaining() < 8 {
        return None;
    }
    let count = buf.get_i32();
    if count < 1 {
        return None;
    }
    Some(buf.get_f32())
}

/// The byte range and element count a channel split hands back.
/// first_residual_element is an absolute 0-based data-channel index;
/// handed_back_from is the exclusive upper bound of elements already
/// handed back (None = through the end of input_spans).
/// Returns (start, end, count) for the newly relinquished [first, upper) range,
/// or None when nothing new is relinquished or the boundary is out of range.
fn channel_split_range(
    first_residual_element: i64,
    handed_back_from: Option<i64>,
    input_spans: &[SdfInputSpan],
) -> Option<(usize, usize, i64)> {
    let first = first_residual_element as usize;
    let upper = handed_back_from
        .map(|value| value as usize)
        .unwrap_or(input_spans.len());

    if first < upper {
        let start = input_spans[first].start;
        let end = input_spans[upper - 1].end;
        let count = (upper - first) as i64;
        return Some((start, end, count));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_model_rs::v1::executable_stage_payload::WireCoderSetting;
    use beam_model_rs::v1::{
        BundleApplication, Components, Environment, MonitoringInfo, PCollection, PTransform,
    };
    use bytes::BufMut;
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
                initialization_stages: vec![stage.clone()],
                process_stage: stage,
                process_kind: SplittableProcessKind::ProcessElements,
            },
        )
    }

    fn output_metadata(pcollection_id: &str) -> ConsumerMetaData {
        ConsumerMetaData {
            producer_transform_id: "producer".to_string(),
            produced_pcol_id: pcollection_id.to_string(),
            coder_id: "coder".to_string(),
            component_coder: None,
            consumer_transfrom_id: "consumer".to_string(),
        }
    }

    #[test]
    fn every_sdf_boundary_output_gets_a_decoder_endpoint() {
        let splittable = splittable_stage(
            &["out-0", "out-1", "out-2"],
            "multi-output",
            &[("0", "out-0"), ("1", "out-1"), ("2", "out-2")],
        );
        let stage = splittable.plan().process_stage.clone();
        let endpoints = output_endpoints(
            &stage,
            "instruction",
            vec![
                output_metadata("out-0"),
                output_metadata("out-1"),
                output_metadata("out-2"),
            ],
        );

        assert_eq!(endpoints.len(), 3);
        for pcollection_id in ["out-0", "out-1", "out-2"] {
            assert!(endpoints.iter().any(|(key, metadata)| {
                metadata.produced_pcol_id == pcollection_id
                    && key.transform_id == stage_sink_transform_id(&stage, pcollection_id)
            }));
        }
    }

    fn residual(transform_id: &str, local_output: &str, millis: i64) -> DelayedBundleApplication {
        DelayedBundleApplication {
            application: Some(BundleApplication {
                transform_id: transform_id.to_string(),
                // Non-empty element so the residual is not dropped as invalid; the
                // holds tests are independent of the encoded element bytes.
                element: b"element".to_vec(),
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
    fn residual_items_default_unreported_boundary_outputs_to_min_timestamp() {
        let stage = splittable_stage(
            &["pcol-out"],
            "process",
            &[("out", "pcol-out"), ("internal", "pcol-internal")],
        );

        // A boundary output reported at 5000ms becomes a hold at 5000.
        let items = residual_items(&stage, &[residual("process", "out", 5_000)]);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].holds, vec![5_000]);

        // An internal output is not tracked by the runner, so the residual
        // reports no usable watermark for the boundary output: the Fn API's
        // MIN_TIMESTAMP default applies (it may still produce anything).
        let items = residual_items(&stage, &[residual("process", "internal", 5_000)]);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].holds, vec![MIN_TIMESTAMP]);

        // The same default covers a local output name not declared on the
        // transform: the boundary output was not reported.
        assert_eq!(
            residual_items(&stage, &[residual("process", "missing", 5_000)])[0].holds,
            vec![MIN_TIMESTAMP]
        );

        // A transform that is not part of the process stage cannot be mapped to
        // an output at all; the existing warning path imposes no hold.
        assert!(
            residual_items(&stage, &[residual("elsewhere", "out", 5_000)])[0]
                .holds
                .is_empty()
        );

        // No residuals: no items.
        assert!(residual_items(&stage, &[]).is_empty());
    }

    #[test]
    fn residual_items_hold_explicit_min_timestamp_reports() {
        let stage = splittable_stage(&["pcol-out"], "process", &[("out", "pcol-out")]);

        // A tracker that reports "no progress" as MIN_TIMESTAMP is held at MIN,
        // not treated as "no hold".
        let items = residual_items(&stage, &[residual("process", "out", MIN_TIMESTAMP)]);
        assert_eq!(items[0].holds, vec![MIN_TIMESTAMP]);
    }

    #[test]
    fn residual_items_hold_every_boundary_output() {
        let stage = splittable_stage(
            &["pcol-a", "pcol-b"],
            "process",
            &[("a", "pcol-a"), ("b", "pcol-b")],
        );

        let mut application = residual("process", "a", 5_000);
        application
            .application
            .as_mut()
            .unwrap()
            .output_watermarks
            .remove("a");
        // Nothing reported: both boundary outputs hold at MIN.
        let items = residual_items(&stage, &[application.clone()]);
        assert_eq!(items[0].holds.len(), 2);
        assert!(items[0].holds.iter().all(|hold| *hold == MIN_TIMESTAMP));

        // Reporting one output holds that one at 5000 and the other at MIN.
        application
            .application
            .as_mut()
            .unwrap()
            .output_watermarks
            .insert(
                "a".to_string(),
                prost_types::Timestamp {
                    seconds: 5,
                    nanos: 0,
                },
            );
        let mut holds = residual_items(&stage, &[application])[0].holds.clone();
        holds.sort_unstable();
        assert_eq!(holds, vec![MIN_TIMESTAMP, 5_000]);
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

    #[test]
    fn residual_items_skip_empty_element_applications() {
        let stage = splittable_stage(&["pcol-out"], "process", &[("out", "pcol-out")]);

        let mut empty_element = residual("process", "out", 5_000);
        empty_element.application.as_mut().unwrap().element = Vec::new();
        let mut valid = residual("process", "out", 2_000);
        valid.application.as_mut().unwrap().element = b"valid".to_vec();

        let items = residual_items(&stage, &[empty_element, valid]);
        assert_eq!(items.len(), 1, "empty-element residuals must be dropped");
        assert_eq!(items[0].seed, b"valid".to_vec());
        assert_eq!(items[0].holds, vec![2_000]);
    }

    #[test]
    fn channel_item_timestamps_returns_only_the_newly_handed_back_range() {
        let spans = vec![
            SdfInputSpan {
                start: 0,
                end: 2,
                timestamp_millis: 10,
            },
            SdfInputSpan {
                start: 2,
                end: 4,
                timestamp_millis: 20,
            },
            SdfInputSpan {
                start: 4,
                end: 6,
                timestamp_millis: 30,
            },
            SdfInputSpan {
                start: 6,
                end: 8,
                timestamp_millis: 40,
            },
        ];

        assert_eq!(channel_item_timestamps(&spans, None, 2), vec![30, 40]);
        assert_eq!(channel_item_timestamps(&spans, Some(3), 1), vec![20, 30]);
        assert!(channel_item_timestamps(&spans, Some(2), 2).is_empty());
    }

    #[test]
    fn channel_item_holds_uses_the_earliest_timestamp_per_boundary_output() {
        let stage = splittable_stage(
            &["pcol-a", "pcol-b"],
            "process",
            &[("a", "pcol-a"), ("b", "pcol-b")],
        );

        let mut holds = channel_item_holds(&stage, &[5_000, 2_000, 4_000]);
        holds.sort_unstable();
        assert_eq!(holds, vec![2_000, 2_000]);
    }

    #[test]
    fn channel_item_holds_skips_empty_min_or_non_boundary_timestamps() {
        let stage = splittable_stage(&["pcol-out"], "process", &[("out", "pcol-out")]);
        assert!(channel_item_holds(&stage, &[]).is_empty());
        assert!(channel_item_holds(&stage, &[MIN_TIMESTAMP, 1_000]).is_empty());

        let no_output = splittable_stage(&[], "process", &[]);
        assert!(channel_item_holds(&no_output, &[1_000]).is_empty());
    }

    /// Encode a signed varint the way the SDK encodes metric payloads.
    fn varint_payload(value: i64) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        VarIntCoder.encode(value, &mut buf);
        buf.to_vec()
    }

    fn progress_info(urn: &str, value: i64) -> MonitoringInfo {
        MonitoringInfo {
            urn: urn.to_string(),
            payload: varint_payload(value),
            ..Default::default()
        }
    }

    /// Back-to-back 2-byte elements: element `i` occupies `[2i, 2i + 2)`.
    fn spans(n: usize) -> Vec<SdfInputSpan> {
        (0..n)
            .map(|i| SdfInputSpan {
                start: 2 * i,
                end: 2 * i + 2,
                timestamp_millis: 0,
            })
            .collect()
    }

    #[test]
    fn progress_tick_is_clamped_to_the_authoritative_range() {
        assert_eq!(
            clamp_progress_tick(Duration::from_millis(1)),
            MIN_PROGRESS_TICK
        );
        assert_eq!(clamp_progress_tick(MIN_PROGRESS_TICK), MIN_PROGRESS_TICK);
        assert_eq!(
            clamp_progress_tick(Duration::from_millis(400)),
            Duration::from_millis(400)
        );
        assert_eq!(
            clamp_progress_tick(Duration::from_secs(60)),
            MAX_PROGRESS_TICK
        );
        assert_eq!(clamp_progress_tick(MAX_PROGRESS_TICK), MAX_PROGRESS_TICK);
    }

    #[test]
    fn split_backoff_grows_before_clamping_at_the_ceiling() {
        // Flare multiplies the stage tick by 4 after each successful split and
        // clamps to [100ms, 30s]; the sequence is what keeps residual bundles
        // from re-splitting aggressively.
        let mut tick = MIN_PROGRESS_TICK;
        let mut observed = Vec::new();
        for _ in 0..7 {
            tick = clamp_progress_tick(tick * 4);
            observed.push(tick);
        }
        assert_eq!(
            observed,
            vec![
                Duration::from_millis(400),
                Duration::from_millis(1_600),
                Duration::from_millis(6_400),
                Duration::from_millis(25_600),
                MAX_PROGRESS_TICK,
                MAX_PROGRESS_TICK,
                MAX_PROGRESS_TICK,
            ]
        );
    }

    #[test]
    fn next_shared_tick_passes_through_when_ticked_and_shrinks_when_not() {
        // A bundle that polled at least once keeps the shared tick unchanged.
        assert_eq!(
            next_shared_tick(true, Duration::from_millis(1_600)),
            Duration::from_millis(1_600)
        );
        // A bundle that finished before its first poll shrinks it by one step.
        assert_eq!(
            next_shared_tick(false, Duration::from_millis(1_600)),
            Duration::from_millis(1_500)
        );
        // Shrinking never goes below the floor.
        assert_eq!(
            next_shared_tick(false, MIN_PROGRESS_TICK),
            MIN_PROGRESS_TICK
        );
        assert_eq!(
            next_shared_tick(false, Duration::from_millis(50)),
            MIN_PROGRESS_TICK
        );
    }

    #[test]
    fn monitor_adopts_the_stage_shared_tick_and_clamps_it() {
        // No shared tick yet: the first bundle starts at the floor, with clean
        // `-2` baselines so the first poll only records numbers.
        let monitor = SplitMonitor::new(7, None);
        assert_eq!(monitor.tick, MIN_PROGRESS_TICK);
        assert_eq!(monitor.estimated_input_elements, 7);
        assert_eq!(monitor.prev_index, -2);
        assert_eq!(monitor.prev_output_element_count_sum, -2);
        assert!(monitor.can_split);
        assert!(!monitor.ticked);
        assert_eq!(monitor.handed_back_from, None);

        // An in-range shared tick is adopted verbatim; out-of-range is clamped.
        assert_eq!(
            SplitMonitor::new(1, Some(Duration::from_secs(2))).tick,
            Duration::from_secs(2)
        );
        assert_eq!(
            SplitMonitor::new(1, Some(Duration::from_millis(1))).tick,
            MIN_PROGRESS_TICK
        );
        assert_eq!(
            SplitMonitor::new(1, Some(Duration::from_secs(120))).tick,
            MAX_PROGRESS_TICK
        );
    }

    #[test]
    fn decode_varint_payload_round_trips_signed_values() {
        for value in [0i64, 1, 127, 128, 300, -1, -128] {
            assert_eq!(decode_varint_payload(&varint_payload(value)), Some(value));
        }
    }

    #[test]
    fn progress_metrics_sum_element_counts_and_take_the_max_read_index() {
        let resp = ProcessBundleProgressResponse {
            monitoring_infos: vec![
                progress_info(beam_urns::DATA_CHANNEL_READ_INDEX_METRIC, 3),
                progress_info(beam_urns::DATA_CHANNEL_READ_INDEX_METRIC, 5),
                progress_info(beam_urns::ELEMENT_COUNT_METRIC, 4),
                progress_info(beam_urns::ELEMENT_COUNT_METRIC, 2),
                // Unrelated URNs are ignored.
                progress_info("beam:metric:some_other:v1", 999),
            ],
            ..Default::default()
        };
        let metrics = extract_progress_metrics(&resp);
        assert_eq!(metrics.read_index, 5);
        assert_eq!(metrics.output_element_count_sum, 6);
        assert_eq!(metrics.work_completed, None);
        assert_eq!(metrics.work_remaining, None);
    }

    #[test]
    fn progress_metrics_are_empty_when_absent() {
        let metrics = extract_progress_metrics(&ProcessBundleProgressResponse::default());
        assert_eq!(metrics.read_index, 0);
        assert_eq!(metrics.output_element_count_sum, 0);
        assert_eq!(metrics.work_completed, None);
        assert_eq!(metrics.work_remaining, None);
    }

    #[test]
    fn read_index_stays_zero_while_the_first_input_element_is_active() {
        // The SDK reports read_index = -1 before it reads the first element. The
        // accumulator starts at 0 and combines with `max`, so -1 is floored to 0.
        // It stays 0 throughout the first active input element even when fused
        // transforms emit millions of rows; it is an input index, not output work.
        let resp = ProcessBundleProgressResponse {
            monitoring_infos: vec![
                progress_info(beam_urns::DATA_CHANNEL_READ_INDEX_METRIC, -1),
                progress_info(beam_urns::ELEMENT_COUNT_METRIC, 8_600_000),
            ],
            ..Default::default()
        };
        let metrics = extract_progress_metrics(&resp);
        assert_eq!(metrics.read_index, 0);
        assert_eq!(metrics.output_element_count_sum, 8_600_000);
    }

    fn progress_float_info(urn: &str, value: f32) -> MonitoringInfo {
        let mut payload = Vec::new();
        payload.put_i32(1);
        payload.put_f32(value);
        MonitoringInfo {
            urn: urn.to_string(),
            r#type: "beam:metrics:progress:v1".to_string(),
            payload,
            labels: HashMap::new(),
            start_time: None,
        }
    }

    fn split_config() -> SdfSplitConfig {
        SdfSplitConfig {
            split_after: Duration::from_secs(2),
            stall_after: Duration::from_secs(5),
            fraction_of_remainder: 0.5,
        }
    }

    #[test]
    fn progress_metrics_extract_sdk_work_progress() {
        let resp = ProcessBundleProgressResponse {
            monitoring_infos: vec![
                progress_float_info(beam_urns::WORK_COMPLETED_METRIC, 0.25),
                progress_float_info(beam_urns::WORK_REMAINING_METRIC, 0.75),
            ],
            ..Default::default()
        };
        let metrics = extract_progress_metrics(&resp);
        assert_eq!(metrics.work_completed, Some(0.25));
        assert_eq!(metrics.work_remaining, Some(0.75));
    }

    #[test]
    fn sdk_work_progress_prevents_a_false_stall_while_outputs_are_buffered() {
        let mut monitor = SplitMonitor::with_config(100, None, split_config());
        let now = Instant::now();
        monitor.record_poll(&ProgressMetrics {
            read_index: 0,
            output_element_count_sum: 0,
            work_completed: Some(0.1),
            work_remaining: Some(0.9),
        });
        let decision = monitor.decision(
            &ProgressMetrics {
                read_index: 0,
                output_element_count_sum: 0,
                work_completed: Some(0.2),
                work_remaining: Some(0.8),
            },
            now + Duration::from_secs(3),
        );
        assert_eq!(decision, SplitDecision::RequestTime);
        assert!(
            !matches!(decision, SplitDecision::RequestStall),
            "SDK work progress must not be classified as a stall"
        );
    }

    #[test]
    fn fallback_stall_detection_waits_for_the_configured_quiet_window() {
        let mut monitor = SplitMonitor::with_config(100, None, split_config());
        monitor.record_poll(&ProgressMetrics::default());
        let now = Instant::now();
        assert_eq!(
            monitor.decision(&ProgressMetrics::default(), now + Duration::from_secs(1)),
            SplitDecision::WaitForProgress
        );
        assert_eq!(
            monitor.decision(&ProgressMetrics::default(), now + Duration::from_secs(6)),
            SplitDecision::RequestStall
        );
    }

    #[test]
    fn completed_sdk_work_skips_a_redundant_split_request() {
        let monitor = SplitMonitor::with_config(1, None, split_config());
        let decision = monitor.decision(
            &ProgressMetrics {
                work_completed: Some(1.0),
                work_remaining: Some(0.0),
                ..Default::default()
            },
            Instant::now() + Duration::from_secs(10),
        );
        assert_eq!(decision, SplitDecision::SkipNoRemainingWork);
    }

    #[test]
    fn split_delay_and_fraction_are_configurable() {
        assert_eq!(
            duration_from_seconds_value(Some("0.75"), Duration::from_secs(2)),
            Duration::from_millis(750)
        );
        assert_eq!(
            duration_from_seconds_value(Some("0"), Duration::from_secs(2)),
            Duration::from_secs(2)
        );
        assert_eq!(
            split_fraction_from_value(Some("0.25"), 0.5, "FLAREDB_SDF_SPLIT_FRACTION"),
            0.25
        );
        assert_eq!(
            split_fraction_from_value(Some("1.0"), 0.5, "FLAREDB_SDF_SPLIT_FRACTION"),
            0.5
        );
    }

    #[test]
    fn channel_split_range_hands_back_the_whole_tail_on_first_split() {
        let spans = spans(4);
        // First split at element 2 with nothing handed back yet: elements 2..4.
        assert_eq!(channel_split_range(2, None, &spans), Some((4, 8, 2)));
        // A split at element 0 hands back everything.
        assert_eq!(channel_split_range(0, None, &spans), Some((0, 8, 4)));
    }

    #[test]
    fn channel_split_range_only_hands_back_newly_relinquished_elements() {
        let spans = spans(4);
        // Boundary moved down to 1 since the prior hand-back from 2: element 1.
        assert_eq!(channel_split_range(1, Some(2), &spans), Some((2, 4, 1)));
        // A deeper move hands back elements 1 and 2.
        assert_eq!(channel_split_range(1, Some(3), &spans), Some((2, 6, 2)));
        // Boundary did not move down: nothing new (no duplicate hand-back).
        assert_eq!(channel_split_range(2, Some(2), &spans), None);
        assert_eq!(channel_split_range(3, Some(2), &spans), None);
        assert_eq!(channel_split_range(0, Some(0), &spans), None);
    }

    #[test]
    fn channel_split_range_rejects_out_of_range_boundaries() {
        let spans = spans(3);
        // The residual can start at/after the data we sent: filter those out.
        assert_eq!(channel_split_range(3, None, &spans), None);
        assert_eq!(channel_split_range(9, None, &spans), None);
        // A negative boundary can never index an element.
        assert_eq!(channel_split_range(-1, None, &spans), None);
        // Empty input has nothing to hand back.
        assert_eq!(channel_split_range(0, None, &[]), None);
    }
}
