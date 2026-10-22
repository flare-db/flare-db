use std::collections::{HashMap, HashSet};

use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};

use std::{
    ops::Range,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::anyhow;
use beam_model_rs::v1::process_bundle_split_request::DesiredSplit;
use beam_model_rs::v1::{
    BundleApplication, DelayedBundleApplication, ProcessBundleProgressResponse,
    ProcessBundleResponse,
};
use bytes::Buf;
use log::{debug, error, info, warn};
use tokio::sync::mpsc::UnboundedSender;

use crate::coders::primitives::{VarIntCoder, WindowedValue, WindowedValueCoder};
use crate::coders::{BeamCoder, StandardBeamCoders};
use crate::engine::harness::control::ControlChannel;
use crate::engine::metrics::sum_element_count;
use crate::fusion::stage::SplittableProcessKind;
use crate::jobservice::urns::beam_urns;
use crate::store::record::{BeamRecord, PrimitiveValue};
use crate::worker::pool::WorkerPool;
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

/// Default duration between progress polls.
const DEFAULT_PROGRESS_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Default fraction of remaining work the SDK keeps after a split.
const DEFAULT_SPLIT_FRACTION_OF_REMAINDER: f64 = 0.5;
/// Default per-bundle cooldown after a successful split.
const DEFAULT_SPLIT_COOLDOWN: Duration = Duration::from_secs(2);
/// Default minimum estimated remaining duration before a split is useful.
const DEFAULT_MIN_SPLIT: Duration = Duration::from_secs(10);
/// Spawn cost multiplier used for the default minimum split duration.
const SPAWN_COST_MULTIPLIER: u32 = 3;
/// Default no-progress window for the capacity-gated stall fallback.
const DEFAULT_STALL_AFTER: Duration = Duration::from_secs(5);
/// Default maximum recursive split depth.
const DEFAULT_MAX_SPLIT_DEPTH: usize = 4;
/// Stop creating new bundles after this many per configured worker.
const MAX_BUNDLES_PER_WORKER: usize = 8;
/// Default number of initial SDF partitions per configured worker.
const DEFAULT_SDF_OVERDECOMPOSE: usize = 4;
/// Minimum duration between estimator rate samples.
const ESTIMATOR_RATE_WINDOW: Duration = Duration::from_millis(500);
const BUNDLE_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct SdfSplitConfig {
    progress_poll_interval: Duration,
    stall_after: Duration,
    cooldown: Duration,
    min_split_override: Option<Duration>,
    fraction_of_remainder: f64,
    max_depth: usize,
}

impl Default for SdfSplitConfig {
    fn default() -> Self {
        Self {
            progress_poll_interval: DEFAULT_PROGRESS_POLL_INTERVAL,
            stall_after: DEFAULT_STALL_AFTER,
            cooldown: DEFAULT_SPLIT_COOLDOWN,
            min_split_override: None,
            fraction_of_remainder: DEFAULT_SPLIT_FRACTION_OF_REMAINDER,
            max_depth: DEFAULT_MAX_SPLIT_DEPTH,
        }
    }
}

impl SdfSplitConfig {
    fn from_env() -> Self {
        let defaults = Self::default();
        let min_split_override = std::env::var("FLAREDB_SDF_MIN_SPLIT_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(Duration::from_secs_f64);
        Self {
            progress_poll_interval: duration_from_seconds_env(
                "FLAREDB_SDF_PROGRESS_POLL_SECS",
                defaults.progress_poll_interval,
            ),
            stall_after: duration_from_seconds_env(
                "FLAREDB_SDF_STALL_AFTER_SECS",
                defaults.stall_after,
            ),
            cooldown: duration_from_seconds_env(
                "FLAREDB_SDF_SPLIT_COOLDOWN_SECS",
                defaults.cooldown,
            ),
            min_split_override,
            fraction_of_remainder: split_fraction_from_env(
                "FLAREDB_SDF_SPLIT_FRACTION",
                defaults.fraction_of_remainder,
            ),
            max_depth: positive_usize_from_env("FLAREDB_SDF_MAX_SPLIT_DEPTH", defaults.max_depth),
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
    /// SDK-reported SplitAndSize work estimate, when available. This is used to
    /// order initial partitions and feed the progress estimator.
    pub estimated_size: Option<f64>,
    /// Number of recursive splits that produced this item.
    pub split_depth: usize,
    /// Split-manager permit reservation that stays filled until this item is
    /// picked up by the dispatcher.
    pub permit_id: Option<u64>,
}

/// A split residual routed to the dispatcher while its parent bundle is still
/// running.
pub type SdfWorkItemSender = UnboundedSender<(String, SdfWorkItem)>;

/// The byte span of one windowed input element plus the event-time and
/// SplitAndSize metadata needed for partitioning, progress, and watermark holds.
#[derive(Clone, Debug)]
struct SdfInputSpan {
    start: usize,
    end: usize,
    timestamp_millis: i64,
    size: Option<f64>,
}

/// What one executed SDF bundle reports back to the dispatcher.
#[derive(Debug, Default)]
pub struct SdfBundleOutcome {
    /// The initialization bundle has run (carried forward across bundles).
    pub initialized: bool,
    /// Output-watermark holds of the work item this bundle consumed (to release).
    pub consumed_holds: Vec<Timestamp>,
    /// Residual work items reported with the final bundle response. Split
    /// residuals are sent directly to the dispatcher as soon as they are
    /// produced rather than waiting for this outcome.
    pub items: Vec<SdfWorkItem>,
    /// Duration between two consecutive `progress + split_evaluation` runs, after this bundle.
    pub progress_tick: Duration,
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
enum ProgressEstimatorMode {
    Weighted,
    Coarse,
    None,
}

impl ProgressEstimatorMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Weighted => "weighted",
            Self::Coarse => "coarse",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BundleProgressEstimate {
    progress: Option<f64>,
    rate_per_second: Option<f64>,
    est_remaining: Option<Duration>,
    mode: ProgressEstimatorMode,
    changed: bool,
}

struct BundleSplitRuntime {
    estimator: ProgressEstimator,
    handed_back_from: Option<i64>,
    polls: u64,
    split_requests: u64,
    splits: u64,
    last_denial: Option<SplitDenyReason>,
    last_progress_log_at: Instant,
}

impl BundleSplitRuntime {
    fn new(now: Instant) -> Self {
        Self {
            estimator: ProgressEstimator::new(now),
            handed_back_from: None,
            polls: 0,
            split_requests: 0,
            splits: 0,
            last_denial: None,
            last_progress_log_at: now,
        }
    }
}

struct ProgressEstimator {
    rate_window: Option<(Instant, f64)>,
    rate_per_second: Option<f64>,
    last_progress: f64,
    last_progress_at: Instant,
    last_metrics: Option<(i64, i64, Option<u32>, Option<u32>)>,
}

impl ProgressEstimator {
    fn new(now: Instant) -> Self {
        Self {
            rate_window: None,
            rate_per_second: None,
            last_progress: 0.0,
            last_progress_at: now,
            last_metrics: None,
        }
    }

    fn observe(
        &mut self,
        metrics: &ProgressMetrics,
        spans: &[SdfInputSpan],
        now: Instant,
    ) -> BundleProgressEstimate {
        let active_fraction = active_work_fraction(metrics);
        let (progress, mode) = estimate_progress(metrics.read_index, active_fraction, spans);
        let metrics_signature = (
            metrics.read_index,
            metrics.output_element_count_sum,
            metrics.work_completed.map(f32::to_bits),
            metrics.work_remaining.map(f32::to_bits),
        );
        let metrics_changed = self.last_metrics != Some(metrics_signature);
        self.last_metrics = Some(metrics_signature);
        let progress_changed =
            progress.is_some_and(|progress| progress > self.last_progress + 1e-9);
        if progress_changed {
            self.last_progress = progress.unwrap_or(self.last_progress);
            self.last_progress_at = now;
        }

        if let Some(progress) = progress {
            match self.rate_window {
                Some((sample_at, sample_progress)) => {
                    let elapsed = now.duration_since(sample_at);
                    if elapsed >= ESTIMATOR_RATE_WINDOW {
                        let delta = (progress - sample_progress).max(0.0);
                        let instantaneous = delta / elapsed.as_secs_f64();
                        if instantaneous.is_finite() && instantaneous > 0.0 {
                            self.rate_per_second = Some(match self.rate_per_second {
                                Some(previous) => previous * 0.7 + instantaneous * 0.3,
                                None => instantaneous,
                            });
                        }
                        self.rate_window = Some((now, progress));
                    }
                }
                None => self.rate_window = Some((now, progress)),
            }
        }

        let est_remaining = progress.and_then(|progress| {
            let rate = self.rate_per_second?;
            if progress >= 1.0 || !rate.is_finite() || rate <= 0.0 {
                return None;
            }
            let seconds = (1.0 - progress) / rate;
            seconds
                .is_finite()
                .then(|| Duration::try_from_secs_f64(seconds.max(0.0)).ok())
                .flatten()
        });

        BundleProgressEstimate {
            progress,
            rate_per_second: self.rate_per_second,
            est_remaining,
            mode,
            changed: metrics_changed || progress_changed,
        }
    }

    fn reset_after_split(&mut self, progress: Option<f64>, now: Instant) {
        self.rate_window = None;
        self.rate_per_second = None;
        if let Some(progress) = progress {
            self.last_progress = progress;
        }
        self.last_progress_at = now;
    }
}

fn active_work_fraction(metrics: &ProgressMetrics) -> Option<f64> {
    if metrics
        .work_remaining
        .is_some_and(|remaining| remaining <= 0.0)
    {
        return Some(1.0);
    }
    let completed = f64::from(metrics.work_completed?);
    let remaining = f64::from(metrics.work_remaining?);
    let total = completed + remaining;
    if total.is_finite() && total > 0.0 {
        let fraction = completed / total;
        fraction.is_finite().then_some(fraction.clamp(0.0, 1.0))
    } else {
        None
    }
}

fn estimate_progress(
    read_index: i64,
    active_fraction: Option<f64>,
    spans: &[SdfInputSpan],
) -> (Option<f64>, ProgressEstimatorMode) {
    if spans.is_empty() {
        return (None, ProgressEstimatorMode::None);
    }
    let active_index = read_index.max(0) as usize;
    let completed_elements = active_index.min(spans.len());
    if completed_elements == spans.len() {
        return (Some(1.0), ProgressEstimatorMode::Coarse);
    }

    if let Some(active_fraction) = active_fraction
        && spans.iter().all(|span| {
            span.size
                .is_some_and(|size| size.is_finite() && size >= 0.0)
        })
    {
        let total_size: f64 = spans.iter().filter_map(|span| span.size).sum();
        if total_size.is_finite() && total_size > 0.0 {
            let completed_size: f64 = spans[..completed_elements]
                .iter()
                .filter_map(|span| span.size)
                .sum();
            let active_size = spans[completed_elements].size.unwrap_or(0.0);
            let progress = (completed_size + active_fraction * active_size) / total_size;
            return (
                Some(progress.clamp(0.0, 1.0)),
                ProgressEstimatorMode::Weighted,
            );
        }
    }

    let progress =
        (completed_elements as f64 + active_fraction.unwrap_or(0.0)) / spans.len() as f64;
    (
        Some(progress.clamp(0.0, 1.0)),
        ProgressEstimatorMode::Coarse,
    )
}

struct ManagedBundle {
    stage: String,
    depth: usize,
    progress: Option<f64>,
    est_remaining: Option<Duration>,
    last_progress_at: Instant,
    last_split_at: Option<Instant>,
    can_split: bool,
}

#[derive(Default)]
struct SplitManagerState {
    bundles: HashMap<u64, ManagedBundle>,
    permits: HashSet<u64>,
    filled_permits: HashMap<u64, usize>,
    queue_depth: HashMap<String, usize>,
    capacity: usize,
    max_workers: usize,
    spawn_cost: Duration,
    next_bundle_id: u64,
    total_bundles: usize,
    total_splits: u64,
    stage_splits: HashMap<String, u64>,
}

pub struct SplitManager {
    config: SdfSplitConfig,
    state: Mutex<SplitManagerState>,
    pool: Option<Arc<WorkerPool>>,
}

struct SplitPermit {
    bundle_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SplitDenyReason {
    Capacity,
    QueueNotEmpty,
    BelowMinimum,
    Cooldown,
    MaxBundles,
    MaxDepth,
    Disabled,
    AlreadyPermitted,
    NotTopCandidate,
}

impl SplitDenyReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Capacity => "no-capacity",
            Self::QueueNotEmpty => "queue-not-empty",
            Self::BelowMinimum => "below-min-split",
            Self::Cooldown => "cooldown",
            Self::MaxBundles => "max-bundles",
            Self::MaxDepth => "max-depth",
            Self::Disabled => "disabled",
            Self::AlreadyPermitted => "already-permitted",
            Self::NotTopCandidate => "not-top-candidate",
        }
    }
}

enum SplitPermitDecision {
    Granted(SplitPermit),
    Denied(SplitDenyReason),
}

impl SplitManager {
    pub fn from_env(pool: Option<Arc<WorkerPool>>) -> Arc<Self> {
        Arc::new(Self {
            config: SdfSplitConfig::from_env(),
            state: Mutex::new(SplitManagerState {
                next_bundle_id: 1,
                spawn_cost: Duration::ZERO,
                ..Default::default()
            }),
            pool,
        })
    }

    fn refresh_pool_state(&self) {
        let Some(pool) = &self.pool else {
            return;
        };
        let snapshot = pool.snapshot();
        self.set_pool_state(
            snapshot.idle,
            snapshot.max_workers,
            snapshot
                .spawn_cost_ms
                .and_then(|millis| u64::try_from(millis).ok())
                .map(Duration::from_millis),
        );
    }

    pub fn set_pool_state(
        &self,
        capacity: usize,
        max_workers: usize,
        spawn_cost: Option<Duration>,
    ) {
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        state.capacity = capacity;
        state.max_workers = max_workers.max(1);
        state.spawn_cost = spawn_cost.unwrap_or(Duration::ZERO);
    }

    pub fn set_queue_depth(&self, stage: &str, depth: usize) {
        self.state
            .lock()
            .expect("split manager lock not poisoned")
            .queue_depth
            .insert(stage.to_string(), depth);
    }

    pub fn total_splits(&self) -> u64 {
        self.state
            .lock()
            .expect("split manager lock not poisoned")
            .total_splits
    }

    pub fn stage_splits(&self, stage: &str) -> u64 {
        self.state
            .lock()
            .expect("split manager lock not poisoned")
            .stage_splits
            .get(stage)
            .copied()
            .unwrap_or(0)
    }

    pub fn register_bundle(&self, stage: &str, depth: usize, now: Instant) -> u64 {
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        let bundle_id = state.next_bundle_id;
        state.next_bundle_id += 1;
        state.total_bundles += 1;
        state.bundles.insert(
            bundle_id,
            ManagedBundle {
                stage: stage.to_string(),
                depth,
                progress: None,
                est_remaining: None,
                last_progress_at: now,
                last_split_at: None,
                can_split: true,
            },
        );
        bundle_id
    }

    pub fn finish_bundle(&self, bundle_id: u64) {
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        state.bundles.remove(&bundle_id);
        if !state.filled_permits.contains_key(&bundle_id) {
            state.permits.remove(&bundle_id);
        }
    }

    fn update_progress(&self, bundle_id: u64, estimate: BundleProgressEstimate, now: Instant) {
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        if let Some(bundle) = state.bundles.get_mut(&bundle_id) {
            bundle.progress = estimate.progress;
            bundle.est_remaining = estimate.est_remaining;
            if estimate.changed {
                bundle.last_progress_at = now;
            }
        }
    }

    fn try_acquire_permit(&self, bundle_id: u64, now: Instant) -> SplitPermitDecision {
        self.refresh_pool_state();
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        let permits_available = state.capacity.saturating_sub(state.permits.len());
        let Some(bundle) = state.bundles.get(&bundle_id) else {
            return SplitPermitDecision::Denied(SplitDenyReason::Disabled);
        };
        if !bundle.can_split {
            return SplitPermitDecision::Denied(SplitDenyReason::Disabled);
        }
        if state.permits.contains(&bundle_id) {
            return SplitPermitDecision::Denied(SplitDenyReason::AlreadyPermitted);
        }
        if state.total_bundles >= state.max_workers.saturating_mul(MAX_BUNDLES_PER_WORKER) {
            return SplitPermitDecision::Denied(SplitDenyReason::MaxBundles);
        }
        if bundle.depth >= self.config.max_depth {
            return SplitPermitDecision::Denied(SplitDenyReason::MaxDepth);
        }
        if state.queue_depth.get(&bundle.stage).copied().unwrap_or(0) > 0 {
            return SplitPermitDecision::Denied(SplitDenyReason::QueueNotEmpty);
        }
        if bundle
            .last_split_at
            .is_some_and(|last| now.duration_since(last) < self.config.cooldown)
        {
            return SplitPermitDecision::Denied(SplitDenyReason::Cooldown);
        }
        if permits_available == 0 {
            return SplitPermitDecision::Denied(SplitDenyReason::Capacity);
        }

        let min_split = self.config.min_split_override.unwrap_or_else(|| {
            Duration::from_secs_f64(
                DEFAULT_MIN_SPLIT
                    .as_secs_f64()
                    .max(SPAWN_COST_MULTIPLIER as f64 * state.spawn_cost.as_secs_f64()),
            )
        });
        let mut candidates = state
            .bundles
            .iter()
            .filter_map(|(id, bundle)| {
                if !bundle.can_split
                    || state.permits.contains(id)
                    || bundle.depth >= self.config.max_depth
                    || state.queue_depth.get(&bundle.stage).copied().unwrap_or(0) > 0
                    || bundle
                        .last_split_at
                        .is_some_and(|last| now.duration_since(last) < self.config.cooldown)
                {
                    return None;
                }
                let stalled =
                    now.duration_since(bundle.last_progress_at) >= self.config.stall_after;
                let rank = bundle
                    .est_remaining
                    .filter(|remaining| *remaining >= min_split)
                    .or(stalled.then_some(Duration::MAX))?;
                Some((*id, rank))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        let Some(position) = candidates
            .iter()
            .position(|(candidate_id, _)| *candidate_id == bundle_id)
        else {
            return SplitPermitDecision::Denied(SplitDenyReason::BelowMinimum);
        };
        if position >= permits_available {
            return SplitPermitDecision::Denied(SplitDenyReason::NotTopCandidate);
        }

        state.permits.insert(bundle_id);
        SplitPermitDecision::Granted(SplitPermit { bundle_id })
    }

    fn decision_context(&self, bundle_id: u64) -> String {
        let state = self.state.lock().expect("split manager lock not poisoned");
        let Some(bundle) = state.bundles.get(&bundle_id) else {
            return "bundle=missing".to_string();
        };
        let min_split = self.config.min_split_override.unwrap_or_else(|| {
            Duration::from_secs_f64(
                DEFAULT_MIN_SPLIT
                    .as_secs_f64()
                    .max(SPAWN_COST_MULTIPLIER as f64 * state.spawn_cost.as_secs_f64()),
            )
        });
        format!(
            "capacity={} permits={} queue_depth={} depth={} total_bundles={} \
             max_bundles={} min_split_s={:.2} est_remaining_s={:?} spawn_cost_ms={}",
            state.capacity,
            state.permits.len(),
            state.queue_depth.get(&bundle.stage).copied().unwrap_or(0),
            bundle.depth,
            state.total_bundles,
            state.max_workers.saturating_mul(MAX_BUNDLES_PER_WORKER),
            min_split.as_secs_f64(),
            bundle.est_remaining.map(|value| value.as_secs_f64()),
            state.spawn_cost.as_millis()
        )
    }

    fn complete_permit(
        &self,
        permit: SplitPermit,
        created_items: usize,
        declined: bool,
        now: Instant,
    ) {
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        let stage = state
            .bundles
            .get(&permit.bundle_id)
            .map(|bundle| bundle.stage.clone());
        if let Some(bundle) = state.bundles.get_mut(&permit.bundle_id) {
            if created_items > 0 {
                bundle.last_split_at = Some(now);
                bundle.last_progress_at = now;
                bundle.progress = None;
                bundle.est_remaining = None;
            } else if declined {
                bundle.can_split = false;
            }
        }
        if let Some(stage) = stage
            && created_items > 0
        {
            state.total_splits += 1;
            *state.stage_splits.entry(stage.clone()).or_default() += 1;
            let depth = state.queue_depth.get(&stage).copied().unwrap_or(0);
            state
                .queue_depth
                .insert(stage.clone(), depth + created_items);
            state.filled_permits.insert(permit.bundle_id, created_items);
        } else {
            state.permits.remove(&permit.bundle_id);
        }
    }

    pub fn note_item_picked(&self, bundle_id: u64) {
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        let Some(remaining) = state.filled_permits.get_mut(&bundle_id) else {
            return;
        };
        *remaining = remaining.saturating_sub(1);
        if *remaining == 0 {
            state.filled_permits.remove(&bundle_id);
            state.permits.remove(&bundle_id);
        }
    }

    fn disable_bundle(&self, permit: SplitPermit) {
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        state.permits.remove(&permit.bundle_id);
        if let Some(bundle) = state.bundles.get_mut(&permit.bundle_id) {
            bundle.can_split = false;
        }
    }

    fn disable_bundle_id(&self, bundle_id: u64) {
        let mut state = self.state.lock().expect("split manager lock not poisoned");
        state.permits.remove(&bundle_id);
        if let Some(bundle) = state.bundles.get_mut(&bundle_id) {
            bundle.can_split = false;
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
    /// from prior bundles and `progress_tick` is its progress-poll interval
    /// (`None` for the first); the outcome carries them forward along with the
    /// residual work items this bundle produced and the holds of the item it
    /// consumed. The caller schedules one bundle per work item and applies the item
    /// holds as output-watermark clamps until the item completes.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_bundle(
        &mut self,
        stage: &SplittableStage,
        output_edge_metadata: Option<ConsumerMetaData>,
        seed: Option<SdfWorkItem>,
        initialized: bool,
        registered: bool,
        progress_tick: Option<Duration>,
        max_workers: usize,
        overdecompose: usize,
        split_manager: Arc<SplitManager>,
        bundle_id: u64,
        split_depth: usize,
        immediate_items: SdfWorkItemSender,
    ) -> anyhow::Result<(ProcessBundleResponse, SdfBundleOutcome)> {
        let plan = stage.plan();

        let (seed_bytes, consumed_holds) = match seed {
            Some(item) => (Some(item.seed), item.holds),
            None => (None, Vec::new()),
        };

        // The first bundle runs the initialization stage to capture its sized
        // restrictions. With more than one worker we return those restrictions
        // as independent work items so dispatch can begin immediately instead of
        // processing the entire input as one bundle.
        let (bytes, initialization_response) = if initialized {
            (seed_bytes.unwrap_or_default(), None)
        } else {
            let (response, bytes) = self
                .run_initialization_stage(&plan.initialization_stage)
                .await?;
            (bytes, Some(response))
        };

        if let Some(initialization_response) = initialization_response {
            if max_workers <= 1 {
                info!(
                    "SDF initialization partition: max_workers=1, overdecompose={overdecompose}, \
                     partitions=1, mode=single-bundle"
                );
            } else {
                let process_input = plan.process_stage.input_pcol();
                match decode_windowed_value_spans(
                    &self.runtime,
                    process_input.id(),
                    &process_input.node().coder_id,
                    &bytes,
                ) {
                    Ok(spans) if !spans.is_empty() => {
                        let target =
                            initial_partition_count(max_workers, overdecompose, spans.len());
                        if target > 1 {
                            if registered {
                                let process_descriptor_id = plan.process_stage.id().to_string();
                                let bundle_status =
                                    self.runtime.register_bundle(&plan.process_stage).await?;
                                if !matches!(bundle_status, ControlResponse::BundleRegistered) {
                                    return Err(anyhow!(
                                        "failed to register SDF process bundle {process_descriptor_id}"
                                    ));
                                }
                                info!(
                                    "SDF process bundle {} registered after initialization partition",
                                    process_descriptor_id
                                );
                            }
                            let items = partitioned_work_items(stage, &bytes, &spans, target);
                            info!(
                                "SDF initialization partition: max_workers={max_workers}, \
                             overdecompose={overdecompose}, elements={}, partitions={}, \
                             sizes={:?}",
                                spans.len(),
                                items.len(),
                                items
                                    .iter()
                                    .map(|item| item.estimated_size)
                                    .collect::<Vec<_>>()
                            );
                            return Ok((
                                initialization_response,
                                SdfBundleOutcome {
                                    initialized: true,
                                    consumed_holds,
                                    items,
                                    progress_tick: progress_tick
                                        .unwrap_or(DEFAULT_PROGRESS_POLL_INTERVAL),
                                },
                            ));
                        }
                        info!(
                            "SDF initialization partition: max_workers={max_workers}, \
                         overdecompose={overdecompose}, elements={}, partitions=1, \
                         mode=single-bundle",
                            spans.len()
                        );
                    }
                    Ok(_) => {}
                    Err(err) => warn!(
                        "failed to decode SplitAndSize output for static partitioning ({}); \
                     falling back to one process bundle",
                        err
                    ),
                }
            }
        }

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
                split_manager,
                bundle_id,
                split_depth,
                immediate_items,
            )
            .await?;

        let items = residual_items(
            stage,
            &response.residual_roots,
            split_depth.saturating_add(1),
            None,
        );

        Ok((
            response,
            SdfBundleOutcome {
                initialized: true,
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
    ) -> anyhow::Result<(ProcessBundleResponse, Vec<u8>)> {
        let started = Instant::now();
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
        self.runtime.record_instruction_id(&instruction_id);

        info!(
            "SDF initialization stage {} instruction_id={}",
            descriptor_id, instruction_id
        );
        info!(
            "bundle start: worker={} instr={} stage={} elements=unknown seed_bytes=0 \
             queue_depth=0 phase=initialization",
            self.runtime.worker_id().as_str(),
            instruction_id,
            descriptor_id
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
        // The initialization stage is `PairWithRestriction → SplitAndSize`, which
        // has exactly one output; anything else indicates incorrect fusion of an
        // unrelated transform into the SDF initialization stage.
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
            (ControlResponse::ProcessBundleSuccess(response), bytes) => {
                info!(
                    "bundle end: worker={} instr={} stage={} dur_ms={} output_elements={} \
                     paimon_write_ms={} status=ok phase=initialization",
                    self.runtime.worker_id().as_str(),
                    instruction_id,
                    descriptor_id,
                    started.elapsed().as_millis(),
                    sum_element_count(&response.monitoring_infos),
                    self.runtime
                        .last_paimon_write_ms()
                        .map_or_else(|| "na".to_string(), |value| value.to_string())
                );
                Ok((response, bytes))
            }
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
    #[allow(clippy::too_many_arguments)]
    async fn run_process_stage(
        &mut self,
        stage: &ExecutableStage,
        splittable_stage: &SplittableStage,
        output_edge_metadata: Option<ConsumerMetaData>,
        seed: Vec<u8>,
        register: bool,
        allow_split: bool,
        progress_tick: Option<Duration>,
        split_manager: Arc<SplitManager>,
        bundle_id: u64,
        split_depth: usize,
        immediate_items: SdfWorkItemSender,
    ) -> anyhow::Result<(ProcessBundleResponse, Duration)> {
        let started = Instant::now();
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
        self.runtime.record_instruction_id(&instruction_id);
        info!(
            "SDF process stage {} instruction_id={}, estimated_input_elements={}",
            descriptor_id, instruction_id, estimated_input_elements
        );
        info!(
            "bundle start: worker={} instr={} stage={} elements={} seed_bytes={} \
             queue_depth=na depth={} phase=process",
            self.runtime.worker_id().as_str(),
            instruction_id,
            descriptor_id,
            estimated_input_elements,
            seed.len(),
            split_depth
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
        info!(
            "SDF split policy: min_split_override_ms={:?}, stall_after_ms={}, cooldown_ms={}, \
             fraction={}, max_depth={}",
            split_manager
                .config
                .min_split_override
                .map(|value| value.as_millis()),
            split_manager.config.stall_after.as_millis(),
            split_manager.config.cooldown.as_millis(),
            split_manager.config.fraction_of_remainder,
            split_manager.config.max_depth
        );
        let poll_interval = progress_tick.unwrap_or(split_manager.config.progress_poll_interval);
        let mut split_runtime = BundleSplitRuntime::new(Instant::now());
        let monitor_enabled = allow_split && estimated_input_elements > 0;

        let control_response = {
            let process_future = async {
                tokio::pin!(response_future);

                let response = loop {
                    let ticker = tokio::time::sleep(poll_interval);
                    tokio::pin!(ticker);

                    tokio::select! {
                        biased;
                        bundle_response = &mut response_future => break bundle_response,
                        _ = &mut ticker, if monitor_enabled => {
                            if let Err(err) = poll_progress_and_maybe_split(
                                &control,
                                &instruction_id,
                                &source_transform_id,
                                &mut split_runtime,
                                &seed,
                                &input_runtime,
                                &input_pcol_id,
                                &input_coder_id,
                                &mut input_spans,
                                splittable_stage,
                                &mut split_residual_roots,
                                &mut channel_items,
                                split_manager.as_ref(),
                                bundle_id,
                                split_depth,
                                &immediate_items,
                            ).await {
                                // Progress error stops further splitting.
                                warn!(
                                    "SDF `progress + split_evaluation` run failed for bundle {}, \
                                    disabling further splits for this bundle: {}",
                                    instruction_id,
                                    err
                                );
                                split_manager.disable_bundle_id(bundle_id);
                            }
                        }
                    }
                }?;

                Ok::<_, anyhow::Error>(response)
            };

            if let Some(output_meta_data) = output_edge_metadata {
                let data_key = DataKey {
                    instruction_id: instruction_id.clone(),
                    transform_id: stage_sink_transform_id(
                        stage,
                        &output_meta_data.produced_pcol_id,
                    ),
                };
                let receiver = self.runtime.data_receiver(data_key);
                let output_runtime = self.runtime.clone();

                let decode_task = tokio::spawn(async move {
                    output_runtime
                        .process_output_elements(receiver, output_meta_data)
                        .await
                });

                let timeout_id = instruction_id.clone();
                crate::engine::guard::idle_guard(
                    async {
                        match process_future.await {
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
                info!(
                    "bundle end: worker={} instr={} stage={} dur_ms={} output_elements={} \
                     paimon_write_ms={} status=ok phase=process",
                    self.runtime.worker_id().as_str(),
                    instruction_id,
                    descriptor_id,
                    started.elapsed().as_millis(),
                    sum_element_count(&response.monitoring_infos),
                    self.runtime
                        .last_paimon_write_ms()
                        .map_or_else(|| "na".to_string(), |value| value.to_string())
                );
                info!(
                    "SDF bundle summary: instruction_id={instruction_id}, \
                     estimated_input_elements={estimated_input_elements}, polls={}, \
                     split_requests={}, splits={}, residual_roots={}, channel_items={}, \
                     poll_interval_ms={}",
                    split_runtime.polls,
                    split_runtime.split_requests,
                    split_runtime.splits,
                    split_residual_roots.len(),
                    channel_items.len(),
                    poll_interval.as_millis()
                );
                Ok((response, poll_interval))
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

pub(crate) fn sdf_overdecompose_from_env() -> usize {
    sdf_overdecompose_from(std::env::var("FLAREDB_SDF_OVERDECOMPOSE").ok().as_deref())
}

fn sdf_overdecompose_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SDF_OVERDECOMPOSE)
}

fn initial_partition_count(max_workers: usize, overdecompose: usize, elements: usize) -> usize {
    if max_workers <= 1 || elements <= 1 {
        return 1;
    }
    max_workers
        .saturating_mul(overdecompose.max(1))
        .clamp(1, elements)
}

fn split_and_size_size(value: &WindowedValue) -> Option<f64> {
    let BeamRecord::KV(outer) = &value.value else {
        return None;
    };
    let BeamRecord::PRIMITIVE(PrimitiveValue::Float64(size)) = outer.value.as_ref() else {
        return None;
    };
    size.is_finite()
        .then_some(*size)
        .filter(|size| *size >= 0.0)
}

/// Build independently schedulable work items from the SplitAndSize output.
///
/// Partition boundaries are contiguous element ranges. When every element has a
/// finite, non-negative SDK size, cuts minimize byte-size skew; otherwise the
/// fallback is equal element counts. Items are returned largest-first so the
/// dispatcher starts expensive partitions before small tail work.
fn partitioned_work_items(
    stage: &SplittableStage,
    bytes: &[u8],
    spans: &[SdfInputSpan],
    target_partitions: usize,
) -> Vec<SdfWorkItem> {
    let ranges = partition_span_ranges(spans, target_partitions);
    let mut items = ranges
        .into_iter()
        .filter(|range| !range.is_empty())
        .map(|range| {
            let start_byte = spans[range.start].start;
            let end_byte = spans[range.end - 1].end;
            let timestamps = spans[range.clone()]
                .iter()
                .map(|span| span.timestamp_millis)
                .collect::<Vec<_>>();
            let estimated_size = partition_size(spans, &range);
            SdfWorkItem {
                seed: bytes[start_byte..end_byte].to_vec(),
                holds: channel_item_holds(stage, &timestamps),
                estimated_size,
                split_depth: 0,
                permit_id: None,
            }
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        right
            .estimated_size
            .unwrap_or_default()
            .total_cmp(&left.estimated_size.unwrap_or_default())
            .then_with(|| right.seed.len().cmp(&left.seed.len()))
    });
    items
}

#[allow(clippy::single_range_in_vec_init)]
fn partition_span_ranges(spans: &[SdfInputSpan], target_partitions: usize) -> Vec<Range<usize>> {
    if spans.is_empty() {
        return Vec::new();
    }
    let partitions = target_partitions.clamp(1, spans.len());
    if partitions == 1 {
        return vec![0..spans.len()];
    }

    let sizes = spans
        .iter()
        .map(|span| span.size)
        .collect::<Option<Vec<_>>>()
        .filter(|sizes| sizes.iter().all(|size| size.is_finite() && *size >= 0.0));
    let Some(sizes) = sizes else {
        return equal_count_ranges(spans.len(), partitions);
    };
    let total_size: f64 = sizes.iter().sum();
    if !total_size.is_finite() || total_size <= 0.0 {
        return equal_count_ranges(spans.len(), partitions);
    }
    if sizes.windows(2).all(|pair| pair[0] == pair[1]) {
        return equal_count_ranges(spans.len(), partitions);
    }

    let mut ranges = Vec::with_capacity(partitions);
    let mut start = 0usize;
    let mut remaining_size = total_size;
    for group in 0..partitions {
        let groups_left = partitions - group;
        if groups_left == 1 {
            ranges.push(start..spans.len());
            break;
        }

        let min_end = start + 1;
        let max_end = spans.len() - (groups_left - 1);
        let target_size = remaining_size / groups_left as f64;
        let mut accumulated = 0.0;
        let mut best_end = min_end;
        let mut best_difference = f64::INFINITY;
        for end in min_end..=max_end {
            accumulated += sizes[end - 1];
            let difference = (accumulated - target_size).abs();
            if difference < best_difference {
                best_difference = difference;
                best_end = end;
            }
            if accumulated >= target_size {
                break;
            }
        }
        let group_size: f64 = sizes[start..best_end].iter().sum();
        remaining_size = (remaining_size - group_size).max(0.0);
        ranges.push(start..best_end);
        start = best_end;
    }
    ranges
}

fn equal_count_ranges(elements: usize, partitions: usize) -> Vec<Range<usize>> {
    let base = elements / partitions;
    let remainder = elements % partitions;
    let mut start = 0usize;
    (0..partitions)
        .map(|index| {
            let count = base + usize::from(index < remainder);
            let end = start + count;
            let range = start..end;
            start = end;
            range
        })
        .collect()
}

fn partition_size(spans: &[SdfInputSpan], range: &Range<usize>) -> Option<f64> {
    spans[range.clone()]
        .iter()
        .map(|span| span.size)
        .sum::<Option<f64>>()
        .filter(|size| size.is_finite() && *size >= 0.0)
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
            Ok(Ok(value)) => {
                spans.push(SdfInputSpan {
                    start,
                    end: cursor.position() as usize,
                    timestamp_millis: value.timestamp_millis,
                    size: split_and_size_size(&value),
                });
            }
            Ok(Err(err)) => {
                return Err(anyhow!("failed to decode SDF input element: {:?}", err));
            }
            Err(_) => return Err(anyhow!("panic while decoding SDF input element")),
        }
    }

    Ok(spans)
}

/// Read progress and ask the SDK to split only when the shared manager grants a
/// capacity permit.
#[allow(clippy::too_many_arguments)]
async fn poll_progress_and_maybe_split(
    control: &ControlChannel,
    bundle_instruction_id: &str,
    input_transform_id: &str,
    runtime_state: &mut BundleSplitRuntime,
    input_bytes: &[u8],
    runtime: &BundleRuntime,
    input_pcol_id: &str,
    input_coder_id: &str,
    input_spans: &mut Option<Vec<SdfInputSpan>>,
    splittable_stage: &SplittableStage,
    split_residual_roots: &mut Vec<DelayedBundleApplication>,
    channel_items: &mut Vec<SdfWorkItem>,
    split_manager: &SplitManager,
    bundle_id: u64,
    split_depth: usize,
    immediate_items: &SdfWorkItemSender,
) -> anyhow::Result<()> {
    runtime_state.polls += 1;
    let first_poll = runtime_state.polls == 1;

    let progress = control
        .send_process_bundle_progress_request(bundle_instruction_id)
        .await?;
    let metrics = extract_progress_metrics(&progress);
    let now = Instant::now();

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
        *input_spans = Some(spans);
    }
    let input_spans = input_spans.as_deref().unwrap_or_default();
    let estimated_input_elements = input_spans.len() as i64;
    let estimate = runtime_state.estimator.observe(&metrics, input_spans, now);
    split_manager.update_progress(bundle_id, estimate, now);

    let can_split = split_manager
        .state
        .lock()
        .expect("split manager lock not poisoned")
        .bundles
        .get(&bundle_id)
        .is_some_and(|bundle| bundle.can_split);
    let progress_log = format!(
        "bundle progress: instruction_id={bundle_instruction_id}, worker_bundle_id={bundle_id}, \
         read_index={}, output_element_count_sum={}, work_completed={:?}, \
         work_remaining={:?}, p={:?}, rate_per_s={:?}, est_remaining_s={:?}, \
         estimator={}, polls={}, can_split={can_split}, estimated_input_elements={}",
        metrics.read_index,
        metrics.output_element_count_sum,
        metrics.work_completed,
        metrics.work_remaining,
        estimate.progress,
        estimate.rate_per_second,
        estimate.est_remaining.map(|value| value.as_secs_f64()),
        estimate.mode.as_str(),
        runtime_state.polls,
        estimated_input_elements
    );
    let progress_log_due =
        now.duration_since(runtime_state.last_progress_log_at) >= BUNDLE_PROGRESS_LOG_INTERVAL;
    if first_poll || progress_log_due {
        runtime_state.last_progress_log_at = now;
        info!("{progress_log}");
    } else {
        debug!("{progress_log}");
    }

    if estimated_input_elements <= 0
        || metrics
            .work_remaining
            .is_some_and(|remaining| remaining <= 0.0)
    {
        return Ok(());
    }

    let decision_context = split_manager.decision_context(bundle_id);
    let permit = match split_manager.try_acquire_permit(bundle_id, now) {
        SplitPermitDecision::Granted(permit) => {
            runtime_state.last_denial = None;
            permit
        }
        SplitPermitDecision::Denied(reason) => {
            let message = format!(
                "SDF split decision: instruction_id={bundle_instruction_id}, \
                 worker_bundle_id={bundle_id}, granted=false, reason={} {decision_context}",
                reason.as_str(),
            );
            if runtime_state.last_denial != Some(reason) {
                info!("{message}");
            } else {
                debug!("{message}");
            }
            runtime_state.last_denial = Some(reason);
            return Ok(());
        }
    };
    info!(
        "SDF split decision: instruction_id={bundle_instruction_id}, \
         worker_bundle_id={bundle_id}, granted=true, p={:?}, est_remaining_s={:?}, \
         estimator={} {decision_context}",
        estimate.progress,
        estimate.est_remaining.map(|value| value.as_secs_f64()),
        estimate.mode.as_str(),
    );

    let mut desired_splits = HashMap::new();
    desired_splits.insert(
        input_transform_id.to_string(),
        DesiredSplit {
            fraction_of_remainder: split_manager.config.fraction_of_remainder,
            allowed_split_points: Vec::new(),
            estimated_input_elements,
        },
    );

    runtime_state.split_requests += 1;
    let split = match control
        .send_process_bundle_split_request(bundle_instruction_id, desired_splits)
        .await
    {
        Ok(split) => split,
        Err(error) => {
            split_manager.disable_bundle(permit);
            return Err(error);
        }
    };
    let channel_splits = split.channel_splits.len();
    let reported_roots = split.residual_roots.len();
    let residual_roots_before = split_residual_roots.len();
    let channel_items_before = channel_items.len();

    if split.channel_splits.is_empty() {
        // SDK cannot split (nothing left, or unsupported). Note that any
        // `residual_roots` in this response are not scheduled.
        split_manager.complete_permit(permit, 0, true, now);
        info!(
            "SDF split response: instruction_id={bundle_instruction_id}, \
             residual_roots={reported_roots}, channel_splits=0, action=disable-splitting, \
             split_requests={}, splits={}",
            runtime_state.split_requests, runtime_state.splits
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
    for item in residual_items(
        splittable_stage,
        &split.residual_roots,
        split_depth.saturating_add(1),
        Some(bundle_id),
    ) {
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
        if let Some(previous) = runtime_state.handed_back_from
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
            first, runtime_state.handed_back_from, input_element_count
        );

        if let Some((start, end, count)) =
            channel_split_range(first, runtime_state.handed_back_from, input_spans)
        {
            let timestamps =
                channel_item_timestamps(input_spans, runtime_state.handed_back_from, first);
            let first_span = first.max(0) as usize;
            let last_span = runtime_state
                .handed_back_from
                .map(|value| value as usize)
                .unwrap_or(input_spans.len());
            let item = SdfWorkItem {
                seed: input_bytes[start..end].to_vec(),
                holds: channel_item_holds(splittable_stage, &timestamps),
                estimated_size: partition_size(input_spans, &(first_span..last_span)),
                split_depth: split_depth.saturating_add(1),
                permit_id: Some(bundle_id),
            };
            immediate_items
                .send((splittable_stage.id().to_string(), item.clone()))
                .map_err(|_| anyhow!("SDF residual dispatcher closed"))?;
            channel_items.push(item);
            runtime_state.handed_back_from = Some(first);
            debug_assert!(count > 0);
        }
        // Otherwise `first == input_element_count` (no whole elements) or
        // `first == handed_back_from` (nothing newly relinquished): no item.
    }

    let new_residual_roots = split_residual_roots.len() - residual_roots_before;
    let new_channel_items = channel_items.len() - channel_items_before;
    let new_work_items = new_residual_roots + new_channel_items;
    split_manager.complete_permit(permit, new_work_items, new_work_items == 0, now);
    if new_work_items > 0 {
        runtime_state.splits += 1;
        runtime_state
            .estimator
            .reset_after_split(estimate.progress, now);
    }
    info!(
        "SDF split response: instruction_id={bundle_instruction_id}, \
         residual_roots={new_residual_roots}, channel_splits={channel_splits}, \
         channel_items={new_channel_items}, new_work_items={}, \
         split_requests={}, splits={}",
        new_work_items, runtime_state.split_requests, runtime_state.splits
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
    split_depth: usize,
    permit_id: Option<u64>,
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
                estimated_size: None,
                split_depth,
                permit_id,
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

fn positive_usize_from_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
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
    use crate::store::record::BeamKV;

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
        let items = residual_items(&stage, &[residual("process", "out", 5_000)], 1, None);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].holds, vec![5_000]);

        // An internal output is not tracked by the runner, so the residual
        // reports no usable watermark for the boundary output: the Fn API's
        // MIN_TIMESTAMP default applies (it may still produce anything).
        let items = residual_items(&stage, &[residual("process", "internal", 5_000)], 1, None);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].holds, vec![MIN_TIMESTAMP]);

        // The same default covers a local output name not declared on the
        // transform: the boundary output was not reported.
        assert_eq!(
            residual_items(&stage, &[residual("process", "missing", 5_000)], 1, None)[0].holds,
            vec![MIN_TIMESTAMP]
        );

        // A transform that is not part of the process stage cannot be mapped to
        // an output at all; the existing warning path imposes no hold.
        assert!(
            residual_items(&stage, &[residual("elsewhere", "out", 5_000)], 1, None)[0]
                .holds
                .is_empty()
        );

        // No residuals: no items.
        assert!(residual_items(&stage, &[], 1, None).is_empty());
    }

    #[test]
    fn residual_items_hold_explicit_min_timestamp_reports() {
        let stage = splittable_stage(&["pcol-out"], "process", &[("out", "pcol-out")]);

        // A tracker that reports "no progress" as MIN_TIMESTAMP is held at MIN,
        // not treated as "no hold".
        let items = residual_items(
            &stage,
            &[residual("process", "out", MIN_TIMESTAMP)],
            1,
            None,
        );
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
        let items = residual_items(&stage, &[application.clone()], 1, None);
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
        let mut holds = residual_items(&stage, &[application], 1, None)[0]
            .holds
            .clone();
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

        let items = residual_items(&stage, &[first, empty, second], 2, None);
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

        let items = residual_items(&stage, &[empty_element, valid], 1, None);
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
                size: Some(1.0),
            },
            SdfInputSpan {
                start: 2,
                end: 4,
                timestamp_millis: 20,
                size: Some(1.0),
            },
            SdfInputSpan {
                start: 4,
                end: 6,
                timestamp_millis: 30,
                size: Some(1.0),
            },
            SdfInputSpan {
                start: 6,
                end: 8,
                timestamp_millis: 40,
                size: Some(1.0),
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
                size: Some(1.0),
            })
            .collect()
    }

    fn spans_with_sizes(sizes: &[Option<f64>]) -> Vec<SdfInputSpan> {
        sizes
            .iter()
            .enumerate()
            .map(|(index, size)| SdfInputSpan {
                start: 2 * index,
                end: 2 * index + 2,
                timestamp_millis: index as i64 * 1_000,
                size: *size,
            })
            .collect()
    }

    #[test]
    fn split_and_size_size_reads_the_outer_kv_value() {
        let value = WindowedValue::global(BeamRecord::KV(BeamKV {
            key: PrimitiveValue::Void,
            value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Float64(42.5))),
        }));
        assert_eq!(split_and_size_size(&value), Some(42.5));

        let invalid = WindowedValue::global(BeamRecord::KV(BeamKV {
            key: PrimitiveValue::Void,
            value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Float64(f64::NAN))),
        }));
        assert_eq!(split_and_size_size(&invalid), None);
        assert_eq!(
            split_and_size_size(&WindowedValue::global(BeamRecord::PRIMITIVE(
                PrimitiveValue::Float64(1.0)
            ))),
            None
        );
    }

    #[test]
    fn initial_partition_count_keeps_one_worker_single_bundle() {
        assert_eq!(initial_partition_count(1, 4, 100), 1);
        assert_eq!(initial_partition_count(4, 4, 100), 16);
        assert_eq!(initial_partition_count(4, 4, 6), 6);
        assert_eq!(initial_partition_count(4, 0, 6), 4);
    }

    #[test]
    fn overdecompose_uses_default_for_missing_or_invalid_values() {
        assert_eq!(sdf_overdecompose_from(None), DEFAULT_SDF_OVERDECOMPOSE);
        assert_eq!(sdf_overdecompose_from(Some(" 2 ")), 2);
        assert_eq!(sdf_overdecompose_from(Some("0")), DEFAULT_SDF_OVERDECOMPOSE);
        assert_eq!(
            sdf_overdecompose_from(Some("invalid")),
            DEFAULT_SDF_OVERDECOMPOSE
        );
    }

    #[test]
    fn partition_ranges_balance_equal_sizes() {
        let spans = spans_with_sizes(&[Some(1.0); 8]);
        assert_eq!(
            partition_span_ranges(&spans, 4),
            vec![0..2, 2..4, 4..6, 6..8]
        );
    }

    #[test]
    fn partition_ranges_keep_contiguous_balanced_byte_groups() {
        let spans = spans_with_sizes(&[
            Some(10.0),
            Some(1.0),
            Some(1.0),
            Some(1.0),
            Some(1.0),
            Some(1.0),
        ]);
        assert_eq!(partition_span_ranges(&spans, 2), vec![0..1, 1..6]);

        let ranges = partition_span_ranges(&spans_with_sizes(&[Some(1.0); 7]), 3);
        assert_eq!(ranges, vec![0..3, 3..5, 5..7]);
    }

    #[test]
    fn partition_ranges_fall_back_to_equal_counts_without_sizes() {
        let spans =
            spans_with_sizes(&[Some(10.0), None, Some(1.0), Some(1.0), Some(1.0), Some(1.0)]);
        assert_eq!(partition_span_ranges(&spans, 3), vec![0..2, 2..4, 4..6]);
    }

    #[test]
    fn partition_ranges_handle_fewer_elements_than_requested() {
        let spans = spans_with_sizes(&[Some(1.0), Some(2.0)]);
        assert_eq!(partition_span_ranges(&spans, 8), vec![0..1, 1..2]);
        assert_eq!(partition_span_ranges(&spans, 1), vec![0..2]);
    }

    #[test]
    fn partitioned_work_items_preserve_bytes_and_dispatch_largest_first() {
        let stage = splittable_stage(&["pcol-out"], "process", &[("out", "pcol-out")]);
        let bytes = b"aabbcc";
        let spans = spans_with_sizes(&[Some(5.0), Some(1.0), Some(5.0)]);

        let items = partitioned_work_items(&stage, bytes, &spans, 2);

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].seed, b"bbcc");
        assert_eq!(items[0].estimated_size, Some(6.0));
        assert_eq!(items[0].holds, vec![1_000]);
        assert_eq!(items[1].seed, b"aa");
        assert_eq!(items[1].estimated_size, Some(5.0));
        assert_eq!(items[1].holds, vec![0]);
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
            progress_poll_interval: DEFAULT_PROGRESS_POLL_INTERVAL,
            stall_after: Duration::from_secs(5),
            cooldown: Duration::from_secs(2),
            min_split_override: Some(Duration::from_secs(10)),
            fraction_of_remainder: 0.5,
            max_depth: 4,
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
    fn weighted_estimator_uses_completed_elements_and_active_work_fraction() {
        let now = Instant::now();
        let spans = spans_with_sizes(&[Some(10.0), Some(30.0), Some(60.0)]);
        let mut estimator = ProgressEstimator::new(now);
        let estimate = estimator.observe(
            &ProgressMetrics {
                read_index: 1,
                work_completed: Some(0.5),
                work_remaining: Some(0.5),
                ..Default::default()
            },
            &spans,
            now,
        );
        assert_eq!(estimate.mode, ProgressEstimatorMode::Weighted);
        assert_eq!(estimate.progress, Some(0.25));
    }

    #[test]
    fn estimator_falls_back_to_counts_without_sizes_or_finite_work_metrics() {
        let now = Instant::now();
        let mut estimator = ProgressEstimator::new(now);
        let estimate = estimator.observe(
            &ProgressMetrics {
                read_index: 1,
                work_completed: Some(0.5),
                work_remaining: Some(0.5),
                ..Default::default()
            },
            &spans_with_sizes(&[None, None, None]),
            now,
        );
        assert_eq!(estimate.mode, ProgressEstimatorMode::Coarse);
        assert_eq!(estimate.progress, Some(0.5));

        let estimate = estimator.observe(
            &ProgressMetrics {
                read_index: 1,
                work_completed: Some(f32::NAN),
                work_remaining: Some(0.5),
                ..Default::default()
            },
            &spans_with_sizes(&[Some(1.0), Some(1.0), Some(1.0)]),
            now,
        );
        assert_eq!(estimate.mode, ProgressEstimatorMode::Coarse);
        assert_eq!(estimate.progress, Some(1.0 / 3.0));
    }

    #[test]
    fn estimator_rate_requires_a_window_and_resets_after_split() {
        let start = Instant::now();
        let spans = spans_with_sizes(&[Some(1.0); 10]);
        let mut estimator = ProgressEstimator::new(start);
        let first = estimator.observe(
            &ProgressMetrics {
                read_index: 0,
                work_completed: Some(0.0),
                work_remaining: Some(1.0),
                ..Default::default()
            },
            &spans,
            start,
        );
        assert_eq!(first.est_remaining, None);

        let second_at = start + Duration::from_secs(1);
        let second = estimator.observe(
            &ProgressMetrics {
                read_index: 2,
                work_completed: Some(0.0),
                work_remaining: Some(1.0),
                ..Default::default()
            },
            &spans,
            second_at,
        );
        assert_eq!(second.rate_per_second, Some(0.2));
        assert_eq!(second.est_remaining, Some(Duration::from_secs(4)));

        estimator.reset_after_split(second.progress, second_at);
        let after_reset = estimator.observe(
            &ProgressMetrics {
                read_index: 3,
                work_completed: Some(0.0),
                work_remaining: Some(1.0),
                ..Default::default()
            },
            &spans,
            second_at,
        );
        assert_eq!(after_reset.rate_per_second, None);
        assert_eq!(after_reset.est_remaining, None);
    }

    #[test]
    fn estimator_treats_counter_changes_as_activity_without_changing_progress() {
        let now = Instant::now();
        let spans = spans_with_sizes(&[Some(1.0); 10]);
        let mut estimator = ProgressEstimator::new(now);
        let first = estimator.observe(
            &ProgressMetrics {
                read_index: 0,
                output_element_count_sum: 0,
                work_completed: Some(0.0),
                work_remaining: Some(1.0),
            },
            &spans,
            now,
        );
        assert!(first.changed);
        let second = estimator.observe(
            &ProgressMetrics {
                read_index: 0,
                output_element_count_sum: 1_000,
                work_completed: Some(0.0),
                work_remaining: Some(1.0),
            },
            &spans,
            now + Duration::from_secs(1),
        );
        assert_eq!(second.progress, Some(0.0));
        assert!(second.changed);
    }

    #[test]
    fn estimator_marks_completed_active_element_work() {
        let now = Instant::now();
        let mut estimator = ProgressEstimator::new(now);
        let estimate = estimator.observe(
            &ProgressMetrics {
                read_index: 0,
                work_completed: Some(1.0),
                work_remaining: Some(0.0),
                ..Default::default()
            },
            &spans_with_sizes(&[Some(100.0), Some(100.0)]),
            now,
        );
        assert_eq!(estimate.progress, Some(0.5));
        assert_eq!(estimate.est_remaining, None);
    }

    fn split_manager() -> SplitManager {
        SplitManager {
            config: split_config(),
            state: Mutex::new(SplitManagerState {
                next_bundle_id: 1,
                max_workers: 2,
                spawn_cost: Duration::from_secs(1),
                ..Default::default()
            }),
            pool: None,
        }
    }

    fn update_managed_remaining(
        manager: &SplitManager,
        bundle_id: u64,
        remaining: Duration,
        changed: bool,
        now: Instant,
    ) {
        manager.update_progress(
            bundle_id,
            BundleProgressEstimate {
                progress: Some(0.25),
                rate_per_second: Some(0.1),
                est_remaining: Some(remaining),
                mode: ProgressEstimatorMode::Weighted,
                changed,
            },
            now,
        );
    }

    fn assert_denied(decision: SplitPermitDecision, expected: SplitDenyReason) {
        match decision {
            SplitPermitDecision::Denied(reason) => assert_eq!(reason, expected),
            SplitPermitDecision::Granted(_) => panic!("expected split permit denial"),
        }
    }

    #[test]
    fn split_manager_grants_one_permit_to_the_largest_remaining_bundle() {
        let now = Instant::now();
        let manager = split_manager();
        manager.set_pool_state(1, 2, Some(Duration::from_secs(1)));
        let small = manager.register_bundle("stage", 0, now);
        let large = manager.register_bundle("stage", 0, now);
        update_managed_remaining(&manager, small, Duration::from_secs(20), true, now);
        update_managed_remaining(&manager, large, Duration::from_secs(30), true, now);

        assert_denied(
            manager.try_acquire_permit(small, now),
            SplitDenyReason::NotTopCandidate,
        );
        assert!(matches!(
            manager.try_acquire_permit(large, now),
            SplitPermitDecision::Granted(_)
        ));
        assert_denied(
            manager.try_acquire_permit(small, now),
            SplitDenyReason::Capacity,
        );
    }

    #[test]
    fn split_manager_denies_without_capacity_or_with_queued_work() {
        let now = Instant::now();
        let manager = split_manager();
        let bundle = manager.register_bundle("stage", 0, now);
        update_managed_remaining(&manager, bundle, Duration::from_secs(30), true, now);

        manager.set_pool_state(0, 2, Some(Duration::from_secs(1)));
        assert_denied(
            manager.try_acquire_permit(bundle, now),
            SplitDenyReason::Capacity,
        );

        manager.set_pool_state(1, 2, Some(Duration::from_secs(1)));
        manager.set_queue_depth("stage", 1);
        assert_denied(
            manager.try_acquire_permit(bundle, now),
            SplitDenyReason::QueueNotEmpty,
        );
    }

    #[test]
    fn split_manager_enforces_minimum_cooldown_depth_and_bundle_limit() {
        let now = Instant::now();
        let manager = split_manager();
        manager.set_pool_state(1, 2, Some(Duration::from_secs(1)));
        let bundle = manager.register_bundle("stage", 0, now);
        update_managed_remaining(&manager, bundle, Duration::from_secs(5), true, now);
        assert_denied(
            manager.try_acquire_permit(bundle, now),
            SplitDenyReason::BelowMinimum,
        );

        update_managed_remaining(&manager, bundle, Duration::from_secs(30), true, now);
        let permit = match manager.try_acquire_permit(bundle, now) {
            SplitPermitDecision::Granted(permit) => permit,
            SplitPermitDecision::Denied(reason) => panic!("unexpected denial: {reason:?}"),
        };
        manager.complete_permit(permit, 1, false, now);
        manager.note_item_picked(bundle);
        manager.set_queue_depth("stage", 0);
        assert_denied(
            manager.try_acquire_permit(bundle, now + Duration::from_secs(1)),
            SplitDenyReason::Cooldown,
        );

        let deep = manager.register_bundle("stage", 4, now);
        update_managed_remaining(&manager, deep, Duration::from_secs(30), true, now);
        assert_denied(
            manager.try_acquire_permit(deep, now + Duration::from_secs(3)),
            SplitDenyReason::MaxDepth,
        );

        {
            let mut state = manager.state.lock().expect("split manager lock");
            state.total_bundles = MAX_BUNDLES_PER_WORKER * state.max_workers;
        }
        assert_denied(
            manager.try_acquire_permit(bundle, now + Duration::from_secs(3)),
            SplitDenyReason::MaxBundles,
        );
    }

    #[test]
    fn split_manager_allows_the_stall_fallback_only_after_quiet_window() {
        let now = Instant::now();
        let manager = split_manager();
        manager.set_pool_state(1, 2, Some(Duration::from_secs(1)));
        let bundle = manager.register_bundle("stage", 0, now);
        assert_denied(
            manager.try_acquire_permit(bundle, now + Duration::from_secs(1)),
            SplitDenyReason::BelowMinimum,
        );
        assert!(matches!(
            manager.try_acquire_permit(bundle, now + Duration::from_secs(6)),
            SplitPermitDecision::Granted(_)
        ));
    }

    #[test]
    fn split_manager_releases_permits_when_a_split_produces_nothing() {
        let now = Instant::now();
        let manager = split_manager();
        manager.set_pool_state(1, 2, Some(Duration::from_secs(1)));
        let bundle = manager.register_bundle("stage", 0, now);
        update_managed_remaining(&manager, bundle, Duration::from_secs(30), true, now);
        let permit = match manager.try_acquire_permit(bundle, now) {
            SplitPermitDecision::Granted(permit) => permit,
            SplitPermitDecision::Denied(reason) => panic!("unexpected denial: {reason:?}"),
        };
        manager.complete_permit(permit, 0, false, now);
        assert!(matches!(
            manager.try_acquire_permit(bundle, now),
            SplitPermitDecision::Granted(_)
        ));
    }

    #[test]
    fn filled_permit_reserves_capacity_until_its_residual_is_picked_up() {
        let now = Instant::now();
        let manager = split_manager();
        manager.set_pool_state(1, 2, Some(Duration::from_secs(1)));
        let first = manager.register_bundle("stage-a", 0, now);
        let second = manager.register_bundle("stage-b", 0, now);
        update_managed_remaining(&manager, first, Duration::from_secs(30), true, now);
        update_managed_remaining(&manager, second, Duration::from_secs(30), true, now);

        let permit = match manager.try_acquire_permit(first, now) {
            SplitPermitDecision::Granted(permit) => permit,
            SplitPermitDecision::Denied(reason) => panic!("unexpected denial: {reason:?}"),
        };
        manager.complete_permit(permit, 1, false, now);
        assert_eq!(manager.total_splits(), 1);
        assert_eq!(manager.stage_splits("stage-a"), 1);
        assert_denied(
            manager.try_acquire_permit(second, now),
            SplitDenyReason::Capacity,
        );

        manager.note_item_picked(first);
        manager.set_queue_depth("stage-a", 0);
        assert!(matches!(
            manager.try_acquire_permit(second, now),
            SplitPermitDecision::Granted(_)
        ));
    }

    #[test]
    fn split_manager_default_minimum_scales_with_spawn_cost() {
        let now = Instant::now();
        let mut manager = split_manager();
        manager.config.min_split_override = None;
        manager.set_pool_state(1, 2, Some(Duration::from_secs(5)));
        let bundle = manager.register_bundle("stage", 0, now);
        update_managed_remaining(&manager, bundle, Duration::from_secs(14), true, now);
        assert_denied(
            manager.try_acquire_permit(bundle, now),
            SplitDenyReason::BelowMinimum,
        );
        update_managed_remaining(&manager, bundle, Duration::from_secs(15), true, now);
        assert!(matches!(
            manager.try_acquire_permit(bundle, now),
            SplitPermitDecision::Granted(_)
        ));
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
