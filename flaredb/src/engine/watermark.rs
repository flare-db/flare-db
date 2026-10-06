//! In-memory watermark propagation and stage eligibility over an
//! [`ExecutableGraph`].
//!
//! This is the single owner of per-stage state: watermarks, holds, and the
//! pending/in-flight state that decides when a stage may run a bundle.
//! There is deliberately no second copy of this state elsewhere (for example in
//! [`crate::engine::scheduler`]). It owns no I/O, no transform execution and no
//! proto types; runner sources report into it via `report_source_*`, and the
//! dispatcher drives scheduling from it.
//!
//! # Model
//!
//! A *stage* is an executable node. It has main inputs and, for SDK stages, side
//! inputs — both PCollections — plus one or more output PCollections. Each
//! PCollection is produced by exactly one stage, so a PCollection's watermark is
//! its producer's output watermark.
//!
//! For a non-source stage:
//!
//! ```text
//! input  = MIN(upstream output watermarks of main inputs,
//!              upstream output watermarks of side inputs)
//! output = MIN(upstream output watermarks of MAIN inputs,
//!              min timestamp of unconsumed input,
//!              earliest watermark hold)
//! ```
//!
//! Side inputs gate the *input* watermark — they hold back execution — but they
//! do not advance the *output* watermark; only main inputs do. This matches how
//! portable runners treat side inputs: they hold back a stage's input watermark
//! without advancing its output watermark.
//!
//! A *source* (a stage with no main inputs) reports its own watermark. Sources
//! may have partitions; idle partitions are excluded from the minimum, and a
//! source whose partitions are all idle is treated as advanced to
//! [`MAX_TIMESTAMP`]. Reporting a source finished pins its output to
//! [`MAX_TIMESTAMP`], which is how the end of a bounded input propagates.
//!
//! All watermarks are monotonic and start at [`MIN_TIMESTAMP`].
//!
//! # Watermark holds
//!
//! A *watermark hold* is a promise not to let a stage's **output** watermark
//! advance past a timestamp, because something still owes work at that time.
//! Concretely: a stage with a pending **event-time timer** must not look
//! "complete" to its consumers until that timer fires, so its output watermark is
//! held at the timer's `hold_timestamp` (the Beam timer field of the same name).
//! Holds are a multiset per stage — the same timestamp can be held more than once
//! and must be released as many times — and `compute_output` takes the earliest
//! one: `output = MIN(MAIN upstream outputs, earliest hold)`. They clamp output
//! only; readiness uses the unclamped input, so a hold never blocks the holding
//! stage itself.
//!
//! A stage can also hold its output watermark for **deferred work** — work the
//! SDK has split off and will run later, such as an SDF residual. Those holds live
//! in a separate multiset ([`WatermarkManager::set_residual_holds`]) because the
//! event-time timer holds are rebuilt from the durable timer store on every tick
//! and would otherwise erase them. `compute_output` takes the minimum over both
//! multisets.
//!
//! # Eligibility
//!
//! A stage is ready for a bundle when it has no main input left unproduced, no
//! bundle in flight, and its [`StageKind`] gate is satisfied. Completing a bundle
//! marks the stage done *and* push-wakes every consumer by marking each output
//! PCollection pending for it; a consumer that already ran is thereby re-armed
//! when its upstream appends more output (the streams-tables table-append signal).
//! See [`WatermarkManager::ready_stages`], [`WatermarkManager::start_bundle`] and
//! [`WatermarkManager::complete_bundle`].
//!
//! # Refresh
//!
//! [`WatermarkManager::refresh`] advances the graph to a fixpoint and returns the
//! stages whose input or output watermark moved, in deterministic order. The
//! dispatcher calls it after any watermark or hold change and then re-evaluates
//! readiness from the updated state (a watermark advance can make a gated stage
//! ready again).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{Result, anyhow, bail};
use beam_model_rs::v1::Components;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::coders::primitives::{
    BEAM_MAX_TIMESTAMP_MILLIS, BEAM_MIN_TIMESTAMP_MILLIS, GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS,
};
use crate::fusion::pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode};
use crate::jobservice::urns::beam_urns;

/// A watermark timestamp in milliseconds since the Unix epoch.
pub type Timestamp = i64;

/// The watermark below which nothing is known; the initial value of every stage.
pub const MIN_TIMESTAMP: Timestamp = BEAM_MIN_TIMESTAMP_MILLIS;

/// Beam's maximum timestamp, used as "+∞" for a finished bounded source.
pub const MAX_TIMESTAMP: Timestamp = BEAM_MAX_TIMESTAMP_MILLIS;

/// Render a watermark for logs: the sentinels become `-inf`/`+inf`, everything
/// else is millis since the Unix epoch.
pub fn format_timestamp(timestamp: Timestamp) -> String {
    if timestamp <= MIN_TIMESTAMP {
        "-inf".to_string()
    } else if timestamp >= MAX_TIMESTAMP {
        "+inf".to_string()
    } else {
        timestamp.to_string()
    }
}

/// Partition key used by [`WatermarkManager::report_source_watermark`] for
/// sources that do not report per-partition watermarks.
pub const DEFAULT_PARTITION: &str = "";

/// Identifies a stage (an executable node). Equivalent to [`ExecutableNode::id`].
pub type StageId = String;

/// Identifies a PCollection.
pub type PCollectionId = String;

/// Per-partition watermark state reported by a source.
#[derive(Debug, Clone, Default)]
struct PartitionWatermark {
    watermark: Timestamp,
    idle: bool,
}

/// External watermark reports for a source stage.
#[derive(Debug, Clone)]
struct SourceState {
    partitions: BTreeMap<String, PartitionWatermark>,
    finished: bool,
    /// Whether completing one bundle finishes this source.
    ///
    /// A bounded source (`Impulse`) emits everything in one bundle, so completion
    /// is its end. A runner source that drives an event stream (`TestStream`) is
    /// set to `false` and reports completion itself, so it may run again for the
    /// next scripted event.
    auto_finish: bool,
}

impl Default for SourceState {
    fn default() -> Self {
        Self {
            partitions: BTreeMap::new(),
            finished: false,
            auto_finish: true,
        }
    }
}

impl SourceState {
    /// The source's output watermark.
    ///
    /// `MAX_TIMESTAMP` once finished or once every partition is idle (an idle
    /// source must not hold downstream watermarks back); otherwise the min over
    /// non-idle partitions. `MIN_TIMESTAMP` while no partition has reported, so
    /// a source that has never spoken holds everything back.
    ///
    /// Note the simplification: once the output reaches `MAX_TIMESTAMP` because
    /// every partition was idle, it stays there — a later active report is
    /// clamped by the manager's monotonicity rule.
    fn output(&self) -> Timestamp {
        if self.finished {
            return MAX_TIMESTAMP;
        }
        if self.partitions.is_empty() {
            return MIN_TIMESTAMP;
        }
        let mut watermark = MAX_TIMESTAMP;
        for partition in self.partitions.values() {
            if partition.idle {
                continue;
            }
            watermark = watermark.min(partition.watermark);
        }
        watermark
    }
}

/// How a stage becomes eligible for a bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StageKind {
    /// Ready as soon as every main input has been produced. This is the bounded
    /// data-driven case; it does not consult the watermark.
    #[default]
    Ordinary,
    /// Additionally requires the input watermark to reach
    /// [`required_watermark`](WatermarkManager::required_watermark). This is
    /// aggregate/stateful readiness: such a stage must not run until time has
    /// moved past what its work needs. A finished bounded source's output is
    /// [`MAX_TIMESTAMP`], so a bounded aggregation still runs exactly when its
    /// upstream completes.
    WatermarkGated,
}

/// Per-stage watermark, hold and pending/in-flight state.
#[derive(Debug, Clone)]
struct StageState {
    main_inputs: Vec<PCollectionId>,
    side_inputs: Vec<PCollectionId>,
    outputs: Vec<PCollectionId>,
    /// Present iff the stage has no main inputs, i.e. it is a source.
    source: Option<SourceState>,
    input: Timestamp,
    output: Timestamp,
    /// Watermark holds from pending event-time timers: a multiset keyed by hold
    /// timestamp (counted, so the same timestamp can be held more than once). See
    /// the module docs on holds. Rebuilt as a whole by [`set_event_time_holds`]
    /// (WatermarkManager::set_event_time_holds) on every scheduling tick.
    watermark_holds: BTreeMap<Timestamp, usize>,
    /// Watermark holds for deferred work (SDF residual output watermarks), kept
    /// separate from [`watermark_holds`] because the event-time holds are rebuilt
    /// as a whole each tick and would clobber this multiset.
    residual_holds: BTreeMap<Timestamp, usize>,

    /// Main inputs not yet produced by their producer *for the first time*; empty
    /// means every input is available. This is the fan-in barrier and never
    /// regresses once an input has been produced.
    unproduced: BTreeSet<PCollectionId>,
    /// Main inputs whose producer has appended output that this stage has not yet
    /// consumed. Populated on every producer bundle completion (push-wake) and
    /// drained when the stage starts a bundle. This is what re-arms a *completed*
    /// consumer when an upstream stage produces more output — the streams-tables
    /// "the table was appended to, so its stream has more" signal.
    pending: BTreeSet<PCollectionId>,
    /// Minimum event-time among the pending inputs; [`MAX_TIMESTAMP`] when nothing
    /// is pending. Set on push-wake from the producer's committed minimum and
    /// cleared when a bundle consumes the pending inputs. It clamps two things: the
    /// [`effective_input_watermark`](WatermarkManager::effective_input_watermark)
    /// (decisions that must not run ahead of unconsumed input) and the stage's
    /// **output** watermark — a stage that has not consumed input at `t` cannot have
    /// produced output past `t`. The readiness gate deliberately uses the
    /// *unclamped* `input` (the triggering watermark).
    pending_min: Timestamp,
    /// Number of bundles for this stage currently executing. Zero when idle. Up
    /// to [`max_in_flight`](Self::max_in_flight) may run concurrently.
    in_flight: usize,
    /// Bundle work items the executor has declared but not started yet (for
    /// example SDF residuals). The stage is not complete while this is non-zero;
    /// each [`start_bundle`](WatermarkManager::start_bundle) consumes one.
    queued: usize,
    /// Ceiling on concurrent bundles for this stage. Runner-native and ordinary
    /// SDK stages keep `1`; a splittable stage may raise it to run residual work
    /// in parallel (see the multi-bundle execution docs).
    max_in_flight: usize,
    /// The stage has completed at least one bundle.
    completed: bool,
    /// The stage has been re-armed to run another bundle (for example by a
    /// fired timer). Only meaningful once `completed`.
    rerun_pending: bool,
    kind: StageKind,
    /// Input watermark the stage's work needs before it may run; only consulted
    /// for [`StageKind::WatermarkGated`]. [`MIN_TIMESTAMP`] means no gate.
    required_watermark: Timestamp,
    /// The stage's input watermark when it last started a bundle. A completed
    /// [`StageKind::WatermarkGated`] stage runs again once its input watermark
    /// advances past this (so newly-ready windows can emit).
    watermark_at_last_run: Timestamp,
}

impl StageState {
    fn is_source(&self) -> bool {
        self.source.is_some()
    }

    /// The earliest watermark hold across event-time and deferred-work holds, or
    /// `None` when none are held.
    fn min_hold(&self) -> Option<Timestamp> {
        match (
            self.watermark_holds.keys().next(),
            self.residual_holds.keys().next(),
        ) {
            (Some(timer), Some(residual)) => Some(*timer.min(residual)),
            (Some(timer), None) => Some(*timer),
            (None, Some(residual)) => Some(*residual),
            (None, None) => None,
        }
    }

    /// Whether this stage may run a bundle now, given its current input
    /// watermark.
    fn is_ready(&self) -> bool {
        if self.in_flight >= self.max_in_flight || !self.unproduced.is_empty() {
            return false;
        }
        // A stage that has never run starts once its inputs are produced; a
        // stage that has already run runs again when it is re-armed (a fired
        // timer), has declared bundle work (queued, e.g. SDF residuals), has
        // unconsumed upstream output (push-wake), or — for a watermark-gated
        // stage — when its input watermark has advanced past the watermark at its
        // last run.
        let has_work = if self.completed {
            self.rerun_pending
                || self.queued > 0
                || !self.pending.is_empty()
                || (self.kind == StageKind::WatermarkGated
                    && self.input > self.watermark_at_last_run)
        } else {
            true
        };
        if !has_work {
            return false;
        }
        match self.kind {
            StageKind::Ordinary => true,
            StageKind::WatermarkGated => self.input >= self.required_watermark,
        }
    }
}

/// Graph-level watermark propagation state.
///
/// Build one with [`from_executable_graph`](Self::from_executable_graph), or
/// incrementally with [`add_stage`](Self::add_stage). Report watermarks with the
/// `report_*` methods, then call [`refresh`](Self::refresh).
#[derive(Debug, Default, Clone)]
pub struct WatermarkManager {
    stages: HashMap<StageId, StageState>,
    /// PCollection id to the id of the single stage that produces it.
    producers: HashMap<PCollectionId, StageId>,
    /// PCollection id to the stages that consume it as a main input.
    consumers: HashMap<PCollectionId, Vec<StageId>>,
}

impl WatermarkManager {
    /// An empty manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build the stage graph from an [`ExecutableGraph`].
    ///
    /// Main inputs come from incoming graph edges, side inputs from the stage's
    /// side-input refs, and outputs from the node's output PCollections. Roots
    /// (nodes with no incoming edges) become sources.
    pub fn from_executable_graph(executable: &ExecutableGraph) -> Self {
        let graph = executable.get_executable_graph();
        let mut manager = Self::new();

        for index in graph.node_indices() {
            let node = &graph[index];
            let mut main_inputs: Vec<PCollectionId> = graph
                .edges_directed(index, Direction::Incoming)
                .map(|edge| edge.weight().produced_pcol_id.clone())
                .collect();
            main_inputs.sort();
            main_inputs.dedup();

            let mut side_inputs: Vec<PCollectionId> = match node {
                ExecutableNode::Worker(stage) => stage
                    .side_inputs()
                    .iter()
                    .map(|side| side.collection().id.clone())
                    .collect(),
                _ => Vec::new(),
            };
            side_inputs.sort();
            side_inputs.dedup();

            let mut outputs: Vec<PCollectionId> = node.output_pcols().into_iter().collect();
            outputs.extend(
                graph
                    .edges_directed(index, Direction::Outgoing)
                    .map(|edge| edge.weight().produced_pcol_id.clone()),
            );
            outputs.sort();
            outputs.dedup();

            manager.add_stage(node.id(), main_inputs, side_inputs, outputs);

            // A runner source that replays an event stream (TestStream) is not
            // finished by completing a bundle; it reports completion explicitly.
            if let ExecutableNode::Runner(transform) = node {
                if !transform.source_auto_finishes() {
                    manager.set_source_auto_finish(&node.id(), false).ok();
                }
            }

            if let Some(required) =
                aggregation_required_watermark(node, graph, index, &executable.components)
            {
                let id = node.id();
                manager.set_stage_kind(&id, StageKind::WatermarkGated).ok();
                manager.set_required_watermark(&id, required).ok();
            }
        }

        manager
    }

    /// Register a stage and its PCollection wiring.
    ///
    /// A stage with no `main_inputs` is a source. The outputs are recorded as
    /// produced by this stage for [`pcollection_watermark`](Self::pcollection_watermark).
    pub fn add_stage(
        &mut self,
        stage: impl Into<StageId>,
        main_inputs: Vec<PCollectionId>,
        side_inputs: Vec<PCollectionId>,
        outputs: Vec<PCollectionId>,
    ) {
        let stage = stage.into();
        for output in &outputs {
            self.producers.insert(output.clone(), stage.clone());
        }
        for input in &main_inputs {
            let consumers = self.consumers.entry(input.clone()).or_default();
            if !consumers.contains(&stage) {
                consumers.push(stage.clone());
            }
        }
        let source = main_inputs.is_empty().then(SourceState::default);
        // A source has no upstream, so its input watermark is already maximal;
        // this keeps `refresh` from reporting it as newly advanced.
        let input = if source.is_some() {
            MAX_TIMESTAMP
        } else {
            MIN_TIMESTAMP
        };
        let unproduced = main_inputs.iter().cloned().collect();
        self.stages.insert(
            stage,
            StageState {
                main_inputs,
                side_inputs,
                outputs,
                source,
                input,
                output: MIN_TIMESTAMP,
                watermark_holds: BTreeMap::new(),
                residual_holds: BTreeMap::new(),
                unproduced,
                pending: BTreeSet::new(),
                pending_min: MAX_TIMESTAMP,
                in_flight: 0,
                queued: 0,
                max_in_flight: 1,
                completed: false,
                rerun_pending: false,
                kind: StageKind::default(),
                required_watermark: MIN_TIMESTAMP,
                watermark_at_last_run: MIN_TIMESTAMP,
            },
        );
    }

    /// Whether a stage is registered.
    pub fn contains_stage(&self, stage: &str) -> bool {
        self.stages.contains_key(stage)
    }

    /// All registered stage ids, in deterministic order.
    pub fn stages(&self) -> Vec<StageId> {
        let mut ids: Vec<StageId> = self.stages.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Report a source's watermark for its default partition.
    pub fn report_source_watermark(&mut self, stage: &str, timestamp: Timestamp) -> Result<()> {
        self.report_source_partition_watermark(stage, DEFAULT_PARTITION, timestamp)
    }

    /// Report a source partition's watermark. Monotonic per partition.
    pub fn report_source_partition_watermark(
        &mut self,
        stage: &str,
        partition: &str,
        timestamp: Timestamp,
    ) -> Result<()> {
        let source = self.source_mut(stage)?;
        let entry = source.partitions.entry(partition.to_string()).or_default();
        entry.watermark = entry.watermark.max(timestamp);
        entry.idle = false;
        Ok(())
    }

    /// Mark a source partition idle (or active).
    ///
    /// Idle partitions are excluded from the source's minimum; when every
    /// partition is idle the source is treated as advanced to [`MAX_TIMESTAMP`].
    pub fn set_source_partition_idle(
        &mut self,
        stage: &str,
        partition: &str,
        idle: bool,
    ) -> Result<()> {
        let source = self.source_mut(stage)?;
        let entry = source.partitions.entry(partition.to_string()).or_default();
        entry.idle = idle;
        Ok(())
    }

    /// Report that a bounded source has finished; its output becomes
    /// [`MAX_TIMESTAMP`].
    pub fn report_source_finished(&mut self, stage: &str) -> Result<()> {
        self.source_mut(stage)?.finished = true;
        Ok(())
    }

    /// Set whether completing a bundle marks this source finished.
    ///
    /// Default `true` (bounded sources). A runner source that drives an event
    /// stream sets `false` so the dispatcher can re-run it for the next event.
    pub fn set_source_auto_finish(&mut self, stage: &str, auto_finish: bool) -> Result<()> {
        self.source_mut(stage)?.auto_finish = auto_finish;
        Ok(())
    }

    /// Whether completing a bundle marks this source finished (default `true`).
    pub fn source_auto_finishes(&self, stage: &str) -> bool {
        self.stages
            .get(stage)
            .and_then(|stage| stage.source.as_ref())
            .map(|source| source.auto_finish)
            .unwrap_or(true)
    }

    /// Add a watermark hold at `timestamp` for `stage`.
    ///
    /// Holds are a multiset: the same timestamp may be held more than once and
    /// must be released as many times.
    pub fn add_hold(&mut self, stage: &str, timestamp: Timestamp) -> Result<()> {
        let stage = self.stage_mut(stage)?;
        *stage.watermark_holds.entry(timestamp).or_insert(0) += 1;
        Ok(())
    }

    /// Replace every stage's watermark holds with `watermark_holds`.
    ///
    /// The durable timer store is the source of truth for event-time holds: a
    /// pending event-time timer holds its owning stage's output watermark at the
    /// timer's `hold_timestamp` until it fires. Because that set changes as
    /// timers are set and cleared, the caller reconciles the whole map rather
    /// than incrementally adding/releasing. Stages absent from `watermark_holds`
    /// are cleared. Call [`Self::refresh`] afterwards to propagate the clamp.
    ///
    /// Holds only ever *clamp* output watermarks (which are monotonic), so
    /// releasing one lets the watermark advance again but never regresses it.
    pub fn set_event_time_holds(&mut self, watermark_holds: &HashMap<StageId, Vec<Timestamp>>) {
        for (id, stage) in self.stages.iter_mut() {
            stage.watermark_holds.clear();
            if let Some(timestamps) = watermark_holds.get(id) {
                for timestamp in timestamps {
                    *stage.watermark_holds.entry(*timestamp).or_insert(0) += 1;
                }
            }
        }
    }

    /// Replace every stage's deferred-work watermark holds with `residual_holds`.
    ///
    /// Unlike [`set_event_time_holds`](Self::set_event_time_holds) — which is
    /// rebuilt from the durable timer store every tick — these holds describe work
    /// the SDK has deferred (an SDF residual's `output_watermarks`: a lower bound
    /// on the timestamps that deferred work will still produce). They live in a
    /// separate multiset so a timer reconcile does not erase them. Stages absent
    /// from `residual_holds` are cleared. Call [`Self::refresh`] afterwards to
    /// propagate the clamp.
    pub fn set_residual_holds(&mut self, residual_holds: &HashMap<StageId, Vec<Timestamp>>) {
        for (id, stage) in self.stages.iter_mut() {
            stage.residual_holds.clear();
            if let Some(timestamps) = residual_holds.get(id) {
                for timestamp in timestamps {
                    *stage.residual_holds.entry(*timestamp).or_insert(0) += 1;
                }
            }
        }
    }

    /// Release one watermark hold at `timestamp` for `stage`.
    pub fn release_hold(&mut self, stage: &str, timestamp: Timestamp) -> Result<()> {
        let stage = self.stage_mut(stage)?;
        if let Some(count) = stage.watermark_holds.get_mut(&timestamp) {
            *count -= 1;
            if *count == 0 {
                stage.watermark_holds.remove(&timestamp);
            }
        }
        Ok(())
    }

    /// The earliest outstanding hold for `stage`, if any.
    pub fn min_hold(&self, stage: &str) -> Option<Timestamp> {
        self.stages.get(stage).and_then(StageState::min_hold)
    }

    /// The stage's input watermark, or `None` if the stage is unknown.
    ///
    /// This is the *unclamped* watermark (the minimum upstream output), used for
    /// readiness and for propagating watermarks. For decisions that must not run
    /// ahead of unconsumed input, use
    /// [`effective_input_watermark`](Self::effective_input_watermark).
    ///
    /// For a source this is [`MAX_TIMESTAMP`].
    pub fn input_watermark(&self, stage: &str) -> Option<Timestamp> {
        self.stages.get(stage).map(|stage| stage.input)
    }

    /// The stage's input watermark clamped by the minimum event-time of its
    /// pending (unconsumed) input, or `None` if the stage is unknown.
    ///
    /// While unconsumed input exists, the watermark cannot advance past its oldest
    /// element. When nothing is pending this equals
    /// [`input_watermark`](Self::input_watermark).
    pub fn effective_input_watermark(&self, stage: &str) -> Option<Timestamp> {
        self.stages
            .get(stage)
            .map(|stage| stage.input.min(stage.pending_min))
    }

    /// The stage's output watermark, or `None` if the stage is unknown.
    pub fn output_watermark(&self, stage: &str) -> Option<Timestamp> {
        self.stages.get(stage).map(|stage| stage.output)
    }

    /// The watermark of a PCollection: its producer stage's output watermark.
    pub fn pcollection_watermark(&self, pcollection: &str) -> Option<Timestamp> {
        let producer = self.producers.get(pcollection)?;
        self.output_watermark(producer)
    }

    /// The eligibility gate for a stage.
    pub fn stage_kind(&self, stage: &str) -> Option<StageKind> {
        self.stages.get(stage).map(|stage| stage.kind)
    }

    /// Set the eligibility gate for a stage.
    pub fn set_stage_kind(&mut self, stage: &str, kind: StageKind) -> Result<()> {
        self.stage_mut(stage)?.kind = kind;
        Ok(())
    }

    /// The input watermark a [`StageKind::WatermarkGated`] stage must reach.
    pub fn required_watermark(&self, stage: &str) -> Option<Timestamp> {
        self.stages.get(stage).map(|stage| stage.required_watermark)
    }

    /// Set the input watermark a gated stage must reach.
    pub fn set_required_watermark(&mut self, stage: &str, watermark: Timestamp) -> Result<()> {
        self.stage_mut(stage)?.required_watermark = watermark;
        Ok(())
    }

    /// Whether a stage is a source (it has no main inputs).
    pub fn is_source(&self, stage: &str) -> bool {
        self.stages
            .get(stage)
            .map(StageState::is_source)
            .unwrap_or(false)
    }

    /// Whether a stage has completed a bundle.
    pub fn is_stage_completed(&self, stage: &str) -> bool {
        self.stages
            .get(stage)
            .map(|stage| stage.completed)
            .unwrap_or(false)
    }

    /// Whether a stage has a bundle in flight.
    pub fn is_stage_in_flight(&self, stage: &str) -> bool {
        self.stages
            .get(stage)
            .map(|stage| stage.in_flight > 0)
            .unwrap_or(false)
    }

    /// The number of bundles currently executing for `stage`.
    pub fn in_flight_bundles(&self, stage: &str) -> usize {
        self.stages.get(stage).map(|s| s.in_flight).unwrap_or(0)
    }

    /// The number of declared-but-not-started bundles for `stage` (e.g. SDF
    /// residuals). The stage is not complete while this is non-zero.
    pub fn queued_bundles(&self, stage: &str) -> usize {
        self.stages.get(stage).map(|s| s.queued).unwrap_or(0)
    }

    /// The concurrency ceiling for `stage` (default `1`).
    pub fn max_in_flight(&self, stage: &str) -> usize {
        self.stages.get(stage).map(|s| s.max_in_flight).unwrap_or(1)
    }

    /// Set how many bundles of `stage` may run concurrently. Callers must only
    /// raise this for stages whose executor can actually produce independent
    /// work items (splittable stages); stateful runner-native stages must stay at
    /// `1`.
    pub fn set_max_in_flight(&mut self, stage: &str, max_in_flight: usize) -> Result<()> {
        let stage_state = self.stage_mut(stage)?;
        stage_state.max_in_flight = max_in_flight.max(1);
        Ok(())
    }

    /// Declare `count` additional bundle work items for `stage` (e.g. SDF
    /// residuals or splits). Each is consumed by one
    /// [`start_bundle`](Self::start_bundle); the stage does not complete until
    /// every queued item has run.
    pub fn enqueue_bundles(&mut self, stage: &str, count: usize) -> Result<()> {
        let stage_state = self.stage_mut(stage)?;
        stage_state.queued += count;
        Ok(())
    }

    /// Stages that may run a bundle now, sorted by id.
    ///
    /// A stage is ready when every main input has been produced, it has fewer
    /// bundles in flight than its concurrency ceiling, it has work to do (a first
    /// run, a re-arm, queued work, pending input, or a gated watermark advance),
    /// and its [`StageKind`] gate is satisfied. This does not reserve the stages;
    /// call [`Self::start_bundle`] before executing each one.
    pub fn ready_stages(&self) -> Vec<StageId> {
        let mut ready: Vec<StageId> = self
            .stages
            .iter()
            .filter(|(_, stage)| stage.is_ready())
            .map(|(id, _)| id.clone())
            .collect();
        ready.sort();
        ready
    }

    /// Mark that a bundle for `stage` has started.
    ///
    /// A stage may have several bundles in flight up to its
    /// [`max_in_flight`](Self::max_in_flight); each call consumes one queued work
    /// item (if any) and increments the in-flight count. The first bundle of a
    /// wave consumes the `pending` input; the rest are independent work items
    /// (SDF residuals).
    pub fn start_bundle(&mut self, stage: &str) -> Result<()> {
        let stage_state = self
            .stages
            .get_mut(stage)
            .ok_or_else(|| anyhow!("unknown stage '{stage}'"))?;
        if stage_state.in_flight >= stage_state.max_in_flight {
            bail!("stage '{stage}' already has its maximum number of bundles in flight");
        }
        let watermark_rerun = stage_state.kind == StageKind::WatermarkGated
            && stage_state.input > stage_state.watermark_at_last_run;
        if stage_state.completed
            && !stage_state.rerun_pending
            && stage_state.queued == 0
            && !watermark_rerun
            && stage_state.pending.is_empty()
        {
            bail!("stage '{stage}' has already completed and is not re-armed");
        }
        if !stage_state.unproduced.is_empty() {
            bail!("stage '{stage}' still has unproduced main inputs");
        }
        stage_state.in_flight += 1;
        stage_state.queued = stage_state.queued.saturating_sub(1);
        stage_state.rerun_pending = false;
        // The first bundle of a wave consumes every input currently pending.
        // Anything a producer appends while a bundle is in flight re-populates
        // `pending` and re-arms the stage for another run.
        if stage_state.in_flight == 1 {
            stage_state.pending.clear();
            stage_state.pending_min = MAX_TIMESTAMP;
            // Remember the watermark this wave ran at, so a later advance re-arms
            // a watermark-gated stage.
            stage_state.watermark_at_last_run = stage_state.input;
        }
        Ok(())
    }

    /// Mark a bundle for `stage` complete and propagate its outputs to consumers
    /// without a min-timestamp (no pending-input clamp).
    pub fn complete_bundle(&mut self, stage: &str) -> Result<Vec<StageId>> {
        self.complete_bundle_with_min(stage, None)
    }

    /// Mark a bundle for `stage` complete and propagate its outputs to consumers.
    ///
    /// Push-wake: each output PCollection is recorded as appended, which removes
    /// it from a consumer's first-run `unproduced` barrier *and* marks it pending,
    /// so a consumer that already ran is re-armed when its upstream produces more
    /// (the streams-tables table-append signal).
    ///
    /// `output_min_ts` is the minimum event-time committed by this bundle; it
    /// clamps each consumer's [`effective_input_watermark`](Self::effective_input_watermark)
    /// until the consumer drains the pending input. `None` means the bundle
    /// produced nothing with an event-time (no clamp).
    ///
    /// The returned vector lists the consumers that became ready as a result,
    /// sorted and deduplicated.
    pub fn complete_bundle_with_min(
        &mut self,
        stage: &str,
        output_min_ts: Option<Timestamp>,
    ) -> Result<Vec<StageId>> {
        let outputs = {
            let stage_state = self
                .stages
                .get(stage)
                .ok_or_else(|| anyhow!("unknown stage '{stage}'"))?;
            if stage_state.in_flight == 0 {
                bail!("stage '{stage}' has no bundle in flight");
            }
            stage_state.outputs.clone()
        };

        {
            let stage_state = self.stages.get_mut(stage).expect("checked above");
            stage_state.in_flight -= 1;
            stage_state.completed = true;
            // Do not clear `rerun_pending` here: a timer promoted while a bundle
            // was in flight (or during a concurrent bundle) must survive this
            // completion. `start_bundle` consumes the re-arm when the stage next
            // runs, so a stale one is harmless.
        }

        let mut newly_ready = Vec::new();
        for pcollection in outputs {
            let Some(consumers) = self.consumers.get(&pcollection).cloned() else {
                continue;
            };
            for consumer in consumers {
                if let Some(consumer_state) = self.stages.get_mut(&consumer) {
                    consumer_state.unproduced.remove(&pcollection);
                    consumer_state.pending.insert(pcollection.clone());
                    if let Some(output_min_ts) = output_min_ts {
                        consumer_state.pending_min = consumer_state.pending_min.min(output_min_ts);
                    }
                    if consumer_state.is_ready() {
                        newly_ready.push(consumer);
                    }
                }
            }
        }

        newly_ready.sort();
        newly_ready.dedup();
        Ok(newly_ready)
    }

    /// Whether every stage is done (vacuously true when empty).
    ///
    /// A stage is not done while it has unconsumed upstream output, is re-armed,
    /// or is a completed watermark-gated stage whose input watermark has advanced
    /// past its last run (it still has windows to emit).
    pub fn is_complete(&self) -> bool {
        self.stages.values().all(|stage| {
            stage.completed
                && stage.pending.is_empty()
                && !stage.rerun_pending
                && stage.queued == 0
                && stage.in_flight == 0
                && !(stage.kind == StageKind::WatermarkGated
                    && stage.input > stage.watermark_at_last_run)
        })
    }

    /// Re-arm a stage to run another bundle (e.g. after a timer fires).
    pub fn mark_rerun(&mut self, stage: &str) -> Result<()> {
        self.stage_mut(stage)?.rerun_pending = true;
        Ok(())
    }

    /// Advance every watermark to a fixpoint and return the stages whose input
    /// or output watermark moved, sorted by id.
    ///
    /// Call after any `report_*`/hold change. Watermarks never regress, so
    /// re-running with no new input returns an empty vector.
    pub fn refresh(&mut self) -> Vec<StageId> {
        let ids = self.stages();
        let mut advanced: HashSet<StageId> = HashSet::new();

        loop {
            let mut changed = false;
            for id in &ids {
                let (new_input, new_output) = {
                    let stage = &self.stages[id];
                    (self.compute_input(stage), self.compute_output(stage))
                };

                let stage = self.stages.get_mut(id).expect("stage id from this manager");
                if new_input > stage.input {
                    stage.input = new_input;
                    advanced.insert(id.clone());
                    changed = true;
                }
                if new_output > stage.output {
                    stage.output = new_output;
                    advanced.insert(id.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        let mut advanced: Vec<StageId> = advanced.into_iter().collect();
        advanced.sort();
        advanced
    }

    /// `MIN` over the upstream output watermarks of the stage's main and side
    /// inputs; [`MAX_TIMESTAMP`] for a source.
    fn compute_input(&self, stage: &StageState) -> Timestamp {
        if stage.is_source() {
            return MAX_TIMESTAMP;
        }
        let mut watermark = MAX_TIMESTAMP;
        for input in stage.main_inputs.iter().chain(stage.side_inputs.iter()) {
            watermark = watermark.min(self.upstream(input));
        }
        watermark
    }

    /// `MIN` over the upstream output watermarks of the stage's MAIN inputs, the
    /// minimum timestamp of its **unconsumed** input (`pending_min`), and its
    /// earliest watermark hold (event-time timer or deferred work); the source's own
    /// report for a source.
    ///
    /// The `pending_min` term is what makes holds effective: a stage that has not yet
    /// consumed input at timestamp `t` cannot have produced output past `t`, so its
    /// output must not advance to a finished upstream's `+∞` before that input is
    /// processed. Without it, a deferred-work hold (e.g. an SDF residual bound below
    /// the input watermark) could never clamp a watermark that had already advanced.
    fn compute_output(&self, stage: &StageState) -> Timestamp {
        if let Some(source) = &stage.source {
            return source.output();
        }
        let mut watermark = MAX_TIMESTAMP;
        for input in &stage.main_inputs {
            watermark = watermark.min(self.upstream(input));
        }
        watermark = watermark.min(stage.pending_min);
        if let Some(watermark_hold) = stage.min_hold() {
            watermark = watermark.min(watermark_hold);
        }
        watermark
    }

    /// Output watermark of the stage producing `pcollection`; [`MIN_TIMESTAMP`]
    /// when the producer is unknown, so an unwired input holds watermarks back
    /// rather than advancing them.
    fn upstream(&self, pcollection: &str) -> Timestamp {
        self.producers
            .get(pcollection)
            .and_then(|stage| self.stages.get(stage))
            .map(|stage| stage.output)
            .unwrap_or(MIN_TIMESTAMP)
    }

    fn stage_mut(&mut self, stage: &str) -> Result<&mut StageState> {
        self.stages
            .get_mut(stage)
            .ok_or_else(|| anyhow!("unknown stage '{stage}'"))
    }

    fn source_mut(&mut self, stage: &str) -> Result<&mut SourceState> {
        let stage_state = self.stage_mut(stage)?;
        if stage_state.source.is_none() {
            bail!("stage '{stage}' is not a source (it has main inputs)");
        }
        Ok(stage_state
            .source
            .as_mut()
            .expect("source stage has source state"))
    }
}

/// The earliest watermark at which a global window is complete, given the
/// windowing strategy's allowed lateness: `GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS +
/// allowed_lateness`.
///
/// [`MAX_TIMESTAMP`] is [`BEAM_MAX_TIMESTAMP_MILLIS`], which is above
/// [`GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS`], so a finished bounded source's output
/// always clears this gate: a global-window aggregation becomes ready exactly
/// when its upstream bounded input completes.
pub fn global_window_completion_watermark(allowed_lateness: Timestamp) -> Timestamp {
    GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS.saturating_add(allowed_lateness)
}

/// The watermark an aggregation runner stage must reach before it may run, or
/// `None` for stages that do not aggregate.
///
/// For a `GroupByKey` this is its input window's end plus the windowing
/// strategy's allowed lateness. M2.6 resolves the global-window case (what
/// bounded pipelines use); M5 generalizes this per window.
fn aggregation_required_watermark(
    node: &ExecutableNode,
    graph: &petgraph::Graph<ExecutableNode, ConsumerMetaData>,
    index: NodeIndex,
    components: &Components,
) -> Option<Timestamp> {
    let is_aggregation = matches!(
        node,
        ExecutableNode::Runner(transform)
            if transform.transfrom_spec().values().any(|pt| pt
                .spec
                .as_ref()
                .is_some_and(|spec| spec.urn == beam_urns::GROUP_BY_KEY_TRANSFORM))
    );
    if !is_aggregation {
        return None;
    }

    // Read the incoming edge's windowing strategy: its window fn decides the gate
    // and its allowed lateness extends the global-window case.
    let mut window_fn_urn: Option<String> = None;
    let mut allowed_lateness = 0i64;
    for edge in graph.edges_directed(index, Direction::Incoming) {
        let Some(pcol) = components.pcollections.get(&edge.weight().produced_pcol_id) else {
            continue;
        };
        let Some(strategy) = components
            .windowing_strategies
            .get(&pcol.windowing_strategy_id)
        else {
            continue;
        };
        allowed_lateness = allowed_lateness.max(strategy.allowed_lateness);
        if let Some(window_fn) = strategy.window_fn.as_ref() {
            window_fn_urn = Some(window_fn.urn.clone());
        }
    }

    let is_global_window = match window_fn_urn.as_deref() {
        Some(urn) => urn == beam_urns::GLOBAL_WINDOWS_FN,
        // Unknown/absent window fn: keep the M2.6 global-window guarantee.
        None => true,
    };
    if is_global_window {
        // The global window has no finite end, so it becomes ready only when a
        // finished bounded source reports `+inf` (or every partition is idle).
        Some(global_window_completion_watermark(allowed_lateness))
    } else {
        // Interval windows are ready per window (`window.max_timestamp() <= input
        // watermark`), decided inside `GroupByKey`. Run as soon as inputs are
        // produced and re-run as the watermark advances.
        Some(MIN_TIMESTAMP)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fusion::pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode};
    use crate::jobservice::urns::beam_urns;
    use crate::transforms::from_urn;
    use petgraph::Graph;
    use std::collections::HashMap;

    fn manager(stages: &[(&str, &[&str], &[&str], &[&str])]) -> WatermarkManager {
        let mut manager = WatermarkManager::new();
        for (id, mains, sides, outputs) in stages {
            manager.add_stage(
                *id,
                mains.iter().map(|s| s.to_string()).collect(),
                sides.iter().map(|s| s.to_string()).collect(),
                outputs.iter().map(|s| s.to_string()).collect(),
            );
        }
        manager
    }

    #[test]
    fn input_watermark_is_min_over_upstreams() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("B", &[], &[], &["q"]),
            ("C", &["p", "q"], &[], &["r"]),
        ]);

        manager.report_source_watermark("A", 100).unwrap();
        manager.report_source_watermark("B", 200).unwrap();
        manager.refresh();

        assert_eq!(manager.input_watermark("C"), Some(100));
        assert_eq!(manager.output_watermark("C"), Some(100));

        // Raising the slower source raises C's watermark to the new minimum.
        manager.report_source_watermark("A", 300).unwrap();
        manager.refresh();
        assert_eq!(manager.input_watermark("C"), Some(200));
    }

    #[test]
    fn watermarks_are_monotonic() {
        let mut manager = manager(&[("A", &[], &[], &["p"])]);

        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("A"), Some(100));

        // A lower report must not regress the watermark.
        manager.report_source_watermark("A", 50).unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("A"), Some(100));
    }

    #[test]
    fn idle_partitions_are_excluded_and_all_idle_advances_to_max() {
        let mut manager = manager(&[("S", &[], &[], &["p"])]);

        manager
            .report_source_partition_watermark("S", "p0", 100)
            .unwrap();
        manager
            .report_source_partition_watermark("S", "p1", 200)
            .unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("S"), Some(100));

        // The slow partition goes idle: the min is now over p1 alone.
        manager.set_source_partition_idle("S", "p0", true).unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("S"), Some(200));

        // Every partition idle: the source no longer holds anything back.
        manager.set_source_partition_idle("S", "p1", true).unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("S"), Some(MAX_TIMESTAMP));
    }

    #[test]
    fn side_inputs_hold_input_but_not_output() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("S", &[], &[], &["side"]),
            ("C", &["p"], &["side"], &["r"]),
        ]);

        manager.report_source_watermark("A", 100).unwrap();
        manager.report_source_watermark("S", 50).unwrap();
        manager.refresh();

        // Input is held back by the side input...
        assert_eq!(manager.input_watermark("C"), Some(50));
        // ...but the output advances with the main input only.
        assert_eq!(manager.output_watermark("C"), Some(100));

        // Once the side input catches up, the input watermark follows.
        manager.report_source_watermark("S", 200).unwrap();
        manager.refresh();
        assert_eq!(manager.input_watermark("C"), Some(100));
    }

    #[test]
    fn hold_clamps_output_and_downstream_until_released() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("B", &["p"], &[], &["q"]),
            ("C", &["q"], &[], &["r"]),
        ]);

        // The hold is registered before the input advances, as in a real bundle
        // where a timer holds the output timestamp.
        manager.add_hold("B", 30).unwrap();
        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();

        assert_eq!(manager.input_watermark("B"), Some(100));
        assert_eq!(manager.output_watermark("B"), Some(30));
        assert_eq!(manager.input_watermark("C"), Some(30));

        manager.release_hold("B", 30).unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("B"), Some(100));
        assert_eq!(manager.input_watermark("C"), Some(100));
    }

    #[test]
    fn holds_are_a_multiset() {
        let mut manager = manager(&[("A", &[], &[], &["p"])]);

        manager.add_hold("A", 30).unwrap();
        manager.add_hold("A", 30).unwrap();
        assert_eq!(manager.min_hold("A"), Some(30));

        manager.release_hold("A", 30).unwrap();
        assert_eq!(manager.min_hold("A"), Some(30), "one hold remains");
        manager.release_hold("A", 30).unwrap();
        assert_eq!(manager.min_hold("A"), None);
    }

    #[test]
    fn event_time_holds_clamp_output_and_release_when_cleared() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("B", &["p"], &[], &["q"]),
            ("C", &["q"], &[], &["r"]),
        ]);

        // A pending event-time timer on B holds B's output at 30, so C cannot
        // see B as complete past 30 even after the source finishes.
        manager.set_event_time_holds(&HashMap::from([("B".to_string(), vec![30])]));
        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();

        assert_eq!(manager.input_watermark("B"), Some(100));
        assert_eq!(manager.output_watermark("B"), Some(30));
        assert_eq!(manager.input_watermark("C"), Some(30));

        // The timer fires: the hold is gone and B's output catches up.
        manager.set_event_time_holds(&HashMap::new());
        manager.refresh();
        assert_eq!(manager.output_watermark("B"), Some(100));
        assert_eq!(manager.input_watermark("C"), Some(100));
    }

    #[test]
    fn reconciling_holds_reflects_added_and_removed_timers() {
        let mut manager = manager(&[("A", &[], &[], &["p"])]);

        // Two timers at the same hold timestamp: the multiset keeps the hold.
        manager.set_event_time_holds(&HashMap::from([("A".to_string(), vec![30, 30])]));
        assert_eq!(manager.min_hold("A"), Some(30));

        // One fires: the other still holds.
        manager.set_event_time_holds(&HashMap::from([("A".to_string(), vec![30])]));
        assert_eq!(manager.min_hold("A"), Some(30));

        // All fire: no holds remain.
        manager.set_event_time_holds(&HashMap::new());
        assert_eq!(manager.min_hold("A"), None);
    }

    #[test]
    fn residual_holds_clamp_output_and_release_when_cleared() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("B", &["p"], &[], &["q"]),
            ("C", &["q"], &[], &["r"]),
        ]);

        // Deferred (residual) work on B promises its remaining output is >= 30, so
        // B's output is held there and C cannot advance past it.
        manager.set_residual_holds(&HashMap::from([("B".to_string(), vec![30])]));
        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();

        assert_eq!(manager.input_watermark("B"), Some(100));
        assert_eq!(manager.output_watermark("B"), Some(30));
        assert_eq!(manager.input_watermark("C"), Some(30));

        // The deferred work completes: the hold is dropped and B catches up.
        manager.set_residual_holds(&HashMap::new());
        manager.refresh();
        assert_eq!(manager.output_watermark("B"), Some(100));
        assert_eq!(manager.input_watermark("C"), Some(100));
    }

    #[test]
    fn residual_holds_are_independent_of_event_time_holds() {
        let mut manager = manager(&[("A", &[], &[], &["p"])]);

        manager.set_residual_holds(&HashMap::from([("A".to_string(), vec![30])]));
        assert_eq!(manager.min_hold("A"), Some(30));

        // Rebuilding the event-time holds must not erase the deferred-work hold.
        manager.set_event_time_holds(&HashMap::new());
        assert_eq!(manager.min_hold("A"), Some(30));

        // Two kinds of hold: the earliest wins.
        manager.set_event_time_holds(&HashMap::from([("A".to_string(), vec![10])]));
        assert_eq!(manager.min_hold("A"), Some(10));

        // Clearing the deferred hold leaves only the event-time hold.
        manager.set_residual_holds(&HashMap::new());
        assert_eq!(manager.min_hold("A"), Some(10));
    }

    #[test]
    fn end_of_bounded_input_propagates_as_max_timestamp() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("B", &["p"], &[], &["q"]),
            ("C", &["q"], &[], &["r"]),
        ]);

        manager.report_source_finished("A").unwrap();
        manager.refresh();

        assert_eq!(manager.output_watermark("A"), Some(MAX_TIMESTAMP));
        assert_eq!(manager.input_watermark("B"), Some(MAX_TIMESTAMP));
        assert_eq!(manager.output_watermark("B"), Some(MAX_TIMESTAMP));
        assert_eq!(manager.input_watermark("C"), Some(MAX_TIMESTAMP));
    }

    #[test]
    fn unsourced_stage_starts_at_min_and_does_not_advance() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        // Nothing reported yet.
        assert_eq!(manager.refresh(), Vec::<StageId>::new());
        assert_eq!(manager.input_watermark("B"), Some(MIN_TIMESTAMP));
    }

    #[test]
    fn refresh_reports_advanced_stages_then_stabilizes() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        manager.report_source_watermark("A", 100).unwrap();
        assert_eq!(manager.refresh(), vec!["A".to_string(), "B".to_string()]);

        // No new input: nothing advances, so nothing is reported.
        assert!(manager.refresh().is_empty());
    }

    #[test]
    fn pcollection_watermark_tracks_the_producer() {
        let mut manager = manager(&[("A", &[], &[], &["p"])]);

        manager.report_source_watermark("A", 123).unwrap();
        manager.refresh();
        assert_eq!(manager.pcollection_watermark("p"), Some(123));
        assert_eq!(manager.pcollection_watermark("missing"), None);
    }

    #[test]
    fn reporting_on_unknown_or_non_source_stage_errors() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        assert!(manager.report_source_watermark("missing", 1).is_err());
        assert!(manager.report_source_watermark("B", 1).is_err());
        assert!(manager.add_hold("missing", 1).is_err());
    }

    #[test]
    fn stage_runs_only_after_all_inputs_are_produced() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("B", &[], &[], &["q"]),
            ("C", &["p", "q"], &[], &["r"]),
        ]);

        // Both roots are sources and are ready immediately.
        assert_eq!(
            manager.ready_stages(),
            vec!["A".to_string(), "B".to_string()]
        );
        manager.start_bundle("A").unwrap();
        manager.start_bundle("B").unwrap();
        assert!(manager.ready_stages().is_empty());

        // C still waits for "q".
        assert!(manager.complete_bundle("A").unwrap().is_empty());
        assert!(manager.ready_stages().is_empty());

        // Completing the other producer readies the fan-in consumer.
        assert_eq!(manager.complete_bundle("B").unwrap(), vec!["C".to_string()]);
        assert_eq!(manager.ready_stages(), vec!["C".to_string()]);
    }

    #[test]
    fn completing_a_bundle_readies_every_consumer() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p1", "p2"]),
            ("B", &["p1"], &[], &["q"]),
            ("C", &["p2"], &[], &["r"]),
        ]);

        assert_eq!(manager.ready_stages(), vec!["A".to_string()]);
        manager.start_bundle("A").unwrap();
        assert_eq!(
            manager.complete_bundle("A").unwrap(),
            vec!["B".to_string(), "C".to_string()]
        );
        assert_eq!(
            manager.ready_stages(),
            vec!["B".to_string(), "C".to_string()]
        );
    }

    #[test]
    fn gated_stage_waits_until_its_watermark_gate_is_reached() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("S", &["p"], &[], &["q"])]);
        manager
            .set_stage_kind("S", StageKind::WatermarkGated)
            .unwrap();
        manager.set_required_watermark("S", 100).unwrap();
        assert_eq!(manager.stage_kind("S"), Some(StageKind::WatermarkGated));
        assert_eq!(manager.required_watermark("S"), Some(100));

        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();

        // Input is available, but the watermark is below the gate: not runnable.
        manager.report_source_watermark("A", 50).unwrap();
        manager.refresh();
        assert!(
            manager.ready_stages().is_empty(),
            "50 is below the required 100"
        );

        // Crossing the gate makes it runnable.
        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();
        assert_eq!(manager.ready_stages(), vec!["S".to_string()]);

        // Outputs advance monotonically as the watermark keeps moving.
        manager.start_bundle("S").unwrap();
        manager.complete_bundle("S").unwrap();
        assert_eq!(manager.output_watermark("S"), Some(100));

        manager.report_source_watermark("A", 250).unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("S"), Some(250));
    }

    #[test]
    fn completed_gated_stage_reruns_only_when_the_watermark_advances() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("S", &["p"], &[], &["q"])]);
        manager
            .set_stage_kind("S", StageKind::WatermarkGated)
            .unwrap();
        manager.set_required_watermark("S", MIN_TIMESTAMP).unwrap();

        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();
        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();

        // First run of the gated stage.
        assert_eq!(manager.ready_stages(), vec!["S".to_string()]);
        manager.start_bundle("S").unwrap();
        manager.complete_bundle("S").unwrap();

        // No watermark advance: the completed stage must not re-run.
        manager.refresh();
        assert!(
            manager.ready_stages().is_empty(),
            "a completed gated stage must not re-run without a watermark advance"
        );
        assert!(manager.is_complete());

        // The watermark advances: the gated stage re-runs for newly-ready windows.
        manager.report_source_watermark("A", 200).unwrap();
        manager.refresh();
        assert_eq!(manager.ready_stages(), vec!["S".to_string()]);
        assert!(
            !manager.is_complete(),
            "a pending re-run means the graph is not complete"
        );

        manager.start_bundle("S").unwrap();
        manager.complete_bundle("S").unwrap();
        assert!(manager.is_complete());
    }

    #[test]
    fn global_window_aggregation_becomes_ready_when_the_bounded_source_finishes() {
        let gate = global_window_completion_watermark(0);

        // The constant relationship the gate relies on: a finished source's
        // output (MAX_TIMESTAMP) clears any global window plus lateness.
        assert!(GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS < BEAM_MAX_TIMESTAMP_MILLIS);
        assert!(gate >= GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS);
        assert!(MAX_TIMESTAMP >= gate);
        assert!(
            global_window_completion_watermark(1_000) > gate,
            "allowed lateness pushes the gate later"
        );

        let mut manager = manager(&[("A", &[], &[], &["p"]), ("G", &["p"], &[], &["q"])]);
        manager
            .set_stage_kind("G", StageKind::WatermarkGated)
            .unwrap();
        manager.set_required_watermark("G", gate).unwrap();

        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();
        assert!(
            manager.ready_stages().iter().all(|stage| stage != "G"),
            "aggregation must not run before its watermark reaches the window end"
        );

        manager.report_source_finished("A").unwrap();
        manager.refresh();
        assert_eq!(manager.input_watermark("G"), Some(MAX_TIMESTAMP));
        assert_eq!(manager.ready_stages(), vec!["G".to_string()]);
    }

    #[test]
    fn gated_stage_considers_the_side_input_watermark() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("Side", &[], &[], &["s"]),
            ("C", &["p"], &["s"], &["r"]),
        ]);
        manager
            .set_stage_kind("C", StageKind::WatermarkGated)
            .unwrap();
        manager.set_required_watermark("C", 100).unwrap();

        manager.start_bundle("A").unwrap();
        manager.start_bundle("Side").unwrap();
        manager.complete_bundle("A").unwrap();
        manager.complete_bundle("Side").unwrap();

        manager.report_source_watermark("A", 200).unwrap();
        manager.report_source_watermark("Side", 40).unwrap();
        manager.refresh();
        // Main input passes the gate, but the side input holds the input
        // watermark below it; the output watermark still advances with the main
        // input only.
        assert!(manager.ready_stages().is_empty());
        assert_eq!(manager.input_watermark("C"), Some(40));
        assert_eq!(manager.output_watermark("C"), Some(200));

        manager.report_source_watermark("Side", 150).unwrap();
        manager.refresh();
        assert_eq!(manager.input_watermark("C"), Some(150));
        assert_eq!(manager.ready_stages(), vec!["C".to_string()]);
    }

    #[test]
    fn fan_in_gated_stage_waits_for_all_upstreams_and_the_watermark() {
        let mut manager = manager(&[
            ("A", &[], &[], &["p"]),
            ("B", &[], &[], &["q"]),
            ("G", &["p", "q"], &[], &["r"]),
        ]);
        manager
            .set_stage_kind("G", StageKind::WatermarkGated)
            .unwrap();
        manager.set_required_watermark("G", 100).unwrap();

        manager.start_bundle("A").unwrap();
        manager.start_bundle("B").unwrap();
        manager.complete_bundle("A").unwrap();
        manager.report_source_watermark("A", 200).unwrap();
        manager.report_source_watermark("B", 200).unwrap();
        manager.refresh();
        // The watermark passes, but B's PCollection is not produced yet.
        assert!(manager.ready_stages().iter().all(|stage| stage != "G"));

        manager.complete_bundle("B").unwrap();
        manager.refresh();
        assert_eq!(manager.ready_stages(), vec!["G".to_string()]);
    }

    #[test]
    fn bundle_lifecycle_guards_are_enforced() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        manager.start_bundle("B").unwrap_err();
        manager.complete_bundle("A").unwrap_err();
        manager.start_bundle("unknown").unwrap_err();

        manager.start_bundle("A").unwrap();
        manager.start_bundle("A").unwrap_err();
        manager.complete_bundle("A").unwrap();
        manager.start_bundle("A").unwrap_err();
        manager.complete_bundle("A").unwrap_err();
    }

    #[test]
    fn is_complete_once_every_stage_has_run() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        assert!(!manager.is_complete());
        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();
        assert!(!manager.is_complete());
        manager.start_bundle("B").unwrap();
        manager.complete_bundle("B").unwrap();
        assert!(manager.is_complete());
    }

    #[test]
    fn completed_stage_runs_again_only_when_rearmed() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();
        manager.start_bundle("B").unwrap();
        manager.complete_bundle("B").unwrap();
        assert!(manager.is_complete());
        assert!(!manager.ready_stages().contains(&"B".to_string()));

        // Re-arming (as a fired timer does) makes it runnable and the pipeline
        // incomplete again.
        manager.mark_rerun("B").unwrap();
        assert!(!manager.is_complete());
        assert!(manager.ready_stages().contains(&"B".to_string()));

        // Starting the rerun consumes the re-arm; completing finishes it.
        manager.start_bundle("B").unwrap();
        assert!(!manager.ready_stages().contains(&"B".to_string()));
        manager.complete_bundle("B").unwrap();
        assert!(manager.is_complete());
    }

    #[test]
    fn queued_bundles_keep_a_stage_incomplete_until_drained() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);
        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();

        // B declares two residual bundles; neither starts until scheduled.
        manager.enqueue_bundles("B", 2).unwrap();
        assert_eq!(manager.queued_bundles("B"), 2);

        manager.start_bundle("B").unwrap();
        assert_eq!(manager.queued_bundles("B"), 1);
        manager.complete_bundle("B").unwrap();
        assert!(
            !manager.is_complete(),
            "one queued residual bundle still owes work"
        );

        manager.start_bundle("B").unwrap();
        assert_eq!(manager.queued_bundles("B"), 0);
        manager.complete_bundle("B").unwrap();
        assert!(manager.is_complete(), "all queued bundles drained");
    }

    #[test]
    fn max_in_flight_bounds_concurrent_queued_bundles() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);
        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();

        manager.set_max_in_flight("B", 3).unwrap();
        manager.enqueue_bundles("B", 3).unwrap();

        manager.start_bundle("B").unwrap();
        manager.start_bundle("B").unwrap();
        manager.start_bundle("B").unwrap();
        assert_eq!(manager.in_flight_bundles("B"), 3);
        assert_eq!(manager.queued_bundles("B"), 0);
        // At the concurrency ceiling: no fourth bundle.
        manager.start_bundle("B").unwrap_err();

        manager.complete_bundle("B").unwrap();
        assert_eq!(manager.in_flight_bundles("B"), 2);
        manager.complete_bundle("B").unwrap();
        manager.complete_bundle("B").unwrap();
        assert!(manager.is_complete());
    }

    #[test]
    fn enqueue_on_an_unknown_stage_errors() {
        let mut manager = manager(&[("A", &[], &[], &["p"])]);
        manager.enqueue_bundles("nope", 1).unwrap_err();
        manager.set_max_in_flight("nope", 2).unwrap_err();
        // A ceiling below one is clamped up so a stage can always run.
        manager.set_max_in_flight("A", 0).unwrap();
        assert_eq!(manager.max_in_flight("A"), 1);
    }

    /// A multi-bundle producer (as an SDF stage is) clamps its gated consumer at
    /// the earliest outstanding deferred-work hold, and the consumer only becomes
    /// ready once the last overlapping hold releases.
    #[test]
    fn concurrent_producer_bundles_hold_a_gated_consumer_until_all_release() {
        let mut manager = manager(&[
            ("S", &[], &[], &["s"]),
            ("P", &["s"], &[], &["p"]),
            ("C", &["p"], &[], &["q"]),
        ]);
        manager
            .set_stage_kind("C", StageKind::WatermarkGated)
            .unwrap();
        manager.set_required_watermark("C", 100).unwrap();

        // The source runs and completes (bounded end = +inf) committing a minimum
        // event-time of 50, which produces the producer's input and makes P
        // runnable. P's output is clamped by that unconsumed input.
        manager.start_bundle("S").unwrap();
        manager.complete_bundle_with_min("S", Some(50)).unwrap();
        manager.report_source_finished("S").unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("P"), Some(50));

        // P declares three concurrent residual bundles and starts them as a wave.
        manager.set_max_in_flight("P", 3).unwrap();
        manager.enqueue_bundles("P", 3).unwrap();
        manager.start_bundle("P").unwrap();
        manager.start_bundle("P").unwrap();
        manager.start_bundle("P").unwrap();
        assert_eq!(manager.in_flight_bundles("P"), 3);
        assert_eq!(manager.queued_bundles("P"), 0);

        // One bundle completes: this produces P's output (clears C's fan-in
        // barrier) while two bundles remain in flight.
        manager.complete_bundle("P").unwrap();
        assert_eq!(manager.in_flight_bundles("P"), 2);

        // The two outstanding residuals hold P's output at the earliest bound 50,
        // so the gated consumer C (needs >= 100) is not ready.
        manager.set_residual_holds(&HashMap::from([("P".to_string(), vec![50, 200])]));
        manager.refresh();
        assert_eq!(manager.output_watermark("P"), Some(50));
        assert!(!manager.ready_stages().contains(&"C".to_string()));

        // The bundle holding 50 completes and its hold releases; the remaining
        // bound 200 clears the gate, so C becomes ready.
        manager.complete_bundle("P").unwrap();
        manager.set_residual_holds(&HashMap::from([("P".to_string(), vec![200])]));
        manager.refresh();
        assert_eq!(manager.output_watermark("P"), Some(200));
        assert!(manager.ready_stages().contains(&"C".to_string()));

        // The last bundle completes and releases; P reaches +inf.
        manager.complete_bundle("P").unwrap();
        manager.set_residual_holds(&HashMap::new());
        manager.refresh();
        assert_eq!(manager.in_flight_bundles("P"), 0);
        assert_eq!(manager.output_watermark("P"), Some(MAX_TIMESTAMP));
    }

    #[test]
    fn completing_a_producer_rearms_an_already_completed_consumer() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        // Both stages run once; the pipeline is done be the bounded definition.
        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();
        manager.start_bundle("B").unwrap();
        manager.complete_bundle("B").unwrap();
        assert!(manager.is_complete());

        // A runs again (as a fired timer would): its appended output re-arms B
        // even though B has already completed.
        manager.mark_rerun("A").unwrap();
        manager.start_bundle("A").unwrap();
        assert_eq!(
            manager.complete_bundle("A").unwrap(),
            vec!["B".to_string()],
            "an upstream append wakes the completed consumer"
        );
        assert!(!manager.is_complete());
        assert_eq!(manager.ready_stages(), vec!["B".to_string()]);
    }

    #[test]
    fn starting_a_bundle_drains_pending_inputs() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();
        manager.start_bundle("B").unwrap();
        manager.complete_bundle("B").unwrap();
        manager.mark_rerun("A").unwrap();
        manager.start_bundle("A").unwrap();
        manager.complete_bundle("A").unwrap();
        assert_eq!(manager.ready_stages(), vec!["B".to_string()]);

        // Consuming the pending input clears it: with no further append, B is
        // complete again and the pipeline terminates.
        manager.start_bundle("B").unwrap();
        manager.complete_bundle("B").unwrap();
        assert!(manager.is_complete());
        assert!(manager.ready_stages().is_empty());
    }

    #[test]
    fn pending_min_clamps_output_and_effective_input() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        // A completes, committing a minimum event-time of 42 that B has not consumed.
        manager.start_bundle("A").unwrap();
        manager.complete_bundle_with_min("A", Some(42)).unwrap();
        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();

        // The propagated watermark and the readiness gate stay unclamped...
        assert_eq!(manager.input_watermark("B"), Some(100));
        // ...but neither the effective watermark nor B's output may pass the
        // oldest unconsumed input.
        assert_eq!(manager.effective_input_watermark("B"), Some(42));
        assert_eq!(manager.output_watermark("B"), Some(42));

        // Consuming the pending input lifts the clamp.
        manager.start_bundle("B").unwrap();
        assert_eq!(manager.effective_input_watermark("B"), Some(100));
        manager.complete_bundle("B").unwrap();
        manager.refresh();
        assert_eq!(manager.output_watermark("B"), Some(100));
    }

    #[test]
    fn a_bundle_without_event_times_does_not_clamp() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        manager.start_bundle("A").unwrap();
        manager.complete_bundle_with_min("A", None).unwrap();
        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();

        assert_eq!(manager.effective_input_watermark("B"), Some(100));
    }

    fn runner_node(name: &str, outputs: &[&str]) -> ExecutableNode {
        let outputs: HashMap<String, String> = outputs
            .iter()
            .map(|output| (format!("out-{output}"), output.to_string()))
            .collect();
        ExecutableNode::Runner(from_urn(
            beam_urns::IMPULSE_TRANSFORM,
            name.to_string(),
            HashMap::new(),
            outputs,
        ))
    }

    fn metadata(pcollection: &str) -> ConsumerMetaData {
        ConsumerMetaData {
            producer_transform_id: "producer".to_string(),
            produced_pcol_id: pcollection.to_string(),
            coder_id: "coder".to_string(),
            component_coder: None,
            consumer_transfrom_id: "consumer".to_string(),
        }
    }

    #[test]
    fn built_from_executable_graph_wires_inputs_outputs_and_sources() {
        let mut graph = Graph::new();
        let a = graph.add_node(runner_node("A", &["p"]));
        let b = graph.add_node(runner_node("B", &["q"]));
        graph.add_edge(a, b, metadata("p"));

        let executable = ExecutableGraph::from_graph_for_test(graph, metadata("p"));
        let mut manager = WatermarkManager::from_executable_graph(&executable);

        let a_id = executable.get_executable_graph()[a].id();
        let b_id = executable.get_executable_graph()[b].id();

        // The root is a source; the consumer treats "p" as a main input.
        assert!(manager.contains_stage(&a_id));
        assert!(manager.contains_stage(&b_id));
        manager.report_source_watermark(&a_id, 100).unwrap();
        manager.refresh();

        assert_eq!(manager.output_watermark(&a_id), Some(100));
        assert_eq!(manager.input_watermark(&b_id), Some(100));
        assert_eq!(manager.pcollection_watermark("p"), Some(100));
    }

    #[test]
    fn group_by_key_stage_is_gated_at_the_global_window_end() {
        let gbk = ExecutableNode::Runner(from_urn(
            beam_urns::GROUP_BY_KEY_TRANSFORM,
            "gbk".to_string(),
            HashMap::new(),
            HashMap::new(),
        ));

        let mut graph = Graph::new();
        let a = graph.add_node(runner_node("A", &["p"]));
        let g = graph.add_node(gbk);
        graph.add_edge(a, g, metadata("p"));

        let executable = ExecutableGraph::from_graph_for_test(graph, metadata("p"));
        let manager = WatermarkManager::from_executable_graph(&executable);
        let g_id = executable.get_executable_graph()[g].id();

        assert_eq!(manager.stage_kind(&g_id), Some(StageKind::WatermarkGated));
        assert_eq!(
            manager.required_watermark(&g_id),
            Some(global_window_completion_watermark(0))
        );
    }
}
