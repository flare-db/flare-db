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
//! output = MIN(upstream output watermarks of MAIN inputs, min watermark hold)
//! ```
//!
//! Side inputs gate the *input* watermark — they hold back execution — but they
//! do not advance the *output* watermark; only main inputs do. This matches the
//! portable runners (Prism's `bundleReady`/`updateWatermarks` split and the
//! Python `WatermarkManager.StageNode`).
//!
//! A *source* (a stage with no main inputs) reports its own watermark. Sources
//! may have partitions; idle partitions are excluded from the minimum, and a
//! source whose partitions are all idle is treated as advanced to
//! [`MAX_TIMESTAMP`]. Reporting a source finished pins its output to
//! [`MAX_TIMESTAMP`], which is how the end of a bounded input propagates.
//!
//! All watermarks are monotonic and start at [`MIN_TIMESTAMP`].
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
    /// Prism's aggregate/stateful readiness: such a stage must not run until
    /// time has moved past what its work needs. A finished bounded source's
    /// output is [`MAX_TIMESTAMP`], so a bounded aggregation still runs exactly
    /// when its upstream completes.
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
    /// Multiset of watermark holds, keyed by hold timestamp.
    holds: BTreeMap<Timestamp, usize>,

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
    /// cleared when a bundle consumes the pending inputs. This is the input
    /// watermark clamp: the watermark must not advance past data not yet consumed.
    /// The readiness gate deliberately uses the *unclamped* `input`, mirroring
    /// Prism's split between its trigger watermark (`upstreamW`) and `ss.input`.
    pending_min: Timestamp,
    /// A bundle for this stage is currently executing.
    in_flight: bool,
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

    /// The earliest outstanding hold, or `None` when none are held.
    fn min_hold(&self) -> Option<Timestamp> {
        self.holds.keys().next().copied()
    }

    /// Whether this stage may run a bundle now, given its current input
    /// watermark.
    fn is_ready(&self) -> bool {
        if self.in_flight || !self.unproduced.is_empty() {
            return false;
        }
        // A stage that has never run starts once its inputs are produced; a
        // stage that has already run runs again when it is re-armed (a fired
        // timer), has unconsumed upstream output (push-wake), or — for a
        // watermark-gated stage — when its input watermark has advanced past the
        // watermark at its last run.
        let has_work = if self.completed {
            self.rerun_pending
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
                holds: BTreeMap::new(),
                unproduced,
                pending: BTreeSet::new(),
                pending_min: MAX_TIMESTAMP,
                in_flight: false,
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
        *stage.holds.entry(timestamp).or_insert(0) += 1;
        Ok(())
    }

    /// Replace every stage's watermark holds with `holds`.
    ///
    /// The durable timer store is the source of truth for event-time holds: a
    /// pending event-time timer holds its owning stage's output watermark at the
    /// timer's `hold_timestamp` until it fires. Because that set changes as
    /// timers are set and cleared, the caller reconciles the whole map rather
    /// than incrementally adding/releasing. Stages absent from `holds` are
    /// cleared. Call [`Self::refresh`] afterwards to propagate the clamp.
    ///
    /// Holds only ever *clamp* output watermarks (which are monotonic), so
    /// releasing one lets the watermark advance again but never regresses it.
    pub fn set_event_time_holds(&mut self, holds: &HashMap<StageId, Vec<Timestamp>>) {
        for (id, stage) in self.stages.iter_mut() {
            stage.holds.clear();
            if let Some(timestamps) = holds.get(id) {
                for timestamp in timestamps {
                    *stage.holds.entry(*timestamp).or_insert(0) += 1;
                }
            }
        }
    }

    /// Release one hold at `timestamp` for `stage`.
    pub fn release_hold(&mut self, stage: &str, timestamp: Timestamp) -> Result<()> {
        let stage = self.stage_mut(stage)?;
        if let Some(count) = stage.holds.get_mut(&timestamp) {
            *count -= 1;
            if *count == 0 {
                stage.holds.remove(&timestamp);
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
    /// Prism's `In' = MAX(In, MIN(minPendingTimestamp, upstream))`, minus the
    /// `MAX` because our watermarks are already monotonic: while unconsumed input
    /// exists, the watermark cannot advance past its oldest element. When nothing
    /// is pending this equals [`input_watermark`](Self::input_watermark).
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
            .map(|stage| stage.in_flight)
            .unwrap_or(false)
    }

    /// Stages that may run a bundle now, sorted by id.
    ///
    /// A stage is ready when every main input has been produced, it has no
    /// bundle in flight, it has not completed, and its [`StageKind`] gate is
    /// satisfied. This does not reserve the stages; call [`Self::start_bundle`]
    /// before executing each one.
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
    pub fn start_bundle(&mut self, stage: &str) -> Result<()> {
        let stage_state = self
            .stages
            .get_mut(stage)
            .ok_or_else(|| anyhow!("unknown stage '{stage}'"))?;
        if stage_state.in_flight {
            bail!("stage '{stage}' already has a bundle in flight");
        }
        let watermark_rerun = stage_state.kind == StageKind::WatermarkGated
            && stage_state.input > stage_state.watermark_at_last_run;
        if stage_state.completed
            && !stage_state.rerun_pending
            && !watermark_rerun
            && stage_state.pending.is_empty()
        {
            bail!("stage '{stage}' has already completed and is not re-armed");
        }
        if !stage_state.unproduced.is_empty() {
            bail!("stage '{stage}' still has unproduced main inputs");
        }
        stage_state.in_flight = true;
        stage_state.rerun_pending = false;
        // This bundle consumes every input currently pending. Anything a producer
        // appends while this bundle is in flight re-populates `pending` and
        // re-arms the stage for another run.
        stage_state.pending.clear();
        stage_state.pending_min = MAX_TIMESTAMP;
        // Remember the watermark this bundle ran at, so a later advance re-arms
        // a watermark-gated stage.
        stage_state.watermark_at_last_run = stage_state.input;
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
            if !stage_state.in_flight {
                bail!("stage '{stage}' has no bundle in flight");
            }
            stage_state.outputs.clone()
        };

        {
            let stage_state = self.stages.get_mut(stage).expect("checked above");
            stage_state.in_flight = false;
            stage_state.completed = true;
            stage_state.rerun_pending = false;
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

    /// `MIN` over the upstream output watermarks of the stage's MAIN inputs and
    /// its earliest hold; the source's own report for a source.
    fn compute_output(&self, stage: &StageState) -> Timestamp {
        if let Some(source) = &stage.source {
            return source.output();
        }
        let mut watermark = MAX_TIMESTAMP;
        for input in &stage.main_inputs {
            watermark = watermark.min(self.upstream(input));
        }
        if let Some(hold) = stage.min_hold() {
            watermark = watermark.min(hold);
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
    fn pending_min_clamps_only_the_effective_input_watermark() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("B", &["p"], &[], &["q"])]);

        // A completes, committing a minimum event-time of 42 that B has not consumed.
        manager.start_bundle("A").unwrap();
        manager.complete_bundle_with_min("A", Some(42)).unwrap();
        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();

        // The propagated watermark and the readiness gate stay unclamped...
        assert_eq!(manager.input_watermark("B"), Some(100));
        // ...but the effective watermark cannot pass unconsumed input.
        assert_eq!(manager.effective_input_watermark("B"), Some(42));

        // Consuming the pending input lifts the clamp.
        manager.start_bundle("B").unwrap();
        assert_eq!(manager.effective_input_watermark("B"), Some(100));
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
