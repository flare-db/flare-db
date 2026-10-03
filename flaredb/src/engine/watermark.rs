//! In-memory watermark propagation and stage eligibility over an
//! [`ExecutableGraph`].
//!
//! This is the single owner of per-stage state: watermarks, holds, and the
//! pending/in-flight bookkeeping that decides when a stage may run a bundle.
//! There is deliberately no second copy of this state elsewhere (for example in
//! [`crate::engine::scheduler`]). It owns no I/O, no transform execution and no
//! proto types; a later milestone wires external watermark reports into it.
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
//! marks the stage done (for a bounded pipeline) and appends each output
//! PCollection to its consumers' pending inputs. See [`WatermarkManager::ready_stages`],
//! [`WatermarkManager::start_bundle`] and [`WatermarkManager::complete_bundle`].
//!
//! # Subscription
//!
//! [`WatermarkManager::refresh`] advances the graph to a fixpoint and returns the
//! stages whose input or output watermark moved, in deterministic order. A later
//! milestone uses that return value as the push notification that drives
//! scheduling; this module only computes it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{Result, anyhow, bail};
use petgraph::Direction;

use crate::coders::primitives::{BEAM_MAX_TIMESTAMP_MILLIS, BEAM_MIN_TIMESTAMP_MILLIS};
use crate::fusion::pipeline::{ExecutableGraph, ExecutableNode};

/// A watermark timestamp in milliseconds since the Unix epoch.
pub type Timestamp = i64;

/// The watermark below which nothing is known; the initial value of every stage.
pub const MIN_TIMESTAMP: Timestamp = BEAM_MIN_TIMESTAMP_MILLIS;

/// Beam's maximum timestamp, used as "+∞" for a finished bounded source.
pub const MAX_TIMESTAMP: Timestamp = BEAM_MAX_TIMESTAMP_MILLIS;

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
#[derive(Debug, Clone, Default)]
struct SourceState {
    partitions: BTreeMap<String, PartitionWatermark>,
    finished: bool,
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
    /// case and matches the previous predecessor-complete scheduler.
    #[default]
    Ordinary,
    /// Additionally waits for the input watermark to advance past the value
    /// observed at the previous bundle. This is Prism's aggregate/stateful
    /// readiness: such a stage must not run again until time moves.
    WatermarkGated,
}

/// Per-stage watermark, hold and eligibility bookkeeping.
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

    // -- eligibility --
    /// Main inputs not yet produced by their producer; empty means every input
    /// is available and the stage may run.
    unproduced: BTreeSet<PCollectionId>,
    /// A bundle for this stage is currently executing.
    in_flight: bool,
    /// The stage has completed a bundle. A bounded stage runs exactly once.
    completed: bool,
    kind: StageKind,
    /// Input watermark observed when the last bundle started; the gate for
    /// [`StageKind::WatermarkGated`].
    last_bundle_input: Timestamp,
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
        if self.in_flight || self.completed || !self.unproduced.is_empty() {
            return false;
        }
        match self.kind {
            StageKind::Ordinary => true,
            StageKind::WatermarkGated => self.input > self.last_bundle_input,
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
    pub fn from_executable_graph(graph: &ExecutableGraph) -> Self {
        let graph = graph.get_executable_graph();
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
                in_flight: false,
                completed: false,
                kind: StageKind::default(),
                last_bundle_input: MIN_TIMESTAMP,
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

    /// Add a watermark hold at `timestamp` for `stage`.
    ///
    /// Holds are a multiset: the same timestamp may be held more than once and
    /// must be released as many times.
    pub fn add_hold(&mut self, stage: &str, timestamp: Timestamp) -> Result<()> {
        let stage = self.stage_mut(stage)?;
        *stage.holds.entry(timestamp).or_insert(0) += 1;
        Ok(())
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
    /// For a source this is [`MAX_TIMESTAMP`].
    pub fn input_watermark(&self, stage: &str) -> Option<Timestamp> {
        self.stages.get(stage).map(|stage| stage.input)
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
        if stage_state.completed {
            bail!("stage '{stage}' has already completed");
        }
        if !stage_state.unproduced.is_empty() {
            bail!("stage '{stage}' still has unproduced main inputs");
        }
        stage_state.in_flight = true;
        Ok(())
    }

    /// Mark a bundle for `stage` complete and propagate its outputs to
    /// consumers.
    ///
    /// Each output PCollection is appended to its consumers' pending inputs;
    /// the returned vector lists the consumers that became ready as a result,
    /// sorted and deduplicated.
    pub fn complete_bundle(&mut self, stage: &str) -> Result<Vec<StageId>> {
        let (outputs, input) = {
            let stage_state = self
                .stages
                .get(stage)
                .ok_or_else(|| anyhow!("unknown stage '{stage}'"))?;
            if !stage_state.in_flight {
                bail!("stage '{stage}' has no bundle in flight");
            }
            (stage_state.outputs.clone(), stage_state.input)
        };

        {
            let stage_state = self.stages.get_mut(stage).expect("checked above");
            stage_state.in_flight = false;
            stage_state.completed = true;
            stage_state.last_bundle_input = input;
        }

        let mut newly_ready = Vec::new();
        for pcollection in outputs {
            let Some(consumers) = self.consumers.get(&pcollection).cloned() else {
                continue;
            };
            for consumer in consumers {
                if let Some(consumer_state) = self.stages.get_mut(&consumer) {
                    consumer_state.unproduced.remove(&pcollection);
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

    /// Whether every stage has completed a bundle (vacuously true when empty).
    pub fn is_complete(&self) -> bool {
        self.stages.values().all(|stage| stage.completed)
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
    fn watermark_gated_stage_waits_for_an_advance() {
        let mut manager = manager(&[("A", &[], &[], &["p"]), ("S", &["p"], &[], &["q"])]);
        manager
            .set_stage_kind("S", StageKind::WatermarkGated)
            .unwrap();
        assert_eq!(manager.stage_kind("S"), Some(StageKind::WatermarkGated));

        manager.start_bundle("A").unwrap();
        // No watermark reported yet, so S's input is still MIN and the gate holds.
        assert!(manager.complete_bundle("A").unwrap().is_empty());
        assert!(
            manager.ready_stages().is_empty(),
            "gated stage must wait for a watermark advance"
        );

        manager.report_source_watermark("A", 100).unwrap();
        manager.refresh();
        assert_eq!(manager.ready_stages(), vec!["S".to_string()]);
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
}
