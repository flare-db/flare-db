use std::collections::{BTreeMap, HashMap};

use petgraph::Direction;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;

use log::{debug, info};

use crate::engine::watermark::{
    MAX_TIMESTAMP, MIN_TIMESTAMP, StageKind, WatermarkManager, format_timestamp,
};
use crate::fusion::pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode};
use crate::state::timer::{TimeDomain, TimerEntry};

/// Scheduler that manages execution state for an `ExecutableGraph`.
///
/// Eligibility is delegated to [`WatermarkManager`], the single owner of
/// per-stage pending/in-flight and watermark state. In the bounded case a node
/// is ready once every main input PCollection has been produced by a completing
/// predecessor; this replaces the previous all-predecessors-executed check,
/// whose executed/in-flight tracking it duplicated.
pub struct NodeScheduler {
    graph: ExecutableGraph,
    /// Shared per-stage watermark, hold and pending/in-flight state.
    watermarks: WatermarkManager,
    /// Stage id to graph node index.
    index_of: HashMap<String, NodeIndex>,
    /// Owning stage id for each transform inside an SDK stage, used to route a
    /// fired timer back to the stage that must run its `@OnTimer`.
    transform_to_stage: HashMap<String, String>,
}

impl NodeScheduler {
    pub fn new(graph: ExecutableGraph) -> Self {
        let watermarks = WatermarkManager::from_executable_graph(&graph);
        let mut index_of = HashMap::new();
        let mut transform_to_stage = HashMap::new();
        for index in graph.get_executable_graph().node_indices() {
            let node = &graph.get_executable_graph()[index];
            index_of.insert(node.id(), index);
            if let ExecutableNode::Worker(stage) = node {
                for transform in stage.transforms() {
                    transform_to_stage.insert(transform.id().clone(), stage.id());
                }
            }
        }

        // One-line plan summary so a job's scheduling shape is visible up front.
        let stages = watermarks.stages();
        let gated: Vec<String> = stages
            .iter()
            .filter(|stage| watermarks.stage_kind(stage) == Some(StageKind::WatermarkGated))
            .map(|stage| {
                format!(
                    "{stage}@{}",
                    format_timestamp(
                        watermarks
                            .required_watermark(stage)
                            .unwrap_or(MIN_TIMESTAMP)
                    )
                )
            })
            .collect();
        info!(
            "scheduler: {} stages, {} watermark-gated [{}]",
            stages.len(),
            gated.len(),
            gated.join(", ")
        );

        Self {
            graph,
            watermarks,
            index_of,
            transform_to_stage,
        }
    }

    /// Returns the next ready nodes to execute, ordered by stage id.
    ///
    /// Eligibility is `refresh() -> ready_stages()`: watermarks are propagated
    /// first, then the shared pending/in-flight state decides. A node is ready
    /// when all of its main input PCollections have been produced and its
    /// watermark gate (if any) is satisfied. Nodes returned by this method are
    /// marked in-flight.
    pub fn next_nodes(&mut self) -> Vec<(NodeIndex, ExecutableNode)> {
        self.watermarks.refresh();
        let ready = self.watermarks.ready_stages();

        // Per-step eligibility trace. `debug!` keeps this off the default
        // `info` firehose; enable with `RUST_LOG=flaredb::engine=debug`.
        debug!(
            "scheduler: ready [{}]",
            ready
                .iter()
                .map(|stage| format!(
                    "{stage}(in={}, out={})",
                    format_timestamp(
                        self.watermarks
                            .input_watermark(stage)
                            .unwrap_or(MIN_TIMESTAMP)
                    ),
                    format_timestamp(
                        self.watermarks
                            .output_watermark(stage)
                            .unwrap_or(MIN_TIMESTAMP)
                    ),
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );

        #[cfg(debug_assertions)]
        self.debug_assert_eligibility_matches_dependency_rule(&ready);

        let mut next = Vec::new();

        for stage in ready {
            let Some(&index) = self.index_of.get(&stage) else {
                continue;
            };
            if self.watermarks.start_bundle(&stage).is_err() {
                continue;
            }
            next.push((index, self.graph.get_executable_graph()[index].clone()));
        }

        next
    }

    /// Marks a node as completed, propagating its outputs.
    ///
    /// A bounded source reports `+∞` when it finishes, so a source's completion
    /// is what satisfies the watermark permit of everything downstream. The
    /// completion happens once per node, after all of that node's internal
    /// bundles (including SDF residuals and splits) are done, so a stage's output
    /// never advances after just the first bundle.
    pub fn mark_complete(&mut self, idx: NodeIndex) {
        let stage = self.graph.get_executable_graph()[idx].id();
        if self.watermarks.is_source(&stage) {
            let _ = self.watermarks.report_source_finished(&stage);
            info!(
                "scheduler: source '{stage}' finished; output watermark = {}",
                format_timestamp(MAX_TIMESTAMP)
            );
        }
        let newly_ready = self.watermarks.complete_bundle(&stage).unwrap_or_default();
        let advanced = self.watermarks.refresh();
        debug!(
            "scheduler: stage '{stage}' completed; newly ready [{}]; advanced [{}]",
            newly_ready.join(", "),
            advanced
                .iter()
                .map(|stage| format!(
                    "{stage}(in={}, out={})",
                    format_timestamp(
                        self.watermarks
                            .input_watermark(stage)
                            .unwrap_or(MIN_TIMESTAMP)
                    ),
                    format_timestamp(
                        self.watermarks
                            .output_watermark(stage)
                            .unwrap_or(MIN_TIMESTAMP)
                    ),
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    /// Debug-only cross-check that the watermark-driven eligibility answer agrees
    /// with the previous all-predecessors-complete rule on a bounded run.
    ///
    /// The watermark gate can only restrict, so the new answer must be a subset
    /// of the dependency answer; for ungated (`Ordinary`) stages the two must be
    /// equal.
    #[cfg(debug_assertions)]
    fn debug_assert_eligibility_matches_dependency_rule(&self, ready: &[String]) {
        use std::collections::HashSet;

        let graph = self.graph.get_executable_graph();
        let new: HashSet<NodeIndex> = ready
            .iter()
            .filter_map(|stage| self.index_of.get(stage).copied())
            .collect();

        let mut legacy: HashSet<NodeIndex> = HashSet::new();
        for index in graph.node_indices() {
            let stage = graph[index].id();
            if self.watermarks.is_stage_completed(&stage)
                || self.watermarks.is_stage_in_flight(&stage)
            {
                continue;
            }
            let all_predecessors_done =
                graph
                    .edges_directed(index, Direction::Incoming)
                    .all(|edge| {
                        self.watermarks
                            .is_stage_completed(&graph[edge.source()].id())
                    });
            if all_predecessors_done {
                legacy.insert(index);
            }
        }

        // A completed stage may be legitimately ready again after a timer fires;
        // the dependency rule only models first-run eligibility. So the new
        // answer must agree on stages that have not yet run, and every
        // dependency-ready ordinary stage must be first-run ready.
        for index in &new {
            let stage = graph[*index].id();
            if !self.watermarks.is_stage_completed(&stage) {
                assert!(
                    legacy.contains(index),
                    "first-run stage '{stage}' is not dependency-ready"
                );
            }
        }

        for index in &legacy {
            let stage = graph[*index].id();
            if self.watermarks.stage_kind(&stage) == Some(StageKind::Ordinary) {
                assert!(
                    new.contains(index),
                    "ordinary stage '{stage}' is dependency-ready but not watermark-ready"
                );
            }
        }
    }

    /// Re-arm the stage that owns each due timer, and return the timers grouped
    /// by owning stage so they can be delivered when that stage runs again.
    ///
    /// Timers whose transform maps to no stage are ignored (a warning is logged
    /// by the caller).
    pub fn promote_due_timers(&mut self, due: &[TimerEntry]) -> Vec<(String, Vec<TimerEntry>)> {
        let mut by_stage: BTreeMap<String, Vec<TimerEntry>> = BTreeMap::new();
        for entry in due {
            let Some(stage) = self
                .transform_to_stage
                .get(&entry.key.transform_id)
                .cloned()
            else {
                continue;
            };
            let _ = self.watermarks.mark_rerun(&stage);
            by_stage.entry(stage).or_default().push(entry.clone());
        }
        by_stage.into_iter().collect()
    }

    /// Re-arm the stage that owns each due **event-time** timer and return the
    /// timers grouped by owning stage.
    ///
    /// Beam fires an event-time timer once its owning stage's input watermark has
    /// reached the timer's timestamp ([`TimerEntry::is_due_at_watermark`], i.e.
    /// `fire_timestamp <= input_watermark`). Processing-time timers are ignored
    /// here; they are driven by [`Self::promote_due_timers`] off the clock.
    ///
    /// A timer whose stage currently has a bundle in flight is deliberately left
    /// un-promoted: re-arming it now would be cleared when that bundle completes
    /// (`complete_bundle` resets `rerun_pending`), and the caller may have already
    /// deleted it from the store. It will be promoted after the bundle finishes.
    pub fn promote_due_event_time_timers(
        &mut self,
        timers: &[TimerEntry],
    ) -> Vec<(String, Vec<TimerEntry>)> {
        let mut by_stage: BTreeMap<String, Vec<TimerEntry>> = BTreeMap::new();
        for entry in timers {
            if entry.domain != TimeDomain::EventTime {
                continue;
            }
            let Some(stage) = self
                .transform_to_stage
                .get(&entry.key.transform_id)
                .cloned()
            else {
                continue;
            };
            if self.watermarks.is_stage_in_flight(&stage) {
                continue;
            }
            let watermark = self
                .watermarks
                .input_watermark(&stage)
                .unwrap_or(MIN_TIMESTAMP);
            if entry.is_due_at_watermark(watermark) {
                let _ = self.watermarks.mark_rerun(&stage);
                by_stage.entry(stage).or_default().push(entry.clone());
            }
        }
        by_stage.into_iter().collect()
    }

    /// The shared per-stage watermark and eligibility state.
    pub fn watermarks(&self) -> &WatermarkManager {
        &self.watermarks
    }

    /// Mutable access to the shared per-stage watermark and eligibility state.
    pub fn watermarks_mut(&mut self) -> &mut WatermarkManager {
        &mut self.watermarks
    }

    /// Returns metadata for every incoming edge to `idx`.
    ///
    /// - If the node has incoming edges, returns each edge's `ConsumerMetaData`
    ///   (a fan-in node such as a Flatten has one entry per input).
    /// - If the node has no predecessors (i.e. it's a root), returns a
    ///   single-element vector holding the graph's root metadata.
    pub fn input_edge_metadata(&self, idx: NodeIndex) -> Vec<ConsumerMetaData> {
        let graph = self.graph.get_executable_graph();

        let incoming: Vec<ConsumerMetaData> = graph
            .edges_directed(idx, Direction::Incoming)
            .map(|edge| edge.weight().clone())
            .collect();

        if incoming.is_empty() {
            // sometimes, a graph might have multiple roots
            // we need to identify the apporiate inpluse/root for a pirticular transfrom and return it
            vec![self.root_metadata().clone()]
        } else {
            incoming
        }
    }

    /// Returns metadata for the first outgoing edge from `idx`, if any.
    pub fn output_edge_metadata(&self, idx: NodeIndex) -> Option<ConsumerMetaData> {
        self.graph
            .get_executable_graph()
            .edges_directed(idx, Direction::Outgoing)
            .next()
            .map(|edge| edge.weight().clone())
    }

    /// Returns the root `ConsumerMetaData` for the graph.
    pub fn root_metadata(&self) -> &ConsumerMetaData {
        self.graph.get_root_metadata()
    }

    /// Returns `true` when every node in the graph has been executed.
    pub fn is_complete(&self) -> bool {
        self.watermarks.is_complete()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use petgraph::Graph;

    use crate::engine::scheduler::NodeScheduler;
    use crate::fusion::pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode};
    use crate::jobservice::urns::beam_urns;
    use crate::state::timer::{TimeDomain, TimerEntry, TimerKey};
    use crate::transforms::from_urn;

    fn runner_node(name: &str) -> ExecutableNode {
        ExecutableNode::Runner(from_urn(
            beam_urns::IMPULSE_TRANSFORM,
            name.to_string(),
            HashMap::new(),
            HashMap::new(),
        ))
    }

    /// A minimal SDK (worker) stage with a single transform, so `NodeScheduler`
    /// maps that transform id to this stage (required for timer routing).
    fn worker_stage_node(transform_id: &str) -> ExecutableNode {
        use beam_model_rs::v1::executable_stage_payload::WireCoderSetting;
        use beam_model_rs::v1::{Components, Environment, PCollection, PTransform};
        use indexmap::IndexSet;

        use crate::fusion::pipeline::{PCollectionNode, PTransformNode};
        use crate::fusion::stage::ExecutableStage;

        let mut components = Components::default();
        let transform = PTransform {
            unique_name: transform_id.to_string(),
            ..Default::default()
        };
        components
            .transforms
            .insert(transform_id.to_string(), transform.clone());

        let mut transforms = IndexSet::new();
        transforms.insert(PTransformNode {
            id: transform_id.to_string(),
            transform,
        });

        ExecutableNode::Worker(ExecutableStage::from(
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
                    coder_id: "c".to_string(),
                    ..Default::default()
                },
            },
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            transforms,
        ))
    }

    fn event_timer(transform_id: &str, fire: i64) -> TimerEntry {
        TimerEntry {
            key: TimerKey {
                transform_id: transform_id.to_string(),
                timer_family_id: "f".to_string(),
                tag: String::new(),
                window: "global".to_string(),
                user_key: b"k".to_vec(),
            },
            domain: TimeDomain::EventTime,
            fire_timestamp: fire,
            hold_timestamp: fire,
        }
    }

    fn gbk_node(name: &str) -> ExecutableNode {
        ExecutableNode::Runner(from_urn(
            beam_urns::GROUP_BY_KEY_TRANSFORM,
            name.to_string(),
            HashMap::new(),
            HashMap::new(),
        ))
    }

    fn dummy_metadata(name: &str) -> ConsumerMetaData {
        ConsumerMetaData {
            producer_transform_id: format!("producer-{}", name),
            produced_pcol_id: format!("pcol-{}", name),
            coder_id: format!("coder-{}", name),
            component_coder: None,
            consumer_transfrom_id: format!("consumer-{}", name),
        }
    }

    fn graph_for_test(
        graph: Graph<ExecutableNode, ConsumerMetaData>,
        root_metadata: ConsumerMetaData,
    ) -> ExecutableGraph {
        ExecutableGraph::from_graph_for_test(graph, root_metadata)
    }

    #[test]
    fn next_nodes_linear_chain() {
        let mut graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let a = graph.add_node(runner_node("A"));
        let b = graph.add_node(runner_node("B"));
        let c = graph.add_node(runner_node("C"));

        graph.add_edge(a, b, dummy_metadata("ab"));
        graph.add_edge(b, c, dummy_metadata("bc"));

        let executable_graph = graph_for_test(graph, dummy_metadata("root"));
        let mut scheduler = NodeScheduler::new(executable_graph);

        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, a);

        scheduler.mark_complete(a);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, b);

        scheduler.mark_complete(b);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, c);
    }

    #[test]
    fn next_nodes_fan_out() {
        let mut graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let a = graph.add_node(runner_node("A"));
        let b = graph.add_node(runner_node("B"));
        let c = graph.add_node(runner_node("C"));

        graph.add_edge(a, b, dummy_metadata("ab"));
        graph.add_edge(a, c, dummy_metadata("ac"));

        let executable_graph = graph_for_test(graph, dummy_metadata("root"));
        let mut scheduler = NodeScheduler::new(executable_graph);

        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, a);

        scheduler.mark_complete(a);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 2);
        let ready_indices: HashSet<_> = ready.into_iter().map(|(idx, _)| idx).collect();
        assert!(ready_indices.contains(&b));
        assert!(ready_indices.contains(&c));
    }

    #[test]
    fn next_nodes_fan_in_requires_both_parents() {
        let mut graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let a = graph.add_node(runner_node("A"));
        let b = graph.add_node(runner_node("B"));
        let c = graph.add_node(runner_node("C"));

        graph.add_edge(a, c, dummy_metadata("ac"));
        graph.add_edge(b, c, dummy_metadata("bc"));

        let executable_graph = graph_for_test(graph, dummy_metadata("root"));
        let mut scheduler = NodeScheduler::new(executable_graph);

        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 2);
        let ready_indices: HashSet<_> = ready.into_iter().map(|(idx, _)| idx).collect();
        assert!(ready_indices.contains(&a));
        assert!(ready_indices.contains(&b));

        scheduler.mark_complete(a);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 0);

        scheduler.mark_complete(b);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, c);
    }

    #[test]
    fn watermark_gated_aggregation_runs_only_after_the_source_completes() {
        use crate::engine::watermark::StageKind;

        let mut graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let source = graph.add_node(runner_node("source"));
        let aggregate = graph.add_node(gbk_node("aggregate"));
        graph.add_edge(source, aggregate, dummy_metadata("sa"));

        let executable_graph = graph_for_test(graph, dummy_metadata("root"));
        let aggregate_id = executable_graph.get_executable_graph()[aggregate].id();
        let mut scheduler = NodeScheduler::new(executable_graph);

        // The GroupByKey stage is gated at the global window end.
        assert_eq!(
            scheduler.watermarks().stage_kind(&aggregate_id),
            Some(StageKind::WatermarkGated)
        );

        // Only the source is ready; the aggregation waits for the watermark.
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, source);

        // Completing the source reports +inf, which is what satisfies the gate.
        scheduler.mark_complete(source);
        let ready = scheduler.next_nodes();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, aggregate);
    }

    /// Build `source -> stateful(SDK)`, returning the scheduler, the graph node
    /// indices and the two stage ids.
    fn source_and_stateful() -> (
        NodeScheduler,
        petgraph::graph::NodeIndex,
        petgraph::graph::NodeIndex,
        String,
        String,
    ) {
        let mut graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let source = graph.add_node(runner_node("source"));
        let stateful = graph.add_node(worker_stage_node("stateful"));
        graph.add_edge(source, stateful, dummy_metadata("ss"));

        let executable_graph = graph_for_test(graph, dummy_metadata("root"));
        let source_id = executable_graph.get_executable_graph()[source].id();
        let stateful_id = executable_graph.get_executable_graph()[stateful].id();
        (
            NodeScheduler::new(executable_graph),
            source,
            stateful,
            source_id,
            stateful_id,
        )
    }

    /// Beam fires an event-time timer only once the owning stage's input
    /// watermark has reached the timer's timestamp (`fire_timestamp <= WM`).
    #[test]
    fn event_time_timer_fires_only_once_the_stage_watermark_reaches_it() {
        let (mut scheduler, _source, _stateful, source_id, stateful_id) = source_and_stateful();
        let timer = event_timer("stateful", 100);

        // No watermark report yet: the source output is MIN, so nothing is due.
        assert!(
            scheduler
                .promote_due_event_time_timers(&[timer.clone()])
                .is_empty()
        );

        // WM = 99: still below the timer.
        scheduler
            .watermarks_mut()
            .report_source_watermark(&source_id, 99)
            .unwrap();
        scheduler.watermarks_mut().refresh();
        assert!(
            scheduler
                .promote_due_event_time_timers(&[timer.clone()])
                .is_empty()
        );

        // WM = 100: the watermark has reached the timer, so it fires (equal timestamps fire).
        scheduler
            .watermarks_mut()
            .report_source_watermark(&source_id, 100)
            .unwrap();
        scheduler.watermarks_mut().refresh();
        let promoted = scheduler.promote_due_event_time_timers(&[timer]);
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].0, stateful_id);
        assert_eq!(promoted[0].1.len(), 1);
    }

    /// Processing-time timers are never promoted by the watermark path.
    #[test]
    fn processing_time_timer_is_not_promoted_by_the_watermark() {
        let (mut scheduler, _source, _stateful, source_id, _stateful_id) = source_and_stateful();
        scheduler
            .watermarks_mut()
            .report_source_watermark(&source_id, 1_000)
            .unwrap();
        scheduler.watermarks_mut().refresh();

        let mut timer = event_timer("stateful", 100);
        timer.domain = TimeDomain::ProcessingTime;
        assert!(scheduler.promote_due_event_time_timers(&[timer]).is_empty());
    }

    /// A timer whose stage already has a bundle in flight is not promoted (the
    /// re-arm would be lost when that bundle completes).
    #[test]
    fn event_time_timer_for_an_in_flight_stage_is_not_promoted() {
        let (mut scheduler, source, stateful, _source_id, _stateful_id) = source_and_stateful();
        let timer = event_timer("stateful", 100);

        // Start and complete the source: +inf, which readies the stateful stage.
        let ready = scheduler.next_nodes();
        assert!(ready.iter().any(|(idx, _)| *idx == source));
        scheduler.mark_complete(source);

        // Start the stateful stage's bundle: it is now in flight.
        let ready = scheduler.next_nodes();
        assert!(ready.iter().any(|(idx, _)| *idx == stateful));
        assert!(
            scheduler
                .promote_due_event_time_timers(&[timer.clone()])
                .is_empty()
        );

        // Once it completes, the timer is promoted.
        scheduler.mark_complete(stateful);
        let promoted = scheduler.promote_due_event_time_timers(&[timer]);
        assert_eq!(promoted.len(), 1);
    }

    /// After a bounded source reports +inf, a timer re-armed by the fired
    /// callback (a new event-time timer) is due again and must drain.
    #[test]
    fn event_time_timers_drain_after_a_finished_source() {
        let (mut scheduler, source, stateful, _source_id, stateful_id) = source_and_stateful();

        // Start and complete the source: bounded end reports +inf.
        let ready = scheduler.next_nodes();
        assert!(ready.iter().any(|(idx, _)| *idx == source));
        scheduler.mark_complete(source);

        // First timer fires at +inf.
        let first = event_timer("stateful", 100);
        let promoted = scheduler.promote_due_event_time_timers(&[first]);
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].0, stateful_id);

        // Run the timer-only bundle (this consumes the re-arm).
        let ready = scheduler.next_nodes();
        assert!(ready.iter().any(|(idx, _)| *idx == stateful));
        scheduler.mark_complete(stateful);

        // The callback re-armed: a brand-new event-time timer, also at/below +inf.
        let rearmed = event_timer("stateful", 100);
        let promoted = scheduler.promote_due_event_time_timers(&[rearmed]);
        assert_eq!(
            promoted.len(),
            1,
            "a timer re-armed after +inf must still be promoted, not stranded"
        );
        assert_eq!(promoted[0].0, stateful_id);
    }

    /// End-to-end for the M6a gap: a completed consumer re-runs when its upstream
    /// appends again, and on that re-run it reads only the newly appended rows
    /// (incremental cursor), not the whole input again.
    #[tokio::test]
    async fn a_completed_consumer_reruns_and_reads_only_newly_appended_rows() {
        use crate::store::element_store::{FlareElementStore, ScanCollectionRequest};
        use crate::transforms::ExecutionContext;
        use std::sync::Arc;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let warehouse = dir.path().to_str().expect("utf8").to_string();
        let store = Arc::new(
            FlareElementStore::new(warehouse, "testdb".to_string(), None)
                .await
                .expect("store"),
        );

        // A (source, Impulse) -> p; B (Flatten) consumes p and emits q.
        let impulse = ExecutableNode::Runner(from_urn(
            beam_urns::IMPULSE_TRANSFORM,
            "A".to_string(),
            HashMap::new(),
            HashMap::from([("out".to_string(), "p".to_string())]),
        ));
        let flatten = ExecutableNode::Runner(from_urn(
            beam_urns::FLATTEN_TRANSFORM,
            "B".to_string(),
            HashMap::from([("in".to_string(), "p".to_string())]),
            HashMap::from([("out".to_string(), "q".to_string())]),
        ));
        let mut graph = Graph::new();
        let a = graph.add_node(impulse);
        let b = graph.add_node(flatten);
        graph.add_edge(
            a,
            b,
            ConsumerMetaData {
                producer_transform_id: "A".to_string(),
                produced_pcol_id: "p".to_string(),
                coder_id: "c".to_string(),
                component_coder: None,
                consumer_transfrom_id: "B".to_string(),
            },
        );

        let executable = graph_for_test(graph, dummy_metadata("root"));
        let a_stage = executable.get_executable_graph()[a].id();
        let mut scheduler = NodeScheduler::new(executable);

        // One scheduling round: run every ready node once, then mark it complete.
        // Runner transforms execute directly against the store (no SDK harness),
        // mirroring `StageExecutor`'s runner branch.
        async fn run_round(scheduler: &mut NodeScheduler, store: &Arc<FlareElementStore>) {
            for (idx, node) in scheduler.next_nodes() {
                let ExecutableNode::Runner(transform) = node else {
                    panic!("test graph is runner-only");
                };
                let inputs = scheduler
                    .input_edge_metadata(idx)
                    .iter()
                    .map(|meta| meta.produced_pcol_id.clone())
                    .collect();
                let output = scheduler
                    .output_edge_metadata(idx)
                    .map(|meta| meta.produced_pcol_id)
                    .or_else(|| transform.output_pcol_ids().into_iter().next())
                    .expect("runner output pcollection");
                transform
                    .execute(ExecutionContext {
                        store: store.clone(),
                        input_pcollection_ids: inputs,
                        output_pcollection_id: output,
                        consumer_transfrom_id: "test".to_string(),
                        stage_id: transform.id(),
                        input_watermark: i64::MAX,
                    })
                    .await
                    .expect("runner execute");
                scheduler.mark_complete(idx);
            }
        }

        async fn output_len(store: &Arc<FlareElementStore>) -> usize {
            store
                .scan_windowed_values(ScanCollectionRequest {
                    pcollection_id: "q".to_string(),
                })
                .await
                .expect("scan q")
                .len()
        }

        // First run to quiescence: A appends one element, B consumes it.
        let mut rounds = 0;
        while !scheduler.is_complete() {
            run_round(&mut scheduler, &store).await;
            rounds += 1;
            assert!(rounds < 10, "first run did not quiesce");
        }
        assert_eq!(output_len(&store).await, 1);

        // A re-runs (as a fired timer would): its append must re-wake the already
        // completed B, and B's incremental read must deliver only the new element.
        scheduler.watermarks_mut().mark_rerun(&a_stage).unwrap();
        assert!(!scheduler.is_complete());

        let mut rounds = 0;
        while !scheduler.is_complete() {
            run_round(&mut scheduler, &store).await;
            rounds += 1;
            assert!(
                rounds < 10,
                "re-run did not quiesce (consumer never woken?)"
            );
        }

        // 2, not 3: exactly one new element was appended, so B re-read only new
        // rows rather than re-emitting the whole input.
        assert_eq!(output_len(&store).await, 2);
    }
}
