use crate::fusion::refs::{SideInputRef, TimerRef, UserStateRef};
use crate::fusion::stage::{
    ExecutableStage, SplittableExecutionPlan, SplittableProcessKind, SplittableStage,
};
use crate::jobservice::urns;
use crate::transforms::{FlareRunnerTransform, from_urn_with_payload};
use crate::utils::errors::*;
use beam_model_rs::v1::executable_stage_payload::{SideInputId, TimerId, UserStateId};
use beam_model_rs::v1::{Components, Environment, PCollection, PTransform, ParDoPayload};
use indexmap::IndexSet;
use log::info;
use petgraph::{Graph, graph::NodeIndex};
use prost::Message;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct ExecutableGraph {
    graph: Graph<ExecutableNode, ConsumerMetaData>,
    node_indices: HashMap<String, NodeIndex>,
    pub(crate) components: Components,
    root_metadata: Option<ConsumerMetaData>,
}

#[derive(Clone)]
struct ConsumerLink {
    node: ExecutableNode,
    transform_id: String,
}

fn splittable_process_kind(stage: &ExecutableStage) -> Option<SplittableProcessKind> {
    stage.transforms().iter().find_map(|transform| {
        match transform.node().spec.as_ref()?.urn.as_str() {
            urns::beam_urns::SPLITTABLE_PROCESS_ELEMENTS_URN => {
                Some(SplittableProcessKind::ProcessElements)
            }
            urns::beam_urns::SPLITTABLE_PROCESS_KEYED_URN => {
                Some(SplittableProcessKind::ProcessKeyedElements)
            }
            urns::beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN => {
                Some(SplittableProcessKind::ProcessSizedElementsAndRestrictions)
            }
            _ => None,
        }
    })
}

impl ExecutableGraph {
    pub fn from(
        sdk_stages: IndexSet<ExecutableStage>,
        runner_stages: IndexSet<PTransformNode>,
        components: Components,
    ) -> Self {
        let mut ep = Self {
            graph: Graph::new(),
            node_indices: HashMap::new(),
            components: components.clone(),
            root_metadata: None,
        };

        ep.build_executable_graph(sdk_stages, runner_stages);
        ep
    }

    fn build_executable_graph(
        &mut self,
        sdk_stages: IndexSet<ExecutableStage>,
        runner_stages: IndexSet<PTransformNode>,
    ) {
        info!("Building graph");
        self.graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        self.node_indices.clear();

        // Map each PCollection to all of its consumer transform links. This
        // map is left uncollapsed; splittable stages are folded into `Splittable`
        // nodes lazily while walking downstream from the root.
        let mut consumer_map: HashMap<String, Vec<ConsumerLink>> = HashMap::new();
        let mut side_input_pcollections = HashSet::new();
        for stage in sdk_stages.iter() {
            for side_input in stage.side_inputs() {
                side_input_pcollections.insert(side_input.collection().id.clone());
            }
        }

        for stage in sdk_stages.iter() {
            let stage_input_id = stage.input_pcol().id.clone();
            let worker_node = ExecutableNode::Worker(stage.clone());

            for transform in stage.transforms() {
                if transform
                    .node()
                    .inputs
                    .values()
                    .any(|input| input == &stage_input_id)
                {
                    consumer_map
                        .entry(stage_input_id.clone())
                        .or_default()
                        .push(ConsumerLink {
                            node: worker_node.clone(),
                            transform_id: transform.node().unique_name.clone(),
                        });
                }
            }
        }

        for stage in runner_stages.iter() {
            if let Some(spec) = stage.node().spec.as_ref() {
                let runner_node = ExecutableNode::Runner(from_urn_with_payload(
                    &spec.urn,
                    stage.node().unique_name.clone(),
                    stage.node().inputs.clone(),
                    stage.node().outputs.clone(),
                    &spec.payload,
                ));

                for input_id in stage.node().inputs.values() {
                    consumer_map
                        .entry(input_id.clone())
                        .or_default()
                        .push(ConsumerLink {
                            node: runner_node.clone(),
                            transform_id: stage.node().unique_name.clone(),
                        });
                }
            }
        }

        // Memoizes the `Splittable` node built for a splittable run so every
        // member stage resolves to the same outer node.
        let mut splittable_cache: HashMap<String, ExecutableNode> = HashMap::new();

        let mut work_queue = VecDeque::<ExecutableNode>::new();
        // Tracks every node already scheduled for expansion. A node reachable
        // from more than one upstream branch (a fan-in such as a Flatten) must
        // only be expanded once, otherwise its outgoing edges are duplicated.
        let mut queued: HashSet<String> = HashSet::new();

        info!("Created work queue");
        // Seed the traversal from every root node, not just the first one. A
        // pipeline can have multiple roots (e.g. several `Create`/`Impulse`
        // sources whose outputs are later merged by a Flatten).
        for root in Self::get_roots(&runner_stages) {
            let Some(spec) = root.transform.spec.as_ref() else {
                continue;
            };
            let urn = &spec.urn;
            info!("Root node urn: {}", urn);
            info!("root outputs: {:?}", root.transform.outputs.clone());
            let root_node = ExecutableNode::Runner(from_urn_with_payload(
                urn,
                root.node().unique_name.clone(),
                root.transform.inputs.clone(),
                root.transform.outputs.clone(),
                &spec.payload,
            ));

            let runner_index = self.ensure_node_exists(&root_node);
            queued.insert(root_node.id());

            for (_output_key, output_id) in root.node().outputs.iter() {
                if let Some(consumer_links) = consumer_map.get(output_id).cloned() {
                    for consumer_link in consumer_links {
                        let consumer_node = self.resolve_consumer(
                            &consumer_link.node,
                            &sdk_stages,
                            &runner_stages,
                            &consumer_map,
                            &side_input_pcollections,
                            &mut splittable_cache,
                        );

                        let consumer_index = self.ensure_node_exists(&consumer_node);

                        let edge = self.build_consumer_metadata(
                            output_id,
                            &consumer_link.transform_id,
                            &sdk_stages,
                            &runner_stages,
                        );

                        self.root_metadata.get_or_insert_with(|| edge.clone());

                        self.graph.add_edge(runner_index, consumer_index, edge);
                        if queued.insert(consumer_node.id()) {
                            work_queue.push_back(consumer_node);
                        }
                    }
                }
            }
        }

        // Build rest of all downstream nodes (either a executable stage or a runner transfrom)
        //
        // A ExecutableStage can produce multiple outputs since ES is basically a set of
        // fused transforms and sometimes the satge might give out multiple PCollection outputs
        // cause there are downstream stages/runner transforms that consume those collections.
        // So, its not always per stage per output PCollection.
        while let Some(producer_node) = work_queue.pop_front() {
            // create or get node if it exists
            let producer_index = self.ensure_node_exists(&producer_node);

            // Itterate over the set of output PCollection that a stage(node) might produce
            for output_pcol in producer_node.output_pcols() {
                // get all consumer nodes of that PCollection
                if let Some(consumer_links) = consumer_map.get(&output_pcol).cloned() {
                    for consumer_link in consumer_links {
                        let consumer_node = self.resolve_consumer(
                            &consumer_link.node,
                            &sdk_stages,
                            &runner_stages,
                            &consumer_map,
                            &side_input_pcollections,
                            &mut splittable_cache,
                        );

                        // Skip links that were folded into the producer's own
                        // nested graph (fan-out from a splittable stage to both
                        // a splittable sibling and an external consumer).
                        if consumer_node.id() == producer_node.id() {
                            continue;
                        }

                        let consumer_index = self.ensure_node_exists(&consumer_node);

                        // Create edge metadata for this producer -> consumer edge.
                        let edge = self.build_consumer_metadata(
                            &output_pcol,
                            &consumer_link.transform_id,
                            &sdk_stages,
                            &runner_stages,
                        );

                        // Connect the producer and consumer nodes with PCollection metadata
                        self.graph.add_edge(producer_index, consumer_index, edge);
                        // Expand the consumer's downstream nodes exactly once.
                        if queued.insert(consumer_node.id()) {
                            work_queue.push_back(consumer_node);
                        }
                    }
                }
            }
        }
    }

    /// Return the outer-graph node for a consumer link.
    ///
    /// If the consumer is a splittable worker stage, return the `Splittable`
    /// node for the whole downstream run it belongs to (building it once and
    /// memoizing it in `cache`). Otherwise return the node unchanged.
    fn resolve_consumer(
        &self,
        node: &ExecutableNode,
        sdk_stages: &IndexSet<ExecutableStage>,
        runner_stages: &IndexSet<PTransformNode>,
        consumer_map: &HashMap<String, Vec<ConsumerLink>>,
        side_input_pcollections: &HashSet<String>,
        cache: &mut HashMap<String, ExecutableNode>,
    ) -> ExecutableNode {
        let ExecutableNode::Worker(stage) = node else {
            return node.clone();
        };

        if !stage.is_splittable() {
            return node.clone();
        }

        if let Some(existing) = cache.get(&stage.id()) {
            return existing.clone();
        }

        let (splittable, member_ids) = self.build_splittable_node(
            stage,
            sdk_stages,
            runner_stages,
            consumer_map,
            side_input_pcollections,
        );
        for member_id in member_ids {
            cache.insert(member_id, splittable.clone());
        }
        splittable
    }

    /// Build the nested graph for the run of splittable stages of a source transform after root.
    ///
    /// Follows downstream stages while they are splittable, and stops at the
    /// first regular stage. Any output that goes to a regular stage becomes a
    /// boundary output of the outer node.
    ///
    /// Returns the built node plus the IDs of every stage in the run.
    fn build_splittable_node(
        &self,
        start: &ExecutableStage,
        sdk_stages: &IndexSet<ExecutableStage>,
        runner_stages: &IndexSet<PTransformNode>,
        consumer_map: &HashMap<String, Vec<ConsumerLink>>,
        side_input_pcollections: &HashSet<String>,
    ) -> (ExecutableNode, Vec<String>) {
        let mut nested_graph = Graph::<ExecutableNode, ConsumerMetaData>::new();
        let mut nested_indices = HashMap::new();
        let mut boundary_outputs = HashSet::new();
        let mut pending = VecDeque::from([start.clone()]);
        let mut visited = HashSet::new();
        let mut member_ids = Vec::new();

        while let Some(stage) = pending.pop_front() {
            let stage_id = stage.id();
            if !visited.insert(stage_id.clone()) {
                continue;
            }

            // One SplittableStage execution runs exactly one
            // PairWithRestriction/SplitAndSize stage followed by one process
            // stage. A pipeline can contain consecutive chained SDFs (for
            // example, BoundedSourceAsSDFWrapper feeding a user SDF). Treat the
            // process output as a materialized boundary so the downstream SDF
            // becomes its own SplittableStage instead of being folded into a
            // run whose executor cannot represent it.
            let completes_splittable_run = splittable_process_kind(&stage).is_some();

            let stage_node = ExecutableNode::Worker(stage.clone());
            let stage_index = *nested_indices
                .entry(stage_id.clone())
                .or_insert_with(|| nested_graph.add_node(stage_node));
            member_ids.push(stage_id);

            for output_pcol in stage.get_output_pcol_ids() {
                let mut has_external_consumer = side_input_pcollections.contains(&output_pcol);

                if let Some(consumers) = consumer_map.get(&output_pcol) {
                    for consumer in consumers {
                        match &consumer.node {
                            ExecutableNode::Worker(next_stage)
                                if next_stage.is_splittable() && !completes_splittable_run =>
                            {
                                let next_id = next_stage.id();
                                let next_node = ExecutableNode::Worker(next_stage.clone());
                                let next_index = *nested_indices
                                    .entry(next_id.clone())
                                    .or_insert_with(|| nested_graph.add_node(next_node));
                                let edge = self.build_consumer_metadata(
                                    &output_pcol,
                                    &consumer.transform_id,
                                    sdk_stages,
                                    runner_stages,
                                );
                                nested_graph.add_edge(stage_index, next_index, edge);
                                pending.push_back(next_stage.clone());
                            }
                            _ => has_external_consumer = true,
                        }
                    }
                }

                if has_external_consumer {
                    boundary_outputs.insert(output_pcol);
                }
            }
        }

        member_ids.sort();
        let id = format!("splittable:{}", member_ids.join(","));
        let plan = Self::build_splittable_execution_plan(&id, &nested_graph);
        let splittable = ExecutableNode::Splittable(SplittableStage::new(
            id,
            nested_graph,
            boundary_outputs,
            plan,
        ));

        (splittable, member_ids)
    }

    fn build_splittable_execution_plan(
        stage_id: &str,
        nested_graph: &Graph<ExecutableNode, ConsumerMetaData>,
    ) -> SplittableExecutionPlan {
        let sorted = petgraph::algo::toposort(nested_graph, None).unwrap_or_else(|_| {
            panic!(
                "splittable stage {} has a cycle in its nested graph",
                stage_id
            );
        });

        let mut initialization_stages = Vec::new();
        let mut process_stage = None;
        let mut process_kind = None;

        for idx in sorted {
            let node = &nested_graph[idx];
            if let ExecutableNode::Worker(stage) = node {
                if let Some(kind) = splittable_process_kind(stage) {
                    process_stage = Some(stage.clone());
                    process_kind = Some(kind);
                } else {
                    // Initialization may span more than one stage. In
                    // particular, side-input-bearing SDFs keep
                    // PairWithRestriction and SplitAndSize in separate SDK
                    // stages, so both must run in topological order before the
                    // restriction bytes are handed to the process stage.
                    initialization_stages.push(stage.clone());
                }
            }
        }

        assert!(
            !initialization_stages.is_empty(),
            "SDF splittable stage {} missing initialization stage",
            stage_id
        );
        let process_stage = process_stage.unwrap_or_else(|| {
            panic!("SDF splittable stage {} missing process stage", stage_id);
        });
        let process_kind = process_kind.unwrap_or_else(|| {
            panic!(
                "SDF splittable stage {} missing process transform URN",
                stage_id
            );
        });

        SplittableExecutionPlan {
            initialization_stages,
            process_stage,
            process_kind,
        }
    }

    fn ensure_node_exists(&mut self, node: &ExecutableNode) -> NodeIndex {
        if let Some(&index) = self.node_indices.get(&node.id()) {
            return index;
        }

        let index = self.graph.add_node(node.clone());

        self.node_indices.insert(node.id().clone(), index);

        index
    }

    fn get_roots(runner_stages: &IndexSet<PTransformNode>) -> Vec<&PTransformNode> {
        info!("fetching root nodes");
        let roots: Vec<&PTransformNode> = runner_stages
            .iter()
            .filter(|pt| pt.node().inputs.is_empty())
            .collect();
        for pt in &roots {
            info!("Root transfrom: {}", pt.id);
        }
        roots
    }

    pub fn get_root_metadata(&self) -> &ConsumerMetaData {
        self.root_metadata
            .as_ref()
            .expect("Root metadata not initialized")
    }

    fn build_consumer_metadata(
        &self,
        output_pcol: &String,
        consumer_transform_id: &String,
        sdk_stages: &IndexSet<ExecutableStage>,
        runner_stages: &IndexSet<PTransformNode>,
    ) -> ConsumerMetaData {
        let producer_pt_id = Self::get_producer_transform(sdk_stages, runner_stages, output_pcol);

        let pcollection = self
            .components
            .pcollections
            .get(output_pcol)
            .expect("Output PCollection not found");

        let component_coder_ids = self
            .components
            .coders
            .get(&pcollection.coder_id)
            .map(|coder| coder.component_coder_ids.clone());

        ConsumerMetaData {
            producer_transform_id: producer_pt_id,
            produced_pcol_id: output_pcol.clone(),
            coder_id: pcollection.coder_id.clone(),
            component_coder: component_coder_ids,
            consumer_transfrom_id: consumer_transform_id.clone(),
        }
    }

    fn get_producer_transform(
        sdk_stages: &IndexSet<ExecutableStage>,
        runner_stages: &IndexSet<PTransformNode>,
        output_pcol: &str,
    ) -> String {
        sdk_stages
            .iter()
            .flat_map(|stage| stage.transforms())
            .find(|transform| {
                transform
                    .node()
                    .outputs
                    .values()
                    .any(|output| output == output_pcol)
            })
            .map(|transform| transform.node().unique_name.clone())
            .or_else(|| {
                runner_stages
                    .iter()
                    .find(|transform| {
                        transform
                            .node()
                            .outputs
                            .values()
                            .any(|output| output == output_pcol)
                    })
                    .map(|transform| transform.node().unique_name.clone())
            })
            .expect("Producer transform not found")
    }

    pub fn get_executable_graph(&self) -> &Graph<ExecutableNode, ConsumerMetaData> {
        &self.graph
    }

    /// Metadata for a PCollection consumed only as a side input. Such a
    /// PCollection has no ordinary graph edge, but its producer still needs an
    /// output metadata entry so bundle output is decoded and committed.
    pub(crate) fn side_input_output_metadata(
        &self,
        pcollection_id: &str,
    ) -> Option<ConsumerMetaData> {
        for node in self.graph.node_weights() {
            for stage in node.worker_stages() {
                if let Some(side_input) = stage
                    .side_inputs()
                    .iter()
                    .find(|side| side.collection().id == pcollection_id)
                {
                    let pcollection = self.components.pcollections.get(pcollection_id)?;
                    return Some(ConsumerMetaData {
                        producer_transform_id: node.id(),
                        produced_pcol_id: pcollection_id.to_string(),
                        coder_id: pcollection.coder_id.clone(),
                        component_coder: self
                            .components
                            .coders
                            .get(&pcollection.coder_id)
                            .map(|coder| coder.component_coder_ids.clone()),
                        consumer_transfrom_id: side_input.transform().node().unique_name.clone(),
                    });
                }
            }
        }
        None
    }

    #[cfg(test)]
    pub fn from_graph_for_test(
        graph: Graph<ExecutableNode, ConsumerMetaData>,
        root_metadata: ConsumerMetaData,
    ) -> Self {
        Self::from_graph_with_components_for_test(graph, Components::default(), root_metadata)
    }

    #[cfg(test)]
    pub fn from_graph_with_components_for_test(
        graph: Graph<ExecutableNode, ConsumerMetaData>,
        components: Components,
        root_metadata: ConsumerMetaData,
    ) -> Self {
        let mut node_indices = HashMap::new();
        for idx in graph.node_indices() {
            node_indices.insert(graph[idx].id(), idx);
        }

        Self {
            graph,
            node_indices,
            components,
            root_metadata: Some(root_metadata),
        }
    }
}

#[derive(Clone)]
pub enum ExecutableNode {
    Worker(ExecutableStage),
    Runner(FlareRunnerTransform),
    Splittable(SplittableStage),
}

impl ExecutableNode {
    pub fn debug_label(&self) -> String {
        match self {
            ExecutableNode::Worker(stage) => {
                let mut transform_names: Vec<_> = stage
                    .transforms()
                    .iter()
                    .map(|transform| transform.node().unique_name.clone())
                    .collect();
                transform_names.sort();

                let mut output_pcols: Vec<_> = stage
                    .output_pcols()
                    .iter()
                    .map(|pcollection| pcollection.id.clone())
                    .collect();
                output_pcols.sort();

                format!(
                    "Worker id={} input_pcol={} transforms={:?} output_pcols={:?}",
                    stage.id(),
                    stage.input_pcol().id,
                    transform_names,
                    output_pcols,
                )
            }
            ExecutableNode::Runner(transform) => {
                let mut output_pcols: Vec<_> = transform.output_pcol_ids().into_iter().collect();
                output_pcols.sort();

                format!(
                    "Runner id={} name={} output_pcols={:?}",
                    transform.id(),
                    transform.unique_name(),
                    output_pcols,
                )
            }
            ExecutableNode::Splittable(stage) => {
                let mut output_pcols: Vec<_> = stage.output_pcols().iter().cloned().collect();
                output_pcols.sort();
                format!(
                    "Splittable id={} stages={} output_pcols={:?}",
                    stage.id(),
                    stage.graph().node_count(),
                    output_pcols,
                )
            }
        }
    }

    pub fn output_pcols(&self) -> HashSet<String> {
        match self {
            ExecutableNode::Worker(s) => s.get_output_pcol_ids(),
            ExecutableNode::Runner(r) => r.output_pcol_ids(),
            ExecutableNode::Splittable(s) => s.output_pcols().clone(),
        }
    }

    /// Worker stages represented by this executable node, including the stages
    /// folded into a splittable run.
    pub(crate) fn worker_stages(&self) -> Vec<&ExecutableStage> {
        fn collect<'a>(node: &'a ExecutableNode, stages: &mut Vec<&'a ExecutableStage>) {
            match node {
                ExecutableNode::Worker(stage) => stages.push(stage),
                ExecutableNode::Splittable(splittable) => {
                    for nested in splittable.graph().node_weights() {
                        collect(nested, stages);
                    }
                }
                ExecutableNode::Runner(_) => {}
            }
        }

        let mut stages = Vec::new();
        collect(self, &mut stages);
        stages
    }

    pub fn id(&self) -> String {
        match self {
            ExecutableNode::Worker(e) => e.id(),
            ExecutableNode::Runner(r) => r.id(),
            ExecutableNode::Splittable(s) => s.id().to_string(),
        }
    }
}

impl Hash for ExecutableNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ExecutableNode::Worker(s) => s.id().hash(state),
            ExecutableNode::Runner(r) => r.id().hash(state),
            ExecutableNode::Splittable(s) => s.id().hash(state),
        }
    }

    fn hash_slice<H: Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        for piece in data {
            piece.hash(state)
        }
    }
}

impl PartialEq for ExecutableNode {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ExecutableNode::Worker(a), ExecutableNode::Worker(b)) => a.id() == b.id(),
            (ExecutableNode::Runner(a), ExecutableNode::Runner(b)) => a.id() == b.id(),
            (ExecutableNode::Splittable(a), ExecutableNode::Splittable(b)) => a.id() == b.id(),
            _ => false,
        }
    }
}

impl Eq for ExecutableNode {}

#[derive(Clone, Debug)]
pub struct ConsumerMetaData {
    pub(crate) producer_transform_id: String,
    pub(crate) produced_pcol_id: String,
    pub(crate) coder_id: String,
    pub(crate) component_coder: Option<Vec<String>>,
    pub(crate) consumer_transfrom_id: String,
}

#[derive(Debug, Clone)]
pub struct FusedPipeline {
    /// Components after single-producer deduplication: the original pipeline
    /// components plus the synthetic "partial" PCollections and merging
    /// Flattens that `ensure_single_producer` injected.
    components: Components,
    sdk_stages: IndexSet<ExecutableStage>,
    runner_stages: IndexSet<PTransformNode>,
    //requirements: HashSet<String>,
}

impl FusedPipeline {
    pub fn of(
        components: Components,
        sdk_stages: IndexSet<ExecutableStage>,
        runner_stages: IndexSet<PTransformNode>,
        //requirements: HashSet<String>,
    ) -> Self {
        Self {
            components,
            sdk_stages,
            runner_stages,
            // requirements,
        }
    }

    pub fn components(&self) -> &Components {
        &self.components
    }

    pub fn sdk_stages(&self) -> &IndexSet<ExecutableStage> {
        &self.sdk_stages
    }

    pub fn runner_stages(&self) -> &IndexSet<PTransformNode> {
        &self.runner_stages
    }
}
pub struct QueryablePipeline {
    graph: Graph<PipelineNode, PipelineEdge>,
    transform_ids: HashMap<String, NodeIndex>,
    pcollection_ids: HashMap<String, NodeIndex>,
    components: Components,
    primitives: HashSet<String>,
}
impl QueryablePipeline {
    pub fn new(comps: &Components) -> Self {
        let mut qp = Self {
            graph: Graph::new(),
            transform_ids: HashMap::new(),
            pcollection_ids: HashMap::new(),
            components: comps.clone(),
            primitives: get_primitives(comps),
        };
        qp.build_graph();
        qp
    }

    pub fn build_graph(&mut self) -> &mut Self {
        self.graph = Graph::<PipelineNode, PipelineEdge>::new();
        let mut unproduced_collections = HashSet::<String>::new();
        self.pcollection_ids.clear();
        self.transform_ids.clear();

        for id in self.primitives.iter() {
            if let Some(transform) = self.components.transforms.get(id) {
                let transform_idx = self.graph.add_node(PipelineNode::Transform(PTransformNode {
                    id: id.clone(),
                    transform: transform.clone(),
                }));

                self.transform_ids.insert(id.clone(), transform_idx);

                for (_output_key, output_id) in transform.outputs.iter() {
                    if let Some(produced_collection) = self.components.pcollections.get(output_id) {
                        let produced_idx = if let Some(&idx) = self.pcollection_ids.get(output_id) {
                            idx // already created as someone else's input — reuse it
                        } else {
                            let idx =
                                self.graph
                                    .add_node(PipelineNode::Collection(PCollectionNode {
                                        id: output_id.clone(),
                                        collection: produced_collection.clone(),
                                    }));
                            self.pcollection_ids.insert(output_id.clone(), idx);
                            idx
                        };
                        self.graph
                            .add_edge(transform_idx, produced_idx, PipelineEdge::PerElement);
                        unproduced_collections.remove(output_id);

                        // throw error if more than one coll node is present for a transfrom node.
                    }
                }

                for (input_key, input_id) in transform.inputs.iter() {
                    if let Some(consumed_collection) = self.components.pcollections.get(input_id) {
                        let consumed_idx = if let Some(&idx) = self.pcollection_ids.get(input_id) {
                            idx // already created as someone's output — reuse it
                        } else {
                            let idx: NodeIndex =
                                self.graph
                                    .add_node(PipelineNode::Collection(PCollectionNode {
                                        id: input_id.clone(),
                                        collection: consumed_collection.clone(),
                                    }));
                            self.pcollection_ids.insert(input_id.clone(), idx);
                            unproduced_collections.insert(input_id.clone());
                            idx
                        };
                        if let Ok(side_inputs) = get_local_side_input_names(transform) {
                            let edge = if side_inputs.contains(input_key) {
                                PipelineEdge::Singleton //side input edge
                            } else {
                                PipelineEdge::PerElement
                            };

                            self.graph.add_edge(consumed_idx, transform_idx, edge);
                        }
                    }
                }
            }
        }
        self
    }

    pub fn get_root_transforms(&self) -> HashSet<PTransformNode> {
        self.graph
            .node_indices()
            .filter(|&node_idx| {
                self.graph
                    .neighbors_directed(node_idx, petgraph::Direction::Incoming)
                    .next()
                    .is_none()
            })
            .filter_map(|node_idx| {
                if let PipelineNode::Transform(transform) = &self.graph[node_idx] {
                    Some(transform.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_per_element_consumers(
        &self,
        collection: &PCollectionNode,
    ) -> HashSet<PTransformNode> {
        if let Some(&col_idx) = self.pcollection_ids.get(&collection.id) {
            self.graph
                .neighbors(col_idx) // Get all successor nodes
                .filter(|consumer_idx| {
                    // Check if there's a PerElement edge connecting them
                    self.graph
                        .edges_connecting(col_idx, *consumer_idx)
                        .any(|edge| matches!(edge.weight(), PipelineEdge::PerElement))
                })
                .filter_map(|consumer_idx| {
                    // Only extract Transform nodes, ignore Collections
                    if let PipelineNode::Transform(transform) = &self.graph[consumer_idx] {
                        Some(transform.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            HashSet::new()
        }
    }

    pub fn get_singleton_consumers(&self, collection: &PCollectionNode) -> HashSet<PTransformNode> {
        if let Some(&col_idx) = self.pcollection_ids.get(&collection.id) {
            self.graph
                .neighbors(col_idx)
                .filter(|consumer_idx| {
                    self.graph
                        .edges_connecting(col_idx, *consumer_idx)
                        .any(|edge| matches!(edge.weight(), PipelineEdge::Singleton))
                })
                .filter_map(|consumer_idx| {
                    if let PipelineNode::Transform(transform) = &self.graph[consumer_idx] {
                        Some(transform.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            HashSet::new()
        }
    }

    pub fn get_output_pcol(&self, transfrom: &PTransformNode) -> HashSet<PCollectionNode> {
        if let Some(&node_idx) = self.transform_ids.get(&transfrom.id) {
            // Only follow outgoing edges
            self.graph
                .neighbors_directed(node_idx, petgraph::Direction::Outgoing)
                .filter_map(|neighbor_idx| {
                    if let PipelineNode::Collection(collection) = &self.graph[neighbor_idx] {
                        Some(collection.clone())
                    } else {
                        None
                    }
                })
                .collect::<HashSet<_>>()
        } else {
            HashSet::new()
        }
    }

    /*fn get_environment(&self, transform_id: &String) -> Option<String> {
        self.components
            .transforms
            .get(transform_id)
            .and_then(|t| Some(t.environment_id.clone()))
    }*/

    pub fn get_environment(&self, transform: &PTransform) -> Option<Environment> {
        // A runner-owned primitive executes in FlareDB, not an SDK harness, even
        // when the SDK attaches an environment to it. Without this,
        // it would be fused into an SDK stage and having no runner root the
        // executable graph would come out empty at runtime.
        if let Some(spec) = transform.spec.as_ref() {
            let flatten_requires_sdk_transcoding = spec.urn == urns::beam_urns::FLATTEN_TRANSFORM
                && self.flatten_has_different_input_and_output_coders(transform);
            if urns::beam_urns::FLARE.contains(&spec.urn.as_str())
                && !flatten_requires_sdk_transcoding
            {
                return None;
            }
        }
        self.components
            .environments
            .get(&transform.environment_id)
            .cloned()
    }

    /// A native Flatten copies logical values unchanged. That is only valid when
    /// the output uses the same coder as every input. If an SDK assigns a
    /// different coder (for example an SDK-private `SerializableCoder`), the
    /// harness must transcode the values so its input can decode them.
    fn flatten_has_different_input_and_output_coders(&self, transform: &PTransform) -> bool {
        let Some(output_id) = transform.outputs.values().next() else {
            return false;
        };
        let Some(output_coder_id) = self
            .components
            .pcollections
            .get(output_id)
            .map(|pcollection| pcollection.coder_id.as_str())
        else {
            return false;
        };

        transform.inputs.values().any(|input_id| {
            self.components
                .pcollections
                .get(input_id)
                .is_some_and(|pcollection| pcollection.coder_id != output_coder_id)
        })
    }

    pub fn get_side_inputs(
        &self,
        transform: &PTransformNode,
    ) -> Result<HashSet<SideInputRef>, BeamTranslationError> {
        get_local_side_input_names(&transform.transform)?
            .into_iter()
            .map(|local_name| {
                SideInputRef::from_id(
                    &SideInputId {
                        transform_id: transform.id.clone(),
                        local_name,
                    },
                    &self.components,
                )
            })
            .collect()
    }

    pub fn get_user_states(
        &self,
        transform: &PTransformNode,
    ) -> Result<HashSet<UserStateRef>, BeamTranslationError> {
        get_local_user_state_names(&transform.transform)?
            .into_iter()
            .map(|local_name| {
                UserStateRef::from_id(
                    &UserStateId {
                        transform_id: transform.id.clone(),
                        local_name,
                    },
                    &self.components,
                )
            })
            .collect()
    }

    pub fn get_timers(
        &self,
        transform: &PTransformNode,
    ) -> Result<HashSet<TimerRef>, BeamTranslationError> {
        get_local_timer_names(&transform.transform)?
            .into_iter()
            .map(|local_name| {
                TimerRef::from_id(
                    &TimerId {
                        transform_id: transform.id.clone(),
                        local_name,
                    },
                    &self.components,
                )
            })
            .collect()
    }

    pub fn transform_ids(&self) -> &HashMap<String, NodeIndex> {
        &self.transform_ids
    }

    pub fn graph(&self) -> &Graph<PipelineNode, PipelineEdge> {
        &self.graph
    }

    pub fn components(&self) -> &Components {
        &self.components
    }
}

pub enum PipelineNode {
    Transform(PTransformNode),
    Collection(PCollectionNode),
}

#[derive(Clone, Debug)]
pub struct PTransformNode {
    pub(crate) id: String,
    pub(crate) transform: PTransform,
}
impl Hash for PTransformNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl Eq for PTransformNode {}

impl PartialEq for PTransformNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PTransformNode {
    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn node(&self) -> &PTransform {
        &self.transform
    }
}

#[derive(Clone, Debug)]
pub struct PCollectionNode {
    pub(crate) id: String,
    pub(crate) collection: PCollection,
}

impl Hash for PCollectionNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Eq for PCollectionNode {}

impl PartialEq for PCollectionNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl PCollectionNode {
    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn node(&self) -> &PCollection {
        &self.collection
    }
}

pub enum PipelineEdge {
    PerElement, // Standard flow
    Singleton,  // Side Input (Broadcast)
    Timer,      // Timer Input (Self-loop)
}

fn get_primitives(components: &Components) -> HashSet<String> {
    //let mut ids: Vec<String> = Vec::new();
    let mut primitives_ids = HashSet::new();

    for transform in components.transforms.iter() {
        if is_primitive(transform.1) {
            let mut deque = VecDeque::<&String>::new();
            deque.push_front(transform.0);

            while let Some(id) = deque.pop_front() {
                let next = components.transforms.get(id);

                if let Some(transform) = next {
                    if transform.subtransforms.is_empty() {
                        primitives_ids.insert(id.clone());
                    } else {
                        for subtransform_id in transform.subtransforms.iter() {
                            deque.push_front(subtransform_id);
                        }
                    }
                }
            }
        }
    }
    //ids.into_iter().collect()
    primitives_ids
}

fn is_primitive(transform: &PTransform) -> bool {
    if let Some(spec) = &transform.spec {
        urns::beam_urns::PRIMITIVES.contains(&spec.urn.as_str())
    } else {
        false
    }
}

fn get_local_side_input_names(
    transform: &PTransform,
) -> Result<HashSet<String>, BeamTranslationError> {
    let Some(spec) = transform.spec.as_ref() else {
        return Ok(HashSet::new());
    };

    if !urns::beam_urns::VALID_SIDE_INPUT_URNS.contains(&spec.urn.as_str()) {
        return Ok(HashSet::new());
    }

    ParDoPayload::decode(spec.payload.as_slice())
        .map(|payload| payload.side_inputs.into_keys().collect())
        .map_err(|e| {
            BeamTranslationError::InvalidArgument(format!("Failed to decode ParDoPayload: {e}"))
        })
}

fn get_local_user_state_names(
    transform: &PTransform,
) -> Result<HashSet<String>, BeamTranslationError> {
    let Some(spec) = transform.spec.as_ref() else {
        return Ok(HashSet::new());
    };

    if spec.urn != urns::beam_urns::PAR_DO_TRANSFORM {
        return Ok(HashSet::new());
    }
    ParDoPayload::decode(spec.payload.as_slice())
        .map(|payload| payload.state_specs.into_keys().collect())
        .map_err(|e| {
            BeamTranslationError::InvalidArgument(format!("Failed to decode ParDoPayload: {e}"))
        })
}

fn get_local_timer_names(transform: &PTransform) -> Result<HashSet<String>, BeamTranslationError> {
    let Some(spec) = transform.spec.as_ref() else {
        return Ok(HashSet::new());
    };

    if spec.urn != urns::beam_urns::PAR_DO_TRANSFORM {
        return Ok(HashSet::new());
    }

    ParDoPayload::decode(spec.payload.as_slice())
        .map(|payload| payload.timer_family_specs.into_keys().collect())
        .map_err(|e| {
            BeamTranslationError::InvalidArgument(format!("Failed to decode ParDoPayload: {e}"))
        })
}

#[cfg(test)]
mod sdf_execution_tests {
    use super::*;
    use beam_model_rs::v1::FunctionSpec;

    fn pcollection(id: &str) -> PCollectionNode {
        PCollectionNode {
            id: id.to_string(),
            collection: PCollection {
                unique_name: id.to_string(),
                coder_id: "coder".to_string(),
                ..Default::default()
            },
        }
    }

    fn transform(
        id: &str,
        urn: &str,
        input: &PCollectionNode,
        output: &PCollectionNode,
    ) -> PTransformNode {
        PTransformNode {
            id: id.to_string(),
            transform: PTransform {
                unique_name: id.to_string(),
                spec: Some(FunctionSpec {
                    urn: urn.to_string(),
                    payload: Vec::new(),
                }),
                inputs: HashMap::from([("in".to_string(), input.id.clone())]),
                outputs: HashMap::from([("out".to_string(), output.id.clone())]),
                environment_id: "env".to_string(),
                ..Default::default()
            },
        }
    }

    fn stage(
        input: &PCollectionNode,
        output: &PCollectionNode,
        transform: PTransformNode,
    ) -> ExecutableStage {
        ExecutableStage::from(
            Components::default(),
            Environment::default(),
            HashSet::new(),
            input.clone(),
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            [output.clone()].into_iter().collect(),
            [transform].into_iter().collect(),
        )
    }

    fn link(stage: &ExecutableStage, transform_id: &str) -> ConsumerLink {
        ConsumerLink {
            node: ExecutableNode::Worker(stage.clone()),
            transform_id: transform_id.to_string(),
        }
    }

    #[test]
    fn chained_sdfs_become_separate_runs_and_initialize_from_pair_stage() {
        let p0 = pcollection("p0");
        let p1 = pcollection("p1");
        let p2 = pcollection("p2");
        let p3 = pcollection("p3");
        let p4 = pcollection("p4");
        let p5 = pcollection("p5");
        let p6 = pcollection("p6");

        let pair1_transform = transform(
            "pair1",
            urns::beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
            &p0,
            &p1,
        );
        let split1_transform = transform(
            "split1",
            urns::beam_urns::SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
            &p1,
            &p2,
        );
        let process1_transform = transform(
            "process1",
            urns::beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
            &p2,
            &p3,
        );
        let pair2_transform = transform(
            "pair2",
            urns::beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
            &p3,
            &p4,
        );
        let split2_transform = transform(
            "split2",
            urns::beam_urns::SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
            &p4,
            &p5,
        );
        let process2_transform = transform(
            "process2",
            urns::beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
            &p5,
            &p6,
        );

        let pair1 = stage(&p0, &p1, pair1_transform);
        let split1 = stage(&p1, &p2, split1_transform);
        let process1 = stage(&p2, &p3, process1_transform);
        let pair2 = stage(&p3, &p4, pair2_transform);
        let split2 = stage(&p4, &p5, split2_transform);
        let process2 = stage(&p5, &p6, process2_transform);

        let sdk_stages: IndexSet<_> = [pair1.clone(), split1.clone(), process1.clone()]
            .into_iter()
            .collect();
        let consumer_map = HashMap::from([
            ("p1".to_string(), vec![link(&split1, "split1")]),
            ("p2".to_string(), vec![link(&process1, "process1")]),
            ("p3".to_string(), vec![link(&pair2, "pair2")]),
            ("p4".to_string(), vec![link(&split2, "split2")]),
            ("p5".to_string(), vec![link(&process2, "process2")]),
        ]);
        let components = Components {
            pcollections: [p0, p1, p2, p3, p4, p5, p6]
                .into_iter()
                .map(|pcollection| (pcollection.id().clone(), pcollection.node().clone()))
                .collect(),
            ..Default::default()
        };
        let graph = ExecutableGraph {
            graph: Graph::new(),
            node_indices: HashMap::new(),
            components,
            root_metadata: None,
        };

        let (node, members) = graph.build_splittable_node(
            &pair1,
            &sdk_stages,
            &IndexSet::new(),
            &consumer_map,
            &HashSet::new(),
        );

        assert_eq!(
            members.len(),
            3,
            "only the first SDF chain should be folded"
        );
        let ExecutableNode::Splittable(splittable) = node else {
            panic!("SDF chain should produce a Splittable node");
        };
        assert_eq!(splittable.graph().node_count(), 3);
        assert_eq!(
            splittable.plan().initialization_stages.len(),
            2,
            "PairWithRestriction and SplitAndSize must both initialize the SDF"
        );
        assert_eq!(
            splittable.plan().initialization_stages[0].input_pcol().id(),
            "p0"
        );
        assert!(
            splittable.plan().initialization_stages[0]
                .transforms()
                .iter()
                .any(|transform| transform.id() == "pair1"),
            "the first initialization stage must be PairWithRestriction"
        );
        assert!(
            splittable.plan().initialization_stages[1]
                .transforms()
                .iter()
                .any(|transform| transform.id() == "split1"),
            "the second initialization stage must be SplitAndSize"
        );
        assert_eq!(
            splittable.plan().initialization_stages[1].input_pcol().id(),
            "p1",
            "SplitAndSize must consume PairWithRestriction output"
        );
        assert_eq!(splittable.plan().process_stage.input_pcol().id(), "p2");
        assert_eq!(
            splittable.output_pcols(),
            &HashSet::from(["p3".to_string()]),
            "the first process output must become the boundary to the next SDF"
        );
    }
}
