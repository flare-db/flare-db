use crate::errors::BeamTranslationError;
use crate::fusion::refs::{SideInputRef, TimerRef, UserStateRef};
use crate::fusion::stage::ExecutableStage;
use crate::jobservice::urns;
use crate::transforms::{FlareRunnerTransform, from_urn};
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

        // Map each PCollection to all of its consumer transform links.
        let mut consumer_map: HashMap<String, Vec<ConsumerLink>> = HashMap::new();

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
                let runner_node = ExecutableNode::Runner(from_urn(
                    &spec.urn,
                    stage.node().unique_name.clone(),
                    stage.node().inputs.clone(),
                    stage.node().outputs.clone(),
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

        let mut work_queue = VecDeque::<ExecutableNode>::new();
        info!("Created work queue");
        // Create Root node and immidate consumer pair
        if let Some(root) = Self::get_root(&runner_stages) {
            if let Some(spec) = root.transform.spec.as_ref() {
                let urn = &spec.urn;
                info!("Root node urn: {}", urn);
                let root_node = ExecutableNode::Runner(from_urn(
                    urn,
                    root.node().unique_name.clone(),
                    root.transform.inputs.clone(),
                    root.transform.outputs.clone(),
                ));

                let runner_index = self.graph.add_node(root_node.clone());

                self.node_indices
                    .insert(root_node.id().clone(), runner_index);

                for (_output_key, output_id) in root.node().outputs.iter() {
                    if let Some(consumer_links) = consumer_map.get(output_id).cloned() {
                        for consumer_link in consumer_links {
                            let consumer_index = self.ensure_node_exists(&consumer_link.node);

                            let edge = self.build_consumer_metadata(
                                output_id,
                                &consumer_link.transform_id,
                                &sdk_stages,
                                &runner_stages,
                            );

                            self.root_metadata.get_or_insert_with(|| edge.clone());

                            self.graph.add_edge(runner_index, consumer_index, edge);
                            work_queue.push_back(consumer_link.node);
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
                        let consumer_index = self.ensure_node_exists(&consumer_link.node);

                        // Create edge metadata for this producer -> consumer edge.
                        let edge = self.build_consumer_metadata(
                            &output_pcol,
                            &consumer_link.transform_id,
                            &sdk_stages,
                            &runner_stages,
                        );

                        // Connect the producer and consumer nodes with PCollection metadata
                        self.graph.add_edge(producer_index, consumer_index, edge);
                        // Add consumer node to queue to itterate and do the same for its downstream nodes.
                        work_queue.push_back(consumer_link.node);
                    }
                }
            }
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

    fn get_root(runner_stages: &IndexSet<PTransformNode>) -> Option<&PTransformNode> {
        info!("fetching root node");
        for pt in runner_stages.iter() {
            if pt.node().inputs.is_empty() {
                info!("Root transfrom: {}", pt.id);
                return Some(pt);
            }
        }
        None
        /* `PTransformNode` value */
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

        ConsumerMetaData {
            producer_transform_id: producer_pt_id,
            produced_pcol_id: output_pcol.clone(),
            coder_id: pcollection.coder_id.clone(),
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
}

#[derive(Clone)]
pub enum ExecutableNode {
    Worker(ExecutableStage),
    Runner(FlareRunnerTransform),
}

impl ExecutableNode {
    pub fn output_pcols(&self) -> HashSet<String> {
        match self {
            ExecutableNode::Worker(s) => s.get_output_pcol_ids(),
            ExecutableNode::Runner(r) => r.output_pcol_ids(),
        }
    }

    pub fn id(&self) -> String {
        match self {
            ExecutableNode::Worker(e) => e.id(),
            ExecutableNode::Runner(r) => r.id(),
        }
    }
}

impl Hash for ExecutableNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ExecutableNode::Worker(s) => s.id().hash(state),
            ExecutableNode::Runner(r) => r.id().hash(state),
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
    pub(crate) consumer_transfrom_id: String,
}

#[derive(Debug, Clone)]
pub struct FusedPipeline {
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
            self.graph
                .neighbors(node_idx)
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
        return self
            .components
            .environments
            .get(&transform.environment_id)
            .cloned();
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
