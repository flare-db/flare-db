use beam_model_rs::v1::executable_stage_payload::{SideInputId, TimerId, UserStateId};
use beam_model_rs::v1::{Components, Environment, PCollection, PTransform, ParDoPayload};
use indexmap::{IndexMap, IndexSet};
use petgraph::{Graph, graph::NodeIndex};
use prost::Message;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use uuid::Uuid;

use crate::errors::BeamTranslationError;
use crate::fusion::refs::{SideInputRef, TimerRef, UserStateRef};
use crate::fusion::stage::ExecutableStage;
use crate::jobservice::urns;
use crate::transforms::FlareTransform;
use crate::transforms::gbk::GroupByKey;
use crate::transforms::impluse::Impulse;

pub struct ExecutablePipeline {
    graph: Graph<ExecutableNode, Consumer>,
    worker_ids: HashMap<Uuid, NodeIndex>,
    runner_ids: HashMap<String, NodeIndex>,
}

impl ExecutablePipeline {
    pub fn from(
        sdk_stages: IndexSet<ExecutableStage>,
        runner_stages: IndexSet<PTransformNode>,
    ) -> Self {
        let ep = Self {
            graph: Graph::new(),
            worker_ids: HashMap::new(),
            runner_ids: HashMap::new(),
        };

        ep
    }

    fn build_graph(
        &mut self,
        sdk_stages: IndexSet<ExecutableStage>,
        runner_stages: IndexSet<PTransformNode>,
        components: &Components,
    ) {
        self.graph = Graph::<ExecutableNode, Consumer>::new();
        self.worker_ids.clear();
        self.runner_ids.clear();

        if let Some(root) = Self::get_root(&runner_stages) {
            if let Some(spec) = root.transform.spec.as_ref() {
                let urn = &spec.urn;

                let runner_idx =
                    self.graph
                        .add_node(ExecutableNode::Runner(RunnerTransform::from_urn(
                            urn,
                            root.transform.inputs.clone(),
                            root.transform.outputs.clone(),
                        )));

                self.runner_ids.insert(root.id.clone(), runner_idx);

                // Asuming root is always Impluse and it is typically consumed by only one ExecutableStage
                for (_output_key, output_id) in root.node().outputs.iter() {
                    if let Some(consumer_stage) = sdk_stages
                        .iter()
                        .find(|stage| stage.input_pcol().id == *output_id)
                    {
                        // use consumer_stage here
                        let worker_idx = self
                            .graph
                            .add_node(ExecutableNode::Worker(consumer_stage.clone()));

                        self.worker_ids.insert(consumer_stage.id(), worker_idx);

                        self.graph
                            .add_edge(runner_idx, worker_idx, Consumer::Direct);
                    }
                }
            }
        }
    }

    fn get_root(runner_stages: &IndexSet<PTransformNode>) -> Option<&PTransformNode> {
        for pt in runner_stages.iter() {
            if pt.node().inputs.is_empty() {
                return Some(pt);
            }
        }
        None
        /* `PTransformNode` value */
    }
}

pub enum ExecutableNode {
    Worker(ExecutableStage),
    Runner(RunnerTransform),
}
pub enum RunnerTransform {
    Impulse(Impulse),
    GBK(GroupByKey),
}
impl RunnerTransform {
    pub fn from_urn(
        urn: &str,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
    ) -> Self {
        match urn {
            "beam:transform:impulse:v1" => RunnerTransform::Impulse(Impulse::with(inputs, outputs)),

            "beam:transform:gbk:v1" => RunnerTransform::GBK(GroupByKey::with(inputs, outputs)),
            _ => panic!("Unknown URN"),
        }
    }
}

pub enum Consumer {
    Direct,
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

        // TODO: return runner native transfroms for the urn
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
                            let idx =
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
