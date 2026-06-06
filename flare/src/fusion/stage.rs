use beam_model_rs::v1::executable_stage_payload::WireCoderSetting;
use beam_model_rs::v1::{Components, Environment, PCollection, PTransform};
use indexmap::IndexSet;
use uuid::Uuid;

use crate::fusion::pipeline::{PCollectionNode, PTransformNode};
use crate::fusion::refs::{SideInputRef, TimerRef, UserStateRef};

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug)]
pub struct ExecutableStage {
    id: String,
    components: Components,
    environment: Environment,
    wire_coder: HashSet<WireCoderSetting>,
    input_pcol: PCollectionNode,
    side_inputs: IndexSet<SideInputRef>,
    user_states: IndexSet<UserStateRef>,
    timers: IndexSet<TimerRef>,
    output_pcols: IndexSet<PCollectionNode>,
    transforms: IndexSet<PTransformNode>,
}

impl ExecutableStage {
    pub fn get_output_pcols(&self) -> &IndexSet<PCollectionNode> {
        &self.output_pcols
    }

    pub fn get_output_pcol_ids(&self) -> HashSet<String> {
        self.output_pcols
            .iter()
            .map(|pcol| pcol.collection.unique_name.clone())
            .collect()
    }
}

impl PartialEq for ExecutableStage {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ExecutableStage {}

impl Hash for ExecutableStage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
/*
impl PartialEq for ExecutableStage {
    fn eq(&self, other: &Self) -> bool {
        self.components.encode_to_vec() == other.components.encode_to_vec()
            && self.environment.encode_to_vec() == other.environment.encode_to_vec()
            && self.wire_coder == other.wire_coder
            && self.input_pcol == other.input_pcol
            && self.side_inputs == other.side_inputs
            && self.user_states == other.user_states
            && self.timers == other.timers
            && self.output_pcols == other.output_pcols
            && self.transforms == other.transforms
    }
}

impl Eq for ExecutableStage {}

impl Hash for ExecutableStage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.components.encode_to_vec().hash(state);
        self.environment.encode_to_vec().hash(state);

        let mut wire_hashes: Vec<u64> = self
            .wire_coder
            .iter()
            .map(|item| {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                item.hash(&mut hasher);
                hasher.finish()
            })
            .collect();
        wire_hashes.sort_unstable();
        wire_hashes.hash(state);

        self.input_pcol.hash(state);
        for item in &self.side_inputs {
            item.hash(state);
        }
        for item in &self.user_states {
            item.hash(state);
        }
        for item in &self.timers {
            item.hash(state);
        }
        for item in &self.output_pcols {
            item.hash(state);
        }
        for item in &self.transforms {
            item.hash(state);
        }
    }
}
*/
impl ExecutableStage {
    pub fn from(
        components: Components,
        environment: Environment,
        wire_coder: HashSet<WireCoderSetting>,
        input_pcol: PCollectionNode,
        side_inputs: IndexSet<SideInputRef>,
        user_states: IndexSet<UserStateRef>,
        timers: IndexSet<TimerRef>,
        output_pcols: IndexSet<PCollectionNode>,
        transforms: IndexSet<PTransformNode>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            components,
            environment,
            wire_coder,
            input_pcol,
            side_inputs,
            user_states,
            timers,
            output_pcols,
            transforms,
        }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn transforms(&self) -> IndexSet<PTransformNode> {
        self.transforms.clone()
    }

    pub fn components(&self) -> Components {
        self.components.clone()
    }

    pub fn environment(&self) -> Environment {
        self.environment.clone()
    }

    pub fn wire_coder(&self) -> HashSet<WireCoderSetting> {
        self.wire_coder.clone()
    }

    pub fn input_pcol(&self) -> PCollectionNode {
        self.input_pcol.clone()
    }

    pub fn side_inputs(&self) -> IndexSet<SideInputRef> {
        self.side_inputs.clone()
    }

    pub fn user_states(&self) -> IndexSet<UserStateRef> {
        self.user_states.clone()
    }

    pub fn timers(&self) -> IndexSet<TimerRef> {
        self.timers.clone()
    }

    pub fn output_pcols(&self) -> IndexSet<PCollectionNode> {
        self.output_pcols.clone()
    }

    pub fn ptmap(&self) -> HashMap<String, PTransform> {
        let mut pt_map = HashMap::<String, PTransform>::new();
        for t in &self.transforms {
            pt_map.insert(t.id.clone(), t.transform.clone());
        }

        pt_map
    }

    pub fn pcolmap(&self) {
        let pcol_map = HashMap::<String, PCollection>::new();
        //self.
    }
}

#[derive(Eq, PartialEq, Clone, Hash)]
/// Pairs of PTransfrom and PCollection nodes that is consumed by transfrom
pub struct CollectionConsumers {
    collection: PCollectionNode,
    transform: PTransformNode,
}

impl CollectionConsumers {
    pub fn of(pcol: PCollectionNode, pt: PTransformNode) -> Self {
        Self {
            collection: pcol,
            transform: pt,
        }
    }

    pub fn collection(&self) -> &PCollectionNode {
        &self.collection
    }

    pub fn node(&self) -> &PTransformNode {
        &self.transform
    }
}

impl Ord for CollectionConsumers {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.collection
            .id
            .cmp(&other.collection.id)
            .then_with(|| self.transform.id.cmp(&other.transform.id))
    }
}

impl PartialOrd for CollectionConsumers {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Collection of runner and SDK implemented transfroms
pub struct DescendantConsumers {
    /// runner impl
    unfusible: HashSet<PTransformNode>,

    /// SDK impl
    fusible: BTreeSet<CollectionConsumers>,
}

impl DescendantConsumers {
    /// Creates a new `DescendantConsumers` with the given sets.
    pub fn new(unfusible: HashSet<PTransformNode>, fusible: BTreeSet<CollectionConsumers>) -> Self {
        Self { unfusible, fusible }
    }

    /// Returns a reference to the set of unfusible transform nodes.
    pub fn get_unfusible(&self) -> &HashSet<PTransformNode> {
        &self.unfusible
    }

    /// Returns a reference to the set of fusible collection consumers.
    pub fn get_fusible(&self) -> &BTreeSet<CollectionConsumers> {
        &self.fusible
    }
}

#[derive(Clone, PartialEq)]
pub struct SiblingKey {
    pcol: PCollectionNode,
    env: Option<Environment>,
}

impl Eq for SiblingKey {}

impl Hash for SiblingKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pcol.hash(state);
        // Hash based on whether env exists rather than its contents
        // since Environment doesn't implement Hash
        self.env.is_some().hash(state);
    }
}

impl SiblingKey {
    pub fn from(col: &PCollectionNode, env: &Option<Environment>) -> Self {
        Self {
            pcol: col.clone(),
            env: env.clone(),
        }
    }

    pub fn get_input_pcol(&self) -> &PCollectionNode {
        &self.pcol
    }

    pub fn get_env(&self) -> &Option<Environment> {
        &self.env
    }
}
// have Executable Graph struct that
// has Executable stages
