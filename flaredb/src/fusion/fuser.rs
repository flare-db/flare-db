use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use beam_model_rs::v1::{
    Components, Environment, FunctionSpec, PCollection, PTransform, ParDoPayload,
    executable_stage_payload::WireCoderSetting,
};
use indexmap::IndexSet;
use log::debug;
use prost::Message;

use crate::{
    check_argument,
    fusion::{
        pipeline::{FusedPipeline, PCollectionNode, PTransformNode, QueryablePipeline},
        refs::{SideInputRef, TimerRef, UserStateRef},
        stage::{CollectionConsumers, DescendantConsumers, ExecutableStage, SiblingKey},
    },
    jobservice::urns,
    utils::errors::*,
};

pub struct GreedyPipelineFuser {
    pub(crate) pipeline: QueryablePipeline,
    //fused_pipeline: FusedPipeline,
}

impl GreedyPipelineFuser {
    pub fn with(p: QueryablePipeline) -> Self {
        Self { pipeline: p }
    }

    pub fn fuse_pipeline(
        &self,
        initial_unfused_pt: HashSet<PTransformNode>,
        initial_consumers: BTreeSet<CollectionConsumers>,
    ) -> Result<FusedPipeline, BeamTranslationError> {
        let mut consumed_pairs = HashMap::<CollectionConsumers, ExecutableStage>::new();

        let mut stages = IndexSet::<ExecutableStage>::new();

        let mut unfused_pt = IndexSet::<PTransformNode>::new();
        unfused_pt.extend(initial_unfused_pt);

        let mut pending_siblings: VecDeque<BTreeSet<CollectionConsumers>> = self
            .group_siblings(&initial_consumers)
            .iter()
            .cloned()
            .collect();
        // initial_consumers.iter().cloned().collect();

        while let Some(candidate_siblings) = pending_siblings.pop_front() {
            // candidate_siblings MINUS already-consumed consumers.
            let sibling_set: BTreeSet<CollectionConsumers> = candidate_siblings
                .difference(&consumed_pairs.keys().cloned().collect())
                .cloned()
                .collect();

            check_argument!(
                sibling_set.eq(&candidate_siblings) || sibling_set.is_empty(),
                BeamTranslationError::InvalidState(
                    "Inconsistent collection of siblings reported".to_string(),
                )
            );
            if sibling_set.is_empty() {
                debug!("Filtered out duplicate stage root");
                continue;
            }

            let stage = self.fuse_siblings(&sibling_set)?;

            for sibling in sibling_set.iter() {
                consumed_pairs.insert(sibling.clone(), stage.clone());
            }
            stages.insert(stage.clone());

            for materialized_output in stage.get_output_pcols().iter() {
                let descendant_consumers = self.get_descendant_consumers(materialized_output);
                unfused_pt.extend(descendant_consumers.get_unfusible().iter().cloned());

                let siblings = self.group_siblings(descendant_consumers.get_fusible());
                pending_siblings.extend(siblings);
            }
        }
        let dedup = ensure_single_producer(&self.pipeline, &stages, &unfused_pt)?;

        Ok(FusedPipeline::of(
            // dedup.components(),
            dedup.get_sdk_stages(&stages),
            dedup.get_runner_stages(&unfused_pt),
        ))
    }

    fn fuse_siblings(
        &self,
        mutually_compactable: &BTreeSet<CollectionConsumers>,
    ) -> Result<ExecutableStage, BeamTranslationError> {
        let stage_root = mutually_compactable
            .iter()
            .next()
            .expect("sibling_set cannot be empty after early-continue");

        let initial_nodes: HashSet<PTransformNode> = mutually_compactable
            .iter()
            .map(|set| set.node().clone())
            .collect();

        GreedyStageFuser::fuse(&self.pipeline, stage_root.collection(), &initial_nodes)
    }

    /// Groups consumers of the same PCollection into sibling sets based on fusion compatibility.
    ///
    /// Two consumers are considered siblings if they consume the same PCollection,
    /// run in the same environment, and are mutually compatible for fusion
    /// (via [`GreedyCollectionFuser::is_compatible`]).
    ///
    /// 1. Key each consumer by `(PCollection, Environment)` → [`SiblingKey`]
    /// 2. For each key, maintain a list of sibling groups (`Vec<BTreeSet<...>>`)
    /// 3. A consumer joins the first existing group where it is compatible with
    ///    every current member. If no compatible group exists, it starts a new one.
    /// 4. Flatten all groups across all keys into a single ordered set.
    ///
    /// ## Returns:
    /// A `BTreeSet<BTreeSet<CollectionConsumers>>` — each inner set is a group of
    /// mutually fusion-compatible consumers. Ordered by natural `BTreeSet` ordering.
    fn group_siblings(
        &self,
        new_consumers: &BTreeSet<CollectionConsumers>,
    ) -> BTreeSet<BTreeSet<CollectionConsumers>> {
        // one key -> array of many sets
        let mut compactable: HashMap<SiblingKey, Vec<BTreeSet<CollectionConsumers>>> =
            HashMap::new();

        for consumer in new_consumers {
            let key = SiblingKey::from(
                &consumer.collection(),
                &self.pipeline.get_environment(&consumer.node().transform),
            );

            // gets all existing sibling groups for that SiblingKey, When the key doesn't exist( Eg, First attempt)
            // it Inserts an empty Vec::new() into the HashMap at that key and returns &mut to that newly inserted empty Vec
            let sets = compactable.entry(key).or_default();
            let mut found_siblings = false;

            // Check all existing groups that belongs to the key
            for existing_set in sets.iter_mut() {
                if existing_set.iter().all(|c| {
                    GreedyCollectionFuser::is_compatible(
                        &c.node(),
                        &consumer.node(),
                        &self.pipeline,
                    )
                }) {
                    existing_set.insert(consumer.clone());
                    found_siblings = true;
                    break;
                }
            }

            if !found_siblings {
                let mut new_set = BTreeSet::new();
                new_set.insert(consumer.clone());
                sets.push(new_set);
            }
        }

        // Flatten and order
        let mut ordered = BTreeSet::new();
        for sets in compactable.into_values() {
            for set in sets {
                ordered.insert(set);
            }
        }
        ordered
    }

    pub fn get_root_consumers(&self, root_node: PTransformNode) -> DescendantConsumers {
        // TODO:
        // 1. vefify if root has no inputs
        // 2. if runner implemented

        let mut unfused = HashSet::<PTransformNode>::new();
        let mut enviroment_nodes = BTreeSet::<CollectionConsumers>::new();

        for output in self.pipeline.get_output_pcol(&root_node) {
            // 1st Immidate downstream comsumers of root nodes's output pcol
            let descendants = self.get_descendant_consumers(&output);
            unfused.extend(descendants.get_unfusible().iter().cloned());
            enviroment_nodes.extend(descendants.get_fusible().iter().cloned());
        }

        return DescendantConsumers::new(unfused, enviroment_nodes);
    }

    pub fn get_descendant_consumers(&self, pcol: &PCollectionNode) -> DescendantConsumers {
        let mut unfused = HashSet::<PTransformNode>::new();
        let mut downstream_consumers = BTreeSet::<CollectionConsumers>::new();

        for consumer in self.pipeline.get_per_element_consumers(pcol) {
            // Transfroms that doesn't have an environment are typically runner implemented transfroms. So, we add them to unfused
            // Ones that have an environment are SDK implemented and goes into downstream_consumers
            match self.pipeline.get_environment(&consumer.transform) {
                Some(_) => {
                    downstream_consumers
                        .insert(CollectionConsumers::of(pcol.clone(), consumer.clone()));
                }
                None => {
                    unfused.insert(consumer.clone());

                    // once we hit a runner's boundry we go deep on runner transfroms's output pcol
                    // and collect the fusable and unfusable pairs
                    for output in self.pipeline.get_output_pcol(&consumer) {
                        let descendant = self.get_descendant_consumers(&output);
                        unfused.extend(descendant.get_unfusible().iter().cloned());
                        downstream_consumers.extend(descendant.get_fusible().iter().cloned());
                    }
                }
            }
        }

        return DescendantConsumers::new(unfused, downstream_consumers);
    }
}

struct GreedyStageFuser {}

enum PCollectionFusibility {
    FUSE,
    MATERIALIZE,
}

impl GreedyStageFuser {
    fn fuse(
        pipeline: &QueryablePipeline,
        input_pcol: &PCollectionNode,
        initial_nodes: &HashSet<PTransformNode>,
    ) -> Result<ExecutableStage, BeamTranslationError> {
        check_argument!(
            !initial_nodes.is_empty(),
            BeamTranslationError::InvalidArgument(
                "must contain atleast one element GreedyStageFuser".to_string()
            )
        );

        let env = get_stage_environment(pipeline, initial_nodes)?;

        let mut fused_transforms: IndexSet<PTransformNode> =
            initial_nodes.iter().cloned().collect();

        let mut side_inputs = IndexSet::<SideInputRef>::new();
        let mut user_states = IndexSet::<UserStateRef>::new();
        let mut timers = IndexSet::<TimerRef>::new();

        let mut fused_pcols = IndexSet::<PCollectionNode>::new();
        let mut materialized_pcols = IndexSet::<PCollectionNode>::new();

        let mut fusion_candidates = VecDeque::<PCollectionNode>::new();
        //fusion_candidates.push_back(input_pcol.clone());

        for initial_consumer in initial_nodes {
            fusion_candidates.extend(pipeline.get_output_pcol(initial_consumer));
            side_inputs.extend(pipeline.get_side_inputs(initial_consumer)?);
            user_states.extend(pipeline.get_user_states(initial_consumer)?);
            timers.extend(pipeline.get_timers(initial_consumer)?);
        }

        while let Some(candidate) = fusion_candidates.pop_front() {
            if fused_pcols.contains(&candidate) || materialized_pcols.contains(&candidate) {
                debug!(
                    "Skipping fusion candidate {} because it is {} in this {}",
                    candidate.id(),
                    if fused_pcols.contains(&candidate) {
                        "fused"
                    } else {
                        "materialized"
                    },
                    "ExecutableStage"
                );
                continue;
            }
            match can_fuse(&pipeline, &candidate, &env) {
                PCollectionFusibility::FUSE => {
                    for consumer in pipeline.get_per_element_consumers(&candidate) {
                        fusion_candidates.extend(pipeline.get_output_pcol(&consumer));
                        side_inputs.extend(pipeline.get_side_inputs(&consumer)?);
                    }
                    fused_transforms.extend(pipeline.get_per_element_consumers(&candidate));
                    fused_pcols.insert(candidate);
                    //break;
                }
                PCollectionFusibility::MATERIALIZE => {
                    materialized_pcols.insert(candidate);
                    //break;
                }
            }
        }

        let stage = ExecutableStage::from(
            pipeline.components().clone(),
            env,
            HashSet::<WireCoderSetting>::new(),
            input_pcol.clone(),
            side_inputs,
            user_states,
            timers,
            materialized_pcols,
            fused_transforms,
        );

        Ok(sanitize_dangling_ptransform_inputs(stage))
    }
}

struct GreedyCollectionFuser {}

const COMBINE_FUSIBLE: &[&str] = &[
    urns::beam_urns::COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN,
    urns::beam_urns::COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN,
    urns::beam_urns::COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN,
];

impl GreedyCollectionFuser {
    /// Bidirectional compatibility check
    fn is_compatible(
        node: &PTransformNode,
        other: &PTransformNode,
        pipeline: &QueryablePipeline,
    ) -> bool {
        Self::is_compatible_one_way(node, other, pipeline)
            && Self::is_compatible_one_way(other, node, pipeline)
    }

    /// Checks if node is compatible with other
    /// for sibling fusion.
    fn is_compatible_one_way(
        node: &PTransformNode,
        other: &PTransformNode,
        pipeline: &QueryablePipeline,
    ) -> bool {
        let urn = get_urn(node);

        match urn {
            // ParDo family: compatible if no side-inputs/state/timers + same env.
            urns::beam_urns::PAR_DO_TRANSFORM
            | urns::beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN
            | urns::beam_urns::SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN
            | urns::beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN => {
                Self::par_do_compatibility(node, other, pipeline)
            }

            // Combine sub-components + window assignment: compatible if same env.
            u if COMBINE_FUSIBLE.contains(&u) || u == urns::beam_urns::ASSIGN_WINDOWS_TRANSFORM => {
                Self::compatible_environments(node, other, pipeline)
            }

            // Flatten, GBK, Impulse:
            urns::beam_urns::FLATTEN_TRANSFORM => false,
            urns::beam_urns::GROUP_BY_KEY_TRANSFORM => false,
            urns::beam_urns::IMPULSE_TRANSFORM => false,

            unknown => {
                debug!(
                    "PTransform '{}' (urn: {}) will not root a stage with other transforms",
                    node.id, unknown
                );
                false
            }
        }
    }

    fn par_do_compatibility(
        par_do: &PTransformNode,
        other: &PTransformNode,
        pipeline: &QueryablePipeline,
    ) -> bool {
        // Self-loop: a ParDo is always compatible with itself (timer case).
        par_do == other
            || (!Self::has_side_inputs_in_payload(par_do, pipeline)
                && !Self::has_state_or_timers(par_do)
                && Self::compatible_environments(par_do, other, pipeline))
    }

    /// Returns true if this transform's ParDoPayload declares side inputs.
    fn has_side_inputs_in_payload(
        transform: &PTransformNode,
        pipeline: &QueryablePipeline,
    ) -> bool {
        pipeline
            .get_side_inputs(transform)
            .map(|s| !s.is_empty())
            .unwrap_or(true) // on decode error, treat as having side inputs (safer)
    }
    /// Parses `par_do.transform.spec.payload` as a `ParDoPayload` proto and
    /// checks if `state_specs` or `timer_family_specs` are non-empty.
    fn has_state_or_timers(par_do: &PTransformNode) -> bool {
        let spec = match &par_do.transform.spec {
            Some(s) if !s.payload.is_empty() => s,
            _ => return false,
        };

        match ParDoPayload::decode(spec.payload.as_slice()) {
            Ok(payload) => {
                !payload.state_specs.is_empty() || !payload.timer_family_specs.is_empty()
            }
            Err(_) => true, // safer runner behavior
        }
    }

    fn can_fuse(
        node: &PTransformNode,
        environment: &Environment,
        candidate: &PCollectionNode,
        //stage_pcols: &HashSet<PCollectionNode>,
        pipeline: &QueryablePipeline,
    ) -> bool {
        match get_urn(node) {
            urns::beam_urns::PAR_DO_TRANSFORM
            | urns::beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN
            | urns::beam_urns::SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN => {
                Self::can_fuse_pardo(node, environment, candidate, pipeline)
            }

            urns::beam_urns::SPLITTABLE_PROCESS_KEYED_URN
            | urns::beam_urns::SPLITTABLE_PROCESS_ELEMENTS_URN
            | urns::beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN
            | urns::beam_urns::GROUP_BY_KEY_TRANSFORM
            | urns::beam_urns::CREATE_VIEW_TRANSFORM => false,

            u if COMBINE_FUSIBLE.contains(&u) || u == urns::beam_urns::ASSIGN_WINDOWS_TRANSFORM => {
                Self::can_fuse_compatible_env(node, environment, pipeline)
            }

            urns::beam_urns::FLATTEN_TRANSFORM => true,

            unknown => {
                debug!(
                    "PTransform '{}' (urn: {}) has no fusibility checker, defaulting to cannot fuse",
                    node.id, unknown
                );
                false
            }
        }
    }

    fn compatible_environments(
        left: &PTransformNode,
        right: &PTransformNode,
        pipeline: &QueryablePipeline,
    ) -> bool {
        pipeline.get_environment(&left.transform) == pipeline.get_environment(&right.transform)
    }

    // if pardo has state or timer -> return false
    // if pardo has side input -> return false
    // else -> return true
    fn can_fuse_pardo(
        pardo: &PTransformNode,
        environment: &Environment,
        candidate: &PCollectionNode,
        //stage_pcols: &HashSet<PCollectionNode>,
        pipeline: &QueryablePipeline,
    ) -> bool {
        // if stage env != pardo env -> return false
        if !pipeline
            .get_environment(&pardo.transform)
            .map_or(false, |env| same_environment(&env, &environment))
        {
            return false;
        }

        let spec = match &pardo.transform.spec {
            Some(s) if !s.payload.is_empty() => s,
            _ => return false,
        };

        let payload = match ParDoPayload::decode(spec.payload.as_slice()) {
            Ok(p) => p,
            Err(_) => return false,
        };

        // Allow fusion across timer PCollections — they are a self-loop
        if pardo
            .transform
            .inputs
            .iter()
            .any(|(key, val)| payload.timer_family_specs.contains_key(key) && val == &candidate.id)
        {
            return true;
        }

        // State or timers: must be key-partitioned, don't fuse
        if !payload.state_specs.is_empty() || !payload.timer_family_specs.is_empty() {
            return false;
        }

        // Can't fuse if it has side inputs.
        if Self::has_side_inputs_in_payload(&pardo, pipeline) {
            return false;
        }

        true
    }

    fn can_fuse_compatible_env(
        operation: &PTransformNode,
        environment: &Environment,
        pipeline: &QueryablePipeline,
    ) -> bool {
        pipeline
            .get_environment(&operation.transform)
            .map_or(false, |env| same_environment(&env, environment))
    }
}

// Extracts the URN string from a transform node's spec, or `""` if absent.
fn get_urn(node: &PTransformNode) -> &str {
    node.transform
        .spec
        .as_ref()
        .map(|s| s.urn.as_str())
        .unwrap_or("")
}

fn get_stage_environment(
    pipeline: &QueryablePipeline,
    initial_nodes: &HashSet<PTransformNode>,
) -> Result<Environment, BeamTranslationError> {
    let first_node = initial_nodes
        .iter()
        .next()
        .expect("initial_nodes must not be empty");

    // plain .ok_or()? instead of require_present!
    let env: Environment = pipeline
        .get_environment(&first_node.transform)
        .ok_or_else(|| {
            BeamTranslationError::InvalidArgument(
                "Environment must be populated on all PTransformNodes in GreedyStageFuser".into(),
            )
        })?;

    for node in initial_nodes {
        let node_env = pipeline.get_environment(&node.transform).ok_or_else(|| {
            BeamTranslationError::InvalidArgument(
                "Environment must be populated on all PTransformNodes in GreedyStageFuser".into(),
            )
        })?;

        check_argument!(
            same_environment(&env, &node_env),
            "All PTransformNodes in an ExecutableStage must be the same. Got {:?} and {:?}",
            env,
            node_env
        );
    }

    Ok(env)
}

// TODO: validate if right way to compare
pub fn same_environment(a: &Environment, b: &Environment) -> bool {
    a.urn == b.urn
    //&& a.payload == b.payload
}

fn can_fuse(
    pipeline: &QueryablePipeline,
    candidate: &PCollectionNode,
    environment: &Environment,
    //fused_pocl: HashSet<PCollectionNode>,
) -> PCollectionFusibility {
    for consumer in pipeline.get_per_element_consumers(&candidate) {
        if any_sideinputs(&consumer, pipeline)
            || !GreedyCollectionFuser::can_fuse(&consumer, &environment, &candidate, pipeline)
        {
            return PCollectionFusibility::MATERIALIZE;
        }
    }
    if !pipeline.get_singleton_consumers(&candidate).is_empty() {
        return PCollectionFusibility::MATERIALIZE;
    }

    return PCollectionFusibility::FUSE;
}

fn any_sideinputs(consumer: &PTransformNode, pipeline: &QueryablePipeline) -> bool {
    for (_input_key, input_id) in consumer.transform.inputs.iter() {
        if let Some(col) = pipeline.components().pcollections.get(input_id) {
            if !pipeline
                .get_singleton_consumers(&PCollectionNode {
                    id: input_id.clone(),
                    collection: col.clone(),
                })
                .is_empty()
            {
                return true;
            }
        }
    }
    return false;
}

/// Remove dangling transform inputs from a stage and drop dangling PCollections
/// from stage components.
///
/// Valid inputs are:
/// 1. Explicit stage input PCollection
/// 2. Explicit stage output PCollections
/// 3. Side-input PCollections
/// 4. Timer PCollections
/// 5. Outputs produced by transforms within the stage
fn sanitize_dangling_ptransform_inputs(stage: ExecutableStage) -> ExecutableStage {
    let mut possible_inputs: HashSet<String> = HashSet::new();
    possible_inputs.insert(stage.input_pcol().id.clone());
    possible_inputs.extend(stage.output_pcols().iter().map(|p| p.id.clone()));
    possible_inputs.extend(
        stage
            .side_inputs()
            .iter()
            .map(|side_input| side_input.collection().id.clone()),
    );
    possible_inputs.extend(stage.timers().iter().filter_map(|timer| {
        timer
            .transform()
            .node()
            .inputs
            .get(timer.local_name())
            .cloned()
    }));
    possible_inputs.extend(
        stage
            .transforms()
            .iter()
            .flat_map(|transform| transform.transform.outputs.values().cloned()),
    );

    let dangling_inputs: HashSet<String> = stage
        .transforms()
        .iter()
        .flat_map(|transform| transform.transform.inputs.values().cloned())
        .filter(|input| !possible_inputs.contains(input))
        .collect();

    if dangling_inputs.is_empty() {
        return stage;
    }

    let sanitized_transforms: IndexSet<PTransformNode> = stage
        .transforms()
        .iter()
        .map(|transform_node| {
            let mut sanitized_transform = transform_node.transform.clone();
            sanitized_transform
                .inputs
                .retain(|_, input_id| !dangling_inputs.contains(input_id));

            PTransformNode {
                id: transform_node.id.clone(),
                transform: sanitized_transform,
            }
        })
        .collect();

    let mut sanitized_components = stage.components();
    sanitized_components.transforms.clear();
    for transform_node in &sanitized_transforms {
        sanitized_components
            .transforms
            .insert(transform_node.id.clone(), transform_node.transform.clone());
    }
    sanitized_components
        .pcollections
        .retain(|id, _| !dangling_inputs.contains(id));

    ExecutableStage::from(
        sanitized_components,
        stage.environment(),
        stage.wire_coder(),
        stage.input_pcol(),
        stage.side_inputs(),
        stage.user_states(),
        stage.timers(),
        stage.output_pcols(),
        sanitized_transforms,
    )
}

pub struct DeduplicationResult {
    /// Updated pipeline components (with synthetic partial PCollections + Flattens injected).
    pub components: Components,
    /// Synthetic Flatten transforms introduced to merge partial PCollections.
    pub introduced_transforms: IndexSet<PTransformNode>,
    /// Stages that were rewritten; stages not present here are unchanged.
    pub deduplicated_stages: HashMap<ExecutableStage, ExecutableStage>,
    /// Unfused transforms that were rewritten; keyed by original transform ID.
    pub deduplicated_transforms: HashMap<String, PTransformNode>,
}

impl DeduplicationResult {
    pub fn components(&self) -> Components {
        self.components.clone()
    }

    pub fn get_sdk_stages(&self, stages: &IndexSet<ExecutableStage>) -> IndexSet<ExecutableStage> {
        stages
            .iter()
            .map(|s| {
                self.deduplicated_stages
                    .get(s)
                    .cloned()
                    .unwrap_or_else(|| s.clone())
            })
            .map(|s| sanitize_dangling_ptransform_inputs(s))
            .collect()
    }

    pub fn get_runner_stages(
        &self,
        unfused_pt: &IndexSet<PTransformNode>,
    ) -> IndexSet<PTransformNode> {
        unfused_pt
            .iter()
            .map(|t| {
                self.deduplicated_transforms
                    .get(&t.id)
                    .cloned()
                    .unwrap_or_else(|| t.clone())
            })
            .collect::<IndexSet<_>>()
            .union(&self.introduced_transforms)
            .cloned()
            .collect()
    }
}

// Ensure no PCollection is produced by more than one stage or unfused transform.
//
// For each PCollection with multiple producers, each producer is rewritten to
// emit a *partial* PCollection.  A synthetic Flatten is then introduced that
// merges all partials back into the original PCollection.
pub fn ensure_single_producer(
    pipeline: &QueryablePipeline,
    stages: &IndexSet<ExecutableStage>,
    unfused_transforms: &IndexSet<PTransformNode>,
) -> Result<DeduplicationResult, BeamTranslationError> {
    let mut components = pipeline.components().clone();

    // 1. Build pcollection -> [producers] map
    // A "producer" is either a stage or an unfused transform.
    let producers = collect_producers(pipeline, stages, unfused_transforms);

    // 2. Find PCollections with more than one producer
    // producer -> set of PCollections it must be rewritten for
    let mut requires_new_output: HashMap<StageOrTransform, Vec<PCollectionNode>> = HashMap::new();

    for (pcol, prods) in &producers {
        if prods.len() > 1 {
            for producer in prods {
                requires_new_output
                    .entry(producer.clone())
                    .or_default()
                    .push(pcol.clone());
            }
        }
    }

    // 3. Rewrite each affected producer
    let mut updated_stages: HashMap<ExecutableStage, ExecutableStage> = HashMap::new();
    let mut updated_transforms: HashMap<String, PTransformNode> = HashMap::new();

    // original pcol id -> list of synthetic partial PCollectionNodes
    let mut original_to_partials: HashMap<String, Vec<PCollectionNode>> = HashMap::new();

    for (producer, duplicates) in &requires_new_output {
        match producer {
            StageOrTransform::Stage(stage) => {
                let dedup = deduplicate_stage(stage, duplicates, &components)?;

                // register synthetic partial PCollections into components
                for (orig_id, partial) in &dedup.original_to_partial {
                    components
                        .pcollections
                        .insert(partial.id.clone(), partial.collection.clone());

                    original_to_partials
                        .entry(orig_id.clone())
                        .or_default()
                        .push(partial.clone());
                }
                updated_stages.insert(stage.clone(), dedup.updated_stage);
            }
            StageOrTransform::Transform(transform) => {
                let dedup = deduplicate_transform(transform, duplicates, &components)?;

                for (orig_id, partial) in &dedup.original_to_partial {
                    components
                        .pcollections
                        .insert(partial.id.clone(), partial.collection.clone());
                    original_to_partials
                        .entry(orig_id.clone())
                        .or_default()
                        .push(partial.clone());
                }
                updated_transforms.insert(transform.id.clone(), dedup.updated_transform);
            }
        }
    }

    // Introduce a Flatten for each deduplicated PCollection
    let mut introduced_transforms: IndexSet<PTransformNode> = IndexSet::new();

    for (original_id, partials) in &original_to_partials {
        let flatten_id = unique_id("unzipped_flatten", |id| {
            components.transforms.contains_key(id)
        });

        let flatten = create_flatten_of_partials(&flatten_id, original_id, partials);
        components
            .transforms
            .insert(flatten_id.clone(), flatten.clone());
        introduced_transforms.insert(PTransformNode {
            id: flatten_id,
            transform: flatten,
        });
    }

    Ok(DeduplicationResult {
        components,
        introduced_transforms,
        deduplicated_stages: updated_stages,
        deduplicated_transforms: updated_transforms,
    })
}

/// Discriminated union: a producer is either a fused stage or an unfused transform.
#[derive(Clone, PartialEq, Eq, Hash)]
enum StageOrTransform {
    Stage(ExecutableStage),
    Transform(PTransformNode),
}

/// Collect every (pcollection → producer) pair across stages and unfused transforms.
fn collect_producers(
    pipeline: &QueryablePipeline,
    stages: &IndexSet<ExecutableStage>,
    unfused_transforms: &IndexSet<PTransformNode>,
) -> HashMap<PCollectionNode, Vec<StageOrTransform>> {
    // Collections of pcol and its producers
    let mut pcol_producers: HashMap<PCollectionNode, Vec<StageOrTransform>> = HashMap::new();

    // collect pcols produed by all stages
    for stage in stages {
        for output in stage.get_output_pcols() {
            // look up this PCollection in the map
            // if it doesn't exist yet, insert an empty Vec
            // append this producer to the Vec
            pcol_producers
                .entry(output.clone())
                .or_default()
                .push(StageOrTransform::Stage(stage.clone()));
        }
    }

    // collect pcols produced by unfused runner impl transforms
    for transform in unfused_transforms {
        for output in pipeline.get_output_pcol(transform) {
            pcol_producers
                .entry(output.clone())
                .or_default()
                .push(StageOrTransform::Transform(transform.clone()));
        }
    }

    pcol_producers
}

// Per-producer deduplication results

struct StageDeduplication {
    updated_stage: ExecutableStage,
    /// original pcol id → synthetic partial PCollectionNode
    original_to_partial: HashMap<String, PCollectionNode>,
}

struct TransformDeduplication {
    updated_transform: PTransformNode,
    original_to_partial: HashMap<String, PCollectionNode>,
}

// Stage rewriting

fn deduplicate_stage(
    stage: &ExecutableStage,
    duplicates: &[PCollectionNode],
    components: &Components,
) -> Result<StageDeduplication, BeamTranslationError> {
    let original_to_partial = create_partial_pcollections(duplicates, components)?;

    // Rewrite every transform inside the stage to point at partials instead of originals.
    let updated_transforms: Vec<PTransformNode> = stage
        .transforms()
        .iter()
        .map(|t| {
            let updated = update_outputs(&t.transform, &original_to_partial);
            PTransformNode {
                id: t.id.clone(),
                transform: updated,
            }
        })
        .collect();

    // Rewrite stage output list.
    let updated_outputs: Vec<PCollectionNode> = stage
        .get_output_pcols()
        .iter()
        .map(|pcol: &PCollectionNode| {
            original_to_partial
                .get(&pcol.id)
                .cloned()
                .unwrap_or_else(|| pcol.clone())
        })
        .collect();

    // Rebuild stage components: swap transforms + add partial PCollections.
    let mut stage_components = stage.components();
    stage_components.transforms.clear();
    for pt_node in &updated_transforms {
        stage_components
            .transforms
            .insert(pt_node.id.clone(), pt_node.transform.clone());
    }
    for partial in original_to_partial.values() {
        stage_components
            .pcollections
            .insert(partial.id.clone(), partial.collection.clone());
    }

    let updated_stage = ExecutableStage::from(
        stage_components,
        stage.environment(),
        stage.wire_coder(),
        stage.input_pcol(),
        stage.side_inputs(),
        stage.user_states(),
        stage.timers(),
        updated_outputs.into_iter().collect(),
        updated_transforms.into_iter().collect(),
    );

    Ok(StageDeduplication {
        updated_stage,
        original_to_partial,
    })
}

// Unfused transform rewriting

fn deduplicate_transform(
    transform: &PTransformNode,
    duplicates: &[PCollectionNode],
    components: &beam_model_rs::v1::Components,
) -> Result<TransformDeduplication, BeamTranslationError> {
    let original_to_partial = create_partial_pcollections(duplicates, components)?;
    let updated_proto = update_outputs(&transform.transform, &original_to_partial);

    Ok(TransformDeduplication {
        updated_transform: PTransformNode {
            id: transform.id.clone(),
            transform: updated_proto,
        },
        original_to_partial,
    })
}

// dedup utilities

/// For each duplicate PCollection, mint a unique ID and build a "partial" clone.
/// Returns a map: original_id -> partial PCollectionNode.
/// basically cretes a branched PCollection with a unique id.
fn create_partial_pcollections(
    duplicates: &[PCollectionNode],
    components: &Components,
) -> Result<HashMap<String, PCollectionNode>, BeamTranslationError> {
    let mut result: HashMap<String, PCollectionNode> = HashMap::new();

    for dup in duplicates {
        // Avoid collisions with both existing pipeline PCollections and ones
        // we've already minted in this call.
        let partial_id = unique_id(&dup.id, |id| {
            components.pcollections.contains_key(id)
                || result.values().any(|n: &PCollectionNode| n.id == id)
        });

        let partial_pcol = PCollection {
            unique_name: partial_id.clone(),
            ..dup.collection.clone()
        };

        // Guard: each original ID must appear at most once per producer.
        let prev = result.insert(
            dup.id.clone(),
            PCollectionNode {
                id: partial_id,
                collection: partial_pcol,
            },
        );
        check_argument!(
            prev.is_none(),
            "duplicate pcollection appeared more than once in a single stage: {}",
            dup.id
        );
    }

    Ok(result)
}

/// Rewrite a `PTransform`'s output map: any output pointing at an original
/// PCollection is redirected to the corresponding partial (branched) PCollection.
fn update_outputs(
    transform: &PTransform,
    original_to_partial: &HashMap<String, PCollectionNode>,
) -> PTransform {
    let mut updated = transform.clone();
    for (_local_name, pcol_id) in updated.outputs.iter_mut() {
        if let Some(partial) = original_to_partial.get(pcol_id.as_str()) {
            *pcol_id = partial.id.clone();
        }
    }
    updated
}

/// Build a Flatten transform whose inputs are all the partial PCollections and
/// whose single output is the original PCollection ID.
fn create_flatten_of_partials(
    transform_id: &str,
    output_pcol_id: &str,
    inputs: &[PCollectionNode],
) -> PTransform {
    let input_map: HashMap<String, String> = inputs
        .iter()
        .enumerate()
        .map(|(i, node)| (format!("input_{}", i), node.id.clone()))
        .collect();

    PTransform {
        unique_name: transform_id.to_string(),
        inputs: input_map,
        outputs: [("output".to_string(), output_pcol_id.to_string())]
            .into_iter()
            .collect(),
        spec: Some(FunctionSpec {
            urn: urns::beam_urns::FLATTEN_TRANSFORM.to_string(),
            payload: vec![],
        }),
        ..Default::default()
    }
}

/// Generate an ID that is unique with respect to the `exists` predicate by
/// appending a numeric suffix when necessary (mirrors Java's `SyntheticComponents.uniqueId`).
fn unique_id(prefix: &str, exists: impl Fn(&str) -> bool) -> String {
    let mut candidate = prefix.to_string();
    let mut counter = 0usize;
    while exists(&candidate) {
        candidate = format!("{}-{}", prefix, counter);
        counter += 1;
    }
    candidate
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fusion::pipeline::{PCollectionNode, PTransformNode, QueryablePipeline};
    use crate::fusion::stage::{CollectionConsumers, ExecutableStage};
    use crate::jobservice::urns::beam_urns;
    use beam_model_rs::v1::{
        Components, Environment, FunctionSpec, PCollection, PTransform, ParDoPayload, SideInput,
        StateSpec, TimerFamilySpec, executable_stage_payload::WireCoderSetting,
    };
    use indexmap::IndexSet;
    use prost::Message;
    use std::collections::{BTreeSet, HashMap, HashSet};

    //
    // helpers
    //

    /// Build a minimal Environment recognizable by same_environment.
    fn make_env(id: &str) -> Environment {
        Environment {
            urn: format!("beam:env:test:{id}"),
            ..Default::default()
        }
    }

    /// Build a FunctionSpec with a given URN and no payload.
    fn make_spec(urn: &str) -> FunctionSpec {
        FunctionSpec {
            urn: urn.to_string(),
            ..Default::default()
        }
    }

    /// Build a ParDo-aware FunctionSpec whose payload is an encoded ParDoPayload.
    fn make_pardo_spec(urn: &str, payload: &ParDoPayload) -> FunctionSpec {
        FunctionSpec {
            urn: urn.to_string(),
            payload: payload.encode_to_vec(),
            ..Default::default()
        }
    }

    /// Create a bare PTransformNode with a given URN, no payload.
    fn make_transform(
        id: &str,
        urn: &str,
        inputs: &[(&str, &str)],
        outputs: &[(&str, &str)],
        env_id: &str,
    ) -> PTransformNode {
        PTransformNode {
            id: id.to_string(),
            transform: PTransform {
                unique_name: id.to_string(),
                spec: Some(make_spec(urn)),
                inputs: inputs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                outputs: outputs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                environment_id: env_id.to_string(),
                ..Default::default()
            },
        }
    }

    /// Create a PTransformNode whose spec encodes a ParDoPayload.
    fn make_pardo_transform(
        id: &str,
        urn: &str,
        inputs: &[(&str, &str)],
        outputs: &[(&str, &str)],
        env_id: &str,
        payload: &ParDoPayload,
    ) -> PTransformNode {
        PTransformNode {
            id: id.to_string(),
            transform: PTransform {
                unique_name: id.to_string(),
                spec: Some(make_pardo_spec(urn, payload)),
                inputs: inputs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                outputs: outputs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                environment_id: env_id.to_string(),
                ..Default::default()
            },
        }
    }

    /// A ParDoPayload with no side inputs, state, or timers, but with a non-default
    /// `do_fn` so it encodes to non-empty bytes — required because
    /// `can_fuse_pardo` treats an empty encoded payload as "unknown, don't fuse,"
    /// and `ParDoPayload::default().encode_to_vec()` is empty (protobuf omits
    /// all-default fields), so `ParDoPayload::default()` alone does not pass
    /// the `!s.payload.is_empty()` guard.
    fn clean_pardo_payload() -> ParDoPayload {
        ParDoPayload {
            do_fn: Some(FunctionSpec {
                urn: "test:dofn".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Create a PCollectionNode.
    fn make_pcol(id: &str) -> PCollectionNode {
        PCollectionNode {
            id: id.to_string(),
            collection: PCollection {
                unique_name: id.to_string(),
                coder_id: "test_coder".to_string(),
                ..Default::default()
            },
        }
    }

    /// Assemble a QueryablePipeline from transform nodes, pcollection nodes and environments.
    fn build_pipeline(
        transforms: Vec<PTransformNode>,
        pcollections: Vec<PCollectionNode>,
        environments: HashMap<String, Environment>,
    ) -> QueryablePipeline {
        let mut components = Components::default();
        for t in &transforms {
            components
                .transforms
                .insert(t.id.clone(), t.transform.clone());
        }
        for p in &pcollections {
            components
                .pcollections
                .insert(p.id.clone(), p.collection.clone());
        }
        for (id, env) in environments {
            components.environments.insert(id, env);
        }
        QueryablePipeline::new(&components)
    }

    /// Convenience: build pipeline, extract root transforms, then produce
    /// (initial_unfused_pt, initial_consumers) suitable for fuse_pipeline.
    fn extract_initial_sets(
        pipeline: &QueryablePipeline,
        fuser: &GreedyPipelineFuser,
    ) -> (HashSet<PTransformNode>, BTreeSet<CollectionConsumers>) {
        let roots = pipeline.get_root_transforms();
        let mut unfused = HashSet::new();
        let mut consumers = BTreeSet::new();
        for root in &roots {
            let desc = fuser.get_root_consumers(root.clone());
            unfused.extend(desc.get_unfusible().iter().cloned());
            consumers.extend(desc.get_fusible().iter().cloned());
        }
        (unfused, consumers)
    }

    /// Find an ExecutableStage whose transforms set contains a transform with the given id.
    fn find_stage_with<'a>(
        stages: &'a IndexSet<ExecutableStage>,
        transform_id: &str,
    ) -> Option<&'a ExecutableStage> {
        stages.iter().find(|s| {
            let xforms = s.transforms();
            xforms.iter().any(|t| t.id.as_str() == transform_id)
        })
    }

    //
    // Group 1: baseline fusion regression
    //

    /// Impulse -> ParDo(A) -> ParDo(B) -> GroupByKey, same env, no side-inputs/state/timers.
    /// Expect: exactly one ExecutableStage containing both A and B, GBK in runner_stages.
    #[test]
    fn linear_chain_fuses_into_one_stage() {
        let env = make_env("env1");
        // ParDoPayload::default() decodes successfully with empty side_inputs/
        // state_specs/timer_family_specs, which is required for can_fuse_pardo
        // to return true (it checks !s.payload.is_empty()).
        let clean = clean_pardo_payload();
        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0")],
            "",
        );
        let pardo_a = make_pardo_transform(
            "pardo_a",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
            &clean,
        );
        let pardo_b = make_pardo_transform(
            "pardo_b",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p1")],
            &[("out", "p2")],
            "env1",
            &clean,
        );
        let gbk = make_transform(
            "gbk",
            beam_urns::GROUP_BY_KEY_TRANSFORM,
            &[("in", "p2")],
            &[("out", "p3")],
            "",
        );
        let pcols = vec![
            make_pcol("p0"),
            make_pcol("p1"),
            make_pcol("p2"),
            make_pcol("p3"),
        ];
        let transforms = vec![impulse, pardo_a, pardo_b, gbk];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env);

        let pipeline = build_pipeline(transforms, pcols, envs);
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        let sdk_stages = fused.sdk_stages();
        let runner_stages = fused.runner_stages();

        // Exactly one SDK stage
        assert_eq!(
            sdk_stages.len(),
            1,
            "Expected exactly 1 SDK stage, got {}",
            sdk_stages.len()
        );
        let stage = sdk_stages.iter().next().unwrap();
        let stage_transforms = stage.transforms();
        let stage_transform_ids: HashSet<&str> =
            stage_transforms.iter().map(|t| t.id.as_str()).collect();
        assert!(
            stage_transform_ids.contains("pardo_a"),
            "Stage should contain pardo_a"
        );
        assert!(
            stage_transform_ids.contains("pardo_b"),
            "Stage should contain pardo_b"
        );

        // GBK appears in runner_stages (unfused)
        let runner_ids: HashSet<&str> = runner_stages.iter().map(|t| t.id.as_str()).collect();
        assert!(
            runner_ids.contains("gbk"),
            "GBK should be in runner_stages, got: {runner_ids:?}"
        );
        assert!(
            !stage_transform_ids.contains("gbk"),
            "GBK should NOT be in the SDK stage"
        );
    }

    /// Two ParDos with different environment_ids should produce two separate ExecutableStages.
    #[test]
    fn different_environments_split_into_separate_stages() {
        let env_a = make_env("env_a");
        let env_b = make_env("env_b");
        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0")],
            "",
        );
        let pardo_a = make_transform(
            "pardo_a",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p1")],
            "env_a",
        );
        let pardo_b = make_transform(
            "pardo_b",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p1")],
            &[("out", "p2")],
            "env_b",
        );
        let pcols = vec![make_pcol("p0"), make_pcol("p1"), make_pcol("p2")];
        let transforms = vec![impulse, pardo_a, pardo_b];
        let mut envs = HashMap::new();
        envs.insert("env_a".to_string(), env_a);
        envs.insert("env_b".to_string(), env_b);

        let pipeline = build_pipeline(transforms, pcols, envs);
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        let sdk_stages = fused.sdk_stages();
        assert_eq!(
            sdk_stages.len(),
            2,
            "Expected 2 separate SDK stages for different environments, got {}",
            sdk_stages.len()
        );

        let ids_in_stages: Vec<HashSet<String>> = sdk_stages
            .iter()
            .map(|s| {
                let xforms = s.transforms();
                xforms.iter().map(|t| t.id.clone()).collect()
            })
            .collect();
        let mut found_a = false;
        let mut found_b = false;
        for ids in &ids_in_stages {
            if ids.contains("pardo_a") {
                found_a = true;
            }
            if ids.contains("pardo_b") {
                found_b = true;
            }
            assert!(
                !(ids.contains("pardo_a") && ids.contains("pardo_b")),
                "pardo_a and pardo_b should not be in the same stage"
            );
        }
        assert!(found_a, "pardo_a not found in any stage");
        assert!(found_b, "pardo_b not found in any stage");
    }

    /// A PCollection with one PerElement ParDo consumer and one Singleton (side-input)
    /// consumer should be materialized — the PerElement consumer does NOT inline-fuse.
    #[test]
    fn side_input_consumer_materializes_producer() {
        let env = make_env("env1");

        // ParDoPayload for the side-input consumer: declares "side" as a side input.
        let mut side_payload = ParDoPayload::default();
        side_payload
            .side_inputs
            .insert("side".to_string(), SideInput::default());

        // impulse -> p0_raw (root, no env)
        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0_raw")],
            "",
        );
        // SDK-side intermediate: consumes p0_raw -> produces p0.
        // This ensures p0 enters GreedyStageFuser::fuse's fusion_candidates queue,
        // where can_fuse will run its get_singleton_consumers(p0) check.
        let producer_pardo = make_pardo_transform(
            "producer_pardo",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0_raw")],
            &[("out", "p0")],
            "env1",
            &ParDoPayload::default(),
        );
        // Per-element consumer of p0
        let main_pardo = make_pardo_transform(
            "main_pardo",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p_main")],
            "env1",
            &ParDoPayload::default(),
        );
        // Side-input consumer: main input from p0_alt, side input = p0
        let side_pardo = make_pardo_transform(
            "side_pardo",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0_alt"), ("side", "p0")],
            &[("out", "p_side")],
            "env1",
            &side_payload,
        );
        let impulse2 = make_transform(
            "impulse2",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0_alt")],
            "",
        );

        let pcols = vec![
            make_pcol("p0_raw"),
            make_pcol("p0"),
            make_pcol("p0_alt"),
            make_pcol("p_main"),
            make_pcol("p_side"),
        ];
        let transforms = vec![impulse, impulse2, producer_pardo, main_pardo, side_pardo];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env);

        let pipeline = build_pipeline(transforms, pcols, envs);
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        let sdk_stages = fused.sdk_stages();

        // p0 should be materialized (appear as a stage output) when it has a
        // side-input consumer, because can_fuse sees get_singleton_consumers(p0)
        // is non-empty and returns MATERIALIZE.
        let all_outputs: HashSet<String> = sdk_stages
            .iter()
            .flat_map(|s| {
                let outputs = s.output_pcols();
                outputs.iter().map(|p| p.id.clone()).collect::<Vec<_>>()
            })
            .collect();
        assert!(
            all_outputs.contains("p0"),
            "p0 should be materialized (appear as stage output) when it has a side-input consumer.\
             Outputs: {all_outputs:?}"
        );
    }

    /// A ParDo with non-empty state_specs should NOT be inline-fused — its input
    /// PCollection must be materialized.
    #[test]
    fn stateful_pardo_is_not_inline_fused() {
        let env = make_env("env1");
        let mut payload = ParDoPayload::default();
        payload
            .state_specs
            .insert("my_state".to_string(), StateSpec::default());

        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0")],
            "",
        );
        let stateful_pardo = make_pardo_transform(
            "stateful",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
            &payload,
        );
        let downstream = make_transform(
            "downstream",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p1")],
            &[("out", "p2")],
            "env1",
        );

        let pcols = vec![make_pcol("p0"), make_pcol("p1"), make_pcol("p2")];
        let transforms = vec![impulse, stateful_pardo.clone(), downstream];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env.clone());

        let pipeline = build_pipeline(transforms, pcols, envs);
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        // stateful_pardo should be in its own stage (not fused with downstream).
        let _stateful_stage = find_stage_with(&fused.sdk_stages(), "stateful")
            .expect("stateful_pardo not found in any stage");
        let downstream_stage = find_stage_with(&fused.sdk_stages(), "downstream")
            .expect("downstream not found in any stage");

        // Verify they are in different stages.
        let downstream_xforms = downstream_stage.transforms();
        let stateful_in_downstream = downstream_xforms.iter().any(|t| t.id == "stateful");
        assert!(
            !stateful_in_downstream,
            "stateful_pardo should NOT be fused into the same stage as downstream"
        );

        // Also verify can_fuse returns false for this transform.
        assert!(
            !payload.state_specs.is_empty(),
            "fixture sanity: state_specs should be non-empty"
        );
        let can = GreedyCollectionFuser::can_fuse(
            &stateful_pardo,
            &env,
            &make_pcol("p0"),
            &fuser.pipeline,
        );
        assert!(!can, "can_fuse should be false for stateful ParDo");
    }

    /// A ParDo with non-empty timer_family_specs should NOT be inline-fused.
    #[test]
    fn timer_pardo_is_not_inline_fused() {
        let env = make_env("env1");
        let mut payload = ParDoPayload::default();
        payload
            .timer_family_specs
            .insert("my_timer".to_string(), TimerFamilySpec::default());

        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0")],
            "",
        );
        let timer_pardo = make_pardo_transform(
            "timer_pardo",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
            &payload,
        );
        let downstream = make_transform(
            "downstream",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p1")],
            &[("out", "p2")],
            "env1",
        );

        let pcols = vec![make_pcol("p0"), make_pcol("p1"), make_pcol("p2")];
        let transforms = vec![impulse, timer_pardo.clone(), downstream];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env.clone());

        let pipeline = build_pipeline(transforms, pcols, envs);
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        let _timer_stage =
            find_stage_with(&fused.sdk_stages(), "timer_pardo").expect("timer_pardo not found");
        let downstream_stage =
            find_stage_with(&fused.sdk_stages(), "downstream").expect("downstream not found");

        let downstream_xforms = downstream_stage.transforms();
        let timer_in_downstream = downstream_xforms.iter().any(|t| t.id == "timer_pardo");
        assert!(
            !timer_in_downstream,
            "timer_pardo should NOT be fused into the same stage as downstream"
        );

        // Verify can_fuse returns false.
        assert!(
            !payload.timer_family_specs.is_empty(),
            "fixture sanity: timer_family_specs should be non-empty"
        );
        let can =
            GreedyCollectionFuser::can_fuse(&timer_pardo, &env, &make_pcol("p0"), &fuser.pipeline);
        assert!(!can, "can_fuse should be false for timer ParDo");
    }

    /// par_do_compatibility(transform, transform, pipeline) must return true —
    /// the explicit self-loop exception for state/timer ParDos.
    #[test]
    fn stateful_pardo_self_loop_is_compatible() {
        let env = make_env("env1");
        let mut payload = ParDoPayload::default();
        payload
            .state_specs
            .insert("s".to_string(), StateSpec::default());
        payload
            .timer_family_specs
            .insert("t".to_string(), TimerFamilySpec::default());

        let pardo = make_pardo_transform(
            "sdf_pardo",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
            &payload,
        );

        let pcols = vec![make_pcol("p0"), make_pcol("p1")];
        let transforms = vec![pardo.clone()];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env);
        let pipeline = build_pipeline(transforms, pcols, envs);

        // Even though this transform has state + timers (which normally block fusion),
        // par_do_compatibility with itself must return true (self-loop exception).
        let compatible = GreedyCollectionFuser::par_do_compatibility(&pardo, &pardo, &pipeline);
        assert!(
            compatible,
            "par_do_compatibility must return true for a ParDo compared with itself (self-loop)"
        );

        // But can_fuse still returns false (it doesn't have the self-loop exception).
        let can =
            GreedyCollectionFuser::can_fuse(&pardo, &make_env("env1"), &make_pcol("p0"), &pipeline);
        assert!(
            !can,
            "can_fuse should still return false for stateful ParDo (self-loop is only in par_do_compatibility)"
        );
    }

    //
    // Group 2: fan-out / multi-consumer
    //

    /// A PCollection with two per-element ParDo consumers (same env, no side-inputs)
    /// should fuse both into the same ExecutableStage.
    #[test]
    fn compatible_siblings_fuse_into_one_stage() {
        let env = make_env("env1");
        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0")],
            "",
        );
        let pardo_a = make_transform(
            "pardo_a",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "pa")],
            "env1",
        );
        let pardo_b = make_transform(
            "pardo_b",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "pb")],
            "env1",
        );

        let pcols = vec![make_pcol("p0"), make_pcol("pa"), make_pcol("pb")];
        let transforms = vec![impulse, pardo_a, pardo_b];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env);

        let pipeline = build_pipeline(transforms, pcols, envs);
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        let sdk_stages = fused.sdk_stages();
        // Both siblings should be in the same stage.
        let both_stage = sdk_stages.iter().find(|s| {
            let xforms = s.transforms();
            let ids: HashSet<&str> = xforms.iter().map(|t| t.id.as_str()).collect();
            ids.contains("pardo_a") && ids.contains("pardo_b")
        });
        assert!(
            both_stage.is_some(),
            "pardo_a and pardo_b should be in the same ExecutableStage"
        );
    }

    /// With bidirectional compatibility
    #[test]
    fn side_input_bearing_pardo_is_incompatible_with_plain_sibling() {
        let env = make_env("env1");

        let mut side_payload = ParDoPayload::default();
        side_payload
            .side_inputs
            .insert("side".to_string(), SideInput::default());

        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0")],
            "",
        );
        // Separate source for the side input PCollection.
        let impulse_side = make_transform(
            "impulse_side",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p_side_src")],
            "",
        );
        // Plain sibling — consumes p0 as main PerElement input, no side inputs.
        let plain = make_pardo_transform(
            "plain",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p_plain")],
            "env1",
            &clean_pardo_payload(),
        );
        // Side-input sibling — consumes p0 as main PerElement input AND
        // p_side_src as a Singleton (side) input.
        let side_pardo = make_pardo_transform(
            "side_pardo",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0"), ("side", "p_side_src")],
            &[("out", "p_side")],
            "env1",
            &side_payload,
        );

        let pcols = vec![
            make_pcol("p0"),
            make_pcol("p_side_src"),
            make_pcol("p_plain"),
            make_pcol("p_side"),
        ];
        let transforms = vec![impulse, impulse_side, plain.clone(), side_pardo.clone()];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env);

        let pipeline = build_pipeline(transforms, pcols, envs);

        // Bidirectional is_compatible: side_pardo's payload has side inputs,
        // so is_compatible_one_way(side_pardo, plain) returns false.
        let compatible = GreedyCollectionFuser::is_compatible(&plain, &side_pardo, &pipeline);
        assert!(
            !compatible,
            "side-input-bearing ParDo must be incompatible with plain sibling (bidirectional check)"
        );

        // Run full fusion: they end up in separate stages.
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        let same_stage = fused.sdk_stages().iter().any(|s| {
            let xforms = s.transforms();
            let ids: HashSet<&str> = xforms.iter().map(|t| t.id.as_str()).collect();
            ids.contains("plain") && ids.contains("side_pardo")
        });
        assert!(
            !same_stage,
            "plain and side_pardo should be in separate stages"
        );
    }

    //
    // Group 3: fan-in / multi-producer dedup
    //

    /// When two stages both claim to produce the same PCollection ID,
    /// ensure_single_producer must insert a Flatten with one input per original producer.
    #[test]
    fn duplicate_producer_gets_flatten_inserted() {
        let common_pcol_id = "shared_out";

        // Build two minimal ExecutableStages that both output `shared_out`.
        let env = make_env("env1");
        let wire_coders = HashSet::<WireCoderSetting>::new();

        let shared_pcol = make_pcol(common_pcol_id);

        let stage_a = ExecutableStage::from(
            Components::default(),
            env.clone(),
            wire_coders.clone(),
            make_pcol("in_a"),
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            [shared_pcol.clone()].into_iter().collect(),
            IndexSet::new(),
        );
        let stage_b = ExecutableStage::from(
            Components::default(),
            env.clone(),
            wire_coders.clone(),
            make_pcol("in_b"),
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            [shared_pcol.clone()].into_iter().collect(),
            IndexSet::new(),
        );

        // Build a QueryablePipeline where the shared PCollection exists.
        let pcols = vec![shared_pcol.clone(), make_pcol("in_a"), make_pcol("in_b")];
        let transforms: Vec<PTransformNode> = vec![];
        let envs: HashMap<String, Environment> = HashMap::new();
        let pipeline = build_pipeline(transforms, pcols, envs);

        let stages: IndexSet<ExecutableStage> =
            [stage_a.clone(), stage_b.clone()].into_iter().collect();
        let unfused: IndexSet<PTransformNode> = IndexSet::new();

        let result = ensure_single_producer(&pipeline, &stages, &unfused).unwrap();

        // There should be exactly one Flatten introduced.
        let flattens: Vec<&PTransformNode> = result
            .introduced_transforms
            .iter()
            .filter(|t| {
                t.transform
                    .spec
                    .as_ref()
                    .map_or(false, |s| s.urn == beam_urns::FLATTEN_TRANSFORM)
            })
            .collect();
        assert_eq!(flattens.len(), 1, "Expected exactly 1 Flatten introduced");

        let flatten = flattens[0];
        // The Flatten's inputs should have one entry per original producer.
        assert_eq!(
            flatten.transform.inputs.len(),
            2,
            "Flatten should have 2 inputs (one per original producer)"
        );
        // Its single output should equal the original (pre-dedup) PCollection ID.
        assert_eq!(
            flatten.transform.outputs.get("output"),
            Some(&common_pcol_id.to_string()),
            "Flatten output should be the original PCollection ID"
        );
    }

    /// When an ExecutableStage has a transform input referencing a PCollection ID
    /// that is not the stage input, not an internal output, not a side input, and
    /// not a timer input, sanitize_dangling_ptransform_inputs must remove it.
    #[test]
    fn dangling_input_is_sanitized() {
        let env = make_env("env1");
        let wire_coders = HashSet::<WireCoderSetting>::new();

        let stage_input = make_pcol("stage_input");
        let internal_output = make_pcol("internal_out");
        let dangling = make_pcol("dangling");

        // A transform that consumes both a valid internal output AND a dangling PCollection.
        let transform_in_stage = PTransformNode {
            id: "t1".to_string(),
            transform: PTransform {
                unique_name: "t1".to_string(),
                spec: Some(make_spec(beam_urns::PAR_DO_TRANSFORM)),
                inputs: [
                    ("in".to_string(), internal_output.id.clone()),
                    ("dangling_input".to_string(), dangling.id.clone()),
                ]
                .into_iter()
                .collect(),
                outputs: [("out".to_string(), "extra".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        };

        // The producer of internal_out.
        let producer = PTransformNode {
            id: "producer".to_string(),
            transform: PTransform {
                unique_name: "producer".to_string(),
                spec: Some(make_spec(beam_urns::PAR_DO_TRANSFORM)),
                inputs: [("in".to_string(), stage_input.id.clone())]
                    .into_iter()
                    .collect(),
                outputs: [("out".to_string(), internal_output.id.clone())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        };

        let mut components = Components::default();
        components
            .transforms
            .insert("t1".to_string(), transform_in_stage.transform.clone());
        components
            .transforms
            .insert("producer".to_string(), producer.transform.clone());
        components
            .pcollections
            .insert(stage_input.id.clone(), stage_input.collection.clone());
        components.pcollections.insert(
            internal_output.id.clone(),
            internal_output.collection.clone(),
        );
        components
            .pcollections
            .insert(dangling.id.clone(), dangling.collection.clone());

        let stage = ExecutableStage::from(
            components,
            env,
            wire_coders,
            stage_input.clone(),
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            [make_pcol("extra")].into_iter().collect(),
            [producer, transform_in_stage].into_iter().collect(),
        );

        let sanitized = sanitize_dangling_ptransform_inputs(stage);

        // The dangling input key should be removed from t1's inputs.
        let sanitized_transforms = sanitized.transforms();
        let t1_sanitized = sanitized_transforms
            .iter()
            .find(|t| t.id == "t1")
            .expect("t1 should still be present");
        assert!(
            !t1_sanitized.transform.inputs.contains_key("dangling_input"),
            "dangling_input key should be removed from t1's inputs"
        );
        assert!(
            t1_sanitized.transform.inputs.contains_key("in"),
            "valid input 'in' should be preserved"
        );

        // The dangling PCollection should be absent from the sanitized components.
        assert!(
            !sanitized.components().pcollections.contains_key("dangling"),
            "dangling PCollection should be removed from stage components"
        );
        // Valid PCollections should remain.
        assert!(
            sanitized
                .components()
                .pcollections
                .contains_key("stage_input"),
            "stage_input should remain"
        );
        assert!(
            sanitized
                .components()
                .pcollections
                .contains_key("internal_out"),
            "internal_out should remain"
        );
    }

    //
    // Group 4: SDF-specific tests

    /// PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS is isolated into its own stage
    /// by can_fuse returning false. We test this directly because the URN is
    /// not in PRIMITIVES, making full fuse_pipeline unreachable.
    #[test]
    fn sdf_process_transform_is_isolated_into_own_stage() {
        // NOTE: Because SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN is
        // not in beam_urns::PRIMITIVES, this transform won't appear in the pipeline
        // graph.  We test can_fuse directly — the dispatch at line 407-411 returns
        // false unconditionally for this URN.
        let env = make_env("env1");
        let process = make_transform(
            "process_sized",
            beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
        );

        let pcols = vec![make_pcol("p0"), make_pcol("p1")];
        let transforms = vec![process.clone()];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env.clone());
        let pipeline = build_pipeline(transforms, pcols, envs);

        // can_fuse must return false for PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS.
        let can = GreedyCollectionFuser::can_fuse(&process, &env, &make_pcol("p0"), &pipeline);
        assert!(
            !can,
            "can_fuse must return false for SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN"
        );

        // Also verify the URN is present in the dispatch match arm (compile-time check).
        // If can_fuse is extended, this arm must remain false.
    }

    /// PAIR_WITH_RESTRICTION and SPLIT_AND_SIZE_RESTRICTIONS can fuse together
    /// (both hit the can_fuse_pardo dispatch arm). We verify this via can_fuse.
    ///
    /// This is checking the forward-extension path: can_fuse_pardo is the dispatch
    /// for both URNs (line 402), so they can fuse inline.  However, SPLIT_AND_SIZE_
    /// RESTRICTIONS is absent from is_compatible's sibling-grouping match arms, so
    /// siblings cannot root a stage together — that asymmetry is intentional because
    /// root/sibling compatibility vs. forward-extension compatibility are different
    /// code paths.
    #[test]
    fn pair_with_restriction_fuses_with_split_and_size() {
        let env = make_env("env1");

        // Because these URNs are not in PRIMITIVES, we test the can_fuse path directly.
        // clean_pardo_payload() encodes to non-empty bytes so can_fuse_pardo
        // passes the !s.payload.is_empty() guard.
        let clean = clean_pardo_payload();
        let pair = make_pardo_transform(
            "pair",
            beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
            &clean,
        );
        let split = make_pardo_transform(
            "split",
            beam_urns::SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
            &[("in", "p1")],
            &[("out", "p2")],
            "env1",
            &clean,
        );

        let pcols = vec![make_pcol("p0"), make_pcol("p1"), make_pcol("p2")];
        let transforms = vec![pair.clone(), split.clone()];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env.clone());
        let pipeline = build_pipeline(transforms, pcols, envs);

        // Both should be fusible via can_fuse (they hit can_fuse_pardo).
        let can_pair = GreedyCollectionFuser::can_fuse(&pair, &env, &make_pcol("p0"), &pipeline);
        assert!(can_pair, "PAIR_WITH_RESTRICTION should be fusible");

        let can_split = GreedyCollectionFuser::can_fuse(&split, &env, &make_pcol("p1"), &pipeline);
        assert!(can_split, "SPLIT_AND_SIZE_RESTRICTIONS should be fusible");
    }

    /// A PCollection with two consumers: one plain PAR_DO_TRANSFORM and one
    /// SPLITTABLE_PAIR_WITH_RESTRICTION_URN (both side-input/state/timer-free,
    /// same env). Check whether GreedyCollectionFuser::is_compatible returns
    /// true — they share the par_do_compatibility branch.
    #[test]
    fn pair_with_restriction_can_sibling_with_plain_pardo() {
        let env = make_env("env1");

        let plain = make_transform(
            "plain",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p_plain")],
            "env1",
        );
        let pair = make_transform(
            "pair",
            beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
            &[("in", "p0")],
            &[("out", "p_pair")],
            "env1",
        );

        let pcols = vec![make_pcol("p0"), make_pcol("p_plain"), make_pcol("p_pair")];
        let transforms = vec![plain.clone(), pair.clone()];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env);
        let pipeline = build_pipeline(transforms, pcols, envs);

        let result = GreedyCollectionFuser::is_compatible(&pair, &plain, &pipeline);
        // Both hit the par_do_compatibility branch (line 327-331). Since neither
        // has side inputs, state, or timers, and they share the same environment,
        // the current code returns true.
        assert!(
            result,
            "current behavior: PairWithRestriction IS compatible with plain ParDo for sibling fusion.\
             REVIEW: is this intended for FlareDB's SDF model?"
        );
    }

    /// A plain ParDo consuming the output of PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS
    /// fuses into the same stage when using PAR_DO_TRANSFORM as a stand-in for the
    /// SDF process transform (since the real SDF URN is not in PRIMITIVES).
    ///
    /// This confirms that nothing in can_fuse's dispatch keys off "am I downstream
    /// of an SDF transform" — only the candidate's own consumer URN matters. This
    /// is intentional/current-behavior and matters for SdfStageExecutor: residual
    /// bundle resubmission carries any fused downstream logic along with it.
    #[test]
    fn downstream_pardo_fuses_into_sdf_stage() {
        let env = make_env("env1");

        // Simulate: upstream ParDo (stand-in for SDF process) -> downstream plain ParDo.
        // Both use PAR_DO_TRANSFORM so they appear in the graph and can be fused.
        // clean_pardo_payload() encodes to non-empty bytes so can_fuse_pardo returns true.
        let clean = clean_pardo_payload();
        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0")],
            "",
        );
        let upstream = make_pardo_transform(
            "upstream",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
            &clean,
        );
        let downstream = make_pardo_transform(
            "downstream",
            beam_urns::PAR_DO_TRANSFORM,
            &[("in", "p1")],
            &[("out", "p2")],
            "env1",
            &clean,
        );

        let pcols = vec![make_pcol("p0"), make_pcol("p1"), make_pcol("p2")];
        let transforms = vec![impulse, upstream, downstream];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env);

        let pipeline = build_pipeline(transforms, pcols, envs);
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        // Both upstream and downstream should end up in the same ExecutableStage.
        let stages = fused.sdk_stages();
        let combined = stages.iter().find(|s| {
            let xforms = s.transforms();
            let ids: HashSet<&str> = xforms.iter().map(|t| t.id.as_str()).collect();
            ids.contains("upstream") && ids.contains("downstream")
        });
        assert!(
            combined.is_some(),
            "downstream should fuse into same stage as upstream (current behavior for SdfStageExecutor)"
        );
    }

    /// TRUNCATE_SIZED_RESTRICTION can be a sibling at root/group formation (it has
    /// an is_compatible arm at line 329) but can NEVER be extended forward into an
    /// existing stage via can_fuse — there is no dispatch arm for it:
    /// can_fuse's match only lists PAR_DO_TRANSFORM, PAIR_WITH_RESTRICTION,
    /// and SPLIT_AND_SIZE_RESTRICTIONS for can_fuse_pardo.  TRUNCATE_SIZED_
    /// RESTRICTION falls into the `unknown => false` catch-all.
    ///
    /// This may be intentional (drain-only steps might always form a stage
    /// boundary) or a missing dispatch arm — flag for review before any drain-mode
    /// work is built on top of it.
    ///
    /// This is a placeholder for future drain-mode support and not currently
    /// exercised by the runner.
    #[test]
    fn truncate_sized_restriction_fuses_like_pair_with_restriction() {
        let env = make_env("env1");

        let trunc = make_transform(
            "trunc",
            beam_urns::SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
        );

        let pcols = vec![make_pcol("p0"), make_pcol("p1")];
        let transforms = vec![trunc.clone()];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env.clone());
        let pipeline = build_pipeline(transforms, pcols, envs);

        // can_fuse has no arm for TRUNCATE_SIZED_RESTRICTION_URN — it falls into
        // the `unknown => false` catch-all.  This is NOT the same dispatch arm as
        // PAIR_WITH_RESTRICTION/SPLIT_AND_SIZE_RESTRICTIONS, which correctly hit
        // can_fuse_pardo.
        let can = GreedyCollectionFuser::can_fuse(&trunc, &env, &make_pcol("p0"), &pipeline);
        assert!(
            !can,
            "TRUNCATE_SIZED_RESTRICTION falls into can_fuse's unknown => false catch-all"
        );
    }

    #[test]
    fn sdf_pipeline_fuses_end_to_end_with_process_transform_isolated() {
        let env = make_env("env1");
        let clean = clean_pardo_payload();

        let impulse = make_transform(
            "impulse",
            beam_urns::IMPULSE_TRANSFORM,
            &[],
            &[("out", "p0")],
            "",
        );
        let pair = make_pardo_transform(
            "pair",
            beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
            &[("in", "p0")],
            &[("out", "p1")],
            "env1",
            &clean,
        );
        let split = make_pardo_transform(
            "split",
            beam_urns::SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
            &[("in", "p1")],
            &[("out", "p2")],
            "env1",
            &clean,
        );
        let process = make_pardo_transform(
            "process",
            beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
            &[("in", "p2")],
            &[("out", "p3")],
            "env1",
            &clean,
        );

        let pcols = vec![
            make_pcol("p0"),
            make_pcol("p1"),
            make_pcol("p2"),
            make_pcol("p3"),
        ];
        let transforms = vec![impulse, pair, split, process];
        let mut envs = HashMap::new();
        envs.insert("env1".to_string(), env);

        let pipeline = build_pipeline(transforms, pcols, envs);
        let fuser = GreedyPipelineFuser::with(pipeline);
        let (initial_unfused, initial_consumers) = extract_initial_sets(&fuser.pipeline, &fuser);
        let fused = fuser
            .fuse_pipeline(initial_unfused, initial_consumers)
            .unwrap();

        let stages = fused.sdk_stages();

        // pair and split should be fused together in one stage.
        let pair_split_stage = stages.iter().find(|s| {
            let xforms = s.transforms();
            let ids: HashSet<&str> = xforms.iter().map(|t| t.id.as_str()).collect();
            ids.contains("pair") && ids.contains("split")
        });
        assert!(
            pair_split_stage.is_some(),
            "pair and split should be fused into one stage"
        );

        // process must be isolated in its own stage — NOT with pair/split.
        let process_stage =
            find_stage_with(&stages, "process").expect("process transform should be in some stage");
        let process_xforms = process_stage.transforms();
        let process_stage_ids: HashSet<String> =
            process_xforms.iter().map(|t| t.id.clone()).collect();
        assert_eq!(
            process_stage_ids.len(),
            1,
            "process transform must be the sole member of its stage, got: {process_stage_ids:?}"
        );
        assert!(process_stage_ids.contains("process"));

        // Confirm pair/split stage and process stage are genuinely different stages.
        let pair_split_stage = pair_split_stage.unwrap();
        assert_ne!(
            pair_split_stage.id(),
            process_stage.id(),
            "pair/split stage and process stage must be different ExecutableStages"
        );
    }
}
