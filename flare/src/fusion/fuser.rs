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
    errors::BeamTranslationError,
    fusion::{
        pipeline::{
            FusedPipeline, PCollectionNode, PTransformNode, PipelineEdge, QueryablePipeline,
        },
        refs::{SideInputRef, TimerRef, UserStateRef},
        stage::{CollectionConsumers, DescendantConsumers, ExecutableStage, SiblingKey},
    },
    jobservice::urns,
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
            dedup.components(),
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

        Ok(ExecutableStage::from(
            pipeline.components().clone(),
            env,
            HashSet::<WireCoderSetting>::new(),
            input_pcol.clone(),
            side_inputs,
            user_states,
            timers,
            materialized_pcols,
            fused_transforms,
        ))
    }
}

struct GreedyCollectionFuser {}

impl GreedyCollectionFuser {
    fn is_compatible(
        node: &PTransformNode,
        other: &PTransformNode,
        pipeline: &QueryablePipeline,
    ) -> bool {
        let urn = get_urn(node);

        match urn {
            // ParDo family: compatible if no side-inputs/state/timers + same env
            urns::beam_urns::PAR_DO_TRANSFORM
            | urns::beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN
            | urns::beam_urns::SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN => {
                Self::par_do_compatibility(node, other, pipeline)
            }

            // Combine sub-components + window assignment: compatible if same env
            u if urns::beam_urns::COMBINE_COMPONENTS.contains(&u)
                || u == urns::beam_urns::ASSIGN_WINDOWS_TRANSFORM =>
            {
                Self::compatible_environments(node, other, pipeline)
            }

            // Flatten, GBK, Impulse: no sibling fusion
            urns::beam_urns::FLATTEN_TRANSFORM => false,
            u if urns::beam_urns::FLARE.contains(&u) => false,

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
            || (!Self::has_side_inputs(par_do, pipeline)
                && !Self::has_state_or_timers(par_do)
                && Self::compatible_environments(par_do, other, pipeline))
    }

    /// Returns `true` if this transform consumes any side-input (`Singleton`) edges.
    fn has_side_inputs(transform: &PTransformNode, pipeline: &QueryablePipeline) -> bool {
        if let Some(&node_idx) = pipeline.transform_ids().get(&transform.id) {
            pipeline
                .graph()
                .edges_directed(node_idx, petgraph::Direction::Incoming)
                .any(|edge| matches!(edge.weight(), PipelineEdge::Singleton))
        } else {
            false
        }
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

            u if urns::beam_urns::COMBINE_COMPONENTS.contains(&u)
                || u == urns::beam_urns::ASSIGN_WINDOWS_TRANSFORM =>
            {
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

        // Can't fuse if it has side inputs
        if any_sideinputs(&pardo, pipeline) {
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
