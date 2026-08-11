use crate::jobservice::urns;
use beam_model_rs::v1::{
    Coder, Components, FunctionSpec, MessageWithComponents, PCollection, PTransform, ParDoPayload,
    Pipeline, message_with_components,
};
use prost::Message;
use std::collections::HashMap;

/// A function that takes a transform id and existing components, returning
/// the replacement `MessageWithComponents`, or `None` if no replacement
/// should be performed.
///
/// The returned `MessageWithComponents` must contain a single `PTransform`
/// as the root message. Its `Components` will be merged into the existing
/// pipeline components.
pub trait TransformReplacement {
    fn get_replacement(
        &self,
        transform_id: &str,
        existing_components: &Components,
    ) -> Option<MessageWithComponents>;
}

/// Applies [`TransformReplacement`]s to a [`Pipeline`].
pub struct ProtoOverrides;

impl ProtoOverrides {
    /// Update all transforms in `pipeline` whose spec URN equals `urn` using
    /// the supplied `replacement`.
    ///
    /// Returns a new `Pipeline` with the replacements applied.
    pub fn update_transform(
        urn: &str,
        pipeline: &Pipeline,
        replacement: &dyn TransformReplacement,
    ) -> Pipeline {
        let components = pipeline
            .components
            .as_ref()
            .expect("Pipeline must have components");

        let mut result_components = components.clone();

        // Snapshot the existing transform ids – we must iterate over this
        // snapshot because the map may be mutated by remove_subtransforms.
        let transform_ids: Vec<String> = result_components.transforms.keys().cloned().collect();

        for transform_id in &transform_ids {
            let transform = match result_components.transforms.get(transform_id) {
                Some(t) => t.clone(),
                // already removed as a sub-transform of another
                None => continue,
            };

            let should_replace = transform
                .spec
                .as_ref()
                .map(|spec| spec.urn == urn)
                .unwrap_or(false);

            if !should_replace {
                continue;
            }

            let updated = replacement.get_replacement(transform_id, &result_components);

            let updated = match updated {
                Some(u) => u,
                None => continue,
            };

            let updated_pt = match &updated.root {
                Some(message_with_components::Root::Ptransform(pt)) => pt,
                _ => continue,
            };

            // Verify that the replacement produces all the original outputs.
            assert_eq!(
                updated_pt.outputs, transform.outputs,
                "TransformReplacement must produce all outputs of the original PTransform"
            );

            // Remove sub-transforms of the original transform recursively.
            Self::remove_subtransforms(&transform, &mut result_components);

            // Merge the replacement's components into the result.
            if let Some(updated_comps) = &updated.components {
                result_components
                    .transforms
                    .extend(updated_comps.transforms.clone());
                result_components
                    .pcollections
                    .extend(updated_comps.pcollections.clone());
                result_components
                    .coders
                    .extend(updated_comps.coders.clone());
                result_components
                    .windowing_strategies
                    .extend(updated_comps.windowing_strategies.clone());
                result_components
                    .environments
                    .extend(updated_comps.environments.clone());
            }

            // Replace the transform with the updated version.
            result_components
                .transforms
                .insert(transform_id.clone(), updated_pt.clone());
        }

        Pipeline {
            components: Some(result_components),
            ..pipeline.clone()
        }
    }

    /// Recursively remove all sub-transforms of `pt` from `target`.
    fn remove_subtransforms(pt: &PTransform, target: &mut Components) {
        for sub_id in &pt.subtransforms {
            if let Some(sub) = target.transforms.get(sub_id).cloned() {
                Self::remove_subtransforms(&sub, target);
                target.transforms.remove(sub_id);
            }
        }
    }
}

/// A set of transform replacements for expanding a splittable ParDo into its
/// sub-components.
pub struct SplittableParDoExpander;

impl SplittableParDoExpander {
    /// Returns a [`SizedReplacement`] that expands a splittable ParDo into
    ///
    /// `PairWithRestriction → SplitAndSize → ProcessSizedElementsAndRestrictions`
    ///
    /// This is the normal (non-drain) expansion.
    pub fn create_sized_replacement() -> SizedReplacement {
        SizedReplacement { is_drain: false }
    }

    /// Returns a [`SizedReplacement`] in drain mode, which additionally inserts
    /// a `TruncateAndSize` step between `SplitAndSize` and
    /// `ProcessSizedElementsAndRestrictions`.
    pub fn create_truncate_replacement() -> SizedReplacement {
        SizedReplacement { is_drain: true }
    }
}

/// The [`TransformReplacement`] that performs the actual expansion logic.
pub struct SizedReplacement {
    is_drain: bool,
}

impl TransformReplacement for SizedReplacement {
    fn get_replacement(
        &self,
        transform_id: &str,
        existing_components: &Components,
    ) -> Option<MessageWithComponents> {
        //  1. Fetch the original splittable ParDo
        let splittable_par_do = existing_components.transforms.get(transform_id)?;

        let spec = splittable_par_do.spec.as_ref()?;

        let payload = ParDoPayload::decode(spec.payload.as_slice()).ok()?;

        // Only expand if this is truly a splittable DoFn.
        if payload.restriction_coder_id.is_empty() {
            return None;
        }

        // 2. Determine main input and side inputs
        let main_input_name = get_main_input_name_from_payload(splittable_par_do, &payload);
        let main_input_pcol_id = splittable_par_do.inputs.get(&main_input_name)?.clone();
        let main_input_pcol = existing_components
            .pcollections
            .get(&main_input_pcol_id)?
            .clone();

        // Side inputs: only those whose local name appears in ParDoPayload.
        let side_inputs: HashMap<String, String> = splittable_par_do
            .inputs
            .iter()
            .filter(|(name, _)| payload.side_inputs.contains_key(*name))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // 3. Build up new components
        let mut rval_components = Components::default();

        // 3a. PairWithRestriction output coder (KV<element, restriction>)
        let pair_with_restriction_out_coder_id = generate_unique_id(
            &format!("{}/PairWithRestriction", main_input_pcol.coder_id),
            |id| existing_components.coders.contains_key(id),
        );
        rval_components.coders.insert(
            pair_with_restriction_out_coder_id.clone(),
            make_kv_coder(&main_input_pcol.coder_id, &payload.restriction_coder_id),
        );

        // 3b. PairWithRestriction output PCollection
        let pair_with_restriction_out_id = generate_unique_id(
            &format!("{}/PairWithRestriction", main_input_pcol_id),
            |id| existing_components.pcollections.contains_key(id),
        );
        rval_components.pcollections.insert(
            pair_with_restriction_out_id.clone(),
            make_pcollection(
                &pair_with_restriction_out_coder_id,
                &main_input_pcol,
                &format!("{}/PairWithRestriction", main_input_pcol.unique_name),
                existing_components,
            ),
        );

        // 3c. SplitAndSize output coder (KV<KV<e,r>, double>)
        let split_and_size_out_coder_id = generate_unique_id(
            &format!("{}/SplitAndSize", main_input_pcol.coder_id),
            |id| existing_components.coders.contains_key(id),
        );
        let double_coder_id = get_or_add_double_coder(existing_components, &mut rval_components);
        rval_components.coders.insert(
            split_and_size_out_coder_id.clone(),
            make_kv_coder(&pair_with_restriction_out_coder_id, &double_coder_id),
        );

        // 3d. SplitAndSize output PCollection
        let split_and_size_out_id =
            generate_unique_id(&format!("{}/SplitAndSize", main_input_pcol_id), |id| {
                existing_components.pcollections.contains_key(id)
            });
        rval_components.pcollections.insert(
            split_and_size_out_id.clone(),
            make_pcollection(
                &split_and_size_out_coder_id,
                &main_input_pcol,
                &format!("{}/SplitAndSize", main_input_pcol.unique_name),
                existing_components,
            ),
        );

        // 3e. PairWithRestriction PTransform
        let pair_with_restriction_id =
            generate_unique_id(&format!("{}/PairWithRestriction", transform_id), |id| {
                existing_components.transforms.contains_key(id)
            });
        {
            let pt = make_sdf_sub_transform(
                urns::beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
                &spec.payload,
                &splittable_par_do.inputs,
                &[("out", &pair_with_restriction_out_id)],
                &format!("{}/PairWithRestriction", splittable_par_do.unique_name),
                &splittable_par_do.environment_id,
                existing_components,
            );
            rval_components
                .transforms
                .insert(pair_with_restriction_id.clone(), pt);
        }

        // 3f. SplitAndSize PTransform
        let split_and_size_id =
            generate_unique_id(&format!("{}/SplitAndSize", transform_id), |id| {
                existing_components.transforms.contains_key(id)
            });
        {
            let mut split_and_size_inputs = HashMap::new();
            split_and_size_inputs.insert(
                main_input_name.clone(),
                pair_with_restriction_out_id.clone(),
            );
            split_and_size_inputs.extend(side_inputs.clone());

            let pt = make_sdf_sub_transform(
                urns::beam_urns::SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
                &spec.payload,
                &split_and_size_inputs,
                &[("out", &split_and_size_out_id)],
                &format!("{}/SplitAndSize", splittable_par_do.unique_name),
                &splittable_par_do.environment_id,
                existing_components,
            );
            rval_components
                .transforms
                .insert(split_and_size_id.clone(), pt);
        }

        //  3g. Build the new composite root (replaces original ParDo)
        let mut new_composite_root = splittable_par_do.clone();
        new_composite_root.spec = None; // clear spec → becomes a composite
        new_composite_root.subtransforms =
            vec![pair_with_restriction_id.clone(), split_and_size_id.clone()];

        //  3h. ProcessSizedElementsAndRestrictions
        let process_sized_elements_id = generate_unique_id(
            &format!("{}/ProcessSizedElementsAndRestrictions", transform_id),
            |id| existing_components.transforms.contains_key(id),
        );
        let mut process_sized_input_pcol_id = split_and_size_out_id.clone();

        // Drain mode: insert TruncateAndSize
        if self.is_drain {
            let truncate_and_size_coder_id = generate_unique_id(
                &format!("{}/TruncateAndSize", main_input_pcol.coder_id),
                |id| existing_components.coders.contains_key(id),
            );
            let double2_id = get_or_add_double_coder(existing_components, &mut rval_components);
            rval_components.coders.insert(
                truncate_and_size_coder_id.clone(),
                make_kv_coder(&split_and_size_out_coder_id, &double2_id),
            );

            let truncate_and_size_out_id =
                generate_unique_id(&format!("{}/TruncateAndSize", main_input_pcol_id), |id| {
                    existing_components.pcollections.contains_key(id)
                });
            rval_components.pcollections.insert(
                truncate_and_size_out_id.clone(),
                make_pcollection(
                    &truncate_and_size_coder_id,
                    &main_input_pcol,
                    &format!("{}/TruncateAndSize", main_input_pcol.unique_name),
                    existing_components,
                ),
            );

            let truncate_and_size_id =
                generate_unique_id(&format!("{}/TruncateAndSize", transform_id), |id| {
                    existing_components.transforms.contains_key(id)
                });
            {
                let mut truncate_inputs = HashMap::new();
                truncate_inputs.insert(main_input_name.clone(), split_and_size_out_id.clone());
                truncate_inputs.extend(side_inputs.clone());

                let pt = make_sdf_sub_transform(
                    urns::beam_urns::SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN,
                    &spec.payload,
                    &truncate_inputs,
                    &[("out", &truncate_and_size_out_id)],
                    &format!("{}/TruncateAndSize", splittable_par_do.unique_name),
                    &splittable_par_do.environment_id,
                    existing_components,
                );
                rval_components
                    .transforms
                    .insert(truncate_and_size_id.clone(), pt);
            }

            new_composite_root.subtransforms.push(truncate_and_size_id);
            process_sized_input_pcol_id = truncate_and_size_out_id;
        }

        // 3i. ProcessSizedElementsAndRestrictions PTransform
        {
            let mut process_inputs = HashMap::new();
            process_inputs.insert(main_input_name, process_sized_input_pcol_id);
            process_inputs.extend(side_inputs);

            let process_outputs: Vec<(&str, &str)> = splittable_par_do
                .outputs
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            let pt = make_sdf_sub_transform(
                urns::beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
                &spec.payload,
                &process_inputs,
                &process_outputs,
                &format!(
                    "{}/ProcessSizedElementsAndRestrictions",
                    splittable_par_do.unique_name
                ),
                &splittable_par_do.environment_id,
                existing_components,
            );
            rval_components
                .transforms
                .insert(process_sized_elements_id.clone(), pt);
        }

        new_composite_root
            .subtransforms
            .push(process_sized_elements_id);

        Some(MessageWithComponents {
            components: Some(rval_components),
            root: Some(message_with_components::Root::Ptransform(
                new_composite_root,
            )),
        })
    }
}

/// Builds a KV coder from two component coder IDs.
fn make_kv_coder(key_coder_id: &str, value_coder_id: &str) -> Coder {
    Coder {
        spec: Some(FunctionSpec {
            urn: urns::beam_urns::KV_CODER.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: vec![key_coder_id.to_string(), value_coder_id.to_string()],
    }
}

/// Builds a `PCollection` with a guaranteed-unique name.
fn make_pcollection(
    coder_id: &str,
    template: &PCollection,
    unique_name_prefix: &str,
    existing_components: &Components,
) -> PCollection {
    PCollection {
        unique_name: generate_unique_pcollection_name(unique_name_prefix, existing_components),
        coder_id: coder_id.to_string(),
        is_bounded: template.is_bounded,
        windowing_strategy_id: template.windowing_strategy_id.clone(),
        display_data: Vec::new(),
    }
}

/// Builds a sub-transform for the SDF expansion.
fn make_sdf_sub_transform(
    urn: &str,
    payload: &[u8],
    inputs: &HashMap<String, String>,
    outputs: &[(&str, &str)],
    unique_name_prefix: &str,
    environment_id: &str,
    existing_components: &Components,
) -> PTransform {
    PTransform {
        unique_name: generate_unique_pcollection_name(unique_name_prefix, existing_components),
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: payload.to_vec(),
        }),
        subtransforms: Vec::new(),
        inputs: inputs.clone(),
        outputs: outputs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        environment_id: environment_id.to_string(),
        display_data: Vec::new(),
        annotations: HashMap::new(),
    }
}

/// Look up an existing double coder in `existing_components`, or create one
/// in `out_components` if none is found.  Returns the coder's ID.
fn get_or_add_double_coder(
    existing_components: &Components,
    out_components: &mut Components,
) -> String {
    for (id, coder) in existing_components.coders.iter() {
        if let Some(spec) = &coder.spec {
            if spec.urn == urns::beam_urns::DOUBLE_CODER {
                return id.clone();
            }
        }
    }

    let double_coder_id = generate_unique_id("DoubleCoder", |id| {
        existing_components.coders.contains_key(id)
    });

    out_components.coders.insert(
        double_coder_id.clone(),
        Coder {
            spec: Some(FunctionSpec {
                urn: urns::beam_urns::DOUBLE_CODER.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );

    double_coder_id
}

/// Produces a PCollection unique-name that does not collide with any existing
/// PCollection in `existing_components`.
fn generate_unique_pcollection_name(prefix: &str, existing_components: &Components) -> String {
    generate_unique_id(prefix, |name| {
        existing_components
            .pcollections
            .values()
            .any(|pc| pc.unique_name == name)
    })
}

/// Produces an ID by appending an integer suffix to `prefix`, incrementing
/// until `is_existing` returns `false`.
fn generate_unique_id(prefix: &str, is_existing: impl Fn(&str) -> bool) -> String {
    let mut i = 0u32;
    loop {
        let candidate = format!("{}{}", prefix, i);
        if !is_existing(&candidate) {
            return candidate;
        }
        i += 1;
    }
}

/// Extract the main input name from a PTransform using its ParDoPayload to
/// distinguish main inputs from side inputs.
fn get_main_input_name_from_payload(transform: &PTransform, payload: &ParDoPayload) -> String {
    let excluded: std::collections::HashSet<&String> = payload.side_inputs.keys().collect();

    for name in transform.inputs.keys() {
        if !excluded.contains(name) {
            return name.clone();
        }
    }

    // Fallback – should not happen for well-formed transforms.
    transform.inputs.keys().next().cloned().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobservice::urns::beam_urns;

    /// Hand-built ParDoPayload that signals a splittable DoFn.
    fn splittable_payload() -> ParDoPayload {
        ParDoPayload {
            restriction_coder_id: "bytes_coder".to_string(),
            do_fn: Some(FunctionSpec {
                urn: "dofn".to_string(),
                payload: vec![1, 2, 3],
            }),
            ..Default::default()
        }
    }

    /// Non-splittable ParDoPayload (empty restriction coder).
    fn non_splittable_payload() -> ParDoPayload {
        ParDoPayload {
            restriction_coder_id: String::new(),
            do_fn: Some(FunctionSpec {
                urn: "dofn".to_string(),
                payload: vec![1, 2, 3],
            }),
            ..Default::default()
        }
    }

    fn make_pcol(id: &str, coder: &str, bounded: i32, windowing: &str) -> PCollection {
        PCollection {
            unique_name: id.to_string(),
            coder_id: coder.to_string(),
            is_bounded: bounded,
            windowing_strategy_id: windowing.to_string(),
            ..Default::default()
        }
    }

    fn make_components_for_sdf() -> Components {
        let mut comps = Components::default();

        comps.pcollections.insert(
            "in_pcol".to_string(),
            make_pcol("in_pcol", "elem_coder", 1, "global_window"),
        );

        let payload = splittable_payload();
        comps.transforms.insert(
            "sdf_par_do".to_string(),
            PTransform {
                unique_name: "sdf_par_do".to_string(),
                spec: Some(FunctionSpec {
                    urn: beam_urns::PAR_DO_TRANSFORM.to_string(),
                    payload: payload.encode_to_vec(),
                }),
                inputs: [("in".to_string(), "in_pcol".to_string())]
                    .into_iter()
                    .collect(),
                outputs: [("out".to_string(), "out_pcol".to_string())]
                    .into_iter()
                    .collect(),
                environment_id: "env1".to_string(),
                ..Default::default()
            },
        );

        comps
    }

    #[test]
    fn non_splittable_par_do_is_not_expanded() {
        let mut comps = Components::default();
        comps.pcollections.insert(
            "in_pcol".to_string(),
            make_pcol("in_pcol", "elem_coder", 1, "global_window"),
        );

        let payload = non_splittable_payload();
        comps.transforms.insert(
            "plain_par_do".to_string(),
            PTransform {
                unique_name: "plain_par_do".to_string(),
                spec: Some(FunctionSpec {
                    urn: beam_urns::PAR_DO_TRANSFORM.to_string(),
                    payload: payload.encode_to_vec(),
                }),
                inputs: [("in".to_string(), "in_pcol".to_string())]
                    .into_iter()
                    .collect(),
                outputs: [("out".to_string(), "out_pcol".to_string())]
                    .into_iter()
                    .collect(),
                environment_id: "env1".to_string(),
                ..Default::default()
            },
        );

        let pipeline = Pipeline {
            components: Some(comps.clone()),
            ..Default::default()
        };

        let result = ProtoOverrides::update_transform(
            beam_urns::PAR_DO_TRANSFORM,
            &pipeline,
            &SplittableParDoExpander::create_sized_replacement(),
        );

        let result_comps = result.components.as_ref().unwrap();
        let pt = result_comps.transforms.get("plain_par_do").unwrap();
        assert!(pt.spec.is_some(), "spec should still be present");
        assert!(pt.subtransforms.is_empty(), "no subtransforms expected");
    }

    #[test]
    fn splittable_par_do_is_expanded_into_composite() {
        let comps = make_components_for_sdf();

        let pipeline = Pipeline {
            components: Some(comps),
            ..Default::default()
        };

        let result = ProtoOverrides::update_transform(
            beam_urns::PAR_DO_TRANSFORM,
            &pipeline,
            &SplittableParDoExpander::create_sized_replacement(),
        );

        let result_comps = result.components.as_ref().unwrap();

        // The original transform should now be a composite (no spec).
        let root_pt = result_comps.transforms.get("sdf_par_do").unwrap();
        assert!(
            root_pt.spec.is_none(),
            "root should have no spec (composite)"
        );
        assert_eq!(root_pt.subtransforms.len(), 3, "expected 3 subtransforms");

        // Verify subtransforms exist
        let pair_id = &root_pt.subtransforms[0];
        let split_id = &root_pt.subtransforms[1];
        let process_id = &root_pt.subtransforms[2];

        let pair = result_comps.transforms.get(pair_id).unwrap();
        assert_eq!(
            pair.spec.as_ref().unwrap().urn,
            beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN
        );

        let split = result_comps.transforms.get(split_id).unwrap();
        assert_eq!(
            split.spec.as_ref().unwrap().urn,
            beam_urns::SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN
        );

        let process = result_comps.transforms.get(process_id).unwrap();
        assert_eq!(
            process.spec.as_ref().unwrap().urn,
            beam_urns::SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN
        );

        // Outputs of the composite must match the original.
        assert_eq!(
            root_pt.outputs,
            result_comps.transforms.get("sdf_par_do").unwrap().outputs
        );
    }

    #[test]
    fn expanded_sdf_chain_wires_input_to_output() {
        let comps = make_components_for_sdf();

        let pipeline = Pipeline {
            components: Some(comps),
            ..Default::default()
        };

        let result = ProtoOverrides::update_transform(
            beam_urns::PAR_DO_TRANSFORM,
            &pipeline,
            &SplittableParDoExpander::create_sized_replacement(),
        );

        let result_comps = result.components.as_ref().unwrap();
        let root_pt = result_comps.transforms.get("sdf_par_do").unwrap();

        let pair_id = &root_pt.subtransforms[0];
        let split_id = &root_pt.subtransforms[1];
        let process_id = &root_pt.subtransforms[2];

        let pair = result_comps.transforms.get(pair_id).unwrap();
        let split = result_comps.transforms.get(split_id).unwrap();
        let process = result_comps.transforms.get(process_id).unwrap();

        // Pair should receive the original main input
        assert_eq!(pair.inputs.get("in").unwrap(), "in_pcol");

        // Pair output -> Split input
        let pair_out = pair.outputs.get("out").unwrap();
        assert_eq!(split.inputs.get("in").unwrap(), pair_out);

        // Split output -> Process input
        let split_out = split.outputs.get("out").unwrap();
        assert_eq!(process.inputs.get("in").unwrap(), split_out);

        // Process output -> original outputs
        assert_eq!(process.outputs.get("out").unwrap(), "out_pcol");
    }

    #[test]
    fn drain_mode_inserts_truncate_and_size() {
        let comps = make_components_for_sdf();

        let pipeline = Pipeline {
            components: Some(comps),
            ..Default::default()
        };

        let result = ProtoOverrides::update_transform(
            beam_urns::PAR_DO_TRANSFORM,
            &pipeline,
            &SplittableParDoExpander::create_truncate_replacement(),
        );

        let result_comps = result.components.as_ref().unwrap();
        let root_pt = result_comps.transforms.get("sdf_par_do").unwrap();

        assert_eq!(
            root_pt.subtransforms.len(),
            4,
            "drain mode: 4 subtransforms expected"
        );

        let truncate_id = &root_pt.subtransforms[2];
        let truncate = result_comps.transforms.get(truncate_id).unwrap();
        assert_eq!(
            truncate.spec.as_ref().unwrap().urn,
            beam_urns::SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN
        );
    }

    #[test]
    fn generated_ids_are_unique() {
        let existing: std::collections::HashSet<String> =
            vec!["foo0".to_string(), "foo1".to_string()]
                .into_iter()
                .collect();

        let id = generate_unique_id("foo", |candidate| existing.contains(candidate));
        assert_eq!(id, "foo2");
    }

    #[test]
    fn double_coder_is_reused() {
        let mut comps = Components::default();
        comps.coders.insert(
            "existing_double".to_string(),
            Coder {
                spec: Some(FunctionSpec {
                    urn: beam_urns::DOUBLE_CODER.to_string(),
                    payload: vec![],
                }),
                component_coder_ids: vec![],
            },
        );

        let mut out = Components::default();
        let id = get_or_add_double_coder(&comps, &mut out);

        assert_eq!(id, "existing_double");
        assert!(out.coders.is_empty(), "no new coder should be created");
    }

    #[test]
    fn double_coder_is_created_when_missing() {
        let comps = Components::default();
        let mut out = Components::default();

        let id = get_or_add_double_coder(&comps, &mut out);

        assert!(id.starts_with("DoubleCoder"));
        assert!(out.coders.contains_key(&id));
        assert_eq!(
            out.coders[&id].spec.as_ref().unwrap().urn,
            beam_urns::DOUBLE_CODER
        );
    }
}
