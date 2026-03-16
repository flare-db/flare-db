use std::collections::HashSet;

use crate::fusion::pipeline::{PCollectionNode, PTransformNode};
use crate::jobservice::urns;
use beam_model_rs::v1::executable_stage_payload::{SideInputId, TimerId, UserStateId};
use beam_model_rs::v1::{Components, PTransform, ParDoPayload};
use prost::Message;

use crate::errors::BeamTranslationError;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct SideInputRef {
    pcol: PCollectionNode,
    pt: PTransformNode,
    name: String,
}

impl SideInputRef {
    pub fn from_id(id: &SideInputId, comps: &Components) -> Result<Self, BeamTranslationError> {
        let name = id.local_name.clone();
        let pt_id = id.transform_id.clone();

        let transform = comps
            .transforms
            .get(&pt_id)
            .ok_or_else(|| BeamTranslationError::NotFound(format!("Transform '{pt_id}'")))?;

        let pcol_id = transform
            .inputs
            .get(&name)
            .ok_or_else(|| {
                BeamTranslationError::NotFound(format!("Input '{name}' on transform '{pt_id}'"))
            })?
            .clone();

        let pcol = comps
            .pcollections
            .get(&pcol_id)
            .ok_or_else(|| BeamTranslationError::NotFound(format!("PCollection '{pcol_id}'")))?
            .clone();

        Ok(Self {
            pcol: PCollectionNode {
                id: pcol_id,
                collection: pcol,
            },
            pt: PTransformNode {
                id: pt_id,
                transform: transform.clone(),
            },
            name,
        })
    }

    fn get_pcol(&self) -> &PCollectionNode {
        &self.pcol
    }

    fn get_pt(&self) -> &PTransformNode {
        &self.pt
    }

    fn get_name(&self) -> &str {
        &self.name
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct TimerRef {
    pt: PTransformNode,
    name: String,
}

impl TimerRef {
    pub fn from_id(id: &TimerId, comps: &Components) -> Result<Self, BeamTranslationError> {
        let pt_id = id.transform_id.clone();
        let pt = comps
            .transforms
            .get(&pt_id)
            .ok_or_else(|| BeamTranslationError::NotFound(format!("Transform '{pt_id}'")))?
            .clone();

        Ok(Self {
            pt: PTransformNode {
                id: pt_id,
                transform: pt,
            },
            name: id.local_name.clone(),
        })
    }

    fn get_pt(&self) -> &PTransformNode {
        &self.pt
    }
    fn get_name(&self) -> &str {
        &self.name
    }
}

/// A reference to user state. Includes the PTransform that references the user state
/// and the local name both necessary to fully resolve user state.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct UserStateRef {
    pt: PTransformNode,
    name: String,
    pcol: PCollectionNode,
}

impl UserStateRef {
    /// Create a user state reference.
    fn new(pt: PTransformNode, name: String, pcol: PCollectionNode) -> Self {
        Self { pt, name, pcol }
    }

    /// Create a user state reference from a UserStateId and components.
    pub fn from_id(id: &UserStateId, comps: &Components) -> Result<Self, BeamTranslationError> {
        let pt_id = id.transform_id.clone();

        let transform = comps
            .transforms
            .get(&pt_id)
            .ok_or_else(|| BeamTranslationError::NotFound(format!("Transform '{pt_id}'")))?;

        // mirrors: transform.getInputsOrThrow(ParDoTranslation.getMainInputName(transform))
        let main_input_name = get_main_input_name(transform)?;
        let pcol_id = transform
            .inputs
            .get(&main_input_name)
            .ok_or_else(|| {
                BeamTranslationError::NotFound(format!(
                    "Main input '{main_input_name}' on transform '{pt_id}'"
                ))
            })?
            .clone();

        let pcol = comps
            .pcollections
            .get(&pcol_id)
            .ok_or_else(|| BeamTranslationError::NotFound(format!("PCollection '{pcol_id}'")))?
            .clone();

        Ok(Self::new(
            PTransformNode {
                id: pt_id,
                transform: transform.clone(),
            },
            id.local_name.clone(),
            PCollectionNode {
                id: pcol_id,
                collection: pcol,
            },
        ))
    }

    /// The PTransform that uses this user state.
    fn transform(&self) -> &PTransformNode {
        &self.pt
    }

    /// The local name the referencing PTransform uses to refer to this user state.
    fn local_name(&self) -> &str {
        &self.name
    }

    /// The PCollection that represents the main input to the PTransform.
    fn collection(&self) -> &PCollectionNode {
        &self.pcol
    }
}

fn get_main_input_name(transform: &PTransform) -> Result<String, BeamTranslationError> {
    let spec = transform
        .spec
        .as_ref()
        .ok_or_else(|| BeamTranslationError::InvalidArgument("Transform is missing spec".into()))?;

    if !urns::beam_urns::VALID_MAIN_INPUT_URNS.contains(&spec.urn.as_str()) {
        return Err(BeamTranslationError::InvalidArgument(format!(
            "Unexpected payload type '{}'",
            spec.urn
        )));
    }

    let payload = ParDoPayload::decode(spec.payload.as_slice()).map_err(|e| {
        BeamTranslationError::InvalidArgument(format!("Failed to decode ParDoPayload: {e}"))
    })?;

    get_main_input_name_from_payload(transform, &payload)
}

fn get_main_input_name_from_payload(
    transform: &PTransform,
    payload: &ParDoPayload,
) -> Result<String, BeamTranslationError> {
    // mirrors: Sets.difference(inputs, Sets.union(sideInputs, timerFamilySpecs))
    let excluded: HashSet<&String> = payload
        .side_inputs
        .keys()
        .chain(payload.timer_family_specs.keys())
        .collect();

    let mut main_inputs: Vec<&String> = transform
        .inputs
        .keys()
        .filter(|name| !excluded.contains(name))
        .collect();

    match main_inputs.len() {
        1 => Ok(main_inputs.remove(0).clone()),
        n => Err(BeamTranslationError::InvalidArgument(format!(
            "Expected exactly one main input, found {n}"
        ))),
    }
}
