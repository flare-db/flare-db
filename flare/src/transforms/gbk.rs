use std::collections::HashMap;

use crate::transforms::{FlareTransform, TransformContext};

pub struct GroupByKey {
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
}

impl FlareTransform for GroupByKey {
    type Context = GroupByKeyContext;

    fn urn() -> &'static str
    where
        Self: Sized,
    {
        todo!()
    }

    fn with(inputs: HashMap<String, String>, outputs: HashMap<String, String>) -> Self {
        Self { inputs, outputs }
    }

    fn execute(
        &self,
        ctx: &Self::Context,
    ) -> Result<beam_model_rs::v1::Elements, crate::errors::TransformError> {
        todo!()
    }
}
pub struct GroupByKeyContext {}

impl TransformContext for GroupByKeyContext {}
