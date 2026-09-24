use anyhow::Error;
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
};
use log::info;
use std::collections::{HashMap, HashSet};

use crate::{
    jobservice::urns::beam_urns,
    store::element_store::{NewCollectionRequest, ScanCollectionRequest},
    transforms::{ExecutionContext, FlareTransform},
};

/// Merges multiple input PCollections into a single output PCollection.
///
/// Flatten is a pure concatenation: every element from every input is emitted,
/// unchanged, into the single output. Each input PCollection has already been
/// materialized into the element store by its upstream (worker or runner)
#[derive(Clone)]
pub struct Flatten {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
}

#[async_trait]
impl FlareTransform for Flatten {
    fn urn() -> &'static str
    where
        Self: Sized,
    {
        beam_urns::FLATTEN_TRANSFORM
    }

    fn with(
        id: String,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        name: String,
    ) -> Self {
        Self {
            id,
            inputs,
            outputs,
            name,
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<(), Error> {
        let mut merged = Vec::new();

        // Read every input collection from the store.
        for input_id in &ctx.input_pcollection_ids {
            let records = ctx
                .store
                .scan_collection(ScanCollectionRequest {
                    pcollection_id: input_id.clone(),
                })
                .await?;
            merged.extend(records);
        }

        info!(
            "Executed Flatten: {} input collections, {} total elements",
            ctx.input_pcollection_ids.len(),
            merged.len()
        );

        // A Flatten whose inputs are all empty correctly produces an empty output.
        if merged.is_empty() {
            return Ok(());
        }

        ctx.store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: ctx.output_pcollection_id.clone(),
                elements: merged,
            })
            .await?;

        Ok(())
    }

    fn output_pcol_ids(&self) -> HashSet<String> {
        self.outputs.clone().into_values().collect()
    }

    fn unique_name(&self) -> String {
        self.name.clone()
    }

    fn windowing_strategies(&self) -> HashMap<String, WindowingStrategy> {
        HashMap::new()
    }

    fn coders(&self) -> HashMap<String, Coder> {
        HashMap::new()
    }

    fn environments(&self) -> HashMap<String, Environment> {
        HashMap::new()
    }

    fn transfrom_spec(&self) -> HashMap<String, PTransform> {
        let mut transforms = HashMap::new();
        transforms.insert(
            self.id.clone(),
            PTransform {
                spec: Some(FunctionSpec {
                    urn: Self::urn().to_string(),
                    payload: Vec::new(),
                }),
                inputs: self.inputs.clone(),
                outputs: self.outputs.clone(),
                unique_name: self.name.clone(),
                subtransforms: Vec::new(),
                environment_id: String::new(),
                display_data: Vec::new(),
                annotations: HashMap::new(),
            },
        );
        transforms
    }

    fn pcollections(&self, components: &Components) -> HashMap<String, PCollection> {
        self.inputs
            .values()
            .chain(self.outputs.values())
            .filter_map(|id| {
                components
                    .pcollections
                    .get(id)
                    .cloned()
                    .map(|pcollection| (id.clone(), pcollection))
            })
            .collect()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}
