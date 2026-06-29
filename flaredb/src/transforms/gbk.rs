use anyhow::Error;
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
};
use std::collections::{HashMap, HashSet};

use crate::{
    engine::store::{
        BeamGbk, BeamRecord, IterableValue, NewCollectionRequest, PrimitiveValue,
        ScanCollectionRequest,
    },
    jobservice::urns::beam_urns,
    transforms::{ExecutionContext, FlareTransform},
};
use std::collections::hash_map::Entry::{Occupied, Vacant};

use typed_arrow::List;
#[derive(Clone)]
pub struct GroupByKey {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
}

#[async_trait]
impl FlareTransform for GroupByKey {
    fn urn() -> &'static str
    where
        Self: Sized,
    {
        beam_urns::GROUP_BY_KEY_TRANSFORM
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
        let request = ScanCollectionRequest {
            pcollection_id: ctx.input_pcollection_id,
        };

        let records = ctx.store.scan_collection(request).await?;

        let mut per_key_map = HashMap::<PrimitiveValue, IterableValue>::new();

        for record in records {
            match record {
                BeamRecord::KV(kv) => match per_key_map.entry(kv.key) {
                    Occupied(mut entry) => {
                        let mut values = entry.get().list.values().clone();
                        values.push(kv.value);
                        *entry.get_mut() = IterableValue::new(List::new(values));
                    }
                    Vacant(entry) => {
                        entry.insert(IterableValue::new(List::new(vec![kv.value])));
                    }
                },

                _ => {
                    anyhow::bail!("other types are not expected");
                }
            }
        }

        let beam_records: Vec<BeamRecord> = per_key_map
            .iter()
            .map(|(key, values)| {
                BeamRecord::GBK(BeamGbk {
                    key: key.clone(),
                    value: values.clone(),
                })
            })
            .collect();

        let new_pcol_req = NewCollectionRequest {
            pcollection_id: ctx.output_pcollection_id,
            elements: beam_records,
        };

        ctx.store.write_collection(new_pcol_req).await?;
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
