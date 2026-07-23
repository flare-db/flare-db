use anyhow::Error;
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
};
use datafusion::functions_aggregate::expr_fn::array_agg;
use datafusion::prelude::*;
use datafusion::{common::TableReference, execution::context::SessionContext};
use flare_datafusion::tonbo_table::TonboTable;
use log::info;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::engine::store::create_schema_with_record_type;

use crate::{
    jobservice::urns::beam_urns,
    transforms::{ExecutionContext, FlareTransform},
};
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
        let df_ctx = SessionContext::new();

        let input_schema = ctx.store.registry.get(&ctx.input_pcollection_id).unwrap();
        let db = ctx
            .store
            .resolve_db(&ctx.input_pcollection_id, Some(input_schema.clone()))
            .await?;
        let table = TonboTable::from(db.clone(), input_schema.clone());

        info!("created tonbo table");

        df_ctx.register_table(TableReference::bare("gbk"), Arc::new(table))?;

        let query = df_ctx.table("gbk").await?.aggregate(
            vec![col("key")],
            vec![array_agg(col("value")).alias("value")],
        )?;

        let batches = query.collect().await?;
        info!("Executed GroupByKey");

        for batch in batches {
            let output_schema = create_schema_with_record_type(
                batch
                    .schema_ref()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect::<Vec<_>>(),
                "gbk",
                &ctx.output_pcollection_id,
            )?;

            ctx.store
                .write_record_batch(&ctx.output_pcollection_id, batch, output_schema)
                .await?;
        }
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
