use anyhow::Error;
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
};
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::expr_fn::array_agg;
use datafusion::prelude::*;
//use flare_datafusion::tonbo_table::TonboTable;
use log::info;
use paimon::Catalog;
use paimon_datafusion::PaimonTableProvider;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::store::record::{RecordTableSchema, TableType};

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
        let identifier = ctx.store.table_identifier(&ctx.input_pcollection_id);

        let table = ctx.store.catalog.get_table(&identifier).await?;
        let provider = PaimonTableProvider::try_new(table)?;
        let df_ctx = SessionContext::new();

        df_ctx.register_table("gbk", Arc::new(provider))?;

        let query = df_ctx.table("gbk").await?.aggregate(
            vec![col("key")],
            vec![array_agg(col("value")).alias("value")],
        )?;

        let batches = query.collect().await?;

        let output_groups: usize = batches.iter().map(|b| b.num_rows()).sum();
        info!("Executed GroupByKey: {} output groups", output_groups);

        for batch in batches {
            let table_schema = Arc::new(RecordTableSchema {
                table_type: TableType::Gbk,
                arrow_schema: batch.schema(),
            });

            ctx.store
                .write_record_batch(&ctx.output_pcollection_id, batch, table_schema)
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
