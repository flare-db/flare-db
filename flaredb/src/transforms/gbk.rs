use anyhow::{Error, anyhow};
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
use paimon::Error as PaimonError;
use paimon_datafusion::PaimonTableProvider;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::store::record::{BeamRecord, RecordTableSchema, TableType, record_batch_to_beamrecords};

use crate::{
    jobservice::urns::beam_urns,
    transforms::{ExecutionContext, FlareTransform},
};
/// Runner-native implementation of Beam's `GroupByKey` transform.
///
/// Reads the input PCollection from Paimon table, groups rows by their logical key, and
/// aggregates each key's values into a single iterable (via DataFusion
/// `array_agg`), writing one output group per key.
///
/// Window metadata is carried forward from the input rather than recomputed: each
/// output group reuses the [`WindowedValue`](crate::coders::primitives::WindowedValue)
/// of the first input element whose key matches. Grouping is still **key-only**,
/// so when the same key appears in more than one window the resulting metadata is
/// only representative of one of them.
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
        // GroupByKey consumes exactly one input PCollection.
        let input_pcollection_id = ctx
            .input_pcollection_ids
            .first()
            .cloned()
            .expect("GroupByKey expects exactly one input PCollection");
        let identifier = ctx.store.table_identifier(&input_pcollection_id);

        // The input table may not exist when the upstream stage produced zero
        // elements (write_beamrecord_batch is never called for empty output).
        // An empty input to GroupByKey correctly yields an empty output.
        // Preserve a representative input window on GBK output without changing
        // the current key-only grouping behavior.
        let input_windows = ctx
            .store
            .scan_windowed_values(crate::store::element_store::ScanCollectionRequest {
                pcollection_id: input_pcollection_id.clone(),
            })
            .await?;

        let table = match ctx.store.catalog.get_table(&identifier).await {
            Ok(table) => table,
            Err(PaimonError::TableNotExist { .. }) => {
                info!(
                    "GroupByKey: input PCollection table '{}' does not exist (0 elements), \
                     producing empty output",
                    input_pcollection_id
                );
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };
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
                has_windowed_metadata: false,
            });

            let output_records = record_batch_to_beamrecords(&batch, &table_schema)?;
            let mut metadata = Vec::with_capacity(output_records.len());
            for output in &output_records {
                let BeamRecord::GBK(group) = output else {
                    return Err(anyhow!("GBK output row was not decoded as a GBK record"));
                };
                let matching_input = input_windows
                    .iter()
                    .find(|input| match &input.value {
                        BeamRecord::KV(kv) => kv.key == group.key,
                        _ => false,
                    })
                    .ok_or_else(|| anyhow!("GBK output key has no input WindowedValue metadata"))?;
                metadata.push(matching_input.clone());
            }
            ctx.store
                .write_record_batch_with_windowed_metadata(
                    &ctx.output_pcollection_id,
                    batch,
                    table_schema,
                    metadata,
                )
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
