use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
use dashmap::DashMap;
use paimon::arrow::arrow_fields_to_paimon;
use paimon::spec::Schema as PaimonSchema;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options, Table, catalog::Identifier};
use tokio_stream::StreamExt;

use crate::store::record::{
    BeamRecord, RecordTableSchema, beamrecords_to_record_batch, derive_table_schema,
    record_batch_to_beamrecords,
};

/// In-memory cache of [`RecordTableSchema`] per PCollection id.
///
/// Derived once per PCollection and reused for the lifetime of the job.
#[derive(Clone, Default)]
pub struct FlareSchemaRegistry {
    table_schemas: Arc<DashMap<String, Arc<RecordTableSchema>>>,
}

impl FlareSchemaRegistry {
    pub fn new() -> Self {
        Self {
            table_schemas: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, pcollection_id: &str) -> Option<Arc<RecordTableSchema>> {
        self.table_schemas.get(pcollection_id).map(|s| s.clone())
    }

    pub fn register(&self, pcollection_id: &str, schema: Arc<RecordTableSchema>) {
        self.table_schemas
            .insert(pcollection_id.to_string(), schema);
    }

    pub fn register_if_absent(&self, pcollection_id: &str, schema: Arc<RecordTableSchema>) {
        self.table_schemas
            .entry(pcollection_id.to_string())
            .or_insert(schema);
    }

    pub fn clear(&self) {
        self.table_schemas.clear();
    }
}

pub struct FlareElementStore {
    registry: FlareSchemaRegistry,
    catalog: FileSystemCatalog,
    db_name: String,
}

impl FlareElementStore {
    pub async fn new(warehouse: String, db_name: String) -> Result<Self> {
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse.as_str());
        let catalog = FileSystemCatalog::new(options)?;
        catalog
            .create_database(&db_name, true, HashMap::new())
            .await?;
        Ok(Self {
            registry: FlareSchemaRegistry::new(),
            catalog,
            db_name,
        })
    }

    /// Write a [`RecordBatch`] to the Paimon table for `pcollection_id`.
    pub async fn ingest_batch(
        &self,
        pcollection_id: &str,
        table_schema: Arc<RecordTableSchema>,
        batch: RecordBatch,
    ) -> Result<()> {
        let table = self.get_table(pcollection_id, &table_schema).await?;
        let builder = table.new_write_builder();

        let mut writer = builder.new_write()?;
        writer.write_arrow_batch(&batch).await?;

        let messages = writer.prepare_commit().await?;
        builder.new_commit().commit(messages).await?;

        Ok(())
    }

    /// Convert a batch of [`BeamRecord`]s into a [`RecordBatch`] and ingest.
    ///
    /// The table schema is derived from the first batch written to a PCollection
    /// and cached in the registry.
    pub async fn write_beamrecord_batch(&self, req: NewCollectionRequest) -> Result<()> {
        let table_schema = match self.registry.get(&req.pcollection_id) {
            Some(ts) => ts,
            None => {
                let ts = Arc::new(derive_table_schema(&req.pcollection_id, &req.elements)?);
                self.registry.register(&req.pcollection_id, ts.clone());
                ts
            }
        };

        let batch = beamrecords_to_record_batch(&req.elements, &table_schema)?;

        self.ingest_batch(&req.pcollection_id, table_schema, batch)
            .await
    }

    /// Ingest a pre-built [`RecordBatch`] with its known table schema.
    ///
    /// The schema is also cached for later scans.
    pub async fn write_record_batch(
        &self,
        pcollection_id: &str,
        batch: RecordBatch,
        table_schema: Arc<RecordTableSchema>,
    ) -> Result<()> {
        self.registry
            .register_if_absent(pcollection_id, table_schema.clone());
        self.ingest_batch(pcollection_id, table_schema, batch)
            .await?;

        Ok(())
    }

    /// Full scan of a PCollection — reads all rows from Paimon and converts
    /// them back into [`BeamRecord`]s.
    pub async fn scan_collection(&self, req: ScanCollectionRequest) -> Result<Vec<BeamRecord>> {
        let table_schema = self
            .registry
            .get(&req.pcollection_id)
            .ok_or_else(|| anyhow!("table schema not found for pcollection"))?;

        let identifier = Identifier::new(self.db_name.as_str(), req.pcollection_id);
        let table = self.catalog.get_table(&identifier).await?;

        let read_builder = table.new_read_builder();
        let plan = read_builder.new_scan().plan().await?;
        let read = read_builder.new_read()?;
        let mut stream = read.to_arrow(plan.splits())?;

        let mut records = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            records.extend(record_batch_to_beamrecords(&batch, &table_schema)?);
        }

        Ok(records)
    }

    /// Resolve (or create) the Paimon table backing a PCollection.
    pub async fn get_table(
        &self,
        pcollection_id: &str,
        table_schema: &RecordTableSchema,
    ) -> Result<Table> {
        let identifier = Identifier::new(self.db_name.as_str(), pcollection_id);

        match self.catalog.get_table(&identifier).await {
            Ok(table) => Ok(table),
            Err(paimon::Error::TableNotExist { .. }) => {
                let paimon_schema = arrow_schema_to_paimon(&table_schema.arrow_schema)?;
                self.catalog
                    .create_table(&identifier, paimon_schema, false)
                    .await?;
                let table = self.catalog.get_table(&identifier).await?;
                Ok(table)
            }
            Err(err) => Err(err.into()),
        }
    }
}

/// Convert an Arrow [`Schema`](ArrowSchema) into a Paimon [`Schema`](PaimonSchema).
///
/// The resulting schema preserves Arrow field names and converted data types,
/// and is built with no partition keys, no primary keys, no options, and no comment.
pub fn arrow_schema_to_paimon(schema: &ArrowSchema) -> Result<PaimonSchema> {
    let arrow_fields: Vec<ArrowField> =
        schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let fields = arrow_fields_to_paimon(&arrow_fields)?;
    let builder = fields
        .into_iter()
        .fold(PaimonSchema::builder(), |builder, field| {
            builder.column(field.name().to_string(), field.data_type().clone())
        });
    let schema = builder.build()?;
    Ok(schema)
}

#[derive(Debug)]
pub struct NewCollectionRequest {
    pub(crate) pcollection_id: String,
    pub(crate) elements: Vec<BeamRecord>,
}

#[derive(Debug)]
pub struct ScanCollectionRequest {
    pub(crate) pcollection_id: String,
}
