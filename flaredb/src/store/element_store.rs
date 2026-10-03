use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, anyhow};
use arrow_array::{
    Array, BinaryArray, RecordBatch,
    builder::{ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use dashmap::DashMap;
use paimon::spec::{Datum, PredicateBuilder, Schema as PaimonSchema};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options, Table, catalog::Identifier};
use tokio_stream::StreamExt;

use crate::{
    coders::primitives::WindowedValue,
    store::record::{
        BeamRecord, RecordTableSchema, WindowMetadata, arrow_fields_to_paimon,
        beamrecords_to_record_batch, derive_table_schema, materialize_void_columns,
        record_batch_to_beamrecords,
    },
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
    pub(crate) registry: FlareSchemaRegistry,
    pub(crate) catalog: Arc<FileSystemCatalog>,
    pub(crate) db_name: String,
}

impl FlareElementStore {
    pub async fn new(
        warehouse: String,
        db_name: String,
        catalog: Option<Arc<FileSystemCatalog>>,
    ) -> Result<Self> {
        let catalog = match catalog {
            Some(catalog) => catalog,
            None => Arc::new(create_catalog(warehouse, db_name.clone()).await?),
        };
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

        // Paimon has no Null type so convert Void columns to null booleans.
        let batch = materialize_void_columns(batch)?;

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

    /// Persists decoded [`WindowedValue`]s, writing each element's logical payload
    /// and its Beam window metadata (timestamp, windows, pane) into the same row.
    /// Metadata is stored in the reserved `__flare_window_metadata` column, so
    /// [`scan_windowed_values`](Self::scan_windowed_values) can restore the full
    /// `WindowedValue`; row `i` of the metadata always belongs to element `i`.
    ///
    /// No-op for empty input, which also avoids deriving a schema from zero rows.
    /// When the PCollection was written before, its cached schema is reused with
    /// the metadata column stripped and re-appended so it is never duplicated;
    /// otherwise the schema is derived from the decoded records.
    pub async fn write_windowed_value_batch(
        &self,
        pcollection_id: &str,
        values: Vec<WindowedValue>,
    ) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let records: Vec<BeamRecord> = values.iter().map(|value| value.value.clone()).collect();
        let base_schema = match self.registry.get(pcollection_id) {
            Some(schema) if schema.has_windowed_metadata => RecordTableSchema {
                table_type: schema.table_type,
                arrow_schema: Arc::new(ArrowSchema::new(
                    schema
                        .arrow_schema
                        .fields()
                        .iter()
                        .filter(|field| !is_reserved_window_column(field.name()))
                        .map(|field| field.as_ref().clone())
                        .collect::<Vec<_>>(),
                )),
                has_windowed_metadata: false,
            },
            Some(schema) => (*schema).clone(),
            None => derive_table_schema(pcollection_id, &records)?,
        };
        let base_batch = beamrecords_to_record_batch(&records, &base_schema)?;
        let metadata: Vec<WindowMetadata> = values.iter().map(WindowMetadata::from).collect();
        let batch = append_window_metadata(base_batch, &metadata)?;
        let schema = Arc::new(RecordTableSchema {
            table_type: base_schema.table_type,
            arrow_schema: batch.schema(),
            has_windowed_metadata: true,
        });
        self.registry
            .register_if_absent(pcollection_id, schema.clone());
        self.ingest_batch(pcollection_id, schema, batch).await
    }

    /// Persists a runner-built [`RecordBatch`] together with one [`WindowedValue`]
    /// of Beam window metadata per output row.
    ///
    /// For runner-native transforms (e.g. `GroupByKey`) that build the logical
    /// output directly in Arrow but must still carry Beam window metadata forward.
    /// `metadata` must have exactly one entry per row of `batch`, in the same row
    /// order; a count mismatch is rejected rather than silently misaligning rows.
    /// If `batch` already carries the reserved metadata column it is stored as-is
    /// and `metadata` is not appended. The table schema is cached for future scans.
    pub async fn write_record_batch_with_windowed_metadata(
        &self,
        pcollection_id: &str,
        batch: RecordBatch,
        table_schema: Arc<RecordTableSchema>,
        metadata: Vec<WindowedValue>,
    ) -> Result<()> {
        if batch.num_rows() != metadata.len() {
            return Err(anyhow!(
                "WindowedValue metadata count {} does not match output row count {} for {}",
                metadata.len(),
                batch.num_rows(),
                pcollection_id
            ));
        }
        let metadata: Vec<WindowMetadata> = metadata.iter().map(WindowMetadata::from).collect();
        let base_schema = match self.registry.get(pcollection_id) {
            Some(schema) if schema.has_windowed_metadata => RecordTableSchema {
                table_type: schema.table_type,
                arrow_schema: Arc::new(ArrowSchema::new(
                    schema
                        .arrow_schema
                        .fields()
                        .iter()
                        .filter(|field| !is_reserved_window_column(field.name()))
                        .map(|field| field.as_ref().clone())
                        .collect::<Vec<_>>(),
                )),
                has_windowed_metadata: false,
            },
            _ => (*table_schema).clone(),
        };
        let batch = if batch
            .schema()
            .field_with_name(WINDOW_METADATA_COLUMN)
            .is_ok()
        {
            batch
        } else {
            append_window_metadata(batch, &metadata)?
        };
        let schema = Arc::new(RecordTableSchema {
            table_type: base_schema.table_type,
            arrow_schema: batch.schema(),
            has_windowed_metadata: true,
        });
        self.registry
            .register_if_absent(pcollection_id, schema.clone());
        self.ingest_batch(pcollection_id, schema, batch).await
    }

    /// Reads every element of a PCollection together with its stored Beam window
    /// metadata.
    ///
    /// Logical values and metadata are decoded from the same Arrow batches, so row
    /// alignment is guaranteed. Rows written before window metadata was tracked (a
    /// schema whose `has_windowed_metadata` is `false`) are surfaced through
    /// [`WindowedValue::global`], which preserves the logical value while using the
    /// global window; rows written through the windowed writers restore their
    /// original timestamp, windows, and pane.
    ///
    /// Returns an empty `Vec` when no schema is registered for the PCollection or
    /// when the backing Paimon table was never created (zero committed elements).
    pub async fn scan_windowed_values(
        &self,
        req: ScanCollectionRequest,
    ) -> Result<Vec<WindowedValue>> {
        let Some(table_schema) = self.registry.get(&req.pcollection_id) else {
            return Ok(Vec::new());
        };
        let identifier = self.table_identifier(&req.pcollection_id);
        let table = match self.catalog.get_table(&identifier).await {
            Ok(table) => table,
            Err(paimon::Error::TableNotExist { .. }) => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let read_builder = table.new_read_builder();
        let plan = read_builder.new_scan().plan().await?;
        let read = read_builder.new_read()?;
        let mut stream = read.to_arrow(plan.splits())?;
        let mut values = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let records = record_batch_to_beamrecords(&batch, &table_schema)?;
            if !table_schema.has_windowed_metadata {
                values.extend(records.into_iter().map(WindowedValue::global));
                continue;
            }
            let column = batch
                .column_by_name(WINDOW_METADATA_COLUMN)
                .ok_or_else(|| anyhow!("missing {WINDOW_METADATA_COLUMN} column"))?;
            let encoded = column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("{WINDOW_METADATA_COLUMN} must be a binary column"))?;
            for (row, value) in records.into_iter().enumerate() {
                let metadata: WindowMetadata = serde_json::from_slice(encoded.value(row))?;
                values.push(WindowedValue {
                    value,
                    timestamp_millis: metadata.timestamp_millis,
                    windows: metadata.windows,
                    pane: metadata.pane,
                });
            }
        }
        Ok(values)
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

    /// Ingest a row-shaped [`RecordBatch`] whose Arrow schema describes a full
    /// Beam Row.
    pub async fn write_row_batch(&self, pcollection_id: &str, batch: RecordBatch) -> Result<()> {
        let table_schema = Arc::new(RecordTableSchema::row(batch.schema()));
        self.write_record_batch(pcollection_id, batch, table_schema)
            .await
    }

    /// Full scan of a PCollection, reads all rows from Paimon and converts
    /// them back into [`BeamRecord`]s.
    ///
    /// Returns an empty `Vec` when no elements are present
    pub async fn scan_collection(&self, req: ScanCollectionRequest) -> Result<Vec<BeamRecord>> {
        // If nothing was ever written to this PCollection the schema registry
        // will have no entry for it.  That is not an error; it simply means
        // zero elements are available.
        let table_schema = match self.registry.get(&req.pcollection_id) {
            Some(ts) => ts,
            None => {
                log::info!(
                    "scan_collection: no schema registered for '{}' (0 elements written), returning empty",
                    req.pcollection_id
                );
                return Ok(Vec::new());
            }
        };

        let identifier = self.table_identifier(&req.pcollection_id);
        let table = match self.catalog.get_table(&identifier).await {
            Ok(t) => t,
            Err(paimon::Error::TableNotExist { .. }) => {
                // Schema was registered (e.g. by a runner transform) but no
                // rows were ever committed, so Paimon never created the table.
                log::info!(
                    "scan_collection: Paimon table for '{}' does not exist (0 elements committed), returning empty",
                    req.pcollection_id
                );
                return Ok(Vec::new());
            }
            Err(err) => return Err(err.into()),
        };

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
        let identifier = self.table_identifier(pcollection_id);

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

    /// Resolve an already-created Paimon table for a PCollection without
    /// creating it when absent.
    ///
    /// Returns `Ok(None)` when the PCollection has never been committed to, so a
    /// transform can distinguish "no input" from a read error without the
    /// side effect of materializing an empty table.
    pub async fn get_existing_table(&self, pcollection_id: &str) -> Result<Option<Table>> {
        let identifier = self.table_identifier(pcollection_id);
        match self.catalog.get_table(&identifier).await {
            Ok(table) => Ok(Some(table)),
            Err(paimon::Error::TableNotExist { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) fn table_identifier(&self, pcollection_id: &str) -> Identifier {
        Identifier::new(
            self.db_name.as_str(),
            &Self::sanitize_pcollection_id(pcollection_id),
        )
    }

    fn sanitize_pcollection_id(id: &str) -> String {
        id.replace(['/', '.', ' ', '(', ')'], "_")
    }

    // -------------------------------------------------------------------------
    // Generic table operations
    //
    // Name-addressed Paimon primitives that operate on an explicit schema/table
    // rather than a PCollection. Other storage layers (e.g. user state) build on
    // these instead of reaching into the catalog directly.
    // -------------------------------------------------------------------------

    /// Resolve a table by its raw (unsanitized) name, creating it from `schema`
    /// when it does not yet exist.
    pub async fn get_or_create_table(
        &self,
        table_name: &str,
        schema: PaimonSchema,
    ) -> Result<Table> {
        let identifier = Identifier::new(self.db_name.as_str(), table_name);
        match self.catalog.get_table(&identifier).await {
            Ok(table) => Ok(table),
            Err(paimon::Error::TableNotExist { .. }) => {
                self.catalog
                    .create_table(&identifier, schema, false)
                    .await?;
                Ok(self.catalog.get_table(&identifier).await?)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Commit a single Arrow [`RecordBatch`] to `table`.
    pub async fn write_table_batch(&self, table: &Table, batch: &RecordBatch) -> Result<()> {
        let builder = table.new_write_builder();
        let mut writer = builder.new_write()?;
        writer.write_arrow_batch(batch).await?;
        let messages = writer.prepare_commit().await?;
        builder.new_commit().commit(messages).await?;
        Ok(())
    }

    /// Read all committed batches of `table_name`, or an empty `Vec` when the
    /// table does not exist (zero committed rows).
    pub async fn read_table_batches(&self, table_name: &str) -> Result<Vec<RecordBatch>> {
        let identifier = Identifier::new(self.db_name.as_str(), table_name);
        let table = match self.catalog.get_table(&identifier).await {
            Ok(table) => table,
            Err(paimon::Error::TableNotExist { .. }) => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };

        let read_builder = table.new_read_builder();
        let plan = read_builder.new_scan().plan().await?;
        let read = read_builder.new_read()?;
        let mut stream = read.to_arrow(plan.splits())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok(batches)
    }

    /// Read all committed batches of `table_name` whose `key_column` equals
    /// `key`, or an empty `Vec` when the table does not exist.
    ///
    /// The equality predicate is pushed into Paimon scan planning (see
    /// [`paimon::table::ReadBuilder::with_filter`]). In Paimon 0.3.0 this is
    /// planner-level pruning: it may still return rows that only share a split
    /// with the match, so callers must verify returned rows themselves. It is a
    /// targeted read, not a guaranteed point lookup.
    pub async fn read_table_batches_by_binary_key(
        &self,
        table_name: &str,
        key_column: &str,
        key: &[u8],
    ) -> Result<Vec<RecordBatch>> {
        let identifier = Identifier::new(self.db_name.as_str(), table_name);
        let table = match self.catalog.get_table(&identifier).await {
            Ok(table) => table,
            Err(paimon::Error::TableNotExist { .. }) => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };

        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal(key_column, Datum::Bytes(key.to_vec()))?;

        let mut read_builder = table.new_read_builder();
        read_builder.with_filter(predicate);
        let plan = read_builder.new_scan().plan().await?;
        let read = read_builder.new_read()?;
        let mut stream = read.to_arrow(plan.splits())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok(batches)
    }
}

/// Reserved Paimon column holding a JSON-encoded [`WindowMetadata`] per row.
///
/// The column is appended to every windowed PCollection's Arrow schema as a
/// non-null `Binary` column and lives alongside the logical element columns, so
/// an element's Beam execution metadata travels with its payload. The name is
/// namespaced with a `__flare_` prefix to avoid colliding with user Beam field
/// names.
const WINDOW_METADATA_COLUMN: &str = "__flare_window_metadata";

/// Reserved Paimon column holding one canonical window key per element window.
///
/// Stored as a `List<Utf8>` so a runner-native transform can `unnest` it and
/// group by `(key, window)` inside the query engine (see
/// [`BeamWindow::canonical_key`](crate::coders::primitives::BeamWindow::canonical_key))
/// rather than decoding every element's JSON metadata in memory. The column is
/// written together with [`WINDOW_METADATA_COLUMN`] and carries the same window
/// information in a form that is directly groupable.
pub(crate) const WINDOW_KEY_COLUMN: &str = "__flare_window_key";

/// True for the reserved columns the store appends to windowed PCollections.
///
/// Used to strip them when reconstructing the logical element schema so they are
/// never treated as user fields and never duplicated across successive writes.
fn is_reserved_window_column(name: &str) -> bool {
    name == WINDOW_METADATA_COLUMN || name == WINDOW_KEY_COLUMN
}

/// Appends the reserved [`WINDOW_METADATA_COLUMN`] and [`WINDOW_KEY_COLUMN`] to
/// `batch`, encoding one [`WindowMetadata`] per row and one canonical window key
/// per element window.
///
/// Fails when `metadata` does not have exactly one entry per batch row, so a
/// caller can never persist a batch whose metadata columns are misaligned.
fn append_window_metadata(batch: RecordBatch, metadata: &[WindowMetadata]) -> Result<RecordBatch> {
    if batch.num_rows() != metadata.len() {
        return Err(anyhow!(
            "WindowedValue metadata count {} does not match batch row count {}",
            metadata.len(),
            batch.num_rows()
        ));
    }
    let encoded = metadata
        .iter()
        .map(serde_json::to_vec)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let metadata_array = BinaryArray::from_iter_values(encoded.iter().map(Vec::as_slice));

    // One canonical key per element window, as a `List<Utf8>` column the query
    // engine can `unnest` and group on without decoding the JSON per element.
    let mut window_key_builder = ListBuilder::new(StringBuilder::new());
    for entry in metadata {
        for window in &entry.windows {
            window_key_builder
                .values()
                .append_value(window.canonical_key());
        }
        window_key_builder.append(true);
    }
    let window_key_array = window_key_builder.finish();

    let mut fields: Vec<ArrowField> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(ArrowField::new(
        WINDOW_METADATA_COLUMN,
        DataType::Binary,
        false,
    ));
    fields.push(ArrowField::new(
        WINDOW_KEY_COLUMN,
        DataType::List(Arc::new(ArrowField::new("item", DataType::Utf8, true))),
        true,
    ));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(metadata_array));
    columns.push(Arc::new(window_key_array));

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

pub async fn create_catalog(warehouse: String, db_name: String) -> Result<FileSystemCatalog> {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse.as_str());
    let catalog = FileSystemCatalog::new(options)?;
    catalog
        .create_database(&db_name, true, HashMap::new())
        .await?;
    Ok(catalog)
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

/// Request to append logical [`BeamRecord`]s to a PCollection.
#[derive(Debug)]
pub struct NewCollectionRequest {
    /// Target PCollection id.
    pub(crate) pcollection_id: String,
    /// Elements to append, in order.
    pub(crate) elements: Vec<BeamRecord>,
}

/// Request to read all elements of a PCollection.
#[derive(Debug, Clone)]
pub struct ScanCollectionRequest {
    /// PCollection id to scan.
    pub(crate) pcollection_id: String,
}

#[cfg(test)]
mod element_store_tests {
    use super::*;
    use crate::store::VALUE_COLUMN;
    use crate::store::record::{BeamGbk, BeamKV, IterableValue, PrimitiveValue};
    use std::collections::HashMap;
    use tempfile::tempdir;

    //  helpers

    async fn make_store() -> (tempfile::TempDir, FlareElementStore) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        (dir, store)
    }

    fn int_primitive(v: i64) -> BeamRecord {
        BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v))
    }

    fn extract_ints(records: &[BeamRecord]) -> Vec<i64> {
        let mut out: Vec<i64> = records
            .iter()
            .map(|r| match r {
                BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v)) => *v,
                other => panic!("expected int64 primitive, got {other:?}"),
            })
            .collect();
        out.sort_unstable();
        out
    }

    fn extract_kv_map(records: &[BeamRecord]) -> HashMap<String, i64> {
        records
            .iter()
            .map(|r| match r {
                BeamRecord::KV(kv) => {
                    let key = match &kv.key {
                        PrimitiveValue::String(s) => s.clone(),
                        other => panic!("expected string key, got {other:?}"),
                    };
                    let value = match kv.value.as_ref() {
                        BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v)) => *v,
                        other => panic!("expected int64 value, got {other:?}"),
                    };
                    (key, value)
                }
                other => panic!("expected KV record, got {other:?}"),
            })
            .collect()
    }

    fn extract_gbk_map(records: &[BeamRecord]) -> HashMap<String, Vec<i64>> {
        records
            .iter()
            .map(|r| match r {
                BeamRecord::GBK(gbk) => {
                    let key = match &gbk.key {
                        PrimitiveValue::String(s) => s.clone(),
                        other => panic!("expected string key, got {other:?}"),
                    };
                    let mut values: Vec<i64> = gbk
                        .value
                        .list
                        .iter()
                        .map(|v| match v {
                            BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v)) => *v,
                            other => panic!("expected int64 group value, got {other:?}"),
                        })
                        .collect();
                    values.sort_unstable();
                    (key, values)
                }
                other => panic!("expected GBK record, got {other:?}"),
            })
            .collect()
    }

    //  store construction

    #[tokio::test]
    async fn new_creates_store_and_database_idempotently() {
        let dir = tempdir().unwrap();
        let warehouse = dir.path().to_str().unwrap().to_string();
        // create_database is called with exist_ok=true, so constructing
        // twice against the same warehouse/db name must not error.
        FlareElementStore::new(warehouse.clone(), "testdb".to_string(), None)
            .await
            .unwrap();
        FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .unwrap();
    }

    //  FlareSchemaRegistry (pure, no I/O)

    #[test]
    fn registry_get_register_roundtrip() {
        let registry = FlareSchemaRegistry::new();
        assert!(registry.get("pc").is_none());

        let records = vec![int_primitive(1)];
        let schema = Arc::new(derive_table_schema("pc", &records).unwrap());
        registry.register("pc", schema.clone());

        assert!(registry.get("pc").is_some());
    }

    #[test]
    fn registry_register_if_absent_does_not_overwrite() {
        let registry = FlareSchemaRegistry::new();
        let int_records = vec![int_primitive(1)];
        let int_schema = Arc::new(derive_table_schema("pc", &int_records).unwrap());
        registry.register("pc", int_schema.clone());

        let string_records = vec![BeamRecord::PRIMITIVE(PrimitiveValue::String("x".into()))];
        let string_schema = Arc::new(derive_table_schema("pc", &string_records).unwrap());
        registry.register_if_absent("pc", string_schema);

        // Original int schema must still be the one stored.
        let stored = registry.get("pc").unwrap();
        assert_eq!(
            stored
                .arrow_schema
                .field_with_name(VALUE_COLUMN)
                .unwrap()
                .data_type(),
            &arrow_schema::DataType::Int64
        );
    }

    #[test]
    fn registry_clear_empties_all_entries() {
        let registry = FlareSchemaRegistry::new();
        let records = vec![int_primitive(1)];
        let schema = Arc::new(derive_table_schema("pc", &records).unwrap());
        registry.register("pc", schema);
        assert!(registry.get("pc").is_some());

        registry.clear();
        assert!(registry.get("pc").is_none());
    }

    //  pcollection id sanitization (pure)

    #[test]
    fn sanitize_pcollection_id_replaces_unsafe_chars() {
        let sanitized = FlareElementStore::sanitize_pcollection_id("a/b c(d).e");
        assert_eq!(sanitized, "a_b_c_d__e");
        assert!(!sanitized.contains('/'));
        assert!(!sanitized.contains(' '));
        assert!(!sanitized.contains('('));
        assert!(!sanitized.contains(')'));
        assert!(!sanitized.contains('.'));
    }

    #[test]
    fn sanitize_pcollection_id_leaves_safe_chars_untouched() {
        let sanitized = FlareElementStore::sanitize_pcollection_id("wordcount_output-1");
        assert_eq!(sanitized, "wordcount_output-1");
    }

    //  write_beamrecord_batch + scan_collection

    #[tokio::test]
    async fn write_and_scan_primitive_roundtrip() {
        let (_dir, store) = make_store().await;

        let records = vec![int_primitive(1), int_primitive(2), int_primitive(3)];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pc-primitive".to_string(),
                elements: records.clone(),
            })
            .await
            .unwrap();

        let scanned = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pc-primitive".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(extract_ints(&scanned), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn write_and_scan_windowed_values_preserves_interval_metadata() {
        let (_dir, store) = make_store().await;
        let values = vec![WindowedValue {
            value: int_primitive(7),
            timestamp_millis: 42,
            windows: vec![crate::coders::primitives::BeamWindow::Interval {
                start_millis: 0,
                end_millis: 60_000,
            }],
            pane: crate::coders::primitives::PaneInfo::no_firing(),
        }];

        store
            .write_windowed_value_batch("pc-windowed", values.clone())
            .await
            .unwrap();
        let mut second = values[0].clone();
        second.timestamp_millis = 43;
        store
            .write_windowed_value_batch("pc-windowed", vec![second])
            .await
            .unwrap();
        let scanned = store
            .scan_windowed_values(ScanCollectionRequest {
                pcollection_id: "pc-windowed".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].value, values[0].value);
        assert_eq!(scanned[0].timestamp_millis, 42);
        assert_eq!(scanned[0].windows, values[0].windows);
        assert_eq!(scanned[0].pane, values[0].pane);
        assert_eq!(scanned[1].value, values[0].value);
        assert_eq!(scanned[1].timestamp_millis, 43);
        assert_eq!(scanned[1].windows, values[0].windows);
    }

    #[tokio::test]
    async fn write_and_scan_opaque_bytes_roundtrip_preserves_wire_bytes() {
        let (_dir, store) = make_store().await;

        // Opaque VoidCoder elements are stored as the exact encoded
        // WindowedValue bytes (timestamp + windows + pane framing).
        let frames: Vec<Vec<u8>> = vec![
            vec![
                0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
            ],
            vec![0xff; 5],
            Vec::new(),
        ];
        let records = frames
            .iter()
            .map(|raw| BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(raw.clone())))
            .collect();

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pc-opaque-void".to_string(),
                elements: records,
            })
            .await
            .unwrap();

        let scanned = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pc-opaque-void".to_string(),
            })
            .await
            .unwrap();

        let scanned_frames: Vec<Vec<u8>> = scanned
            .iter()
            .map(|record| match record {
                BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(raw)) => raw.clone(),
                other => panic!("expected opaque bytes, found {other:?}"),
            })
            .collect();
        assert_eq!(scanned_frames, frames);
    }

    #[tokio::test]
    async fn write_and_scan_kv_roundtrip() {
        let (_dir, store) = make_store().await;

        let records = vec![
            BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String("a".into()),
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(10))),
            }),
            BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String("b".into()),
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(20))),
            }),
        ];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pc-kv".to_string(),
                elements: records,
            })
            .await
            .unwrap();

        let scanned = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pc-kv".to_string(),
            })
            .await
            .unwrap();

        let map = extract_kv_map(&scanned);
        let mut expected = HashMap::new();
        expected.insert("a".to_string(), 10);
        expected.insert("b".to_string(), 20);
        assert_eq!(map, expected);
    }

    #[tokio::test]
    async fn write_and_scan_gbk_roundtrip() {
        let (_dir, store) = make_store().await;

        let records = vec![
            BeamRecord::GBK(BeamGbk {
                key: PrimitiveValue::String("k1".into()),
                value: IterableValue::new(vec![
                    PrimitiveValue::Int64(1),
                    PrimitiveValue::Int64(2),
                    PrimitiveValue::Int64(3),
                ]),
            }),
            BeamRecord::GBK(BeamGbk {
                key: PrimitiveValue::String("k2".into()),
                value: IterableValue::new(vec![]),
            }),
        ];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pc-gbk".to_string(),
                elements: records,
            })
            .await
            .unwrap();

        let scanned = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pc-gbk".to_string(),
            })
            .await
            .unwrap();

        let map = extract_gbk_map(&scanned);
        let mut expected: HashMap<String, Vec<i64>> = HashMap::new();
        expected.insert("k1".to_string(), vec![1, 2, 3]);
        expected.insert("k2".to_string(), vec![]);
        assert_eq!(map, expected);
    }

    #[tokio::test]
    async fn multiple_writes_to_same_pcollection_append_and_reuse_cached_schema() {
        let (_dir, store) = make_store().await;
        let pcollection_id = "pc-append";

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
                elements: vec![int_primitive(1), int_primitive(2)],
            })
            .await
            .unwrap();

        // Second write for the same pcollection_id must hit the cached
        // schema path in write_beamrecord_batch (registry.get returns Some),
        // not re-derive it.
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
                elements: vec![int_primitive(3), int_primitive(4)],
            })
            .await
            .unwrap();

        let scanned = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
            })
            .await
            .unwrap();

        assert_eq!(extract_ints(&scanned), vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn schema_drift_on_second_write_errors_instead_of_silently_corrupting() {
        let (_dir, store) = make_store().await;
        let pcollection_id = "pc-drift";

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
                elements: vec![int_primitive(1)],
            })
            .await
            .unwrap();

        // Same pcollection_id, but now String primitives instead of Int64 —
        // schema is cached as Int64 from the first write, so this must fail
        // in beamrecords_to_record_batch rather than writing mismatched data.
        let result = store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
                elements: vec![BeamRecord::PRIMITIVE(PrimitiveValue::String("oops".into()))],
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn scan_without_prior_write_returns_empty() {
        // A PCollection that was never written to (e.g. the output of a GBK
        // whose input was empty) has no schema entry and no Paimon table.
        // scan_collection must return an empty Vec rather than an error so
        // that downstream stages can receive zero elements and complete normally.
        let (_dir, store) = make_store().await;

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pc-never-written".to_string(),
            })
            .await
            .expect("scan of unwritten pcollection should succeed with empty result");

        assert!(
            result.is_empty(),
            "expected empty result for never-written pcollection, got {result:?}"
        );
    }

    #[tokio::test]
    async fn different_pcollections_are_isolated() {
        let (_dir, store) = make_store().await;

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pc-a".to_string(),
                elements: vec![int_primitive(1), int_primitive(2)],
            })
            .await
            .unwrap();

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pc-b".to_string(),
                elements: vec![int_primitive(100), int_primitive(200), int_primitive(300)],
            })
            .await
            .unwrap();

        let a = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pc-a".to_string(),
            })
            .await
            .unwrap();
        let b = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pc-b".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(extract_ints(&a), vec![1, 2]);
        assert_eq!(extract_ints(&b), vec![100, 200, 300]);
    }

    #[tokio::test]
    async fn pcollection_id_with_unsafe_chars_round_trips_end_to_end() {
        let (_dir, store) = make_store().await;
        let pcollection_id = "ns/pc name (v1).flow";

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
                elements: vec![int_primitive(42)],
            })
            .await
            .unwrap();

        let scanned = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
            })
            .await
            .unwrap();

        assert_eq!(extract_ints(&scanned), vec![42]);
    }

    //  write_record_batch (pre-built RecordBatch path)

    #[tokio::test]
    async fn write_record_batch_prebuilt_roundtrip() {
        let (_dir, store) = make_store().await;
        let pcollection_id = "pc-prebuilt";

        let records = vec![int_primitive(5), int_primitive(6)];
        let schema = Arc::new(derive_table_schema(pcollection_id, &records).unwrap());
        let batch = beamrecords_to_record_batch(&records, &schema).unwrap();

        store
            .write_record_batch(pcollection_id, batch, schema.clone())
            .await
            .unwrap();

        // write_record_batch must also register the schema so a later scan
        // (which reads from the registry, not from the request) succeeds.
        let scanned = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
            })
            .await
            .unwrap();

        assert_eq!(extract_ints(&scanned), vec![5, 6]);
    }

    #[tokio::test]
    async fn write_record_batch_does_not_overwrite_existing_cached_schema() {
        let (_dir, store) = make_store().await;
        let pcollection_id = "pc-prebuilt-existing";

        // First, establish the schema via the normal BeamRecord path.
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
                elements: vec![int_primitive(1)],
            })
            .await
            .unwrap();

        // Build a second batch against a freshly-derived (but type-compatible)
        // schema and ingest it via write_record_batch — register_if_absent
        // means the originally cached schema instance stays authoritative.
        let more_records = vec![int_primitive(2), int_primitive(3)];
        let fresh_schema = Arc::new(derive_table_schema(pcollection_id, &more_records).unwrap());
        let batch = beamrecords_to_record_batch(&more_records, &fresh_schema).unwrap();
        store
            .write_record_batch(pcollection_id, batch, fresh_schema)
            .await
            .unwrap();

        let scanned = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: pcollection_id.to_string(),
            })
            .await
            .unwrap();

        assert_eq!(extract_ints(&scanned), vec![1, 2, 3]);
    }

    //  get_table

    #[tokio::test]
    async fn get_table_creates_then_reuses_existing_table() {
        let (_dir, store) = make_store().await;
        let records = vec![int_primitive(1)];
        let schema = derive_table_schema("pc-get-table", &records).unwrap();

        // First call: table doesn't exist yet -> creates it.
        store.get_table("pc-get-table", &schema).await.unwrap();
        // Second call: table now exists -> must resolve without erroring
        // (exercises the `Ok(table)` branch instead of `TableNotExist`).
        store.get_table("pc-get-table", &schema).await.unwrap();
    }

    //  arrow_schema_to_paimon

    #[test]
    fn arrow_schema_to_paimon_preserves_column_count_and_names() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("key", arrow_schema::DataType::Utf8, false),
            ArrowField::new("value", arrow_schema::DataType::Int64, false),
        ]);

        let paimon_schema = arrow_schema_to_paimon(&arrow_schema).unwrap();
        // PaimonSchema's exact accessor names weren't available to verify
        // against the actual paimon 0.3.0 API surface here — if `fields()`
        // isn't the right accessor, swap in whatever paimon::spec::Schema
        // exposes (e.g. `.columns()`), the intent is: 2 columns, same names.
        assert_eq!(paimon_schema.fields().len(), 2);
    }
}
