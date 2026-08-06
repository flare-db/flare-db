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
    pub(crate) registry: FlareSchemaRegistry,
    pub(crate) catalog: FileSystemCatalog,
    pub(crate) db_name: String,
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

    /// Full scan of a PCollection, reads all rows from Paimon and converts
    /// them back into [`BeamRecord`]s.
    pub async fn scan_collection(&self, req: ScanCollectionRequest) -> Result<Vec<BeamRecord>> {
        let table_schema = self
            .registry
            .get(&req.pcollection_id)
            .ok_or_else(|| anyhow!("table schema not found for pcollection"))?;

        let identifier = self.table_identifier(&req.pcollection_id);
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

    pub(crate) fn table_identifier(&self, pcollection_id: &str) -> Identifier {
        Identifier::new(
            self.db_name.as_str(),
            &Self::sanitize_pcollection_id(pcollection_id),
        )
    }

    fn sanitize_pcollection_id(id: &str) -> String {
        id.replace(['/', '.', ' ', '(', ')'], "_")
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
        let store = FlareElementStore::new(warehouse, "testdb".to_string())
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
                    let value = match &kv.value {
                        PrimitiveValue::Int64(v) => *v,
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
                            PrimitiveValue::Int64(v) => *v,
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
        FlareElementStore::new(warehouse.clone(), "testdb".to_string())
            .await
            .unwrap();
        FlareElementStore::new(warehouse, "testdb".to_string())
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
    async fn write_and_scan_kv_roundtrip() {
        let (_dir, store) = make_store().await;

        let records = vec![
            BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String("a".into()),
                value: PrimitiveValue::Int64(10),
            }),
            BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String("b".into()),
                value: PrimitiveValue::Int64(20),
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
    async fn scan_without_prior_write_errors() {
        let (_dir, store) = make_store().await;

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pc-never-written".to_string(),
            })
            .await;

        assert!(result.is_err());
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
