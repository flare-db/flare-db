use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use dashmap::DashMap;
use fusio::disk::TokioFs;
use fusio::executor::tokio::TokioExecutor;
use tokio_util::task::LocalPoolHandle;
use tonbo::db::{DB, DbBuilder};
use tonbo::prelude::*;
use uuid::Uuid;

use crate::store::record::{
    BeamRecord, beamrecords_to_record_batch, create_schema_with_record_type,
    derive_schema_from_records, record_batch_to_beamrecords, record_type_from_schema,
};

use super::ELEMENT_ID_COLUMN;

// Registry for maintaing each Pcollection's schema
#[derive(Clone, Default)]
pub struct FlareSchemaRegistry {
    schemas: Arc<DashMap<String, Arc<Schema>>>,
}

impl FlareSchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, pcollection_id: &str) -> Option<Arc<Schema>> {
        self.schemas
            .get(pcollection_id)
            .map(|schema| schema.clone())
    }

    pub fn register_schema(&self, pcollection_id: &str, schema: Arc<Schema>) {
        self.schemas.insert(pcollection_id.to_string(), schema);
    }

    pub fn register_schema_if_absent(&self, pcollection_id: &str, schema: Arc<Schema>) {
        self.schemas
            .entry(pcollection_id.to_string())
            .or_insert(schema);
    }

    pub fn clear(&self) {
        self.schemas.clear();
    }
}

#[derive(Clone)]
pub struct FlareElementStore {
    pub(crate) registry: FlareSchemaRegistry,
    open_dbs: Arc<DashMap<String, Arc<DB<TokioFs, TokioExecutor>>>>,
    local_pool: LocalPoolHandle,
    base_path: String,
}

impl FlareElementStore {
    // Clear all open database handles so the next job starts with a fresh cache.
    pub fn reset(&self) {
        self.open_dbs.clear();
        self.registry.clear();
    }

    async fn ingest_batch(
        &self,
        pcollection_id: &str,
        schema: Arc<Schema>,
        batch: RecordBatch,
    ) -> Result<()> {
        self.registry
            .register_schema_if_absent(pcollection_id, schema.clone());

        let db = self.resolve_db(pcollection_id, Some(schema)).await?;

        db.ingest(batch)
            .await
            .with_context(|| format!("failed to ingest pcollection {pcollection_id}"))?;

        Ok(())
    }

    fn prepare_record_batch(
        &self,
        pcollection_id: &str,
        batch: RecordBatch,
        schema: Arc<Schema>,
    ) -> Result<(Arc<Schema>, RecordBatch)> {
        // if ELEMENT_ID_COLUMN is present, return the schema.
        let full_schema = if schema.field_with_name(ELEMENT_ID_COLUMN).is_ok() {
            schema.clone()
        } else {
            // create fields with ELEMENT_ID_COLUMN, cause sometimes transfroms(like gbk)
            // can only prouce record batches with projected schema i.e, filtered output
            // that only return the required columns, but ELEMENT_ID_COLUMN is required primary key
            // column for tonbo So, we add that to output schema.
            let mut fields = Vec::with_capacity(schema.fields().len() + 1);
            fields.push(Field::new(ELEMENT_ID_COLUMN, DataType::Utf8, false));
            fields.extend(schema.fields().iter().map(|field| field.as_ref().clone()));
            // create schema using fields.
            create_schema_with_record_type(
                fields,
                record_type_from_schema(&schema)?.as_str(),
                pcollection_id,
            )?
        };

        if batch
            .schema_ref()
            .field_with_name(ELEMENT_ID_COLUMN)
            .is_ok()
        {
            return Ok((full_schema, batch));
        }

        let row_count = batch.num_rows();
        let element_ids: Vec<String> = (0..row_count).map(|_| Uuid::new_v4().to_string()).collect();
        let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(element_ids))];
        columns.extend(batch.columns().iter().cloned());

        let batch = RecordBatch::try_new(full_schema.clone(), columns)
            .context("failed to build store record batch")?;

        Ok((full_schema, batch))
    }

    pub fn new(registry: FlareSchemaRegistry) -> Self {
        let default_base = crate::utils::path::base_dir().join("store");
        Self::with_base_path(registry, default_base.to_str().unwrap_or(".").to_string())
    }

    pub fn with_base_path(registry: FlareSchemaRegistry, base_path: String) -> Self {
        Self {
            registry,
            open_dbs: Arc::new(DashMap::new()),
            local_pool: LocalPoolHandle::new(1),
            base_path,
        }
    }

    // each db is per pcollection_id, So, each db has its own schema and the data stored
    // in each db belongs to that pcollection_id only.
    pub async fn resolve_db(
        &self,
        pcollection_id: &str,
        schema: Option<Arc<Schema>>,
    ) -> Result<Arc<DB<TokioFs, TokioExecutor>>> {
        if let Some(db) = self.open_dbs.get(pcollection_id) {
            return Ok(db.value().clone());
        }

        let schema = match self.registry.get(pcollection_id) {
            Some(schema) => schema,
            None => {
                let schema = schema
                    .ok_or_else(|| anyhow!("schema not found for pcollection {pcollection_id}"))?;
                self.registry
                    .register_schema(pcollection_id, schema.clone());
                schema
            }
        };

        let safe_id = pcollection_id.replace(['/', '.', ' '], "_");

        let db = DbBuilder::from_schema_key_name(schema, ELEMENT_ID_COLUMN)?
            .on_disk(format!("{}/{safe_id}", self.base_path))?
            //.with_seal_policy(Arc::new(BatchesThreshold { batches: 4 }))
            .open()
            .await?;

        let db = Arc::new(db);
        self.open_dbs.insert(pcollection_id.to_string(), db.clone());

        Ok(db)
    }

    // used when a transfrom/stage produces beam records and that needs to be converted
    // to arrow record batch before ingesting into db.
    pub async fn write_beamrecord_batch(&self, req: NewCollectionRequest) -> Result<()> {
        let schema = match self.registry.get(&req.pcollection_id) {
            Some(schema) => schema,
            None => derive_schema_from_records(&req.pcollection_id, &req.elements)?,
        };

        let batch =
            beamrecords_to_record_batch(&req.pcollection_id, &req.elements, schema.clone())?;

        self.ingest_batch(&req.pcollection_id, schema, batch).await
    }

    // used when a transfrom can directly produce arrow record batch
    pub async fn write_record_batch(
        &self,
        pcollection_id: &str,
        batch: RecordBatch,
        schema: Arc<Schema>,
    ) -> Result<()> {
        let schema = self.registry.get(pcollection_id).unwrap_or(schema);

        let (schema, batch) = self.prepare_record_batch(pcollection_id, batch, schema)?;

        self.ingest_batch(pcollection_id, schema, batch).await
    }

    pub async fn scan_collection(&self, req: ScanCollectionRequest) -> Result<Vec<BeamRecord>> {
        let db = self.resolve_db(&req.pcollection_id, None).await?;
        let schema = self
            .registry
            .get(&req.pcollection_id)
            .ok_or_else(|| anyhow!("schema not found for pcollection {}", req.pcollection_id))?;

        //Tonbo's scan is a !Send so we isolate that in a separate thread.
        let batches = self
            .local_pool
            .spawn_pinned(move || async move { db.scan().collect().await })
            .await
            .map_err(|error| anyhow!("scan task panicked: {error}"))??;

        let mut records = Vec::new();
        for batch in batches {
            records.extend(record_batch_to_beamrecords(&batch, &schema)?);
        }

        Ok(records)
    }
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
mod tests {

    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field};

    use super::{FlareSchemaRegistry, NewCollectionRequest, ScanCollectionRequest};
    use crate::store::element_store::FlareElementStore;
    use crate::store::record::{BeamGbk, BeamKV, BeamRecord, IterableValue, PrimitiveValue};
    use crate::store::record::{
        create_schema_with_record_type, iterable_values_to_array, primitive_values_to_array,
    };
    use crate::store::{KEY_COLUMN, VALUE_COLUMN};
    use typed_arrow::{List, Null};

    // helpers

    fn str(s: &str) -> PrimitiveValue {
        PrimitiveValue::String(s.to_string())
    }

    fn bytes(b: &[u8]) -> PrimitiveValue {
        PrimitiveValue::Bytes(b.to_vec())
    }

    fn int(i: i64) -> PrimitiveValue {
        PrimitiveValue::Int64(i)
    }

    fn void() -> PrimitiveValue {
        PrimitiveValue::Void(Null)
    }

    fn iterable(values: Vec<PrimitiveValue>) -> IterableValue {
        IterableValue::new(List::new(values))
    }

    fn primitive(v: PrimitiveValue) -> BeamRecord {
        BeamRecord::PRIMITIVE(v)
    }

    fn kv(k: PrimitiveValue, v: PrimitiveValue) -> BeamRecord {
        BeamRecord::KV(BeamKV { key: k, value: v })
    }

    fn gbk(k: PrimitiveValue, v: IterableValue) -> BeamRecord {
        BeamRecord::GBK(BeamGbk { key: k, value: v })
    }

    fn iter_record(v: IterableValue) -> BeamRecord {
        BeamRecord::ITERABLE(v)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_write_and_scan_primitive() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let records = vec![primitive(bytes(b"hello")), primitive(bytes(b"world"))];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pcol1".to_string(),
                elements: records.clone(),
            })
            .await
            .unwrap();

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pcol1".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut values: Vec<Vec<u8>> = result
            .into_iter()
            .map(|r| match r {
                BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(b)) => b,
                _ => panic!("unexpected record type"),
            })
            .collect();
        values.sort();
        assert_eq!(values, vec![b"hello".to_vec(), b"world".to_vec()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_write_and_scan_kv() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let records = vec![kv(str("apple"), void()), kv(str("banana"), void())];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "kv_pcol".to_string(),
                elements: records,
            })
            .await
            .unwrap();

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "kv_pcol".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut keys: Vec<String> = result
            .into_iter()
            .map(|r| match r {
                BeamRecord::KV(BeamKV {
                    key: PrimitiveValue::String(k),
                    ..
                }) => k,
                _ => panic!("unexpected record type"),
            })
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["apple", "banana"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_write_and_scan_gbk() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let records = vec![
            gbk(str("to"), iterable(vec![void(), void()])),
            gbk(str("be"), iterable(vec![void(), void(), void(), void()])),
        ];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "gbk_pcol".to_string(),
                elements: records,
            })
            .await
            .unwrap();

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "gbk_pcol".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut entries: Vec<(String, usize)> = result
            .into_iter()
            .map(|r| match r {
                BeamRecord::GBK(BeamGbk {
                    key: PrimitiveValue::String(k),
                    value: v,
                }) => (k, v.list.values().len()),
                _ => panic!("unexpected record type"),
            })
            .collect();
        entries.sort_by_key(|(k, _)| k.clone());
        assert_eq!(entries, vec![("be".to_string(), 4), ("to".to_string(), 2)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_write_projected_batch_and_scan_gbk() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let keys = vec![str("to"), str("be")];
        let values = vec![
            iterable(vec![void(), void()]),
            iterable(vec![void(), void(), void(), void()]),
        ];

        let batch_schema = create_schema_with_record_type(
            vec![
                Field::new(KEY_COLUMN, DataType::Utf8, false),
                Field::new(
                    VALUE_COLUMN,
                    DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
                    true,
                ),
            ],
            "gbk",
            "gbk_projected_pcol",
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            batch_schema.clone(),
            vec![
                primitive_values_to_array(&keys, &DataType::Utf8).unwrap(),
                iterable_values_to_array(&values, &DataType::Null).unwrap(),
            ],
        )
        .unwrap();

        store
            .write_record_batch("gbk_projected_pcol", batch, batch_schema)
            .await
            .unwrap();

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "gbk_projected_pcol".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut entries: Vec<(String, usize)> = result
            .into_iter()
            .map(|r| match r {
                BeamRecord::GBK(BeamGbk {
                    key: PrimitiveValue::String(k),
                    value: v,
                }) => (k, v.list.values().len()),
                _ => panic!("unexpected record type"),
            })
            .collect();
        entries.sort_by_key(|(k, _)| k.clone());
        assert_eq!(entries, vec![("be".to_string(), 4), ("to".to_string(), 2)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_separate_pcollections_dont_interfere() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pcol_a".to_string(),
                elements: vec![primitive(str("from_a"))],
            })
            .await
            .unwrap();

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pcol_b".to_string(),
                elements: vec![primitive(str("from_b"))],
            })
            .await
            .unwrap();

        let result_a = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pcol_a".to_string(),
            })
            .await
            .unwrap();

        let result_b = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pcol_b".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result_a.len(), 1);
        assert_eq!(result_b.len(), 1);
        assert!(
            matches!(&result_a[0], BeamRecord::PRIMITIVE(PrimitiveValue::String(s)) if s == "from_a")
        );
        assert!(
            matches!(&result_b[0], BeamRecord::PRIMITIVE(PrimitiveValue::String(s)) if s == "from_b")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_scan_unknown_pcollection_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "nonexistent".to_string(),
            })
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("schema not found"));
    }

    fn store_with_base(base: &str) -> FlareElementStore {
        FlareElementStore::with_base_path(FlareSchemaRegistry::new(), base.to_string())
    }
}
