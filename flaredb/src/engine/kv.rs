//! Shared opaque key/value storage for runner subsystems that need fast point
//! access: Beam user state and user timers.
//!
//! Two implementations back the [`KvStore`] trait:
//! - [`PaimonKvStore`]: a primary-key Paimon table (the historical backend). It
//!   is fine for bulk scans but pays a durable commit per batch and a scan per
//!   point read, and the `paimon` crate exposes no data compaction, so reads get
//!   slower as committed files accumulate.
//! - [`SlateKvStore`]: an embedded SlateDB LSM on `object_store`. SlateDB is a
//!   log-structured merge tree: `put` lands in an in-memory WAL + memtable
//!   (fast), the memtable is flushed to an SST in object storage periodically
//!   (default 100ms) or on demand, and a background worker compacts SSTs
//!   (size-tiered) so reads stay bounded. Point gets/puts are O(1)-ish and
//!   memory stays flat.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow_array::{Array, BinaryArray, Int8Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::spec::{
    DataType as PaimonDataType, RowKind, Schema as PaimonSchema, VALUE_KIND_FIELD_NAME,
    VarBinaryType,
};

use crate::store::element_store::FlareElementStore;

/// Value column name shared by both Paimon KV tables.
const VALUE_COLUMN: &str = "value";

/// Whether the embedded SlateDB backend is selected.
///
/// Defaults to `true`; set `FLAREDB_STATE_BACKEND=paimon` to fall back to Paimon
/// tables for the same subsystems.
pub fn use_slatedb() -> bool {
    !std::env::var("FLAREDB_STATE_BACKEND")
        .map(|value| value.eq_ignore_ascii_case("paimon"))
        .unwrap_or(false)
}

/// Opaque key/value store for fast point access.
#[async_trait::async_trait]
pub trait KvStore: Send + Sync {
    /// Read the value for `key`, or `None` when absent.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Apply a batch of last-write-wins upserts (`Some`) and deletes (`None`) as
    /// a single durable unit.
    async fn apply(&self, writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()>;

    /// All live `(key, value)` pairs, in key order.
    async fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// Paimon primary-key table backing for a key/value namespace.
///
/// `table` and `key_column` identify an existing or to-be-created table; the
/// value column is the shared [`VALUE_COLUMN`].
#[derive(Clone)]
pub struct PaimonKvStore {
    store: Arc<FlareElementStore>,
    table: &'static str,
    key_column: &'static str,
}

impl PaimonKvStore {
    /// Wrap an element store, addressing `table`'s `key_column`.
    pub fn new(
        store: Arc<FlareElementStore>,
        table: &'static str,
        key_column: &'static str,
    ) -> Self {
        Self {
            store,
            table,
            key_column,
        }
    }
}

#[async_trait::async_trait]
impl KvStore for PaimonKvStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for batch in self
            .store
            .read_table_batches_by_binary_key(self.table, self.key_column, key)
            .await?
        {
            let keys = batch
                .column_by_name(self.key_column)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| anyhow!("{} column is not Binary", self.key_column))?;
            let values = batch
                .column_by_name(VALUE_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| anyhow!("{VALUE_COLUMN} column is not Binary"))?;

            for row in 0..batch.num_rows() {
                if keys.value(row) == key {
                    return Ok(Some(values.value(row).to_vec()));
                }
            }
        }
        Ok(None)
    }

    async fn apply(&self, writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let mut keys = Vec::with_capacity(writes.len());
        let mut values = Vec::with_capacity(writes.len());
        let mut kinds = Vec::with_capacity(writes.len());
        for (key, value) in writes {
            keys.push(key.clone());
            match value {
                Some(value) => {
                    values.push(value.clone());
                    kinds.push(RowKind::Insert.to_value());
                }
                None => {
                    values.push(Vec::new());
                    kinds.push(RowKind::Delete.to_value());
                }
            }
        }

        let table = self
            .store
            .get_or_create_table(self.table, kv_paimon_schema(self.key_column)?)
            .await?;
        let batch = build_kv_record_batch_many(&keys, &values, &kinds, self.key_column)?;
        self.store.write_table_batch(&table, &batch).await
    }

    async fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        for batch in self.store.read_table_batches(self.table).await? {
            let keys = batch
                .column_by_name(self.key_column)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| anyhow!("{} column is not Binary", self.key_column))?;
            let values = batch
                .column_by_name(VALUE_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| anyhow!("{VALUE_COLUMN} column is not Binary"))?;
            for row in 0..batch.num_rows() {
                out.push((keys.value(row).to_vec(), values.value(row).to_vec()));
            }
        }
        Ok(out)
    }
}

/// SlateDB (embedded LSM on object storage) backing for a key/value namespace.
pub struct SlateKvStore {
    db: slatedb::Db,
}

impl SlateKvStore {
    /// Open a SlateDB database rooted at `dir`, creating it if needed. `name`
    /// namespaces this store within `dir` (e.g. `"state"` or `"timers"`).
    pub async fn open(dir: &std::path::Path, name: &str) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::local::LocalFileSystem::new_with_prefix(dir)?);
        let db = slatedb::Db::open(name, object_store).await?;
        Ok(Self { db })
    }
}

#[async_trait::async_trait]
impl KvStore for SlateKvStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key).await?.map(|value| value.to_vec()))
    }

    async fn apply(&self, writes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()> {
        for (key, value) in writes {
            match value {
                Some(value) => {
                    self.db.put(key, value.clone()).await?;
                }
                None => {
                    self.db.delete(key).await?;
                }
            }
        }
        self.db.flush().await?;
        Ok(())
    }

    async fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        let mut iter = self.db.scan(..).await?;
        while let Some(item) = iter.next().await? {
            out.push((item.key.to_vec(), item.value.to_vec()));
        }
        Ok(out)
    }
}

/// Paimon schema for a primary-key key/value table.
fn kv_paimon_schema(key_column: &str) -> Result<PaimonSchema> {
    let schema = PaimonSchema::builder()
        .column(
            key_column,
            PaimonDataType::VarBinary(VarBinaryType::try_new(false, VarBinaryType::MAX_LENGTH)?),
        )
        .column(
            VALUE_COLUMN,
            PaimonDataType::VarBinary(VarBinaryType::try_new(false, VarBinaryType::MAX_LENGTH)?),
        )
        .primary_key([key_column])
        .option("bucket", "1")
        .build()?;
    Ok(schema)
}

/// Build a multi-row batch carrying the `_VALUE_KIND` changelog column so many
/// upserts/deletes commit in one durable write.
fn build_kv_record_batch_many(
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    kinds: &[i8],
    key_column: &str,
) -> Result<RecordBatch> {
    let schema = ArrowSchema::new(vec![
        ArrowField::new(key_column, DataType::Binary, false),
        ArrowField::new(VALUE_COLUMN, DataType::Binary, false),
        ArrowField::new(VALUE_KIND_FIELD_NAME, DataType::Int8, false),
    ]);
    let key_array = BinaryArray::from_iter_values(keys.iter().map(|key| key.as_slice()));
    let value_array = BinaryArray::from_iter_values(values.iter().map(|value| value.as_slice()));
    let kind_array = Int8Array::from_iter_values(kinds.iter().copied());

    Ok(RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(key_array),
            Arc::new(value_array),
            Arc::new(kind_array),
        ],
    )?)
}
