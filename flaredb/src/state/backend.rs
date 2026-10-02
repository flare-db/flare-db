//! Opaque key/value storage backend for Beam user state.
//!
//! Backs the Beam Fn State API with a dedicated primary-key Paimon table that
//! maps a composite state key (opaque bytes) to a single value blob. The default
//! `deduplicate` merge engine gives last-write-wins upserts, and deletes are
//! `-D` changelog rows via the `_VALUE_KIND` column. This layer only moves
//! opaque bytes in and out of Paimon; the Beam semantics (append =
//! read-modify-write, clear = delete) live in the per-state-type modules, e.g.
//! [`super::bag`]. The core table read/write operations are delegated to
//! [`FlareElementStore`].

use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow_array::{Array, BinaryArray, Int8Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::spec::{
    DataType as PaimonDataType, RowKind, Schema as PaimonSchema, VALUE_KIND_FIELD_NAME,
    VarBinaryType,
};

use crate::store::element_store::FlareElementStore;

/// Paimon table name backing the Beam Fn user-state API.
const STATE_TABLE: &str = "__flare_state";

/// Column names for the user-state table.
const STATE_KEY_COLUMN: &str = "state_key";
const STATE_VALUE_COLUMN: &str = "value";

/// Opaque key/value storage for Beam user state, sharing the element store's
/// catalog and database.
#[derive(Clone)]
pub struct StateBackend {
    store: Arc<FlareElementStore>,
}

impl StateBackend {
    /// Wrap an element store, reusing its catalog and database for the state table.
    pub fn new(store: Arc<FlareElementStore>) -> Self {
        Self { store }
    }

    /// Read the value stored for `state_key`, or `None` when absent.
    pub async fn get(&self, state_key: &[u8]) -> Result<Option<Vec<u8>>> {
        for batch in self.store.read_table_batches(STATE_TABLE).await? {
            let keys = batch
                .column_by_name(STATE_KEY_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| anyhow!("state table {} column is not Binary", STATE_KEY_COLUMN))?;
            let values = batch
                .column_by_name(STATE_VALUE_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| {
                    anyhow!("state table {} column is not Binary", STATE_VALUE_COLUMN)
                })?;

            for row in 0..batch.num_rows() {
                if keys.value(row) == state_key {
                    return Ok(Some(values.value(row).to_vec()));
                }
            }
        }

        Ok(None)
    }

    /// Upsert `value` for `state_key` (last-write-wins).
    pub async fn put(&self, state_key: &[u8], value: &[u8]) -> Result<()> {
        self.write_row(state_key, value, RowKind::Insert).await
    }

    /// Delete the row for `state_key` (no-op when absent).
    pub async fn delete(&self, state_key: &[u8]) -> Result<()> {
        self.write_row(state_key, &[], RowKind::Delete).await
    }

    /// Write a single insert/delete row and commit it to the user-state table.
    async fn write_row(&self, state_key: &[u8], value: &[u8], kind: RowKind) -> Result<()> {
        let table = self
            .store
            .get_or_create_table(STATE_TABLE, state_paimon_schema()?)
            .await?;
        let batch = build_state_record_batch(state_key, value, kind)?;
        self.store.write_table_batch(&table, &batch).await
    }
}

/// Paimon schema for the primary-key user-state table.
fn state_paimon_schema() -> Result<PaimonSchema> {
    let schema = PaimonSchema::builder()
        .column(
            STATE_KEY_COLUMN,
            PaimonDataType::VarBinary(VarBinaryType::try_new(false, VarBinaryType::MAX_LENGTH)?),
        )
        .column(
            STATE_VALUE_COLUMN,
            PaimonDataType::VarBinary(VarBinaryType::try_new(false, VarBinaryType::MAX_LENGTH)?),
        )
        .primary_key([STATE_KEY_COLUMN])
        .option("bucket", "1")
        .build()?;
    Ok(schema)
}

/// Build a single-row [`RecordBatch`] for the state table, carrying the
/// `_VALUE_KIND` changelog column so inserts and deletes share one path.
fn build_state_record_batch(state_key: &[u8], value: &[u8], kind: RowKind) -> Result<RecordBatch> {
    let schema = ArrowSchema::new(vec![
        ArrowField::new(STATE_KEY_COLUMN, DataType::Binary, false),
        ArrowField::new(STATE_VALUE_COLUMN, DataType::Binary, false),
        ArrowField::new(VALUE_KIND_FIELD_NAME, DataType::Int8, false),
    ]);
    let key_array = BinaryArray::from_iter_values([state_key]);
    let value_array = BinaryArray::from_iter_values([value]);
    let kind_array = Int8Array::from_iter_values([kind.to_value()]);

    Ok(RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(key_array),
            Arc::new(value_array),
            Arc::new(kind_array),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn make_backend() -> (tempfile::TempDir, StateBackend) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        (dir, StateBackend::new(Arc::new(store)))
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let (_dir, backend) = make_backend().await;

        // Missing key reads back None.
        assert!(backend.get(b"key").await.unwrap().is_none());

        // Put then get.
        backend.put(b"key", b"value-1").await.unwrap();
        assert_eq!(
            backend.get(b"key").await.unwrap().as_deref(),
            Some(&b"value-1"[..])
        );

        // Last write wins (deduplicate merge engine).
        backend.put(b"key", b"value-2").await.unwrap();
        assert_eq!(
            backend.get(b"key").await.unwrap().as_deref(),
            Some(&b"value-2"[..])
        );

        // Delete removes the key.
        backend.delete(b"key").await.unwrap();
        assert!(backend.get(b"key").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_is_isolated_between_keys() {
        let (_dir, backend) = make_backend().await;

        backend.put(b"k1", b"one").await.unwrap();
        backend.put(b"k2", b"two").await.unwrap();

        assert_eq!(
            backend.get(b"k1").await.unwrap().as_deref(),
            Some(&b"one"[..])
        );
        assert_eq!(
            backend.get(b"k2").await.unwrap().as_deref(),
            Some(&b"two"[..])
        );

        // Deleting one key leaves the other untouched.
        backend.delete(b"k1").await.unwrap();
        assert!(backend.get(b"k1").await.unwrap().is_none());
        assert_eq!(
            backend.get(b"k2").await.unwrap().as_deref(),
            Some(&b"two"[..])
        );
    }
}
