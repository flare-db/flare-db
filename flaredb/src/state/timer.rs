//! Durable Beam user-timer storage.
//!
//! A timer is addressed by `(transform_id, timer_family_id, tag, window, user
//! key)`. Setting a timer replaces it and clearing deletes it, so there is
//! exactly one logical timer per identity. Entries are persisted in a dedicated
//! Paimon table keyed by the composite identity, mirroring [`super::backend`].

use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow_array::{Array, BinaryArray, Int8Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::spec::{
    DataType as PaimonDataType, RowKind, Schema as PaimonSchema, VALUE_KIND_FIELD_NAME,
    VarBinaryType,
};
use serde::{Deserialize, Serialize};

use crate::store::element_store::FlareElementStore;

/// Paimon table backing the Beam user-timer store.
const TIMER_TABLE: &str = "__flare_timer";
const TIMER_KEY_COLUMN: &str = "timer_key";
const TIMER_VALUE_COLUMN: &str = "value";

/// Beam's timer time domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeDomain {
    EventTime,
    ProcessingTime,
}

/// Identity of a Beam user timer.
///
/// `tag` is the timer's dynamic tag (empty for an ordinary `@OnTimer`); it is
/// part of the identity because a family may carry dynamically tagged timers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerKey {
    pub transform_id: String,
    pub timer_family_id: String,
    pub tag: String,
    /// Canonical window key (see `BeamWindow::canonical_key`).
    pub window: String,
    /// The user key, as the raw encoded bytes the SDK sent.
    pub user_key: Vec<u8>,
}

impl TimerKey {
    /// Deterministic, collision-free composite key for durable storage.
    ///
    /// Every component is length-prefixed (4-byte big-endian) so binary key and
    /// window bytes cannot make distinct identities collide.
    pub fn storage_key(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for part in [
            self.transform_id.as_bytes(),
            self.timer_family_id.as_bytes(),
            self.tag.as_bytes(),
            self.window.as_bytes(),
            self.user_key.as_slice(),
        ] {
            out.extend_from_slice(&(part.len() as u32).to_be_bytes());
            out.extend_from_slice(part);
        }
        out
    }
}

/// A single logical timer: its identity, domain, firing time and output hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerEntry {
    pub key: TimerKey,
    pub domain: TimeDomain,
    pub fire_timestamp: i64,
    pub hold_timestamp: i64,
}

/// Durable keyed storage for Beam user timers.
#[derive(Clone)]
pub struct TimerStore {
    store: Arc<FlareElementStore>,
}

impl TimerStore {
    /// Wrap an element store, reusing its catalog and database for the timer table.
    pub fn new(store: Arc<FlareElementStore>) -> Self {
        Self { store }
    }

    /// Insert or replace the timer (last-write-wins on the composite key).
    pub async fn upsert(&self, entry: &TimerEntry) -> Result<()> {
        let value = serde_json::to_vec(entry)?;
        self.write_row(&entry.key.storage_key(), &value, RowKind::Insert)
            .await
    }

    /// Delete the timer at `storage_key` (no-op when absent).
    pub async fn delete(&self, storage_key: &[u8]) -> Result<()> {
        self.write_row(storage_key, &[], RowKind::Delete).await
    }

    /// Read a single timer, or `None` when absent.
    pub async fn get(&self, storage_key: &[u8]) -> Result<Option<TimerEntry>> {
        let batches = self
            .store
            .read_table_batches_by_binary_key(TIMER_TABLE, TIMER_KEY_COLUMN, storage_key)
            .await?;
        for batch in batches {
            let keys = batch
                .column_by_name(TIMER_KEY_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| anyhow!("timer table {} column is not Binary", TIMER_KEY_COLUMN))?;
            let values = batch
                .column_by_name(TIMER_VALUE_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| {
                    anyhow!("timer table {} column is not Binary", TIMER_VALUE_COLUMN)
                })?;
            for row in 0..batch.num_rows() {
                if keys.value(row) == storage_key {
                    return Ok(Some(serde_json::from_slice(values.value(row))?));
                }
            }
        }
        Ok(None)
    }

    /// All persisted timers.
    pub async fn entries(&self) -> Result<Vec<TimerEntry>> {
        let mut out = Vec::new();
        for batch in self.store.read_table_batches(TIMER_TABLE).await? {
            let values = batch
                .column_by_name(TIMER_VALUE_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| {
                    anyhow!("timer table {} column is not Binary", TIMER_VALUE_COLUMN)
                })?;
            for row in 0..batch.num_rows() {
                out.push(serde_json::from_slice(values.value(row))?);
            }
        }
        Ok(out)
    }

    async fn write_row(&self, key: &[u8], value: &[u8], kind: RowKind) -> Result<()> {
        let table = self
            .store
            .get_or_create_table(TIMER_TABLE, timer_paimon_schema()?)
            .await?;
        let batch = build_timer_record_batch(key, value, kind)?;
        self.store.write_table_batch(&table, &batch).await
    }
}

/// Paimon schema for the primary-key timer table.
fn timer_paimon_schema() -> Result<PaimonSchema> {
    let schema = PaimonSchema::builder()
        .column(
            TIMER_KEY_COLUMN,
            PaimonDataType::VarBinary(VarBinaryType::try_new(false, VarBinaryType::MAX_LENGTH)?),
        )
        .column(
            TIMER_VALUE_COLUMN,
            PaimonDataType::VarBinary(VarBinaryType::try_new(false, VarBinaryType::MAX_LENGTH)?),
        )
        .primary_key([TIMER_KEY_COLUMN])
        .option("bucket", "1")
        .build()?;
    Ok(schema)
}

/// Build a single-row batch carrying the `_VALUE_KIND` changelog column so
/// inserts and deletes share one write path.
fn build_timer_record_batch(key: &[u8], value: &[u8], kind: RowKind) -> Result<RecordBatch> {
    let schema = ArrowSchema::new(vec![
        ArrowField::new(TIMER_KEY_COLUMN, DataType::Binary, false),
        ArrowField::new(TIMER_VALUE_COLUMN, DataType::Binary, false),
        ArrowField::new(VALUE_KIND_FIELD_NAME, DataType::Int8, false),
    ]);
    let key_array = BinaryArray::from_iter_values([key]);
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

    async fn make_store() -> (tempfile::TempDir, Arc<FlareElementStore>) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        (dir, Arc::new(store))
    }

    fn entry(user_key: &[u8], domain: TimeDomain, fire: i64, hold: i64) -> TimerEntry {
        TimerEntry {
            key: TimerKey {
                transform_id: "transform".to_string(),
                timer_family_id: "family".to_string(),
                tag: String::new(),
                window: "global".to_string(),
                user_key: user_key.to_vec(),
            },
            domain,
            fire_timestamp: fire,
            hold_timestamp: hold,
        }
    }

    #[tokio::test]
    async fn set_get_replace_and_clear() {
        let (_dir, store) = make_store().await;
        let timers = TimerStore::new(store);

        let first = entry(b"k", TimeDomain::ProcessingTime, 100, 100);
        timers.upsert(&first).await.unwrap();
        assert_eq!(
            timers.get(&first.key.storage_key()).await.unwrap(),
            Some(first.clone())
        );

        // Setting again replaces the one logical timer.
        let replaced = entry(b"k", TimeDomain::ProcessingTime, 250, 250);
        timers.upsert(&replaced).await.unwrap();
        assert_eq!(
            timers.get(&replaced.key.storage_key()).await.unwrap(),
            Some(replaced.clone())
        );

        // Clearing deletes it.
        timers.delete(&replaced.key.storage_key()).await.unwrap();
        assert_eq!(timers.get(&replaced.key.storage_key()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn identities_are_isolated() {
        let (_dir, store) = make_store().await;
        let timers = TimerStore::new(store);

        let a = entry(b"a", TimeDomain::ProcessingTime, 10, 10);
        let b = entry(b"b", TimeDomain::ProcessingTime, 20, 20);
        timers.upsert(&a).await.unwrap();
        timers.upsert(&b).await.unwrap();

        assert_eq!(timers.get(&a.key.storage_key()).await.unwrap(), Some(a));
        assert_eq!(timers.get(&b.key.storage_key()).await.unwrap(), Some(b));
        assert_eq!(timers.entries().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn entries_survive_a_store_reload() {
        let (dir, store) = make_store().await;
        let warehouse = dir.path().to_str().unwrap().to_string();

        let original = entry(b"persisted", TimeDomain::EventTime, 555, 555);
        TimerStore::new(store).upsert(&original).await.unwrap();

        // A fresh store over the same warehouse sees the committed timer.
        let reopened = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .unwrap();
        let reloaded = TimerStore::new(Arc::new(reopened));
        assert_eq!(
            reloaded.get(&original.key.storage_key()).await.unwrap(),
            Some(original)
        );
    }

    #[test]
    fn storage_key_is_deterministic_and_collision_free() {
        let a = entry(b"k", TimeDomain::EventTime, 0, 0).key;
        let b = entry(b"k", TimeDomain::EventTime, 0, 0).key;
        assert_eq!(a.storage_key(), b.storage_key());

        let mut other = a.clone();
        other.tag = "tag".to_string();
        assert_ne!(a.storage_key(), other.storage_key());
    }
}
