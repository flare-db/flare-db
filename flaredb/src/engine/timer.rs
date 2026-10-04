//! Durable user timers.
//!
//! A timer is a request to be called back later. Instead of only reacting to each
//! element as it arrives, user code sets a timer for a key and later gets a
//! callback — the SDK's `@OnTimer` — for example "call me when this window's data
//! is complete", or "call me five minutes from now".
//!
//! When is a timer due? There are two answers, and [`TimeDomain`] names them. An
//! **event-time** timer is due once event time passes its timestamp, where event
//! time is progress through the data's own timestamps, tracked by the watermark;
//! the scheduler checks that. A **processing-time** timer is due once the wall
//! clock reaches it, ignoring the data; [`TimerService`] drives those here.
//!
//! Because "later" can be well after the element that set the timer — and after a
//! restart — a timer is persisted, not held in memory, and must fire exactly
//! once. It is stored as a [`TimerEntry`] under a [`TimerKey`]: its
//! `(transform, family, tag, window, key)` identity plus its fire and hold
//! timestamps. Setting the same identity again replaces it, clearing deletes it,
//! and firing deletes it before delivering, so it can never fire twice. Storage
//! is [`TimerStore`] over a `__flare_timer` Paimon table.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::engine::watermark::Timestamp;
use anyhow::{Result, anyhow};
use arrow_array::{Array, BinaryArray, Int8Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use log::warn;
use paimon::spec::{
    DataType as PaimonDataType, RowKind, Schema as PaimonSchema, VALUE_KIND_FIELD_NAME,
    VarBinaryType,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;

use crate::store::element_store::FlareElementStore;

/// Durable timers plus the processing-time clock.
pub struct TimerService {
    store: TimerStore,
    notify: Arc<Notify>,
    /// Epoch millis at construction. Adding the elapsed tokio time yields an
    /// epoch-domain processing-time clock that paused-time tests can advance.
    origin_epoch_millis: Timestamp,
    origin: Instant,
    /// A paused, deterministic processing-time clock, used when a `TestStream`
    /// advances processing time explicitly. `i64::MIN` means "unset" (use the wall
    /// clock); any other value is the processing time the runner should report.
    manual_now: AtomicI64,
}

impl TimerService {
    /// Create a service over `store`, taking the current wall clock as its origin.
    pub fn new(store: TimerStore) -> Self {
        let origin_epoch_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Self {
            store,
            notify: Arc::new(Notify::new()),
            origin_epoch_millis,
            origin: Instant::now(),
            manual_now: AtomicI64::new(i64::MIN),
        }
    }

    /// The durable timer store.
    pub fn store(&self) -> &TimerStore {
        &self.store
    }

    /// Current processing time in epoch millis.
    ///
    /// Returns the wall-clock time unless a `TestStream` has set a deterministic
    /// processing time via [`set_processing_time`](Self::set_processing_time).
    pub fn now(&self) -> Timestamp {
        let manual = self.manual_now.load(Ordering::Relaxed);
        if manual != i64::MIN {
            return manual;
        }
        self.origin_epoch_millis + self.origin.elapsed().as_millis() as i64
    }

    /// Whether the processing-time clock is paused at a deterministic time.
    pub fn is_manual(&self) -> bool {
        self.manual_now.load(Ordering::Relaxed) != i64::MIN
    }

    /// Pause the processing-time clock at `timestamp` (epoch millis).
    ///
    /// Called by the dispatcher when a `TestStream` advances processing time, so
    /// trigger evaluation and timer due-ness become deterministic. Wakes waiters so
    /// a sleeping firing loop re-checks its deadline.
    pub fn set_processing_time(&self, timestamp: Timestamp) {
        self.manual_now.store(timestamp, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    /// Set (replace) a timer.
    pub async fn set(&self, entry: TimerEntry) -> Result<()> {
        self.store.upsert(&entry).await?;
        self.notify.notify_waiters();
        Ok(())
    }

    /// Clear (delete) a timer.
    pub async fn clear(&self, key: &TimerKey) -> Result<()> {
        self.store.delete(&key.storage_key()).await?;
        self.notify.notify_waiters();
        Ok(())
    }

    /// Apply many timer sets/clears in one durable commit.
    ///
    /// `pending` maps a timer's storage key to `Some(entry)` to set/replace it or
    /// `None` to delete it. A batch is a single Paimon commit instead of one per
    /// timer, which is what makes a stateful bundle that sets thousands of timers
    /// finish in time (see N3).
    pub async fn apply(&self, pending: &HashMap<Vec<u8>, Option<TimerEntry>>) -> Result<()> {
        self.store.apply(pending).await?;
        self.notify.notify_waiters();
        Ok(())
    }

    /// Processing-time timers due at `now`, earliest first.
    pub async fn due_processing_time(&self, now: Timestamp) -> Result<Vec<TimerEntry>> {
        let mut due: Vec<TimerEntry> = self
            .store
            .entries()
            .await?
            .into_iter()
            .filter(|timer| {
                timer.domain == TimeDomain::ProcessingTime && timer.fire_timestamp <= now
            })
            .collect();
        due.sort_by_key(|timer| timer.fire_timestamp);
        Ok(due)
    }

    /// The earliest processing-time deadline, if any.
    pub async fn next_processing_deadline(&self) -> Result<Option<Timestamp>> {
        Ok(self
            .store
            .entries()
            .await?
            .into_iter()
            .filter(|timer| timer.domain == TimeDomain::ProcessingTime)
            .map(|timer| timer.fire_timestamp)
            .min())
    }

    /// Delete and return the processing-time timers due at `now`, earliest first.
    ///
    /// Deletion happens before the caller can deliver them, so a timer set again
    /// by the fired callback is a distinct new timer. Delivery is therefore
    /// at-most-once: a crash after deletion loses the timer.
    pub async fn take_due_processing_time(&self, now: Timestamp) -> Result<Vec<TimerEntry>> {
        let due = self.due_processing_time(now).await?;
        for entry in &due {
            self.store.delete(&entry.key.storage_key()).await?;
        }
        Ok(due)
    }

    /// All persisted event-time timers.
    ///
    /// Event-time timers do not fire on the processing-time clock; which of them
    /// are due depends on the *owning stage's* input watermark, so the caller
    /// (the scheduler) filters this list per stage with
    /// [`TimerEntry::is_due_at_watermark`].
    pub async fn all_event_time_timers(&self) -> Result<Vec<TimerEntry>> {
        Ok(self
            .store
            .entries()
            .await?
            .into_iter()
            .filter(|timer| timer.domain == TimeDomain::EventTime)
            .collect())
    }

    /// Delete the given timers (delete-before-deliver).
    ///
    /// Used when an event-time timer is delivered, so it fires at most once per
    /// delivery. A timer the fired callback re-sets is a distinct new entry.
    pub async fn delete_all(&self, entries: &[TimerEntry]) -> Result<()> {
        for entry in entries {
            self.store.delete(&entry.key.storage_key()).await?;
        }
        if !entries.is_empty() {
            self.notify.notify_waiters();
        }
        Ok(())
    }

    /// Deliver due processing-time timers into `tx`, earliest first.
    pub async fn deliver_due_processing_time(
        &self,
        now: Timestamp,
        tx: &mpsc::UnboundedSender<Vec<TimerEntry>>,
    ) -> Result<usize> {
        let due = self.take_due_processing_time(now).await?;
        if due.is_empty() {
            return Ok(0);
        }
        let count = due.len();
        let _ = tx.send(due);
        Ok(count)
    }

    /// Run the processing-time firing loop, delivering due timers to `tx`.
    ///
    /// Sleeps until the earliest deadline, waking early whenever a timer is set
    /// or cleared. Runs until `tx` is dropped.
    pub async fn run_processing_time(self: Arc<Self>, tx: mpsc::UnboundedSender<Vec<TimerEntry>>) {
        loop {
            let deadline = match self.next_processing_deadline().await {
                Ok(deadline) => deadline,
                Err(e) => {
                    warn!("failed to look up timer deadline: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };

            let Some(deadline) = deadline else {
                self.notify.notified().await;
                continue;
            };

            let now = self.now();
            if now < deadline {
                let sleep_for = Duration::from_millis((deadline - now) as u64);
                tokio::select! {
                    _ = tokio::time::sleep(sleep_for) => {}
                    _ = self.notify.notified() => continue,
                }
            }

            if let Err(e) = self.deliver_due_processing_time(self.now(), &tx).await {
                warn!("failed to deliver due timers: {e}");
            }
            if tx.is_closed() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::element_store::FlareElementStore;
    use tempfile::tempdir;

    async fn make_store() -> (tempfile::TempDir, TimerStore) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        (dir, TimerStore::new(Arc::new(store)))
    }

    fn entry(user_key: &[u8], domain: TimeDomain, fire: Timestamp, hold: Timestamp) -> TimerEntry {
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
    async fn due_filters_by_domain_and_time_and_orders_by_fire() {
        let (_dir, store) = make_store().await;
        let service = TimerService::new(store);

        service
            .set(entry(b"late", TimeDomain::ProcessingTime, 100, 100))
            .await
            .unwrap();
        service
            .set(entry(b"early", TimeDomain::ProcessingTime, 50, 50))
            .await
            .unwrap();
        service
            .set(entry(b"event", TimeDomain::EventTime, 10, 10))
            .await
            .unwrap();

        // Nothing is due before the earliest processing-time deadline.
        assert!(service.due_processing_time(25).await.unwrap().is_empty());

        // At t=50 only "early" is due; event-time timers never appear here.
        let due = service.due_processing_time(50).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].key.user_key, b"early");

        // At t=100 both are due, earliest first.
        let due = service.due_processing_time(100).await.unwrap();
        assert_eq!(
            due.iter()
                .map(|t| t.key.user_key.clone())
                .collect::<Vec<_>>(),
            vec![b"early".to_vec(), b"late".to_vec()]
        );
    }

    #[tokio::test]
    async fn next_processing_deadline_is_the_minimum() {
        let (_dir, store) = make_store().await;
        let service = TimerService::new(store);

        assert_eq!(service.next_processing_deadline().await.unwrap(), None);

        service
            .set(entry(b"a", TimeDomain::ProcessingTime, 300, 300))
            .await
            .unwrap();
        service
            .set(entry(b"b", TimeDomain::ProcessingTime, 100, 100))
            .await
            .unwrap();
        // An event-time timer must not affect the processing-time deadline.
        service
            .set(entry(b"c", TimeDomain::EventTime, 1, 1))
            .await
            .unwrap();

        assert_eq!(service.next_processing_deadline().await.unwrap(), Some(100));
    }

    #[tokio::test]
    async fn take_due_deletes_and_returns_in_order() {
        let (_dir, store) = make_store().await;
        let service = TimerService::new(store);

        service
            .set(entry(b"b", TimeDomain::ProcessingTime, 20, 20))
            .await
            .unwrap();
        service
            .set(entry(b"a", TimeDomain::ProcessingTime, 10, 10))
            .await
            .unwrap();

        let due = service.take_due_processing_time(20).await.unwrap();
        assert_eq!(
            due.iter()
                .map(|t| t.key.user_key.clone())
                .collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        // Taken timers are gone, so they never fire twice.
        assert!(service.due_processing_time(100).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn all_event_time_timers_filters_by_domain_and_delete_all_removes() {
        let (_dir, store) = make_store().await;
        let service = TimerService::new(store);

        service
            .set(entry(b"event", TimeDomain::EventTime, 100, 100))
            .await
            .unwrap();
        service
            .set(entry(b"processing", TimeDomain::ProcessingTime, 100, 100))
            .await
            .unwrap();

        let event_timers = service.all_event_time_timers().await.unwrap();
        assert_eq!(event_timers.len(), 1);
        assert_eq!(event_timers[0].key.user_key, b"event");

        // delete_all removes exactly the delivered event-time timer.
        service.delete_all(&event_timers).await.unwrap();
        assert!(service.all_event_time_timers().await.unwrap().is_empty());
        // The processing-time timer is untouched.
        assert_eq!(service.due_processing_time(100).await.unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn processing_time_timers_fire_once_in_order() {
        let (_dir, store) = make_store().await;
        let service = Arc::new(TimerService::new(store));

        let t0 = service.now();
        service
            .set(entry(
                b"late",
                TimeDomain::ProcessingTime,
                t0 + 100,
                t0 + 100,
            ))
            .await
            .unwrap();
        service
            .set(entry(
                b"early",
                TimeDomain::ProcessingTime,
                t0 + 50,
                t0 + 50,
            ))
            .await
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let runner = service.clone();
        let handle = tokio::spawn(async move { runner.run_processing_time(tx).await });

        // Cross the first deadline.
        tokio::time::advance(Duration::from_millis(60)).await;
        let first = rx.recv().await.expect("first firing");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].key.user_key, b"early");

        // Cross the second deadline.
        tokio::time::advance(Duration::from_millis(50)).await;
        let second = rx.recv().await.expect("second firing");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].key.user_key, b"late");

        // Fired exactly once: nothing is left to fire.
        assert!(
            service
                .due_processing_time(service.now())
                .await
                .unwrap()
                .is_empty()
        );
        handle.abort();
    }
}

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

impl TimerEntry {
    /// Whether this timer is due given its owning stage's input watermark.
    ///
    /// This is Beam's firing rule: `InMemoryTimerInternals.advanceInputWatermark`
    /// fires every **event-time** timer whose timestamp is `<=` the new
    /// watermark (an equal timestamp fires), and never fires a processing-time
    /// timer on the watermark (those fire on the processing-time clock). Beam's
    /// watermark is a lower bound on future element timestamps, so once it
    /// reaches a timer's timestamp no earlier work can still arrive.
    pub fn is_due_at_watermark(&self, watermark: i64) -> bool {
        self.domain == TimeDomain::EventTime && self.fire_timestamp <= watermark
    }
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

    /// Apply many timer writes in a single durable commit.
    ///
    /// `pending` maps a timer's storage key to `Some(entry)` to insert/replace or
    /// `None` to delete; because it is a map, multiple writes to the same key in a
    /// batch already resolve last-write-wins. Committing the whole batch once is
    /// the difference between ~38ms per timer and ~one commit for thousands.
    pub async fn apply(&self, pending: &HashMap<Vec<u8>, Option<TimerEntry>>) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let mut keys = Vec::with_capacity(pending.len());
        let mut values = Vec::with_capacity(pending.len());
        let mut kinds = Vec::with_capacity(pending.len());
        for (key, value) in pending {
            keys.push(key.clone());
            match value {
                Some(entry) => {
                    values.push(serde_json::to_vec(entry)?);
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
            .get_or_create_table(TIMER_TABLE, timer_paimon_schema()?)
            .await?;
        let batch = build_timer_record_batch_many(&keys, &values, &kinds)?;
        self.store.write_table_batch(&table, &batch).await
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
    build_timer_record_batch_many(
        std::slice::from_ref(&key.to_vec()),
        std::slice::from_ref(&value.to_vec()),
        std::slice::from_ref(&kind.to_value()),
    )
}

/// Build a multi-row batch carrying the `_VALUE_KIND` changelog column, so many
/// timer sets/clears commit in one durable write.
fn build_timer_record_batch_many(
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    kinds: &[i8],
) -> Result<RecordBatch> {
    let schema = ArrowSchema::new(vec![
        ArrowField::new(TIMER_KEY_COLUMN, DataType::Binary, false),
        ArrowField::new(TIMER_VALUE_COLUMN, DataType::Binary, false),
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

#[cfg(test)]
mod timer_store_tests {
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
    async fn apply_batch_sets_and_deletes_in_one_commit() {
        let (_dir, store) = make_store().await;
        let timers = TimerStore::new(store);

        // Seed a timer to replace and one to delete.
        let keep = entry(b"keep", TimeDomain::EventTime, 100, 100);
        let drop = entry(b"drop", TimeDomain::EventTime, 200, 200);
        timers.upsert(&keep).await.unwrap();
        timers.upsert(&drop).await.unwrap();

        // One batch: replace `keep`, delete `drop`, and insert `new`.
        let new = entry(b"new", TimeDomain::EventTime, 300, 300);
        let replaced = entry(b"keep", TimeDomain::EventTime, 150, 150);
        let mut pending: HashMap<Vec<u8>, Option<TimerEntry>> = HashMap::new();
        pending.insert(keep.key.storage_key(), Some(replaced));
        pending.insert(drop.key.storage_key(), None);
        pending.insert(new.key.storage_key(), Some(new.clone()));
        timers.apply(&pending).await.unwrap();

        assert_eq!(
            timers
                .get(&keep.key.storage_key())
                .await
                .unwrap()
                .unwrap()
                .fire_timestamp,
            150
        );
        assert_eq!(timers.get(&drop.key.storage_key()).await.unwrap(), None);
        assert_eq!(timers.get(&new.key.storage_key()).await.unwrap(), Some(new));
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

    #[test]
    fn event_time_timers_are_due_at_or_below_the_watermark() {
        let timer = entry(b"k", TimeDomain::EventTime, 100, 100);

        // Beam fires a timer once the watermark reaches it: `timestamp <= watermark`.
        assert!(!timer.is_due_at_watermark(99));
        assert!(timer.is_due_at_watermark(100));
        assert!(timer.is_due_at_watermark(1_000));

        // Processing-time timers never fire on the watermark.
        let processing = entry(b"k", TimeDomain::ProcessingTime, 100, 100);
        assert!(!processing.is_due_at_watermark(i64::MAX));
    }
}
