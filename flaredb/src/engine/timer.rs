//! Processing-time timer service.
//!
//! Owns durable timer storage ([`TimerStore`]) plus the in-process
//! processing-time clock, and computes which processing-time timers are due.
//! Event-time due computation against a stage's input watermark lands in M4;
//! the coordinator that turns a due timer into a bundle for its owning stage
//! lands alongside the scheduler re-run seam.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use log::warn;
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;

use crate::engine::watermark::Timestamp;
use crate::state::timer::{TimeDomain, TimerEntry, TimerKey, TimerStore};

/// Durable timers plus the processing-time clock.
pub struct TimerService {
    store: TimerStore,
    notify: Arc<Notify>,
    /// Epoch millis at construction. Adding the elapsed tokio time yields an
    /// epoch-domain processing-time clock that paused-time tests can advance.
    origin_epoch_millis: Timestamp,
    origin: Instant,
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
        }
    }

    /// The durable timer store.
    pub fn store(&self) -> &TimerStore {
        &self.store
    }

    /// Current processing time in epoch millis.
    pub fn now(&self) -> Timestamp {
        self.origin_epoch_millis + self.origin.elapsed().as_millis() as i64
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
