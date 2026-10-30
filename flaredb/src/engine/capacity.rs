use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::sync::Notify;

const MIB: u64 = 1024 * 1024;
const MIN_QUEUE_BYTES: u64 = 64 * MIB;
const MAX_QUEUE_BYTES: u64 = 512 * MIB;
const QUEUE_BYTES_PER_SLOT: u64 = 8 * MIB;

/// Fixed execution limits for one job.
///
/// FlareDB derives the worker pool from the logical CPUs visible to the
/// process. These values are deliberately not configurable while the scheduling
/// policy is being established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolConfig {
    pub worker_count: usize,
    pub bundles_per_worker: usize,
    pub total_bundle_slots: usize,
    pub sdf_queue_max_items: usize,
    pub sdf_queue_max_bytes: u64,
    pub sdf_max_split_depth: usize,
}

impl PoolConfig {
    pub fn for_logical_cpus(logical_cpus: usize) -> Self {
        let logical_cpus = logical_cpus.max(1);
        let worker_count = (logical_cpus / 2).max(1);
        let bundles_per_worker = (worker_count / 2).max(1);
        let total_bundle_slots = worker_count * bundles_per_worker;
        let sdf_queue_max_items = (2 * total_bundle_slots).max(2);
        let sdf_queue_max_bytes = QUEUE_BYTES_PER_SLOT
            .saturating_mul(total_bundle_slots as u64)
            .clamp(MIN_QUEUE_BYTES, MAX_QUEUE_BYTES);

        Self {
            worker_count,
            bundles_per_worker,
            total_bundle_slots,
            sdf_queue_max_items,
            sdf_queue_max_bytes,
            sdf_max_split_depth: 16,
        }
    }

    pub fn detect() -> Self {
        let logical_cpus = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1);
        Self::for_logical_cpus(logical_cpus)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CapacitySnapshot {
    pub in_flight_bundles: usize,
    pub queued_sdf_items: usize,
    pub queued_sdf_bytes: u64,
    pub split_reservations: usize,
}

/// Shared accounting for bundles and deferred SDF work.
///
/// `in_flight`, queued residuals, and split reservations all consume the same
/// fixed execution budget. A successful split therefore reserves capacity before
/// the Fn API request is sent instead of discovering saturation afterwards.
#[derive(Debug)]
pub struct ExecutionCapacity {
    config: PoolConfig,
    in_flight_bundles: AtomicUsize,
    queued_sdf_items: AtomicUsize,
    queued_sdf_bytes: AtomicUsize,
    split_reservations: AtomicUsize,
    changed: Notify,
}

impl ExecutionCapacity {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            in_flight_bundles: AtomicUsize::new(0),
            queued_sdf_items: AtomicUsize::new(0),
            queued_sdf_bytes: AtomicUsize::new(0),
            split_reservations: AtomicUsize::new(0),
            changed: Notify::new(),
        }
    }

    pub fn config(&self) -> PoolConfig {
        self.config
    }

    /// Start a fresh job on this reusable capacity controller.
    pub fn reset(&self) {
        self.in_flight_bundles.store(0, Ordering::Release);
        self.queued_sdf_items.store(0, Ordering::Release);
        self.queued_sdf_bytes.store(0, Ordering::Release);
        self.split_reservations.store(0, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub fn snapshot(&self) -> CapacitySnapshot {
        CapacitySnapshot {
            in_flight_bundles: self.in_flight_bundles.load(Ordering::Acquire),
            queued_sdf_items: self.queued_sdf_items.load(Ordering::Acquire),
            queued_sdf_bytes: self.queued_sdf_bytes.load(Ordering::Acquire) as u64,
            split_reservations: self.split_reservations.load(Ordering::Acquire),
        }
    }

    fn outstanding(&self) -> usize {
        let snapshot = self.snapshot();
        snapshot.in_flight_bundles + snapshot.queued_sdf_items + snapshot.split_reservations
    }

    /// Reserve a slot for a bundle that creates new work.
    pub fn try_acquire_bundle(&self) -> bool {
        self.try_acquire_in_flight(|outstanding| outstanding < self.config.total_bundle_slots)
    }

    /// Reserve a slot for an SDF item that is already counted in the queue.
    pub fn try_acquire_queued_bundle(&self) -> bool {
        self.try_acquire_in_flight(|_| true)
    }

    fn try_acquire_in_flight(&self, can_start: impl Fn(usize) -> bool) -> bool {
        loop {
            let in_flight = self.in_flight_bundles.load(Ordering::Acquire);
            if in_flight >= self.config.total_bundle_slots || !can_start(self.outstanding()) {
                return false;
            }
            if self
                .in_flight_bundles
                .compare_exchange(
                    in_flight,
                    in_flight + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    pub fn finish_bundle(&self) {
        let previous = self.in_flight_bundles.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "bundle capacity underflow");
        self.changed.notify_waiters();
    }

    /// Queue a residual produced by the SDK. Protocol responses are indivisible,
    /// so callers that did not reserve a ticket may admit one response beyond the
    /// soft limits; no further splits are allowed until capacity drains.
    pub fn enqueue_sdf_item(&self, bytes: usize) {
        self.queued_sdf_items.fetch_add(1, Ordering::AcqRel);
        self.queued_sdf_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    pub fn consume_sdf_item(&self, bytes: usize) {
        let previous_items = self.queued_sdf_items.fetch_sub(1, Ordering::AcqRel);
        let previous_bytes = self.queued_sdf_bytes.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous_items > 0, "queued SDF item underflow");
        debug_assert!(
            previous_bytes >= bytes,
            "queued SDF byte capacity underflow"
        );
        self.changed.notify_waiters();
    }

    pub fn try_reserve_split(self: &Arc<Self>) -> Option<SplitReservation> {
        loop {
            let snapshot = self.snapshot();
            if snapshot.in_flight_bundles + snapshot.queued_sdf_items + snapshot.split_reservations
                >= self.config.total_bundle_slots
            {
                return None;
            }
            if snapshot.queued_sdf_items + snapshot.split_reservations
                >= self.config.sdf_queue_max_items
            {
                return None;
            }
            if snapshot.queued_sdf_bytes >= self.config.sdf_queue_max_bytes {
                return None;
            }
            if self
                .split_reservations
                .compare_exchange(
                    snapshot.split_reservations,
                    snapshot.split_reservations + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Some(SplitReservation {
                    capacity: Arc::clone(self),
                    active: true,
                });
            }
        }
    }

    pub fn has_split_capacity(&self) -> bool {
        let snapshot = self.snapshot();
        snapshot.in_flight_bundles + snapshot.queued_sdf_items + snapshot.split_reservations
            < self.config.total_bundle_slots
            && snapshot.queued_sdf_items + snapshot.split_reservations
                < self.config.sdf_queue_max_items
            && snapshot.queued_sdf_bytes < self.config.sdf_queue_max_bytes
    }

    pub async fn wait_for_split_capacity(&self) {
        loop {
            let notified = self.changed.notified();
            if self.has_split_capacity() {
                return;
            }
            notified.await;
        }
    }

    fn release_reservation(&self) {
        let previous = self.split_reservations.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "split reservation underflow");
        self.changed.notify_waiters();
    }

    fn reservation_became_item(&self, bytes: usize) {
        self.split_reservations.fetch_sub(1, Ordering::AcqRel);
        self.enqueue_sdf_item(bytes);
    }
}

/// A capacity ticket held while asking the SDK for one split.
pub struct SplitReservation {
    capacity: Arc<ExecutionCapacity>,
    active: bool,
}

impl SplitReservation {
    /// Transfer the reservation to the first residual from the SDK response.
    pub fn consume_as_item(mut self, bytes: usize) {
        self.capacity.reservation_became_item(bytes);
        self.active = false;
    }
}

impl Drop for SplitReservation {
    fn drop(&mut self) {
        if self.active {
            self.capacity.release_reservation();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_pool_configuration_matches_the_approved_formula() {
        let one = PoolConfig::for_logical_cpus(1);
        assert_eq!((one.worker_count, one.bundles_per_worker), (1, 1));
        assert_eq!(one.total_bundle_slots, 1);

        let four = PoolConfig::for_logical_cpus(4);
        assert_eq!((four.worker_count, four.bundles_per_worker), (2, 1));
        assert_eq!(four.total_bundle_slots, 2);

        let eight = PoolConfig::for_logical_cpus(8);
        assert_eq!((eight.worker_count, eight.bundles_per_worker), (4, 2));
        assert_eq!(eight.total_bundle_slots, 8);
        assert_eq!(eight.sdf_queue_max_items, 16);
        assert_eq!(eight.sdf_queue_max_bytes, 64 * MIB);
        assert_eq!(eight.sdf_max_split_depth, 16);

        let large = PoolConfig::for_logical_cpus(64);
        assert_eq!(large.total_bundle_slots, 512);
        assert_eq!(large.sdf_queue_max_bytes, MAX_QUEUE_BYTES);
    }

    #[test]
    fn bundle_and_split_admission_share_one_budget() {
        let capacity = Arc::new(ExecutionCapacity::new(PoolConfig::for_logical_cpus(8)));
        for _ in 0..8 {
            assert!(capacity.try_acquire_bundle());
        }
        assert!(!capacity.try_acquire_bundle());
        assert!(!capacity.has_split_capacity());
        for _ in 0..8 {
            capacity.finish_bundle();
        }
        assert!(capacity.try_reserve_split().is_some());
    }

    #[test]
    fn queued_items_can_move_to_execution_without_using_more_budget() {
        let capacity = Arc::new(ExecutionCapacity::new(PoolConfig::for_logical_cpus(8)));
        assert!(capacity.try_acquire_bundle());
        capacity.enqueue_sdf_item(10);
        assert_eq!(capacity.snapshot().queued_sdf_items, 1);
        assert!(capacity.try_acquire_bundle());
        capacity.consume_sdf_item(10);
        assert_eq!(capacity.snapshot().queued_sdf_items, 0);
        assert_eq!(capacity.snapshot().in_flight_bundles, 2);
        assert_eq!(capacity.snapshot().queued_sdf_bytes, 0);
        capacity.finish_bundle();
        capacity.finish_bundle();
    }

    #[test]
    fn split_admission_stops_at_item_or_byte_headroom() {
        let mut config = PoolConfig::for_logical_cpus(8);
        config.total_bundle_slots = 32;
        let capacity = Arc::new(ExecutionCapacity::new(config));
        assert!(capacity.try_acquire_bundle());

        let mut reservations = Vec::new();
        for _ in 0..config.sdf_queue_max_items {
            reservations.push(capacity.try_reserve_split().expect("split reservation"));
        }
        assert!(
            capacity.try_reserve_split().is_none(),
            "reservations count against the bounded split inbox"
        );

        drop(reservations);
        capacity.enqueue_sdf_item(config.sdf_queue_max_bytes as usize);
        assert!(!capacity.has_split_capacity());
    }

    #[tokio::test]
    async fn releasing_a_split_ticket_wakes_the_capacity_waiter() {
        let config = PoolConfig::for_logical_cpus(1);
        let capacity = Arc::new(ExecutionCapacity::new(config));
        assert!(capacity.try_acquire_bundle());
        let waiter_capacity = capacity.clone();
        let waiter = tokio::spawn(async move {
            waiter_capacity.wait_for_split_capacity().await;
        });
        assert!(!capacity.has_split_capacity());
        capacity.finish_bundle();
        waiter.await.unwrap();
        assert!(capacity.has_split_capacity());
    }

    #[test]
    fn reset_clears_all_per_job_accounting() {
        let capacity = Arc::new(ExecutionCapacity::new(PoolConfig::for_logical_cpus(8)));
        assert!(capacity.try_acquire_bundle());
        capacity.enqueue_sdf_item(42);
        assert!(capacity.try_reserve_split().is_some());
        capacity.reset();
        assert_eq!(capacity.snapshot(), CapacitySnapshot::default());
    }
}
