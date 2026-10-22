use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinSet;

use crate::{
    engine::harness::{WorkerChannelRegistry, WorkerHandle},
    worker::manager::{WorkerExit, WorkerId, WorkerManager, WorkerRuntime},
};

const DEFAULT_HARNESS_RSS_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_MEMORY_FRACTION: f64 = 0.70;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerKind {
    Cpu,
    Io,
}

#[derive(Clone, Debug)]
pub struct WorkerPoolConfig {
    pub max_workers: usize,
    pub min_workers: usize,
    pub idle_timeout: Duration,
    pub bundles_per_worker: usize,
    pub log_interval: Duration,
    pub memory_budget_bytes: u64,
    pub harness_rss_bytes: u64,
}

impl WorkerPoolConfig {
    pub fn from_env() -> Self {
        let harness_rss_bytes = env_u64("FLAREDB_HARNESS_RSS_BYTES", DEFAULT_HARNESS_RSS_BYTES);
        let memory_budget_bytes = std::env::var("FLAREDB_MEMORY_BUDGET_BYTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(|| {
                let mut system = sysinfo::System::new();
                system.refresh_memory();
                (system.available_memory() as f64 * DEFAULT_MEMORY_FRACTION) as u64
            });
        let memory_bound = (memory_budget_bytes / harness_rss_bytes.max(1)).max(1) as usize;
        let cpu_bound = std::thread::available_parallelism()
            .map(|value| value.get().saturating_sub(1).max(1))
            .unwrap_or(1);
        let max_workers = std::env::var("FLAREDB_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(|| cpu_bound.min(memory_bound).max(1));
        let min_workers = std::env::var("FLAREDB_MIN_WORKERS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1)
            .min(max_workers);
        Self {
            max_workers,
            min_workers,
            idle_timeout: Duration::from_secs(env_u64("FLAREDB_WORKER_IDLE_TIMEOUT_SECS", 10)),
            bundles_per_worker: env_u64("FLAREDB_BUNDLES_PER_WORKER", 1).max(1) as usize,
            log_interval: Duration::from_secs(env_u64("FLAREDB_POOL_LOG_INTERVAL_SECS", 5)),
            memory_budget_bytes,
            harness_rss_bytes,
        }
    }

    pub fn max_bundle_slots(&self) -> usize {
        self.max_workers
            .saturating_mul(self.bundles_per_worker)
            .max(1)
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[derive(Clone)]
pub struct SpawnedWorker {
    pub id: WorkerId,
    pub pid: Option<u32>,
    pub handle: WorkerHandle,
}

#[async_trait]
pub trait PoolWorkerBackend: Send + Sync {
    async fn spawn_worker(&self) -> Result<SpawnedWorker>;
    async fn stop_worker(&self, worker_id: &WorkerId) -> Result<()>;
    fn subscribe_exits(&self) -> broadcast::Receiver<WorkerExit>;
    fn worker_exited(&self, exit: &WorkerExit);
}

pub struct ManagerPoolBackend {
    manager: WorkerManager,
    channels: WorkerChannelRegistry,
    runtime: WorkerRuntime,
    job_id: String,
    staging_dir: String,
    instance_id: String,
}

impl ManagerPoolBackend {
    pub fn new(
        manager: WorkerManager,
        channels: WorkerChannelRegistry,
        runtime: WorkerRuntime,
        job_id: impl Into<String>,
        staging_dir: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Self {
        Self {
            manager,
            channels,
            runtime,
            job_id: job_id.into(),
            staging_dir: staging_dir.into(),
            instance_id: instance_id.into(),
        }
    }
}

#[async_trait]
impl PoolWorkerBackend for ManagerPoolBackend {
    async fn spawn_worker(&self) -> Result<SpawnedWorker> {
        let worker_id = self
            .manager
            .spawn_worker(
                &self.job_id,
                &self.runtime,
                &self.staging_dir,
                &self.instance_id,
            )
            .await
            .map_err(|status| anyhow!(status.message().to_string()))?;
        if let Err(error) = self
            .channels
            .wait_connected(
                &worker_id,
                Duration::from_secs(self.manager.config().connect_timeout_secs),
            )
            .await
        {
            let _ = self.manager.stop_worker(&worker_id).await;
            return Err(error);
        }
        Ok(SpawnedWorker {
            pid: self.manager.worker_pid(&worker_id),
            handle: self.channels.handle(&worker_id),
            id: worker_id,
        })
    }

    async fn stop_worker(&self, worker_id: &WorkerId) -> Result<()> {
        self.channels.remove_worker(worker_id).await;
        self.manager
            .stop_worker(worker_id)
            .await
            .map_err(|status| anyhow!(status.message().to_string()))
    }

    fn subscribe_exits(&self) -> broadcast::Receiver<WorkerExit> {
        self.manager.subscribe_worker_exits()
    }

    fn worker_exited(&self, exit: &WorkerExit) {
        if !exit.intentional {
            self.channels.status().report_worker_failure(
                exit.worker_id.as_str(),
                "harness process",
                format_args!("unexpected exit status {}", exit.status),
            );
        }
    }
}

struct WorkerEntry {
    id: WorkerId,
    pid: Option<u32>,
    handle: WorkerHandle,
    active: HashMap<u64, Instant>,
    bundles_done: u64,
    busy_seconds: f64,
    idle_since: Instant,
}

struct PoolInner {
    starting: usize,
    shutting_down: bool,
    workers: HashMap<String, WorkerEntry>,
    spawn_count: u64,
    spawn_cost: Option<Duration>,
    total_busy_seconds: f64,
    total_idle_seconds: f64,
    completed_bundles: u64,
}

pub struct WorkerPool {
    config: WorkerPoolConfig,
    backend: Arc<dyn PoolWorkerBackend>,
    inner: StdMutex<PoolInner>,
    notify: Notify,
    next_lease_id: AtomicU64,
}

pub struct Lease {
    pool: Arc<WorkerPool>,
    worker_id: WorkerId,
    lease_id: u64,
    started: Instant,
}

impl Lease {
    pub fn worker(&self) -> WorkerHandle {
        self.pool
            .inner
            .lock()
            .expect("worker pool lock not poisoned")
            .workers
            .get(self.worker_id.as_str())
            .expect("leased worker must exist")
            .handle
            .clone()
    }

    pub fn id(&self) -> &WorkerId {
        &self.worker_id
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        {
            let mut inner = self
                .pool
                .inner
                .lock()
                .expect("worker pool lock not poisoned");
            if let Some(worker) = inner.workers.get_mut(self.worker_id.as_str()) {
                worker.active.remove(&self.lease_id);
                if worker.active.is_empty() {
                    worker.idle_since = Instant::now();
                }
                worker.busy_seconds += self.started.elapsed().as_secs_f64();
                inner.total_busy_seconds += self.started.elapsed().as_secs_f64();
            }
        }
        self.pool.notify.notify_waiters();
    }
}

#[derive(Clone, Debug, Default)]
pub struct WorkerSnapshot {
    pub id: String,
    pub pid: Option<u32>,
    pub active_bundles: usize,
    pub bundles_done: u64,
    pub busy_seconds: f64,
    pub idle_seconds: f64,
}

#[derive(Clone, Debug, Default)]
pub struct PoolSnapshot {
    pub max_workers: usize,
    pub min_workers: usize,
    pub live: usize,
    pub busy: usize,
    pub idle: usize,
    pub starting: usize,
    pub spawn_count: u64,
    pub completed_bundles: u64,
    pub total_busy_seconds: f64,
    pub total_idle_seconds: f64,
    pub spawn_cost_ms: Option<u128>,
    pub workers: Vec<WorkerSnapshot>,
}

impl WorkerPool {
    pub fn new(config: WorkerPoolConfig, backend: Arc<dyn PoolWorkerBackend>) -> Arc<Self> {
        let pool = Arc::new(Self {
            config,
            backend,
            inner: StdMutex::new(PoolInner {
                starting: 0,
                shutting_down: false,
                workers: HashMap::new(),
                spawn_count: 0,
                spawn_cost: None,
                total_busy_seconds: 0.0,
                total_idle_seconds: 0.0,
                completed_bundles: 0,
            }),
            notify: Notify::new(),
            next_lease_id: AtomicU64::new(1),
        });
        pool.start_exit_monitor();
        pool
    }

    pub fn config(&self) -> &WorkerPoolConfig {
        &self.config
    }

    pub fn min_workers(&self) -> usize {
        self.config.min_workers
    }

    pub async fn start_min_workers(self: &Arc<Self>) -> Result<()> {
        self.ensure_capacity(self.config.min_workers).await
    }

    pub async fn acquire(self: &Arc<Self>, _kind: WorkerKind) -> Result<Lease> {
        loop {
            let notified = self.notify.notified();
            let spawn_needed = {
                let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
                if inner.shutting_down {
                    bail!("worker pool is shutting down");
                }
                let lease_id = self.next_lease_id.fetch_add(1, Ordering::SeqCst);
                let selected = inner
                    .workers
                    .iter()
                    .filter(|(_, worker)| worker.active.len() < self.config.bundles_per_worker)
                    .min_by_key(|(_, worker)| worker.active.len())
                    .map(|(id, _)| id.clone());
                if let Some(selected) = selected {
                    let idle_elapsed = {
                        let worker = inner
                            .workers
                            .get_mut(&selected)
                            .expect("selected worker must exist");
                        let idle_elapsed = if worker.active.is_empty() {
                            worker.idle_since.elapsed().as_secs_f64()
                        } else {
                            0.0
                        };
                        let started = Instant::now();
                        worker.active.insert(lease_id, started);
                        idle_elapsed
                    };
                    inner.total_idle_seconds += idle_elapsed;
                    let worker = inner
                        .workers
                        .get(&selected)
                        .expect("selected worker must exist");
                    let started = *worker
                        .active
                        .get(&lease_id)
                        .expect("leased worker slot must exist");
                    return Ok(Lease {
                        pool: Arc::clone(self),
                        worker_id: worker.id.clone(),
                        lease_id,
                        started,
                    });
                }
                if inner.workers.len() + inner.starting < self.config.max_workers {
                    inner.starting += 1;
                    true
                } else {
                    false
                }
            };
            if !spawn_needed {
                notified.await;
                continue;
            }
            if let Err(error) = self.spawn_one().await {
                let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
                inner.starting = inner.starting.saturating_sub(1);
                drop(inner);
                self.notify.notify_waiters();
                return Err(error);
            }
        }
    }

    pub async fn ensure_capacity(self: &Arc<Self>, target: usize) -> Result<()> {
        let target = target.min(self.config.max_workers);
        let to_spawn = {
            let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
            if inner.shutting_down {
                bail!("worker pool is shutting down");
            }
            let missing = target.saturating_sub(inner.workers.len() + inner.starting);
            inner.starting += missing;
            missing
        };
        if to_spawn == 0 {
            return Ok(());
        }

        let mut tasks = JoinSet::new();
        for _ in 0..to_spawn {
            let pool = Arc::clone(self);
            tasks.spawn(async move { pool.spawn_one().await });
        }
        let mut first_error = None;
        while let Some(result) = tasks.join_next().await {
            let result = result.map_err(|error| anyhow!("worker spawn task failed: {error}"))?;
            if let Err(error) = result {
                let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
                inner.starting = inner.starting.saturating_sub(1);
                first_error.get_or_insert(error);
            }
        }
        self.notify.notify_waiters();
        first_error.map_or(Ok(()), Err)
    }

    async fn spawn_one(self: &Arc<Self>) -> Result<()> {
        let started = Instant::now();
        let spawned = self.backend.spawn_worker().await?;
        let startup = started.elapsed();
        let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
        inner.starting = inner.starting.saturating_sub(1);
        inner.spawn_count += 1;
        inner.spawn_cost = Some(match inner.spawn_cost {
            Some(previous) => {
                Duration::from_secs_f64(previous.as_secs_f64() * 0.8 + startup.as_secs_f64() * 0.2)
            }
            None => startup,
        });
        inner.workers.insert(
            spawned.id.as_str().to_string(),
            WorkerEntry {
                id: spawned.id,
                pid: spawned.pid,
                handle: spawned.handle,
                active: HashMap::new(),
                bundles_done: 0,
                busy_seconds: 0.0,
                idle_since: Instant::now(),
            },
        );
        drop(inner);
        self.notify.notify_waiters();
        Ok(())
    }

    pub async fn shrink_to(self: &Arc<Self>, target: usize) -> Result<()> {
        let target = target.max(self.config.min_workers);
        let stop_ids = {
            let inner = self.inner.lock().expect("worker pool lock not poisoned");
            let mut idle: Vec<(String, WorkerId)> = inner
                .workers
                .iter()
                .filter(|(_, worker)| worker.active.is_empty())
                .map(|(id, worker)| (id.clone(), worker.id.clone()))
                .collect();
            let allowed = target.saturating_sub(
                inner
                    .workers
                    .values()
                    .filter(|worker| !worker.active.is_empty())
                    .count(),
            );
            idle.truncate(inner.workers.len().saturating_sub(allowed));
            idle.into_iter().map(|(_, id)| id).collect::<Vec<_>>()
        };
        for worker_id in stop_ids {
            self.backend.stop_worker(&worker_id).await?;
            self.inner
                .lock()
                .expect("worker pool lock not poisoned")
                .workers
                .remove(worker_id.as_str());
        }
        self.notify.notify_waiters();
        Ok(())
    }

    pub fn record_bundle_complete(&self, worker_id: &WorkerId) {
        let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
        inner.completed_bundles += 1;
        if let Some(worker) = inner.workers.get_mut(worker_id.as_str()) {
            worker.bundles_done += 1;
        }
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        let inner = self.inner.lock().expect("worker pool lock not poisoned");
        let workers: Vec<WorkerSnapshot> = inner
            .workers
            .values()
            .map(|worker| WorkerSnapshot {
                id: worker.id.as_str().to_string(),
                pid: worker.pid,
                active_bundles: worker.active.len(),
                bundles_done: worker.bundles_done,
                busy_seconds: worker.busy_seconds,
                idle_seconds: if worker.active.is_empty() {
                    worker.idle_since.elapsed().as_secs_f64()
                } else {
                    0.0
                },
            })
            .collect();
        let busy = workers
            .iter()
            .filter(|worker| worker.active_bundles > 0)
            .count();
        PoolSnapshot {
            max_workers: self.config.max_workers,
            min_workers: self.config.min_workers,
            live: workers.len(),
            busy,
            idle: workers.len().saturating_sub(busy),
            starting: inner.starting,
            spawn_count: inner.spawn_count,
            completed_bundles: inner.completed_bundles,
            total_busy_seconds: inner.total_busy_seconds,
            total_idle_seconds: inner.total_idle_seconds
                + workers
                    .iter()
                    .map(|worker| worker.idle_seconds)
                    .sum::<f64>(),
            spawn_cost_ms: inner.spawn_cost.map(|value| value.as_millis()),
            workers,
        }
    }

    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        let worker_ids = {
            let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
            inner.shutting_down = true;
            inner
                .workers
                .keys()
                .cloned()
                .map(WorkerId::from_string)
                .collect::<Vec<_>>()
        };
        let mut first_error = None;
        for worker_id in worker_ids {
            if let Err(error) = self.backend.stop_worker(&worker_id).await {
                first_error.get_or_insert(error);
            }
        }
        self.inner
            .lock()
            .expect("worker pool lock not poisoned")
            .workers
            .clear();
        self.notify.notify_waiters();
        first_error.map_or(Ok(()), Err)
    }

    fn start_exit_monitor(self: &Arc<Self>) {
        let pool = Arc::clone(self);
        let mut exits = self.backend.subscribe_exits();
        tokio::spawn(async move {
            while let Ok(exit) = exits.recv().await {
                pool.backend.worker_exited(&exit);
                pool.inner
                    .lock()
                    .expect("worker pool lock not poisoned")
                    .workers
                    .remove(exit.worker_id.as_str());
                pool.notify.notify_waiters();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::harness::{HarnessStatus, WorkerChannelRegistry};
    use crate::worker::manager::WorkerLogRouter;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    struct FakeBackend {
        next: AtomicUsize,
        live: StdMutex<Vec<WorkerId>>,
        exit_tx: broadcast::Sender<WorkerExit>,
        fail: AtomicBool,
    }

    impl FakeBackend {
        fn new() -> Self {
            let (exit_tx, _) = broadcast::channel(16);
            Self {
                next: AtomicUsize::new(0),
                live: StdMutex::new(Vec::new()),
                exit_tx,
                fail: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl PoolWorkerBackend for FakeBackend {
        async fn spawn_worker(&self) -> Result<SpawnedWorker> {
            if self.fail.load(Ordering::SeqCst) {
                bail!("fake spawn failure");
            }
            let id = WorkerId::from_string(format!(
                "fake-w{}",
                self.next.fetch_add(1, Ordering::SeqCst)
            ));
            self.live.lock().expect("fake live lock").push(id.clone());
            let registry = WorkerChannelRegistry::new(HarnessStatus::new(), WorkerLogRouter::new());
            Ok(SpawnedWorker {
                handle: registry.handle(&id),
                id,
                pid: None,
            })
        }

        async fn stop_worker(&self, worker_id: &WorkerId) -> Result<()> {
            self.live
                .lock()
                .expect("fake live lock")
                .retain(|id| id != worker_id);
            Ok(())
        }

        fn subscribe_exits(&self) -> broadcast::Receiver<WorkerExit> {
            self.exit_tx.subscribe()
        }

        fn worker_exited(&self, _exit: &WorkerExit) {}
    }

    fn config(max: usize, min: usize, slots: usize) -> WorkerPoolConfig {
        WorkerPoolConfig {
            max_workers: max,
            min_workers: min,
            idle_timeout: Duration::from_secs(1),
            bundles_per_worker: slots,
            log_interval: Duration::from_secs(5),
            memory_budget_bytes: 8 * 1024 * 1024 * 1024,
            harness_rss_bytes: 1024 * 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn pool_starts_minimum_and_acquires_additional_workers() {
        let backend = Arc::new(FakeBackend::new());
        let pool = WorkerPool::new(config(3, 1, 1), backend.clone());
        pool.start_min_workers().await.unwrap();
        assert_eq!(pool.snapshot().live, 1);

        let first = pool.acquire(WorkerKind::Cpu).await.unwrap();
        assert_eq!(first.id().as_str(), "fake-w0");
        let second = pool.acquire(WorkerKind::Cpu).await.unwrap();
        assert_eq!(second.id().as_str(), "fake-w1");
        assert_eq!(pool.snapshot().live, 2);
        assert_eq!(pool.snapshot().busy, 2);
        drop(first);
        drop(second);
        assert_eq!(pool.snapshot().idle, 2);
    }

    #[tokio::test]
    async fn bundles_per_worker_reuses_worker_slots() {
        let backend = Arc::new(FakeBackend::new());
        let pool = WorkerPool::new(config(2, 1, 2), backend);
        pool.start_min_workers().await.unwrap();
        let first = pool.acquire(WorkerKind::Io).await.unwrap();
        let second = pool.acquire(WorkerKind::Io).await.unwrap();
        assert_eq!(first.id(), second.id());
        assert_eq!(pool.snapshot().live, 1);
        assert_eq!(pool.snapshot().workers[0].active_bundles, 2);
    }

    #[tokio::test]
    async fn ensure_capacity_is_bounded_and_shrink_removes_idle_workers() {
        let backend = Arc::new(FakeBackend::new());
        let pool = WorkerPool::new(config(4, 1, 1), backend);
        pool.ensure_capacity(10).await.unwrap();
        assert_eq!(pool.snapshot().live, 4);
        pool.shrink_to(1).await.unwrap();
        assert_eq!(pool.snapshot().live, 1);
    }
}
