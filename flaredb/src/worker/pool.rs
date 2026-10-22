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
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinSet;

use crate::{
    engine::harness::{WorkerChannelRegistry, WorkerHandle},
    worker::manager::{WorkerExit, WorkerId, WorkerManager, WorkerRuntime},
};

const DEFAULT_HARNESS_RSS_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_MEMORY_FRACTION: f64 = 0.70;
const MIN_SCALE_RATE_SAMPLE: Duration = Duration::from_secs(1);
const MIN_SCALE_TARGET: Duration = Duration::from_secs(10);
const SPAWN_COST_MULTIPLIER: f64 = 3.0;
const SCALE_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerKind {
    Cpu,
    Io,
}

/// Inputs to the capacity-driven scale-out policy.
///
/// Throughput is measured in completed bundles per worker-second. The remaining
/// work is therefore converted to an estimated single-worker duration before
/// deciding how many workers are useful. Stage 7 replaces the bundle-count
/// estimate with the size-weighted per-bundle progress estimator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScaleDecisionInputs {
    pub remaining_bundles: usize,
    pub active_bundles: usize,
    pub completed_bundles: u64,
    pub busy_worker_seconds: f64,
    pub spawn_cost: Duration,
    pub min_workers: usize,
    pub max_workers: usize,
    pub live_workers: usize,
    pub starting_workers: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScaleReason {
    WorkComplete,
    NoParallelWork,
    NoRateSample,
    ScaleUp,
    ScaleDown,
    Hold,
}

impl ScaleReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkComplete => "work-complete",
            Self::NoParallelWork => "no-parallel-work",
            Self::NoRateSample => "no-rate-sample",
            Self::ScaleUp => "scale-up",
            Self::ScaleDown => "scale-down",
            Self::Hold => "hold",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScaleDecision {
    pub target_seconds_per_worker: f64,
    pub observed_rate_bundles_per_second: Option<f64>,
    pub estimated_remaining_seconds: Option<f64>,
    pub want_workers: usize,
    pub reason: ScaleReason,
}

/// Pure scale-out policy. Short jobs and jobs without at least one second of
/// observed worker activity remain at the configured minimum.
pub fn decide_scale(inputs: &ScaleDecisionInputs) -> ScaleDecision {
    let min_workers = inputs.min_workers.max(1).min(inputs.max_workers.max(1));
    let max_workers = inputs.max_workers.max(min_workers);
    let spawn_cost = inputs.spawn_cost.as_secs_f64();
    let target_seconds_per_worker = MIN_SCALE_TARGET
        .as_secs_f64()
        .max(SPAWN_COST_MULTIPLIER * spawn_cost);
    let current_workers = inputs.live_workers.saturating_add(inputs.starting_workers);

    if inputs.remaining_bundles == 0 {
        return ScaleDecision {
            target_seconds_per_worker,
            observed_rate_bundles_per_second: None,
            estimated_remaining_seconds: Some(0.0),
            want_workers: min_workers,
            reason: ScaleReason::WorkComplete,
        };
    }

    // A running bundle with elsewhere-queued work is enough for a coarse
    // sample. This lets a long parent that produced residuals scale workers
    // without first requiring the parent itself to finish. A single long
    // unsplittable bundle does not benefit from another worker, so it stays at
    // the minimum until queued work appears.
    let has_parallel_work = inputs.remaining_bundles > inputs.active_bundles;
    if !has_parallel_work {
        return ScaleDecision {
            target_seconds_per_worker,
            observed_rate_bundles_per_second: None,
            estimated_remaining_seconds: None,
            want_workers: min_workers,
            reason: ScaleReason::NoParallelWork,
        };
    }

    let rate_is_usable = inputs.busy_worker_seconds >= MIN_SCALE_RATE_SAMPLE.as_secs_f64();
    if !rate_is_usable {
        return ScaleDecision {
            target_seconds_per_worker,
            observed_rate_bundles_per_second: None,
            estimated_remaining_seconds: None,
            want_workers: min_workers,
            reason: ScaleReason::NoRateSample,
        };
    }

    // Before the first completion, assume one completion per accumulated
    // worker-second. This deliberately overestimates remaining time while a
    // long bundle is active, avoiding the false-stall problem that motivated
    // progress-based sampling.
    let observed_completions = (inputs.completed_bundles as f64).max(1.0);
    let observed_rate = observed_completions / inputs.busy_worker_seconds;
    let remaining_seconds = inputs.remaining_bundles as f64 / observed_rate;
    let raw_want = (remaining_seconds / target_seconds_per_worker).ceil();
    let want_workers = if raw_want.is_finite() {
        (raw_want.max(1.0) as usize).clamp(min_workers, max_workers)
    } else {
        max_workers
    };
    let reason = match want_workers.cmp(&current_workers) {
        std::cmp::Ordering::Greater => ScaleReason::ScaleUp,
        std::cmp::Ordering::Less => ScaleReason::ScaleDown,
        std::cmp::Ordering::Equal => ScaleReason::Hold,
    };

    ScaleDecision {
        target_seconds_per_worker,
        observed_rate_bundles_per_second: Some(observed_rate),
        estimated_remaining_seconds: Some(remaining_seconds),
        want_workers,
        reason,
    }
}

#[derive(Clone, Debug)]
pub struct WorkerPoolConfig {
    pub job_id: String,
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
            job_id: String::new(),
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

struct ActiveLease {
    started: Instant,
    stage: Option<String>,
    instruction_id: Option<String>,
}

struct WorkerEntry {
    id: WorkerId,
    pid: Option<u32>,
    handle: WorkerHandle,
    active: HashMap<u64, ActiveLease>,
    stopping: bool,
    bundles_done: u64,
    busy_seconds: f64,
    idle_since: Instant,
}

struct PoolInner {
    starting: usize,
    shutting_down: bool,
    workers: HashMap<String, WorkerEntry>,
    remaining_bundles: usize,
    spawn_count: u64,
    spawn_cost: Option<Duration>,
    total_busy_seconds: f64,
    total_idle_seconds: f64,
    completed_bundles: u64,
    peak_workers: usize,
    total_output_elements: u64,
    reported_remaining_work_seconds: Option<f64>,
    queue_depth: usize,
    in_flight_bundles: usize,
    splits: u64,
    started_at: Instant,
    last_scale_log: Option<(ScaleReason, usize)>,
    last_worker_log: HashMap<String, (String, Option<String>)>,
    last_worker_log_at: HashMap<String, Instant>,
    process_system: System,
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

    pub fn token(&self) -> u64 {
        self.lease_id
    }

    pub fn annotate_stage(&mut self, stage: impl Into<String>) {
        if let Some(lease) = self
            .pool
            .inner
            .lock()
            .expect("worker pool lock not poisoned")
            .workers
            .get_mut(self.worker_id.as_str())
            .and_then(|worker| worker.active.get_mut(&self.lease_id))
        {
            lease.stage = Some(stage.into());
        }
    }

    pub fn annotate_instruction(&self, lease_id: u64, instruction_id: &str) {
        if let Some(lease) = self
            .pool
            .inner
            .lock()
            .expect("worker pool lock not poisoned")
            .workers
            .get_mut(self.worker_id.as_str())
            .and_then(|worker| worker.active.get_mut(&lease_id))
        {
            lease.instruction_id = Some(instruction_id.to_string());
        }
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
            let decision = scale_decision(&self.pool.config, &inner);
            log_scale_decision(&self.pool.config, &mut inner, decision, "lease-release");
        }
        self.pool.notify.notify_waiters();
    }
}

#[derive(Clone, Debug, Default)]
pub struct WorkerSnapshot {
    pub id: String,
    pub pid: Option<u32>,
    pub state: String,
    pub bundle: Option<String>,
    pub oldest_bundle_age_seconds: Option<f64>,
    pub active_bundles: usize,
    pub bundles_done: u64,
    pub busy_seconds: f64,
    pub idle_seconds: f64,
    pub rss_mb: Option<f64>,
    pub cpu_pct: Option<f32>,
}

#[derive(Clone, Debug, Default)]
pub struct PoolSnapshot {
    pub max_workers: usize,
    pub min_workers: usize,
    pub live: usize,
    pub busy: usize,
    pub idle: usize,
    pub starting: usize,
    pub peak_workers: usize,
    pub remaining_bundles: usize,
    pub queue_depth: usize,
    pub in_flight_bundles: usize,
    pub splits: u64,
    pub want_workers: usize,
    pub scale_reason: String,
    pub target_seconds_per_worker: f64,
    pub observed_rate_bundles_per_second: Option<f64>,
    pub remaining_work_seconds: Option<f64>,
    pub estimated_remaining_seconds: Option<f64>,
    pub spawn_count: u64,
    pub completed_bundles: u64,
    pub output_elements: u64,
    pub output_elements_per_second: f64,
    pub utilization: f64,
    pub total_busy_seconds: f64,
    pub total_idle_seconds: f64,
    pub spawn_cost_ms: Option<u128>,
    pub workers: Vec<WorkerSnapshot>,
}

fn scale_decision(config: &WorkerPoolConfig, inner: &PoolInner) -> ScaleDecision {
    let active_bundles = inner
        .workers
        .values()
        .map(|worker| worker.active.len())
        .sum();
    let active_worker_seconds = inner
        .workers
        .values()
        .flat_map(|worker| worker.active.values())
        .map(|lease| lease.started.elapsed())
        .map(|elapsed| elapsed.as_secs_f64())
        .sum::<f64>();
    decide_scale(&ScaleDecisionInputs {
        remaining_bundles: inner.remaining_bundles,
        active_bundles,
        completed_bundles: inner.completed_bundles,
        busy_worker_seconds: inner.total_busy_seconds + active_worker_seconds,
        spawn_cost: inner
            .spawn_cost
            .unwrap_or_else(|| config.idle_timeout.min(MIN_SCALE_TARGET)),
        min_workers: config.min_workers,
        max_workers: config.max_workers,
        live_workers: inner
            .workers
            .values()
            .filter(|worker| !worker.stopping)
            .count(),
        starting_workers: inner.starting,
    })
}

fn log_scale_decision(
    config: &WorkerPoolConfig,
    inner: &mut PoolInner,
    decision: ScaleDecision,
    source: &str,
) {
    let decision_key = (decision.reason, decision.want_workers);
    if inner.last_scale_log == Some(decision_key) {
        log::debug!(
            "pool scale: source={source} reason={} remaining_bundles={} active_bundles={} \
             want={} live={} starting={} spawn_cost_ms={}",
            decision.reason.as_str(),
            inner.remaining_bundles,
            inner
                .workers
                .values()
                .map(|worker| worker.active.len())
                .sum::<usize>(),
            decision.want_workers,
            inner
                .workers
                .values()
                .filter(|worker| !worker.stopping)
                .count(),
            inner.starting,
            inner.spawn_cost.map(|value| value.as_millis()).unwrap_or(0)
        );
        return;
    }
    inner.last_scale_log = Some(decision_key);
    log::info!(
        "pool scale: source={source} reason={} remaining_bundles={} active_bundles={} \
         rate_bundles_s={:?} remaining_work_s={:?} target_s={:.2} want={} live={} starting={} \
         spawn_cost_ms={} max={} min={}",
        decision.reason.as_str(),
        inner.remaining_bundles,
        inner
            .workers
            .values()
            .map(|worker| worker.active.len())
            .sum::<usize>(),
        decision.observed_rate_bundles_per_second,
        decision.estimated_remaining_seconds,
        decision.target_seconds_per_worker,
        decision.want_workers,
        inner
            .workers
            .values()
            .filter(|worker| !worker.stopping)
            .count(),
        inner.starting,
        inner.spawn_cost.map(|value| value.as_millis()).unwrap_or(0),
        config.max_workers,
        config.min_workers
    );
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
                remaining_bundles: 0,
                spawn_count: 0,
                spawn_cost: None,
                total_busy_seconds: 0.0,
                total_idle_seconds: 0.0,
                completed_bundles: 0,
                peak_workers: 0,
                total_output_elements: 0,
                reported_remaining_work_seconds: None,
                queue_depth: 0,
                in_flight_bundles: 0,
                splits: 0,
                started_at: Instant::now(),
                last_scale_log: None,
                last_worker_log: HashMap::new(),
                last_worker_log_at: HashMap::new(),
                process_system: System::new(),
            }),
            notify: Notify::new(),
            next_lease_id: AtomicU64::new(1),
        });
        pool.start_exit_monitor();
        pool.start_scale_monitor();
        pool.start_pool_log_monitor();
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

    /// Publish the number of queued plus in-flight bundles. The dispatcher
    /// updates this before starting a scheduling wave and whenever work is
    /// queued or drained.
    pub fn observe_workload(&self, remaining_bundles: usize) {
        let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
        inner.remaining_bundles = remaining_bundles;
        let decision = scale_decision(&self.config, &inner);
        log_scale_decision(&self.config, &mut inner, decision, "workload");
        drop(inner);
        self.notify.notify_waiters();
    }

    pub fn observe_dispatcher_state(
        &self,
        queue_depth: usize,
        in_flight_bundles: usize,
        splits: u64,
    ) {
        let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
        inner.queue_depth = queue_depth;
        inner.in_flight_bundles = in_flight_bundles;
        inner.splits = splits;
    }

    pub fn observe_remaining_work_seconds(&self, seconds: Option<f64>) {
        self.inner
            .lock()
            .expect("worker pool lock not poisoned")
            .reported_remaining_work_seconds = seconds;
    }

    pub fn record_output_elements(&self, count: u64) {
        if count == 0 {
            return;
        }
        let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
        inner.total_output_elements = inner.total_output_elements.saturating_add(count);
    }

    pub fn annotate_instruction(&self, worker_id: &WorkerId, lease_id: u64, instruction_id: &str) {
        if let Some(lease) = self
            .inner
            .lock()
            .expect("worker pool lock not poisoned")
            .workers
            .get_mut(worker_id.as_str())
            .and_then(|worker| worker.active.get_mut(&lease_id))
        {
            lease.instruction_id = Some(instruction_id.to_string());
        }
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
                    .filter(|(_, worker)| {
                        !worker.stopping && worker.active.len() < self.config.bundles_per_worker
                    })
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
                        worker.active.insert(
                            lease_id,
                            ActiveLease {
                                started,
                                stage: None,
                                instruction_id: None,
                            },
                        );
                        idle_elapsed
                    };
                    inner.total_idle_seconds += idle_elapsed;
                    let worker = inner
                        .workers
                        .get(&selected)
                        .expect("selected worker must exist");
                    let started = worker
                        .active
                        .get(&lease_id)
                        .expect("leased worker slot must exist")
                        .started;
                    return Ok(Lease {
                        pool: Arc::clone(self),
                        worker_id: worker.id.clone(),
                        lease_id,
                        started,
                    });
                }
                let decision = scale_decision(&self.config, &inner);
                log_scale_decision(&self.config, &mut inner, decision, "acquire");
                let current_workers = inner
                    .workers
                    .values()
                    .filter(|worker| !worker.stopping)
                    .count()
                    .saturating_add(inner.starting);
                if current_workers < decision.want_workers
                    && current_workers < self.config.max_workers
                {
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
        let spawned_id = spawned.id.as_str().to_string();
        let spawned_pid = spawned.pid;
        inner.workers.insert(
            spawned_id.clone(),
            WorkerEntry {
                id: spawned.id,
                pid: spawned.pid,
                handle: spawned.handle,
                active: HashMap::new(),
                stopping: false,
                bundles_done: 0,
                busy_seconds: 0.0,
                idle_since: Instant::now(),
            },
        );
        inner.peak_workers = inner.peak_workers.max(
            inner
                .workers
                .values()
                .filter(|worker| !worker.stopping)
                .count(),
        );
        log::info!(
            "worker spawned: id={} pid={} startup_ms={} live={} peak={} reason=pool-demand",
            spawned_id,
            spawned_pid.map_or_else(|| "na".to_string(), |pid| pid.to_string()),
            startup.as_millis(),
            inner
                .workers
                .values()
                .filter(|worker| !worker.stopping)
                .count(),
            inner.peak_workers
        );
        let decision = scale_decision(&self.config, &inner);
        log_scale_decision(&self.config, &mut inner, decision, "spawn-complete");
        drop(inner);
        self.notify.notify_waiters();
        Ok(())
    }

    pub async fn shrink_to(self: &Arc<Self>, target: usize) -> Result<()> {
        let target = target.max(self.config.min_workers);
        let stop_ids = {
            let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
            let mut idle: Vec<(String, WorkerId)> = inner
                .workers
                .iter()
                .filter(|(_, worker)| !worker.stopping && worker.active.is_empty())
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
            let ids = idle.into_iter().map(|(_, id)| id).collect::<Vec<_>>();
            for worker_id in &ids {
                if let Some(worker) = inner.workers.get_mut(worker_id.as_str()) {
                    worker.stopping = true;
                }
            }
            ids
        };
        self.stop_selected(&stop_ids, "shrink-to-min").await
    }

    pub fn record_bundle_complete(&self, worker_id: &WorkerId) {
        let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
        inner.completed_bundles += 1;
        if let Some(worker) = inner.workers.get_mut(worker_id.as_str()) {
            worker.bundles_done += 1;
        }
    }

    pub async fn reconcile_scale(self: &Arc<Self>) -> Result<()> {
        let (decision, live) = {
            let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
            let decision = scale_decision(&self.config, &inner);
            let live = inner
                .workers
                .values()
                .filter(|worker| !worker.stopping)
                .count();
            log_scale_decision(&self.config, &mut inner, decision, "reconcile");
            (decision, live)
        };
        let current = live.saturating_add(
            self.inner
                .lock()
                .expect("worker pool lock not poisoned")
                .starting,
        );
        if decision.want_workers > current {
            self.ensure_capacity(decision.want_workers).await?;
            return Ok(());
        }

        let floor = self.config.min_workers.max(decision.want_workers);
        if live <= floor {
            return Ok(());
        }
        let stop_ids = {
            let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
            let stop_limit = live.saturating_sub(floor);
            let mut candidates = inner
                .workers
                .iter()
                .filter(|(_, worker)| {
                    !worker.stopping
                        && worker.active.is_empty()
                        && worker.idle_since.elapsed() >= self.config.idle_timeout
                })
                .map(|(id, worker)| (worker.idle_since, id.clone(), worker.id.clone()))
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(idle_since, _, _)| *idle_since);
            candidates.truncate(stop_limit);
            let ids = candidates
                .into_iter()
                .map(|(_, _, worker_id)| worker_id)
                .collect::<Vec<_>>();
            for worker_id in &ids {
                if let Some(worker) = inner.workers.get_mut(worker_id.as_str()) {
                    worker.stopping = true;
                }
            }
            ids
        };
        if stop_ids.is_empty() {
            return Ok(());
        }
        log::info!(
            "pool scale: source=idle-timeout reason=scale-down stopping={} want={} live={} \
             floor={} idle_timeout_s={}",
            stop_ids.len(),
            decision.want_workers,
            live,
            floor,
            self.config.idle_timeout.as_secs()
        );
        self.stop_selected(&stop_ids, "idle-timeout").await
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        self.snapshot_with_processes(false)
    }

    pub fn telemetry_snapshot(&self) -> PoolSnapshot {
        self.snapshot_with_processes(true)
    }

    fn snapshot_with_processes(&self, include_processes: bool) -> PoolSnapshot {
        let mut inner = self.inner.lock().expect("worker pool lock not poisoned");
        if include_processes {
            let pids = inner
                .workers
                .values()
                .filter_map(|worker| worker.pid)
                .map(Pid::from_u32)
                .collect::<Vec<_>>();
            if !pids.is_empty() {
                inner
                    .process_system
                    .refresh_processes(ProcessesToUpdate::Some(&pids), true);
            }
        }
        let workers: Vec<WorkerSnapshot> = inner
            .workers
            .values()
            .filter(|worker| !worker.stopping)
            .map(|worker| {
                let oldest = worker
                    .active
                    .values()
                    .min_by_key(|lease| lease.started)
                    .map(|lease| {
                        (
                            lease
                                .instruction_id
                                .clone()
                                .or_else(|| {
                                    lease.stage.as_ref().map(|stage| format!("stage:{stage}"))
                                })
                                .unwrap_or_else(|| "unknown".to_string()),
                            lease.started.elapsed().as_secs_f64(),
                        )
                    });
                let process = include_processes
                    .then(|| {
                        worker
                            .pid
                            .and_then(|pid| inner.process_system.process(Pid::from_u32(pid)))
                    })
                    .flatten();
                WorkerSnapshot {
                    id: worker.id.as_str().to_string(),
                    pid: worker.pid,
                    state: if worker.active.is_empty() {
                        "idle".to_string()
                    } else {
                        "busy".to_string()
                    },
                    bundle: oldest.as_ref().map(|(bundle, _)| bundle.clone()),
                    oldest_bundle_age_seconds: oldest.map(|(_, age)| age),
                    active_bundles: worker.active.len(),
                    bundles_done: worker.bundles_done,
                    busy_seconds: worker.busy_seconds,
                    idle_seconds: if worker.active.is_empty() {
                        worker.idle_since.elapsed().as_secs_f64()
                    } else {
                        0.0
                    },
                    rss_mb: process.map(|process| process.memory() as f64 / 1024.0 / 1024.0),
                    cpu_pct: process.map(|process| process.cpu_usage()),
                }
            })
            .collect();
        let decision = scale_decision(&self.config, &inner);
        let busy = workers
            .iter()
            .filter(|worker| worker.active_bundles > 0)
            .count();
        let active_worker_seconds = inner
            .workers
            .values()
            .flat_map(|worker| worker.active.values())
            .map(|lease| lease.started.elapsed().as_secs_f64())
            .sum::<f64>();
        let total_idle_seconds = inner.total_idle_seconds
            + workers
                .iter()
                .map(|worker| worker.idle_seconds)
                .sum::<f64>();
        let busy_worker_seconds = inner.total_busy_seconds + active_worker_seconds;
        let observed_worker_seconds = busy_worker_seconds + total_idle_seconds;
        let utilization = if observed_worker_seconds > 0.0 {
            (busy_worker_seconds / observed_worker_seconds).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let elapsed = inner.started_at.elapsed().as_secs_f64().max(f64::EPSILON);
        let remaining_work_seconds = inner
            .reported_remaining_work_seconds
            .or_else(|| (inner.remaining_bundles == 0).then_some(0.0));
        let estimated_remaining_seconds =
            remaining_work_seconds.map(|seconds| seconds / workers.len().max(1) as f64);
        PoolSnapshot {
            max_workers: self.config.max_workers,
            min_workers: self.config.min_workers,
            live: workers.len(),
            busy,
            idle: workers.len().saturating_sub(busy),
            starting: inner.starting,
            peak_workers: inner.peak_workers,
            remaining_bundles: inner.remaining_bundles,
            queue_depth: inner.queue_depth,
            in_flight_bundles: inner.in_flight_bundles,
            splits: inner.splits,
            want_workers: decision.want_workers,
            scale_reason: decision.reason.as_str().to_string(),
            target_seconds_per_worker: decision.target_seconds_per_worker,
            observed_rate_bundles_per_second: decision.observed_rate_bundles_per_second,
            remaining_work_seconds,
            estimated_remaining_seconds,
            spawn_count: inner.spawn_count,
            completed_bundles: inner.completed_bundles,
            output_elements: inner.total_output_elements,
            output_elements_per_second: inner.total_output_elements as f64 / elapsed,
            utilization,
            total_busy_seconds: busy_worker_seconds,
            total_idle_seconds,
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
            } else {
                log::info!(
                    "worker stopped: id={} reason=job-shutdown",
                    worker_id.as_str()
                );
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

    async fn stop_selected(&self, worker_ids: &[WorkerId], reason: &str) -> Result<()> {
        let mut first_error = None;
        for worker_id in worker_ids {
            match self.backend.stop_worker(worker_id).await {
                Ok(()) => {
                    log::info!("worker stopped: id={} reason={reason}", worker_id.as_str());
                    self.inner
                        .lock()
                        .expect("worker pool lock not poisoned")
                        .workers
                        .remove(worker_id.as_str());
                }
                Err(error) => {
                    if let Some(worker) = self
                        .inner
                        .lock()
                        .expect("worker pool lock not poisoned")
                        .workers
                        .get_mut(worker_id.as_str())
                    {
                        worker.stopping = false;
                    }
                    first_error.get_or_insert(error);
                }
            }
        }
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

    fn start_scale_monitor(self: &Arc<Self>) {
        let pool = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SCALE_RECONCILE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(pool) = pool.upgrade() else {
                    break;
                };
                if pool
                    .inner
                    .lock()
                    .expect("worker pool lock not poisoned")
                    .shutting_down
                {
                    break;
                }
                if let Err(error) = pool.reconcile_scale().await {
                    log::error!("pool scale reconciliation failed: {error}");
                }
            }
        });
    }

    fn start_pool_log_monitor(self: &Arc<Self>) {
        let pool = Arc::downgrade(self);
        let interval_duration = self.config.log_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(pool) = pool.upgrade() else {
                    break;
                };
                if pool
                    .inner
                    .lock()
                    .expect("worker pool lock not poisoned")
                    .shutting_down
                {
                    break;
                }
                let snapshot = pool.telemetry_snapshot();
                log::info!(
                    "pool: job={} max={} live={} busy={} idle={} starting={} queue={} \
                     in_flight={} done_bundles={} splits={} remaining_work_s={:?} \
                     eta_s={:?} want={} out_rows_per_s={:.0} util={:.2} peak={} spawned={}",
                    pool.config.job_id,
                    snapshot.max_workers,
                    snapshot.live,
                    snapshot.busy,
                    snapshot.idle,
                    snapshot.starting,
                    snapshot.queue_depth,
                    snapshot.in_flight_bundles,
                    snapshot.completed_bundles,
                    snapshot.splits,
                    snapshot.remaining_work_seconds,
                    snapshot.estimated_remaining_seconds,
                    snapshot.want_workers,
                    snapshot.output_elements_per_second,
                    snapshot.utilization,
                    snapshot.peak_workers,
                    snapshot.spawn_count
                );
                for worker in &snapshot.workers {
                    let key = (worker.state.clone(), worker.bundle.clone());
                    let changed = {
                        let mut inner = pool.inner.lock().expect("worker pool lock not poisoned");
                        if inner.last_worker_log.get(&worker.id) == Some(&key) {
                            inner
                                .last_worker_log_at
                                .get(&worker.id)
                                .is_none_or(|last| last.elapsed() >= Duration::from_secs(30))
                        } else {
                            inner.last_worker_log.insert(worker.id.clone(), key);
                            true
                        }
                    };
                    if changed {
                        pool.inner
                            .lock()
                            .expect("worker pool lock not poisoned")
                            .last_worker_log_at
                            .insert(worker.id.clone(), Instant::now());
                        log::info!(
                            "worker {}: pid={} state={} bundle={} bundle_age_s={:?} \
                             bundles_done={} busy_s={:.1} idle_s={:.1} rss_mb={:?} cpu_pct={:?}",
                            worker.id,
                            worker
                                .pid
                                .map_or_else(|| "na".to_string(), |pid| pid.to_string()),
                            worker.state,
                            worker.bundle.as_deref().unwrap_or("none"),
                            worker.oldest_bundle_age_seconds,
                            worker.bundles_done,
                            worker.busy_seconds,
                            worker.idle_seconds,
                            worker.rss_mb,
                            worker.cpu_pct
                        );
                    }
                }
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
            job_id: "test-job".to_string(),
            max_workers: max,
            min_workers: min,
            idle_timeout: Duration::from_secs(1),
            bundles_per_worker: slots,
            log_interval: Duration::from_secs(5),
            memory_budget_bytes: 8 * 1024 * 1024 * 1024,
            harness_rss_bytes: 1024 * 1024 * 1024,
        }
    }

    fn scale_inputs(
        remaining_bundles: usize,
        completed_bundles: u64,
        busy_worker_seconds: f64,
        spawn_cost: Duration,
    ) -> ScaleDecisionInputs {
        ScaleDecisionInputs {
            remaining_bundles,
            active_bundles: 0,
            completed_bundles,
            busy_worker_seconds,
            spawn_cost,
            min_workers: 1,
            max_workers: 4,
            live_workers: 1,
            starting_workers: 0,
        }
    }

    #[test]
    fn scale_policy_waits_for_a_rate_sample() {
        let decision = decide_scale(&scale_inputs(
            100,
            0,
            MIN_SCALE_RATE_SAMPLE.as_secs_f64() - 0.01,
            Duration::from_millis(500),
        ));
        assert_eq!(decision.reason, ScaleReason::NoRateSample);
        assert_eq!(decision.want_workers, 1);
        assert_eq!(decision.observed_rate_bundles_per_second, None);
    }

    #[test]
    fn scale_policy_uses_active_progress_when_residual_work_is_queued() {
        let mut inputs = scale_inputs(4, 0, 12.0, Duration::from_secs(2));
        inputs.active_bundles = 1;
        let decision = decide_scale(&inputs);
        assert_eq!(decision.reason, ScaleReason::ScaleUp);
        assert_eq!(decision.want_workers, 4);
        assert_eq!(decision.estimated_remaining_seconds, Some(48.0));
    }

    #[test]
    fn scale_policy_does_not_add_a_worker_for_one_long_bundle() {
        let mut inputs = scale_inputs(1, 0, 30.0, Duration::from_secs(2));
        inputs.active_bundles = 1;
        let decision = decide_scale(&inputs);
        assert_eq!(decision.reason, ScaleReason::NoParallelWork);
        assert_eq!(decision.want_workers, 1);
    }

    #[test]
    fn scale_policy_does_not_spawn_for_the_short_csv_job() {
        let decision = decide_scale(&scale_inputs(2, 1, 1.0, Duration::from_secs(1)));
        assert_eq!(decision.reason, ScaleReason::Hold);
        assert_eq!(decision.want_workers, 1);
        assert_eq!(decision.target_seconds_per_worker, 10.0);
        assert_eq!(decision.estimated_remaining_seconds, Some(2.0));
    }

    #[test]
    fn scale_policy_uses_spawn_cost_and_remaining_work() {
        let mut inputs = scale_inputs(100, 2, 1.0, Duration::from_secs(2));
        let decision = decide_scale(&inputs);
        assert_eq!(decision.reason, ScaleReason::ScaleUp);
        assert_eq!(decision.want_workers, 4);
        assert_eq!(decision.target_seconds_per_worker, 10.0);
        assert_eq!(decision.estimated_remaining_seconds, Some(50.0));

        inputs.spawn_cost = Duration::from_secs(10);
        inputs.remaining_bundles = 61;
        let decision = decide_scale(&inputs);
        assert_eq!(decision.target_seconds_per_worker, 30.0);
        assert_eq!(decision.want_workers, 2);
    }

    #[test]
    fn scale_policy_returns_to_the_minimum_when_work_drains() {
        let mut inputs = scale_inputs(0, 10, 5.0, Duration::from_secs(1));
        inputs.live_workers = 4;
        let decision = decide_scale(&inputs);
        assert_eq!(decision.reason, ScaleReason::WorkComplete);
        assert_eq!(decision.want_workers, 1);
    }

    #[tokio::test]
    async fn pool_starts_minimum_and_acquires_additional_workers() {
        let backend = Arc::new(FakeBackend::new());
        let pool = WorkerPool::new(config(3, 1, 1), backend.clone());
        pool.start_min_workers().await.unwrap();
        assert_eq!(pool.snapshot().live, 1);

        let mut first = pool.acquire(WorkerKind::Cpu).await.unwrap();
        assert_eq!(first.id().as_str(), "fake-w0");
        first.annotate_stage("stage-a");
        pool.annotate_instruction(first.id(), first.token(), "instr-1");
        let active = pool.snapshot();
        assert_eq!(active.workers[0].state, "busy");
        assert_eq!(active.workers[0].bundle.as_deref(), Some("instr-1"));
        assert!(active.workers[0].oldest_bundle_age_seconds.is_some());
        pool.observe_dispatcher_state(4, 1, 2);
        pool.observe_remaining_work_seconds(Some(80.0));
        pool.record_output_elements(120);
        pool.record_bundle_complete(first.id());
        pool.observe_workload(100);
        {
            let mut inner = pool.inner.lock().expect("worker pool lock not poisoned");
            inner.completed_bundles = 2;
            inner.total_busy_seconds = 1.0;
        }
        let second = pool.acquire(WorkerKind::Cpu).await.unwrap();
        assert_eq!(second.id().as_str(), "fake-w1");
        assert_eq!(pool.snapshot().live, 2);
        assert_eq!(pool.snapshot().busy, 2);
        let snapshot = pool.snapshot();
        assert_eq!(snapshot.peak_workers, 2);
        assert_eq!(snapshot.queue_depth, 4);
        assert_eq!(snapshot.in_flight_bundles, 1);
        assert_eq!(snapshot.splits, 2);
        assert_eq!(snapshot.output_elements, 120);
        assert_eq!(snapshot.remaining_work_seconds, Some(80.0));
        assert_eq!(snapshot.estimated_remaining_seconds, Some(40.0));
        assert!(snapshot.output_elements_per_second > 0.0);
        assert!(snapshot.utilization > 0.0);
        drop(first);
        drop(second);
        assert_eq!(pool.snapshot().idle, 2);
    }

    #[tokio::test]
    async fn pool_does_not_spawn_another_worker_without_a_rate_sample() {
        let backend = Arc::new(FakeBackend::new());
        let pool = WorkerPool::new(config(3, 1, 1), backend);
        pool.start_min_workers().await.unwrap();
        pool.observe_workload(100);

        let first = pool.acquire(WorkerKind::Cpu).await.unwrap();
        let second =
            tokio::time::timeout(Duration::from_millis(25), pool.acquire(WorkerKind::Cpu)).await;
        assert!(
            second.is_err(),
            "pool scaled before observing a rate sample"
        );
        assert_eq!(pool.snapshot().live, 1);
        assert_eq!(pool.snapshot().scale_reason, "no-rate-sample");

        {
            let mut inner = pool.inner.lock().expect("worker pool lock not poisoned");
            inner.completed_bundles = 2;
            inner.total_busy_seconds = 1.0;
        }
        let second = pool.acquire(WorkerKind::Cpu).await.unwrap();
        assert_eq!(second.id().as_str(), "fake-w1");
        assert_eq!(pool.snapshot().live, 2);
        drop(first);
        drop(second);
    }

    #[tokio::test]
    async fn scale_down_stops_only_expired_idle_workers_above_the_floor() {
        let backend = Arc::new(FakeBackend::new());
        let pool = WorkerPool::new(config(3, 1, 1), backend);
        pool.start_min_workers().await.unwrap();
        pool.ensure_capacity(3).await.unwrap();
        pool.observe_workload(0);
        {
            let mut inner = pool.inner.lock().expect("worker pool lock not poisoned");
            let expired = Instant::now() - Duration::from_secs(2);
            for worker in inner.workers.values_mut() {
                worker.idle_since = expired;
            }
        }

        pool.reconcile_scale().await.unwrap();

        assert_eq!(pool.snapshot().live, 1);
        assert_eq!(pool.snapshot().want_workers, 1);
        assert_eq!(pool.snapshot().scale_reason, "work-complete");
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
