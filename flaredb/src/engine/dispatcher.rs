use crate::engine::metrics::JobMetrics;
use crate::engine::metrics::sum_element_count;
use crate::engine::sdf::{SdfBundleOutcome, SdfWorkItem, SplitManager, SplittableStageExecutor};
use crate::transforms::SourceProgress;
use crate::worker::pool::{WorkerKind, WorkerPool};
use crate::{
    engine::timer::{TimerEntry, build_timer_store},
    engine::{
        executor::StageExecutor,
        harness::{Channels, HarnessStatus, control::ControlResponse, state::SideInputRegistry},
        runtime::BundleRuntime,
        scheduler::NodeScheduler,
        timer::TimerService,
        watermark::{MIN_TIMESTAMP, Timestamp, format_timestamp},
    },
    fusion::pipeline::{ExecutableGraph, ExecutableNode},
    store::element_store::FlareElementStore,
};
use anyhow::anyhow;
use beam_model_rs::v1::{Coder, Components};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Outcome of one dispatched stage bundle.
///
/// Non-splittable stages carry no extra state; a splittable (SDF) stage returns
/// the residual work items it produced and the holds of the item it consumed, so
/// the dispatcher can queue them and clamp the stage's output watermark.
enum StageRun {
    Plain,
    Sdf { outcome: SdfBundleOutcome },
}

#[derive(Default)]
struct StageTelemetry {
    started_at: Option<Instant>,
    bundles: u64,
    output_elements: u64,
    bundle_durations_ms: Vec<u64>,
}

#[derive(Clone, Copy, Default)]
struct BundleObservation {
    duration: Duration,
    output_elements: u64,
}

/// Pending bundle work for one splittable stage.
#[derive(Default)]
struct SdfQueue {
    /// The initialization bundle has run (carried forward across bundles).
    initialized: bool,
    /// Residual work items awaiting a bundle, oldest first.
    items: VecDeque<SdfWorkItem>,
    /// Stage-specific duration between two consecutive `progress + split_evaluation` runs.
    progress_tick: Option<Duration>,
}

/// Add each hold in `holds` to `map[stage]` (a multiset).
fn add_holds(map: &mut HashMap<String, Vec<Timestamp>>, stage: &str, holds: &[Timestamp]) {
    if holds.is_empty() {
        return;
    }
    map.entry(stage.to_string())
        .or_default()
        .extend_from_slice(holds);
}

/// Remove one occurrence of each hold in `holds` from `map[stage]`.
fn remove_holds(map: &mut HashMap<String, Vec<Timestamp>>, stage: &str, holds: &[Timestamp]) {
    let Some(list) = map.get_mut(stage) else {
        return;
    };
    for hold in holds {
        if let Some(pos) = list.iter().position(|candidate| candidate == hold) {
            list.remove(pos);
        }
    }
    if list.is_empty() {
        map.remove(stage);
    }
}

fn control_output_elements(response: &ControlResponse) -> Option<u64> {
    match response {
        ControlResponse::ProcessBundleSuccess(response) => {
            Some(sum_element_count(&response.monitoring_infos))
        }
        _ => None,
    }
}

fn enqueue_sdf_items(
    scheduler: &mut NodeScheduler,
    sdf_queues: &mut HashMap<String, SdfQueue>,
    residual_holds: &mut HashMap<String, Vec<Timestamp>>,
    stage_id: &str,
    items: impl IntoIterator<Item = SdfWorkItem>,
) -> anyhow::Result<usize> {
    let queue = sdf_queues.entry(stage_id.to_string()).or_default();
    let mut count = 0usize;
    for item in items {
        add_holds(residual_holds, stage_id, &item.holds);
        queue.items.push_back(item);
        count += 1;
    }
    if count > 0 {
        // Split events can arrive before the parent's final outcome. The parent
        // had to initialize before the SDK could produce a residual.
        queue.initialized = true;
        scheduler
            .watermarks_mut()
            .set_residual_holds(residual_holds);
        scheduler
            .watermarks_mut()
            .enqueue_bundles(stage_id, count)?;
    }
    Ok(count)
}

/// Owns the harness channels and per-job state needed to prepare a pipeline for
/// execution. A [`StageExecutor`] is built from this prepared state.
pub struct ExecutorDispatcher {
    channels: Channels,
    store: Arc<FlareElementStore>,
    pipeline_coders: Arc<HashMap<String, Coder>>,
    pipeline_components: Arc<Components>,
    pool: Option<Arc<WorkerPool>>,
    registered_on: Arc<std::sync::Mutex<HashSet<(String, String)>>>,
    timer_service: Arc<TimerService>,
    /// Runner-source progress reports (TestStream) sent from bundle execution.
    source_reports_tx: mpsc::UnboundedSender<SourceProgress>,
    source_reports_rx: mpsc::UnboundedReceiver<SourceProgress>,
}

impl ExecutorDispatcher {
    pub async fn new(channels: Channels) -> anyhow::Result<Self> {
        let store_path = crate::utils::path::warehouse_dir();
        let store_base = store_path.to_str().unwrap_or(".").to_string();
        let store =
            Arc::new(FlareElementStore::new(store_base, "pcollection".to_string(), None).await?);
        let timer_service = Arc::new(TimerService::new(build_timer_store(store.clone()).await?));
        let (source_reports_tx, source_reports_rx) = mpsc::unbounded_channel();
        Ok(Self {
            channels,
            store,
            pipeline_coders: Arc::new(HashMap::new()),
            pipeline_components: Arc::new(Components::default()),
            pool: None,
            registered_on: Arc::new(std::sync::Mutex::new(HashSet::new())),
            timer_service,
            source_reports_tx,
            source_reports_rx,
        })
    }

    /// Reset all channels so a new worker can connect.
    pub async fn reset_channels(&mut self) {
        self.channels.reset().await;
        self.pool = None;
        self.registered_on
            .lock()
            .expect("registration lock not poisoned")
            .clear();
    }

    /// Route SDK worker log entries for a job into that job's per-job
    /// `flare-worker.log`.
    pub async fn set_log_target(&self, instance_id: &str, job_id: &str) -> anyhow::Result<()> {
        self.channels.set_log_target(instance_id, job_id).await
    }

    /// Install the metrics accumulator for the job that is about to run.
    pub(crate) fn set_job_metrics(&self, metrics: Arc<JobMetrics>) {
        self.channels.set_job_metrics(metrics);
    }

    /// Point the element store (and timer store) at the job's warehouse database.
    pub async fn set_job_store(&mut self, job_id: &str) -> anyhow::Result<()> {
        let store_path = crate::utils::path::warehouse_dir();
        let store_base = store_path.to_str().unwrap_or(".").to_string();
        let store = Arc::new(FlareElementStore::new(store_base, job_id.to_string(), None).await?);
        self.timer_service = Arc::new(TimerService::new(build_timer_store(store.clone()).await?));
        self.store = store;
        Ok(())
    }

    /// Job-scoped health signal shared with the Fn API stream handlers.
    pub fn harness_status(&self) -> HarnessStatus {
        self.channels.status()
    }

    pub fn worker_channels(&self) -> Channels {
        self.channels.clone()
    }

    pub fn set_worker_pool(&mut self, pool: Arc<WorkerPool>) {
        self.pool = Some(pool);
    }

    pub fn bundle_capacity(&self) -> usize {
        self.pool
            .as_ref()
            .map(|pool| pool.config().max_bundle_slots())
            .unwrap_or(1)
    }

    /// Start dispatcher tasks and cache the pipeline coders/components used to
    /// build a [`StageExecutor`].
    pub async fn prepare_pipeline(
        &mut self,
        pipeline_graph: &ExecutableGraph,
    ) -> anyhow::Result<()> {
        // Resolve `pickled_python` leaves through length-prefixed wrappers so the
        // runner's decoder sees the same coder graph the SDK is asked to emit.
        let mut coders = pipeline_graph.components.coders.clone();
        crate::coders::length_prefix_pickled_leaves(&mut coders);
        let side_inputs = SideInputRegistry::from_executable_graph(pipeline_graph, &coders)?;

        self.channels
            .prepare_state(self.store.clone(), side_inputs)
            .await?;
        self.pipeline_coders = Arc::new(coders);
        self.pipeline_components = Arc::new(pipeline_graph.components.clone());
        Ok(())
    }

    /// Run the executable graph to completion.
    ///
    /// The loop is `refresh -> ready` (via [`NodeScheduler::next_nodes`]) then run.
    /// When no data work is ready it applies any runner-source progress
    /// (`TestStream` watermarks / re-arms), fires due **event-time** timers, and
    /// fires **processing-time** timers already due. If any of those made progress
    /// it re-evaluates scheduling — a watermark advance can make a gated stage
    /// ready (e.g. GBK's windows expiring at `+∞`) and a re-arm makes a source
    /// runnable — before concluding completion or deadlock.
    ///
    /// A **splittable (SDF)** stage produces residual work items one bundle at a
    /// time and may run several concurrently (up to its concurrency ceiling). The
    /// loop queues those items, runs one bundle per item, and holds the stage's
    /// output watermark at the minimum over all outstanding items' reported
    /// `output_watermarks`, so consumers do not treat earlier event-time work as
    /// complete until the deferred work drains.
    pub async fn run_pipeline(&mut self, scheduler: &mut NodeScheduler) -> anyhow::Result<()> {
        let mut in_flight = JoinSet::new();
        let sdf_overdecompose = crate::engine::sdf::sdf_overdecompose_from_env();
        let split_manager = SplitManager::from_env(self.pool.clone());
        // Timers to deliver the next time each re-armed stage runs.
        let mut pending_timers: HashMap<String, Vec<TimerEntry>> = HashMap::new();
        let mut stage_telemetry: HashMap<String, StageTelemetry> = HashMap::new();
        let mut summarized_stages = HashSet::new();
        let mut first_idle_queue_empty_at: Option<Instant> = None;
        // Pending residual work items per splittable stage, and the union of the
        // output-watermark holds their deferred work imposes. Both per-run.
        let mut sdf_queues: HashMap<String, SdfQueue> = HashMap::new();
        let mut residual_holds: HashMap<String, Vec<Timestamp>> = HashMap::new();
        // Split residuals are announced while their parent bundle is still
        // running so the scheduler can fill the stage's remaining bundle slots.
        let (immediate_sdf_tx, mut immediate_sdf_rx) =
            mpsc::unbounded_channel::<(String, SdfWorkItem)>();
        // Drop any stale reports left by a previous job over the same dispatcher.
        while self.source_reports_rx.try_recv().is_ok() {}

        // Long bundles can park the scheduling loop in `select!` for minutes.
        // Refresh the pool's view of outstanding work on a timer so telemetry
        // does not keep showing the snapshot taken before those bundles
        // registered (which can look like a completed job).
        let mut observation_tick = tokio::time::interval(Duration::from_secs(1));
        observation_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        observation_tick.tick().await;

        loop {
            // Reconcile output-watermark holds with the durable event-time timers
            // before scheduling: a pending event-time timer holds its owning
            // stage's output watermark, so a downstream consumer does not see the
            // stage as complete before the timer fires. Holds clamp output only,
            // so this cannot deadlock a stage's own readiness.
            self.sync_event_time_holds(scheduler).await?;
            // Deferred-work (SDF residual) holds live in a separate multiset, so a
            // timer reconcile never erases them; apply them alongside the timers.
            scheduler
                .watermarks_mut()
                .set_residual_holds(&residual_holds);

            if let Some(pool) = &self.pool {
                let snapshot = pool.snapshot();
                split_manager.set_pool_state(
                    snapshot.free_bundle_slots,
                    snapshot.max_bundle_slots,
                    snapshot
                        .spawn_cost_ms
                        .and_then(|millis| u64::try_from(millis).ok())
                        .map(Duration::from_millis),
                );
            }
            let nodes = scheduler.next_nodes();
            if let Some(pool) = &self.pool {
                pool.observe_workload(scheduler.outstanding_bundle_count());
                pool.observe_dispatcher_state(
                    scheduler.queued_bundle_count(),
                    scheduler.in_flight_bundle_count(),
                    split_manager.total_splits(),
                );
                pool.observe_remaining_work_seconds(
                    split_manager.estimated_remaining_work_seconds(),
                );
                let snapshot = pool.snapshot();
                if scheduler.queued_bundle_count() == 0 && snapshot.idle > 0 {
                    first_idle_queue_empty_at.get_or_insert_with(Instant::now);
                }
            }
            for (idx, node) in nodes {
                let input_metadata = scheduler.input_edge_metadata(idx);
                let output_metadata = scheduler.output_edge_metadata(idx);
                let stage_id = node.id();
                stage_telemetry
                    .entry(stage_id.clone())
                    .or_default()
                    .started_at
                    .get_or_insert_with(Instant::now);
                let timers = pending_timers.remove(&stage_id).unwrap_or_default();
                // The stage's input watermark at the start of this bundle, used by
                // windowed aggregation to decide which windows are ready.
                let input_watermark = scheduler
                    .watermarks()
                    .bundle_input_watermark(&stage_id)
                    .unwrap_or(MIN_TIMESTAMP);
                scheduler
                    .watermarks()
                    .validate_bundle_input_watermark(&stage_id, input_watermark)?;

                let (sdf_seed, sdf_initialized, sdf_progress_tick) = match &node {
                    ExecutableNode::Splittable(_stage) => {
                        let queue = sdf_queues.entry(stage_id.clone()).or_default();
                        let initialized = queue.initialized;
                        let seed = if initialized {
                            queue.items.pop_front()
                        } else {
                            None
                        };
                        if let Some(permit_id) = seed.as_ref().and_then(|item| item.permit_id) {
                            split_manager.note_item_picked(permit_id);
                        }
                        split_manager.set_queue_depth(&stage_id, queue.items.len());
                        log::debug!(
                            "SDF stage '{stage_id}': starting bundle, initialized={initialized}, \
                             queue_depth={}, in_flight={}, seed_bytes={}",
                            queue.items.len(),
                            scheduler.watermarks().in_flight_bundles(&stage_id),
                            seed.as_ref().map_or(0, |item| item.seed.len())
                        );
                        (seed, queue.initialized, queue.progress_tick)
                    }
                    _ => (None, false, None),
                };

                let immediate_sdf_tx = immediate_sdf_tx.clone();
                let pool = self
                    .pool
                    .clone()
                    .ok_or_else(|| anyhow!("worker pool is not configured"))?;
                let registered_on = Arc::clone(&self.registered_on);
                let store = self.store.clone();
                let pipeline_coders = self.pipeline_coders.clone();
                let pipeline_components = self.pipeline_components.clone();
                let timer_service = self.timer_service.clone();
                let source_reports = self.source_reports_tx.clone();
                let split_manager = Arc::clone(&split_manager);
                in_flight.spawn(async move {
                    if matches!(&node, ExecutableNode::Runner(_))
                        && let Err(error) = pool.shrink_to(pool.min_workers()).await
                    {
                        return (
                            idx,
                            Err(error),
                            StageRun::Plain,
                            BundleObservation::default(),
                        );
                    }
                    let mut lease = match pool.acquire(WorkerKind::Cpu).await {
                        Ok(lease) => lease,
                        Err(error) => {
                            return (
                                idx,
                                Err(error),
                                StageRun::Plain,
                                BundleObservation::default(),
                            );
                        }
                    };
                    lease.annotate_stage(stage_id.clone());
                    let bundle_started = Instant::now();
                    let worker_id = lease.id().clone();
                    let snapshot = pool.snapshot();
                    split_manager.set_pool_state(
                        snapshot.free_bundle_slots,
                        snapshot.max_bundle_slots,
                        snapshot
                            .spawn_cost_ms
                            .and_then(|millis| u64::try_from(millis).ok())
                            .map(Duration::from_millis),
                    );
                    let worker = lease.worker();
                    let instruction_pool = Arc::clone(&pool);
                    let instruction_worker = worker_id.clone();
                    let instruction_lease = lease.token();
                    let mut runtime = BundleRuntime::new(
                        worker,
                        store,
                        pipeline_coders,
                        pipeline_components,
                        timer_service,
                        source_reports,
                    );
                    runtime.set_instruction_observer(Arc::new(move |instruction_id| {
                        instruction_pool.annotate_instruction(
                            &instruction_worker,
                            instruction_lease,
                            instruction_id,
                        );
                    }));
                    let output_pool = Arc::clone(&pool);
                    runtime.set_output_progress_observer(Arc::new(move |count| {
                        output_pool.record_output_elements(count);
                    }));
                    let mark_registration = |descriptor_id: &str| {
                        registered_on
                            .lock()
                            .expect("registration lock not poisoned")
                            .insert((worker_id.as_str().to_string(), descriptor_id.to_string()))
                    };
                    if let ExecutableNode::Splittable(stage) = node {
                        let split_depth =
                            sdf_seed.as_ref().map(|item| item.split_depth).unwrap_or(0);
                        let split_bundle_id =
                            split_manager.register_bundle(&stage_id, split_depth, Instant::now());
                        if !sdf_initialized {
                            mark_registration(&stage.plan().initialization_stage.id());
                        }
                        let register_process_bundle =
                            mark_registration(&stage.plan().process_stage.id());
                        let mut executor = SplittableStageExecutor::new(runtime);
                        let max_bundle_slots = pool.config().max_bundle_slots();
                        let run = match executor
                            .execute_bundle(
                                &stage,
                                output_metadata,
                                sdf_seed,
                                sdf_initialized,
                                register_process_bundle,
                                sdf_progress_tick,
                                max_bundle_slots,
                                sdf_overdecompose,
                                Arc::clone(&split_manager),
                                split_bundle_id,
                                split_depth,
                                immediate_sdf_tx,
                            )
                            .await
                        {
                            Ok((response, outcome)) => {
                                let output_elements = sum_element_count(&response.monitoring_infos);
                                (
                                    Ok(ControlResponse::ProcessBundleSuccess(response)),
                                    StageRun::Sdf { outcome },
                                    output_elements,
                                )
                            }
                            Err(err) => (Err(err), StageRun::Plain, 0),
                        };
                        split_manager.finish_bundle(split_bundle_id);
                        if run.0.is_ok() {
                            pool.record_bundle_complete(&worker_id);
                        }
                        (
                            idx,
                            run.0,
                            run.1,
                            BundleObservation {
                                duration: bundle_started.elapsed(),
                                output_elements: run.2,
                            },
                        )
                    } else {
                        let ordinary_register = if let ExecutableNode::Worker(stage) = &node {
                            mark_registration(&stage.id())
                        } else {
                            false
                        };
                        let mut executor = StageExecutor::new(runtime);
                        let result = executor
                            .execute_with_timers(
                                node,
                                input_metadata,
                                output_metadata,
                                timers,
                                input_watermark,
                                ordinary_register,
                            )
                            .await;
                        let output_elements = result
                            .as_ref()
                            .ok()
                            .and_then(control_output_elements)
                            .unwrap_or(0);
                        if result.is_ok() {
                            pool.record_bundle_complete(&worker_id);
                        }
                        (
                            idx,
                            result,
                            StageRun::Plain,
                            BundleObservation {
                                duration: bundle_started.elapsed(),
                                output_elements,
                            },
                        )
                    }
                });
            }

            if in_flight.is_empty() {
                // (1) Runner-source progress (TestStream): one event per run. Apply
                // the reported watermark / processing time, and either re-arm the
                // source for its next event or mark it finished.
                let source_progress = self.apply_source_progress(scheduler).await?;

                // (2) Fire event-time timers the (possibly advanced) watermark
                // reached; this also drains timers re-armed from inside `@OnTimer`
                // once the source has reported `+inf`.
                let promoted_et = self
                    .promote_due_event_time_timers(scheduler, &mut pending_timers)
                    .await?;

                // (3) Fire any processing-time timers already due at `now`. With the
                // wall clock this is usually empty and the wait below handles the
                // deadline; with a TestStream's paused clock it is how an
                // `advanceProcessingTime` event fires timers.
                let fired_pt = self
                    .fire_due_processing_time(scheduler, &mut pending_timers)
                    .await?;

                // Any applied source progress can change scheduling: a re-arm makes a
                // source runnable, and a watermark advance can make a gated
                // aggregation ready again (e.g. its window expiring at `+inf`). Re-loop
                // so `next_nodes` re-evaluates readiness before concluding deadlock.
                if source_progress > 0 || promoted_et > 0 || fired_pt > 0 {
                    continue;
                }

                // Nothing is due now. A paused (TestStream) clock never advances on
                // its own, so if the graph is not complete it is deadlocked.
                if self.timer_service.is_manual() {
                    if scheduler.is_complete() {
                        self.warn_stranded_event_time_timers().await;
                        break;
                    }
                    return Err(anyhow!(
                        "executable graph deadlocked: processing time is paused at {} with no due timers",
                        self.timer_service.now()
                    ));
                }

                match self.timer_service.next_processing_deadline().await? {
                    Some(deadline) => {
                        let now = self.timer_service.now();
                        if now < deadline {
                            tokio::time::sleep(Duration::from_millis((deadline - now) as u64))
                                .await;
                        }
                        if self
                            .fire_due_processing_time(scheduler, &mut pending_timers)
                            .await?
                            == 0
                        {
                            if scheduler.is_complete() {
                                break;
                            }
                            return Err(anyhow!(
                                "executable graph deadlocked: timer deadline passed but nothing was due"
                            ));
                        }
                        continue;
                    }
                    None => {
                        if scheduler.is_complete() {
                            self.warn_stranded_event_time_timers().await;
                            break;
                        }
                        return Err(anyhow!(
                            "executable graph deadlocked: no ready nodes and no in-flight work"
                        ));
                    }
                }
            }

            let joined = tokio::select! {
                biased;
                Some((stage_id, item)) = immediate_sdf_rx.recv() => {
                    let count = enqueue_sdf_items(
                        scheduler,
                        &mut sdf_queues,
                        &mut residual_holds,
                        &stage_id,
                        [item],
                    )?;
                    let queue = sdf_queues
                        .get(&stage_id)
                        .expect("enqueued SDF stage must have a queue");
                    log::debug!(
                        "SDF stage '{stage_id}': queued {count} split work item immediately, \
                         queue_depth={}, in_flight={}, holds={:?}",
                        queue.items.len(),
                        scheduler.watermarks().in_flight_bundles(&stage_id),
                        residual_holds.get(&stage_id)
                    );
                    continue;
                }
                _ = observation_tick.tick() => {
                    if let Some(pool) = &self.pool {
                        // Queue depths change when residuals/splits are queued
                        // or a bundle starts, not only when a bundle finishes.
                        for (stage, queue) in &sdf_queues {
                            split_manager.set_queue_depth(stage, queue.items.len());
                        }
                        pool.observe_workload(scheduler.outstanding_bundle_count());
                        pool.observe_dispatcher_state(
                            scheduler.queued_bundle_count(),
                            scheduler.in_flight_bundle_count(),
                            split_manager.total_splits(),
                        );
                        pool.observe_remaining_work_seconds(
                            split_manager.estimated_remaining_work_seconds(),
                        );
                    }
                    continue;
                }
                joined = in_flight.join_next() => joined
            };

            if let Some(joined) = joined {
                let (idx, result, run, observation) = joined?;
                let stage_id = scheduler.stage_id(idx);
                {
                    let telemetry = stage_telemetry.entry(stage_id.clone()).or_default();
                    telemetry.bundles += 1;
                    telemetry.output_elements = telemetry
                        .output_elements
                        .saturating_add(observation.output_elements);
                    telemetry
                        .bundle_durations_ms
                        .push(observation.duration.as_millis().min(u128::from(u64::MAX)) as u64);
                }
                result?;

                // Release the holds of the item this bundle consumed, publish the
                // holds of the residuals it produced, queue those residuals, and
                // declare them to the scheduler so the stage is not considered
                // complete while any remain.
                //
                // This must happen BEFORE `mark_complete_with_min` refreshes: a hold
                // can only clamp a watermark that has not advanced yet, and the
                // refresh inside completion would otherwise let the producer's output
                // run ahead of the deferred work.
                if let StageRun::Sdf { outcome } = run {
                    remove_holds(&mut residual_holds, &stage_id, &outcome.consumed_holds);
                    let queue = sdf_queues.entry(stage_id.clone()).or_default();
                    queue.initialized = outcome.initialized;
                    queue.progress_tick = Some(outcome.progress_tick);
                    let count = enqueue_sdf_items(
                        scheduler,
                        &mut sdf_queues,
                        &mut residual_holds,
                        &stage_id,
                        outcome.items,
                    )?;
                    let queue = sdf_queues
                        .get(&stage_id)
                        .expect("SDF completion stage must have a queue");
                    log::debug!(
                        "SDF stage '{stage_id}': queued {count} work item(s), queue_depth={}, \
                         in_flight={}, holds={:?}",
                        queue.items.len(),
                        // This bundle has not been marked complete yet.
                        scheduler
                            .watermarks()
                            .in_flight_bundles(&stage_id)
                            .saturating_sub(1),
                        residual_holds.get(&stage_id)
                    );
                    // Any split events sent just before the parent response are
                    // already queued in this channel. Drain them before the parent
                    // can be marked complete, otherwise an empty in-flight set could
                    // briefly make the stage look finished.
                    while let Ok((event_stage, item)) = immediate_sdf_rx.try_recv() {
                        let count = enqueue_sdf_items(
                            scheduler,
                            &mut sdf_queues,
                            &mut residual_holds,
                            &event_stage,
                            [item],
                        )?;
                        let queue = sdf_queues
                            .get(&event_stage)
                            .expect("drained SDF stage must have a queue");
                        log::debug!(
                            "SDF stage '{event_stage}': queued {count} split work item before \
                             parent completion, queue_depth={}, in_flight={}, holds={:?}",
                            queue.items.len(),
                            scheduler.watermarks().in_flight_bundles(&event_stage),
                            residual_holds.get(&event_stage)
                        );
                    }
                }

                // Take the committed minimum only on the producer's last in-flight
                // bundle (the end of a wave). Concurrent producer bundles fold into
                // one running min, so a per-bundle take could clear another bundle's
                // contribution; taking at wave end yields the min over the whole
                // wave. `in_flight_bundles` still counts this bundle.
                let is_last_of_wave = scheduler.watermarks().in_flight_bundles(&stage_id) <= 1;
                let output_min_ts = if is_last_of_wave {
                    scheduler
                        .output_edge_metadata(idx)
                        .map(|meta| meta.produced_pcol_id)
                        .and_then(|pcollection| self.store.take_commit_min_timestamp(&pcollection))
                } else {
                    None
                };
                scheduler.mark_complete_with_min(idx, output_min_ts);
                if scheduler.watermarks().in_flight_bundles(&stage_id) == 0
                    && scheduler.watermarks().queued_bundles(&stage_id) == 0
                    && summarized_stages.insert(stage_id.clone())
                {
                    let telemetry = stage_telemetry.entry(stage_id.clone()).or_default();
                    let wall = telemetry
                        .started_at
                        .map(|started| started.elapsed().as_secs_f64())
                        .unwrap_or(0.0);
                    let mut durations = telemetry.bundle_durations_ms.clone();
                    durations.sort_unstable();
                    let percentile = |fraction: f64| {
                        if durations.is_empty() {
                            0
                        } else {
                            durations[((durations.len() - 1) as f64 * fraction).round() as usize]
                        }
                    };
                    let busy_seconds =
                        telemetry.bundle_durations_ms.iter().sum::<u64>() as f64 / 1_000.0;
                    let max_workers = self
                        .pool
                        .as_ref()
                        .map(|pool| pool.config().max_workers)
                        .unwrap_or(1)
                        .max(1);
                    let worker_utilization = if wall > 0.0 {
                        (busy_seconds / (wall * max_workers as f64)).clamp(0.0, 1.0)
                    } else {
                        0.0
                    };
                    let tail_s = first_idle_queue_empty_at
                        .map(|started| started.elapsed().as_secs_f64())
                        .unwrap_or(0.0);
                    log::info!(
                        "stage summary: stage={} wall_s={:.3} bundles={} splits={} \
                         worker_utilization={:.2} tail_s={:.3} bundle_ms_min={} \
                         bundle_ms_p50={} bundle_ms_max={} output_elements={}",
                        stage_id,
                        wall,
                        telemetry.bundles,
                        split_manager.stage_splits(&stage_id),
                        worker_utilization,
                        tail_s,
                        percentile(0.0),
                        percentile(0.5),
                        percentile(1.0),
                        telemetry.output_elements
                    );
                }
            }
        }

        Ok(())
    }

    /// Rebuild every stage's watermark holds from the durable event-time
    /// timers, before the scheduler picks the next work to run. Iit helps decide
    /// whether downstream stages are allowed to consider this stage's event-time work complete.
    ///
    /// FlareDB executes the graph stage-by-stage in the scheduling
    /// loop (`run_pipeline`). As a stage's bundle executes, the SDK may persist
    /// event-time timers — set, clear, or re-arm — so the set of pending timers
    /// changes as the run progresses.
    ///
    /// A pending event-time timer represents future work (e.g. a window's closing
    /// pane). Until it fires, the owning stage's **output watermark** is held at
    /// the timer's `hold_timestamp`. Downstream stages use watermarks to determine
    /// when earlier event-time work is complete (which windows are ready and which
    /// timers are due), without the hold, downstream stages could advance past
    /// this stage's pending work and finalize too early.
    ///
    /// The durable timer store is the source of truth, so the holds map is rebuilt
    /// from it on each scheduling iteration. This ensures timers set, cleared, or
    /// re-armed during a bundle are reflected before the next scheduling decision.
    /// Holds clamp output only and never block a stage's own readiness. Timers whose
    /// transform maps to no stage are ignored.
    ///
    async fn sync_event_time_holds(&self, scheduler: &mut NodeScheduler) -> anyhow::Result<()> {
        let mut watermark_holds: HashMap<String, Vec<Timestamp>> = HashMap::new();
        for timer in self.timer_service.all_event_time_timers().await? {
            if let Some(stage) = scheduler.stage_for_transform(&timer.key.transform_id) {
                watermark_holds
                    .entry(stage)
                    .or_default()
                    .push(timer.hold_timestamp);
            }
        }
        scheduler
            .watermarks_mut()
            .set_event_time_holds(&watermark_holds);
        Ok(())
    }

    /// Promote every persisted event-time timer whose owning stage's input
    /// watermark has reached it, deleting each from the store before delivering
    /// it (at-most-once) and queueing it for that stage's next bundle.
    ///
    /// Returns the number of timers promoted.
    async fn promote_due_event_time_timers(
        &self,
        scheduler: &mut NodeScheduler,
        pending_timers: &mut HashMap<String, Vec<TimerEntry>>,
    ) -> anyhow::Result<usize> {
        let event_timers = self.timer_service.all_event_time_timers().await?;
        if event_timers.is_empty() {
            return Ok(0);
        }

        let mut promoted = 0usize;
        for (stage, timers) in scheduler.promote_due_event_time_timers(&event_timers) {
            self.timer_service.delete_all(&timers).await?;
            log::info!(
                "firing {} due event-time timer(s) for stage '{}'",
                timers.len(),
                stage
            );
            promoted += timers.len();
            pending_timers.entry(stage).or_default().extend(timers);
        }
        Ok(promoted)
    }

    /// Apply runner-source progress reports (TestStream).
    ///
    /// For each report: advance the source's watermark and/or the processing-time
    /// clock, then either mark the source finished (its last event) or re-arm it so
    /// it runs again for the next scripted event. Returns the number of reports
    /// applied, so the caller re-evaluates scheduling after the watermark moved (a
    /// gated aggregation may have become ready).
    async fn apply_source_progress(
        &mut self,
        scheduler: &mut NodeScheduler,
    ) -> anyhow::Result<usize> {
        let mut applied = 0usize;
        while let Ok(progress) = self.source_reports_rx.try_recv() {
            applied += 1;
            if let Some(watermark) = progress.watermark {
                scheduler
                    .watermarks_mut()
                    .report_source_watermark(&progress.stage_id, watermark)?;
                log::info!(
                    "source '{}' advanced watermark to {}",
                    progress.stage_id,
                    format_timestamp(watermark)
                );
            }
            if let Some(processing_time) = progress.processing_time {
                self.timer_service.set_processing_time(processing_time);
                log::info!(
                    "source '{}' advanced processing time to {}",
                    progress.stage_id,
                    processing_time
                );
            }
            if progress.done {
                scheduler
                    .watermarks_mut()
                    .report_source_finished(&progress.stage_id)?;
                log::info!("source '{}' finished", progress.stage_id);
            } else {
                scheduler.watermarks_mut().mark_rerun(&progress.stage_id)?;
            }
        }
        if applied > 0 {
            scheduler.watermarks_mut().refresh();
        }
        Ok(applied)
    }

    /// Fire every processing-time timer already due at [`TimerService::now`].
    ///
    /// Returns the number of timers promoted. Used both for a paused TestStream
    /// clock and to fire timers due immediately at the wall-clock time.
    async fn fire_due_processing_time(
        &self,
        scheduler: &mut NodeScheduler,
        pending_timers: &mut HashMap<String, Vec<TimerEntry>>,
    ) -> anyhow::Result<usize> {
        let due = self
            .timer_service
            .take_due_processing_time(self.timer_service.now())
            .await?;
        if due.is_empty() {
            return Ok(0);
        }
        log::info!(
            "firing {} due processing-time timer(s): [{}]",
            due.len(),
            summarize_due_timers(&due)
        );
        for (stage, timers) in scheduler.promote_due_timers(&due) {
            pending_timers.entry(stage).or_default().extend(timers);
        }
        Ok(due.len())
    }

    /// Warn about event-time timers that will never fire because no watermark
    /// reached them before the run ended.
    async fn warn_stranded_event_time_timers(&self) {
        match self.timer_service.all_event_time_timers().await {
            Ok(stranded) if !stranded.is_empty() => log::warn!(
                "finishing with {} event-time timer(s) never reached by a watermark; they will not fire",
                stranded.len()
            ),
            _ => {}
        }
    }
}

/// Render due timers for logging, capping a large batch so one line stays
/// readable: up to five `transform:family@fire_ms` entries, then `+N more`.
fn summarize_due_timers(due: &[TimerEntry]) -> String {
    const SHOWN: usize = 5;
    let shown = due
        .iter()
        .take(SHOWN)
        .map(|timer| {
            format!(
                "{}:{}@{}ms",
                timer.key.transform_id, timer.key.timer_family_id, timer.fire_timestamp
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    if due.len() > SHOWN {
        format!("{shown}, +{} more", due.len() - SHOWN)
    } else {
        shown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_are_a_multiset_union_across_bundles() {
        let mut holds: HashMap<String, Vec<Timestamp>> = HashMap::new();
        add_holds(&mut holds, "s", &[50, 200]);
        add_holds(&mut holds, "s", &[50]);
        // The same timestamp can be held more than once (one per bundle).
        assert_eq!(holds["s"], vec![50, 200, 50]);

        // Releasing a bundle removes one occurrence of each of its holds.
        remove_holds(&mut holds, "s", &[50]);
        assert_eq!(holds["s"], vec![200, 50]);

        remove_holds(&mut holds, "s", &[50, 200]);
        assert!(!holds.contains_key("s"), "all holds released");
    }

    #[test]
    fn releasing_absent_holds_is_a_no_op() {
        let mut holds: HashMap<String, Vec<Timestamp>> = HashMap::new();
        remove_holds(&mut holds, "missing", &[1]);
        add_holds(&mut holds, "s", &[]);
        assert!(holds.is_empty());
    }
}
