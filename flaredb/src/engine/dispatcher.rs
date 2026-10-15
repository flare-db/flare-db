use crate::engine::sdf::{SdfBundleOutcome, SdfWorkItem, SplittableStageExecutor};
use crate::transforms::SourceProgress;
use crate::{
    engine::timer::{TimerEntry, build_timer_store},
    engine::{
        executor::StageExecutor,
        harness::{Channels, control::ControlResponse},
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
use std::{collections::HashMap, collections::VecDeque, sync::Arc, time::Duration};
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

/// Pending bundle work for one splittable stage.
#[derive(Default)]
struct SdfQueue {
    /// The initialization bundle has run (carried forward across bundles).
    initialized: bool,
    /// The descriptors are registered with the worker.
    registered: bool,
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

/// Owns the harness channels and per-job state needed to prepare a pipeline for
/// execution. A [`StageExecutor`] is built from this prepared state.
pub struct ExecutorDispatcher {
    channels: Channels,
    store: Arc<FlareElementStore>,
    pipeline_coders: Arc<HashMap<String, Coder>>,
    pipeline_components: Arc<Components>,
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
            timer_service,
            source_reports_tx,
            source_reports_rx,
        })
    }

    /// Reset all channels so a new worker can connect.
    pub async fn reset_channels(&self) {
        self.channels.reset().await;
    }

    /// Route SDK worker log entries for a job into that job's per-job
    /// `flare-worker.log`.
    pub async fn set_log_target(&self, instance_id: &str, job_id: &str) -> anyhow::Result<()> {
        self.channels.set_log_target(instance_id, job_id).await
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

    /// Wait for the worker to connect its control stream.
    pub async fn wait_connected(&self) -> anyhow::Result<()> {
        self.channels.wait_connected().await
    }

    /// Start dispatcher tasks and cache the pipeline coders/components used to
    /// build a [`StageExecutor`].
    pub fn prepare_pipeline(&mut self, pipeline_graph: &ExecutableGraph) {
        // Start data channel dispatcher to listen and demux incoming elements.
        self.channels.stream_elements();
        // Start control channel dispatcher to route responses to waiting futures.
        self.channels.stream_responses();
        // Start state channel dispatcher to service Fn State requests against the
        // job's element store (bag user state).
        self.channels.state().stream_requests(self.store.clone());
        // Resolve `pickled_python` leaves through length-prefixed wrappers so the
        // runner's decoder sees the same coder graph the SDK is asked to emit.
        let mut coders = pipeline_graph.components.coders.clone();
        crate::coders::length_prefix_pickled_leaves(&mut coders);
        self.pipeline_coders = Arc::new(coders);
        self.pipeline_components = Arc::new(pipeline_graph.components.clone());
    }

    /// Build a [`StageExecutor`] from the currently prepared state.
    pub fn new_executor(&self) -> StageExecutor {
        StageExecutor::new(self.new_bundle_runtime())
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
        // Timers to deliver the next time each re-armed stage runs.
        let mut pending_timers: HashMap<String, Vec<TimerEntry>> = HashMap::new();
        // Pending residual work items per splittable stage, and the union of the
        // output-watermark holds their deferred work imposes. Both per-run.
        let mut sdf_queues: HashMap<String, SdfQueue> = HashMap::new();
        let mut residual_holds: HashMap<String, Vec<Timestamp>> = HashMap::new();
        // Drop any stale reports left by a previous job over the same dispatcher.
        while self.source_reports_rx.try_recv().is_ok() {}

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

            for (idx, node) in scheduler.next_nodes() {
                let runtime = self.new_bundle_runtime();
                let input_metadata = scheduler.input_edge_metadata(idx);
                let output_metadata = scheduler.output_edge_metadata(idx);
                let stage_id = node.id();
                let timers = pending_timers.remove(&stage_id).unwrap_or_default();
                // The stage's input watermark at the start of this bundle, used by
                // windowed aggregation to decide which windows are ready.
                let input_watermark = scheduler
                    .watermarks()
                    .input_watermark(&stage_id)
                    .unwrap_or(MIN_TIMESTAMP);

                // A splittable stage runs one bundle per queued work item. Take
                // the next item here (the initialization bundle has no item), so
                // each started bundle gets distinct seed bytes.
                let (sdf_seed, sdf_initialized, sdf_registered, sdf_progress_tick) = match &node {
                    ExecutableNode::Splittable(_) => {
                        let queue = sdf_queues.entry(stage_id.clone()).or_default();
                        let seed = if queue.initialized {
                            queue.items.pop_front()
                        } else {
                            None
                        };
                        (
                            seed,
                            queue.initialized,
                            queue.registered,
                            queue.progress_tick,
                        )
                    }
                    _ => (None, false, false, None),
                };

                in_flight.spawn(async move {
                    if let ExecutableNode::Splittable(stage) = node {
                        let mut executor = SplittableStageExecutor::new(runtime);
                        let run = match executor
                            .execute_bundle(
                                &stage,
                                output_metadata,
                                sdf_seed,
                                sdf_initialized,
                                sdf_registered,
                                sdf_progress_tick,
                            )
                            .await
                        {
                            Ok((response, outcome)) => (
                                Ok(ControlResponse::ProcessBundleSuccess(response)),
                                StageRun::Sdf { outcome },
                            ),
                            Err(err) => (Err(err), StageRun::Plain),
                        };
                        (idx, run.0, run.1)
                    } else {
                        let mut executor = StageExecutor::new(runtime);
                        let result = executor
                            .execute_with_timers(
                                node,
                                input_metadata,
                                output_metadata,
                                timers,
                                input_watermark,
                            )
                            .await;
                        (idx, result, StageRun::Plain)
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

            if let Some(joined) = in_flight.join_next().await {
                let (idx, result, run) = joined?;
                result?;
                let stage_id = scheduler.stage_id(idx);

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
                    queue.registered = outcome.registered;
                    queue.progress_tick = Some(outcome.progress_tick);
                    for item in &outcome.items {
                        add_holds(&mut residual_holds, &stage_id, &item.holds);
                    }
                    let count = outcome.items.len();
                    queue.items.extend(outcome.items);
                    scheduler
                        .watermarks_mut()
                        .set_residual_holds(&residual_holds);
                    if count > 0 {
                        scheduler
                            .watermarks_mut()
                            .enqueue_bundles(&stage_id, count)?;
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

    fn new_bundle_runtime(&self) -> BundleRuntime {
        BundleRuntime::new(
            self.channels.control(),
            self.channels.data(),
            self.store.clone(),
            self.pipeline_coders.clone(),
            self.pipeline_components.clone(),
            self.timer_service.clone(),
            self.source_reports_tx.clone(),
        )
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
