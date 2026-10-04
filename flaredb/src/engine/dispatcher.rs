use crate::engine::sdf::SplittableStageExecutor;
use crate::{
    engine::timer::{TimerEntry, TimerStore},
    engine::{
        executor::{Executor, StageExecutor},
        harness::Channels,
        runtime::BundleRuntime,
        scheduler::NodeScheduler,
        timer::TimerService,
        watermark::{MIN_TIMESTAMP, Timestamp},
    },
    fusion::pipeline::{ExecutableGraph, ExecutableNode},
    store::element_store::FlareElementStore,
};
use anyhow::anyhow;
use beam_model_rs::v1::{Coder, Components};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::task::JoinSet;

/// Owns the harness channels and per-job state needed to prepare a pipeline for
/// execution. A [`StageExecutor`] is built from this prepared state.
pub struct ExecutorDispatcher {
    channels: Channels,
    store: Arc<FlareElementStore>,
    pipeline_coders: Arc<HashMap<String, Coder>>,
    pipeline_components: Arc<Components>,
    timer_service: Arc<TimerService>,
}

impl ExecutorDispatcher {
    pub async fn new(channels: Channels) -> anyhow::Result<Self> {
        let store_path = crate::utils::path::warehouse_dir();
        let store_base = store_path.to_str().unwrap_or(".").to_string();
        let store =
            Arc::new(FlareElementStore::new(store_base, "pcollection".to_string(), None).await?);
        let timer_service = Arc::new(TimerService::new(TimerStore::new(store.clone())));
        Ok(Self {
            channels,
            store,
            pipeline_coders: Arc::new(HashMap::new()),
            pipeline_components: Arc::new(Components::default()),
            timer_service,
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
        self.timer_service = Arc::new(TimerService::new(TimerStore::new(store.clone())));
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
    /// The loop is `refresh -> ready` (via [`NodeScheduler::next_nodes`]) then
    /// run. When no data work is ready it first fires any due **event-time**
    /// timers (whose owning stage's input watermark has advanced past them), then
    /// waits for the next processing-time timer, fires it, and re-arms the owning
    /// stage so its `@OnTimer` runs.
    pub async fn run_pipeline(&self, scheduler: &mut NodeScheduler) -> anyhow::Result<()> {
        let mut in_flight = JoinSet::new();
        // Timers to deliver the next time each re-armed stage runs.
        let mut pending_timers: HashMap<String, Vec<TimerEntry>> = HashMap::new();

        loop {
            // Reconcile output-watermark holds with the durable event-time timers
            // before scheduling: a pending event-time timer holds its owning
            // stage's output watermark, so a downstream consumer does not see the
            // stage as complete before the timer fires. Holds clamp output only,
            // so this cannot deadlock a stage's own readiness.
            self.sync_event_time_holds(scheduler).await?;

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

                in_flight.spawn(async move {
                    let result = if matches!(node, ExecutableNode::Splittable(_)) {
                        let mut executor = SplittableStageExecutor::new(runtime);
                        executor
                            .execute(node, input_metadata, output_metadata, input_watermark)
                            .await
                    } else {
                        let mut executor = StageExecutor::new(runtime);
                        executor
                            .execute_with_timers(
                                node,
                                input_metadata,
                                output_metadata,
                                timers,
                                input_watermark,
                            )
                            .await
                    };
                    (idx, result)
                });
            }

            if in_flight.is_empty() {
                // Watermarks may have advanced since the last round (a bundle
                // completed, or a source reported). Fire any event-time timers
                // whose owning stage's input watermark has now reached them; this
                // also drains timers re-armed from inside `@OnTimer` once the
                // source has reported `+inf`.
                if self
                    .promote_due_event_time_timers(scheduler, &mut pending_timers)
                    .await?
                    > 0
                {
                    continue;
                }

                match self.timer_service.next_processing_deadline().await? {
                    Some(deadline) => {
                        let now = self.timer_service.now();
                        if now < deadline {
                            tokio::time::sleep(Duration::from_millis((deadline - now) as u64))
                                .await;
                        }
                        let due = self
                            .timer_service
                            .take_due_processing_time(self.timer_service.now())
                            .await?;
                        if due.is_empty() {
                            if scheduler.is_complete() {
                                break;
                            }
                            return Err(anyhow!(
                                "executable graph deadlocked: timer deadline passed but nothing was due"
                            ));
                        }
                        log::info!(
                            "firing {} due processing-time timer(s): [{}]",
                            due.len(),
                            summarize_due_timers(&due)
                        );
                        for (stage, timers) in scheduler.promote_due_timers(&due) {
                            pending_timers.entry(stage).or_default().extend(timers);
                        }
                        continue;
                    }
                    None => {
                        if scheduler.is_complete() {
                            let stranded = self.timer_service.all_event_time_timers().await?;
                            if !stranded.is_empty() {
                                log::warn!(
                                    "finishing with {} event-time timer(s) never reached by a watermark; they will not fire",
                                    stranded.len()
                                );
                            }
                            break;
                        }
                        return Err(anyhow!(
                            "executable graph deadlocked: no ready nodes and no in-flight work"
                        ));
                    }
                }
            }

            if let Some(joined) = in_flight.join_next().await {
                let (idx, result) = joined?;
                result?;
                // The minimum event-time this bundle committed to its primary
                // output clamps each consumer's input watermark until consumed.
                let output_min_ts = scheduler
                    .output_edge_metadata(idx)
                    .map(|meta| meta.produced_pcol_id)
                    .and_then(|pcollection| self.store.take_commit_min_timestamp(&pcollection));
                scheduler.mark_complete_with_min(idx, output_min_ts);
            }
        }

        Ok(())
    }

    /// Replace the watermark manager's holds with one hold per pending event-time
    /// timer, at that timer's `hold_timestamp`, on its owning stage.
    ///
    /// The durable timer store is the source of truth, so the whole map is
    /// rebuilt each time (timers set and cleared during bundles both take
    /// effect). Timers whose transform maps to no stage are ignored.
    async fn sync_event_time_holds(&self, scheduler: &mut NodeScheduler) -> anyhow::Result<()> {
        let mut holds: HashMap<String, Vec<Timestamp>> = HashMap::new();
        for timer in self.timer_service.all_event_time_timers().await? {
            if let Some(stage) = scheduler.stage_for_transform(&timer.key.transform_id) {
                holds.entry(stage).or_default().push(timer.hold_timestamp);
            }
        }
        scheduler.watermarks_mut().set_event_time_holds(&holds);
        scheduler.watermarks_mut().refresh();
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
        )
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
