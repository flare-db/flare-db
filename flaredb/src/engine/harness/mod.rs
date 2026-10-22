use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio::sync::{Mutex, watch};
use tonic::Status;

use crate::engine::harness::{
    control::{ControlChannel, FlareControlService},
    data::{DataChannel, ElementStreamMultiplexer, FlareDataService},
    log::{FlareLogService, LogChannel},
    state::{FlareStateService, StateChannel, StateServiceContext},
};
use crate::engine::metrics::JobMetrics;
use crate::store::element_store::FlareElementStore;
use crate::worker::manager::{WorkerId, WorkerLogRouter};
use ::log::info;

pub mod control;
pub mod data;
pub mod log;
pub mod state;

pub type Channels = WorkerChannelRegistry;

/// Job-scoped health of the SDK harness transport streams.
///
/// The first unexpected data/control/state stream failure is retained here so a
/// waiting pipeline can fail immediately. Intentional worker shutdown flips the
/// flag first, allowing the resulting BrokenPipe/h2 stream closures to stay at
/// debug level instead of being reported as worker deaths.
#[derive(Clone)]
pub struct HarnessStatus {
    inner: Arc<HarnessStatusInner>,
}

struct HarnessStatusInner {
    failure: StdMutex<Option<String>>,
    failure_notify: Notify,
    intentional_shutdown: AtomicBool,
    intentional_workers: StdMutex<HashSet<String>>,
}

impl Default for HarnessStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl HarnessStatus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HarnessStatusInner {
                failure: StdMutex::new(None),
                failure_notify: Notify::new(),
                intentional_shutdown: AtomicBool::new(false),
                intentional_workers: StdMutex::new(HashSet::new()),
            }),
        }
    }

    /// Clear state from the previous job before a new harness connects.
    pub fn begin_job(&self) {
        self.inner
            .intentional_shutdown
            .store(false, Ordering::SeqCst);
        *self
            .inner
            .failure
            .lock()
            .expect("harness failure lock not poisoned") = None;
        self.inner
            .intentional_workers
            .lock()
            .expect("intentional workers lock not poisoned")
            .clear();
    }

    /// Mark the current stream closures as expected shutdown noise.
    pub fn begin_intentional_shutdown(&self) {
        self.inner
            .intentional_shutdown
            .store(true, Ordering::SeqCst);
    }

    pub fn is_intentional_shutdown(&self) -> bool {
        self.inner.intentional_shutdown.load(Ordering::SeqCst)
    }

    pub fn begin_worker_shutdown(&self, worker_id: &str) {
        self.inner
            .intentional_workers
            .lock()
            .expect("intentional workers lock not poisoned")
            .insert(worker_id.to_string());
    }

    pub fn is_worker_intentional_shutdown(&self, worker_id: &str) -> bool {
        self.is_intentional_shutdown()
            || self
                .inner
                .intentional_workers
                .lock()
                .expect("intentional workers lock not poisoned")
                .contains(worker_id)
    }

    /// Record the first unexpected stream failure and wake the job.
    ///
    /// Returns the failure message when this call recorded it. Failures observed
    /// after intentional shutdown, or after another stream already failed, return
    /// `None`.
    pub fn report_failure(&self, stream: &str, error: impl std::fmt::Display) -> Option<String> {
        if self.is_intentional_shutdown() {
            return None;
        }
        let message = format!("{stream} failed before the job completed: {error}");
        let mut failure = self
            .inner
            .failure
            .lock()
            .expect("harness failure lock not poisoned");
        if failure.is_some() {
            return None;
        }
        *failure = Some(message.clone());
        drop(failure);
        self.inner.failure_notify.notify_waiters();
        Some(message)
    }

    pub fn report_worker_failure(
        &self,
        worker_id: &str,
        stream: &str,
        error: impl std::fmt::Display,
    ) -> Option<String> {
        self.report_failure(&format!("{stream} for worker {worker_id}"), error)
    }

    pub fn failure(&self) -> Option<String> {
        self.inner
            .failure
            .lock()
            .expect("harness failure lock not poisoned")
            .clone()
    }

    /// Resolve when a fatal stream failure is reported.
    pub async fn wait_for_failure(&self) -> String {
        loop {
            let notified = self.inner.failure_notify.notified();
            if let Some(failure) = self.failure() {
                return failure;
            }
            notified.await;
        }
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    id: WorkerId,
    control: ControlChannel,
    data: DataChannel,
}

impl WorkerHandle {
    pub fn id(&self) -> &WorkerId {
        &self.id
    }

    pub fn control(&self) -> ControlChannel {
        self.control.clone()
    }

    pub fn data(&self) -> DataChannel {
        self.data.clone()
    }
}

#[derive(Clone)]
pub(crate) struct WorkerChannels {
    handle: WorkerHandle,
    state: StateChannel,
    log: LogChannel,
}

#[derive(Clone)]
pub struct WorkerChannelRegistry {
    inner: Arc<WorkerChannelRegistryInner>,
}

struct WorkerChannelRegistryInner {
    status: HarnessStatus,
    workers: DashMap<String, WorkerChannels>,
    data_multiplexer: Arc<ElementStreamMultiplexer>,
    log_router: WorkerLogRouter,
    metrics: StdMutex<Option<Arc<JobMetrics>>>,
    log_target: Mutex<Option<PathBuf>>,
    state_context: watch::Sender<Option<StateServiceContext>>,
}

impl WorkerChannelRegistry {
    pub fn builder(log_router: WorkerLogRouter) -> ChannelsBuilder {
        ChannelsBuilder { log_router }
    }

    pub fn new(status: HarnessStatus, log_router: WorkerLogRouter) -> Self {
        let (state_context, _) = watch::channel(None);
        Self {
            inner: Arc::new(WorkerChannelRegistryInner {
                status,
                workers: DashMap::new(),
                data_multiplexer: Arc::new(ElementStreamMultiplexer::new()),
                log_router,
                metrics: StdMutex::new(None),
                log_target: Mutex::new(None),
                state_context,
            }),
        }
    }

    pub fn status(&self) -> HarnessStatus {
        self.inner.status.clone()
    }

    pub(crate) fn worker(&self, worker_id: &WorkerId) -> WorkerChannels {
        if let Some(worker) = self.inner.workers.get(worker_id.as_str()) {
            return worker.clone();
        }
        let control =
            ControlChannel::new(self.inner.status.clone(), worker_id.as_str().to_string());
        if let Some(metrics) = self
            .inner
            .metrics
            .lock()
            .expect("worker registry metrics lock not poisoned")
            .clone()
        {
            control.set_job_metrics(metrics);
        }
        let candidate = WorkerChannels {
            handle: WorkerHandle {
                id: worker_id.clone(),
                control,
                data: DataChannel::new(
                    self.inner.status.clone(),
                    Arc::clone(&self.inner.data_multiplexer),
                    worker_id.as_str().to_string(),
                ),
            },
            state: StateChannel::new(self.inner.status.clone(), worker_id.as_str().to_string()),
            log: LogChannel::new(worker_id.clone(), self.inner.log_router.clone()),
        };
        match self.inner.workers.entry(worker_id.as_str().to_string()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(candidate.clone());
                candidate
            }
        }
    }

    pub fn handle(&self, worker_id: &WorkerId) -> WorkerHandle {
        self.worker(worker_id).handle
    }

    pub async fn wait_connected(&self, worker_id: &WorkerId, timeout: Duration) -> Result<()> {
        let worker = self.worker(worker_id);
        tokio::time::timeout(timeout, worker.handle.control.wait_connected())
            .await
            .map_err(|_| anyhow::anyhow!("worker {worker_id} control stream did not connect"))??;
        worker.handle.control.stream_responses();
        Ok(())
    }

    pub async fn set_log_target(&self, instance_id: &str, job_id: &str) -> Result<()> {
        let path = crate::utils::path::logs_dir(instance_id, job_id).join("flare-worker.log");
        *self.inner.log_target.lock().await = Some(path);
        let logs: Vec<LogChannel> = self
            .inner
            .workers
            .iter()
            .map(|entry| entry.log.clone())
            .collect();
        for log in logs {
            log.set_target(instance_id, job_id).await?;
        }
        Ok(())
    }

    pub async fn worker_log_path(&self) -> Option<PathBuf> {
        self.inner.log_target.lock().await.clone()
    }

    pub(crate) fn set_job_metrics(&self, metrics: Arc<JobMetrics>) {
        *self
            .inner
            .metrics
            .lock()
            .expect("worker registry metrics lock not poisoned") = Some(metrics.clone());
        for worker in self.inner.workers.iter() {
            worker.handle.control.set_job_metrics(metrics.clone());
        }
    }

    pub async fn prepare_state(
        &self,
        store: Arc<FlareElementStore>,
        side_inputs: crate::engine::harness::state::SideInputRegistry,
    ) -> Result<()> {
        let context = StateServiceContext::build(store, side_inputs).await?;
        let _ = self.inner.state_context.send(Some(context));
        Ok(())
    }

    pub fn begin_state_driver(&self, worker_id: WorkerId) {
        let registry = self.clone();
        tokio::spawn(async move {
            let worker = registry.worker(&worker_id);
            let mut context_rx = registry.inner.state_context.subscribe();
            let context = loop {
                if let Some(context) = context_rx.borrow().clone() {
                    break context;
                }
                if context_rx.changed().await.is_err() {
                    return;
                }
            };
            worker.state.stream_requests_with_context(context);
        });
    }

    pub async fn reset(&self) {
        self.inner.status.begin_job();
        let workers: Vec<WorkerChannels> = self
            .inner
            .workers
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        for worker in workers {
            worker.handle.control.reset().await;
            worker.handle.data.reset().await;
            worker.log.reset().await;
            worker.state.reset().await;
        }
        self.inner.workers.clear();
        self.inner.data_multiplexer.senders().clear();
        self.inner.data_multiplexer.receivers().clear();
        *self.inner.log_target.lock().await = None;
        let _ = self.inner.state_context.send(None);
        *self
            .inner
            .metrics
            .lock()
            .expect("worker registry metrics lock not poisoned") = None;
        info!("worker channel registry reset");
    }

    pub async fn remove_worker(&self, worker_id: &WorkerId) {
        self.inner.status.begin_worker_shutdown(worker_id.as_str());
        if let Some((_, worker)) = self.inner.workers.remove(worker_id.as_str()) {
            worker.handle.control.reset().await;
            worker.handle.data.reset().await;
            worker.log.reset().await;
            worker.state.reset().await;
        }
    }

    pub fn worker_id(
        request_metadata: &tonic::metadata::MetadataMap,
    ) -> std::result::Result<WorkerId, Status> {
        let value = request_metadata
            .get("worker_id")
            .ok_or_else(|| Status::invalid_argument("Fn stream is missing worker_id metadata"))?
            .to_str()
            .map_err(|_| Status::invalid_argument("worker_id metadata is not valid ASCII"))?;
        if value.is_empty() {
            return Err(Status::invalid_argument("worker_id metadata is empty"));
        }
        Ok(WorkerId::from_string(value))
    }
}

/// The gRPC services that correspond to a [`Channels`] bundle.
///
/// Keeping the services paired with the channels makes it harder to
/// accidentally register a service from one harness with channels from
/// another harness.
pub struct ChannelServices {
    pub control: FlareControlService,
    pub data: FlareDataService,
    pub log: FlareLogService,
    pub state: FlareStateService,
}

/// Builder for a complete set of Beam Fn harness channels.
#[derive(Debug, Clone)]
pub struct ChannelsBuilder {
    log_router: WorkerLogRouter,
}

impl ChannelsBuilder {
    pub async fn build(self) -> Result<(WorkerChannelRegistry, ChannelServices)> {
        let status = HarnessStatus::new();
        let registry = WorkerChannelRegistry::new(status, self.log_router);
        Ok((
            registry.clone(),
            ChannelServices {
                control: FlareControlService::new(registry.clone()),
                data: FlareDataService::new(registry.clone()),
                log: FlareLogService::new(registry.clone()),
                state: FlareStateService::new(registry.clone()),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{HarnessStatus, WorkerChannelRegistry};
    use crate::engine::harness::data::DataKey;
    use crate::worker::manager::{WorkerId, WorkerLogRouter};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn reported_stream_failure_wakes_a_waiting_pipeline() {
        let status = HarnessStatus::new();
        let waiter = status.clone();
        let wait = tokio::spawn(async move { waiter.wait_for_failure().await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let message = status
            .report_failure("BeamFnData", "decoded message length too large")
            .expect("first failure is recorded");
        assert!(message.contains("BeamFnData"));
        assert_eq!(wait.await.expect("wait task"), message);
        assert!(
            status
                .report_failure("BeamFnControl", "broken pipe")
                .is_none(),
            "only the first failure should drive job failure"
        );
    }

    #[test]
    fn failures_after_intentional_shutdown_are_not_fatal() {
        let status = HarnessStatus::new();
        status.begin_intentional_shutdown();
        assert!(status.report_failure("BeamFnData", "broken pipe").is_none());
        assert_eq!(status.failure(), None);

        status.begin_job();
        assert!(
            status
                .report_failure("BeamFnData", "real failure")
                .is_some()
        );
    }

    #[test]
    fn worker_registry_routes_data_receivers_through_one_multiplexer() {
        let registry = WorkerChannelRegistry::new(HarnessStatus::new(), WorkerLogRouter::new());
        let handle0 = registry.handle(&WorkerId::from_string("job-w0"));
        let handle1 = registry.handle(&WorkerId::from_string("job-w1"));

        assert_eq!(handle0.id().as_str(), "job-w0");
        assert_eq!(handle1.id().as_str(), "job-w1");
        assert!(!Arc::ptr_eq(
            &handle0.control().stream,
            &handle1.control().stream
        ));

        let key = DataKey {
            instruction_id: "instruction".to_string(),
            transform_id: "sink".to_string(),
        };
        let receiver0 = handle0.data().get_receiver(key.clone());
        let receiver1 = handle1.data().get_receiver(key);
        assert!(
            Arc::ptr_eq(&receiver0, &receiver1),
            "instruction-keyed receivers must be shared across worker channels"
        );
    }

    #[test]
    fn worker_metadata_is_required_and_preserved() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        assert!(WorkerChannelRegistry::worker_id(&metadata).is_err());
        metadata.insert("worker_id", "job-w7".parse().unwrap());
        assert_eq!(
            WorkerChannelRegistry::worker_id(&metadata)
                .unwrap()
                .as_str(),
            "job-w7"
        );
    }
}
