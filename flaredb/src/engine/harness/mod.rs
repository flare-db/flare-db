use anyhow::Result;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

use crate::engine::harness::{
    control::{ControlChannel, FlareControlService, start_control_server},
    data::{DataChannel, FlareDataService, start_data_server},
    log::{FlareLogService, LogChannel, start_log_server},
    state::{FlareStateService, StateChannel, start_state_server},
};
use ::log::info;

pub mod control;
pub mod data;
pub mod log;
pub mod state;

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

pub struct Channels {
    /// Control plane
    control: ControlChannel,
    /// Data plane
    data: DataChannel,
    /// Logging channel
    log: LogChannel,
    /// State API channel.
    state: StateChannel,
    status: HarnessStatus,
}

impl Channels {
    /// Create a builder for the harness channels and their gRPC services.
    pub fn builder() -> ChannelsBuilder {
        ChannelsBuilder
    }

    /// Split the channel bundle into the channels expected by the executor.
    pub fn into_parts(self) -> (ControlChannel, DataChannel, LogChannel, StateChannel) {
        (self.control, self.data, self.log, self.state)
    }

    /// Clone the control channel so the executor can use it.
    pub fn control(&self) -> ControlChannel {
        self.control.clone()
    }

    /// Clone the data channel so the executor can use it.
    pub fn data(&self) -> DataChannel {
        self.data.clone()
    }

    /// Clone the state channel so the executor can service state requests.
    pub fn state(&self) -> StateChannel {
        self.state.clone()
    }

    pub fn status(&self) -> HarnessStatus {
        self.status.clone()
    }

    /// Wait for the worker to connect its control stream.
    pub async fn wait_connected(&self) -> Result<()> {
        self.control.wait_connected().await
    }

    /// Start the data channel dispatcher to demux incoming elements.
    pub fn stream_elements(&self) {
        self.data.stream_elements();
    }

    /// Start the control channel dispatcher to route responses.
    pub fn stream_responses(&self) {
        self.control.stream_responses();
    }

    /// Reset all channels so a new worker can connect.
    pub async fn reset(&self) {
        self.status.begin_job();
        self.control.reset().await;
        self.data.reset().await;
        self.log.reset().await;
        self.state.reset().await;
        //self.store.reset();
        info!("stage executor channels reset");
    }

    /// Route SDK worker log entries for a job into that job's per-job
    /// `flare-worker.log`.
    pub async fn set_log_target(&self, instance_id: &str, job_id: &str) -> Result<()> {
        self.log.set_target(instance_id, job_id).await
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
#[derive(Debug, Default, Clone, Copy)]
pub struct ChannelsBuilder;

impl ChannelsBuilder {
    /// Start all harness servers and build their matching channels.
    pub async fn build(self) -> Result<(Channels, ChannelServices)> {
        let status = HarnessStatus::new();
        let (control, control_service) = start_control_server(status.clone()).await?;
        let (data, data_service) = start_data_server(status.clone()).await?;
        let (log, log_service) = start_log_server().await?;
        let (state, state_service) = start_state_server(status.clone()).await?;

        Ok((
            Channels {
                control,
                data,
                log,
                state,
                status,
            },
            ChannelServices {
                control: control_service,
                data: data_service,
                log: log_service,
                state: state_service,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::HarnessStatus;
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
}
