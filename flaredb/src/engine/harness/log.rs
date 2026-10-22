use std::path::PathBuf;
use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{LogControl, beam_fn_logging_server::BeamFnLogging, log_entry};
use tokio::sync::{
    Mutex,
    mpsc::{self},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

use crate::utils::path;
use crate::worker::manager::{WorkerId, WorkerLogRouter};

/// Destination for SDK worker log entries flowing in over the Beam Fn Logging
/// API. Entries are appended to a per-job `flare-worker.log`, the same file the
/// harness process writes its stdout/stderr to, so a single file captures the
/// whole worker log for a job.
pub struct LogInner {
    outgoing_tx: Mutex<Option<mpsc::Sender<Result<LogControl, Status>>>>,
    outgoing_rx: Mutex<Option<mpsc::Receiver<Result<LogControl, Status>>>>,
    incoming: Mutex<Option<tonic::Streaming<log_entry::List>>>,
    worker_id: WorkerId,
    log_router: WorkerLogRouter,
    target: Mutex<Option<PathBuf>>,
}

impl LogInner {
    async fn sender(&self) -> Result<mpsc::Sender<Result<LogControl, Status>>> {
        self.outgoing_tx
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow!("log outgoing channel not initialized"))
    }
}

pub async fn start_log_server(
    log_router: WorkerLogRouter,
) -> Result<(LogChannel, FlareLogService)> {
    let channel = LogChannel::new(WorkerId::from_string("test-log-worker"), log_router);
    let service = FlareLogService {
        inner: Some(channel.stream.clone()),
        registry: None,
    };
    Ok((channel, service))
}

impl LogChannel {
    pub(crate) async fn attach_incoming_stream(
        &self,
        incoming: tonic::Streaming<log_entry::List>,
    ) -> std::result::Result<ReceiverStream<Result<LogControl, Status>>, Status> {
        *self.stream.incoming.lock().await = Some(incoming);
        let rx = {
            let mut rx_guard = self.stream.outgoing_rx.lock().await;
            if rx_guard.is_none() {
                let (tx, rx) = mpsc::channel::<Result<LogControl, Status>>(32);
                *self.stream.outgoing_tx.lock().await = Some(tx);
                *rx_guard = Some(rx);
            }
            rx_guard
                .take()
                .expect("log outgoing receiver must be initialized")
        };
        Ok(ReceiverStream::new(rx))
    }

    pub(crate) fn stream_incoming(&self) {
        if self
            .stream_task
            .lock()
            .expect("log task lock not poisoned")
            .is_some()
        {
            return;
        }
        let inner = self.stream.clone();
        let task = tokio::spawn(async move {
            loop {
                let mut guard = inner.incoming.lock().await;
                let Some(stream) = guard.as_mut() else {
                    drop(guard);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                loop {
                    let list = match stream.message().await {
                        Ok(Some(list)) => list,
                        Ok(None) | Err(_) => break,
                    };
                    let target = inner.target.lock().await.clone();
                    for entry in list.log_entries {
                        let message =
                            format!("level={:?} message={}", entry.severity, entry.message);
                        if let Some(path) = target.as_deref() {
                            if let Err(error) = inner
                                .log_router
                                .append_line(path, &inner.worker_id, "fn", &message)
                                .await
                            {
                                log::warn!(
                                    "failed to write Fn log id={} error={}",
                                    inner.worker_id,
                                    error
                                );
                            }
                        } else {
                            log::info!("worker={} source=fn {}", inner.worker_id, message);
                        }
                    }
                }
                break;
            }
        });
        *self.stream_task.lock().expect("log task lock not poisoned") = Some(task);
    }

    pub(crate) fn new(worker_id: WorkerId, log_router: WorkerLogRouter) -> Self {
        let (tx, rx) = mpsc::channel::<Result<LogControl, Status>>(32);

        let stream = Arc::new(LogInner {
            outgoing_tx: Mutex::new(Some(tx)),
            outgoing_rx: Mutex::new(Some(rx)),
            incoming: Mutex::new(None),
            worker_id,
            log_router,
            target: Mutex::new(None),
        });
        LogChannel {
            stream,
            stream_task: Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

pub struct FlareLogService {
    inner: Option<Arc<LogInner>>,
    registry: Option<super::WorkerChannelRegistry>,
}

impl FlareLogService {
    pub(crate) fn new(registry: super::WorkerChannelRegistry) -> Self {
        Self {
            inner: None,
            registry: Some(registry),
        }
    }
}

impl BeamFnLogging for FlareLogService {
    type LoggingStream = ReceiverStream<Result<LogControl, Status>>;

    fn logging<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<tonic::Streaming<log_entry::List>>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<Self::LoggingStream>,
                        tonic::Status,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if let Some(registry) = &self.registry {
                let worker_id = super::WorkerChannelRegistry::worker_id(request.metadata())?;
                let worker = registry.worker(&worker_id);
                if let Some(path) = registry.worker_log_path().await {
                    worker
                        .log
                        .set_target_path(path)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                }
                let rx = worker
                    .log
                    .attach_incoming_stream(request.into_inner())
                    .await?;
                worker.log.stream_incoming();
                return Ok(Response::new(rx));
            }
            let inner = self
                .inner
                .as_ref()
                .expect("log service must have a channel backend");
            let mut stream = request.into_inner();
            let reader_inner = inner.clone();
            tokio::spawn(async move {
                while let Ok(Some(list)) = stream.message().await {
                    let target = reader_inner.target.lock().await.clone();
                    for entry in list.log_entries {
                        let message =
                            format!("level={:?} message={}", entry.severity, entry.message);
                        if let Some(path) = target.as_deref() {
                            if let Err(error) = reader_inner
                                .log_router
                                .append_line(path, &reader_inner.worker_id, "fn", &message)
                                .await
                            {
                                log::warn!(
                                    "failed to write Fn log id={} error={}",
                                    reader_inner.worker_id,
                                    error
                                );
                            }
                        } else {
                            log::info!("worker={} source=fn {}", reader_inner.worker_id, message);
                        }
                    }
                }
            });

            let rx = {
                let mut rx_guard = inner.outgoing_rx.lock().await;
                if rx_guard.is_none() {
                    log::warn!(
                        "Log stream connected while a previous harness stream was still active; replacing stale stream"
                    );
                    let (tx, rx) = mpsc::channel::<Result<LogControl, Status>>(32);
                    *inner.outgoing_tx.lock().await = Some(tx);
                    *rx_guard = Some(rx);
                }
                rx_guard
                    .take()
                    .expect("log outgoing receiver must be initialized")
            };

            Ok(Response::new(ReceiverStream::new(rx)))
        })
    }
}

#[derive(Clone)]
pub struct LogChannel {
    stream: Arc<LogInner>,
    stream_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl LogChannel {
    pub async fn wait_connected(&self) -> Result<()> {
        loop {
            if self.stream.incoming.lock().await.is_some() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Reset the log channel so a new harness can connect.
    pub async fn reset(&self) {
        let task = {
            self.stream_task
                .lock()
                .expect("log task lock not poisoned")
                .take()
        };
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        let (tx, rx) = mpsc::channel::<Result<LogControl, Status>>(32);

        *self.stream.outgoing_tx.lock().await = Some(tx);
        *self.stream.outgoing_rx.lock().await = Some(rx);
        *self.stream.incoming.lock().await = None;
        *self.stream.target.lock().await = None;

        log::info!("log channel reset for next harness");
    }

    /// Route SDK worker log entries for `job_id` into that job's
    /// `flare-worker.log`, alongside the harness process stdout/stderr.
    pub async fn set_target(&self, instance_id: &str, job_id: &str) -> Result<()> {
        let path = path::logs_dir(instance_id, job_id).join("flare-worker.log");
        self.set_target_path(path).await
    }

    pub(crate) async fn set_target_path(&self, path: PathBuf) -> Result<()> {
        *self.stream.target.lock().await = Some(path.clone());
        log::info!("routing SDK worker logs to {}", path.display());
        Ok(())
    }

    pub async fn send_control(&self, control: LogControl) -> Result<()> {
        let sender = self.stream.sender().await?;
        sender
            .send(Ok(control))
            .await
            .map_err(|e| anyhow!("failed to send log control: {}", e))
    }

    pub async fn recv_entries(&self) -> Result<log_entry::List> {
        let mut guard = self.stream.incoming.lock().await;

        let stream = guard
            .as_mut()
            .ok_or_else(|| anyhow!("harness not connected yet"))?;

        stream
            .message()
            .await
            .map_err(|e| anyhow!("log stream error: {}", e))?
            .ok_or_else(|| anyhow!("harness disconnected"))
    }
}
