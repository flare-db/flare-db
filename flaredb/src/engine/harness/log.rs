use std::fs::OpenOptions;
use std::io::Write;
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

/// Destination for SDK worker log entries flowing in over the Beam Fn Logging
/// API. Entries are appended to a per-job `flare-worker.log`, the same file the
/// harness process writes its stdout/stderr to, so a single file captures the
/// whole worker log for a job.
#[derive(Default)]
struct LogSink {
    path: Option<PathBuf>,
    file: Option<std::fs::File>,
}

impl LogSink {
    /// Point the sink at `path`, opening it for appending. No-op when the sink
    /// is already writing to the same file.
    fn set_target(&mut self, path: PathBuf) -> std::io::Result<()> {
        if self.file.is_some() && self.path.as_deref() == Some(path.as_path()) {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        self.path = Some(path);
        self.file = Some(file);
        Ok(())
    }

    /// Append a line to the configured log file. Returns `false` when no target
    /// has been configured yet, so the caller can fall back to the server log.
    fn write_line(&mut self, line: &str) -> bool {
        match self.file.as_mut() {
            Some(file) => {
                let _ = file.write_all(line.as_bytes());
                let _ = file.flush();
                true
            }
            None => false,
        }
    }

    /// Forget the current target so entries fall back to the server log until a
    /// new job sets its own target.
    fn clear(&mut self) {
        self.path = None;
        self.file = None;
    }
}

pub struct LogInner {
    outgoing_tx: Mutex<Option<mpsc::Sender<Result<LogControl, Status>>>>,
    outgoing_rx: Mutex<Option<mpsc::Receiver<Result<LogControl, Status>>>>,
    incoming: Mutex<Option<tonic::Streaming<log_entry::List>>>,
    sink: Mutex<LogSink>,
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

pub async fn start_log_server() -> Result<(LogChannel, FlareLogService)> {
    let (tx, rx) = mpsc::channel::<Result<LogControl, Status>>(32);

    let stream = Arc::new(LogInner {
        outgoing_tx: Mutex::new(Some(tx)),
        outgoing_rx: Mutex::new(Some(rx)),
        incoming: Mutex::new(None),
        sink: Mutex::new(LogSink::default()),
    });

    let service = FlareLogService {
        inner: stream.clone(),
    };

    let channel = LogChannel { stream };

    Ok((channel, service))
}

pub struct FlareLogService {
    inner: Arc<LogInner>,
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
            let mut stream = request.into_inner();
            let inner = self.inner.clone();
            tokio::spawn(async move {
                while let Ok(Some(list)) = stream.message().await {
                    let mut sink = inner.sink.lock().await;
                    for entry in list.log_entries {
                        let line = format!(
                            "[FLARE WORKER LOG] level={:?} message={}\n",
                            entry.severity, entry.message
                        );
                        if !sink.write_line(&line) {
                            // No per-job target configured yet; keep the entry in
                            // the server log rather than dropping it.
                            log::info!("{}", line.trim_end());
                        }
                    }
                }
            });

            let rx = {
                let mut rx_guard = self.inner.outgoing_rx.lock().await;
                if rx_guard.is_none() {
                    log::warn!(
                        "Log stream connected while a previous harness stream was still active; replacing stale stream"
                    );
                    let (tx, rx) = mpsc::channel::<Result<LogControl, Status>>(32);
                    *self.inner.outgoing_tx.lock().await = Some(tx);
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

pub struct LogChannel {
    stream: Arc<LogInner>,
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
        let (tx, rx) = mpsc::channel::<Result<LogControl, Status>>(32);

        *self.stream.outgoing_tx.lock().await = Some(tx);
        *self.stream.outgoing_rx.lock().await = Some(rx);
        *self.stream.incoming.lock().await = None;
        self.stream.sink.lock().await.clear();

        log::info!("log channel reset for next harness");
    }

    /// Route SDK worker log entries for `job_id` into that job's
    /// `flare-worker.log`, alongside the harness process stdout/stderr.
    pub async fn set_target(&self, instance_id: &str, job_id: &str) -> Result<()> {
        let path = path::logs_dir(instance_id, job_id).join("flare-worker.log");
        self.stream
            .sink
            .lock()
            .await
            .set_target(path.clone())
            .map_err(|e| anyhow!("failed to open worker log {}: {}", path.display(), e))?;
        log::info!(
            "routing SDK worker logs for job {} to {}",
            job_id,
            path.display()
        );
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
