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
/// API. Every worker appends to the job's single `flare-worker.log`, and each
/// entry carries the worker id.
#[derive(Default)]
struct LogSink {
    path: Option<PathBuf>,
    file: Option<std::fs::File>,
    worker_id: Option<usize>,
}

impl LogSink {
    /// Point the sink at `path`, opening it for appending. No-op when the sink
    /// is already writing to the same file.
    fn set_target(&mut self, path: PathBuf, worker_id: usize) -> std::io::Result<()> {
        self.worker_id = Some(worker_id);
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
        self.worker_id = None;
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
                        let line =
                            format_worker_log_line(sink.worker_id, &entry.severity, &entry.message);
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

fn format_worker_log_line(
    worker_id: Option<usize>,
    severity: &impl std::fmt::Debug,
    message: &str,
) -> String {
    let worker_id = worker_id
        .map(|worker_id| worker_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    format!("[FLARE WORKER LOG] worker_id={worker_id} level={severity:?} message={message}\n")
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

    /// Route SDK worker log entries for `job_id` into that job's single
    /// `flare-worker.log`, alongside the prefixed harness process stdout/stderr.
    pub async fn set_target(
        &self,
        instance_id: &str,
        job_id: &str,
        worker_id: usize,
    ) -> Result<()> {
        let path = path::logs_dir(instance_id, job_id).join("flare-worker.log");
        self.stream
            .sink
            .lock()
            .await
            .set_target(path.clone(), worker_id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_log_entries_share_one_file_and_carry_worker_ids() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("flare-worker.log");
        let mut first = LogSink::default();
        let mut second = LogSink::default();
        first
            .set_target(path.clone(), 3)
            .expect("open first worker sink");
        second
            .set_target(path.clone(), 7)
            .expect("open second worker sink");

        assert!(first.write_line(&format_worker_log_line(Some(3), &"INFO", "first worker")));
        assert!(second.write_line(&format_worker_log_line(Some(7), &"WARN", "second worker")));

        let contents = std::fs::read_to_string(&path).expect("read worker log");
        assert!(contents.contains("worker_id=3 level=\"INFO\" message=first worker"));
        assert!(contents.contains("worker_id=7 level=\"WARN\" message=second worker"));
        assert_eq!(
            std::fs::read_dir(temp.path())
                .expect("read temp dir")
                .count(),
            1,
            "only the job-level worker log is created"
        );
    }
}
