use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{LogControl, beam_fn_logging_server::BeamFnLogging, log_entry};
use tokio::sync::{
    Mutex,
    mpsc::{self},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub struct LogInner {
    outgoing_tx: Mutex<Option<mpsc::Sender<Result<LogControl, Status>>>>,
    outgoing_rx: Mutex<Option<mpsc::Receiver<Result<LogControl, Status>>>>,
    incoming: Mutex<Option<tonic::Streaming<log_entry::List>>>,
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
            *self.inner.incoming.lock().await = Some(request.into_inner());

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

        log::info!("log channel reset for next harness");
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
