use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{LogControl, beam_fn_logging_server::BeamFnLogging, log_entry};
use tokio::sync::{
    Mutex,
    mpsc::{self, Sender},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub struct LogInner {
    outgoing: Mutex<Option<mpsc::Receiver<Result<LogControl, Status>>>>,
    incoming: Mutex<Option<tonic::Streaming<log_entry::List>>>,
}

pub async fn start_log_server() -> Result<(LogChannel, FlareLogService)> {
    let (tx, rx) = mpsc::channel::<Result<LogControl, Status>>(32);

    let stream = Arc::new(LogInner {
        outgoing: Mutex::new(Some(rx)),
        incoming: Mutex::new(None),
    });

    let service = FlareLogService {
        inner: stream.clone(),
    };

    let channel = LogChannel {
        outgoing: tx,
        stream,
    };

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

            let rx = self
                .inner
                .outgoing
                .lock()
                .await
                .take()
                .ok_or_else(|| Status::internal("harness connected twice"))?;

            Ok(Response::new(ReceiverStream::new(rx)))
        })
    }
}

pub struct LogChannel {
    outgoing: Sender<Result<LogControl, Status>>,
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

    pub async fn send_control(&self, control: LogControl) -> Result<()> {
        self.outgoing
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
