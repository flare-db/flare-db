use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{StateRequest, StateResponse, beam_fn_state_server::BeamFnState};
use tokio::sync::{
    Mutex,
    mpsc::{self},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub struct StateInner {
    outgoing_tx: Mutex<Option<mpsc::Sender<Result<StateResponse, Status>>>>,
    outgoing_rx: Mutex<Option<mpsc::Receiver<Result<StateResponse, Status>>>>,
    incoming: Mutex<Option<tonic::Streaming<StateRequest>>>,
}

impl StateInner {
    async fn sender(&self) -> Result<mpsc::Sender<Result<StateResponse, Status>>> {
        self.outgoing_tx
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow!("state outgoing channel not initialized"))
    }
}

pub async fn start_state_server() -> Result<(StateChannel, FlareStateService)> {
    let (tx, rx) = mpsc::channel::<Result<StateResponse, Status>>(32);

    let stream = Arc::new(StateInner {
        outgoing_tx: Mutex::new(Some(tx)),
        outgoing_rx: Mutex::new(Some(rx)),
        incoming: Mutex::new(None),
    });

    let service = FlareStateService {
        inner: stream.clone(),
    };

    let channel = StateChannel { stream };

    Ok((channel, service))
}

pub struct FlareStateService {
    inner: Arc<StateInner>,
}

impl BeamFnState for FlareStateService {
    type StateStream = ReceiverStream<Result<StateResponse, Status>>;

    fn state<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<tonic::Streaming<StateRequest>>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<Self::StateStream>, tonic::Status>,
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
                        "State stream connected while a previous harness stream was still active; replacing stale stream"
                    );
                    let (tx, rx) = mpsc::channel::<Result<StateResponse, Status>>(32);
                    *self.inner.outgoing_tx.lock().await = Some(tx);
                    *rx_guard = Some(rx);
                }
                rx_guard
                    .take()
                    .expect("state outgoing receiver must be initialized")
            };

            Ok(Response::new(ReceiverStream::new(rx)))
        })
    }
}

pub struct StateChannel {
    stream: Arc<StateInner>,
}

impl StateChannel {
    pub async fn wait_connected(&self) -> Result<()> {
        loop {
            if self.stream.incoming.lock().await.is_some() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Reset the state channel so a new harness can connect.
    pub async fn reset(&self) {
        let (tx, rx) = mpsc::channel::<Result<StateResponse, Status>>(32);

        *self.stream.outgoing_tx.lock().await = Some(tx);
        *self.stream.outgoing_rx.lock().await = Some(rx);
        *self.stream.incoming.lock().await = None;

        log::info!("state channel reset for next harness");
    }

    pub async fn send_response(&self, response: StateResponse) -> Result<()> {
        let sender = self.stream.sender().await?;
        sender
            .send(Ok(response))
            .await
            .map_err(|e| anyhow!("failed to send state response: {}", e))
    }

    pub async fn recv_request(&self) -> Result<StateRequest> {
        let mut guard = self.stream.incoming.lock().await;

        let stream = guard
            .as_mut()
            .ok_or_else(|| anyhow!("harness not connected yet"))?;

        stream
            .message()
            .await
            .map_err(|e| anyhow!("state stream error: {}", e))?
            .ok_or_else(|| anyhow!("harness disconnected"))
    }
}
