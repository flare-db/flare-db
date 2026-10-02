use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{
    StateAppendResponse, StateClearResponse, StateGetResponse, StateRequest, StateResponse,
    beam_fn_state_server::BeamFnState, state_key, state_request, state_response,
};
use log::{info, warn};
use tokio::sync::{
    Mutex,
    mpsc::{self},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

use crate::store::{
    element_store::FlareElementStore,
    state::{FlareStateStore, build_state_key},
};

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

    let channel = StateChannel {
        stream,
        stream_task: Arc::new(std::sync::Mutex::new(None)),
    };

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

/// Client-side channel for the executor to service the worker's State stream.
#[derive(Clone)]
pub struct StateChannel {
    stream: Arc<StateInner>,
    /// Handle to the background state request driver task.
    /// Aborted on reset to prevent a stale task from consuming a new stream.
    stream_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
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
        // Abort the state driver from the previous job so it does not race with
        // the new driver to consume the incoming stream.
        let handle = self.stream_task.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }

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

    /// Receive the next [`StateRequest`] from the worker.
    ///
    /// Blocks (polling) until the worker connects its State stream, then reads
    /// one message. Returns an error once the stream ends or is reset.
    pub async fn recv_request(&self) -> Result<StateRequest> {
        loop {
            let mut guard = self.stream.incoming.lock().await;

            if let Some(stream) = guard.as_mut() {
                match stream.message().await {
                    Ok(Some(request)) => return Ok(request),
                    Ok(None) => return Err(anyhow!("harness disconnected")),
                    Err(e) => return Err(anyhow!("state stream error: {}", e)),
                }
            }

            // Not connected yet — drop the lock and retry after a short sleep.
            drop(guard);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Start a background task that services Beam Fn State requests against
    /// `store` until the stream ends or the channel is reset.
    pub fn stream_requests(&self, store: Arc<FlareElementStore>) {
        let channel = self.clone();
        let task_slot = self.stream_task.clone();
        let state_store = FlareStateStore::new(store);

        let join_handle = tokio::spawn(async move {
            channel.drive_state_requests(state_store).await;
        });

        *task_slot.lock().unwrap() = Some(join_handle);
    }

    async fn drive_state_requests(&self, store: FlareStateStore) {
        info!("state request driver started");
        loop {
            match self.recv_request().await {
                Ok(request) => {
                    let response = handle_state_request(&store, request).await;
                    match response {
                        Ok(response) => {
                            if let Err(e) = self.send_response(response).await {
                                warn!("failed to send state response (worker gone?): {}", e);
                                break;
                            }
                        }
                        Err((id, error)) => {
                            let response = StateResponse {
                                id,
                                error,
                                response: None,
                            };
                            if let Err(e) = self.send_response(response).await {
                                warn!("failed to send state error response (worker gone?): {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    info!("state request stream ended: {}", e);
                    break;
                }
            }
        }
        info!("state request driver stopped");
    }
}

/// Dispatch a single [`StateRequest`] to the bag-user-state store.
///
/// Returns `Ok(response)` on success, or `Err((request_id, error))` so the
/// caller can respond with an error matching the original request id.
async fn handle_state_request(
    store: &FlareStateStore,
    request: StateRequest,
) -> std::result::Result<StateResponse, (String, String)> {
    let id = request.id.clone();
    let state_key = request.state_key.and_then(|k| k.r#type);
    let body = request.request;

    let bag = match state_key {
        Some(state_key::Type::BagUserState(bag)) => bag,
        Some(_) => {
            return Err((
                id,
                "unsupported state key type (only bag_user_state is supported)".to_string(),
            ));
        }
        None => {
            return Err((id, "state request has no state key".to_string()));
        }
    };

    let composite = build_state_key(&bag.transform_id, &bag.user_state_id, &bag.window, &bag.key);

    match body {
        Some(state_request::Request::Get(get)) => {
            if !get.continuation_token.is_empty() {
                return Err((id, "continuation tokens are not supported".to_string()));
            }
            match store.get_bag(&composite).await {
                Ok(data) => Ok(StateResponse {
                    id,
                    error: String::new(),
                    response: Some(state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data,
                    })),
                }),
                Err(e) => Err((id, format!("get state failed: {e}"))),
            }
        }
        Some(state_request::Request::Append(append)) => {
            match store.append_bag(&composite, &append.data).await {
                Ok(()) => Ok(StateResponse {
                    id,
                    error: String::new(),
                    response: Some(state_response::Response::Append(StateAppendResponse {})),
                }),
                Err(e) => Err((id, format!("append state failed: {e}"))),
            }
        }
        Some(state_request::Request::Clear(_)) => match store.clear_bag(&composite).await {
            Ok(()) => Ok(StateResponse {
                id,
                error: String::new(),
                response: Some(state_response::Response::Clear(StateClearResponse {})),
            }),
            Err(e) => Err((id, format!("clear state failed: {e}"))),
        },
        None => Err((id, "state request has no request body".to_string())),
    }
}
