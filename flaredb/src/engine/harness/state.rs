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

use crate::{
    engine::state::{StateBackend, UserStateAddress, UserStateKind, UserStateStore},
    store::element_store::FlareElementStore,
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
            log::info!("BeamFnState stream connected from worker");
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
        let backend = StateBackend::new(store);

        let join_handle = tokio::spawn(async move {
            channel.drive_state_requests(backend).await;
        });

        *task_slot.lock().unwrap() = Some(join_handle);
    }

    async fn drive_state_requests(&self, backend: StateBackend) {
        info!("state request driver started");
        loop {
            match self.recv_request().await {
                Ok(request) => {
                    let summary = describe_request(&request);
                    //info!("state request received: {}", summary);
                    let response = handle_state_request(&backend, request).await;
                    match response {
                        Ok(response) => {
                            //info!("state response sent: id={}", response.id);
                            if let Err(e) = self.send_response(response).await {
                                warn!("failed to send state response (worker gone?): {}", e);
                                break;
                            }
                        }
                        Err((id, error)) => {
                            warn!("state request failed: id={} error={}", id, error);
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

/// Human-readable summary of a [`StateRequest`] for logging.
fn describe_request(request: &StateRequest) -> String {
    let key = match request.state_key.as_ref().and_then(|k| k.r#type.as_ref()) {
        Some(state_key::Type::BagUserState(bag)) => format!(
            "bag_user_state(transform_id={}, user_state_id={}, window={}B, key={}B)",
            bag.transform_id,
            bag.user_state_id,
            bag.window.len(),
            bag.key.len()
        ),
        Some(_) => "non-bag-user-state".to_string(),
        None => "no-state-key".to_string(),
    };
    let op = match request.request {
        Some(state_request::Request::Get(_)) => "get",
        Some(state_request::Request::Append(_)) => "append",
        Some(state_request::Request::Clear(_)) => "clear",
        None => "none",
    };
    format!(
        "id={} instruction_id={} op={} key={}",
        request.id, request.instruction_id, op, key
    )
}

/// Dispatch a single [`StateRequest`] through the user-state abstraction.
///
/// `StateKey` is decoded here (transport concern) into a transport-agnostic
/// [`UserStateAddress`]; the state layer interprets it. Unsupported key types
/// return an error matching the original request id.
///
async fn handle_state_request(
    backend: &StateBackend,
    request: StateRequest,
) -> std::result::Result<StateResponse, (String, String)> {
    let id = request.id.clone();
    let state_key = request.state_key.and_then(|k| k.r#type);
    let body = request.request;

    let address = match state_key {
        Some(key) => match user_state_address(key) {
            Ok(address) => address,
            Err(message) => return Err((id, message)),
        },
        None => return Err((id, "state request has no state key".to_string())),
    };
    let user_state = UserStateStore::new(backend.clone(), address);

    match body {
        Some(state_request::Request::Get(get)) => {
            if !get.continuation_token.is_empty() {
                return Err((id, "continuation tokens are not supported".to_string()));
            }
            match user_state.get().await {
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
        Some(state_request::Request::Append(append)) => match user_state.append(&append.data).await
        {
            Ok(()) => Ok(StateResponse {
                id,
                error: String::new(),
                response: Some(state_response::Response::Append(StateAppendResponse {})),
            }),
            Err(e) => Err((id, format!("append state failed: {e}"))),
        },
        Some(state_request::Request::Clear(_)) => match user_state.clear().await {
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

/// Decode a Beam `StateKey` user-state variant into a [`UserStateAddress`].
///
/// Only `BagUserState` is implemented; every other variant (side inputs and the
/// multimap/ordered-list user-state kinds) is rejected explicitly so the SDK
/// gets a clear error instead of a silent wrong answer.
fn user_state_address(key: state_key::Type) -> std::result::Result<UserStateAddress, String> {
    match key {
        state_key::Type::BagUserState(bag) => Ok(UserStateAddress::new(
            UserStateKind::Bag,
            bag.transform_id,
            bag.user_state_id,
            bag.window,
            bag.key,
        )),
        other => Err(format!(
            "unsupported state key type '{}' (only '{}' is supported)",
            state_key_kind(&other),
            UserStateKind::Bag.wire_name(),
        )),
    }
}

/// Human-readable name for a [`state_key::Type`] variant, for diagnostics.
fn state_key_kind(key: &state_key::Type) -> &'static str {
    match key {
        state_key::Type::Runner(_) => "runner",
        state_key::Type::MultimapSideInput(_) => "multimap_side_input",
        state_key::Type::BagUserState(_) => "bag_user_state",
        state_key::Type::IterableSideInput(_) => "iterable_side_input",
        state_key::Type::MultimapKeysSideInput(_) => "multimap_keys_side_input",
        state_key::Type::MultimapKeysValuesSideInput(_) => "multimap_keys_values_side_input",
        state_key::Type::MultimapKeysUserState(_) => "multimap_keys_user_state",
        state_key::Type::MultimapEntriesUserState(_) => "multimap_entries_user_state",
        state_key::Type::MultimapUserState(_) => "multimap_user_state",
        state_key::Type::OrderedListUserState(_) => "ordered_list_user_state",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::element_store::FlareElementStore;
    use beam_model_rs::v1::{StateKey, state_key};
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn make_backend() -> (tempfile::TempDir, StateBackend) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        (dir, StateBackend::new(Arc::new(store)))
    }

    /// A `StateRequest` targeting `BagUserState` for the given window/key.
    fn bag_request(
        id: &str,
        window: &[u8],
        key: &[u8],
        op: state_request::Request,
    ) -> StateRequest {
        StateRequest {
            id: id.to_string(),
            instruction_id: "instruction".to_string(),
            state_key: Some(StateKey {
                r#type: Some(state_key::Type::BagUserState(state_key::BagUserState {
                    transform_id: "transform".to_string(),
                    user_state_id: "state".to_string(),
                    window: window.to_vec(),
                    key: key.to_vec(),
                })),
            }),
            request: Some(op),
        }
    }

    fn get_op() -> state_request::Request {
        state_request::Request::Get(beam_model_rs::v1::StateGetRequest {
            continuation_token: Vec::new(),
        })
    }

    fn append_op(data: &[u8]) -> state_request::Request {
        state_request::Request::Append(beam_model_rs::v1::StateAppendRequest {
            data: data.to_vec(),
        })
    }

    fn clear_op() -> state_request::Request {
        state_request::Request::Clear(beam_model_rs::v1::StateClearRequest {})
    }

    fn get_response_data(response: StateResponse) -> Vec<u8> {
        match response.response {
            Some(state_response::Response::Get(get)) => get.data,
            other => panic!("expected get response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_then_get_concatenates() {
        let (_dir, backend) = make_backend().await;

        handle_state_request(&backend, bag_request("1", b"w", b"k", append_op(b"a")))
            .await
            .unwrap();
        handle_state_request(&backend, bag_request("2", b"w", b"k", append_op(b"b")))
            .await
            .unwrap();

        let response = handle_state_request(&backend, bag_request("3", b"w", b"k", get_op()))
            .await
            .unwrap();
        assert_eq!(get_response_data(response), b"ab");
    }

    #[tokio::test]
    async fn clear_empties_the_cell() {
        let (_dir, backend) = make_backend().await;

        handle_state_request(&backend, bag_request("1", b"w", b"k", append_op(b"data")))
            .await
            .unwrap();
        handle_state_request(&backend, bag_request("2", b"w", b"k", clear_op()))
            .await
            .unwrap();

        let response = handle_state_request(&backend, bag_request("3", b"w", b"k", get_op()))
            .await
            .unwrap();
        assert!(get_response_data(response).is_empty());
    }

    #[tokio::test]
    async fn state_is_isolated_per_window_and_key() {
        let (_dir, backend) = make_backend().await;

        handle_state_request(
            &backend,
            bag_request("1", b"w1", b"k", append_op(b"window-1")),
        )
        .await
        .unwrap();
        handle_state_request(
            &backend,
            bag_request("2", b"w2", b"k", append_op(b"window-2")),
        )
        .await
        .unwrap();
        handle_state_request(
            &backend,
            bag_request("3", b"w1", b"other", append_op(b"other-key")),
        )
        .await
        .unwrap();

        let w1 = handle_state_request(&backend, bag_request("4", b"w1", b"k", get_op()))
            .await
            .unwrap();
        let w2 = handle_state_request(&backend, bag_request("5", b"w2", b"k", get_op()))
            .await
            .unwrap();
        let other = handle_state_request(&backend, bag_request("6", b"w1", b"other", get_op()))
            .await
            .unwrap();

        assert_eq!(get_response_data(w1), b"window-1");
        assert_eq!(get_response_data(w2), b"window-2");
        assert_eq!(get_response_data(other), b"other-key");
    }

    #[tokio::test]
    async fn multi_element_bag_supports_value_state_shape() {
        // The SDK harness backs ValueState/CombiningState with BagUserState; a
        // single-element bag must round-trip as an opaque encoded value.
        let (_dir, backend) = make_backend().await;

        let encoded_value = b"\x00\x01single-value";
        handle_state_request(
            &backend,
            bag_request("1", b"w", b"k", append_op(encoded_value)),
        )
        .await
        .unwrap();

        let response = handle_state_request(&backend, bag_request("2", b"w", b"k", get_op()))
            .await
            .unwrap();
        assert_eq!(get_response_data(response), encoded_value);
    }

    #[tokio::test]
    async fn unsupported_state_key_reports_kind_and_request_id() {
        let (_dir, backend) = make_backend().await;

        let request = StateRequest {
            id: "req-7".to_string(),
            instruction_id: "instruction".to_string(),
            state_key: Some(StateKey {
                r#type: Some(state_key::Type::MultimapUserState(
                    state_key::MultimapUserState {
                        transform_id: "t".to_string(),
                        user_state_id: "s".to_string(),
                        window: b"w".to_vec(),
                        key: b"k".to_vec(),
                        map_key: b"mk".to_vec(),
                    },
                )),
            }),
            request: Some(get_op()),
        };

        let (id, error) = handle_state_request(&backend, request).await.unwrap_err();
        assert_eq!(id, "req-7");
        assert!(
            error.contains("multimap_user_state"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn continuation_tokens_are_rejected() {
        let (_dir, backend) = make_backend().await;

        let request = bag_request(
            "1",
            b"w",
            b"k",
            state_request::Request::Get(beam_model_rs::v1::StateGetRequest {
                continuation_token: b"resume".to_vec(),
            }),
        );

        let (id, error) = handle_state_request(&backend, request).await.unwrap_err();
        assert_eq!(id, "1");
        assert!(error.contains("continuation"), "unexpected error: {error}");
    }
}
