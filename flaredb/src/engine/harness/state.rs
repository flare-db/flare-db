use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{
    Coder, Components, StateAppendResponse, StateClearResponse, StateGetResponse, StateRequest,
    StateResponse, beam_fn_state_server::BeamFnState, state_key, state_request, state_response,
};
use log::{error, info, warn};
use tokio::sync::{
    Mutex,
    mpsc::{self},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

use crate::{
    coders::{
        StandardBeamCoders,
        primitives::{BeamWindow, WindowCoder},
    },
    engine::{
        kv::SlateKvStore,
        runtime::resolve_window_coder,
        state::{StateBackend, UserStateAddress, UserStateKind, UserStateStore},
    },
    fusion::pipeline::{ExecutableGraph, ExecutableNode},
    store::element_store::{FlareElementStore, ScanCollectionRequest, slate_state_dir},
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SideInputKey {
    transform_id: String,
    side_input_id: String,
}

#[derive(Debug, Clone)]
struct SideInputSpec {
    pcollection_id: String,
    element_coder: StandardBeamCoders,
    window_coder: WindowCoder,
}

/// Fn State lookup metadata for iterable side inputs in a prepared pipeline.
#[derive(Debug, Clone, Default)]
pub struct SideInputRegistry {
    specs: Arc<HashMap<SideInputKey, SideInputSpec>>,
}

impl SideInputRegistry {
    /// Resolve every side input declared by an executable worker stage to its
    /// materialized PCollection, element coder, and window coder.
    pub fn from_executable_graph(
        graph: &ExecutableGraph,
        pipeline_coders: &HashMap<String, Coder>,
    ) -> Result<Self> {
        let executable = graph.get_executable_graph();
        let produced_pcollections: HashSet<String> = executable
            .node_weights()
            .flat_map(|node| node.output_pcols())
            .collect();
        let components: &Components = &graph.components;
        let mut specs = HashMap::new();

        for node in executable.node_weights() {
            let ExecutableNode::Worker(stage) = node else {
                continue;
            };
            for side_input in stage.side_inputs() {
                let pcollection = side_input.collection();
                let pcollection_id = pcollection.id().clone();
                if !produced_pcollections.contains(&pcollection_id) {
                    return Err(anyhow!(
                        "iterable side input '{}' on transform '{}' references PCollection '{}' with no executable producer",
                        side_input.local_name(),
                        side_input.transform().id(),
                        pcollection_id
                    ));
                }
                let coder_id = pcollection.node().coder_id.clone();
                let element_coder =
                    StandardBeamCoders::from_urn(&coder_id, None, Some(pipeline_coders));
                let window_coder =
                    resolve_window_coder(components, pipeline_coders, &pcollection_id)?;
                let key = SideInputKey {
                    transform_id: side_input.transform().id().clone(),
                    side_input_id: side_input.local_name().to_string(),
                };
                let spec = SideInputSpec {
                    pcollection_id,
                    element_coder,
                    window_coder,
                };
                if let Some(existing) = specs.insert(key.clone(), spec) {
                    return Err(anyhow!(
                        "duplicate iterable side input reference for transform '{}' and side input '{}' (PCollections '{}' and '{}')",
                        key.transform_id,
                        key.side_input_id,
                        existing.pcollection_id,
                        specs
                            .get(&key)
                            .expect("inserted side input spec must be present")
                            .pcollection_id
                    ));
                }
            }
        }

        Ok(Self {
            specs: Arc::new(specs),
        })
    }

    fn get(&self, key: &SideInputKey) -> Option<&SideInputSpec> {
        self.specs.get(key)
    }

    #[cfg(test)]
    fn from_entries(entries: impl IntoIterator<Item = (SideInputKey, SideInputSpec)>) -> Self {
        Self {
            specs: Arc::new(entries.into_iter().collect()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IterableSideInputAddress {
    key: SideInputKey,
    window: Vec<u8>,
}

type EncodedSideInputWindows = HashMap<BeamWindow, Vec<u8>>;
type SideInputCache = HashMap<SideInputKey, Arc<EncodedSideInputWindows>>;

/// Per-job side-input reader over the materialized PCollections. Values are
/// grouped and encoded once per side input, then reused for subsequent StateGet
/// requests in the bounded run.
struct SideInputResolver {
    store: Arc<FlareElementStore>,
    registry: SideInputRegistry,
    cache: Mutex<SideInputCache>,
}

impl SideInputResolver {
    fn new(store: Arc<FlareElementStore>, registry: SideInputRegistry) -> Self {
        Self {
            store,
            registry,
            cache: Mutex::new(HashMap::new()),
        }
    }

    async fn read_iterable(&self, address: &IterableSideInputAddress) -> Result<Vec<u8>> {
        let spec = self.registry.get(&address.key).ok_or_else(|| {
            anyhow!(
                "unknown iterable side input transform_id='{}' side_input_id='{}'",
                address.key.transform_id,
                address.key.side_input_id
            )
        })?;
        let window = spec
            .window_coder
            .decode_window(&address.window)
            .map_err(|e| anyhow!("invalid side-input window: {e}"))?;

        let mut cache = self.cache.lock().await;
        if !cache.contains_key(&address.key) {
            let values = self
                .store
                .scan_windowed_values(ScanCollectionRequest {
                    pcollection_id: spec.pcollection_id.clone(),
                })
                .await?;
            let mut grouped = EncodedSideInputWindows::new();
            for value in values {
                let mut seen_windows = HashSet::new();
                for value_window in value.windows {
                    if !seen_windows.insert(value_window.clone()) {
                        continue;
                    }
                    let encoded = grouped.entry(value_window).or_default();
                    let before = encoded.len();
                    spec.element_coder
                        .encode_element(value.value.clone(), encoded);
                    // Beam's data stream decoder advances one byte when a coder
                    // decodes to zero bytes, so preserve that element boundary.
                    if encoded.len() == before {
                        encoded.push(0);
                    }
                }
            }
            cache.insert(address.key.clone(), Arc::new(grouped));
        }

        Ok(cache
            .get(&address.key)
            .and_then(|grouped| grouped.get(&window))
            .cloned()
            .unwrap_or_default())
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
    /// user state and materialized side-input PCollections until the stream ends
    /// or the channel is reset.
    pub fn stream_requests(&self, store: Arc<FlareElementStore>, side_inputs: SideInputRegistry) {
        let channel = self.clone();
        let task_slot = self.stream_task.clone();

        let join_handle = tokio::spawn(async move {
            match build_state_backend(store.clone()).await {
                Ok(backend) => {
                    let side_inputs = SideInputResolver::new(store, side_inputs);
                    channel.drive_state_requests(backend, side_inputs).await
                }
                Err(e) => error!("failed to construct state backend: {e}"),
            }
        });

        *task_slot.lock().unwrap() = Some(join_handle);
    }

    async fn drive_state_requests(&self, backend: StateBackend, side_inputs: SideInputResolver) {
        info!("state request driver started");
        loop {
            match self.recv_request().await {
                Ok(request) => {
                    crate::engine::liveness::touch();
                    let summary = describe_request(&request);
                    let started = std::time::Instant::now();
                    let response = handle_state_request_with_side_inputs(
                        &backend,
                        Some(&side_inputs),
                        request,
                    )
                    .await;
                    // A stateful DoFn drives many state ops per bundle; if each is
                    // slow the bundle misses its deadline (N3), so surface the slow
                    // ones with the request summary.
                    let elapsed = started.elapsed();
                    if elapsed.as_millis() >= 20 {
                        warn!(
                            "slow state request ({}ms): {}",
                            elapsed.as_millis(),
                            summary
                        );
                    }
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
        flush_state_on_exit(&backend).await;
        info!("state request driver stopped");
    }
}

/// Commit the write-back cache when the state driver exits.
///
/// The cache holds the cells in memory. A cell that stays in the cache is lost
/// when the process exits.
async fn flush_state_on_exit(backend: &StateBackend) {
    if let Err(e) = backend.flush().await {
        warn!("failed to flush user state on driver exit: {}", e);
    }
}

/// Construct a job's state backend in the `state` database under
/// `<warehouse>/.slate_state/<db>/`.
async fn build_state_backend(store: Arc<FlareElementStore>) -> Result<StateBackend> {
    let dir = slate_state_dir(&store);
    let kv = SlateKvStore::open(&dir, "state").await?;
    info!("using SlateDB state backend at {}", dir.display());
    Ok(StateBackend::with_store(Arc::new(kv)))
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
        Some(state_key::Type::IterableSideInput(side_input)) => format!(
            "iterable_side_input(transform_id={}, side_input_id={}, window={}B)",
            side_input.transform_id,
            side_input.side_input_id,
            side_input.window.len()
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

enum StateTarget {
    UserState(UserStateAddress),
    IterableSideInput(IterableSideInputAddress),
}

/// Dispatch a user-state request without side-input lookup metadata.
///
/// Production calls [`handle_state_request_with_side_inputs`]; this narrow
/// helper keeps user-state tests independent of PCollection setup.
#[cfg(test)]
async fn handle_state_request(
    backend: &StateBackend,
    request: StateRequest,
) -> std::result::Result<StateResponse, (String, String)> {
    handle_state_request_with_side_inputs(backend, None, request).await
}

/// Dispatch a single [`StateRequest`] through either user state or an iterable
/// side-input lookup.
///
/// `StateKey` is decoded here (transport concern). Unsupported key types return
/// an error matching the original request id.
async fn handle_state_request_with_side_inputs(
    backend: &StateBackend,
    side_inputs: Option<&SideInputResolver>,
    request: StateRequest,
) -> std::result::Result<StateResponse, (String, String)> {
    let id = request.id.clone();
    let state_key = request.state_key.and_then(|k| k.r#type);
    let body = request.request;

    let target = match state_key {
        Some(key) => match state_target(key) {
            Ok(target) => target,
            Err(message) => return Err((id, message)),
        },
        None => return Err((id, "state request has no state key".to_string())),
    };

    match target {
        StateTarget::UserState(address) => {
            handle_user_state_request(backend, id, address, body).await
        }
        StateTarget::IterableSideInput(address) => {
            handle_iterable_side_input_request(side_inputs, id, address, body).await
        }
    }
}

async fn handle_user_state_request(
    backend: &StateBackend,
    id: String,
    address: UserStateAddress,
    body: Option<state_request::Request>,
) -> std::result::Result<StateResponse, (String, String)> {
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

async fn handle_iterable_side_input_request(
    side_inputs: Option<&SideInputResolver>,
    id: String,
    address: IterableSideInputAddress,
    body: Option<state_request::Request>,
) -> std::result::Result<StateResponse, (String, String)> {
    match body {
        Some(state_request::Request::Get(get)) => {
            if !get.continuation_token.is_empty() {
                return Err((id, "continuation tokens are not supported".to_string()));
            }
            let Some(side_inputs) = side_inputs else {
                return Err((
                    id,
                    "iterable side-input resolver is not initialized".to_string(),
                ));
            };
            match side_inputs.read_iterable(&address).await {
                Ok(data) => Ok(StateResponse {
                    id,
                    error: String::new(),
                    response: Some(state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data,
                    })),
                }),
                Err(e) => Err((id, format!("get iterable side input failed: {e}"))),
            }
        }
        Some(state_request::Request::Append(_)) => Err((
            id,
            "append is not supported for iterable side inputs".to_string(),
        )),
        Some(state_request::Request::Clear(_)) => Err((
            id,
            "clear is not supported for iterable side inputs".to_string(),
        )),
        None => Err((id, "state request has no request body".to_string())),
    }
}

/// Decode a Beam `StateKey` into a supported state target.
fn state_target(key: state_key::Type) -> std::result::Result<StateTarget, String> {
    match key {
        state_key::Type::BagUserState(bag) => Ok(StateTarget::UserState(UserStateAddress::new(
            UserStateKind::Bag,
            bag.transform_id,
            bag.user_state_id,
            bag.window,
            bag.key,
        ))),
        state_key::Type::IterableSideInput(side_input) => {
            Ok(StateTarget::IterableSideInput(IterableSideInputAddress {
                key: SideInputKey {
                    transform_id: side_input.transform_id,
                    side_input_id: side_input.side_input_id,
                },
                window: side_input.window,
            }))
        }
        other => Err(format!(
            "unsupported state key type '{}' (supported types: '{}' and 'iterable_side_input')",
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
    use crate::{
        coders::primitives::{LengthPrefixCoder, PaneInfo, StringUtf8Coder},
        store::{
            element_store::FlareElementStore,
            record::{BeamRecord, PrimitiveValue},
        },
    };
    use beam_model_rs::v1::{StateKey, state_key};
    use bytes::BytesMut;
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
        let backend = build_state_backend(Arc::new(store))
            .await
            .expect("failed to build the state backend");
        (dir, backend)
    }

    async fn make_side_input_resolver(
        element_coder: StandardBeamCoders,
    ) -> (tempfile::TempDir, Arc<FlareElementStore>, SideInputResolver) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = Arc::new(
            FlareElementStore::new(warehouse, "testdb".to_string(), None)
                .await
                .expect("failed to construct FlareElementStore"),
        );
        let key = SideInputKey {
            transform_id: "select".to_string(),
            side_input_id: "max-price".to_string(),
        };
        let registry = SideInputRegistry::from_entries([(
            key,
            SideInputSpec {
                pcollection_id: "side-input-pc".to_string(),
                element_coder,
                window_coder: WindowCoder::Interval,
            },
        )]);
        let resolver = SideInputResolver::new(store.clone(), registry);
        (dir, store, resolver)
    }

    fn iterable_side_input_request(
        id: &str,
        window: &[u8],
        op: state_request::Request,
    ) -> StateRequest {
        StateRequest {
            id: id.to_string(),
            instruction_id: "instruction".to_string(),
            state_key: Some(StateKey {
                r#type: Some(state_key::Type::IterableSideInput(
                    state_key::IterableSideInput {
                        transform_id: "select".to_string(),
                        side_input_id: "max-price".to_string(),
                        window: window.to_vec(),
                    },
                )),
            }),
            request: Some(op),
        }
    }

    fn encoded_window(window: &BeamWindow) -> Vec<u8> {
        let mut encoded = BytesMut::new();
        WindowCoder::Interval.encode_window(window, &mut encoded);
        encoded.to_vec()
    }

    fn windowed(value: BeamRecord, window: BeamWindow) -> crate::coders::primitives::WindowedValue {
        crate::coders::primitives::WindowedValue {
            value,
            timestamp_millis: 0,
            windows: vec![window],
            pane: PaneInfo::no_firing(),
        }
    }

    #[tokio::test]
    async fn state_lands_in_slate_db_and_survives_the_exit_flush() {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = Arc::new(
            FlareElementStore::new(warehouse, "testdb".to_string(), None)
                .await
                .expect("failed to construct FlareElementStore"),
        );

        let backend = build_state_backend(store.clone()).await.unwrap();
        handle_state_request(&backend, bag_request("1", b"w", b"k", append_op(b"state")))
            .await
            .unwrap();

        // The driver commits the write-back cache when the state stream ends.
        flush_state_on_exit(&backend).await;
        drop(backend);

        // The cells live in the job's SlateDB store.
        let db_dir = slate_state_dir(&store).join("state");
        assert!(
            db_dir.is_dir(),
            "expected the SlateDB state store at {}",
            db_dir.display()
        );
        assert!(
            std::fs::read_dir(&db_dir)
                .expect("failed to list the SlateDB state store")
                .next()
                .is_some(),
            "SlateDB wrote nothing under {}",
            db_dir.display()
        );
        // The element store holds no user-state table.
        assert!(
            store
                .get_existing_table("__flare_state")
                .await
                .unwrap()
                .is_none()
        );

        // A reopened store returns the committed cell.
        let kv = SlateKvStore::open(&slate_state_dir(&store), "state")
            .await
            .expect("failed to reopen the SlateDB state store");
        let reopened = StateBackend::with_store(Arc::new(kv));
        let response = handle_state_request(&reopened, bag_request("2", b"w", b"k", get_op()))
            .await
            .unwrap();
        assert_eq!(get_response_data(response), b"state");
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

    #[tokio::test]
    async fn iterable_side_input_returns_matching_window_in_non_nested_encoding() {
        let (_dir, store, resolver) =
            make_side_input_resolver(StandardBeamCoders::StringUtf8(StringUtf8Coder)).await;
        let first = BeamWindow::Interval {
            start_millis: 0,
            end_millis: 1_000,
        };
        let second = BeamWindow::Interval {
            start_millis: 1_000,
            end_millis: 2_000,
        };
        store
            .write_windowed_value_batch(
                "side-input-pc",
                vec![
                    windowed(
                        BeamRecord::PRIMITIVE(PrimitiveValue::String("one".to_string())),
                        first.clone(),
                    ),
                    windowed(
                        BeamRecord::PRIMITIVE(PrimitiveValue::String("two".to_string())),
                        first.clone(),
                    ),
                    windowed(
                        BeamRecord::PRIMITIVE(PrimitiveValue::String("other".to_string())),
                        second,
                    ),
                ],
            )
            .await
            .unwrap();

        let backend = build_state_backend(store).await.unwrap();
        let response = handle_state_request_with_side_inputs(
            &backend,
            Some(&resolver),
            iterable_side_input_request("side-1", &encoded_window(&first), get_op()),
        )
        .await
        .unwrap();

        assert_eq!(get_response_data(response), b"onetwo");
    }

    #[tokio::test]
    async fn iterable_side_input_supports_length_prefixed_opaque_values() {
        let (_dir, store, resolver) =
            make_side_input_resolver(StandardBeamCoders::LengthPrefix(LengthPrefixCoder)).await;
        let window = BeamWindow::Interval {
            start_millis: 0,
            end_millis: 1_000,
        };
        store
            .write_windowed_value_batch(
                "side-input-pc",
                vec![windowed(
                    BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(vec![0x2a])),
                    window.clone(),
                )],
            )
            .await
            .unwrap();

        let backend = build_state_backend(store).await.unwrap();
        let response = handle_state_request_with_side_inputs(
            &backend,
            Some(&resolver),
            iterable_side_input_request("side-2", &encoded_window(&window), get_op()),
        )
        .await
        .unwrap();

        assert_eq!(get_response_data(response), vec![1, 0x2a]);
    }

    #[tokio::test]
    async fn iterable_side_input_missing_window_is_empty_and_mutation_is_rejected() {
        let (_dir, store, resolver) =
            make_side_input_resolver(StandardBeamCoders::StringUtf8(StringUtf8Coder)).await;
        let present = BeamWindow::Interval {
            start_millis: 0,
            end_millis: 1_000,
        };
        let missing = BeamWindow::Interval {
            start_millis: 2_000,
            end_millis: 3_000,
        };
        store
            .write_windowed_value_batch(
                "side-input-pc",
                vec![windowed(
                    BeamRecord::PRIMITIVE(PrimitiveValue::String("value".to_string())),
                    present,
                )],
            )
            .await
            .unwrap();

        let backend = build_state_backend(store).await.unwrap();
        let response = handle_state_request_with_side_inputs(
            &backend,
            Some(&resolver),
            iterable_side_input_request("side-3", &encoded_window(&missing), get_op()),
        )
        .await
        .unwrap();
        assert!(get_response_data(response).is_empty());

        let (id, error) = handle_state_request_with_side_inputs(
            &backend,
            Some(&resolver),
            iterable_side_input_request(
                "side-4",
                &encoded_window(&missing),
                append_op(b"not-allowed"),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(id, "side-4");
        assert!(error.contains("append is not supported"), "{error}");
    }
}
