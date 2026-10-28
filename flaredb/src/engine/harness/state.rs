use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{
    Coder, Components, ParDoPayload, StateAppendResponse, StateClearResponse, StateGetResponse,
    StateRequest, StateResponse, beam_fn_state_server::BeamFnState, state_key, state_request,
    state_response, state_spec,
};
use bytes::Buf;
use log::{debug, error, info, warn};
use prost::Message;
use tokio::sync::{
    Mutex,
    mpsc::{self},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

use super::HarnessStatus;

use crate::{
    coders::{
        StandardBeamCoders,
        primitives::{BeamWindow, IterableCoder, WindowCoder},
    },
    engine::{
        kv::SlateKvStore,
        runtime::resolve_window_coder,
        state::{
            StateBackend, UserStateAddress, UserStateKind, UserStateStore,
            multimap::split_encoded_values,
        },
    },
    fusion::{pipeline::ExecutableGraph, refs::SideInputRef},
    jobservice::urns::beam_urns,
    store::{
        element_store::{FlareElementStore, ScanCollectionRequest, slate_state_dir},
        record::{BeamRecord, IterableValue},
    },
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

pub async fn start_state_server(
    status: HarnessStatus,
) -> Result<(StateChannel, FlareStateService)> {
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
        status,
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
enum SideInputMaterialization {
    Iterable {
        element_coder: StandardBeamCoders,
    },
    Multimap {
        key_coder: StandardBeamCoders,
        value_coder: StandardBeamCoders,
    },
}

#[derive(Debug, Clone)]
struct SideInputSpec {
    pcollection_id: String,
    materialization: SideInputMaterialization,
    window_coder: WindowCoder,
}

/// Fn State lookup metadata for materialized side inputs in a prepared pipeline.
#[derive(Debug, Clone, Default)]
pub struct SideInputRegistry {
    specs: Arc<HashMap<SideInputKey, SideInputSpec>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UserStateKey {
    transform_id: String,
    user_state_id: String,
}

#[derive(Debug, Clone)]
struct UserStateCoders {
    key: StandardBeamCoders,
    value: StandardBeamCoders,
}

/// Resolve SDK `MapState` key/value coders from each ParDo's `MapSpec`.
#[derive(Debug, Clone, Default)]
pub struct UserStateRegistry {
    map_coders: Arc<HashMap<UserStateKey, UserStateCoders>>,
}

impl UserStateRegistry {
    pub fn from_components(
        components: &Components,
        pipeline_coders: &HashMap<String, Coder>,
    ) -> Result<Self> {
        let mut map_coders = HashMap::new();
        for (transform_id, transform) in &components.transforms {
            let Some(spec) = transform.spec.as_ref() else {
                continue;
            };
            if spec.urn != beam_urns::PAR_DO_TRANSFORM {
                continue;
            }
            let payload = ParDoPayload::decode(spec.payload.as_slice()).map_err(|err| {
                anyhow!("failed to decode ParDoPayload for transform '{transform_id}': {err}")
            })?;
            for (user_state_id, state) in payload.state_specs {
                let (key_coder_id, value_coder_id) = match state.spec {
                    Some(state_spec::Spec::MapSpec(map)) => (map.key_coder_id, map.value_coder_id),
                    Some(state_spec::Spec::MultimapSpec(map)) => {
                        (map.key_coder_id, map.value_coder_id)
                    }
                    _ => continue,
                };
                map_coders.insert(
                    UserStateKey {
                        transform_id: transform_id.clone(),
                        user_state_id,
                    },
                    UserStateCoders {
                        key: StandardBeamCoders::from_urn(
                            &key_coder_id,
                            None,
                            Some(pipeline_coders),
                        ),
                        value: StandardBeamCoders::from_urn(
                            &value_coder_id,
                            None,
                            Some(pipeline_coders),
                        ),
                    },
                );
            }
        }
        Ok(Self {
            map_coders: Arc::new(map_coders),
        })
    }

    fn map_coders(&self, transform_id: &str, user_state_id: &str) -> Option<&UserStateCoders> {
        self.map_coders.get(&UserStateKey {
            transform_id: transform_id.to_string(),
            user_state_id: user_state_id.to_string(),
        })
    }
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

        let mut worker_stages = Vec::new();
        for node in executable.node_weights() {
            worker_stages.extend(node.worker_stages());
        }

        for stage in worker_stages {
            for side_input in stage.side_inputs() {
                let pcollection = side_input.collection();
                let pcollection_id = pcollection.id().clone();
                if !produced_pcollections.contains(&pcollection_id) {
                    return Err(anyhow!(
                        "side input '{}' on transform '{}' references PCollection '{}' with no executable producer",
                        side_input.local_name(),
                        side_input.transform().id(),
                        pcollection_id
                    ));
                }
                let coder_id = pcollection.node().coder_id.clone();
                let access_pattern = side_input_access_pattern(&side_input)?;
                let materialization = match access_pattern.as_str() {
                    beam_urns::ITERABLE_SIDE_INPUT_ACCESS => SideInputMaterialization::Iterable {
                        element_coder: StandardBeamCoders::from_urn(
                            &coder_id,
                            None,
                            Some(pipeline_coders),
                        ),
                    },
                    beam_urns::MULTIMAP_SIDE_INPUT_ACCESS => {
                        let (key_coder_id, value_coder_id) =
                            multimap_coder_components(&coder_id, pipeline_coders)?;
                        SideInputMaterialization::Multimap {
                            key_coder: StandardBeamCoders::from_urn(
                                &key_coder_id,
                                None,
                                Some(pipeline_coders),
                            ),
                            value_coder: StandardBeamCoders::from_urn(
                                &value_coder_id,
                                None,
                                Some(pipeline_coders),
                            ),
                        }
                    }
                    other => {
                        return Err(anyhow!(
                            "unsupported side-input access pattern '{other}' on transform '{}' side input '{}'",
                            side_input.transform().id(),
                            side_input.local_name()
                        ));
                    }
                };
                let window_coder =
                    resolve_window_coder(components, pipeline_coders, &pcollection_id)?;
                let key = SideInputKey {
                    transform_id: side_input.transform().id().clone(),
                    side_input_id: side_input.local_name().to_string(),
                };
                let spec = SideInputSpec {
                    pcollection_id,
                    materialization,
                    window_coder,
                };
                if let Some(existing) = specs.insert(key.clone(), spec) {
                    return Err(anyhow!(
                        "duplicate side input reference for transform '{}' and side input '{}' (PCollections '{}' and '{}')",
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

fn side_input_access_pattern(side_input: &SideInputRef) -> Result<String> {
    let transform = side_input.transform().node();
    let spec = transform.spec.as_ref().ok_or_else(|| {
        anyhow!(
            "side-input consumer transform '{}' has no function spec",
            side_input.transform().id()
        )
    })?;
    let payload = ParDoPayload::decode(spec.payload.as_slice()).map_err(|e| {
        anyhow!(
            "failed to decode side-input consumer payload for transform '{}': {e}",
            side_input.transform().id()
        )
    })?;
    payload
        .side_inputs
        .get(side_input.local_name())
        .and_then(|side| side.access_pattern.as_ref())
        .map(|access_pattern| access_pattern.urn.clone())
        .ok_or_else(|| {
            anyhow!(
                "side input '{}' on transform '{}' has no access pattern",
                side_input.local_name(),
                side_input.transform().id()
            )
        })
}

fn multimap_coder_components(
    coder_id: &str,
    pipeline_coders: &HashMap<String, Coder>,
) -> Result<(String, String)> {
    let coder = pipeline_coders
        .get(coder_id)
        .ok_or_else(|| anyhow!("multimap side-input coder '{coder_id}' was not found"))?;
    let urn = coder
        .spec
        .as_ref()
        .map(|spec| spec.urn.as_str())
        .unwrap_or(coder_id);
    if urn != beam_urns::KV_CODER {
        return Err(anyhow!(
            "multimap side input requires a KV coder, found '{urn}' for coder '{coder_id}'"
        ));
    }
    match coder.component_coder_ids.as_slice() {
        [key, value] => Ok((key.clone(), value.clone())),
        components => Err(anyhow!(
            "multimap side-input KV coder '{coder_id}' has {} component coder(s), expected 2",
            components.len()
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IterableSideInputAddress {
    key: SideInputKey,
    window: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum MultimapReadKind {
    Keys,
    Values(Vec<u8>),
    KeysValues,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MultimapSideInputAddress {
    key: SideInputKey,
    window: Vec<u8>,
    kind: MultimapReadKind,
}

#[derive(Debug, Clone)]
enum EncodedSideInputWindow {
    Iterable(Vec<u8>),
    Multimap {
        keys: Vec<u8>,
        values: HashMap<Vec<u8>, Vec<u8>>,
    },
}

type EncodedSideInputWindows = HashMap<BeamWindow, EncodedSideInputWindow>;
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

    async fn cached_windows(
        &self,
        key: &SideInputKey,
        spec: &SideInputSpec,
    ) -> Result<Arc<EncodedSideInputWindows>> {
        let mut cache = self.cache.lock().await;
        if let Some(windows) = cache.get(key) {
            return Ok(windows.clone());
        }

        let values = self
            .store
            .scan_windowed_values(ScanCollectionRequest {
                pcollection_id: spec.pcollection_id.clone(),
            })
            .await?;
        let windows = match &spec.materialization {
            SideInputMaterialization::Iterable { element_coder } => {
                encode_iterable_windows(values, element_coder)?
            }
            SideInputMaterialization::Multimap {
                key_coder,
                value_coder,
            } => encode_multimap_windows(values, key_coder, value_coder)?,
        };
        let windows = Arc::new(windows);
        cache.insert(key.clone(), windows.clone());
        Ok(windows)
    }

    async fn read_iterable(&self, address: &IterableSideInputAddress) -> Result<Vec<u8>> {
        let spec = self.registry.get(&address.key).ok_or_else(|| {
            anyhow!(
                "unknown iterable side input transform_id='{}' side_input_id='{}'",
                address.key.transform_id,
                address.key.side_input_id
            )
        })?;
        if !matches!(
            &spec.materialization,
            SideInputMaterialization::Iterable { .. }
        ) {
            return Err(anyhow!(
                "side input transform_id='{}' side_input_id='{}' is not iterable",
                address.key.transform_id,
                address.key.side_input_id
            ));
        }
        let window = spec
            .window_coder
            .decode_window(&address.window)
            .map_err(|e| anyhow!("invalid side-input window: {e}"))?;
        let windows = self.cached_windows(&address.key, spec).await?;
        Ok(match windows.get(&window) {
            Some(EncodedSideInputWindow::Iterable(data)) => data.clone(),
            Some(EncodedSideInputWindow::Multimap { .. }) => {
                return Err(anyhow!(
                    "side input transform_id='{}' side_input_id='{}' is not iterable",
                    address.key.transform_id,
                    address.key.side_input_id
                ));
            }
            None => Vec::new(),
        })
    }

    async fn read_multimap(&self, address: &MultimapSideInputAddress) -> Result<Vec<u8>> {
        let spec = self.registry.get(&address.key).ok_or_else(|| {
            anyhow!(
                "unknown multimap side input transform_id='{}' side_input_id='{}'",
                address.key.transform_id,
                address.key.side_input_id
            )
        })?;
        let SideInputMaterialization::Multimap {
            key_coder,
            value_coder,
        } = &spec.materialization
        else {
            return Err(anyhow!(
                "side input transform_id='{}' side_input_id='{}' is not a multimap",
                address.key.transform_id,
                address.key.side_input_id
            ));
        };
        let window = spec
            .window_coder
            .decode_window(&address.window)
            .map_err(|e| anyhow!("invalid side-input window: {e}"))?;
        let windows = self.cached_windows(&address.key, spec).await?;
        let Some(EncodedSideInputWindow::Multimap { keys, values }) = windows.get(&window) else {
            return Ok(Vec::new());
        };
        match &address.kind {
            MultimapReadKind::Keys => Ok(keys.clone()),
            MultimapReadKind::Values(key) => Ok(values.get(key).cloned().unwrap_or_default()),
            MultimapReadKind::KeysValues => {
                encode_multimap_key_values(keys, values, key_coder, value_coder)
            }
        }
    }
}

fn encode_multimap_key_values(
    encoded_keys: &[u8],
    values_by_key: &HashMap<Vec<u8>, Vec<u8>>,
    key_coder: &StandardBeamCoders,
    value_coder: &StandardBeamCoders,
) -> Result<Vec<u8>> {
    let value_iterable_coder =
        StandardBeamCoders::Iterable(IterableCoder::new(value_coder.clone()));
    let mut encoded = Vec::new();
    let mut keys = encoded_keys;

    while keys.has_remaining() {
        let key = key_coder
            .decode(&mut keys)
            .map_err(|e| anyhow!("failed to decode cached multimap key: {e}"))?;
        let mut encoded_key = Vec::new();
        append_encoded_element(key_coder, key.clone(), &mut encoded_key);
        let encoded_values = values_by_key.get(&encoded_key).ok_or_else(|| {
            anyhow!("cached multimap key has no corresponding encoded value list")
        })?;

        let mut values = encoded_values.as_slice();
        let mut decoded_values = Vec::new();
        while values.has_remaining() {
            decoded_values.push(
                value_coder
                    .decode(&mut values)
                    .map_err(|e| anyhow!("failed to decode cached multimap value: {e}"))?,
            );
        }

        append_encoded_element(key_coder, key, &mut encoded);
        append_encoded_element(
            &value_iterable_coder,
            BeamRecord::ITERABLE(IterableValue::from_records(decoded_values)),
            &mut encoded,
        );
    }

    Ok(encoded)
}

fn encode_iterable_windows(
    values: Vec<crate::coders::primitives::WindowedValue>,
    element_coder: &StandardBeamCoders,
) -> Result<EncodedSideInputWindows> {
    let mut grouped = EncodedSideInputWindows::new();
    for value in values {
        let mut seen_windows = HashSet::new();
        for window in value.windows {
            if !seen_windows.insert(window.clone()) {
                continue;
            }
            let entry = grouped
                .entry(window)
                .or_insert_with(|| EncodedSideInputWindow::Iterable(Vec::new()));
            let EncodedSideInputWindow::Iterable(encoded) = entry else {
                return Err(anyhow!(
                    "side-input PCollection mixed iterable and multimap values"
                ));
            };
            append_encoded_element(element_coder, value.value.clone(), encoded);
        }
    }
    Ok(grouped)
}

fn encode_multimap_windows(
    values: Vec<crate::coders::primitives::WindowedValue>,
    key_coder: &StandardBeamCoders,
    value_coder: &StandardBeamCoders,
) -> Result<EncodedSideInputWindows> {
    let mut grouped = EncodedSideInputWindows::new();
    for value in values {
        let BeamRecord::KV(kv) = value.value else {
            return Err(anyhow!(
                "multimap side input requires KV values, found a different BeamRecord"
            ));
        };
        let value_windows = value.windows;
        let key = kv.key;
        let value = *kv.value;
        let mut seen_windows = HashSet::new();
        for window in value_windows {
            if !seen_windows.insert(window.clone()) {
                continue;
            }
            let entry = grouped
                .entry(window)
                .or_insert_with(|| EncodedSideInputWindow::Multimap {
                    keys: Vec::new(),
                    values: HashMap::new(),
                });
            let EncodedSideInputWindow::Multimap { keys, values } = entry else {
                return Err(anyhow!(
                    "side-input PCollection mixed iterable and multimap values"
                ));
            };

            let mut encoded_key = Vec::new();
            append_encoded_element(
                key_coder,
                BeamRecord::PRIMITIVE(key.clone()),
                &mut encoded_key,
            );
            if !values.contains_key(&encoded_key) {
                keys.extend_from_slice(&encoded_key);
            }
            let encoded_values = values.entry(encoded_key).or_default();
            append_encoded_element(value_coder, value.clone(), encoded_values);
        }
    }
    Ok(grouped)
}

fn append_encoded_element(coder: &StandardBeamCoders, value: BeamRecord, encoded: &mut Vec<u8>) {
    let before = encoded.len();
    coder.encode(value, encoded);
    // Beam's data stream decoder advances one byte when a coder decodes to zero
    // bytes, so preserve that element boundary.
    if encoded.len() == before {
        encoded.push(0);
    }
}

/// Client-side channel for the executor to service the worker's State stream.
#[derive(Clone)]
pub struct StateChannel {
    stream: Arc<StateInner>,
    /// Handle to the background state request driver task.
    /// Aborted on reset to prevent a stale task from consuming a new stream.
    stream_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    status: HarnessStatus,
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
    pub fn stream_requests(
        &self,
        store: Arc<FlareElementStore>,
        side_inputs: SideInputRegistry,
        user_states: UserStateRegistry,
    ) {
        let channel = self.clone();
        let task_slot = self.stream_task.clone();

        let join_handle = tokio::spawn(async move {
            match build_state_backend(store.clone()).await {
                Ok(backend) => {
                    let side_inputs = SideInputResolver::new(store, side_inputs);
                    channel
                        .drive_state_requests(backend, side_inputs, user_states)
                        .await
                }
                Err(e) => {
                    if channel.status.is_intentional_shutdown() {
                        debug!("state backend construction ended during worker shutdown: {e}");
                    } else {
                        error!("failed to construct state backend: {e}");
                        channel.status.report_failure("BeamFnState backend", &e);
                    }
                }
            }
        });

        *task_slot.lock().unwrap() = Some(join_handle);
    }

    async fn drive_state_requests(
        &self,
        backend: StateBackend,
        side_inputs: SideInputResolver,
        user_states: UserStateRegistry,
    ) {
        info!("state request driver started");
        loop {
            match self.recv_request().await {
                Ok(request) => {
                    crate::engine::guard::touch();
                    let summary = describe_request(&request);
                    let started = std::time::Instant::now();
                    let response = handle_state_request_with_registries(
                        &backend,
                        Some(&user_states),
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
                                if self.status.is_intentional_shutdown() {
                                    debug!(
                                        "state response closed during intentional worker shutdown: {}",
                                        e
                                    );
                                } else {
                                    warn!("failed to send state response (worker gone?): {}", e);
                                    self.status.report_failure("BeamFnState", &e);
                                }
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
                                if self.status.is_intentional_shutdown() {
                                    debug!(
                                        "state error response closed during intentional worker shutdown: {}",
                                        e
                                    );
                                } else {
                                    warn!(
                                        "failed to send state error response (worker gone?): {}",
                                        e
                                    );
                                    self.status.report_failure("BeamFnState", &e);
                                }
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    if self.status.is_intentional_shutdown() {
                        debug!(
                            "state request stream ended during intentional worker shutdown: {e}"
                        );
                    } else {
                        warn!("BeamFnState stream stopped before the job completed: {e}");
                        self.status.report_failure("BeamFnState", &e);
                    }
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
        Some(state_key::Type::MultimapKeysUserState(state)) => format!(
            "multimap_keys_user_state(transform_id={}, user_state_id={}, window={}B, key={}B)",
            state.transform_id,
            state.user_state_id,
            state.window.len(),
            state.key.len()
        ),
        Some(state_key::Type::MultimapEntriesUserState(state)) => format!(
            "multimap_entries_user_state(transform_id={}, user_state_id={}, window={}B, key={}B)",
            state.transform_id,
            state.user_state_id,
            state.window.len(),
            state.key.len()
        ),
        Some(state_key::Type::MultimapUserState(state)) => format!(
            "multimap_user_state(transform_id={}, user_state_id={}, window={}B, key={}B, map_key={}B)",
            state.transform_id,
            state.user_state_id,
            state.window.len(),
            state.key.len(),
            state.map_key.len()
        ),
        Some(state_key::Type::IterableSideInput(side_input)) => format!(
            "iterable_side_input(transform_id={}, side_input_id={}, window={}B)",
            side_input.transform_id,
            side_input.side_input_id,
            side_input.window.len()
        ),
        Some(state_key::Type::MultimapKeysSideInput(side_input)) => format!(
            "multimap_keys_side_input(transform_id={}, side_input_id={}, window={}B)",
            side_input.transform_id,
            side_input.side_input_id,
            side_input.window.len()
        ),
        Some(state_key::Type::MultimapSideInput(side_input)) => format!(
            "multimap_side_input(transform_id={}, side_input_id={}, window={}B, key={}B)",
            side_input.transform_id,
            side_input.side_input_id,
            side_input.window.len(),
            side_input.key.len()
        ),
        Some(state_key::Type::MultimapKeysValuesSideInput(side_input)) => format!(
            "multimap_keys_values_side_input(transform_id={}, side_input_id={}, window={}B)",
            side_input.transform_id,
            side_input.side_input_id,
            side_input.window.len()
        ),
        Some(_) => "unsupported-state-key".to_string(),
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
    MultimapSideInput(MultimapSideInputAddress),
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
    handle_state_request_with_registries(backend, None, None, request).await
}

/// Dispatch a single [`StateRequest`] through either user state or an iterable
/// side-input lookup.
///
/// `StateKey` is decoded here (transport concern). Unsupported key types return
/// an error matching the original request id.
#[cfg(test)]
async fn handle_state_request_with_side_inputs(
    backend: &StateBackend,
    side_inputs: Option<&SideInputResolver>,
    request: StateRequest,
) -> std::result::Result<StateResponse, (String, String)> {
    handle_state_request_with_registries(backend, None, side_inputs, request).await
}

async fn handle_state_request_with_registries(
    backend: &StateBackend,
    user_states: Option<&UserStateRegistry>,
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
            handle_user_state_request(backend, user_states, id, address, body).await
        }
        StateTarget::IterableSideInput(address) => {
            handle_iterable_side_input_request(side_inputs, id, address, body).await
        }
        StateTarget::MultimapSideInput(address) => {
            handle_multimap_side_input_request(side_inputs, id, address, body).await
        }
    }
}

async fn handle_user_state_request(
    backend: &StateBackend,
    user_states: Option<&UserStateRegistry>,
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
        Some(state_request::Request::Append(append)) => {
            let append_result = match user_state.address().kind {
                UserStateKind::Bag => user_state.append(&append.data).await,
                UserStateKind::MultimapKeys => {
                    let Some(coders) = user_states.and_then(|registry| {
                        registry.map_coders(
                            &user_state.address().transform_id,
                            &user_state.address().user_state_id,
                        )
                    }) else {
                        return Err((id, "multimap state key coder is not registered".to_string()));
                    };
                    let keys =
                        split_encoded_values(&coders.key, &append.data).unwrap_or_else(|err| {
                            debug!("treating opaque multimap key append as one key: {err}");
                            vec![append.data.clone()]
                        });
                    user_state.append_multimap_keys(&keys).await
                }
                UserStateKind::MultimapValues => {
                    let Some(coders) = user_states.and_then(|registry| {
                        registry.map_coders(
                            &user_state.address().transform_id,
                            &user_state.address().user_state_id,
                        )
                    }) else {
                        return Err((
                            id,
                            "multimap state value coder is not registered".to_string(),
                        ));
                    };
                    let values =
                        split_encoded_values(&coders.value, &append.data).unwrap_or_else(|err| {
                            debug!("treating opaque multimap value append as one value: {err}");
                            vec![append.data.clone()]
                        });
                    user_state.append_multimap_values(&values).await
                }
                UserStateKind::MultimapEntries => Err(anyhow!(
                    "append is not supported for multimap entries state"
                )),
            };
            match append_result {
                Ok(()) => Ok(StateResponse {
                    id,
                    error: String::new(),
                    response: Some(state_response::Response::Append(StateAppendResponse {})),
                }),
                Err(e) => Err((id, format!("append state failed: {e}"))),
            }
        }
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

async fn handle_multimap_side_input_request(
    side_inputs: Option<&SideInputResolver>,
    id: String,
    address: MultimapSideInputAddress,
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
                    "multimap side-input resolver is not initialized".to_string(),
                ));
            };
            match side_inputs.read_multimap(&address).await {
                Ok(data) => Ok(StateResponse {
                    id,
                    error: String::new(),
                    response: Some(state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data,
                    })),
                }),
                Err(e) => Err((id, format!("get multimap side input failed: {e}"))),
            }
        }
        Some(state_request::Request::Append(_)) => Err((
            id,
            "append is not supported for multimap side inputs".to_string(),
        )),
        Some(state_request::Request::Clear(_)) => Err((
            id,
            "clear is not supported for multimap side inputs".to_string(),
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
        state_key::Type::MultimapKeysUserState(state) => {
            Ok(StateTarget::UserState(UserStateAddress::new_multimap(
                UserStateKind::MultimapKeys,
                state.transform_id,
                state.user_state_id,
                state.window,
                state.key,
                Vec::new(),
            )))
        }
        state_key::Type::MultimapEntriesUserState(state) => {
            Ok(StateTarget::UserState(UserStateAddress::new_multimap(
                UserStateKind::MultimapEntries,
                state.transform_id,
                state.user_state_id,
                state.window,
                state.key,
                Vec::new(),
            )))
        }
        state_key::Type::MultimapUserState(state) => {
            Ok(StateTarget::UserState(UserStateAddress::new_multimap(
                UserStateKind::MultimapValues,
                state.transform_id,
                state.user_state_id,
                state.window,
                state.key,
                state.map_key,
            )))
        }
        state_key::Type::IterableSideInput(side_input) => {
            Ok(StateTarget::IterableSideInput(IterableSideInputAddress {
                key: SideInputKey {
                    transform_id: side_input.transform_id,
                    side_input_id: side_input.side_input_id,
                },
                window: side_input.window,
            }))
        }
        state_key::Type::MultimapKeysSideInput(side_input) => {
            Ok(StateTarget::MultimapSideInput(MultimapSideInputAddress {
                key: SideInputKey {
                    transform_id: side_input.transform_id,
                    side_input_id: side_input.side_input_id,
                },
                window: side_input.window,
                kind: MultimapReadKind::Keys,
            }))
        }
        state_key::Type::MultimapSideInput(side_input) => {
            Ok(StateTarget::MultimapSideInput(MultimapSideInputAddress {
                key: SideInputKey {
                    transform_id: side_input.transform_id,
                    side_input_id: side_input.side_input_id,
                },
                window: side_input.window,
                kind: MultimapReadKind::Values(side_input.key),
            }))
        }
        state_key::Type::MultimapKeysValuesSideInput(side_input) => {
            Ok(StateTarget::MultimapSideInput(MultimapSideInputAddress {
                key: SideInputKey {
                    transform_id: side_input.transform_id,
                    side_input_id: side_input.side_input_id,
                },
                window: side_input.window,
                kind: MultimapReadKind::KeysValues,
            }))
        }
        other => Err(format!(
            "unsupported state key type '{}' (supported types: '{}', 'multimap_keys_user_state', 'multimap_entries_user_state', 'multimap_user_state', 'iterable_side_input', 'multimap_keys_side_input', 'multimap_side_input', and 'multimap_keys_values_side_input')",
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
        coders::primitives::{LengthPrefixCoder, PaneInfo, StringUtf8Coder, VarIntCoder},
        fusion::{
            pipeline::{ConsumerMetaData, ExecutableNode, PCollectionNode, PTransformNode},
            stage::{
                ExecutableStage, SplittableExecutionPlan, SplittableProcessKind, SplittableStage,
            },
        },
        store::{
            element_store::FlareElementStore,
            record::{BeamKV, BeamRecord, PrimitiveValue},
        },
    };
    use beam_model_rs::v1::executable_stage_payload::SideInputId;
    use beam_model_rs::v1::{
        Environment, FunctionSpec, PCollection, PTransform, SideInput, StateKey, state_key,
    };
    use bytes::BytesMut;
    use indexmap::IndexSet;
    use petgraph::Graph;
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

    #[test]
    fn side_input_registry_includes_worker_stages_nested_in_splittable_runs() {
        let pair_transform = PTransform {
            unique_name: "pair".to_string(),
            spec: Some(FunctionSpec {
                urn: beam_urns::SPLITTABLE_PAIR_WITH_RESTRICTION_URN.to_string(),
                payload: ParDoPayload {
                    side_inputs: HashMap::from([(
                        "side".to_string(),
                        SideInput {
                            access_pattern: Some(FunctionSpec {
                                urn: beam_urns::ITERABLE_SIDE_INPUT_ACCESS.to_string(),
                                payload: Vec::new(),
                            }),
                            view_fn: None,
                            window_mapping_fn: None,
                        },
                    )]),
                    ..Default::default()
                }
                .encode_to_vec(),
            }),
            inputs: HashMap::from([
                ("main".to_string(), "main_pc".to_string()),
                ("side".to_string(), "side_pc".to_string()),
            ]),
            outputs: HashMap::from([("out".to_string(), "out_pc".to_string())]),
            environment_id: "env".to_string(),
            ..Default::default()
        };
        let pair_node = PTransformNode {
            id: "pair".to_string(),
            transform: pair_transform.clone(),
        };
        let main_pc = PCollectionNode {
            id: "main_pc".to_string(),
            collection: PCollection {
                unique_name: "main_pc".to_string(),
                coder_id: "string".to_string(),
                ..Default::default()
            },
        };
        let side_pc = PCollectionNode {
            id: "side_pc".to_string(),
            collection: PCollection {
                unique_name: "side_pc".to_string(),
                coder_id: "string".to_string(),
                ..Default::default()
            },
        };
        let out_pc = PCollectionNode {
            id: "out_pc".to_string(),
            collection: PCollection {
                unique_name: "out_pc".to_string(),
                coder_id: "string".to_string(),
                ..Default::default()
            },
        };
        let components = Components {
            transforms: HashMap::from([("pair".to_string(), pair_transform)]),
            pcollections: HashMap::from([
                ("main_pc".to_string(), main_pc.node().clone()),
                ("side_pc".to_string(), side_pc.node().clone()),
                ("out_pc".to_string(), out_pc.node().clone()),
            ]),
            coders: HashMap::from([(
                "string".to_string(),
                Coder {
                    spec: Some(FunctionSpec {
                        urn: beam_urns::STRING_UTF8_CODER.to_string(),
                        payload: Vec::new(),
                    }),
                    component_coder_ids: Vec::new(),
                },
            )]),
            ..Default::default()
        };
        let side_input = SideInputRef::from_id(
            &SideInputId {
                transform_id: "pair".to_string(),
                local_name: "side".to_string(),
            },
            &components,
        )
        .unwrap();
        let pair_stage = ExecutableStage::from(
            components.clone(),
            Environment::default(),
            HashSet::new(),
            main_pc,
            [side_input].into_iter().collect(),
            IndexSet::new(),
            IndexSet::new(),
            [out_pc].into_iter().collect(),
            [pair_node].into_iter().collect(),
        );
        let mut nested = Graph::new();
        nested.add_node(ExecutableNode::Worker(pair_stage.clone()));
        let splittable = SplittableStage::new(
            "splittable".to_string(),
            nested,
            HashSet::new(),
            SplittableExecutionPlan {
                initialization_stages: vec![pair_stage.clone()],
                process_stage: pair_stage,
                process_kind: SplittableProcessKind::ProcessElements,
            },
        );
        let producer_stage = ExecutableStage::from(
            components.clone(),
            Environment::default(),
            HashSet::new(),
            PCollectionNode {
                id: "side_pc".to_string(),
                collection: components.pcollections["side_pc"].clone(),
            },
            IndexSet::new(),
            IndexSet::new(),
            IndexSet::new(),
            [PCollectionNode {
                id: "side_pc".to_string(),
                collection: components.pcollections["side_pc"].clone(),
            }]
            .into_iter()
            .collect(),
            IndexSet::new(),
        );
        let mut executable = Graph::new();
        executable.add_node(ExecutableNode::Worker(producer_stage));
        executable.add_node(ExecutableNode::Splittable(splittable));
        let executable = ExecutableGraph::from_graph_with_components_for_test(
            executable,
            components.clone(),
            ConsumerMetaData {
                producer_transform_id: "producer".to_string(),
                produced_pcol_id: "side_pc".to_string(),
                coder_id: "string".to_string(),
                component_coder: None,
                consumer_transfrom_id: "pair".to_string(),
            },
        );

        let registry =
            SideInputRegistry::from_executable_graph(&executable, &components.coders).unwrap();
        let metadata = executable
            .side_input_output_metadata("side_pc")
            .expect("nested SDF side input must synthesize producer output metadata");
        assert_eq!(metadata.consumer_transfrom_id, "pair");
        assert!(
            registry
                .get(&SideInputKey {
                    transform_id: "pair".to_string(),
                    side_input_id: "side".to_string(),
                })
                .is_some(),
            "nested SDF side inputs must be registered"
        );
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
                materialization: SideInputMaterialization::Iterable { element_coder },
                window_coder: WindowCoder::Interval,
            },
        )]);
        let resolver = SideInputResolver::new(store.clone(), registry);
        (dir, store, resolver)
    }

    async fn make_multimap_resolver()
    -> (tempfile::TempDir, Arc<FlareElementStore>, SideInputResolver) {
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
                materialization: SideInputMaterialization::Multimap {
                    key_coder: StandardBeamCoders::VarInt(VarIntCoder),
                    value_coder: StandardBeamCoders::StringUtf8(StringUtf8Coder),
                },
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

    fn multimap_keys_request(id: &str, window: &[u8]) -> StateRequest {
        StateRequest {
            id: id.to_string(),
            instruction_id: "instruction".to_string(),
            state_key: Some(StateKey {
                r#type: Some(state_key::Type::MultimapKeysSideInput(
                    state_key::MultimapKeysSideInput {
                        transform_id: "select".to_string(),
                        side_input_id: "max-price".to_string(),
                        window: window.to_vec(),
                    },
                )),
            }),
            request: Some(get_op()),
        }
    }

    fn multimap_value_request(id: &str, window: &[u8], key: &[u8]) -> StateRequest {
        StateRequest {
            id: id.to_string(),
            instruction_id: "instruction".to_string(),
            state_key: Some(StateKey {
                r#type: Some(state_key::Type::MultimapSideInput(
                    state_key::MultimapSideInput {
                        transform_id: "select".to_string(),
                        side_input_id: "max-price".to_string(),
                        window: window.to_vec(),
                        key: key.to_vec(),
                    },
                )),
            }),
            request: Some(get_op()),
        }
    }

    fn multimap_keys_values_request(id: &str, window: &[u8]) -> StateRequest {
        StateRequest {
            id: id.to_string(),
            instruction_id: "instruction".to_string(),
            state_key: Some(StateKey {
                r#type: Some(state_key::Type::MultimapKeysValuesSideInput(
                    state_key::MultimapKeysValuesSideInput {
                        transform_id: "select".to_string(),
                        side_input_id: "max-price".to_string(),
                        window: window.to_vec(),
                    },
                )),
            }),
            request: Some(get_op()),
        }
    }

    fn kv_record(key: i64, value: &str) -> BeamRecord {
        BeamRecord::KV(BeamKV {
            key: PrimitiveValue::Int64(key),
            value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::String(
                value.to_string(),
            ))),
        })
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

    fn multimap_state_request(
        id: &str,
        kind: state_key::Type,
        op: state_request::Request,
    ) -> StateRequest {
        StateRequest {
            id: id.to_string(),
            instruction_id: "instruction".to_string(),
            state_key: Some(StateKey { r#type: Some(kind) }),
            request: Some(op),
        }
    }

    fn multimap_keys_state_key() -> state_key::Type {
        state_key::Type::MultimapKeysUserState(state_key::MultimapKeysUserState {
            transform_id: "transform".to_string(),
            user_state_id: "map".to_string(),
            window: b"window".to_vec(),
            key: b"user-key".to_vec(),
        })
    }

    fn multimap_entries_state_key() -> state_key::Type {
        state_key::Type::MultimapEntriesUserState(state_key::MultimapEntriesUserState {
            transform_id: "transform".to_string(),
            user_state_id: "map".to_string(),
            window: b"window".to_vec(),
            key: b"user-key".to_vec(),
        })
    }

    fn multimap_values_state_key(map_key: &[u8]) -> state_key::Type {
        state_key::Type::MultimapUserState(state_key::MultimapUserState {
            transform_id: "transform".to_string(),
            user_state_id: "map".to_string(),
            window: b"window".to_vec(),
            key: b"user-key".to_vec(),
            map_key: map_key.to_vec(),
        })
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
    async fn multimap_user_state_serves_map_state_protocols() {
        let (_dir, backend) = make_backend().await;
        let user_states = UserStateRegistry {
            map_coders: Arc::new(HashMap::from([(
                UserStateKey {
                    transform_id: "transform".to_string(),
                    user_state_id: "map".to_string(),
                },
                UserStateCoders {
                    key: StandardBeamCoders::StringUtf8(StringUtf8Coder),
                    value: StandardBeamCoders::StringUtf8(StringUtf8Coder),
                },
            )])),
        };

        handle_state_request_with_registries(
            &backend,
            Some(&user_states),
            None,
            multimap_state_request(
                "append-keys",
                multimap_keys_state_key(),
                append_op(b"\x01a\x01b"),
            ),
        )
        .await
        .unwrap();
        handle_state_request_with_registries(
            &backend,
            Some(&user_states),
            None,
            multimap_state_request(
                "append-values",
                multimap_values_state_key(b"\x01a"),
                append_op(b"\x03one"),
            ),
        )
        .await
        .unwrap();

        let keys = get_response_data(
            handle_state_request_with_registries(
                &backend,
                Some(&user_states),
                None,
                multimap_state_request("get-keys", multimap_keys_state_key(), get_op()),
            )
            .await
            .unwrap(),
        );
        let values = get_response_data(
            handle_state_request_with_registries(
                &backend,
                Some(&user_states),
                None,
                multimap_state_request("get-values", multimap_values_state_key(b"\x01a"), get_op()),
            )
            .await
            .unwrap(),
        );
        let entries = get_response_data(
            handle_state_request_with_registries(
                &backend,
                Some(&user_states),
                None,
                multimap_state_request("get-entries", multimap_entries_state_key(), get_op()),
            )
            .await
            .unwrap(),
        );

        assert_eq!(keys, b"\x01a\x01b");
        assert_eq!(values, b"\x03one");
        assert_eq!(
            entries,
            b"\x01a\x00\x00\x00\x01\x03one\x01b\x00\x00\x00\x00"
        );
    }

    #[test]
    fn user_state_registry_resolves_map_state_coders() {
        let payload = ParDoPayload {
            state_specs: HashMap::from([(
                "map".to_string(),
                beam_model_rs::v1::StateSpec {
                    spec: Some(state_spec::Spec::MapSpec(beam_model_rs::v1::MapStateSpec {
                        key_coder_id: "key-coder".to_string(),
                        value_coder_id: "value-coder".to_string(),
                    })),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let components = Components {
            transforms: HashMap::from([(
                "map-transform".to_string(),
                beam_model_rs::v1::PTransform {
                    spec: Some(beam_model_rs::v1::FunctionSpec {
                        urn: beam_urns::PAR_DO_TRANSFORM.to_string(),
                        payload: payload.encode_to_vec(),
                    }),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let coders = HashMap::from([
            (
                "key-coder".to_string(),
                Coder {
                    spec: Some(beam_model_rs::v1::FunctionSpec {
                        urn: beam_urns::STRING_UTF8_CODER.to_string(),
                        payload: Vec::new(),
                    }),
                    component_coder_ids: Vec::new(),
                },
            ),
            (
                "value-coder".to_string(),
                Coder {
                    spec: Some(beam_model_rs::v1::FunctionSpec {
                        urn: beam_urns::VARINT_CODER.to_string(),
                        payload: Vec::new(),
                    }),
                    component_coder_ids: Vec::new(),
                },
            ),
        ]);

        let registry = UserStateRegistry::from_components(&components, &coders).unwrap();

        assert!(registry.map_coders("map-transform", "map").is_some());
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
                r#type: Some(state_key::Type::OrderedListUserState(
                    state_key::OrderedListUserState {
                        transform_id: "t".to_string(),
                        user_state_id: "s".to_string(),
                        window: b"w".to_vec(),
                        key: b"k".to_vec(),
                        range: None,
                    },
                )),
            }),
            request: Some(get_op()),
        };

        let (id, error) = handle_state_request(&backend, request).await.unwrap_err();
        assert_eq!(id, "req-7");
        assert!(
            error.contains("ordered_list_user_state"),
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
    async fn iterable_side_input_returns_matching_window_with_nested_encoding() {
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

        assert_eq!(get_response_data(response), b"\x03one\x03two".to_vec());
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

    #[tokio::test]
    async fn multimap_side_input_returns_unique_keys_and_values_per_window() {
        let (_dir, store, resolver) = make_multimap_resolver().await;
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
                    windowed(kv_record(1, "a"), first.clone()),
                    windowed(kv_record(2, "b"), first.clone()),
                    windowed(kv_record(1, "c"), first.clone()),
                    windowed(kv_record(3, "other"), second),
                ],
            )
            .await
            .unwrap();

        let backend = build_state_backend(store).await.unwrap();
        let keys = handle_state_request_with_side_inputs(
            &backend,
            Some(&resolver),
            multimap_keys_request("multi-1", &encoded_window(&first)),
        )
        .await
        .unwrap();
        let mut keys = get_response_data(keys);
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2]);

        let values = handle_state_request_with_side_inputs(
            &backend,
            Some(&resolver),
            multimap_value_request("multi-2", &encoded_window(&first), &[1]),
        )
        .await
        .unwrap();
        assert_eq!(get_response_data(values), b"\x01a\x01c".to_vec());

        let missing = handle_state_request_with_side_inputs(
            &backend,
            Some(&resolver),
            multimap_value_request("multi-3", &encoded_window(&first), &[99]),
        )
        .await
        .unwrap();
        assert!(get_response_data(missing).is_empty());

        let bulk = handle_state_request_with_side_inputs(
            &backend,
            Some(&resolver),
            multimap_keys_values_request("multi-4", &encoded_window(&first)),
        )
        .await
        .unwrap();
        let key_one = vec![
            0x01, // key 1
            0x00, 0x00, 0x00, 0x02, // two values
            0x01, b'a', 0x01, b'c', // nested UTF-8 values
        ];
        let key_two = vec![
            0x02, // key 2
            0x00, 0x00, 0x00, 0x01, // one value
            0x01, b'b',
        ];
        let mut expected_one_first = key_one.clone();
        expected_one_first.extend_from_slice(&key_two);
        let mut expected_two_first = key_two;
        expected_two_first.extend_from_slice(&key_one);
        let bulk = get_response_data(bulk);
        assert!(
            bulk == expected_one_first || bulk == expected_two_first,
            "multimap key order is unspecified, got {bulk:?}"
        );
    }
}
