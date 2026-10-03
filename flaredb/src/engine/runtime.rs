use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::Instant,
};

use anyhow::anyhow;
use beam_model_rs::v1::{
    ApiServiceDescriptor, Coder, Components, Elements, FunctionSpec, PTransform, ParDoPayload,
    ProcessBundleDescriptor, RemoteGrpcPort, elements,
};
use bytes::{Buf, BytesMut};
use log::{debug, info};
use prost::Message;
use tokio::sync::{Mutex, mpsc::UnboundedReceiver};

use crate::{
    coders::{
        BeamCoder, StandardBeamCoders, length_prefix_pickled_leaves,
        primitives::{
            BeamWindow, PaneInfo, Timer as WireTimer, TimerCoder, WindowCoder, WindowedValue,
            WindowedValueCoder,
        },
        resolve_length_prefixed_coder_id,
    },
    engine::{
        harness::{
            control::{ControlChannel, ControlResponse},
            data::{DataChannel, DataKey, ElementStreamPayload},
        },
        timer::TimerService,
    },
    fusion::{pipeline::ConsumerMetaData, stage::ExecutableStage},
    jobservice::urns::beam_urns,
    state::timer::{TimeDomain, TimerEntry, TimerKey},
    store::{
        element_store::{FlareElementStore, ScanCollectionRequest},
        record::{BeamRecord, PrimitiveValue},
    },
    transforms::FlareRunnerTransform,
    utils::batch_size_estimator::{BatchConfig, BatchSizeEstimator},
};

#[derive(Clone)]
pub struct BundleRuntime {
    control: ControlChannel,
    data: DataChannel,
    store: Arc<FlareElementStore>,
    pipeline_coders: Arc<HashMap<String, Coder>>,
    pipeline_components: Arc<Components>,
    timer_service: Arc<TimerService>,
}

impl BundleRuntime {
    pub fn new(
        control: ControlChannel,
        data: DataChannel,
        store: Arc<FlareElementStore>,
        pipeline_coders: Arc<HashMap<String, Coder>>,
        pipeline_components: Arc<Components>,
        timer_service: Arc<TimerService>,
    ) -> Self {
        Self {
            control,
            data,
            store,
            pipeline_coders,
            pipeline_components,
            timer_service,
        }
    }

    pub fn timer_service(&self) -> &Arc<TimerService> {
        &self.timer_service
    }

    pub fn control(&mut self) -> &mut ControlChannel {
        &mut self.control
    }

    pub fn data(&self) -> &DataChannel {
        &self.data
    }

    pub fn store(&self) -> &Arc<FlareElementStore> {
        &self.store
    }

    pub fn pipeline_coders(&self) -> &Arc<HashMap<String, Coder>> {
        &self.pipeline_coders
    }

    pub fn pipeline_components(&self) -> &Arc<Components> {
        &self.pipeline_components
    }

    pub fn data_receiver(
        &self,
        data_key: DataKey,
    ) -> Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>> {
        self.data.get_receiver(data_key)
    }

    fn window_coder_for_pcollection(&self, pcollection_id: &str) -> WindowCoder {
        let pcollection = self
            .pipeline_components
            .pcollections
            .get(pcollection_id)
            .unwrap_or_else(|| panic!("PCollection not found: {pcollection_id}"));
        let strategy = self
            .pipeline_components
            .windowing_strategies
            .get(&pcollection.windowing_strategy_id);
        let window_coder_id = strategy
            .map(|strategy| strategy.window_coder_id.as_str())
            .filter(|id| !id.is_empty())
            .or_else(|| {
                strategy
                    .and_then(|strategy| strategy.window_fn.as_ref())
                    .filter(|window_fn| window_fn.urn == "beam:window_fn:fixed_windows:v1")
                    .and_then(|_| {
                        self.pipeline_coders.iter().find_map(|(id, coder)| {
                            (coder.spec.as_ref()?.urn == beam_urns::INTERVAL_WINDOW_CODER)
                                .then_some(id.as_str())
                        })
                    })
            })
            .unwrap_or(beam_urns::GLOBAL_WINDOW_CODER);
        let urn = self
            .pipeline_coders
            .get(window_coder_id)
            .and_then(|coder| coder.spec.as_ref())
            .map(|spec| spec.urn.as_str())
            .unwrap_or(window_coder_id);
        WindowCoder::from_urn(urn)
    }

    pub async fn register_bundle(
        &mut self,
        stage: &ExecutableStage,
    ) -> anyhow::Result<ControlResponse> {
        let endpoint = ApiServiceDescriptor {
            url: crate::DEFAULT_API_SERVICE_URL.to_string(),
            ..Default::default()
        };

        let transforms = stage_transforms_with_data_boundaries(stage, endpoint.clone());
        let mut components = stage.components();
        // The Python SDK emits several distinguishable coders under the single
        // `pickled_python` URN. Ask the SDK to length-prefix those leaves so the
        // runner can store their bytes opaquely (mirrors Prism's runner).
        length_prefix_pickled_leaves(&mut components.coders);
        add_stage_data_boundary_coders(
            stage,
            &mut components.coders,
            self.pipeline_components.as_ref(),
        );

        // ToDo: validate if we need to pass stage scoped or pipeline scoped values
        let descriptor = ProcessBundleDescriptor {
            id: stage.id().to_string(),
            transforms,
            pcollections: components.pcollections,
            windowing_strategies: components.windowing_strategies,
            coders: components.coders,
            environments: components.environments,
            state_api_service_descriptor: Some(endpoint.clone()),
            timer_api_service_descriptor: Some(endpoint),
        };

        let response = self.control.register_bundle(descriptor).await;
        info!(
            "Registered bundle at worker for descriptor id {}",
            stage.id()
        );
        response
    }

    pub async fn process_output_elements(
        &self,
        receiver: Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>>,
        edge_metadata: ConsumerMetaData,
    ) -> anyhow::Result<()> {
        let store = self.store.clone();
        let pipeline_coders = self.pipeline_coders.clone();

        let mut batch_size_estimator = BatchSizeEstimator::new(BatchConfig {
            min_batch_size: 2,
            ..BatchConfig::default()
        });
        let mut target_batch_size = batch_size_estimator.next_batch_size();

        debug!("Spawned task to process stage's output elements");
        debug!(
            "Decoding with coder_id={}, component_coders={:?}",
            edge_metadata.coder_id, edge_metadata.component_coder
        );

        let component_coder = edge_metadata.component_coder.clone();
        let element_coder = StandardBeamCoders::from_urn(
            &edge_metadata.coder_id,
            component_coder,
            Some(pipeline_coders.as_ref()),
        );
        let window_coder = self.window_coder_for_pcollection(&edge_metadata.produced_pcol_id);
        // VoidCoder elements carry no payload, so round-tripping them through
        // `BeamRecord`/Arrow/Paimon is both unnecessary and lossy for the
        // WindowedValue framing. Preserve the original encoded bytes instead.
        let opaque_void = element_coder.is_void();
        let windowed_value_coder =
            WindowedValueCoder::with_window_coder(element_coder, window_coder);

        let mut stream_buffer = BytesMut::new();
        let mut batch = Vec::with_capacity(target_batch_size);
        let pcollection_id = edge_metadata.produced_pcol_id.clone();
        let mut stream_ended = false;
        let mut total_decoded: usize = 0;

        while !stream_ended {
            let payload = {
                let mut receiver_lock = receiver.lock().await;
                receiver_lock.recv().await
            };
            // ToDo: create per bundle schema instred of deriving schema for eveyry record batch.
            // create paimon writer and commitor per bundle
            match payload {
                Some(ElementStreamPayload::Data(data_chunk)) => {
                    stream_buffer.extend_from_slice(&data_chunk.data.data);

                    if data_chunk.data.is_last {
                        stream_ended = true;
                    }

                    // Decode as many complete elements as possible from the buffer.
                    // Elements may span Data message boundaries, when a decode underflows
                    // (panics due to incomplete data), we catch it and wait for more data.
                    loop {
                        if stream_buffer.is_empty() {
                            break;
                        }

                        // Read through a Cursor so stream_buffer is never mutated on panic.
                        let mut cursor = Cursor::new(&stream_buffer[..]);

                        let decode_result = catch_unwind(AssertUnwindSafe(|| {
                            windowed_value_coder.decode(&mut cursor)
                        }));

                        match decode_result {
                            Ok(Ok(windowed_value)) => {
                                let consumed = cursor.position() as usize;
                                drop(cursor);
                                if consumed > stream_buffer.len() {
                                    return Err(anyhow!(
                                        "Coder consumed {} bytes from a {} byte buffer while decoding coder_id={} component_coders={:?}",
                                        consumed,
                                        stream_buffer.len(),
                                        edge_metadata.coder_id,
                                        edge_metadata.component_coder
                                    ));
                                }
                                // For VoidCoder PCollections capture the exact
                                // encoded WindowedValue bytes before they are
                                // consumed from the buffer.
                                let opaque_element = if opaque_void {
                                    Some(stream_buffer[..consumed].to_vec())
                                } else {
                                    None
                                };
                                stream_buffer.advance(consumed);

                                total_decoded += 1;
                                let value = match opaque_element {
                                    Some(raw) => BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(raw)),
                                    None => windowed_value.value.clone(),
                                };
                                batch.push(WindowedValue {
                                    value,
                                    timestamp_millis: windowed_value.timestamp_millis,
                                    windows: windowed_value.windows,
                                    pane: windowed_value.pane,
                                });
                                if batch.len() >= target_batch_size {
                                    let batch_size = batch.len();
                                    let start = Instant::now();
                                    store
                                        .write_windowed_value_batch(
                                            &pcollection_id,
                                            std::mem::take(&mut batch),
                                        )
                                        .await?;
                                    batch_size_estimator.record(batch_size, start.elapsed());
                                    target_batch_size = batch_size_estimator.next_batch_size();
                                }
                            }
                            Ok(Err(coder_err)) => {
                                return Err(anyhow!("Coder decode error: {:?}", coder_err));
                            }
                            Err(_panic) => {
                                if stream_ended {
                                    return Err(anyhow!(
                                        "decode panic with {} leftover bytes after end of stream",
                                        stream_buffer.len()
                                    ));
                                }
                                // Cursor is dropped stream_buffer was never advanced.
                                break;
                            }
                        }
                    }
                }

                Some(ElementStreamPayload::Timers(_timer_chunk)) => {
                    //todo!()
                    debug!("Timers chunk");
                }

                None => {
                    debug!("Receiver channel closed");
                    stream_ended = true;
                }
            }
        }

        if !stream_buffer.is_empty() {
            return Err(anyhow!(
                "{} leftover bytes remain after end of stream — decoded {} elements, {} undecoded bytes discarded",
                stream_buffer.len(),
                batch.len(),
                stream_buffer.len(),
            ));
        }

        // Flush any remaining elements in the batch.
        if !batch.is_empty() {
            let batch_size = batch.len();
            let start = Instant::now();
            store
                .write_windowed_value_batch(&pcollection_id, batch)
                .await?;
            batch_size_estimator.record(batch_size, start.elapsed());
        }

        info!(
            "Finished decoding output elements: {} total elements",
            total_decoded
        );

        Ok(())
    }

    /// Encode a stage's input elements and push them to the worker's source.
    ///
    /// Wire framing (matches how the SDKs themselves write output): the encoded
    /// elements go out on an `Elements.Data` message with `is_last = false`,
    /// followed by an empty `is_last = true` marker. The Python harness drops the
    /// payload of an `is_last` message (it only marks the input done), so a
    /// payload sent with `is_last = true` would be lost; the Java harness decodes
    /// both, so the split framing is the portable-safe choice.
    ///
    /// `timer_endpoints` carries the stage's inbound timer endpoints (see
    /// [`stage_timer_endpoints`]); each is terminated with an empty
    /// `Elements.Timers { is_last = true }` so the harness's `awaitCompletion`
    /// (which waits for data *and* timer endpoints) can return.
    pub async fn process_input_elements(
        &self,
        input_instruction_id: String,
        consumer_transform_id: String,
        input_pcollection_id: String,
        input_coder_id: String,
        input_component_coder_ids: Option<Vec<String>>,
        timer_endpoints: Vec<(String, String)>,
    ) -> anyhow::Result<()> {
        debug!("Spawned task to send stage's input elements to worker");
        debug!(
            "Sending input elements: instruction_id={}, transform_id={}",
            input_instruction_id, consumer_transform_id,
        );

        let request = ScanCollectionRequest {
            pcollection_id: input_pcollection_id.clone(),
        };

        let elements = self.store.scan_windowed_values(request).await?;
        debug!("Input element coder: {}", input_coder_id);

        let element_coder = StandardBeamCoders::from_urn(
            input_coder_id.as_str(),
            input_component_coder_ids.clone(),
            Some(self.pipeline_coders.as_ref()),
        );

        let opaque_void = element_coder.is_void();
        let window_coder = self.window_coder_for_pcollection(&input_pcollection_id);
        let windowed_value_coder =
            WindowedValueCoder::with_window_coder(element_coder, window_coder);
        let mut encoded = BytesMut::new();

        if opaque_void {
            debug!(
                "VoidCoder pcollection {}: forwarding {} opaque element(s) unchanged",
                input_pcollection_id,
                elements.len()
            );
            for element in elements {
                match element.value {
                    BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(raw)) => {
                        encoded.extend_from_slice(&raw);
                    }
                    other => {
                        return Err(anyhow!(
                            "expected opaque bytes for VoidCoder pcollection {}, found {:?}",
                            input_pcollection_id,
                            other
                        ));
                    }
                }
            }
        } else {
            for element in elements {
                windowed_value_coder.encode(element, &mut encoded);
            }
        }

        let payload = encoded.freeze();

        // Terminate the stage's inbound timer endpoints before sending the data
        // terminator, so the harness's awaited completion covers both.
        let timers: Vec<elements::Timers> = timer_endpoints
            .into_iter()
            .map(|(transform_id, timer_family_id)| {
                debug!(
                    "Terminating inbound timer endpoint: instruction_id={}, transform_id={}, timer_family_id={}",
                    input_instruction_id, transform_id, timer_family_id
                );
                elements::Timers {
                    instruction_id: input_instruction_id.clone(),
                    transform_id,
                    timer_family_id,
                    timers: Vec::new(),
                    is_last: true,
                }
            })
            .collect();

        // Wire framing must satisfy both SDK harnesses:
        // - Python (`data_plane.py` `input_elements`) never yields the payload of an
        //   `is_last` Data message; it only marks the input done.
        // - Java (`BeamFnDataInboundObserver.multiplexElements`) decodes the payload
        //   first, then marks the input done.
        // Sending the elements separately with `is_last = false` followed by an empty
        // `is_last = true` terminator is the portable-safe framing, and the only one
        // the Python harness accepts.
        let mut data: Vec<elements::Data> = Vec::new();
        if !payload.is_empty() {
            data.push(elements::Data {
                instruction_id: input_instruction_id.clone(),
                transform_id: consumer_transform_id.clone(),
                data: payload.to_vec(),
                is_last: false,
            });
        }
        data.push(elements::Data {
            instruction_id: input_instruction_id,
            transform_id: consumer_transform_id,
            data: Vec::new(),
            is_last: true,
        });

        let elements = Elements { data, timers };

        self.data.send_elements(elements).await?;
        debug!("Finished sending input elements to worker");
        Ok(())
    }

    /// Terminate a bundle's input endpoints without sending any data.
    ///
    /// Used for a timer-only bundle: the stage must run just its `@OnTimer`, so
    /// its PCollection input must not be re-delivered.
    pub async fn terminate_bundle_input(
        &self,
        instruction_id: String,
        consumer_transform_id: String,
        timer_endpoints: Vec<(String, String)>,
    ) -> anyhow::Result<()> {
        let timers: Vec<elements::Timers> = timer_endpoints
            .into_iter()
            .map(|(transform_id, timer_family_id)| elements::Timers {
                instruction_id: instruction_id.clone(),
                transform_id,
                timer_family_id,
                timers: Vec::new(),
                is_last: true,
            })
            .collect();
        let data = vec![elements::Data {
            instruction_id,
            transform_id: consumer_transform_id,
            data: Vec::new(),
            is_last: true,
        }];
        self.data.send_elements(Elements { data, timers }).await
    }

    /// Consume a stage's inbound `Elements.Timers` chunks, persisting each timer
    /// (set replaces, clear deletes) until that family's stream ends.
    pub async fn process_timer_elements(
        &self,
        transform_id: String,
        timer_family_id: String,
        receiver: Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>>,
    ) -> anyhow::Result<()> {
        let Some((coder, domain)) = resolve_timer_family(
            &self.pipeline_components,
            &self.pipeline_coders,
            &transform_id,
            &timer_family_id,
        ) else {
            log::warn!(
                "no timer coder resolved for {transform_id}/{timer_family_id}; timers dropped"
            );
            return Ok(());
        };

        info!(
            "timer receive: transform_id={transform_id}, timer_family_id={timer_family_id}, domain={domain:?}"
        );

        drain_timer_elements(
            &coder,
            domain,
            &transform_id,
            &timer_family_id,
            &self.timer_service,
            receiver,
        )
        .await;
        Ok(())
    }

    /// Send fired timers to the worker as `Elements.Timers`, grouped by
    /// `(transform, family)`.
    ///
    /// Call this before the input terminator (sent by
    /// [`Self::process_input_elements`]) so the harness observes the timers
    /// before the endpoint closes.
    pub async fn send_fired_timers(
        &self,
        instruction_id: &str,
        timers: &[TimerEntry],
    ) -> anyhow::Result<()> {
        if timers.is_empty() {
            return Ok(());
        }

        let mut groups: BTreeMap<(String, String), Vec<&TimerEntry>> = BTreeMap::new();
        for entry in timers {
            groups
                .entry((
                    entry.key.transform_id.clone(),
                    entry.key.timer_family_id.clone(),
                ))
                .or_default()
                .push(entry);
        }

        let mut messages = Vec::new();
        for ((transform_id, timer_family_id), entries) in groups {
            let Some((coder, _)) = resolve_timer_family(
                &self.pipeline_components,
                &self.pipeline_coders,
                &transform_id,
                &timer_family_id,
            ) else {
                log::warn!(
                    "no timer coder for {transform_id}/{timer_family_id}; fired timers dropped"
                );
                continue;
            };
            let mut bytes = Vec::new();
            for entry in entries {
                if let Some(wire) = timer_to_wire(entry) {
                    coder.encode_into(&wire, &mut bytes);
                }
            }
            messages.push(elements::Timers {
                instruction_id: instruction_id.to_string(),
                transform_id,
                timer_family_id,
                timers: bytes,
                is_last: false,
            });
        }

        if !messages.is_empty() {
            self.data
                .send_elements(Elements {
                    data: Vec::new(),
                    timers: messages,
                })
                .await?;
        }
        Ok(())
    }
}

/// Resolve a timer family's key/window coders and time domain from its
/// transform's `ParDoPayload.timer_family_specs`.
///
/// Free function (rather than a [`BundleRuntime`] method) so the resolution can
/// be unit-tested against a hand-built `Components`/coder map, without harness
/// channels. Returns `None` when the transform, family, coder or domain is
/// unknown, in which case the caller drops the timers.
fn resolve_timer_family(
    components: &Components,
    coders: &HashMap<String, Coder>,
    transform_id: &str,
    timer_family_id: &str,
) -> Option<(TimerCoder, TimeDomain)> {
    let transform = components.transforms.get(transform_id)?;
    let payload = transform
        .spec
        .as_ref()
        .and_then(|spec| ParDoPayload::decode(spec.payload.as_slice()).ok())?;
    let family = payload.timer_family_specs.get(timer_family_id)?;
    let domain = match family.time_domain {
        1 => TimeDomain::EventTime,
        2 => TimeDomain::ProcessingTime,
        _ => return None,
    };

    let coder = coders.get(&family.timer_family_coder_id)?;
    let spec = coder.spec.as_ref()?;
    if spec.urn == beam_urns::TIMER_CODER {
        // `beam:coder:timer:v1` components are [key coder, window coder].
        let key_id = coder.component_coder_ids.first()?;
        let window_id = coder.component_coder_ids.get(1)?;
        let key_coder = StandardBeamCoders::from_urn(key_id, None, Some(coders));
        let window_urn = coders
            .get(window_id)
            .and_then(|c| c.spec.as_ref())
            .map(|s| s.urn.as_str())?;
        Some((
            TimerCoder::new(key_coder, WindowCoder::from_urn(window_urn)),
            domain,
        ))
    } else {
        // Some SDKs register the key coder directly; assume a global window.
        let key_coder = StandardBeamCoders::from_urn(
            &family.timer_family_coder_id,
            Some(coder.component_coder_ids.clone()),
            Some(coders),
        );
        Some((TimerCoder::new(key_coder, WindowCoder::Global), domain))
    }
}

/// Decode a stage's inbound timer stream and persist each timer into the
/// [`TimerService`] (a set replaces, a clear deletes), until the family's stream
/// ends (`is_last`) or the channel closes.
///
/// Split out of [`BundleRuntime::process_timer_elements`] so the decode →
/// persist path can be tested against a real `TimerService` and receiver,
/// without constructing harness channels. Returns the number of timers decoded
/// (sets plus clears).
async fn drain_timer_elements(
    coder: &TimerCoder,
    domain: TimeDomain,
    transform_id: &str,
    timer_family_id: &str,
    timer_service: &TimerService,
    receiver: Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>>,
) -> usize {
    let mut total = 0usize;
    loop {
        let payload = {
            let mut guard = receiver.lock().await;
            guard.recv().await
        };
        match payload {
            Some(ElementStreamPayload::Timers(chunk)) => {
                debug!(
                    "timer chunk received: transform_id={}, timer_family_id={}, is_last={}, bytes={}",
                    chunk.timers.transform_id,
                    chunk.timers.timer_family_id,
                    chunk.timers.is_last,
                    chunk.timers.timers.len()
                );
                let mut buf: &[u8] = &chunk.timers.timers;
                let mut decoded = 0usize;
                while !buf.is_empty() {
                    match coder.decode(&mut buf) {
                        Ok(timer) => {
                            decoded += 1;
                            let entry = timer_entry(transform_id, timer_family_id, &timer, domain);
                            if timer.clear {
                                debug!(
                                    "timer clear: key={:?}, window={}",
                                    entry.key.user_key, entry.key.window
                                );
                            } else {
                                debug!(
                                    "timer set: key={:?}, window={}, fire_timestamp={}, hold_timestamp={}",
                                    entry.key.user_key,
                                    entry.key.window,
                                    entry.fire_timestamp,
                                    entry.hold_timestamp
                                );
                            }
                            let result = if timer.clear {
                                timer_service.clear(&entry.key).await
                            } else {
                                timer_service.set(entry).await
                            };
                            if let Err(e) = result {
                                log::warn!("failed to persist timer: {e}");
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "failed to decode timer for {transform_id}/{timer_family_id}: {e}"
                            );
                            break;
                        }
                    }
                }
                total += decoded;
                if decoded > 0 {
                    info!("persisted {decoded} timer(s) for {transform_id}/{timer_family_id}");
                }
                if chunk.timers.is_last {
                    return total;
                }
            }
            Some(ElementStreamPayload::Data(_)) => {}
            None => return total,
        }
    }
}

/// Build a storage [`TimerEntry`] from a decoded wire timer.
fn timer_entry(
    transform_id: &str,
    timer_family_id: &str,
    timer: &WireTimer,
    domain: TimeDomain,
) -> TimerEntry {
    let window = timer
        .windows
        .first()
        .map(|window| window.canonical_key())
        .unwrap_or_else(|| "global".to_string());
    TimerEntry {
        key: TimerKey {
            transform_id: transform_id.to_string(),
            timer_family_id: timer_family_id.to_string(),
            tag: timer.tag.clone(),
            window,
            user_key: timer.user_key.clone(),
        },
        domain,
        fire_timestamp: timer.fire_timestamp,
        hold_timestamp: timer.hold_timestamp,
    }
}

/// Rebuild the wire timer for a fired [`TimerEntry`].
fn timer_to_wire(entry: &TimerEntry) -> Option<WireTimer> {
    let window = BeamWindow::from_canonical_key(&entry.key.window)?;
    Some(WireTimer {
        user_key: entry.key.user_key.clone(),
        tag: entry.key.tag.clone(),
        windows: vec![window],
        clear: false,
        fire_timestamp: entry.fire_timestamp,
        hold_timestamp: entry.hold_timestamp,
        pane: PaneInfo::no_firing(),
    })
}

pub fn metadata_pcollection_id(metadata: Option<&ConsumerMetaData>) -> String {
    metadata
        .expect("Runner node must have at least one available metadata source")
        .produced_pcol_id
        .clone()
}

pub fn runner_output_pcollection_id(
    runner_transform: &FlareRunnerTransform,
    output_metadata: Option<&ConsumerMetaData>,
) -> String {
    output_metadata
        .map(|meta| meta.produced_pcol_id.clone())
        .or_else(|| runner_transform.output_pcol_ids().into_iter().next())
        .expect("Runner transform must have an output pcollection id")
}

pub fn runner_consumer_transform_id(
    input_metadata: Option<&ConsumerMetaData>,
    output_metadata: Option<&ConsumerMetaData>,
) -> String {
    output_metadata
        .or(input_metadata)
        .expect("Runner node must have available transform metadata")
        .consumer_transfrom_id
        .clone()
}

pub fn stage_source_transform_id(stage: &ExecutableStage) -> String {
    format!("{}/source", stage.id())
}

pub fn stage_sink_transform_id(stage: &ExecutableStage, pcollection_id: &str) -> String {
    format!("{}/sink/{}", stage.id(), pcollection_id)
}

pub fn remote_grpc_port(endpoint: ApiServiceDescriptor, coder_id: String) -> RemoteGrpcPort {
    RemoteGrpcPort {
        api_service_descriptor: Some(endpoint),
        coder_id,
    }
}

pub fn global_window_coder_id(stage: &ExecutableStage) -> String {
    format!("{}/global_window", stage.id())
}

pub fn windowed_value_coder_id(stage: &ExecutableStage, pcollection_id: &str) -> String {
    format!("{}/windowed_value/{}", stage.id(), pcollection_id)
}

/// Adds a Beam `windowed_value:v1` coder whose components are the element
/// coder followed by the PCollection's window coder.
pub fn insert_windowed_value_coder(
    coders: &mut HashMap<String, Coder>,
    windowed_value_coder_id: String,
    element_coder_id: String,
    window_coder_id: String,
) {
    if window_coder_id.ends_with("/global_window") {
        coders.entry(window_coder_id.clone()).or_insert(Coder {
            spec: Some(FunctionSpec {
                urn: beam_urns::GLOBAL_WINDOW_CODER.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        });
    }

    coders.insert(
        windowed_value_coder_id,
        Coder {
            spec: Some(FunctionSpec {
                urn: beam_urns::WINDOWED_VALUE_CODER.to_string(),
                payload: Vec::new(),
            }),
            // Beam defines these components as element coder, then window coder.
            component_coder_ids: vec![element_coder_id, window_coder_id],
        },
    );
}

fn stage_window_coder_id(
    stage: &ExecutableStage,
    pcollection_id: &str,
    pipeline_components: &Components,
) -> String {
    let components = pipeline_components;
    let pcollection = components
        .pcollections
        .get(pcollection_id)
        .unwrap_or_else(|| panic!("PCollection not found: {pcollection_id}"));
    let strategy = components
        .windowing_strategies
        .get(&pcollection.windowing_strategy_id);
    let window_coder_id = strategy
        .map(|strategy| strategy.window_coder_id.clone())
        .filter(|id| !id.is_empty())
        .or_else(|| {
            strategy
                .and_then(|strategy| strategy.window_fn.as_ref())
                .filter(|window_fn| window_fn.urn == "beam:window_fn:fixed_windows:v1")
                .and_then(|_| {
                    components.coders.iter().find_map(|(id, coder)| {
                        (coder.spec.as_ref()?.urn == beam_urns::INTERVAL_WINDOW_CODER)
                            .then_some(id.clone())
                    })
                })
        });
    let resolved = window_coder_id.unwrap_or_else(|| format!("{}/global_window", stage.id()));
    debug!(
        "Resolved boundary window coder: stage={}, pcollection={}, strategy={}, window_coder_id={}",
        stage.id(),
        pcollection_id,
        pcollection.windowing_strategy_id,
        resolved
    );
    resolved
}

/// Registers windowed-value source/sink coders for a stage's input and output
/// PCollections.
///
/// The window coder is resolved from the pipeline's
/// `PCollection → WindowingStrategy → window_coder_id` path. Element coder ids
/// are resolved through [`resolve_length_prefixed_coder_id`] so opaque leaves
/// that the runner asked the SDK to length-prefix are referenced by their
/// wrapper.
pub fn add_stage_data_boundary_coders(
    stage: &ExecutableStage,
    coders: &mut HashMap<String, Coder>,
    pipeline_components: &Components,
) {
    let input_pcol = stage.input_pcol();

    insert_windowed_value_coder(
        coders,
        windowed_value_coder_id(stage, input_pcol.id()),
        resolve_length_prefixed_coder_id(&input_pcol.node().coder_id, coders),
        stage_window_coder_id(stage, input_pcol.id(), pipeline_components),
    );

    for output_pcol in stage.output_pcols() {
        insert_windowed_value_coder(
            coders,
            windowed_value_coder_id(stage, output_pcol.id()),
            resolve_length_prefixed_coder_id(&output_pcol.node().coder_id, coders),
            stage_window_coder_id(stage, output_pcol.id(), pipeline_components),
        );
    }
}

/// Add stage's source and sink boundary( basically tells the worker where a stage begins and ends)
pub fn stage_transforms_with_data_boundaries(
    stage: &ExecutableStage,
    endpoint: ApiServiceDescriptor,
) -> HashMap<String, PTransform> {
    let mut transforms = stage.ptmap();

    let input_pcol = stage.input_pcol();
    let source_id = stage_source_transform_id(stage);
    let input_element_coder_id = input_pcol.node().coder_id.clone();
    let input_wire_coder_id = windowed_value_coder_id(stage, input_pcol.id());
    debug!(
        "Adding SDK stage source transform: id={}, output_pcollection={}, element_coder_id={}, wire_coder_id={}",
        source_id,
        input_pcol.id(),
        input_element_coder_id,
        input_wire_coder_id
    );
    transforms.insert(
        source_id.clone(),
        PTransform {
            unique_name: source_id.clone(),
            spec: Some(FunctionSpec {
                urn: beam_urns::BEAM_SOURCE.to_string(),
                payload: remote_grpc_port(endpoint.clone(), input_wire_coder_id).encode_to_vec(),
            }),
            inputs: HashMap::new(),
            outputs: HashMap::from([("local_output".to_string(), input_pcol.id().clone())]),
            ..Default::default()
        },
    );

    if stage.output_pcols().is_empty() {
        debug!(
            "Stage {} has no boundary output PCollections: every output is consumed inside the stage (fused downstream) or is terminal, so no sink transform is registered",
            stage.id()
        );
    }

    for output_pcol in stage.output_pcols() {
        let sink_id = stage_sink_transform_id(stage, output_pcol.id());
        let output_element_coder_id = output_pcol.node().coder_id.clone();
        let output_wire_coder_id = windowed_value_coder_id(stage, output_pcol.id());
        debug!(
            "Adding SDK stage sink transform: id={}, input_pcollection={}, element_coder_id={}, wire_coder_id={}",
            sink_id,
            output_pcol.id(),
            output_element_coder_id,
            output_wire_coder_id
        );
        transforms.insert(
            sink_id.clone(),
            PTransform {
                unique_name: sink_id.clone(),
                spec: Some(FunctionSpec {
                    urn: beam_urns::BEAM_SINK.to_string(),
                    payload: remote_grpc_port(endpoint.clone(), output_wire_coder_id)
                        .encode_to_vec(),
                }),
                // it may not be right to add the "local_input".to_string() as key, we need to
                // get the actualcollection's key from compos and insert
                inputs: HashMap::from([("local_input".to_string(), output_pcol.id().clone())]),
                outputs: HashMap::new(),
                ..Default::default()
            },
        );
    }

    transforms
}

/// The inbound timer endpoints `(transform_id, timer_family_id)` a stage's harness
/// registers for user timers (`@OnTimer`), derived from the timer families the
/// stage's `ParDo` declares (`ParDoPayload.timer_family_specs`).
///
/// The SDK harness blocks in `awaitCompletion` until *every* inbound endpoint —
/// data **and** timers — receives an `is_last`, so the runner has to terminate
/// these explicitly; otherwise the bundle hangs even though all data was sent and
/// all state reads were answered.
pub fn stage_timer_endpoints(stage: &ExecutableStage) -> Vec<(String, String)> {
    stage
        .timers()
        .iter()
        .map(|timer| {
            (
                timer.transform().id().clone(),
                timer.local_name().to_string(),
            )
        })
        .collect()
}

#[cfg(test)]
mod timer_tests {
    use super::*;
    use crate::coders::primitives::StringUtf8Coder;
    use crate::engine::harness::data::{TimerChunk, TimersKey};
    use crate::state::timer::TimerStore;
    use crate::store::element_store::FlareElementStore;
    use beam_model_rs::v1::TimerFamilySpec;
    use prost::Message;
    use tempfile::tempdir;

    fn coder_for_string_key() -> TimerCoder {
        TimerCoder::new(
            StandardBeamCoders::StringUtf8(StringUtf8Coder),
            WindowCoder::Global,
        )
    }

    /// The nested-encoded bytes a timer coder stores as a timer's `user_key`.
    fn encoded_string_key(s: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        StandardBeamCoders::StringUtf8(StringUtf8Coder).encode(
            BeamRecord::PRIMITIVE(PrimitiveValue::String(s.to_string())),
            &mut buf,
        );
        buf
    }

    fn wire_timer(user_key: Vec<u8>, clear: bool, fire: i64, hold: i64) -> WireTimer {
        WireTimer {
            user_key,
            tag: String::new(),
            windows: vec![BeamWindow::Global],
            clear,
            fire_timestamp: fire,
            hold_timestamp: hold,
            pane: PaneInfo::no_firing(),
        }
    }

    fn timers_payload(
        transform_id: &str,
        timer_family_id: &str,
        bytes: Vec<u8>,
        is_last: bool,
    ) -> ElementStreamPayload {
        ElementStreamPayload::Timers(TimerChunk {
            key: TimersKey {
                instruction_id: "instr".to_string(),
                transform_id: transform_id.to_string(),
                timer_family_id: timer_family_id.to_string(),
            },
            timers: elements::Timers {
                instruction_id: "instr".to_string(),
                transform_id: transform_id.to_string(),
                timer_family_id: timer_family_id.to_string(),
                timers: bytes,
                is_last,
            },
        })
    }

    async fn make_timer_service() -> (tempfile::TempDir, Arc<TimerService>) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir
            .path()
            .to_str()
            .expect("tempdir path is not valid utf8")
            .to_string();
        let store = FlareElementStore::new(warehouse, "timers-test".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        let service = Arc::new(TimerService::new(TimerStore::new(Arc::new(store))));
        (dir, service)
    }

    /// A coder id helper for the resolver test.
    fn coder(urn: &str, component_ids: Vec<&str>) -> Coder {
        Coder {
            spec: Some(FunctionSpec {
                urn: urn.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: component_ids.into_iter().map(str::to_string).collect(),
        }
    }

    #[test]
    fn resolve_timer_family_reads_key_window_coders_and_domain() {
        let mut components = Components::default();

        let mut payload = ParDoPayload::default();
        payload.timer_family_specs.insert(
            "ts-flush".to_string(),
            TimerFamilySpec {
                time_domain: 2, // PROCESSING_TIME
                timer_family_coder_id: "timer_coder".to_string(),
            },
        );
        payload.timer_family_specs.insert(
            "ts-event".to_string(),
            TimerFamilySpec {
                time_domain: 1, // EVENT_TIME
                timer_family_coder_id: "timer_coder".to_string(),
            },
        );
        components.transforms.insert(
            "BufferAndFireOnTimer".to_string(),
            PTransform {
                unique_name: "BufferAndFireOnTimer".to_string(),
                spec: Some(FunctionSpec {
                    urn: beam_urns::PAR_DO_TRANSFORM.to_string(),
                    payload: payload.encode_to_vec(),
                }),
                ..Default::default()
            },
        );

        let mut coders: HashMap<String, Coder> = HashMap::new();
        coders.insert(
            "key_coder".to_string(),
            coder(beam_urns::STRING_UTF8_CODER, vec![]),
        );
        coders.insert(
            "window_coder".to_string(),
            coder(beam_urns::GLOBAL_WINDOW_CODER, vec![]),
        );
        coders.insert(
            "timer_coder".to_string(),
            coder(beam_urns::TIMER_CODER, vec!["key_coder", "window_coder"]),
        );

        let (coder, domain) =
            resolve_timer_family(&components, &coders, "BufferAndFireOnTimer", "ts-flush")
                .expect("timer family resolves");
        assert_eq!(domain, TimeDomain::ProcessingTime);

        // The resolved coder is a timer coder with a StringUtf8 key and the global
        // window: a wire timer round-trips through it.
        let key = encoded_string_key("alice");
        let mut bytes = Vec::new();
        coder.encode_into(&wire_timer(key.clone(), false, 1000, 999), &mut bytes);
        let mut buf: &[u8] = &bytes;
        let decoded = coder.decode(&mut buf).expect("decode with resolved coder");
        assert_eq!(decoded.user_key, key);
        assert_eq!(decoded.fire_timestamp, 1000);

        // The event-time family maps to the event-time domain.
        let (_, event_domain) =
            resolve_timer_family(&components, &coders, "BufferAndFireOnTimer", "ts-event")
                .expect("event-time family resolves");
        assert_eq!(event_domain, TimeDomain::EventTime);

        // Unknown transform or family is a miss (callers drop, not panic).
        assert!(resolve_timer_family(&components, &coders, "nope", "ts-flush").is_none());
        assert!(
            resolve_timer_family(&components, &coders, "BufferAndFireOnTimer", "nope").is_none()
        );
    }

    #[tokio::test]
    async fn drain_persists_a_set_timer() {
        let (_dir, service) = make_timer_service().await;
        let coder = coder_for_string_key();
        let key = encoded_string_key("alice");

        let mut set_bytes = Vec::new();
        coder.encode_into(&wire_timer(key.clone(), false, 1000, 999), &mut set_bytes);

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(timers_payload("t", "f", set_bytes, true)).unwrap();
        drop(tx);

        let decoded = drain_timer_elements(
            &coder,
            TimeDomain::ProcessingTime,
            "t",
            "f",
            service.as_ref(),
            Arc::new(Mutex::new(rx)),
        )
        .await;
        assert_eq!(decoded, 1);

        let entries = service.store().entries().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.transform_id, "t");
        assert_eq!(entries[0].key.timer_family_id, "f");
        assert_eq!(entries[0].key.window, "global");
        assert_eq!(entries[0].key.user_key, key);
        assert_eq!(entries[0].domain, TimeDomain::ProcessingTime);
        assert_eq!(entries[0].fire_timestamp, 1000);
        assert_eq!(entries[0].hold_timestamp, 999);
    }

    #[tokio::test]
    async fn drain_clear_deletes_the_set_timer() {
        let (_dir, service) = make_timer_service().await;
        let coder = coder_for_string_key();
        let key = encoded_string_key("alice");

        let mut set_bytes = Vec::new();
        coder.encode_into(&wire_timer(key.clone(), false, 1000, 999), &mut set_bytes);
        let mut clear_bytes = Vec::new();
        coder.encode_into(&wire_timer(key.clone(), true, 0, 0), &mut clear_bytes);

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(timers_payload("t", "f", set_bytes, false)).unwrap();
        tx.send(timers_payload("t", "f", clear_bytes, true))
            .unwrap();
        drop(tx);

        let decoded = drain_timer_elements(
            &coder,
            TimeDomain::ProcessingTime,
            "t",
            "f",
            service.as_ref(),
            Arc::new(Mutex::new(rx)),
        )
        .await;
        assert_eq!(decoded, 2);
        assert!(
            service.store().entries().await.unwrap().is_empty(),
            "a cleared timer must not remain in the store"
        );
    }

    #[tokio::test]
    async fn drain_decodes_concatenated_timers_in_one_chunk() {
        let (_dir, service) = make_timer_service().await;
        let coder = coder_for_string_key();

        // Two set timers for different keys, concatenated into one chunk, exactly
        // as the SDK frames a batch of set timers.
        let mut bytes = Vec::new();
        coder.encode_into(
            &wire_timer(encoded_string_key("alice"), false, 10, 10),
            &mut bytes,
        );
        coder.encode_into(
            &wire_timer(encoded_string_key("bob"), false, 20, 20),
            &mut bytes,
        );

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(timers_payload("t", "f", bytes, true)).unwrap();
        drop(tx);

        let decoded = drain_timer_elements(
            &coder,
            TimeDomain::ProcessingTime,
            "t",
            "f",
            service.as_ref(),
            Arc::new(Mutex::new(rx)),
        )
        .await;
        assert_eq!(decoded, 2);

        let mut entries = service.store().entries().await.unwrap();
        entries.sort_by_key(|entry| entry.fire_timestamp);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key.user_key, encoded_string_key("alice"));
        assert_eq!(entries[0].fire_timestamp, 10);
        assert_eq!(entries[1].key.user_key, encoded_string_key("bob"));
        assert_eq!(entries[1].fire_timestamp, 20);
    }
}
