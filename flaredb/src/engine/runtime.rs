use std::{
    collections::HashMap,
    io::Cursor,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::Instant,
};

use anyhow::anyhow;
use beam_model_rs::v1::{
    ApiServiceDescriptor, Coder, Components, Elements, FunctionSpec, PTransform,
    ProcessBundleDescriptor, RemoteGrpcPort, elements,
};
use bytes::{Buf, BytesMut};
use log::info;
use prost::Message;
use tokio::sync::{Mutex, mpsc::UnboundedReceiver};

use crate::{
    coders::{
        BeamCoder, StandardBeamCoders, length_prefix_pickled_leaves,
        primitives::WindowedValueCoder, resolve_length_prefixed_coder_id,
    },
    engine::harness::{
        control::{ControlChannel, ControlResponse},
        data::{DataChannel, DataKey, ElementStreamPayload},
    },
    fusion::{pipeline::ConsumerMetaData, stage::ExecutableStage},
    jobservice::urns::beam_urns,
    store::{
        element_store::{FlareElementStore, NewCollectionRequest, ScanCollectionRequest},
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
}

impl BundleRuntime {
    pub fn new(
        control: ControlChannel,
        data: DataChannel,
        store: Arc<FlareElementStore>,
        pipeline_coders: Arc<HashMap<String, Coder>>,
        pipeline_components: Arc<Components>,
    ) -> Self {
        Self {
            control,
            data,
            store,
            pipeline_coders,
            pipeline_components,
        }
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
        add_stage_data_boundary_coders(stage, &mut components.coders);

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

        info!("Spawned task to process stage's output elements");
        info!(
            "Decoding with coder_id={}, component_coders={:?}",
            edge_metadata.coder_id, edge_metadata.component_coder
        );

        let component_coder = edge_metadata.component_coder.clone();
        let element_coder = StandardBeamCoders::from_urn(
            &edge_metadata.coder_id,
            component_coder,
            Some(pipeline_coders.as_ref()),
        );
        // VoidCoder elements carry no payload, so round-tripping them through
        // `BeamRecord`/Arrow/Paimon is both unnecessary and lossy for the
        // WindowedValue framing. Preserve the original encoded bytes instead.
        let opaque_void = element_coder.is_void();
        let windowed_value_coder = WindowedValueCoder::new(element_coder);

        let mut stream_buffer = BytesMut::new();
        let mut batch: Vec<BeamRecord> = Vec::with_capacity(target_batch_size);
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
                                match opaque_element {
                                    Some(raw) => batch
                                        .push(BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(raw))),
                                    None => batch.push(windowed_value.value),
                                }
                                if batch.len() >= target_batch_size {
                                    let batch_size = batch.len();
                                    let request = NewCollectionRequest {
                                        pcollection_id: pcollection_id.clone(),
                                        elements: std::mem::take(&mut batch),
                                    };
                                    let start = Instant::now();
                                    store.write_beamrecord_batch(request).await?;
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
                    info!("Timers chunk");
                }

                None => {
                    info!("Receiver channel closed");
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
            let request = NewCollectionRequest {
                pcollection_id: pcollection_id.clone(),
                elements: batch,
            };
            let start = Instant::now();
            store.write_beamrecord_batch(request).await?;
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
    pub async fn process_input_elements(
        &self,
        input_instruction_id: String,
        consumer_transform_id: String,
        input_pcollection_id: String,
        input_coder_id: String,
        input_component_coder_ids: Option<Vec<String>>,
    ) -> anyhow::Result<()> {
        info!("Spawned task to send stage's input elements to worker");
        info!(
            "Sending input elements: instruction_id={}, transform_id={}",
            input_instruction_id, consumer_transform_id,
        );

        let request = ScanCollectionRequest {
            pcollection_id: input_pcollection_id.clone(),
        };

        let elements = self.store.scan_collection(request).await?;
        info!("Input element coder: {}", input_coder_id);

        let element_coder = StandardBeamCoders::from_urn(
            input_coder_id.as_str(),
            input_component_coder_ids.clone(),
            Some(self.pipeline_coders.as_ref()),
        );

        let opaque_void = element_coder.is_void();
        let windowed_value_coder = WindowedValueCoder::new(element_coder);
        let mut encoded = BytesMut::new();

        if opaque_void {
            info!(
                "VoidCoder pcollection {}: forwarding {} opaque element(s) unchanged",
                input_pcollection_id,
                elements.len()
            );
            for element in elements {
                match element {
                    BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(raw)) => {
                        encoded.extend_from_slice(&raw);
                    }
                    other => {
                        return Err(anyhow!(
                            "expected opaque bytes for VoidCoder pcollection {}, found {:?}",
                            input_pcollection_id,
                            other.record_type()
                        ));
                    }
                }
            }
        } else {
            for element in elements {
                windowed_value_coder.encode_value(element, &mut encoded);
            }
        }

        let payload = encoded.freeze();

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

        let elements = Elements {
            data,
            timers: Vec::new(),
        };

        self.data.send_elements(elements).await?;
        info!("Finished sending input elements to worker");
        Ok(())
    }
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

pub fn insert_windowed_value_coder(
    coders: &mut HashMap<String, Coder>,
    windowed_value_coder_id: String,
    element_coder_id: String,
    global_window_coder_id: String,
) {
    coders
        .entry(global_window_coder_id.clone())
        .or_insert(Coder {
            spec: Some(FunctionSpec {
                urn: beam_urns::GLOBAL_WINDOW_CODER.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        });

    coders.insert(
        windowed_value_coder_id,
        Coder {
            spec: Some(FunctionSpec {
                urn: beam_urns::WINDOWED_VALUE_CODER.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec![element_coder_id, global_window_coder_id],
        },
    );
}

/// Register the windowed-value source/sink coders for a stage's input and output
/// PCollections.
///
/// Element coder ids are resolved through [`resolve_length_prefixed_coder_id`]
/// so that opaque leaves the runner asked the SDK to length-prefix are referenced
/// by their wrapper.
pub fn add_stage_data_boundary_coders(
    stage: &ExecutableStage,
    coders: &mut HashMap<String, Coder>,
) {
    let global_window_coder_id = global_window_coder_id(stage);
    let input_pcol = stage.input_pcol();

    insert_windowed_value_coder(
        coders,
        windowed_value_coder_id(stage, input_pcol.id()),
        resolve_length_prefixed_coder_id(&input_pcol.node().coder_id, coders),
        global_window_coder_id.clone(),
    );

    for output_pcol in stage.output_pcols() {
        insert_windowed_value_coder(
            coders,
            windowed_value_coder_id(stage, output_pcol.id()),
            resolve_length_prefixed_coder_id(&output_pcol.node().coder_id, coders),
            global_window_coder_id.clone(),
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
    info!(
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

    for output_pcol in stage.output_pcols() {
        let sink_id = stage_sink_transform_id(stage, output_pcol.id());
        let output_element_coder_id = output_pcol.node().coder_id.clone();
        let output_wire_coder_id = windowed_value_coder_id(stage, output_pcol.id());
        info!(
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
