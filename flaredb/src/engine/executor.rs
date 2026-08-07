use std::{
    collections::HashMap,
    io::Cursor,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use async_trait::async_trait;
use beam_model_rs::v1::{
    ApiServiceDescriptor, Coder, Components, Elements, FunctionSpec, PTransform,
    ProcessBundleDescriptor, RemoteGrpcPort, elements,
};
use bytes::{Buf, BytesMut};
use log::{error, info};
use prost::Message;
use tokio::{
    sync::{Mutex, mpsc::UnboundedReceiver},
    task::JoinSet,
};

use crate::{
    engine::{
        coders::{BeamCoder, StandardBeamCoders, WindowedValueCoder},
        harness::{
            control::{ControlChannel, ControlResponse},
            data::{DataChannel, DataKey, ElementStreamPayload},
            log::LogChannel,
            state::StateChannel,
        },
        scheduler::Scheduler,
    },
    fusion::{
        pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode},
        stage::ExecutableStage,
    },
    jobservice::urns::beam_urns,
    store::element_store::{FlareElementStore, NewCollectionRequest, ScanCollectionRequest},
    store::record::BeamRecord,
    transforms::{ExecutionContext, FlareRunnerTransform},
    utils::batch_size_estimator::{BatchConfig, BatchSizeEstimator},
};

/// Executor managing worker communication and bundle processing.
/// Owns control and data channels, coordinates input/output element flow.
pub struct StageExecutor {
    /// Control plane
    control: ControlChannel,
    /// Data plane
    data: DataChannel,
    /// Logging channel
    log: LogChannel,
    /// State API channel.
    state: StateChannel,
    /// Pipeline coders.
    pipeline_coders: Arc<HashMap<String, Coder>>,
    /// Pipeline components.
    pipeline_components: Arc<Components>,
    /// Element storage (input/output sets for bundles).
    store: Arc<FlareElementStore>,
    /// Instance ID
    instance_id: String,
}

impl StageExecutor {
    pub async fn new(
        control: ControlChannel,
        data: DataChannel,
        log: LogChannel,
        state: StateChannel,
        instance_id: &str,
    ) -> anyhow::Result<Self> {
        let base_store_path = crate::utils::path::warehouse_dir();
        let base_store_path_str = base_store_path.to_str().unwrap_or(".").to_string();
        let store =
            Arc::new(FlareElementStore::new(base_store_path_str, "flare".to_string()).await?);
        Ok(Self {
            control,
            data,
            log,
            state,
            pipeline_coders: Arc::new(HashMap::new()),
            pipeline_components: Arc::new(Components::default()),
            store,
            instance_id: instance_id.to_string(),
        })
    }

    pub async fn set_job_store(&mut self, job_id: &str) -> anyhow::Result<()> {
        let store_path = crate::utils::path::warehouse_dir();
        let store_base = store_path.to_str().unwrap_or(".").to_string();
        self.store = Arc::new(FlareElementStore::new(store_base, job_id.to_string()).await?);
        Ok(())
    }

    pub async fn wait_connected(&self) -> anyhow::Result<()> {
        self.control.wait_connected().await?;
        Ok(())
    }

    /// Start dispatcher tasks
    pub fn prepare_pipeline(&mut self, pipeline_graph: &ExecutableGraph) {
        // Start data channel dispatcher to listen and demux incoming elements.
        self.data.stream_elements();
        // Start control channel dispatcher to route responses to waiting futures.
        self.control.stream_responses();
        self.pipeline_coders = Arc::new(pipeline_graph.components.coders.clone());
        self.pipeline_components = Arc::new(pipeline_graph.components.clone());
    }

    /// Reset all channels so a new worker can connect.
    pub async fn reset_channels(&self) {
        self.control.reset().await;
        self.data.reset().await;
        self.log.reset().await;
        self.state.reset().await;
        //self.store.reset();
        log::info!("stage executor channels reset");
    }
}

#[async_trait]
pub trait Executor {
    async fn execute(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
    ) -> anyhow::Result<ControlResponse>;
}

#[async_trait]
impl Executor for StageExecutor {
    async fn execute(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
    ) -> anyhow::Result<ControlResponse> {
        self.execute_node(node, input_edge_metadata, output_edge_metadata, None)
            .await
    }
}

pub async fn run_pipeline(
    scheduler: &mut Scheduler,
    executor: Arc<Mutex<StageExecutor>>,
) -> anyhow::Result<()> {
    let mut in_flight = JoinSet::new();

    loop {
        for (idx, node) in scheduler.next_nodes() {
            let executor = executor.clone();
            let input_metadata = scheduler.input_edge_metadata(idx);
            let output_metadata = scheduler.output_edge_metadata(idx);

            in_flight.spawn(async move {
                let mut executor = executor.lock().await;
                // TODO: this fix makes ControlChannel safe for concurrent callers
                // and unblocks switching to Arc<StageExecutor> in a later PR.
                let result = executor
                    .execute(node, input_metadata, output_metadata)
                    .await;
                (idx, result)
            });
        }

        if in_flight.is_empty() {
            if scheduler.is_complete() {
                break;
            }
            return Err(anyhow!(
                "executable graph deadlocked: no ready nodes and no in-flight work"
            ));
        }

        if let Some(joined) = in_flight.join_next().await {
            let (idx, result) = joined?;
            result?;
            scheduler.mark_complete(idx);
        }
    }

    Ok(())
}

impl StageExecutor {
    /// Execute a worker or runner stage node.
    pub async fn execute_node(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
        _instruction_id: Option<String>,
    ) -> anyhow::Result<ControlResponse> {
        match node {
            ExecutableNode::Worker(executable_stage) => {
                info!("Executing worker node");
                let descriptor_id = executable_stage.id().to_string();
                let bundle_status = self.register_bundle(&executable_stage).await;
                info!(
                    "executable_stage input id: {:?}",
                    executable_stage.input_pcol()
                );

                match bundle_status {
                    Ok(response) => {
                        if matches!(response, ControlResponse::BundleRegistered) {
                            info!("Bundle registered at worker");

                            let (instruction_id, bundle_response_rx) = self
                                .control
                                .send_process_bundle_request(&descriptor_id)
                                .await?;

                            info!("Process instruction id {}", instruction_id);

                            // Spawn background task to send input elements to worker.
                            if let Some(meta_data) = &input_edge_metadata {
                                info!("Input edge metadata: {:?}", meta_data.clone());
                            }
                            let output_meta_data = output_edge_metadata;
                            if let Some(meta_data) = &output_meta_data {
                                info!("Output edge metadata: {:?}", meta_data);
                            }

                            let instruction_id_log = instruction_id.clone();

                            let input_coder_id =
                                executable_stage.input_pcol().node().coder_id.clone();

                            let pipeline_coders = self.pipeline_coders.clone();

                            let input_ctx = ProcessInputContext {
                                input_stream: self.data.clone(),
                                store: self.store.clone(),
                                input_instruction_id: instruction_id.clone(),
                                input_pcollection_id: executable_stage.input_pcol().id().clone(),
                                consumer_transform_id: Self::stage_source_transform_id(
                                    &executable_stage,
                                ),
                                input_coder_id,
                                input_component_coder_ids: None,
                                pipeline_coders: pipeline_coders.clone(),
                            };
                            tokio::spawn(async move {
                                if let Err(err) = Self::process_input_elements(input_ctx).await {
                                    error!(
                                        "Failed to send input elements for instruction {}: {}",
                                        instruction_id_log, err
                                    );
                                }
                            });
                            let bundle_response_future = self
                                .control
                                .recv_process_bundle_response(&instruction_id, bundle_response_rx);

                            if let Some(output_meta_data) = output_meta_data {
                                let data_key = DataKey {
                                    instruction_id: instruction_id.clone(),
                                    transform_id: Self::stage_sink_transform_id(
                                        &executable_stage,
                                        &output_meta_data.produced_pcol_id,
                                    ),
                                };
                                // pass data_key to get receiver
                                info!("Data Key: {:?}", data_key);
                                let receiver = self.data.get_receiver(data_key);

                                let store = self.store.clone();
                                let coders = pipeline_coders.clone();

                                let mut decode_task = tokio::spawn(async move {
                                    Self::process_output_elements(
                                        receiver,
                                        output_meta_data,
                                        store,
                                        coders,
                                    )
                                    .await
                                });

                                let timeout_id = instruction_id.clone();
                                let proces_bundle_response = tokio::time::timeout(
                                    Duration::from_secs(60),
                                    async {
                                        tokio::pin!(bundle_response_future);
                                        tokio::select! {
                                            bundle_response = &mut bundle_response_future => {
                                                match bundle_response {
                                                    Ok(response) => {
                                                        decode_task.await.map_err(|err| {
                                                            anyhow!("output decode task failed: {}", err)
                                                        })??;
                                                        Ok(response)
                                                    }
                                                    Err(err) => {
                                                        decode_task.abort();
                                                        Err(err)
                                                    }
                                                }
                                            }
                                            decode_result = &mut decode_task => {
                                                decode_result.map_err(|err| {
                                                    anyhow!("output decode task failed: {}", err)
                                                })??;
                                                bundle_response_future.await
                                            }
                                        }
                                    },
                                )
                                .await
                                .map_err(|_| {
                                    anyhow!(
                                        "timed out waiting for SDK bundle {} output data and control response",
                                        timeout_id
                                    )
                                })??;

                                return Ok(proces_bundle_response);
                            }

                            let timeout_id = instruction_id.clone();
                            let proces_bundle_response = tokio::time::timeout(
                                Duration::from_secs(60),
                                bundle_response_future,
                            )
                            .await
                            .map_err(|_| {
                                anyhow!(
                                    "timed out waiting for SDK bundle {} control response",
                                    timeout_id
                                )
                            })??;
                            return Ok(proces_bundle_response);
                        } else {
                            Ok(ControlResponse::ProcessBundleError(
                                "Error wile registring bundle".to_string(),
                            ))
                        }
                    }
                    Err(err) => {
                        return Err(anyhow!("Error while processing bundle {}", err));
                    }
                }
            }
            ExecutableNode::Runner(runner_transform) => {
                info!("Executing runner node");

                let input_metadata = input_edge_metadata.as_ref();
                let output_metadata = output_edge_metadata.as_ref();

                let input_pcollection_id = Self::metadata_pcollection_id(input_metadata);
                let output_pcollection_id =
                    Self::runner_output_pcollection_id(&runner_transform, output_metadata);
                let consumer_transfrom_id =
                    Self::runner_consumer_transform_id(input_metadata, output_metadata);

                info!("Runner node input metadata: {:?}", input_edge_metadata);
                info!("Runner node output metadata: {:?}", output_edge_metadata);

                let endpoint = ApiServiceDescriptor {
                    url: crate::DEFAULT_API_SERVICE_URL.to_string(),
                    ..Default::default()
                };

                let descriptor = ProcessBundleDescriptor {
                    id: runner_transform.id(),
                    transforms: runner_transform.transfrom_spec(),
                    pcollections: runner_transform.pcollections(&self.pipeline_components),
                    windowing_strategies: runner_transform.windowing_strategies(),
                    coders: runner_transform.coders(),
                    environments: runner_transform.environments(),
                    state_api_service_descriptor: Some(endpoint.clone()),
                    timer_api_service_descriptor: Some(endpoint),
                };

                let bundle_status = self.control.register_bundle(descriptor).await;

                match bundle_status {
                    Ok(response) => {
                        if matches!(response, ControlResponse::BundleRegistered) {
                            info!("Runer bundle registred at worker");
                            let ctx = ExecutionContext {
                                store: self.store.clone(),
                                input_pcollection_id,
                                output_pcollection_id,
                                consumer_transfrom_id,
                            };

                            runner_transform.execute(ctx).await?;
                        } else {
                        }
                    }
                    Err(err) => {
                        return Err(anyhow!("Error while processing bundle {}", err));
                    }
                };

                Ok(ControlResponse::BundleDone)
            }
        }
    }

    fn metadata_pcollection_id(metadata: Option<&ConsumerMetaData>) -> String {
        metadata
            .expect("Runner node must have at least one available metadata source")
            .produced_pcol_id
            .clone()
    }

    fn runner_output_pcollection_id(
        runner_transform: &FlareRunnerTransform,
        output_metadata: Option<&ConsumerMetaData>,
    ) -> String {
        output_metadata
            .map(|meta| meta.produced_pcol_id.clone())
            .or_else(|| runner_transform.output_pcol_ids().into_iter().next())
            .expect("Runner transform must have an output pcollection id")
    }

    fn runner_consumer_transform_id(
        input_metadata: Option<&ConsumerMetaData>,
        output_metadata: Option<&ConsumerMetaData>,
    ) -> String {
        output_metadata
            .or(input_metadata)
            .expect("Runner node must have available transform metadata")
            .consumer_transfrom_id
            .clone()
    }

    fn stage_source_transform_id(stage: &ExecutableStage) -> String {
        format!("{}/source", stage.id())
    }

    fn stage_sink_transform_id(stage: &ExecutableStage, pcollection_id: &str) -> String {
        format!("{}/sink/{}", stage.id(), pcollection_id)
    }

    fn remote_grpc_port(endpoint: ApiServiceDescriptor, coder_id: String) -> RemoteGrpcPort {
        RemoteGrpcPort {
            api_service_descriptor: Some(endpoint),
            coder_id,
        }
    }

    fn global_window_coder_id(stage: &ExecutableStage) -> String {
        format!("{}/global_window", stage.id())
    }

    fn windowed_value_coder_id(stage: &ExecutableStage, pcollection_id: &str) -> String {
        format!("{}/windowed_value/{}", stage.id(), pcollection_id)
    }

    fn insert_windowed_value_coder(
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

    fn add_stage_data_boundary_coders(
        stage: &ExecutableStage,
        coders: &mut HashMap<String, Coder>,
    ) {
        let global_window_coder_id = Self::global_window_coder_id(stage);
        let input_pcol = stage.input_pcol();

        Self::insert_windowed_value_coder(
            coders,
            Self::windowed_value_coder_id(stage, input_pcol.id()),
            input_pcol.node().coder_id.clone(),
            global_window_coder_id.clone(),
        );

        for output_pcol in stage.output_pcols() {
            Self::insert_windowed_value_coder(
                coders,
                Self::windowed_value_coder_id(stage, output_pcol.id()),
                output_pcol.node().coder_id.clone(),
                global_window_coder_id.clone(),
            );
        }
    }

    /// Add stage's source and sink boundary( basically tells the worker where a stage begins and ends)
    fn stage_transforms_with_data_boundaries(
        stage: &ExecutableStage,
        endpoint: ApiServiceDescriptor,
    ) -> HashMap<String, PTransform> {
        let mut transforms = stage.ptmap();

        let input_pcol = stage.input_pcol();
        let source_id = Self::stage_source_transform_id(stage);
        let input_element_coder_id = input_pcol.node().coder_id.clone();
        let input_wire_coder_id = Self::windowed_value_coder_id(stage, input_pcol.id());
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
                    payload: Self::remote_grpc_port(endpoint.clone(), input_wire_coder_id)
                        .encode_to_vec(),
                }),
                inputs: HashMap::new(),
                outputs: HashMap::from([("local_output".to_string(), input_pcol.id().clone())]),
                ..Default::default()
            },
        );

        for output_pcol in stage.output_pcols() {
            let sink_id = Self::stage_sink_transform_id(stage, output_pcol.id());
            let output_element_coder_id = output_pcol.node().coder_id.clone();
            let output_wire_coder_id = Self::windowed_value_coder_id(stage, output_pcol.id());
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
                        payload: Self::remote_grpc_port(endpoint.clone(), output_wire_coder_id)
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

    pub async fn register_bundle(
        &mut self,
        stage: &ExecutableStage,
    ) -> Result<ControlResponse, anyhow::Error> {
        let endpoint = ApiServiceDescriptor {
            url: crate::DEFAULT_API_SERVICE_URL.to_string(),
            ..Default::default()
        };

        let transforms = Self::stage_transforms_with_data_boundaries(stage, endpoint.clone());
        let mut components = stage.components();
        Self::add_stage_data_boundary_coders(stage, &mut components.coders);

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

    async fn process_output_elements(
        receiver: Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>>,
        edge_metadata: ConsumerMetaData,
        store: Arc<FlareElementStore>,
        pipeline_coders: Arc<HashMap<String, Coder>>,
    ) -> anyhow::Result<()> {
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
                                stream_buffer.advance(consumed);

                                total_decoded += 1;
                                batch.push(windowed_value.value);
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

    async fn process_input_elements(ctx: ProcessInputContext) -> anyhow::Result<()> {
        info!("Spawned task to send stage's input elements to worker");
        info!(
            "Sending input elements: instruction_id={}, transform_id={}",
            ctx.input_instruction_id, ctx.consumer_transform_id,
        );

        let request = ScanCollectionRequest {
            pcollection_id: ctx.input_pcollection_id,
        };

        let elements = ctx.store.scan_collection(request).await?;
        info!("Input element coder: {}", ctx.input_coder_id);

        let element_coder = StandardBeamCoders::from_urn(
            ctx.input_coder_id.as_str(),
            ctx.input_component_coder_ids.clone(),
            Some(ctx.pipeline_coders.as_ref()),
        );
        let windowed_value_coder = WindowedValueCoder::new(element_coder);
        let mut encoded = BytesMut::new();

        for element in elements {
            windowed_value_coder.encode_value(element, &mut encoded);
        }

        let elements = Elements {
            data: vec![elements::Data {
                instruction_id: ctx.input_instruction_id,
                transform_id: ctx.consumer_transform_id,
                data: encoded.freeze().to_vec(),
                is_last: true,
            }],
            timers: Vec::new(),
        };

        ctx.input_stream.send_elements(elements).await?;
        info!("Finished sending input elements to worker");
        Ok(())
    }
}

/// Context for process_input_elements task
pub struct ProcessInputContext {
    input_stream: DataChannel,
    store: Arc<FlareElementStore>,
    input_instruction_id: String,
    consumer_transform_id: String,
    input_pcollection_id: String,
    input_coder_id: String,
    input_component_coder_ids: Option<Vec<String>>,
    pipeline_coders: Arc<HashMap<String, Coder>>,
}
