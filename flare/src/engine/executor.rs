use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::{Error, anyhow};
use beam_model_rs::v1::{ApiServiceDescriptor, Elements, ProcessBundleDescriptor, elements};
use bytes::{Buf, BytesMut};
use dashmap::DashMap;
use log::{error, info};
use petgraph::{Direction, graph::NodeIndex};
use prost::Message;
use tokio::sync::{
    Mutex,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
};

use crate::{
    engine::{
        coders::{BeamValue, StandardCoders},
        harness::{
            control::{ControlChannel, ControlResponse},
            data::{DataChannel, DataKey, ElementStreamPayload},
        },
    },
    fusion::{
        pipeline::{ConsumerMetaData, ExecutableGraph, ExecutableNode},
        stage::ExecutableStage,
    },
    transforms::ExecutionContext,
};

pub struct StageExecutor {
    control: ControlChannel,
    data: DataChannel,
    store: Arc<ElementStore>,
    writer_stream_sender: UnboundedSender<Elements>,
    graph: Option<ExecutableGraph>, // ows the Scheduler and asks it to give the next element to execute in
                                    // execute_pipeline and calls execute_node to execute that node
}

impl StageExecutor {
    pub fn new(control: ControlChannel, data: DataChannel) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        data.send_stream(Mutex::new(rx));
        Self {
            control,
            data,
            store: Arc::new(ElementStore::new()),
            writer_stream_sender: tx,
            graph: None,
        }
    }

    pub async fn wait_connected(&self) -> anyhow::Result<()> {
        self.control.wait_connected().await?;
        Ok(())
    }

    pub async fn execute_pipeline(
        &mut self,
        pipeline_graph: &ExecutableGraph,
    ) -> Result<(), Error> {
        info!("Starting to execute pipeline");
        // Spin up channels to start listening for data from worker before node execution
        self.data.stream_elements();

        let graph = pipeline_graph.get_executable_graph();

        self.graph = Some(pipeline_graph.clone());

        // Root node = node with no incoming edges
        let root = graph
            .node_indices()
            .find(|&idx| {
                graph
                    .neighbors_directed(idx, Direction::Incoming)
                    .next()
                    .is_none()
            })
            .ok_or_else(|| anyhow::anyhow!("no root node found"))?;

        let mut current_level = VecDeque::<NodeIndex>::new();
        current_level.push_front(root);
        // vec![root];

        // executed nodes
        let mut executed = HashSet::<NodeIndex>::new();

        while !current_level.is_empty() {
            let mut next_level = HashSet::<NodeIndex>::new();

            // List of graph nodeindex and nodes in current level
            let executable_nodes: Vec<(NodeIndex, ExecutableNode)> = current_level
                .iter()
                .map(|&idx| (idx, graph[idx].clone()))
                .collect();
            info!("Picked up nodes for current level ");

            for (idx, node) in executable_nodes {
                // Skip already executed nodes
                // but there is a bug if we add it to the executed list but the node fail to execute
                // then we'd produce an incorrect list, or we just fail eveything if the node fails
                if !executed.insert(idx) {
                    continue;
                }

                let incoming_edge = graph
                    .edges_directed(idx, Direction::Incoming)
                    .next()
                    .map(|e| e.weight().clone());
                let outgoing_edge = graph
                    .edges_directed(idx, Direction::Outgoing)
                    .next()
                    .map(|e| e.weight().clone());

                // Execute node
                self.execute_node(node, incoming_edge, outgoing_edge, None)
                    .await?;

                // Schedule downstream consumers
                for downstream in graph.neighbors_directed(idx, Direction::Outgoing) {
                    if !executed.contains(&downstream) {
                        next_level.insert(downstream);
                    }
                }
            }

            current_level = next_level.into_iter().collect();
        }
        Ok(())
        // stream_elements write elements to the mpmc channel that StageExecutor ownes
        // later on we read the elements in execute_node by polling element of that instruction id
        // and deserilized or writeten to tonbo as a input to next stage or
        // to perform whatever the operation is like gbk or cgbk etc..

        // Code Pocl metdata into Consumer edge so that we could emit the right pocl to the next stage
        // as input

        // decide here on how to send eleemnts to next stage to the worker
        // use the edge pocl metadata

        // call the scheduler to decide on which node to execute next based on edge connection
    }

    pub async fn execute_node(
        &mut self,
        node: ExecutableNode,
        input_edge_metadata: Option<ConsumerMetaData>,
        output_edge_metadata: Option<ConsumerMetaData>,
        instruction_id: Option<String>,
    ) -> anyhow::Result<ControlResponse> {
        // return node reponse instred of control res
        // create execution context for runner / worker node

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
                            info!("Bundle registered at worker)",);

                            let instruction_id = self
                                .control
                                .send_process_bundle_request(&descriptor_id)
                                .await?;

                            info!("Process instruction id {}", instruction_id);

                            if let Some(meta_data) = &input_edge_metadata {
                                info!("Input edge metadata: {:?}", meta_data.clone());
                            }
                            let output_meta_data = output_edge_metadata;
                            if let Some(meta_data) = &output_meta_data {
                                info!("Output edge metadata: {:?}", meta_data.clone());
                            }

                            let stage_input = executable_stage.input_pcol();
                            let input_pcollection_id = stage_input.id().clone();
                            let input_coder_id = stage_input.node().coder_id.clone();
                            let input_consumer_transform_id =
                                Self::get_stage_input_consumer_transform_id(
                                    &executable_stage,
                                    &input_pcollection_id,
                                )?;
                            let input_instruction_id = instruction_id.clone();
                            let store = self.store.clone();
                            let input_stream = self.data.clone();
                            let input_instruction_id_for_log = input_instruction_id.clone();

                            tokio::spawn(async move {
                                if let Err(err) = Self::process_input_elements(
                                    input_stream,
                                    store,
                                    input_instruction_id,
                                    input_consumer_transform_id,
                                    input_pcollection_id,
                                    input_coder_id,
                                )
                                .await
                                {
                                    error!(
                                        "Failed to send input elements for instruction {}: {}",
                                        input_instruction_id_for_log, err
                                    );
                                }
                            });
                            let bundle_response_future =
                                self.control.recv_process_bundle_response(&instruction_id);

                            if let Some(output_meta_data) = output_meta_data {
                                let data_key = DataKey {
                                    instruction_id: instruction_id.clone(),
                                    transform_id: output_meta_data.producer_transform_id.clone(),
                                };
                                // pass data_key to get resiciver
                                info!("Data Key: {:?}", data_key);
                                let receiver = self.data.get_receiver(data_key);

                                let store = self.store.clone();

                                let decode_task = tokio::spawn(async move {
                                    Self::process_output_elements(receiver, output_meta_data, store)
                                        .await
                                });

                                let (decode_result, bundle_response) =
                                    tokio::join!(decode_task, bundle_response_future);

                                decode_result?;
                                let proces_bundle_response = bundle_response?;

                                return Ok(proces_bundle_response);
                            }

                            let proces_bundle_response = bundle_response_future.await?;
                            return Ok(proces_bundle_response);
                            //TODO return back instruction id as a callback

                            // return Ok(proces_bundle_response);

                            // get recivert, start reading elements from channel
                            // and persist it for sending it to next stage inputs

                            //let elements = self.data.stream_elements();
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

                //Ok(())
            }
            ExecutableNode::Runner(runner_transform) => {
                info!("Executing runner node");
                if let Some(graph) = &self.graph {
                    let meta = output_edge_metadata
                        .as_ref()
                        .or(input_edge_metadata.as_ref())
                        .unwrap_or_else(|| graph.get_root_metadata());

                    info!("Runner node metadata: {:?}", meta);

                    let endpoint = ApiServiceDescriptor {
                        url: "127.0.0.1:8099".to_string(),
                        ..Default::default()
                    };

                    // TODO : build process bundle descriptor for the runner transfrom
                    // send register request to harness and then execute the runner transfrom

                    let descriptor = ProcessBundleDescriptor {
                        id: runner_transform.id(),
                        transforms: runner_transform.transfrom_spec(),
                        pcollections: runner_transform.pcollections(&graph.components),
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
                                    pcollection_id: meta.produced_pcol_id.clone(),
                                    consumer_transfrom_id: meta.consumer_transfrom_id.clone(),
                                };

                                runner_transform.execute(ctx);
                            } else {
                            }
                        }
                        Err(err) => {
                            return Err(anyhow!("Error while processing bundle {}", err));
                        }
                    };
                }

                Ok(ControlResponse::BundleDone)
            }
        }
    }

    fn get_stage_input_consumer_transform_id(
        stage: &ExecutableStage,
        stage_input_id: &str,
    ) -> anyhow::Result<String> {
        stage
            .transforms()
            .into_iter()
            .find(|transform| {
                transform
                    .node()
                    .inputs
                    .values()
                    .any(|input| input == stage_input_id)
            })
            .map(|transform| transform.node().unique_name.clone())
            .ok_or_else(|| {
                anyhow!(
                    "consumer transform not found for stage input pcollection {}",
                    stage_input_id
                )
            })
    }

    pub async fn register_bundle(
        &mut self,
        stage: &ExecutableStage,
    ) -> Result<ControlResponse, anyhow::Error> {
        let endpoint = ApiServiceDescriptor {
            url: "127.0.0.1:8099".to_string(),
            ..Default::default()
        };

        let descriptor = ProcessBundleDescriptor {
            id: stage.id().to_string(),
            transforms: stage.ptmap(),
            pcollections: stage.components().pcollections,
            windowing_strategies: stage.components().windowing_strategies,
            coders: stage.components().coders,
            environments: stage.components().environments,
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
        store: Arc<ElementStore>,
    ) {
        info!("Spawaned task to process stage's output elements");
        let mut stream_buffer = BytesMut::new();

        // we can't let the loop keep running all the time, use while let
        loop {
            info!("Inside the process_output_elements loop ");
            let payload = {
                let mut receiver_lock = receiver.lock().await;
                receiver_lock.recv().await
            };
            info!("Got payload");

            let Some(payload) = payload else {
                info!("payload is empty");
                break;
            };
            info!("Got payload");

            match payload {
                ElementStreamPayload::Data(data_chunk) => {
                    info!("processing data chunk");
                    stream_buffer.extend_from_slice(&data_chunk.data.data);

                    if data_chunk.data.is_last {
                        info!("Last data chunk");
                        let coder = StandardCoders::from_urn(&edge_metadata.coder_id);

                        let mut decoded = Vec::<BeamValue>::new();
                        let mut buf = stream_buffer.freeze();

                        while buf.has_remaining() {
                            decoded.push(coder.decode(&mut buf));
                        }

                        let req = NewCollectionRequest {
                            pcollection_id: edge_metadata.produced_pcol_id.clone(),
                            elements: decoded,
                        };

                        store.insert_new_collection(req);

                        break;
                    }
                }

                ElementStreamPayload::Timers(_timer_chunk) => {
                    //todo!()
                    info!("Timers chunk");
                }
            }
        }
    }

    pub async fn process_input_elements(
        input_stream: DataChannel,
        store: Arc<ElementStore>,
        instruction_id: String,
        consumer_transform_id: String,
        pcollection_id: String,
        coder_id: String,
    ) -> anyhow::Result<()> {
        info!("Spawnned task to send stage's input elemenets to worker");
        info!(
            "Sending input elements: instruction_id={}, transform_id={}",
            instruction_id, consumer_transform_id,
        );
        let request = GetCollectionRequest { pcollection_id };

        let elements = store.get_collection(request);
        info!("Input coder: {}", coder_id);

        let coder = StandardCoders::from_urn(coder_id.as_str());
        let mut encoded = BytesMut::new();

        for element in &elements {
            info!("Starting to encode");
            coder.encode(element, &mut encoded);
        }

        let elements = Elements {
            data: vec![elements::Data {
                instruction_id,
                transform_id: consumer_transform_id,
                data: encoded.freeze().to_vec(),
                is_last: true,
            }],
            timers: Vec::new(),
        };

        input_stream.send_elements(elements).await?;
        info!("Finished sending input elements to worker");
        Ok(())
    }
}
// decode the data chunks.

// transform_id in the data payload is the the id of the transfrom that produced the pcollection
// so we could use transform_id to get the pcol id that it producted and then get the
// decoder for that pcol and decode and process the elemenets.
// decoing should be async task so we can spwan parallel task to the job
// check out how beam does decoding
pub struct ElementStore {
    // {transfrom_id -> {pcol_id, pcol values}}
    // transfrom_id is nothing but a stage and a stage can produce multiple
    // pcollections so, transfrom_id maps to list of pcollection_id that it prodoced
    // and its pcollections
    data: Arc<DashMap<String, Vec<BeamValue>>>,
}

impl ElementStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }

    pub fn insert_new_collection(&self, request: NewCollectionRequest) {
        info!("Inserting new collection into store, request {:?}", request);
        self.data.insert(request.pcollection_id, request.elements);
    }

    pub fn get_collection(&self, request: GetCollectionRequest) -> Vec<BeamValue> {
        info!("Get Collection request: {:?}", request);
        self.data
            .get(&request.pcollection_id)
            .map(|elements| elements.clone())
            .unwrap_or_default()
    }
}

#[derive(Debug)]
pub struct NewCollectionRequest {
    // consumed pcollection id
    pub(crate) pcollection_id: String,
    // collection
    pub(crate) elements: Vec<BeamValue>,
}

#[derive(Debug)]
pub struct GetCollectionRequest {
    pcollection_id: String,
}

// TODO
// let execute() handle the instruction
// persist the bundle id and instruction
// and add data fn in executor to just listen to data that harness is sending
// store it inmemory in hashmap of bundleid and elmeennts
// once all elemeents are scived start next stage and pass the stored elements as input to it.

/*
pub fn log_data(elements: &[Elements]) {
     info!("Logging elements: total_messages={}", elements.len());
     for (idx, msg) in elements.iter().enumerate() {
         info!(
             "elements message {}: data_entries={}, timer_entries={}",
             idx,
             msg.data.len(),
             msg.timers.len()
         );
         for data in &msg.data {
             let decoded = Self::decode_strings(&data.data);
             log::info!(
                 "[data] instruction={} transform={} is_last={} elements={:?}",
                 data.instruction_id,
                 data.transform_id,
                 data.is_last,
                 decoded,
             );
         }
         for timer in &msg.timers {
             info!(
                 "[timer] instruction={} transform={} timer_family_id={} is_last={}",
                 timer.instruction_id, timer.transform_id, timer.timer_family_id, timer.is_last
             );
         }
     }
 }

 fn decode_strings(raw: &[u8]) -> Vec<String> {
     info!("decoing strings");
     use std::io::{Cursor, Read};

     let mut cursor = Cursor::new(raw);
     let mut out = Vec::new();

     while cursor.position() < raw.len() as u64 {
         let mut len_buf = [0u8; 4];
         if cursor.read_exact(&mut len_buf).is_err() {
             break;
         }
         let len = u32::from_be_bytes(len_buf) as usize;
         let mut buf = vec![0u8; len];
         if cursor.read_exact(&mut buf).is_err() {
             break;
         }
         match String::from_utf8(buf) {
             Ok(s) => out.push(s),
             Err(_) => out.push("<invalid utf8>".to_string()),
         }
     }

     out
 } */
