use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{
    GetProcessBundleDescriptorRequest, InstructionRequest, InstructionResponse,
    ProcessBundleDescriptor, ProcessBundleRequest, ProcessBundleResponse, RegisterRequest,
    beam_fn_control_server::BeamFnControl, instruction_request,
};
use dashmap::DashMap;
use log::{info, warn};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

/// Shared gRPC channel for sending instructions to the worker.
pub struct ControlInner {
    // Sender — ControlChannel writes InstructionRequests here.
    pub outgoing_tx: Mutex<Option<mpsc::Sender<Result<InstructionRequest, Status>>>>,

    // Receiver — taken by FlareControlService::control() and handed to the
    // worker as a ReceiverStream.
    pub outgoing_rx: Mutex<Option<mpsc::Receiver<Result<InstructionRequest, Status>>>>,

    // Incoming gRPC stream from the worker (InstructionResponses).
    pub incoming: Mutex<Option<tonic::Streaming<InstructionResponse>>>,

    // ProcessBundleDescriptors registered with the worker.
    pub descriptors: Mutex<HashMap<String, ProcessBundleDescriptor>>,

    // Pending responses from the worker, keyed by instruction_id.
    pub pending: DashMap<String, oneshot::Sender<InstructionResponse>>,
}

impl ControlInner {
    // Clone the sender so callers can send without holding the lock.
    async fn sender(&self) -> Result<mpsc::Sender<Result<InstructionRequest, Status>>> {
        self.outgoing_tx
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow!("control outgoing channel not initialized"))
    }
}

// entry point

pub async fn start_control_server() -> Result<(ControlChannel, FlareControlService)> {
    let (tx, rx) = mpsc::channel::<Result<InstructionRequest, Status>>(32);

    let stream = Arc::new(ControlInner {
        outgoing_tx: Mutex::new(Some(tx)),
        outgoing_rx: Mutex::new(Some(rx)),
        incoming: Mutex::new(None),
        descriptors: Mutex::new(HashMap::new()),
        pending: DashMap::new(),
    });

    let service = FlareControlService {
        inner: stream.clone(),
    };

    let channel = ControlChannel {
        stream,
        next_id: 0,
        response_task: Arc::new(std::sync::Mutex::new(None)),
    };

    Ok((channel, service))
}

pub struct FlareControlService {
    pub inner: Arc<ControlInner>,
}

impl BeamFnControl for FlareControlService {
    #[doc = " Server streaming response type for the Control method."]
    //type ControlStream;
    type ControlStream = ReceiverStream<Result<InstructionRequest, Status>>;

    #[doc = " Instructions sent by the runner to the SDK requesting different types"]
    #[doc = " of work."]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn control<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<tonic::Streaming<InstructionResponse>>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<Self::ControlStream>,
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
            // store the incoming stream
            // ControlChannel.recv_response() will read from it directly
            // persisted in Arc<ControlServiceInner> so it outlives control()
            *self.inner.incoming.lock().await = Some(request.into_inner());

            // Take the rx end of the request channel — this is the stream
            // of InstructionRequests the worker will read.
            let rx = {
                let mut rx_guard = self.inner.outgoing_rx.lock().await;
                // If outgoing_rx is None, a previous worker already took it
                // and disconnected without a clean reset. Create a fresh
                // channel pair so the new worker gets a working stream
                // instead of crashing with "worker connected twice".
                if rx_guard.is_none() {
                    warn!(
                        "Control stream connected while a previous worker stream was still active; replacing stale stream"
                    );
                    let (tx, rx) = mpsc::channel::<Result<InstructionRequest, Status>>(32);
                    *self.inner.outgoing_tx.lock().await = Some(tx);
                    *rx_guard = Some(rx);
                }
                // Take ownership of the receiver and return it as a streaming
                // gRPC response. After this, outgoing_rx is None again until
                // the worker is reset for the next job.
                rx_guard
                    .take()
                    .expect("control outgoing receiver must be initialized")
            };

            Ok(Response::new(ReceiverStream::new(rx)))
        })
    }

    #[doc = " Used to get the full process bundle descriptors for bundles one"]
    #[doc = " is asked to process."]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_process_bundle_descriptor<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<GetProcessBundleDescriptorRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<ProcessBundleDescriptor>,
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
            let id = request.into_inner().process_bundle_descriptor_id;

            let guard = self.inner.descriptors.lock().await;
            let descriptor = guard
                .get(&id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("no descriptor for id {}", id)))?;

            Ok(Response::new(descriptor))
        })
    }
}

#[derive(PartialEq)]
pub enum ControlResponse {
    BundleRegistered,
    ProcessBundleSuccess(ProcessBundleResponse),
    ProcessBundleError(String),
    BundleDone,
}

/// ControlChannel: Flare → worker  (InstructionRequests)
///                worker → Flare  (InstructionResponses, via inner.incoming)
///
/// The sender lives inside the shared `ControlInner` so it can be replaced
/// when a worker disconnects and a new one connects.
#[derive(Clone)]
pub struct ControlChannel {
    /// Shared state with FlareControlService (the gRPC handler).
    pub stream: Arc<ControlInner>,
    pub next_id: u64,
    /// Handle to the background response dispatch task. Aborted and awaited on
    /// during reset so a stale task from the previous job does not race with
    /// the new worker stream.
    response_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl ControlChannel {
    // wait for worker to connect
    // poll until control() fires and stores the incoming stream
    /// Wait for the worker to connect its control stream.
    pub async fn wait_connected(&self) -> Result<()> {
        loop {
            if self.stream.incoming.lock().await.is_some() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Reset the control channel so a new worker can connect.
    //
    // Creates a fresh mpsc pair, clears the incoming stream, and drops all
    // previously registered descriptors.  Call this between jobs (after
    // killing the old worker, before spawning the new one).
    pub async fn reset(&self) {
        let handle = self.response_task.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }

        let (tx, rx) = mpsc::channel::<Result<InstructionRequest, Status>>(32);

        *self.stream.outgoing_tx.lock().await = Some(tx);
        *self.stream.outgoing_rx.lock().await = Some(rx);
        *self.stream.incoming.lock().await = None;
        self.stream.descriptors.lock().await.clear();
        self.stream.pending.clear();

        log::info!("control channel reset for next worker");
    }

    // register stage descriptor with worker
    // sends InstructionRequest { register: descriptor }
    // waits for InstructionResponse ack
    // called once per stage at startup
    pub async fn register_bundle(
        &mut self,
        descriptor: ProcessBundleDescriptor,
    ) -> Result<ControlResponse> {
        let id = self.next_id();
        let response_rx = self.insert_pending_response(id.clone());

        self.stream
            .descriptors
            .lock()
            .await
            .insert(descriptor.id.clone(), descriptor.clone());

        let sender = self.stream.sender().await?;
        let send_result = sender
            .send(Ok(InstructionRequest {
                instruction_id: id.clone(),
                request: Some(instruction_request::Request::Register(RegisterRequest {
                    process_bundle_descriptor: vec![descriptor],
                })),
            }))
            .await;

        if let Err(e) = send_result {
            self.stream.pending.remove(&id);
            return Err(anyhow!("failed to send register request: {}", e));
        }

        // wait for ack
        let response = response_rx.await.map_err(|_| {
            anyhow!(
                "control dispatcher reset or worker disconnected while awaiting register ack {}",
                id
            )
        })?;

        if response.instruction_id != id {
            return Err(anyhow!(
                "register ack id mismatch: expected {} got {}",
                id,
                response.instruction_id
            ));
        }

        match response.response {
            Some(beam_model_rs::v1::instruction_response::Response::Register(_)) => {
                Ok(ControlResponse::BundleRegistered)
            }
            other => {
                if !response.error.is_empty() {
                    return Err(anyhow!("register failed at worker: {}", response.error));
                }
                Err(anyhow!("unexpected register response: {:?}", other))
            }
        }
    }

    // tell worker to start a bundle
    // sends InstructionRequest { process_bundle: descriptor_id }
    // returns bundle_id so caller can match the response later
    // called every bundle
    pub async fn send_process_bundle_request(
        &mut self,
        descriptor_id: &String,
    ) -> Result<(String, oneshot::Receiver<InstructionResponse>)> {
        let id = self.next_id();
        let response_rx = self.insert_pending_response(id.clone());

        let sender = self.stream.sender().await?;
        let send_result = sender
            .send(Ok(InstructionRequest {
                instruction_id: id.clone(),
                request: Some(instruction_request::Request::ProcessBundle(
                    ProcessBundleRequest {
                        process_bundle_descriptor_id: descriptor_id.to_string(),
                        ..Default::default()
                    },
                )),
            }))
            .await;

        if let Err(e) = send_result {
            self.stream.pending.remove(&id);
            return Err(anyhow!("failed to send process bundle request: {}", e));
        }

        Ok((id, response_rx))
    }

    // wait for worker to confirm bundle complete
    // blocks until ProcessBundleResponse arrives on control channel
    // called after sending elements on data channel
    pub async fn recv_process_bundle_response(
        &self,
        bundle_id: &str,
        response_rx: oneshot::Receiver<InstructionResponse>,
    ) -> Result<ControlResponse> {
        info!("Polling for process bundle response");
        let response = response_rx.await.map_err(|_| {
            anyhow!(
                "control dispatcher reset or worker disconnected while awaiting instruction {}",
                bundle_id
            )
        })?;

        if response.instruction_id != bundle_id {
            return Err(anyhow!(
                "bundle response id mismatch: expected {} got {}",
                bundle_id,
                response.instruction_id
            ));
        }

        match response.response {
            Some(beam_model_rs::v1::instruction_response::Response::ProcessBundle(res)) => {
                Ok(ControlResponse::ProcessBundleSuccess(res))
            }
            other => {
                if !response.error.is_empty() {
                    return Err(anyhow!(
                        "process bundle failed at worker: {}",
                        response.error
                    ));
                }
                Err(anyhow!("unexpected bundle response: {:?}", other))
            }
        }
    }

    fn insert_pending_response(&self, id: String) -> oneshot::Receiver<InstructionResponse> {
        let (sender, receiver) = oneshot::channel();
        self.stream.pending.insert(id, sender);
        receiver
    }

    pub fn stream_responses(&self) {
        let stream = self.stream.clone();
        let task_slot = self.response_task.clone();
        info!("Streaming control responses from worker");

        let join_handle = tokio::spawn(async move {
            loop {
                let response = {
                    let mut guard = stream.incoming.lock().await;
                    let Some(response_stream) = &mut *guard else {
                        drop(guard);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    };

                    response_stream.message().await
                };

                match response {
                    Ok(Some(response)) => {
                        let instruction_id = response.instruction_id.clone();
                        if let Some((_, sender)) = stream.pending.remove(&instruction_id) {
                            if sender.send(response).is_err() {
                                warn!(
                                    "control response receiver dropped for instruction_id={}",
                                    instruction_id
                                );
                            }
                        } else {
                            warn!(
                                "no pending control receiver for instruction_id={}",
                                instruction_id
                            );
                        }
                        continue;
                    }
                    Ok(None) => {
                        info!("BeamFnControl stream from worker closed cleanly");
                        break;
                    }
                    Err(e) => {
                        warn!("BeamFnControl stream error (worker gone?): {}", e);
                        break;
                    }
                }
            }

            info!("BeamFnControl stream from worker closed");
        });

        *task_slot.lock().unwrap() = Some(join_handle);
    }

    // generate unique instruction ids
    fn next_id(&mut self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
