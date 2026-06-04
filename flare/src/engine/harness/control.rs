use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{
    ApiServiceDescriptor, GetProcessBundleDescriptorRequest, InstructionRequest,
    InstructionResponse, ProcessBundleDescriptor, ProcessBundleRequest, ProcessBundleResponse,
    RegisterRequest, beam_fn_control_server::BeamFnControl, instruction_request,
};
use log::info;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

//type Result<T> = std::result::Result<T, HarnessError>;

// shared inner state
// Arc so both FlareControlService (owned by tonic)
// and ControlChannel (owned by stage_executor) see the same state

pub struct ControlInner {
    // rx end — handed to harness as ReceiverStream
    // Flare writes InstructionRequests to the tx end
    pub outgoing: Mutex<Option<mpsc::Receiver<Result<InstructionRequest, Status>>>>,

    // the live gRPC stream from harness
    // harness writes InstructionResponses into this
    // ControlChannel reads from it directly
    pub incoming: Mutex<Option<tonic::Streaming<InstructionResponse>>>,
    pub descriptors: Mutex<HashMap<String, ProcessBundleDescriptor>>,
}

// entry point

pub async fn start_control_server() -> Result<(ControlChannel, FlareControlService)> {
    let (tx, rx) = mpsc::channel::<Result<InstructionRequest, Status>>(32);

    let stream = Arc::new(ControlInner {
        //reciver
        outgoing: Mutex::new(Some(rx)),
        incoming: Mutex::new(None),
        descriptors: Mutex::new(HashMap::new()),
    });

    let service = FlareControlService {
        inner: stream.clone(),
    };

    let channel = ControlChannel {
        // sender
        outgoing: tx,
        stream,
        next_id: 0,
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

            // take the rx end of the request channel
            // wrap in ReceiverStream and return to harness
            // harness will read InstructionRequests from this stream
            // Flare writes to the tx end via ControlChannel
            let rx = self
                .inner
                .outgoing
                .lock()
                .await
                .take()
                .ok_or_else(|| Status::internal("harness connected twice"))?;

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
// ControlChannel
// Flare → harness:  request_tx  (InstructionRequests)
// harness → Flare:  inner.incoming (InstructionResponses, read directly)
#[derive(Clone)]
pub struct ControlChannel {
    // write end — Flare sends InstructionRequests to harness
    pub outgoing: mpsc::Sender<Result<InstructionRequest, Status>>,
    // shared with FlareControlService
    // incoming stream stored here when harness connects
    pub stream: Arc<ControlInner>,
    pub next_id: u64,
}

impl ControlChannel {
    // wait for harness to connect
    // poll until control() fires and stores the incoming stream
    pub async fn wait_connected(&self) -> Result<()> {
        loop {
            if self.stream.incoming.lock().await.is_some() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // register stage descriptor with harness
    // sends InstructionRequest { register: descriptor }
    // waits for InstructionResponse ack
    // called once per stage at startup
    pub async fn register_bundle(
        &mut self,
        descriptor: ProcessBundleDescriptor,
    ) -> Result<ControlResponse> {
        let id = self.next_id();

        self.stream
            .descriptors
            .lock()
            .await
            .insert(descriptor.id.clone(), descriptor.clone());

        self.outgoing
            .send(Ok(InstructionRequest {
                instruction_id: id.clone(),
                request: Some(instruction_request::Request::Register(RegisterRequest {
                    process_bundle_descriptor: vec![descriptor],
                })),
            }))
            .await
            .map_err(|e| anyhow!("failed to send register request: {}", e))?;

        // wait for ack
        let response = self.recv_response().await?;

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
                    return Err(anyhow!("register failed at harness: {}", response.error));
                }
                Err(anyhow!("unexpected register response: {:?}", other))
            }
        }
    }

    // tell harness to start a bundle
    // sends InstructionRequest { process_bundle: descriptor_id }
    // returns bundle_id so caller can match the response later
    // called every bundle
    pub async fn send_process_bundle_request(&mut self, descriptor_id: &String) -> Result<String> {
        let id = self.next_id();

        let endpoint = ApiServiceDescriptor {
            url: "127.0.0.1:8099".to_string(),
            ..Default::default()
        };

        self.outgoing
            .send(Ok(InstructionRequest {
                instruction_id: id.clone(),
                request: Some(instruction_request::Request::ProcessBundle(
                    ProcessBundleRequest {
                        process_bundle_descriptor_id: descriptor_id.to_string(),
                        ..Default::default()
                    },
                )),
            }))
            .await
            .map_err(|e| anyhow!("failed to send process bundle request: {}", e))?;

        // return id — caller correlates with recv_process_bundle_response()
        Ok(id)
    }

    // wait for harness to confirm bundle complete
    // blocks until ProcessBundleResponse arrives on control channel
    // called after sending elements on data channel
    pub async fn recv_process_bundle_response(
        &mut self,
        bundle_id: &str,
    ) -> Result<ControlResponse> {
        info!("Polling for process bundle response");
        let response = self.recv_response().await?;

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
                        "process bundle failed at harness: {}",
                        response.error
                    ));
                }
                Err(anyhow!("unexpected bundle response: {:?}", other))
            }
        }
    }

    // read next InstructionResponse from harness
    // reads directly from the persisted gRPC stream
    // no intermediate mpsc, no spawned tasks
    // if stream dies → returns Err immediately, no deadlock
    async fn recv_response(&self) -> Result<InstructionResponse> {
        let mut guard = self.stream.incoming.lock().await;

        let stream = guard
            .as_mut()
            .ok_or_else(|| anyhow!("harness not connected yet"))?;

        stream
            .message()
            .await
            .map_err(|e| anyhow!("control stream error: {}", e))?
            .ok_or_else(|| anyhow!("harness disconnected"))
    }

    // generate unique instruction ids
    fn next_id(&mut self) -> String {
        self.next_id += 1;
        self.next_id.to_string()
    }
}
