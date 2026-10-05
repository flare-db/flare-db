use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use beam_model_rs::v1::{
    Elements,
    beam_fn_data_server::BeamFnData,
    elements::{Data, Timers},
};
use dashmap::DashMap;
use log::{debug, info, warn};
use tokio::sync::{
    Mutex,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

/// Shared gRPC channel state for data plane communication with the worker.
/// Holds outgoing (Flare → worker) and incoming (worker → Flare) element streams.
pub struct DataInner {
    /// Sender for outgoing Elements stream to the worker.
    outgoing_tx: Mutex<Option<mpsc::Sender<Result<Elements, Status>>>>,
    /// Receiver side of outgoing channel; handed to worker as gRPC stream.
    outgoing_rx: Mutex<Option<mpsc::Receiver<Result<Elements, Status>>>>,
    /// Incoming Elements stream from worker; owned by stream_elements() dispatcher.
    incoming: Mutex<Option<tonic::Streaming<Elements>>>,
}

impl DataInner {
    async fn sender(&self) -> anyhow::Result<mpsc::Sender<Result<Elements, Status>>> {
        self.outgoing_tx
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow!("data outgoing channel not initialized"))
    }
}

pub async fn start_data_server() -> Result<(DataChannel, FlareDataService), anyhow::Error> {
    let (tx, rx) = mpsc::channel::<Result<Elements, Status>>(32);

    let stream = Arc::new(DataInner {
        outgoing_tx: Mutex::new(Some(tx)),
        outgoing_rx: Mutex::new(Some(rx)),
        incoming: Mutex::new(None),
    });

    let service = FlareDataService {
        inner: stream.clone(),
    };

    let channel = DataChannel {
        worker_stream: stream,
        runner_stream: Arc::new(ElementStreamMultiplexer::new()),
        stream_task: Arc::new(std::sync::Mutex::new(None)),
    };
    Ok((channel, service))
}
/// gRPC server-side handler for BeamFnData service.
/// Establishes the bidirectional gRPC stream with the worker.
pub struct FlareDataService {
    inner: Arc<DataInner>,
}

impl BeamFnData for FlareDataService {
    #[doc = " Server streaming response type for the Data method."]
    type DataStream = ReceiverStream<Result<Elements, Status>>;

    #[doc = " Used to send data between workeres."]
    //#[must_use]
    #[allow(
        //elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn data<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<tonic::Streaming<Elements>>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<Self::DataStream>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            info!("BeamFnData stream connected from worker");
            *self.inner.incoming.lock().await = Some(request.into_inner());

            let rx = {
                let mut rx_guard = self.inner.outgoing_rx.lock().await;
                if rx_guard.is_none() {
                    warn!(
                        "Data stream connected while a previous worker stream was still active; replacing stale stream"
                    );
                    let (tx, rx) = mpsc::channel::<Result<Elements, Status>>(32);
                    *self.inner.outgoing_tx.lock().await = Some(tx);
                    *rx_guard = Some(rx);
                }
                rx_guard
                    .take()
                    .expect("data outgoing receiver must be initialized")
            };

            std::result::Result::Ok(Response::new(ReceiverStream::new(rx)))
        })
    }
}

/// Client-side data channel for executor to send/receive elements.
#[derive(Clone)]
pub struct DataChannel {
    /// Shared gRPC stream state with FlareDataService.
    worker_stream: Arc<DataInner>,
    /// Multiplexer routing incoming elements by (instruction_id, transform_id).
    runner_stream: Arc<ElementStreamMultiplexer>,
    /// Handle to the background dispatcher task.
    /// Aborted on reset to prevent stale task from consuming new worker stream.
    stream_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl DataChannel {
    pub async fn send_elements(&self, elements: Elements) -> anyhow::Result<()> {
        let sender = self.worker_stream.sender().await?;
        sender
            .send(Ok(elements))
            .await
            .map_err(|e| anyhow!("failed to send data-plane elements to worker: {}", e))
    }

    // Reset the data channel so a new worker can connect.
    pub async fn reset(&self) {
        // Abort the streaming task from the previous job so it does not
        // race with the new task to consume the incoming worker stream.
        let handle = self.stream_task.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.abort();
            // Wait for the task to finish before replacing channels.
            let _ = handle.await;
        }

        let (tx, rx) = mpsc::channel::<Result<Elements, Status>>(32);

        *self.worker_stream.outgoing_tx.lock().await = Some(tx);
        *self.worker_stream.outgoing_rx.lock().await = Some(rx);
        *self.worker_stream.incoming.lock().await = None;

        // Clear the element multiplexer — old entries from the previous
        // worker are stale.
        self.runner_stream.senders().clear();
        self.runner_stream.receivers().clear();

        log::info!("data channel reset for next worker");
    }

    /// Start the background dispatcher task that routes incoming elements.
    /// reads from incoming_stream and demuxes every message by (instruction_id, transform_id) to separate queues.
    pub fn stream_elements(&self) {
        let worker_data_stream = self.worker_stream.clone();
        let runner_stream = self.runner_stream.clone();
        let task_slot = self.stream_task.clone();
        info!("Streaming elements from worker");

        let join_handle = tokio::spawn(async move {
            loop {
                let mut guard = worker_data_stream.incoming.lock().await;

                let Some(stream) = &mut *guard else {
                    drop(guard);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                // timers and data related?
                loop {
                    match stream.message().await {
                        Ok(Some(elements)) => {
                            // Demux Elements message into per-instruction queues
                            debug!(
                                "Received Elements from worker: data={}, timers={}",
                                elements.data.len(),
                                elements.timers.len()
                            );
                            crate::engine::liveness::touch();
                            route_elements(elements, &runner_stream);
                        }
                        Ok(None) => {
                            info!("BeamFnData stream from worker closed cleanly");
                            break;
                        }
                        Err(e) => {
                            warn!("BeamFnData stream error (worker gone?): {}", e);
                            break;
                        }
                    }
                }

                info!("BeamFnData stream from worker closed");
                break;
            }
        });

        // Store the join handle so we can cancel and await this task on reset.
        *task_slot.lock().unwrap() = Some(join_handle);
    }
    fn get_sender(
        key: ElementKey,
        inner_stream: &ElementStreamMultiplexer,
    ) -> UnboundedSender<ElementStreamPayload> {
        let (sender, _receiver) = Self::get_or_create_stream(key, inner_stream);
        sender
    }

    fn get_or_create_stream(
        key: ElementKey,
        inner_stream: &ElementStreamMultiplexer,
    ) -> (
        UnboundedSender<ElementStreamPayload>,
        Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>>,
    ) {
        if let Some(sender) = inner_stream.senders().get(&key) {
            let receiver = inner_stream
                .receivers()
                .get(&key)
                .expect("sender exists without matching receiver");

            return (sender.value().clone(), Arc::clone(receiver.value()));
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let receiver = Arc::new(Mutex::new(rx));

        inner_stream.senders().insert(key.clone(), tx.clone());
        inner_stream.receivers().insert(key, Arc::clone(&receiver));

        (tx, receiver)
    }

    pub fn get_receiver(
        &self,
        key: DataKey,
    ) -> Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>> {
        let element_key = ElementKey::Data(key);

        let (_sender, receiver) = Self::get_or_create_stream(element_key, &self.runner_stream);

        receiver
    }

    /// Receiver for a stage's inbound `Elements.Timers` chunks, keyed by
    /// `(instruction_id, transform_id, timer_family_id)`.
    pub fn get_timer_receiver(
        &self,
        key: TimersKey,
    ) -> Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>> {
        let element_key = ElementKey::Timers(key);

        let (_sender, receiver) = Self::get_or_create_stream(element_key, &self.runner_stream);

        receiver
    }
}

/// Demux one `Elements` message from the worker into the per-key queues of
/// `inner_stream`: each `Data` chunk routes by `(instruction_id, transform_id)`
/// and each `Timers` chunk by `(instruction_id, transform_id, timer_family_id)`.
///
/// Split out of [`DataChannel::stream_elements`] so the routing rules can be
/// unit-tested without a live gRPC stream.
fn route_elements(elements: Elements, inner_stream: &ElementStreamMultiplexer) {
    for data in elements.data {
        debug!(
            "Routing data from worker: instruction_id={}, transform_id={}, is_last={}, bytes={}",
            data.instruction_id,
            data.transform_id,
            data.is_last,
            data.data.len()
        );
        let data_key = DataKey {
            instruction_id: data.instruction_id.clone(),
            transform_id: data.transform_id.clone(),
        };

        // Route to the queue keyed by (instruction_id, transform_id).
        let sender = DataChannel::get_sender(ElementKey::Data(data_key.clone()), inner_stream);
        let _ = sender.send(ElementStreamPayload::Data(DataChunk {
            key: data_key,
            data,
        }));
    }

    for timers in elements.timers {
        debug!(
            "Routing timers from worker: instruction_id={}, transform_id={}, timer_family_id={}, is_last={}, bytes={}",
            timers.instruction_id,
            timers.transform_id,
            timers.timer_family_id,
            timers.is_last,
            timers.timers.len()
        );
        let timers_key = TimersKey {
            instruction_id: timers.instruction_id.clone(),
            transform_id: timers.transform_id.clone(),
            timer_family_id: timers.timer_family_id.clone(),
        };

        // Route to the queue keyed by the full timer identity.
        let sender = DataChannel::get_sender(ElementKey::Timers(timers_key.clone()), inner_stream);
        let _ = sender.send(ElementStreamPayload::Timers(TimerChunk {
            key: timers_key,
            timers,
        }));
    }
}

/// Key for routing data elements: (instruction_id, transform_id) pair.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct DataKey {
    pub(crate) instruction_id: String,
    pub(crate) transform_id: String,
}

/// Key for routing timer elements: the owning instruction, transform and timer
/// family, matching the fields of a `beam_fn_api::Elements.Timers` chunk.
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct TimersKey {
    pub(crate) instruction_id: String,
    pub(crate) transform_id: String,
    pub(crate) timer_family_id: String,
}

/// Union type for routing keys: either data or timers.
#[derive(PartialEq, Eq, Hash, Clone)]
pub enum ElementKey {
    Data(DataKey),
    Timers(TimersKey),
}

/// Multiplexer that routes incoming elements to per-key unbounded channels.
/// Each caller (executor, transform) registers a key and gets its own receiver queue.
pub struct ElementStreamMultiplexer {
    /// Senders for each element key; created on first access.
    senders: DashMap<ElementKey, UnboundedSender<ElementStreamPayload>>,
    /// Corresponding receivers; one per key.
    receivers: DashMap<ElementKey, Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>>>,
}

impl ElementStreamMultiplexer {
    pub fn new() -> Self {
        Self {
            senders: DashMap::new(),
            receivers: DashMap::new(),
        }
    }

    pub fn senders(&self) -> &DashMap<ElementKey, UnboundedSender<ElementStreamPayload>> {
        &self.senders
    }

    pub fn receivers(
        &self,
    ) -> &DashMap<ElementKey, Arc<Mutex<UnboundedReceiver<ElementStreamPayload>>>> {
        &self.receivers
    }
}

/// Payload sent through element queues: either data or timers.
#[derive(Clone)]
pub enum ElementStreamPayload {
    Data(DataChunk),
    Timers(TimerChunk),
}

/// Data element with routing key.
#[derive(Eq, Hash, PartialEq, Clone)]
pub struct DataChunk {
    pub(crate) key: DataKey,
    pub(crate) data: Data,
}

/// Timer element with routing key.
#[derive(Eq, Hash, PartialEq, Clone)]
pub struct TimerChunk {
    pub(crate) key: TimersKey,
    pub(crate) timers: Timers,
}

// Flare control(process bundle request) -> worker
//                                            | prcessed elements
//                                    data channel(processed elements)
// Flare --> inputs elements to worker ->    |

#[cfg(test)]
mod tests {
    use super::*;

    fn timers_chunk(
        instruction_id: &str,
        transform_id: &str,
        timer_family_id: &str,
        bytes: Vec<u8>,
        is_last: bool,
    ) -> Timers {
        Timers {
            instruction_id: instruction_id.to_string(),
            transform_id: transform_id.to_string(),
            timer_family_id: timer_family_id.to_string(),
            timers: bytes,
            is_last,
        }
    }

    fn timers_key(instruction_id: &str, transform_id: &str, timer_family_id: &str) -> TimersKey {
        TimersKey {
            instruction_id: instruction_id.to_string(),
            transform_id: transform_id.to_string(),
            timer_family_id: timer_family_id.to_string(),
        }
    }

    #[tokio::test]
    async fn timer_chunks_route_to_their_instruction_transform_and_family() {
        let (channel, _service) = start_data_server().await.expect("start data server");
        let receiver = channel.get_timer_receiver(timers_key("instr", "transform", "flush"));

        route_elements(
            Elements {
                data: Vec::new(),
                timers: vec![timers_chunk(
                    "instr",
                    "transform",
                    "flush",
                    vec![1, 2, 3],
                    false,
                )],
            },
            &channel.runner_stream,
        );

        let payload = {
            let mut guard = receiver.lock().await;
            guard.recv().await.expect("timer chunk routed")
        };
        match payload {
            ElementStreamPayload::Timers(chunk) => {
                assert_eq!(chunk.timers.timers, vec![1, 2, 3]);
                assert_eq!(chunk.timers.transform_id, "transform");
                assert_eq!(chunk.timers.timer_family_id, "flush");
                assert!(!chunk.timers.is_last);
            }
            ElementStreamPayload::Data(_) => panic!("expected a timer payload"),
        }
    }

    #[tokio::test]
    async fn timer_routing_is_keyed_by_the_full_identity() {
        let (channel, _service) = start_data_server().await.expect("start data server");
        let receiver = channel.get_timer_receiver(timers_key("instr", "t", "flush"));

        // Same instruction and transform, different timer family: the chunk must
        // NOT be delivered to the "flush" receiver.
        route_elements(
            Elements {
                data: Vec::new(),
                timers: vec![timers_chunk("instr", "t", "other", vec![9], true)],
            },
            &channel.runner_stream,
        );

        let empty = {
            let mut guard = receiver.lock().await;
            guard.try_recv().is_err()
        };
        assert!(
            empty,
            "a timer for a different family must not be delivered"
        );
    }

    #[tokio::test]
    async fn data_and_timers_in_one_message_route_independently() {
        let (channel, _service) = start_data_server().await.expect("start data server");
        let data_key = DataKey {
            instruction_id: "instr".to_string(),
            transform_id: "sink".to_string(),
        };
        let data_receiver = channel.get_receiver(data_key);
        let timer_receiver = channel.get_timer_receiver(timers_key("instr", "t", "flush"));

        route_elements(
            Elements {
                data: vec![Data {
                    instruction_id: "instr".to_string(),
                    transform_id: "sink".to_string(),
                    data: vec![7, 8],
                    is_last: true,
                }],
                timers: vec![timers_chunk("instr", "t", "flush", vec![4], false)],
            },
            &channel.runner_stream,
        );

        let data_payload = {
            let mut guard = data_receiver.lock().await;
            guard.recv().await.expect("data routed")
        };
        match data_payload {
            ElementStreamPayload::Data(chunk) => assert_eq!(chunk.data.data, vec![7, 8]),
            ElementStreamPayload::Timers(_) => panic!("expected a data payload"),
        }
        let timer_payload = {
            let mut guard = timer_receiver.lock().await;
            guard.recv().await.expect("timers routed")
        };
        match timer_payload {
            ElementStreamPayload::Timers(chunk) => assert_eq!(chunk.timers.timers, vec![4]),
            ElementStreamPayload::Data(_) => panic!("expected a timer payload"),
        }
    }
}
