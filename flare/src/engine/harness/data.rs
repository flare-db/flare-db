use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use beam_model_rs::v1::{
    Elements,
    beam_fn_data_server::BeamFnData,
    elements::{Data, Timers},
};
use dashmap::DashMap;
use log::{error, info};
use tokio::sync::{
    Mutex,
    mpsc::{self, Sender, UnboundedReceiver, UnboundedSender},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub struct DataInner {
    outgoing: Mutex<Option<mpsc::Receiver<Result<Elements, Status>>>>,
    incoming: Mutex<Option<tonic::Streaming<Elements>>>,
}

pub async fn start_data_server() -> Result<(DataChannel, FlareDataService), anyhow::Error> {
    let (tx, rx) = mpsc::channel::<Result<Elements, Status>>(32);

    let stream = Arc::new(DataInner {
        outgoing: Mutex::new(Some(rx)),
        incoming: Mutex::new(None),
    });

    let service = FlareDataService {
        inner: stream.clone(),
    };

    let channel = DataChannel {
        outgoing: Arc::new(tx),
        worker_stream: stream,
        runner_stream: Arc::new(ElementStreamMultiplexer::new()),
    };
    Ok((channel, service))
}
pub struct FlareDataService {
    //sender: Arc<Mutex<Option<Sender<Result<Elements, Status>>>>>,
    //incoming_tx: mpsc::Sender<Elements>,
    inner: Arc<DataInner>,
}

impl BeamFnData for FlareDataService {
    #[doc = " Server streaming response type for the Data method."]
    type DataStream = ReceiverStream<Result<Elements, Status>>;

    #[doc = " Used to send data between harnesses."]
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
            info!("BeamFnData stream connected from harness");
            *self.inner.incoming.lock().await = Some(request.into_inner());

            let rx = self
                .inner
                .outgoing
                .lock()
                .await
                .take()
                .ok_or_else(|| Status::internal("Error"))?;

            std::result::Result::Ok(Response::new(ReceiverStream::new(rx)))
        })
    }
}

#[derive(Clone)]
pub struct DataChannel {
    outgoing: Arc<Sender<Result<Elements, Status>>>,
    worker_stream: Arc<DataInner>,
    runner_stream: Arc<ElementStreamMultiplexer>,
}

impl DataChannel {
    pub async fn send_elements(&self, elements: Elements) -> anyhow::Result<()> {
        self.outgoing
            .send(Ok(elements))
            .await
            .map_err(|e| anyhow!("failed to send data-plane elements to harness: {}", e))
    }

    pub fn stream_elements(&self) {
        let worker_data_stream = self.worker_stream.clone();
        let runner_stream = self.runner_stream.clone();
        info!("Streaming elements from harness");

        tokio::spawn(async move {
            loop {
                let mut guard = worker_data_stream.incoming.lock().await;

                let Some(stream) = &mut *guard else {
                    drop(guard);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };

                // I am not sure if Data and Timers in elements are realted or not
                // So, for now i am senning it as individual playload, if realted
                // we could modify the code later.
                while let Some(elements) = stream.message().await.unwrap() {
                    info!(
                        "Received Elements from harness: data={}, timers={}",
                        elements.data.len(),
                        elements.timers.len()
                    );
                    for data in elements.data {
                        info!(
                            "Routing data from harness: instruction_id={}, transform_id={}, is_last={}, bytes={}",
                            data.instruction_id,
                            data.transform_id,
                            data.is_last,
                            data.data.len()
                        );
                        let data_key = DataKey {
                            instruction_id: data.instruction_id.clone(),
                            transform_id: data.transform_id.clone(),
                        };

                        let element_key = ElementKey::Data(data_key.clone());

                        let sender = Self::get_sender(element_key, &runner_stream);

                        let _ = sender.send(ElementStreamPayload::Data(DataChunk {
                            key: data_key,
                            data,
                        }));
                    }

                    for timers in elements.timers {
                        let timers_key = TimersKey {
                            instruction_id: timers.instruction_id.clone(),
                            // transform_id: timers.transform_id.clone(),
                            // timer_family_id: timers.timer_family_id.clone(),
                        };

                        let element_key = ElementKey::Timers(timers_key.clone());

                        let sender = Self::get_sender(element_key, &runner_stream);

                        let _ = sender.send(ElementStreamPayload::Timers(TimerChunk {
                            key: timers_key,
                            timers,
                        }));
                    }
                }

                info!("BeamFnData stream from harness closed");
                break;
            }
        });
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
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct DataKey {
    pub(crate) instruction_id: String,
    pub(crate) transform_id: String,
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct TimersKey {
    pub(crate) instruction_id: String,
    // pub(crate) transform_id: String,
    // pub(crate) timer_family_id: String,
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub enum ElementKey {
    Data(DataKey),
    Timers(TimersKey),
}
pub struct ElementStreamMultiplexer {
    senders: DashMap<ElementKey, UnboundedSender<ElementStreamPayload>>,
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

#[derive(Clone)]
pub enum ElementStreamPayload {
    // assuming we might need the data and timers in order
    Data(DataChunk),
    Timers(TimerChunk),
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct DataChunk {
    pub(crate) key: DataKey,
    pub(crate) data: Data,
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct TimerChunk {
    pub(crate) key: TimersKey,
    pub(crate) timers: Timers,
}

// Flare control(process bundle request) -> harness
//                                            | prcessed elements
//                                    data channel(processed elements)
// Flare --> inputs elements to harness ->    |

/*
one background task drains tonic stream
routes elements by instruction_id
bundle sessions receive their own outputs asynchronously
 */
