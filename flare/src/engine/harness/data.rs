use std::sync::Arc;

use anyhow::anyhow;
use beam_model_rs::v1::{Elements, beam_fn_data_server::BeamFnData};
use log::{info, warn};
use tokio::sync::{
    Mutex,
    mpsc::{self, Sender},
};
use tokio::time::{Duration, Instant, sleep};
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
        outgoing: tx,
        stream,
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
    outgoing: Sender<Result<Elements, Status>>,
    stream: Arc<DataInner>,
}

impl DataChannel {
    pub async fn send_elements(&self, elements: Elements) -> anyhow::Result<()> {
        self.outgoing
            .send(Ok(elements))
            .await
            .map_err(|e| anyhow!("failed to send data-plane elements to harness: {}", e))
    }

    pub async fn stream_elements(&self) -> Vec<Elements> {
        let mut elements = Vec::<Elements>::new();
        let deadline = Instant::now() + Duration::from_secs(2);

        loop {
            let mut g: tokio::sync::MutexGuard<'_, Option<tonic::Streaming<Elements>>> =
                self.stream.incoming.lock().await;
            if let Some(stream) = &mut *g {
                while let Some(item) = stream.message().await.unwrap() {
                    let is_last = item.data.iter().any(|data| data.is_last)
                        || item.timers.iter().any(|timer| timer.is_last);
                    elements.push(item);

                    if is_last {
                        info!("received end-of-stream marker on data channel");
                        break;
                    }
                }
                break;
            }

            if Instant::now() >= deadline {
                warn!("data stream not connected yet; returning no elements");
                break;
            }
            drop(g);
            sleep(Duration::from_millis(20)).await;
        }

        info!("stream_elements collected {} message(s)", elements.len());
        elements
    }
}

// Flare control(process bundle request) -> harness
//                                            | prcessed elements
//                                    data channel(processed elements)
// Flare --> inputs elements to harness ->    |
