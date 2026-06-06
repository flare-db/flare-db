use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use beam_model_rs::v1::{StateRequest, StateResponse, beam_fn_state_server::BeamFnState};
use tokio::sync::{
    Mutex,
    mpsc::{self, Sender},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

pub struct StateInner {
    outgoing: Mutex<Option<mpsc::Receiver<Result<StateResponse, Status>>>>,
    incoming: Mutex<Option<tonic::Streaming<StateRequest>>>,
}

pub async fn start_state_server() -> Result<(StateChannel, FlareStateService)> {
    let (tx, rx) = mpsc::channel::<Result<StateResponse, Status>>(32);

    let stream = Arc::new(StateInner {
        outgoing: Mutex::new(Some(rx)),
        incoming: Mutex::new(None),
    });

    let service = FlareStateService {
        inner: stream.clone(),
    };

    let channel = StateChannel {
        outgoing: tx,
        stream,
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
            *self.inner.incoming.lock().await = Some(request.into_inner());

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
}

pub struct StateChannel {
    outgoing: Sender<Result<StateResponse, Status>>,
    stream: Arc<StateInner>,
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

    pub async fn send_response(&self, response: StateResponse) -> Result<()> {
        self.outgoing
            .send(Ok(response))
            .await
            .map_err(|e| anyhow!("failed to send state response: {}", e))
    }

    pub async fn recv_request(&self) -> Result<StateRequest> {
        let mut guard = self.stream.incoming.lock().await;

        let stream = guard
            .as_mut()
            .ok_or_else(|| anyhow!("harness not connected yet"))?;

        stream
            .message()
            .await
            .map_err(|e| anyhow!("state stream error: {}", e))?
            .ok_or_else(|| anyhow!("harness disconnected"))
    }
}
