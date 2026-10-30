use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::{Context, Result};
use beam_model_rs::v1::{
    beam_fn_control_server::BeamFnControlServer, beam_fn_data_server::BeamFnDataServer,
    beam_fn_logging_server::BeamFnLoggingServer, beam_fn_state_server::BeamFnStateServer,
};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::engine::{
    capacity::PoolConfig,
    harness::{Channels, HarnessStatus, control::ControlChannel, data::DataChannel},
};

struct WorkerLoads {
    per_worker_limit: usize,
    loads: Vec<AtomicUsize>,
    cursor: AtomicUsize,
}

impl WorkerLoads {
    fn new(worker_count: usize, per_worker_limit: usize) -> Self {
        Self {
            per_worker_limit,
            loads: (0..worker_count).map(|_| AtomicUsize::new(0)).collect(),
            cursor: AtomicUsize::new(0),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<WorkerLease> {
        let worker_count = self.loads.len();
        if worker_count == 0 {
            return None;
        }
        let start = self.cursor.fetch_add(1, Ordering::AcqRel) % worker_count;
        let mut best: Option<(usize, usize)> = None;
        for offset in 0..worker_count {
            let index = (start + offset) % worker_count;
            let load = self.loads[index].load(Ordering::Acquire);
            if load >= self.per_worker_limit {
                continue;
            }
            if best.is_none_or(|(_, best_load)| load < best_load) {
                best = Some((index, load));
            }
        }

        let (index, load) = best?;
        if self.loads[index]
            .compare_exchange(load, load + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Some(WorkerLease {
                loads: Arc::clone(self),
                index,
            })
        } else {
            // A concurrent completion/start changed this slot. Retry once through
            // the same fair scan using the updated values.
            self.try_acquire()
        }
    }
}

/// A reservation for one in-flight bundle on a specific worker.
pub struct WorkerLease {
    loads: Arc<WorkerLoads>,
    index: usize,
}

impl WorkerLease {
    pub fn worker_index(&self) -> usize {
        self.index
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        let previous = self.loads.loads[self.index].fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "worker lease underflow");
    }
}

pub struct WorkerConnection {
    index: usize,
    endpoint: String,
    channels: Channels,
    server_task: JoinHandle<()>,
    registered_stages: Arc<Mutex<HashSet<String>>>,
}

impl WorkerConnection {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn channels(&self) -> &Channels {
        &self.channels
    }

    pub fn control(&self) -> ControlChannel {
        self.channels.control()
    }

    pub fn data(&self) -> DataChannel {
        self.channels.data()
    }

    pub fn status(&self) -> HarnessStatus {
        self.channels.status()
    }

    pub fn registered_stages(&self) -> Arc<Mutex<HashSet<String>>> {
        Arc::clone(&self.registered_stages)
    }

    pub async fn clear_registered_stages(&self) {
        self.registered_stages.lock().await.clear();
    }
}

impl Drop for WorkerConnection {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

/// Per-job set of isolated local SDK harness transports.
pub struct HarnessPool {
    config: PoolConfig,
    workers: Vec<WorkerConnection>,
    loads: Arc<WorkerLoads>,
}

impl HarnessPool {
    pub async fn build(config: PoolConfig, max_message_size: usize) -> Result<Self> {
        let mut workers = Vec::with_capacity(config.worker_count);
        for index in 0..config.worker_count {
            let (channels, services) = Channels::builder()
                .build()
                .await
                .with_context(|| format!("starting Fn channels for worker {index}"))?;
            let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .with_context(|| format!("binding Fn endpoint for worker {index}"))?;
            let address = listener.local_addr()?;
            let incoming = TcpListenerStream::new(listener);
            let server_status = channels.status();
            let server_task = tokio::spawn(async move {
                let result = Server::builder()
                    .add_service(
                        BeamFnControlServer::new(services.control)
                            .max_decoding_message_size(max_message_size)
                            .max_encoding_message_size(max_message_size),
                    )
                    .add_service(
                        BeamFnDataServer::new(services.data)
                            .max_decoding_message_size(max_message_size)
                            .max_encoding_message_size(max_message_size),
                    )
                    .add_service(
                        BeamFnLoggingServer::new(services.log)
                            .max_decoding_message_size(max_message_size)
                            .max_encoding_message_size(max_message_size),
                    )
                    .add_service(
                        BeamFnStateServer::new(services.state)
                            .max_decoding_message_size(max_message_size)
                            .max_encoding_message_size(max_message_size),
                    )
                    .serve_with_incoming(incoming)
                    .await;
                if let Err(error) = result {
                    log::error!("Fn endpoint for worker {index} stopped: {error}");
                    server_status.report_failure("Fn endpoint", &error);
                }
            });

            workers.push(WorkerConnection {
                index,
                endpoint: address.to_string(),
                channels,
                server_task,
                registered_stages: Arc::new(Mutex::new(HashSet::new())),
            });
        }

        Ok(Self {
            config,
            workers,
            loads: Arc::new(WorkerLoads::new(
                config.worker_count,
                config.bundles_per_worker,
            )),
        })
    }

    pub fn config(&self) -> PoolConfig {
        self.config
    }

    pub fn try_lease(&self) -> Option<WorkerLease> {
        self.loads.try_acquire()
    }

    pub fn worker(&self, lease: &WorkerLease) -> &WorkerConnection {
        &self.workers[lease.worker_index()]
    }

    pub fn worker_by_index(&self, index: usize) -> &WorkerConnection {
        &self.workers[index]
    }

    pub fn workers(&self) -> &[WorkerConnection] {
        &self.workers
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use beam_model_rs::v1::{InstructionResponse, beam_fn_control_client::BeamFnControlClient};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;

    #[tokio::test]
    async fn pool_uses_distinct_endpoints_and_per_worker_leases() {
        let config = PoolConfig::for_logical_cpus(4);
        let pool = HarnessPool::build(config, 1024 * 1024)
            .await
            .expect("build harness pool");
        assert_eq!(pool.workers().len(), 2);
        assert_ne!(pool.workers()[0].endpoint(), pool.workers()[1].endpoint());

        let first = pool.try_lease().expect("first worker lease");
        let second = pool.try_lease().expect("second worker lease");
        assert_ne!(first.worker_index(), second.worker_index());
        assert!(
            pool.try_lease().is_none(),
            "each worker is limited to the fixed per-worker concurrency"
        );

        let released = first.worker_index();
        drop(first);
        assert_eq!(
            pool.try_lease()
                .expect("released worker lease")
                .worker_index(),
            released
        );
    }

    #[tokio::test]
    async fn control_instructions_do_not_cross_worker_endpoints() {
        let config = PoolConfig::for_logical_cpus(4);
        let pool = HarnessPool::build(config, 1024 * 1024)
            .await
            .expect("build harness pool");

        let mut clients = Vec::new();
        let mut streams = Vec::new();
        let mut response_senders = Vec::new();
        for worker in pool.workers() {
            let mut client = BeamFnControlClient::connect(format!("http://{}", worker.endpoint()))
                .await
                .expect("connect control client");
            let (response_tx, response_rx) = mpsc::channel::<InstructionResponse>(1);
            let responses = client
                .control(ReceiverStream::new(response_rx))
                .await
                .expect("open control stream")
                .into_inner();
            clients.push(client);
            streams.push(responses);
            response_senders.push(response_tx);
        }
        pool.worker_by_index(0)
            .channels()
            .wait_connected()
            .await
            .expect("worker zero connects");
        pool.worker_by_index(1)
            .channels()
            .wait_connected()
            .await
            .expect("worker one connects");

        let (instruction_id, _response) = pool
            .worker_by_index(0)
            .control()
            .send_process_bundle_request(&"descriptor".to_string())
            .await
            .expect("send process request");
        let received = streams[0]
            .message()
            .await
            .expect("worker zero stream")
            .expect("worker zero request");
        assert_eq!(received.instruction_id, instruction_id);
        assert!(
            tokio::time::timeout(Duration::from_millis(150), streams[1].message())
                .await
                .is_err(),
            "worker one must not receive worker zero's instruction"
        );
    }
}
