use std::{sync::Arc, time::Duration};

use beam_model_rs::v1::{
    ApiServiceDescriptor, CancelJobRequest, CancelJobResponse, DescribePipelineOptionsRequest,
    DescribePipelineOptionsResponse, DrainJobRequest, DrainJobResponse, GetJobMetricsRequest,
    GetJobMetricsResponse, GetJobPipelineRequest, GetJobPipelineResponse, GetJobStateRequest,
    GetJobsRequest, GetJobsResponse, JobMessagesRequest, JobMessagesResponse, JobStateEvent,
    PrepareJobRequest, PrepareJobResponse, ProcessPayload, RunJobRequest, RunJobResponse,
    job_service_server::JobService, job_state::Enum as JobStateEnum,
};
use dashmap::{DashMap, DashSet};
use prost::Message;
use tokio::sync::{Mutex, watch};
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;
use uuid::Uuid;

use crate::engine::dispatcher::ExecutorDispatcher;
use crate::engine::scheduler::NodeScheduler;
use crate::fusion::pipeline::ExecutableGraph;
use crate::jobservice::artifact::ArtifactStore;
use crate::jobservice::job::Job;
use crate::jobservice::job::JobStore;
use crate::jobservice::state::record_job_state;
use crate::worker::manager::WorkerRuntime;

/// Terminal job states, mirroring Beam's `JobState` contract. A state stream
/// may close once it has published one of these.
fn is_terminal_state(state: JobStateEnum) -> bool {
    matches!(
        state,
        JobStateEnum::Done
            | JobStateEnum::Failed
            | JobStateEnum::Cancelled
            | JobStateEnum::Stopped
            | JobStateEnum::Drained
    )
}

/// Wraps a `JobState` enum value in the event type returned by the Job API.
fn job_state_event(state: JobStateEnum) -> JobStateEvent {
    JobStateEvent {
        state: state as i32,
        timestamp: None,
    }
}

pub struct FlareJobService {
    job_store: JobStore,
    dispatcher: Arc<Mutex<ExecutorDispatcher>>,
    artifact_store: Arc<ArtifactStore>,
    worker_manager: crate::worker::manager::WorkerManager,
    staging_tokens: Arc<DashSet<String>>,
    instance_id: String,
    /// Per-job state, published to `GetStateStream` subscribers. FlareDB runs a
    /// job synchronously inside `Run`, so a `watch` channel is exactly the
    /// level of state propagation the Job API stream needs.
    job_states: Arc<DashMap<String, watch::Sender<JobStateEnum>>>,
}

impl FlareJobService {
    pub fn with(
        artifact_store: Arc<ArtifactStore>,
        worker_manager: crate::worker::manager::WorkerManager,
        instance_id: String,
        dispatcher: Arc<Mutex<ExecutorDispatcher>>,
    ) -> Self {
        Self {
            job_store: JobStore::new(),
            artifact_store,
            worker_manager,
            staging_tokens: Arc::new(DashSet::new()),
            instance_id,
            dispatcher,
            job_states: Arc::new(DashMap::new()),
        }
    }

    pub fn get_staging_tokens(&self) -> Arc<DashSet<String>> {
        self.staging_tokens.clone()
    }

    /// Records/creates the state channel for a job, starting in `Starting`.
    fn init_job_state(&self, job_id: &str) {
        let (tx, _rx) = watch::channel(JobStateEnum::Starting);
        self.job_states.insert(job_id.to_string(), tx);
    }

    /// Advances the published state for a job, if it is known.
    fn set_job_state(&self, job_id: &str, state: JobStateEnum) {
        if let Some(tx) = self.job_states.get(job_id) {
            // `send` only fails when there are no receivers, which is fine.
            let _ = tx.send(state);
        }
    }

    fn current_job_state(&self, job_id: &str) -> Option<JobStateEnum> {
        self.job_states.get(job_id).map(|tx| *tx.borrow())
    }

    /// Resolve the interpreter used to launch a Python SDK harness.
    ///
    /// Prefers the job's PROCESS environment `command`, which the SDK driver
    /// fills with the interpreter that has the Beam SDK installed (it submits
    /// the job from that same interpreter). Falls back to an explicitly
    /// configured binary (`FLAREDB_PYTHON_BIN`), and finally to `python3` on
    /// `PATH`, which the worker manager applies when this returns `None`.
    fn python_interpreter(&self, job_graph: &ExecutableGraph) -> Option<String> {
        for env in job_graph.components.environments.values() {
            if env.urn != "beam:env:process:v1" {
                continue;
            }
            if let Ok(payload) = ProcessPayload::decode(env.payload.as_slice()) {
                if !payload.command.is_empty() {
                    return Some(payload.command);
                }
            }
        }
        self.worker_manager.config().python_bin.clone()
    }
}

impl JobService for FlareJobService {
    #[doc = " Prepare a job for execution. The job will not be executed until a call is made to run with the"]
    #[doc = " returned preparationId."]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn prepare<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<PrepareJobRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<PrepareJobResponse>,
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
            log::info!("prepare request received");

            let pipeline = request.get_ref().pipeline.as_ref().ok_or_else(|| {
                log::warn!("prepare request rejected: pipeline is missing");
                Status::invalid_argument("Pipeline is missing")
            })?;

            let job = Job::new(&self.instance_id, pipeline);
            let job_id = job.job_id;
            if let Err(e) = record_job_state(&self.instance_id, &job_id) {
                log::warn!("failed to record job state for {}: {}", job_id, e);
            }
            self.job_store.add_job(&job_id, job.graph);

            let new_token = Uuid::new_v4().to_string();

            let response = PrepareJobResponse {
                preparation_id: job_id.clone(),
                artifact_staging_endpoint: Some(ApiServiceDescriptor {
                    url: crate::DEFAULT_API_SERVICE_URL.to_string(),
                    authentication: None,
                }),
                staging_session_token: new_token.clone(),
            };

            self.staging_tokens.insert(new_token);
            self.init_job_state(&job_id);

            log::info!("prepare request succeeded: preparation_id={}", job_id);
            Ok(Response::new(response))
        })
    }

    #[doc = " Submit the job for execution"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn run<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<RunJobRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<RunJobResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            log::info!("run request received");
            let preparation_id = request.get_ref().preparation_id.clone();
            if preparation_id.is_empty() {
                log::warn!("run request rejected: preparation_id is required");
                return Err(Status::invalid_argument("preparation_id is required"));
            }

            let job_graph = self.job_store.get_job(&preparation_id).ok_or_else(|| {
                log::warn!(
                    "run request rejected: unknown preparation_id={}",
                    preparation_id
                );
                Status::not_found(format!("unknown preparation_id: {}", preparation_id))
            })?;

            self.set_job_state(&preparation_id, JobStateEnum::Running);

            let staging_dir = self.artifact_store.root_path();
            let staged_jar = self.artifact_store.staged_path();
            let pickled_session_path = format!("{}/staged/pickled_main_session", staging_dir);

            let is_python = tokio::fs::try_exists(&pickled_session_path)
                .await
                .unwrap_or(false)
                || job_graph
                    .components
                    .environments
                    .values()
                    .any(|env| env.urn == "beam:env:process:v1" || env.urn.contains("python"));

            let runtime = if is_python {
                WorkerRuntime::Python {
                    python_bin: self.python_interpreter(&job_graph),
                }
            } else {
                WorkerRuntime::Java { staged_jar }
            };

            if let Err(e) = record_job_state(&self.instance_id, &preparation_id) {
                log::warn!("failed to record job state for {}: {}", preparation_id, e);
            }

            // Reset channels so the new harness can connect on fresh streams.
            self.dispatcher.lock().await.reset_channels().await;

            self.worker_manager
                .spawn_worker(&preparation_id, &runtime, staging_dir, &self.instance_id)
                .await?;

            self.dispatcher
                .lock()
                .await
                .set_job_store(&preparation_id)
                .await
                .map_err(|e| Status::internal(format!("failed to set job store: {}", e)))?;

            let connect_timeout_secs = self.worker_manager.config().connect_timeout_secs;
            timeout(Duration::from_secs(connect_timeout_secs), async {
                let dispatcher = self.dispatcher.lock().await;
                dispatcher.wait_connected().await
            })
            .await
            .map_err(|_| {
                Status::internal(format!(
                    "harness did not connect within {}s for job {}",
                    connect_timeout_secs, preparation_id
                ))
            })?
            .map_err(|e| {
                Status::internal(format!(
                    "failed waiting for harness connection for job {}: {}",
                    preparation_id, e
                ))
            })?;

            let mut dispatcher = self.dispatcher.lock().await;
            dispatcher.prepare_pipeline(job_graph.as_ref());

            let mut scheduler = NodeScheduler::new((*job_graph).clone());
            dispatcher.run_pipeline(&mut scheduler).await.map_err(|e| {
                Status::internal(format!(
                    "failed to execute pipeline for job {}: {}",
                    preparation_id, e
                ))
            })?;

            // stop worker, next job will get a fresh one.
            self.worker_manager.stop_worker(&preparation_id).await?;

            self.set_job_state(&preparation_id, JobStateEnum::Done);
            log::info!("job execution completed: preparation_id={}", preparation_id);
            Ok(Response::new(RunJobResponse {
                job_id: preparation_id,
            }))
        })
    }

    #[doc = " Get a list of all invoked jobs"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_jobs<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<GetJobsRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<GetJobsResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Get the current state of the job"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_state<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<GetJobStateRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<JobStateEvent>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let job_id = request.into_inner().job_id;
            match self.current_job_state(&job_id) {
                Some(state) => Ok(Response::new(job_state_event(state))),
                None => Err(Status::not_found(format!("unknown job_id: {}", job_id))),
            }
        })
    }

    #[doc = " Get the job\'s pipeline"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_pipeline<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<GetJobPipelineRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<GetJobPipelineResponse>,
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
        todo!()
    }

    #[doc = " Cancel the job"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn cancel<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<CancelJobRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<CancelJobResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let job_id = request.into_inner().job_id;
            log::info!("cancel request received for job_id={}", job_id);
            self.worker_manager.stop_worker(&job_id).await?;
            Ok(Response::new(CancelJobResponse {
                state: beam_model_rs::v1::job_state::Enum::Cancelled as i32,
            }))
        })
    }

    #[doc = " Drain the job"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn drain<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<DrainJobRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<DrainJobResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Server streaming response type for the GetStateStream method."]
    // type GetStateStreamStream;
    #[doc = " Subscribe to a stream of state changes of the job, will immediately return the current state of the job as the first response."]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_state_stream<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<GetJobStateRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<Self::GetStateStreamStream>,
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
            let job_id = request.into_inner().job_id;

            // Subscribe to the job's state channel, cloning the receiver out of
            // the map before any await so no shard lock is held across awaits.
            let tx = match self.job_states.get(&job_id) {
                Some(tx) => tx,
                None => {
                    return Err(Status::not_found(format!("unknown job_id: {}", job_id,)));
                }
            };
            let mut rx = tx.subscribe();
            drop(tx);

            let (out_tx, out_rx) =
                tokio::sync::mpsc::channel::<Result<JobStateEvent, tonic::Status>>(16);
            tokio::spawn(async move {
                loop {
                    let state = *rx.borrow();
                    if out_tx.send(Ok(job_state_event(state))).await.is_err() {
                        // Subscriber (driver) went away.
                        return;
                    }
                    if is_terminal_state(state) {
                        return;
                    }
                    if rx.changed().await.is_err() {
                        return;
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(out_rx)))
        })
    }

    #[doc = " Server streaming response type for the GetMessageStream method."]
    // type GetMessageStreamStream;
    #[doc = " Subscribe to a stream of state changes and messages from the job"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_message_stream<'life0, 'async_trait>(
        &'life0 self,
        _request: tonic::Request<JobMessagesRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<Self::GetMessageStreamStream>,
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
            // FlareDB executes a job synchronously inside `Run` and reports no
            // incremental messages, so hand back an already-closed stream. The
            // portable driver drains it after `Run` returns.
            let (_tx, rx) =
                tokio::sync::mpsc::channel::<Result<JobMessagesResponse, tonic::Status>>(1);
            Ok(Response::new(ReceiverStream::new(rx)))
        })
    }

    #[doc = " Fetch metrics for a given job"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_job_metrics<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<GetJobMetricsRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<GetJobMetricsResponse>,
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
        todo!()
    }

    #[doc = " Get the supported pipeline options of the runner"]
    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn describe_pipeline_options<'life0, 'async_trait>(
        &'life0 self,
        _request: tonic::Request<DescribePipelineOptionsRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<DescribePipelineOptionsResponse>,
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
            // FlareDB has no additional runner-specific pipeline options to
            // advertise, so return an empty descriptor list.
            Ok(Response::new(DescribePipelineOptionsResponse {
                options: vec![],
            }))
        })
    }

    #[doc = " Server streaming response type for the GetStateStream method."]
    type GetStateStreamStream = ReceiverStream<Result<JobStateEvent, tonic::Status>>;

    #[doc = " Server streaming response type for the GetMessageStream method."]
    type GetMessageStreamStream = ReceiverStream<Result<JobMessagesResponse, tonic::Status>>;
}
