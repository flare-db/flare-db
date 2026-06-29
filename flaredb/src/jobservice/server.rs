use std::collections::HashSet;
use std::{process::Stdio, sync::Arc, time::Duration};

use beam_model_rs::v1::{
    ApiServiceDescriptor, CancelJobRequest, CancelJobResponse, DescribePipelineOptionsRequest,
    DescribePipelineOptionsResponse, DrainJobRequest, DrainJobResponse, GetJobMetricsRequest,
    GetJobMetricsResponse, GetJobPipelineRequest, GetJobPipelineResponse, GetJobStateRequest,
    GetJobsRequest, GetJobsResponse, JobMessagesRequest, JobMessagesResponse, JobStateEvent,
    PrepareJobRequest, PrepareJobResponse, RunJobRequest, RunJobResponse,
    job_service_server::JobService,
};
use dashmap::DashSet;
use tokio::{process::Command, sync::Mutex, time::timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;
use uuid::Uuid;

use crate::engine::executor::StageExecutor;
use crate::jobservice::artifact::ArtifactStore;
use crate::jobservice::job::Job;
use crate::jobservice::job::JobStore;

#[derive(Clone, Debug)]
pub struct HarnessLaunchConfig {
    pub worker_jar: String,
    pub logs_dir: String,
    pub control_url: String,
    pub pipeline_options: String,
    pub connect_timeout_secs: u64,
}

#[derive(Clone)]
pub struct FlareJobService {
    job_store: JobStore,
    executor: Arc<Mutex<StageExecutor>>,
    artifact_store: Arc<ArtifactStore>,
    harness_cfg: HarnessLaunchConfig,
    staging_tokens: Arc<DashSet<String>>,
}

impl FlareJobService {
    pub fn with(
        executor: StageExecutor,
        artifact_store: Arc<ArtifactStore>,
        harness_cfg: HarnessLaunchConfig,
    ) -> Self {
        Self {
            job_store: JobStore::new(),
            executor: Arc::new(Mutex::new(executor)),
            artifact_store,
            harness_cfg,
            staging_tokens: Arc::new(DashSet::new()),
        }
    }

    pub fn get_staging_tokens(&self) -> Arc<DashSet<String>> {
        self.staging_tokens.clone()
    }

    async fn spawn_harness(&self, job_id: &str) -> Result<(), Status> {
        let staged_jar = self.artifact_store.staged_path();
        let worker_jar = &self.harness_cfg.worker_jar;

        let worker_exists = tokio::fs::try_exists(worker_jar)
            .await
            .map_err(|e| Status::internal(format!("failed to stat worker jar: {}", e)))?;
        if !worker_exists {
            return Err(Status::internal(format!(
                "worker jar not found at {}",
                worker_jar
            )));
        }

        let staged_exists = tokio::fs::try_exists(&staged_jar)
            .await
            .map_err(|e| Status::internal(format!("failed to stat staged artifact: {}", e)))?;
        if !staged_exists {
            return Err(Status::internal(format!(
                "staged artifact not found at {}",
                staged_jar
            )));
        }

        tokio::fs::create_dir_all(&self.harness_cfg.logs_dir)
            .await
            .map_err(|e| Status::internal(format!("failed to create logs dir: {}", e)))?;

        let log_path = format!(
            "{}/worker-harness-{}.log",
            self.harness_cfg.logs_dir, job_id
        );
        let stdout_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| Status::internal(format!("failed to open harness log file: {}", e)))?;
        let stderr_file = stdout_file
            .try_clone()
            .map_err(|e| Status::internal(format!("failed to clone harness log handle: {}", e)))?;

        let classpath = format!("{}:{}", worker_jar, staged_jar);
        let mut cmd = Command::new("java");
        cmd.arg("-cp")
            .arg(&classpath)
            .arg("org.apache.beam.fn.harness.FnHarness")
            .env("HARNESS_ID", job_id)
            .env(
                "CONTROL_API_SERVICE_DESCRIPTOR",
                format!(r#"url: "{}""#, self.harness_cfg.control_url),
            )
            .env(
                "LOGGING_API_SERVICE_DESCRIPTOR",
                format!(r#"url: "{}""#, self.harness_cfg.control_url),
            )
            .env(
                "DATA_API_SERVICE_DESCRIPTOR",
                format!(r#"url: "{}""#, self.harness_cfg.control_url),
            )
            .env(
                "STATE_API_SERVICE_DESCRIPTOR",
                format!(r#"url: "{}""#, self.harness_cfg.control_url),
            )
            .env("PIPELINE_OPTIONS", &self.harness_cfg.pipeline_options)
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file));

        let child = cmd
            .spawn()
            .map_err(|e| Status::internal(format!("failed to spawn harness: {}", e)))?;
        log::info!(
            "spawned harness: job_id={}, pid={:?}, classpath={}, log={}",
            job_id,
            child.id(),
            classpath,
            log_path
        );

        Ok(())
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

            let job = Job::new(pipeline);
            let job_id = job.job_id;
            self.job_store.add_job(&job_id, job.graph);

            let new_token = Uuid::new_v4().to_string();

            let response = PrepareJobResponse {
                preparation_id: job_id.clone(),
                artifact_staging_endpoint: Some(ApiServiceDescriptor {
                    url: String::from("127.0.0.1:8099"),
                    authentication: None,
                }),
                staging_session_token: new_token.clone(),
            };

            self.staging_tokens.insert(new_token);

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

            self.spawn_harness(&preparation_id).await?;

            let executor = self.executor.clone();
            timeout(
                Duration::from_secs(self.harness_cfg.connect_timeout_secs),
                async {
                    let executor = executor.lock().await;
                    executor.wait_connected().await
                },
            )
            .await
            .map_err(|_| {
                Status::internal(format!(
                    "harness did not connect within {}s for job {}",
                    self.harness_cfg.connect_timeout_secs, preparation_id
                ))
            })?
            .map_err(|e| {
                Status::internal(format!(
                    "failed waiting for harness connection for job {}: {}",
                    preparation_id, e
                ))
            })?;

            executor
                .lock()
                .await
                .execute_pipeline(job_graph.as_ref())
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "failed to execute pipeline for job {}: {}",
                        preparation_id, e
                    ))
                })?;

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
        todo!()
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
        todo!()
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
        todo!()
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
        request: tonic::Request<JobMessagesRequest>,
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
        todo!()
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
        request: tonic::Request<DescribePipelineOptionsRequest>,
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
        todo!()
    }

    #[doc = " Server streaming response type for the GetStateStream method."]
    type GetStateStreamStream = ReceiverStream<Result<JobStateEvent, tonic::Status>>;

    #[doc = " Server streaming response type for the GetMessageStream method."]
    type GetMessageStreamStream = ReceiverStream<Result<JobMessagesResponse, tonic::Status>>;
}
