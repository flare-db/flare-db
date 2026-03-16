use std::sync::Arc;

use beam_model_rs::v1::{
    ApiServiceDescriptor, CancelJobRequest, CancelJobResponse, DescribePipelineOptionsRequest,
    DescribePipelineOptionsResponse, DrainJobRequest, DrainJobResponse, GetJobMetricsRequest,
    GetJobMetricsResponse, GetJobPipelineRequest, GetJobPipelineResponse, GetJobStateRequest,
    GetJobsRequest, GetJobsResponse, JobMessagesRequest, JobMessagesResponse, JobStateEvent,
    PrepareJobRequest, PrepareJobResponse, RunJobRequest, RunJobResponse,
    job_service_server::JobService,
};
use dashmap::DashMap;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;

use crate::jobservice::job::Job;
use crate::jobservice::job::JobGraph;

#[derive(Clone)]
pub struct JobStore {
    jobs: Arc<DashMap<String, JobGraph>>, // make Arc<JobGraph> to clone the pointer instread of entire jobgraph
}

impl JobStore {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(DashMap::new()),
        }
    }

    pub fn add_job(&self, id: String, graph: JobGraph) {
        self.jobs.insert(id, graph);
    }

    pub fn get_job(&self, id: &str) -> Option<JobGraph> {
        self.jobs.get(id).map(|entry| entry.value().clone())
    }
    pub fn first_job_id(&self) -> Option<String> {
        self.jobs.iter().next().map(|entry| entry.key().clone())
    }
}
#[derive(Clone)]
pub struct FlareJobService {
    job_store: JobStore,
}

impl FlareJobService {
    pub fn new() -> Self {
        Self {
            job_store: JobStore::new(),
        }
    }
}

impl JobService for FlareJobService {
    #[doc = " Prepare a job for execution. The job will not be executed until a call is made to run with the"]
    #[doc = " returned preparationId."]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
            let pipeline = request
                .get_ref()
                .pipeline
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("Pipeline is missing"))?;

            //println!("pipeline proto: {:#?}", pipeline);
            /* let text: String = format!("{:#?}", pipeline);

                        let path = std::env::current_dir()
                            .map_err(|e| Status::internal(format!("Failed to get CWD: {e}")))?
                            .join("beam_pipeline_context.txt");

                        fs::write(&path, text)
                            .map_err(|e| Status::internal(format!("Failed to write pipeline context: {e}")))?;
            */
            //println!("Pipeline context written to {}", path.display());
            let job = Job::new(pipeline);
            self.job_store.add_job(job.job_id, job.job_graph);

            let response = PrepareJobResponse {
                preparation_id: self.job_store.first_job_id().unwrap_or_default(),
                artifact_staging_endpoint: Some(ApiServiceDescriptor {
                    url: String::from("127.0.0.1:8099"),
                    authentication: None,
                }),
                staging_session_token: "token".to_string(),
            };

            Ok(Response::new(response))
        })
    }

    #[doc = " Submit the job for execution"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
        todo!()
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
    #[must_use]
    #[allow(
        elided_named_lifetimes,
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
