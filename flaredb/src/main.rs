use flaredb::{
    engine::{executor::StageExecutor, harness::Channels},
    jobservice::{
        artifact::{ArtifactStore, FlareArtifactStagingService},
        server::FlareJobService,
    },
    worker::manager::{WorkerLaunchConfig, WorkerManager},
};
use std::{net::SocketAddr, sync::Arc};
use tonic::transport::Server;

use beam_model_rs::v1::{
    artifact_staging_service_server::ArtifactStagingServiceServer,
    beam_fn_control_server::BeamFnControlServer, beam_fn_data_server::BeamFnDataServer,
    beam_fn_logging_server::BeamFnLoggingServer, beam_fn_state_server::BeamFnStateServer,
    org::apache::beam::model::job_management::v1::job_service_server::JobServiceServer,
};

#[tokio::main]
async fn main() {
    env_logger::init();
    if let Err(e) = flare_up().await {
        eprintln!("flare_up failed: {e}");
    }
}

async fn flare_up() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = flaredb::DEFAULT_API_SERVICE_URL.parse()?;

    //set base dir
    if let Some(base_dir) = std::env::args().nth(1) {
        flaredb::utils::path::set_base_dir(&base_dir);
    }

    // instance id
    let instance_id =
        std::env::var("FLAREDB_INSTANCE_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

    let artifact_root = flaredb::utils::path::instance_dir(&instance_id);
    let artifact_root_str = artifact_root.to_str().unwrap_or(".");
    let artifact_store =
        Arc::new(ArtifactStore::from(artifact_root_str, "pipeline-artifact").await?);

    let (channels, services) = Channels::builder().build().await?;
    let (control_channel, data_channel, log_channel, state_channel) = channels.into_parts();
    let executor = StageExecutor::new(
        control_channel,
        data_channel,
        log_channel,
        state_channel,
        &instance_id,
    )
    .await?;

    let worker_jar = std::env::var("WORKER_JAR_PATH").unwrap_or_else(|_| {
        format!(
            "{}/bin/beam-sdks-java-harness-2.72.0-flare-bundled.jar",
            std::env::args().nth(1).unwrap_or_else(|| ".".to_string())
        )
    });

    let worker_cfg = WorkerLaunchConfig {
        worker_jar,
        logs_dir: artifact_root_str.to_string(),
        control_url: flaredb::DEFAULT_API_SERVICE_URL.to_string(),
        pipeline_options: "{}".to_string(),
        connect_timeout_secs: 20,
    };
    let worker_manager = WorkerManager::new(worker_cfg);
    let job_service = FlareJobService::with(
        executor,
        artifact_store.clone(),
        worker_manager,
        instance_id,
    );

    let artifact_service =
        FlareArtifactStagingService::new(artifact_store, job_service.get_staging_tokens());

    Server::builder()
        .add_service(JobServiceServer::new(job_service))
        .add_service(ArtifactStagingServiceServer::new(artifact_service))
        .add_service(BeamFnControlServer::new(services.control))
        .add_service(BeamFnDataServer::new(services.data))
        .add_service(BeamFnLoggingServer::new(services.log))
        .add_service(BeamFnStateServer::new(services.state))
        .serve(addr)
        .await?;

    Ok(())
}
