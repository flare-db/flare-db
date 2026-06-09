use flare::{
    engine::{
        executor::StageExecutor,
        harness::{
            control::start_control_server, data::start_data_server, log::start_log_server,
            state::start_state_server,
        },
    },
    jobservice::{
        artifact::{ArtifactStore, FlareArtifactStagingService},
        server::{FlareJobService, HarnessLaunchConfig},
    },
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
    let addr: SocketAddr = "127.0.0.1:8099".parse()?;

    let artifact_store =
        Arc::new(ArtifactStore::from("/home/ganesh/Dev/flaredir/new", "kafka-to-pinecone").await?);
    let artifact_service = FlareArtifactStagingService::new(artifact_store.clone());

    let (control_channel, control_server) = start_control_server().await?;
    let (data_channel, data_server) = start_data_server().await?;
    let (_log_channel, log_server) = start_log_server().await?;
    let (_state_channel, state_server) = start_state_server().await?;

    let executor = StageExecutor::new(control_channel, data_channel);
    let harness_cfg = HarnessLaunchConfig {
        worker_jar: "/home/ganesh/flare/harness/flare/harness/beam-sdks-java-harness-2.72.0-SNAPSHOT-flare-bundled.jar".to_string(),
        logs_dir:  "/home/ganesh/flare-db/gbk/flare-db/logs".to_string(),
        control_url: "localhost:8099".to_string(),
        pipeline_options: "{}".to_string(),
        connect_timeout_secs: 20,
    };
    let job_service = FlareJobService::with(executor, artifact_store, harness_cfg);

    println!("Flared up 🔥 at {}", addr);

    Server::builder()
        .add_service(JobServiceServer::new(job_service))
        .add_service(ArtifactStagingServiceServer::new(artifact_service))
        .add_service(BeamFnControlServer::new(control_server))
        .add_service(BeamFnDataServer::new(data_server))
        .add_service(BeamFnLoggingServer::new(log_server))
        .add_service(BeamFnStateServer::new(state_server))
        .serve(addr)
        .await?;

    Ok(())
}
