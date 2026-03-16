use flare::jobservice::{
    artifact::{ArtifactStore, FlareArtifactStagingService},
    server::FlareJobService,
};
use std::net::SocketAddr;
use tonic::transport::Server;

use beam_model_rs::v1::{
    artifact_staging_service_server::ArtifactStagingServiceServer,
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

    let job_service = FlareJobService::new();
    let artifact_service = FlareArtifactStagingService::new(
        ArtifactStore::from("/home/ganesh/Dev/flaredir/new", "kafka-to-pinecone").await?,
    );

    println!("Flared up 🔥 at {}", addr);

    Server::builder()
        .add_service(JobServiceServer::new(job_service))
        .add_service(ArtifactStagingServiceServer::new(artifact_service))
        .serve(addr)
        .await?;

    Ok(())
}
