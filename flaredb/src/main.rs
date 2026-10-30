use flaredb::{
    engine::{capacity::PoolConfig, dispatcher::ExecutorDispatcher, harness::pool::HarnessPool},
    io::server::FlareIO,
    jobservice::{
        artifact::{ArtifactStore, FlareArtifactStagingService},
        server::FlareJobService,
    },
    worker::manager::{WorkerLaunchConfig, WorkerManager},
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
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

    let pool_config = PoolConfig::detect();
    log::info!(
        "worker pool: max_workers={}, worker_parallelism={}, total_bundle_slots={}, \
         sdf_queue_max_items={}, sdf_queue_max_bytes={}, sdf_max_split_depth={}",
        pool_config.worker_count,
        pool_config.bundles_per_worker,
        pool_config.total_bundle_slots,
        pool_config.sdf_queue_max_items,
        pool_config.sdf_queue_max_bytes,
        pool_config.sdf_max_split_depth
    );
    let workers = HarnessPool::build(pool_config, flaredb::grpc_max_message_size_bytes()).await?;
    let dispatcher = Arc::new(Mutex::new(ExecutorDispatcher::new(workers).await?));

    let worker_jar = std::env::var("WORKER_JAR_PATH").unwrap_or_else(|_| {
        format!(
            "{}/bin/beam-sdks-java-harness-2.76.0-flare-bundled.jar",
            std::env::args().nth(1).unwrap_or_else(|| ".".to_string())
        )
    });

    let worker_cfg = WorkerLaunchConfig {
        worker_jar,
        connect_timeout_secs: 20,
        python_bin: std::env::var("FLAREDB_PYTHON_BIN").ok(),
    };
    let worker_manager = WorkerManager::new(worker_cfg);
    let job_service = FlareJobService::with(
        artifact_store.clone(),
        worker_manager,
        instance_id,
        dispatcher,
    );

    let artifact_service =
        FlareArtifactStagingService::new(artifact_store, job_service.get_staging_tokens());

    let flare_io = FlareIO::new().await?;
    let grpc_max_message_size = flaredb::grpc_max_message_size_bytes();
    log::info!("gRPC message size limit: {} bytes", grpc_max_message_size);

    Server::builder()
        .add_service(
            JobServiceServer::new(job_service)
                .max_decoding_message_size(grpc_max_message_size)
                .max_encoding_message_size(grpc_max_message_size),
        )
        .add_service(
            ArtifactStagingServiceServer::new(artifact_service)
                .max_decoding_message_size(grpc_max_message_size)
                .max_encoding_message_size(grpc_max_message_size),
        )
        .add_service(
            flare_io
                .into_server()
                .max_decoding_message_size(grpc_max_message_size)
                .max_encoding_message_size(grpc_max_message_size),
        )
        .serve(addr)
        .await?;

    Ok(())
}
