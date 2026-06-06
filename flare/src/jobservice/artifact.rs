use beam_model_rs::v1::{
    ArtifactRequestWrapper, ArtifactResponseWrapper, GetArtifactRequest, ResolveArtifactsRequest,
    artifact_request_wrapper, artifact_response_wrapper,
    artifact_staging_service_server::ArtifactStagingService,
};
use log::info;
use std::{pin::Pin, sync::Arc};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

pub struct FlareArtifactStagingService {
    store: Arc<ArtifactStore>,
}
impl FlareArtifactStagingService {
    pub fn new(store: Arc<ArtifactStore>) -> Self {
        Self { store }
    }
}
// stream that will send requests back to the client
type ResponseStream = Pin<Box<dyn Stream<Item = Result<ArtifactRequestWrapper, Status>> + Send>>;

#[tonic::async_trait]
impl ArtifactStagingService for FlareArtifactStagingService {
    type ReverseArtifactRetrievalServiceStream = ResponseStream;

    async fn reverse_artifact_retrieval_service(
        &self,
        request: Request<Streaming<ArtifactResponseWrapper>>,
    ) -> Result<Response<Self::ReverseArtifactRetrievalServiceStream>, Status> {
        let mut client_stream = request.into_inner();

        let store = self.store.clone();

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ArtifactRequestWrapper, Status>>(32);

        tokio::spawn(async move {
            match client_stream.next().await {
                Some(Ok(warapper)) => {
                    let staging_token = warapper.staging_token;
                    // TODO: Validate staging token

                    info!("Received staging token");

                    let resolve_request = ArtifactRequestWrapper {
                        request: Some(artifact_request_wrapper::Request::ResolveArtifact(
                            ResolveArtifactsRequest {
                                artifacts: vec![],
                                preferred_urns: vec!["beam:env:process:v1".to_string()],
                            },
                        )),
                    };

                    if tx.send(Ok(resolve_request)).await.is_err() {
                        eprintln!("Failed to send resolve request");
                        return;
                    }

                    match client_stream.next().await {
                        Some(Ok(response)) => {
                            if let Some(
                                artifact_response_wrapper::Response::ResolveArtifactResponse(
                                    resolve_response,
                                ),
                            ) = response.response
                            {
                                info!("Received resolve response from client");

                                for artifact_info in resolve_response.replacements {
                                    info!("Fetched artfacts info");
                                    let get_request = ArtifactRequestWrapper {
                                        request: Some(
                                            artifact_request_wrapper::Request::GetArtifact(
                                                GetArtifactRequest {
                                                    artifact: Some(artifact_info),
                                                },
                                            ),
                                        ),
                                    };

                                    if tx.send(Ok(get_request)).await.is_err() {
                                        eprintln!("Faild to send get artfacts request")
                                    }

                                    loop {
                                        match client_stream.next().await {
                                            Some(Ok(artifact_response)) => {
                                                if let Some(artifact_response_wrapper::Response::GetArtifactResponse(res)) = artifact_response.response{

                                                    // Save the artifact data
                                                    if let Err(e) = store.stage_artifact(&res.data).await{
                                                        eprintln!("artifact write failed: {}", e);
                                                        return;
                                                    }
                                                    if artifact_response.is_last {
                                                        info!("Artifacts staging complete");
                                                        break;
                                                    }
                                                }else {
                                                    eprintln!("Unexpected response type");
                                                    break;
                                                }
                                            }
                                            Some(Err(e)) => {
                                                eprintln!("Error receiving artifact: {:?}", e);
                                                return;
                                            }
                                            None => {
                                                eprintln!("Client stream ended unexpectedly");
                                                return;
                                            }
                                        }
                                    }
                                }
                            } else {
                                println!(
                                    "Received unexpected response from client: {:#?}",
                                    response
                                );
                                return;
                            }
                        }
                        Some(Err(e)) => {
                            eprintln!("Error receiving resolve response: {:?}", e);
                            return;
                        }
                        None => {
                            eprintln!("Client disconnected");
                            return;
                        }
                    }
                }
                Some(Err(e)) => {
                    eprintln!("Error receiving staging token: {:?}", e);
                    return;
                }
                None => {
                    eprintln!("Client disconnected before sending staging token");
                    return;
                }
            }

            info!("Artifact staging complete");
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ReverseArtifactRetrievalServiceStream
        ))
    }
}

pub struct ArtifactStore {
    path: String,
    file_name: String,
    file: Mutex<Option<File>>,
}

impl ArtifactStore {
    pub async fn from(path: &str, file_name: &str) -> Result<Self, std::io::Error> {
        let staging_path = format!("{}/{}", path, file_name);
        //print!("inside store constructor");

        // ensure directory exists
        fs::create_dir_all(path).await?;

        // Always (re)create the staged artifact file for this session.
        // Previous behavior left `file=None` when file already existed, causing
        // "file not initialized" at write time.
        println!("creating file {}", staging_path);
        let file = Some(File::create(&staging_path).await?);

        Ok(Self {
            path: path.to_string(),
            file_name: file_name.to_string(),
            file: Mutex::new(file),
        })
    }
    pub async fn stage_artifact(&self, chunk: &[u8]) -> Result<(), std::io::Error> {
        let mut guard = self.file.lock().await;

        let file = guard.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "file not initialized")
        })?;

        file.write_all(chunk).await?;
        file.flush().await?;

        Ok(())
    }

    pub fn staged_path(&self) -> String {
        format!("{}/{}", self.path, self.file_name)
    }

    pub fn fetch_artiafct(&self) {}

    pub fn delete_artifact(&self) {}
}

/*
if ArtifactResolveRequestWarapper:: resolve request {
send resolve request
get resolve artifact response
     if  ArtifactResolveRequestWarapper::resolve artifact response{
     send get artfact info request
     get artifact info response
            stage artfacts locally
     }
} */
