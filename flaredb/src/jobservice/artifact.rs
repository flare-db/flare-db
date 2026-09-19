use beam_model_rs::v1::{
    ArtifactRequestWrapper, ArtifactResponseWrapper, ArtifactStagingToRolePayload, GetArtifactRequest,
    ResolveArtifactsRequest, artifact_request_wrapper, artifact_response_wrapper,
    artifact_staging_service_server::ArtifactStagingService,
};
use dashmap::DashSet;
use log::info;
use prost::Message;
use std::{path::Path, pin::Pin, sync::Arc};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

pub struct FlareArtifactStagingService {
    store: Arc<ArtifactStore>,
    staging_tokens: Arc<DashSet<String>>,
}
impl FlareArtifactStagingService {
    pub fn new(store: Arc<ArtifactStore>, staging_tokens: Arc<DashSet<String>>) -> Self {
        Self {
            store,
            staging_tokens,
        }
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

        let staging_tokens = self.staging_tokens.clone();
        tokio::spawn(async move {
            match client_stream.next().await {
                Some(Ok(warapper)) => {
                    let staging_token = warapper.staging_token;

                    // Validate staging token
                    if !staging_tokens.contains(&staging_token) {
                        eprintln!("Invalid staging token: {}", staging_token);
                        return;
                    }

                    info!("Received and validated staging token");

                    // Reset artifact store before starting staging session.
                    if let Err(e) = store.reset().await {
                        eprintln!("failed to reset artifact store before staging: {}", e);
                        return;
                    }

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
                                info!("Received resolve response from client with {} artifact(s)", resolve_response.replacements.len());

                                for artifact_info in resolve_response.replacements {
                                    info!("Fetched artifact info");

                                    // Determine relative target path for this artifact
                                    let staged_name = if !artifact_info.role_payload.is_empty() {
                                        match ArtifactStagingToRolePayload::decode(
                                            artifact_info.role_payload.as_slice(),
                                        ) {
                                            Ok(payload) if !payload.staged_name.is_empty() => {
                                                payload.staged_name
                                            }
                                            _ => store.default_file_name().to_string(),
                                        }
                                    } else {
                                        store.default_file_name().to_string()
                                    };

                                    info!("Staging artifact to relative path: {}", staged_name);
                                    if let Err(e) = store.begin_file(&staged_name).await {
                                        eprintln!("failed to begin file for {}: {}", staged_name, e);
                                        return;
                                    }

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
                                        eprintln!("Failed to send get artifact request");
                                        return;
                                    }

                                    loop {
                                        match client_stream.next().await {
                                            Some(Ok(artifact_response)) => {
                                                if let Some(artifact_response_wrapper::Response::GetArtifactResponse(res)) = artifact_response.response {
                                                    // Save the artifact chunk
                                                    if let Err(e) = store.stage_artifact(&res.data).await {
                                                        eprintln!("artifact write failed for {}: {}", staged_name, e);
                                                        return;
                                                    }
                                                    if artifact_response.is_last {
                                                        info!("Artifact staging complete for {}", staged_name);
                                                        break;
                                                    }
                                                } else {
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

            info!("Artifact staging session complete");
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ReverseArtifactRetrievalServiceStream
        ))
    }
}

pub struct ArtifactStore {
    path: String,
    default_file_name: String,
    current_file: Mutex<Option<File>>,
    current_relative_path: Mutex<Option<String>>,
}

impl ArtifactStore {
    pub async fn from(path: &str, file_name: &str) -> Result<Self, std::io::Error> {
        let staging_path = format!("{}/{}", path, file_name);

        // ensure directory exists
        fs::create_dir_all(path).await?;

        println!("creating default artifact file {}", staging_path);
        let file = Some(File::create(&staging_path).await?);

        Ok(Self {
            path: path.to_string(),
            default_file_name: file_name.to_string(),
            current_file: Mutex::new(file),
            current_relative_path: Mutex::new(Some(file_name.to_string())),
        })
    }

    /// Resets the current file handle and directory staging state.
    pub async fn reset(&self) -> Result<(), std::io::Error> {
        let mut file_guard = self.current_file.lock().await;
        let mut path_guard = self.current_relative_path.lock().await;
        *file_guard = None;
        *path_guard = None;
        Ok(())
    }

    /// Prepares a new file for staging under the specified relative path.
    /// Parent directories are automatically created if needed.
    pub async fn begin_file(&self, relative_path: &str) -> Result<(), std::io::Error> {
        let full_path = Path::new(&self.path).join(relative_path);

        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        println!("staging artifact file: {}", full_path.display());
        let file = File::create(&full_path).await?;

        let mut file_guard = self.current_file.lock().await;
        let mut path_guard = self.current_relative_path.lock().await;
        *file_guard = Some(file);
        *path_guard = Some(relative_path.to_string());

        Ok(())
    }

    /// Writes a chunk of bytes to the currently active artifact file.
    pub async fn stage_artifact(&self, chunk: &[u8]) -> Result<(), std::io::Error> {
        let mut guard = self.current_file.lock().await;

        let file = guard.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "file not initialized")
        })?;

        file.write_all(chunk).await?;
        file.flush().await?;

        Ok(())
    }

    /// Returns default/primary staged artifact file path for backward compatibility.
    pub fn staged_path(&self) -> String {
        format!("{}/{}", self.path, self.default_file_name)
    }

    pub fn root_path(&self) -> &str {
        &self.path
    }

    pub fn default_file_name(&self) -> &str {
        &self.default_file_name
    }
}

