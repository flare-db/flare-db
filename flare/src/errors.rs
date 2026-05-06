// errors.rs — one place, used everywhere
#[derive(Debug, thiserror::Error)]
pub enum BeamTranslationError {
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Invalid State: {0}")]
    InvalidState(String),

    #[error("Missing environment on transform node: {0}")]
    MissingEnvironment(String),

    #[error("Stage fusion error: {0}")]
    StageFusionError(String),

    #[error("Pipeline graph error: {0}")]
    PipelineGraphError(String),

    #[error("Value not found error: {0}")]
    NotFound(String),
}

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("Harness not connected: {0}")]
    NotConnected(String),
    #[error("Harness disconnected: {0}")]
    Disconnected(String),
    #[error("Control stream error: {0}")]
    StreamError(String),
    #[error("Instruction ID mismatch: expected {expected}, got {actual}")]
    IdMismatch { expected: String, actual: String },
    #[error("Unexpected response: {0}")]
    UnexpectedResponse(String),
    #[error("Send error: {0}")]
    SendError(String),
}

pub enum TransformError {
    Error(String),
}
