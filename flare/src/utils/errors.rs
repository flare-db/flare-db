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

#[derive(Debug, thiserror::Error)]
pub enum CodersError {
    #[error("Error while decoding: {0}")]
    WhileDecoding(String),

    #[error("Error while encoding: {0}")]
    WhileEncoding(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ElementStoreError {
    #[error("Failed to open database: {0}")]
    Open(String),

    #[error("Failed to write collection: {0}")]
    Write(String),

    #[error("Failed to read collection: {0}")]
    Read(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Element not found: {0}")]
    NotFound(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Unsupported type or shape: {0}")]
    UnsupportedType(String),

    #[error("Offset overflow: {0}")]
    OffsetOverflow(String),

    #[error("Unknown field: {0}")]
    UnknownField(String),
}

pub enum TransformError {
    Error(String),
}
