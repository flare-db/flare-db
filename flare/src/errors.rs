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
