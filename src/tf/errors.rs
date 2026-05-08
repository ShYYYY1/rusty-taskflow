use thiserror::Error;

#[derive(Error, Debug)]
pub enum FlowError {
    #[error("flow has cycle")]
    HasCycle,

    #[error("expected 0 inputs, got {0}")]
    SourceTaskHasNoInput(usize),

    #[error("expected {0} inputs, got {1}")]
    TaskInputsNumError(usize, usize),

    #[error("input type downcast error: {0}")]
    TaskInputTypeDowncastError(String),

    #[error("task: {0} not found")]
    TaskNotFound(usize),

    #[error("task: {0}'s meta not found")]
    TaskMetaNotFound(usize),

    #[error("invalid flow configuration: {0}")]
    ConfigBuildError(String),

    #[error("failed to execute tid:{0}, message: {1}")]
    TaskExecutionError(usize, String),
}
