use figment::Error;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Deserialize, Serialize, Debug)]
pub enum TaskError {
    #[error("failed to execute tid:{0}, message: {1}")]
    TaskExecutionError(usize, String)
}

#[derive(Error, Debug)]
pub enum FlowError {
    #[error("configuration error: {0}")]
    ConfigError(#[from] Error),

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

    #[error(transparent)]
    TaskExecutionError(#[from] TaskError),
}
