use thiserror::Error;

#[derive(Error, Debug)]
pub enum FlowError {
    #[error("there is no runnable vertex")]
    NoRunnableVertex,

    #[error("cannot add duplicate data: {0}")]
    DuplicateData(String),

    #[error("data already published")]
    AlreadyPublished,

    #[error("try to publish data while it is not ready, id: {0}")]
    PublishDataWhileNotReady(usize),

    #[error("flow data of id: {0} cannot downcast to type: {1}")]
    FlowDataDownCastError(usize, &'static str),

    #[error("try to get unpublished flow data, id: {0}")]
    GetUnpublishedFlowData(usize),

    #[error("unexpected value retrieved in data of id: {0}")]
    UnexpectedValueRetrieved(usize),

    #[error("sender dropped without sending")]
    RecvError,
}
