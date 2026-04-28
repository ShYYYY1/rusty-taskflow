use crate::tf::{errors::FlowError, flow::{Flow, TaskId}, task::{TaskInput, TaskOutput}};
use std::{any::Any, collections::VecDeque, future::Future, pin::Pin, sync::Arc};

pub trait FromAnyVecDeque: Sized + Send + Sync + 'static {
    fn from_any_vecdeque(inputs: VecDeque<Arc<dyn Any + Send + Sync>>) -> Result<Self, FlowError>;
}

pub trait SyncTask {
    type Input: Send + Sync + 'static;
    type Output;
    fn run(self, input: TaskInput<Self::Input>) -> TaskOutput<Self::Output>;
}

pub trait AsyncTask {
    type Input: Send + Sync + 'static;
    type Output;
    fn run(self, input: TaskInput<Self::Input>) -> impl Future<Output = TaskOutput<Self::Output>> + Send;
}

impl<T> AsyncTask for T
    where T: SyncTask + Send
{
    type Input = T::Input;
    type Output = T::Output;
    fn run(self, input: TaskInput<Self::Input>) -> impl Future<Output = TaskOutput<Self::Output>> + Send {
        (async move || { T::run(self, input) })()
    }
}

pub trait InvocableTask {
    fn invoke(self: Box<Self>, input: VecDeque<Arc<dyn Any + Send + Sync>>) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Any + Send + Sync>, String>> + Send>>;
}

pub trait IntoDependencies<InputType> {
    fn register(self, flow: &mut Flow, target: &TaskId);
}
