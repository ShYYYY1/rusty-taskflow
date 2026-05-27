use std::{pin::Pin, sync::Arc};

use crate::data_driven_tf::{DataId, DepId, error::FlowError, flow_runtime::RuntimeDataAccessor};
use async_trait::async_trait;

#[async_trait]
pub trait Processor: Send + Sync + 'static {
    fn init(&mut self);
    fn deps(&self) -> Vec<DataId>;
    fn emits(&self) -> Vec<DataId>;
    async fn run(&self, data_accessor: RuntimeDataAccessor<'_>) -> Result<(), FlowError>;
}

pub(crate) type ProcessorRunner =
    Arc<dyn for<'r> Fn(RuntimeDataAccessor<'r>) ->
        Pin<Box<dyn Future<Output = Result<(), FlowError>> + Send + 'r>>
        + Send + Sync,
    >;

pub fn make_runtime_processor<P: Processor + ?Sized>(processor: Arc<P>) -> ProcessorRunner {
    Arc::new(move |data_accessor| {
        let processor_clone = processor.clone();
        Box::pin(async move { processor_clone.run(data_accessor).await })
    })
}

#[cfg(test)]
mod test {
    use crate::data_driven_tf::data::DataHandle;

use super::*;

    struct StartProcessor {
        dep: DataHandle<u32>,
        emit: DataHandle<u32>,
        val: u32,
    }

    #[async_trait]
    impl Processor for StartProcessor {
        fn init(&mut self) {
            self.val = 10;
        }

        fn deps(&self) -> Vec<DataId> {
            vec![self.dep.id().clone()]
        }

        fn emits(&self) -> Vec<DataId> {
            vec![self.emit.id().clone()]
        }

        async fn run(&self, data_accessor: RuntimeDataAccessor<'_>) -> Result<(), FlowError> {
            Ok(())
        }
    }
}