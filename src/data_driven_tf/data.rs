use std::{any::{Any, TypeId, type_name}, default, marker::PhantomData, sync::{Arc, OnceLock}};

use crate::data_driven_tf::{DataId, DepId, VertexId, error::FlowError, flow_runtime::RuntimeDataAccessor};

pub(crate) type DataCreator = Box<dyn Fn() -> Arc<dyn AnyFlowData> + Send + Sync>;

pub(crate) struct DataMeta {
    pub(crate) name: String,  // name of data
    pub(crate) producer: Option<VertexId>,  // producer of the data
    pub(crate) successors: Box<[DepId]>,  // successors of the data
    pub(crate) creator: DataCreator,  // data's creator
}

impl DataMeta {
    pub(crate) fn new(name: &str, creator: DataCreator) -> Self {
        Self { name: name.to_string(), producer: None, successors: Box::new([]), creator: creator }
    }
}

pub struct DataHandle<T> {
    id: DataId,
    phantom: PhantomData<T>
}

impl<T> DataHandle<T> {
    pub(crate) fn new(id: DataId) -> Self {
        Self { id, phantom: PhantomData }
    }

    pub fn id(&self) -> DataId {
        self.id.clone()
    }
}

struct Unset;
struct Set;

pub(crate) struct Commiter<'flow, State, DataType>
where 
    DataType: Send + Sync + 'static
{
    data: &'flow FlowData<DataType>,
    state_phantom: PhantomData<State>
}

pub struct FlowData<T: Send + Sync + 'static> {
    id: DataId,
    slot: OnceLock<T>,
}

impl<T> FlowData<T>
where 
    T: Send + Sync + 'static,
{
    pub(crate) fn new(id: DataId) -> Self {
        Self { id, slot: OnceLock::new() }
    }

    pub fn get_published_data(&self) -> Result<Option<&T>, FlowError> {
        match self.slot.get() {
            Some(data) => Ok(Some(data)),
            None => Err(FlowError::GetUnpublishedFlowData(self.get_data_id()))
        }
    }

    pub fn set_data(&self, val: T) -> Result<(), FlowError> {
        self.slot.set(val).map_err(|_| FlowError::AlreadyPublished) 
    }

    pub fn is_ready(&self) -> bool {
        match self.slot.get() {
            Some(_) => true,
            None => false
        }
    }

    pub fn get_data_id(&self) -> usize {
        self.id.0
    }
}

pub trait AnyFlowData: Send + Sync {
    fn is_ready(&self) -> bool;
    fn as_any(&self) -> &dyn Any;
    fn type_name(&self) -> &'static str;
}

impl<T: Send + Sync + 'static> AnyFlowData for FlowData<T> {
    fn is_ready(&self) -> bool {
        self.is_ready()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn type_name(&self) -> &'static str {
        type_name::<T>()
    }
}
