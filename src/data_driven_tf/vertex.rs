use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize}};

use crate::{data_driven_tf::{DataId, DepId, processor::{Processor, ProcessorRunner}}, tf::traits::InvocableTask};

/// definition of vertex
pub(crate) struct VertexDef {
    pub(crate) name: String,
    pub(crate) dependencies: Box<[DepId]>,
    pub(crate) emits: Box<[DataId]>,
    pub(crate) processor: ProcessorRunner
}

pub(crate) struct VertexState {
    pub(crate) waiting: AtomicUsize,
    pub(crate) activated: AtomicBool,
}

impl VertexState {
    pub(crate) fn new(dep_num: usize) -> Self {
        Self { waiting: AtomicUsize::new(dep_num), activated: AtomicBool::new(false) }
    }
}