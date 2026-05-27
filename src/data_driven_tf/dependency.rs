use std::sync::atomic::AtomicUsize;

use crate::data_driven_tf::{DataId, VertexId};

pub(crate) struct DepDef {
    pub(crate) src: VertexId,
    pub(crate) targets: Box<[DataId]>,
    // pub(crate) predicate: Box<dyn Fn() -> bool + Send + Sync>,
}

pub(crate) struct DepState {
    pub(crate) waiting: AtomicUsize,
}

impl DepState {
    pub(crate) fn new(targets_num: usize) -> Self {
        Self { waiting: AtomicUsize::new(targets_num) }
    }
}
