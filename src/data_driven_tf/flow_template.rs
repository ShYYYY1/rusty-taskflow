use std::collections::VecDeque;

use crate::data_driven_tf::{DataId, DepId, VertexId, data::{DataMeta, FlowData}, dependency::DepDef, error::FlowError, vertex::VertexDef};

/// Template to build Flow runtime, owns static metadatas and definitions
pub struct FlowTemplate {
    pub(crate) name: String,
    pub(crate) vertices: Box<[VertexDef]>,
    pub(crate) datas: Box<[DataMeta]>,
    pub(crate) dependencies: Box<[DepDef]>,
}

impl FlowTemplate {
    pub(crate) fn vertex_name(&self, id: VertexId) -> &str {
        self.vertices[id.0].name.as_str()
    }
}