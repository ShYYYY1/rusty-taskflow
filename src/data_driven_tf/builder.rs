use std::{collections::HashMap, sync::Arc};

use crate::data_driven_tf::{DataId, DepId, VertexId, data::{AnyFlowData, DataCreator, DataHandle, DataMeta, FlowData}, dependency::DepDef, error::FlowError, processor::{Processor, make_runtime_processor}, vertex::VertexDef};

/// builder of FlowTemplate

#[derive(Default)]
pub struct FlowTemplateBuilder {
    datas: Vec<DataMeta>,
    vertices: Vec<VertexDef>,
    dependencies: Vec<DepDef>,
    data_by_name: HashMap<String, DataId>,
}

impl FlowTemplateBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_data<T>(&mut self, name: &str) -> Result<DataHandle<T>, FlowError>
    where 
        T: Send + Sync + 'static,
    {
        if self.data_by_name.contains_key(name) {
            return Err(FlowError::DuplicateData(name.to_string()));
        }
        let id = DataId(self.datas.len());
        self.data_by_name.insert(name.to_owned(), id);
        let data_creator: DataCreator = Box::new(move || {
            Arc::new(FlowData::<T>::new(id)) as Arc<dyn AnyFlowData>
        });
        self.datas.push(DataMeta::new(name, data_creator));
        Ok(DataHandle::<T>::new(id))
    }

    pub fn add_node(&mut self, name: impl Into<String>, node: Arc<dyn Processor>) {
        let dep_datas = node.deps();
        let emit_datas = node.emits();

        let vertex_id = self.vertices.len();
        let dep_id = self.dependencies.len();

        let dep_def = DepDef {
            src: VertexId(vertex_id),
            targets: dep_datas.into_boxed_slice(),
        };
        self.dependencies.push(dep_def);

        let vertex_def = VertexDef {
            name: name.into(),
            dependencies: vec![DepId(dep_id)].into_boxed_slice(),
            emits: emit_datas.into_boxed_slice(),
            processor: make_runtime_processor(node),
        };
        self.vertices.push(vertex_def);
    }
}

#[cfg(test)]
mod test {
    use crate::data_driven_tf::{flow_runtime::RuntimeDataAccessor, processor::Processor};

use super::*;

    struct StartNode {
        name: String,
        input: DataHandle<u32>,
        output: DataHandle<u32>,
        factor: u32,
    }

    impl StartNode {
        fn new(name: &str, dep: DataHandle<u32>, emit: DataHandle<u32>) -> Self {
            Self { name: name.to_string(), input: dep, output: emit, factor: 0 }
        }
    }

    use async_trait::async_trait;

    #[async_trait]
    impl Processor for StartNode {
        fn init(&mut self) {
            self.factor = 2;
        }

        fn deps(&self) -> Vec<DataId> {
            vec![self.input.id()]
        }

        fn emits(&self) -> Vec<DataId> {
            vec![self.output.id()]
        }

        async fn run(&self, data_accessor: RuntimeDataAccessor<'_>) -> Result<(), FlowError> {
            let dep = data_accessor
                .get_dependent_data::<u32>(&self.input)?;
            if let Some(dep_val) = dep.get_published_data()? {
                let emit_result = *dep_val * self.factor;
                data_accessor.publish_data(&self.output, emit_result)?;
                Ok(())
            } else {
                // empty value was published, in this case it is not expected
                println!("emtpy value was published in data of id: {}", dep.get_data_id());
                Err(FlowError::UnexpectedValueRetrieved(dep.get_data_id()))
            }
        }
    }

    #[test]
    fn test_builder() -> Result<(), FlowError> {
        let mut builder = FlowTemplateBuilder::new();
        // add all datas with their names
        let start_data = builder.add_data::<u32>("start_data")?;
        let middle_data = builder.add_data::<u32>("middle_data")?;
        let result_data = builder.add_data::<u32>("result_data")?;
        // add nodes
        builder.add_node("StartNode", Arc::new(StartNode::new("StartNode", start_data, middle_data)));
        Ok(())
    }
}