use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::tf::{
    dependency::{DependencyBuilder, OutputWrapper},
    errors::{FlowError, TaskError},
    task::TaskAdapter,
    traits::{AsyncTask, FromAnyVecDeque, InvocableTask},
};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct TaskId(pub usize);

#[derive(Clone)]
struct InDegree(u8);

// todo complete meta datas
struct TaskMeta {
    pub name: String,
}

pub struct FlowContext {
    // todo implement FlowContext for managing the flow's state and context
}

pub struct Flow {
    tasks: Vec<Option<Box<dyn InvocableTask>>>,
    edges: HashMap<TaskId, Vec<TaskId>>,
    rev_edges: HashMap<TaskId, Vec<TaskId>>,
    indegrees: HashMap<TaskId, InDegree>,
    task_metas: HashMap<TaskId, TaskMeta>,
}

impl Flow {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            edges: HashMap::new(),
            rev_edges: HashMap::new(),
            indegrees: HashMap::new(),
            task_metas: HashMap::new(),
        }
    }

    pub fn commit_task<'flow, I, O>(
        &'flow mut self,
        name: impl Into<String>,
        task: impl AsyncTask<Input = I, Output = O> + Send + 'static,
    ) -> DependencyBuilder<'flow, I, O>
    where
        I: FromAnyVecDeque,
        O: Send + Sync + 'static,
    {
        let task_id = TaskId(self.tasks.len());
        self.task_metas
            .insert(task_id.clone(), TaskMeta { name: name.into() });
        let typed_t = Box::new(TaskAdapter::new(task));
        self.tasks.push(Some(typed_t));
        self.indegrees.entry(task_id.clone()).or_insert(InDegree(0));
        DependencyBuilder::new(task_id, self)
    }

    pub fn commit_source_task<'flow, I, O>(
        &'flow mut self,
        name: impl Into<String>,
        task: impl AsyncTask<Input = I, Output = O> + Send + 'static,
    ) -> OutputWrapper<O>
    where
        I: FromAnyVecDeque,
        O: Send + Sync + 'static,
    {
        let task_id = TaskId(self.tasks.len());
        self.task_metas
            .insert(task_id.clone(), TaskMeta { name: name.into() });
        let typed_t = Box::new(TaskAdapter::new(task));
        self.tasks.push(Some(typed_t));
        self.indegrees.entry(task_id.clone()).or_insert(InDegree(0));
        OutputWrapper::new(task_id)
    }

    pub fn commit_invocable_task(
        &mut self,
        name: impl Into<String>,
        task: Box<dyn InvocableTask>,
    ) -> TaskId {
        let task_id = TaskId(self.tasks.len());
        self.task_metas
            .insert(task_id.clone(), TaskMeta { name: name.into() });
        self.tasks.push(Some(task));
        self.indegrees.entry(task_id.clone()).or_insert(InDegree(0));
        task_id
    }

    pub fn add_edges(&mut self, from: TaskId, to: TaskId) {
        self.edges
            .entry(from.clone())
            .or_insert_with(Vec::new)
            .push(to.clone());
        self.rev_edges
            .entry(to.clone())
            .or_insert_with(Vec::new)
            .push(from);
        self.indegrees.entry(to).or_insert(InDegree(0)).0 += 1;
    }

    async fn execute(&mut self) -> Result<HashMap<TaskId, Arc<dyn Any + Send + Sync>>, FlowError> {
        let layers = self.get_topological_layers()?;
        let mut outputs: HashMap<TaskId, Arc<dyn Any + Send + Sync>> = HashMap::new();

        for layer in layers {
            let mut handles = Vec::new();

            if layer.len() == 1 {
                let task_id = &layer[0];
                if let Some(task) = self.tasks[task_id.0].take() {
                    let inputs: VecDeque<Arc<dyn Any + Send + Sync>> = self
                        .rev_edges
                        .get(task_id)
                        .map(|deps| deps.iter().filter_map(|id| outputs.get(id).cloned()).collect())
                        .unwrap_or_default();
                    let output = task.invoke(inputs).await.map_err(|e| {
                        FlowError::TaskExecutionError(TaskError::TaskExecutionError(task_id.0, e))
                    })?;
                    outputs.insert(task_id.clone(), output);
                }
                continue;
            }

            for task_id in &layer {
                if let Some(task) = self.tasks[task_id.0].take() {
                    let inputs: VecDeque<Arc<dyn Any + Send + Sync>> = self
                        .rev_edges
                        .get(task_id)
                        .map(|deps| deps.iter().filter_map(|id| outputs.get(id).cloned()).collect())
                        .unwrap_or_default();
                    let fut = task.invoke(inputs);
                    handles.push((task_id.clone(), tokio::spawn(fut)));
                }
            }

            for (tid, handle) in handles {
                let result = handle
                    .await
                    .map_err(|e| {
                        FlowError::TaskExecutionError(TaskError::TaskExecutionError(
                            tid.clone().0,
                            e.to_string(),
                        ))
                    })?
                    .map_err(|e| {
                        FlowError::TaskExecutionError(TaskError::TaskExecutionError(tid.clone().0, e))
                    })?;
                outputs.insert(tid, result);
            }
        }

        Ok(outputs)
    }

    pub async fn run<Output: Send + Sync + 'static>(
        &mut self,
        sink: OutputWrapper<Output>,
    ) -> Result<Output, FlowError> {
        let final_arc = self
            .execute()
            .await?
            .remove(&sink.id)
            .ok_or_else(|| FlowError::TaskNotFound(sink.id.clone().0))?;
        let typed_arc = final_arc.downcast::<Output>().map_err(|_| {
            FlowError::TaskExecutionError(TaskError::TaskExecutionError(
                sink.id.clone().0,
                "output type mismatch".to_string(),
            ))
        })?;

        Arc::try_unwrap(typed_arc).map_err(|_| {
            FlowError::TaskExecutionError(TaskError::TaskExecutionError(
                sink.id.0,
                "output Arc has multiple owners".to_string(),
            ))
        })
    }

    pub async fn run_with_sink_id(
        &mut self,
        sink_id: TaskId,
    ) -> Result<Arc<dyn Any + Send + Sync>, FlowError> {
        self.execute()
            .await?
            .remove(&sink_id)
            .ok_or(FlowError::TaskNotFound(sink_id.0))
    }

    fn get_topological_layers(&self) -> Result<Vec<Vec<TaskId>>, FlowError> {
        let mut tmp_indegree = self.indegrees.clone();
        let mut queue: VecDeque<TaskId> = tmp_indegree
            .iter()
            .filter(|(_, indegree)| indegree.0 == 0)
            .map(|(taskid, _)| taskid.clone())
            .collect();
        let mut layers: Vec<Vec<TaskId>> = Vec::new();
        let mut visited = 0;
        while !queue.is_empty() {
            let cur_layer: Vec<TaskId> = queue.drain(..).collect();
            for t in &cur_layer {
                visited += 1;
                if let Some(successors) = self.edges.get(t) {
                    for succ in successors {
                        let deg = tmp_indegree.get_mut(succ).unwrap();
                        deg.0 -= 1;
                        if deg.0 == 0 {
                            queue.push_back(succ.clone());
                        }
                    }
                }
            }
            layers.push(cur_layer);
        }
        if visited != self.tasks.len() {
            return Err(FlowError::HasCycle);
        }
        Ok(layers)
    }

    fn get_task_name(&self, id: &TaskId) -> Result<&str, FlowError> {
        match self
            .task_metas
            .get(id)
            .ok_or(FlowError::TaskMetaNotFound(id.clone().0))
        {
            Ok(meta) => Ok(meta.name.as_str()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod test {

    use taskflow_macros::{async_task, sync_task};

    use super::*;

    struct StartData1(u8);
    #[sync_task]
    impl StartData1 {
        fn run(self) -> u8 {
            self.0
        }
    }

    struct StartData2(u8);
    #[sync_task]
    impl StartData2 {
        fn run(self) -> u8 {
            self.0
        }
    }

    #[derive(Clone)]
    struct AddAndPrintOutput {
        pub data: u8,
        pub message: String,
    }

    struct AddAndPrint;
    #[sync_task]
    impl AddAndPrint {
        fn run(self, data1: &u8, data2: &u8) -> AddAndPrintOutput {
            println!("data: {}", data1 + data2);
            AddAndPrintOutput {
                data: data1 + data2,
                message: "this is AddAndPrint".to_string(),
            }
        }
    }

    #[derive(Clone)]
    struct MultiplyAndPrintOutput(u8);

    struct MultiplyAndPrint {
        factor: u8,
    }

    #[sync_task]
    impl MultiplyAndPrint {
        fn new() -> Self {
            MultiplyAndPrint { factor: 2 }
        }

        fn run(self, add_output: &AddAndPrintOutput) -> MultiplyAndPrintOutput {
            println!("here is the message from prev node: {}", add_output.message);
            MultiplyAndPrintOutput(self.factor * add_output.data)
        }
    }

    struct GenerateThreeValue;
    #[derive(Clone)]
    struct GenerateThreeValueOutput(u8, u8, u8);
    #[sync_task]
    impl GenerateThreeValue {
        fn run(self) -> GenerateThreeValueOutput {
            GenerateThreeValueOutput(10, 20, 30)
        }
    }

    struct AddThree;

    #[async_task]
    impl AddThree {
        async fn run(self, a: &GenerateThreeValueOutput) -> u8 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            a.0 + a.1 + a.2
        }
    }

    struct FinalTask;

    #[async_task]
    impl FinalTask {
        async fn run(self, a: &MultiplyAndPrintOutput, b: &u8) -> u8 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            println!("Final result: pipeline1: {}, pipeline2: {}", a.0, b);
            a.0 + b
        }
    }

    #[tokio::test]
    async fn test_flow_run() {
        let mut flow = Flow::new();
        // pipeline1
        let start1 = flow.commit_source_task("StartTask1", StartData1(10));
        let start2 = flow.commit_source_task("StartTask2", StartData2(21));
        let line1_second = flow
            .commit_task("AddAndPrint", AddAndPrint)
            .with_dependencies((start1, start2));
        let line1_final = flow
            .commit_task("MultiplyAndPrint", MultiplyAndPrint::new())
            .with_dependencies(line1_second);

        // pipeline2
        let generate_three = flow.commit_source_task("GenerateThreeValue", GenerateThreeValue);
        let line2_final = flow
            .commit_task("AddThree", AddThree)
            .with_dependencies(generate_three);

        let final_task = flow
            .commit_task("FinalTask", FinalTask)
            .with_dependencies((line1_final, line2_final));
        let result = flow.run(final_task).await.unwrap();

        assert_eq!(result, 122);
    }
}
