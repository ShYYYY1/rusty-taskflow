use std::{any::Any, collections::{HashMap, VecDeque}, marker::PhantomData, sync::Arc};

use crate::tf::{dependency::{DependencyBuilder, OutputWrapper}, errors::{FlowError, TaskError}, task::TaskAdapter, traits::{AsyncTask, FromAnyVec, InvocableTask}};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub(crate) struct TaskId(pub usize);

#[derive(Clone)]
struct InDegree(u8);

struct TaskMeta {
    pub name: String,
}

pub struct Flow {
    tasks: HashMap<TaskId, Box<dyn InvocableTask>>,
    edges: HashMap<TaskId, Vec<TaskId>>,
    rev_edges: HashMap<TaskId, Vec<TaskId>>,
    indegrees: HashMap<TaskId, InDegree>,
    task_metas: HashMap<TaskId, TaskMeta>,
}

impl Flow {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            edges: HashMap::new(),
            rev_edges: HashMap::new(),
            indegrees: HashMap::new(),
            task_metas: HashMap::new(),
        }
    }

    pub fn commit_task<'flow, I, O>(
        &'flow mut self,
        name: impl Into<String>,
        task: impl AsyncTask<Input = I, Output = O> + Send + 'static
    ) -> DependencyBuilder<I, O>
    where
        I: FromAnyVec,
        O: Send + Sync + 'static
    {
        let task_id = TaskId(self.tasks.len());
        self.task_metas.insert(task_id.clone(), TaskMeta { name: name.into() });
        let typed_t = Box::new(TaskAdapter::new(task));
        self.tasks.entry(task_id.clone()).or_insert(typed_t);
        self.indegrees.entry(task_id.clone()).or_insert(InDegree(0));
        DependencyBuilder::new(task_id, self)
    }

    pub fn commit_source_task<'flow, I, O>(
        &'flow mut self,
        name: impl Into<String>,
        task: impl AsyncTask<Input = I, Output = O> + Send + 'static
    ) -> OutputWrapper<O>
    where
        I: FromAnyVec,
        O: Send + Sync + 'static
    {
        let task_id = TaskId(self.tasks.len());
        self.task_metas.insert(task_id.clone(), TaskMeta { name: name.into() });
        let typed_t = Box::new(TaskAdapter::new(task));
        self.tasks.entry(task_id.clone()).or_insert(typed_t);
        self.indegrees.entry(task_id.clone()).or_insert(InDegree(0));
        OutputWrapper::new(task_id)
    }

    pub fn add_edges(&mut self, from: TaskId, to: TaskId) {
        self.edges.entry(from.clone()).or_insert_with(Vec::new).push(to.clone());
        self.rev_edges.entry(to.clone()).or_insert_with(Vec::new).push(from);
        self.indegrees.entry(to).or_insert(InDegree(0)).0 += 1;
    }

    /// 分层并发执行 DAG，使用 outputs 仓库传递 task 之间的数据
    ///
    /// - source task (indegree=0, 无上游依赖): 用 `Arc::new(())` 作为输入
    /// - 有依赖的 task: 从 outputs 仓库取上游 task 的输出作为输入
    /// - `sink` 参数标识终点 task，其输出会被类型化返回
    pub async fn run<'flow, Output: Send + Sync + 'static>(
        &'flow mut self,
        sink: OutputWrapper<Output>,
    ) -> Result<Output, FlowError> {
        let layers = self.get_topological_layers()?;
        let mut outputs: HashMap<TaskId, Arc<dyn Any + Send + Sync>> = HashMap::new();

        for layer in layers {
            let mut handles = Vec::new();

            for task_id in &layer {
                // 从 tasks map 中取出 task (消费所有权)
                let task = self.tasks.remove(task_id)
                    .ok_or(FlowError::TaskNotFound(task_id.0))?;

                // 解析输入: 有上游依赖 -> 取 outputs; 无依赖 -> Arc::new(())
                let input: Vec<Arc<dyn Any + Send + Sync>> = match self.rev_edges.get(task_id) {
                    Some(deps) if !deps.is_empty() => {
                        deps.iter().filter_map(|id| {
                            outputs.get(id).cloned()
                        }).collect()
                    }
                    _ => {
                        // source task: 无输入依赖，传入 ()
                        Vec::new()
                    }
                };

                let fut = task.invoke(input);
                let tid = task_id.clone();
                handles.push((tid, tokio::spawn(fut)));
            }

            // 等待当前层所有 task 完成，收集输出到 outputs 仓库
            for (tid, handle) in handles {
                let result = handle.await
                    .map_err(|e| FlowError::TaskExecutionError(
                        TaskError::TaskExecutionError(tid.clone().0, e.to_string())
                    ))?
                    .map_err(|e| FlowError::TaskExecutionError(
                        TaskError::TaskExecutionError(tid.clone().0, e)
                    ))?;
                outputs.insert(tid, result);
            }
        }

        // 从 outputs 中取出 sink task 的输出，downcast 到具体类型
        let final_arc = outputs.remove(&sink.id)
            .ok_or_else(|| FlowError::TaskNotFound(sink.id.clone().0))?;
        let typed_arc = final_arc.downcast::<Output>()
            .map_err(|_| FlowError::TaskExecutionError(
                TaskError::TaskExecutionError(sink.id.clone().0, "output type mismatch".to_string())
            ))?;
        Arc::try_unwrap(typed_arc)
            .map_err(|_| FlowError::TaskExecutionError(
                TaskError::TaskExecutionError(sink.id.0, "output Arc has multiple owners".to_string())
            ))
    }

    fn get_topological_layers(&self) -> Result<Vec<Vec<TaskId>>, FlowError> {
        let mut tmp_indegree = self.indegrees.clone();
        // indegree = 0 的节点进入初始队列 (source tasks)
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
                // 叶子节点可能没有出边，用空 slice 兜底
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
        match self.task_metas.get(id).ok_or(FlowError::TaskMetaNotFound(id.clone().0)) {
            Ok(meta) => Ok(meta.name.as_str()),
            Err(e) => Err(e)
        }
    }
}

#[cfg(test)]
mod test {

    use taskflow_macros::sync_task;

    use super::*;

    struct StartData(u8);
    #[sync_task]
    impl StartData {
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
        fn run(self, data: u8) -> AddAndPrintOutput {
            println!("data: {}", data + 10);
            AddAndPrintOutput { data: data + 10, message: "this is AddAndPrint".to_string() }
        }
    }

    struct MultiplyAndPrintOutput(u8);

    struct MultiplyAndPrint {
        factor: u8
    }

    #[sync_task]
    impl MultiplyAndPrint {
        fn new() -> Self {
            MultiplyAndPrint { factor: 2 }
        }

        fn run (self, add_output: AddAndPrintOutput) -> MultiplyAndPrintOutput {
            println!("here is the message from prev node: {}", add_output.message);
            MultiplyAndPrintOutput(self.factor * add_output.data)
        }
    }

    #[tokio::test]
    async fn test_flow_run() {
        let mut flow = Flow::new();
        // source task: 无输入，输出 u8
        let start = flow.commit_source_task("StartTask", StartData(10));
        // 中间 task: 输入 u8，输出 AddAndPrintOutput
        let second = flow.commit_task("AddAndPrint", AddAndPrint).with_dependency(start);
        // sink task: 输入 AddAndPrintOutput，输出 MultiplyAndPrintOutput
        let third = flow.commit_task("MultiplyAndPrint", MultiplyAndPrint::new()).with_dependency(second);

        let result = flow.run(third).await.unwrap();
        // StartData(10) -> 10 -> AddAndPrint: 10+10=20 -> MultiplyAndPrint: 2*20=40
        assert_eq!(result.0, 40);
    }
}
