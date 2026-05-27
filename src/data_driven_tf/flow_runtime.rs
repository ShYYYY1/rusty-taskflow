use std::{any::type_name, sync::{Arc, Mutex, OnceLock, Weak, atomic::{AtomicBool, AtomicUsize, Ordering}}};

use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::data_driven_tf::{DataId, DepId, VertexId, data::{AnyFlowData, DataHandle, FlowData}, dependency::DepState, error::FlowError, flow_template::FlowTemplate, vertex::VertexState};

pub struct OneshotWait {
    pending: AtomicUsize,
    has_failed: AtomicBool,
    sender: Mutex<Option<Sender<Result<(), FlowError>>>>,
    receiver: Mutex<Option<Receiver<Result<(), FlowError>>>>,
}

impl OneshotWait {
    /// 创建一次flow运行的管理者
    fn new() -> Self {
        let (tx, rx) = oneshot::channel::<Result<(), FlowError>>();
        Self {
            pending: AtomicUsize::new(0),
            has_failed: AtomicBool::new(false),
            sender: Mutex::new(Some(tx)),
            receiver: Mutex::new(Some(rx)),
        }
    }

    fn on_task_created(&self) {
        self.pending.fetch_add(1, Ordering::AcqRel);
    }

    fn on_task_done(&self) {
        if self.pending.fetch_sub(1, Ordering::AcqRel) == 1 &&
            !self.has_failed.load(Ordering::Acquire) {
            if let Some(sender) = self.sender.lock().unwrap().take() {
                let _ = sender.send(Ok(()));
            }
        }
    }

    fn on_task_failed(&self, error: FlowError) {
        if self.has_failed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok() {
            if let Some(sender) = self.sender.lock().unwrap().take() {
                let _ = sender.send(Err(error));
            }
        }
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }

    /// 等待flow运行完毕或传递出必须终止运行的致命错误
    async fn wait_for_done(&self) -> Result<(), FlowError> {
        let receiver = self.receiver.lock().unwrap().take()
            .expect("wait_for_done can only be called once");
        match receiver.await {
            Ok(res) => res,
            Err(_) => Err(FlowError::RecvError)
        }
    }
}

pub struct FlowRuntime {
    flow_template: Arc<FlowTemplate>,
    flow_run_waiter: OneshotWait,
    datas: Box<[Arc<dyn AnyFlowData>]>,
    vertices: Box<[VertexState]>,
    dependencies: Box<[DepState]>,
    self_ref: OnceLock<Weak<Self>>,
}

impl FlowRuntime {
    pub fn new(template: Arc<FlowTemplate>) -> Arc<Self> {
        let datas: Vec<Arc<dyn AnyFlowData>> = template.datas.iter()
            .map(|meta| {
                (meta.creator)()
            }).collect();
        let vertices: Vec<VertexState> = template.vertices.iter()
            .map(|vertex_def| {
                VertexState::new(vertex_def.dependencies.len())
            }).collect();
        let deps: Vec<DepState> = template.dependencies.iter()
            .map(|dep_def| {
                DepState::new(dep_def.targets.len())
            }).collect();
        let runtime_waiter = OneshotWait::new();
        let arc_self = Arc::new(Self {
            flow_template: template,
            flow_run_waiter: runtime_waiter,
            datas: datas.into_boxed_slice(),
            vertices: vertices.into_boxed_slice(),
            dependencies: deps.into_boxed_slice(),
            self_ref: OnceLock::new()
        });
        let _ = arc_self.self_ref.set(Arc::downgrade(&arc_self));
        arc_self
    }

    pub fn preset<T: Send + Sync + 'static>(&self, handle: &DataHandle<T>, val: T) -> Result<(), FlowError> {
        let target_arc = self.datas[handle.id().0].clone();
        let Some(flow_data) = target_arc
            .as_any()
            .downcast_ref::<FlowData<T>>() else {
            return Err(FlowError::FlowDataDownCastError(handle.id().0, type_name::<T>()));
        };
        flow_data.set_data(val)
    }

    fn rev_visit(&self, id: &DataId, visited: &mut [bool], runnable: &mut Vec<VertexId>) {
        if self.datas[id.0].is_ready() {
            // 数据已经就绪？
            return;
        }
        let data_meta = &self.flow_template.datas[id.0];
        let Some(producer_id) = data_meta.producer else {
            // 数据无产出者?
            return;
        };
        if visited[producer_id.0] {
            // 每个顶点只需要激活一次
            return;
        }
        visited[producer_id.0] = true;
        let vertex_def = &self.flow_template.vertices[producer_id.0];
        // 设置顶点依赖数
        self.vertices[producer_id.0].waiting.store(vertex_def.dependencies.len(), Ordering::Relaxed);
        // 激活vertex的依赖
        for dep in vertex_def.dependencies.iter() {
            let target_data = &self.flow_template.dependencies[dep.0].targets;
            // 设置依赖目标数据数
            self.dependencies[dep.0].waiting.store(target_data.len(), Ordering::Relaxed);
            for did in target_data.iter() {
                if self.datas[did.0].is_ready() {
                    // 数据已preset，立刻减少dependency等待数，如果减少后dependency ready，立刻减少vertex等待数
                    if self.dependencies[dep.0].waiting
                        .fetch_sub(1, Ordering::Relaxed) == 1 {
                        self.vertices[producer_id.0].waiting
                            .fetch_sub(1, Ordering::Relaxed);
                    }
                } else {
                    self.rev_visit(did, visited, runnable);
                }
            }
        }
        if self.vertices[producer_id.0].waiting.load(Ordering::Relaxed) == 0 {
            runnable.push(producer_id);
        }
    }

    // 传入待计算的数据集合，分别反向激活数据所需的所有上游,返回当前可运行的所有顶点集合
    fn activate(&self, targets: &[DataId]) -> Result<Vec<VertexId>, FlowError> {
        let mut runnable_vertices = Vec::new();
        let mut visited = vec![false; self.flow_template.vertices.len()];
        for id in targets.iter() {
            self.rev_visit(id, &mut visited, &mut runnable_vertices);
        }
        Ok(runnable_vertices)
    }

    pub async fn run(self: &Arc<Self>, targets: &[DataId]) -> Result<(), FlowError> {
        let Ok(runnable) = self.activate(targets) else {
            return Err(FlowError::NoRunnableVertex);
        };
        for vid in runnable {
            self.try_spawn(vid);
        }
        let _ = self.wait().await?;
        Ok(())
    }

    fn dep_satisfied(&self, dep_id: DepId) -> bool {
        self.dependencies[dep_id.0]
            .waiting
            .fetch_sub(1, Ordering::AcqRel) == 1
    }

    fn vertex_ready(&self, vertex_id: VertexId) -> bool {
        let v_state = &self.vertices[vertex_id.0];
        v_state.waiting.fetch_sub(1, Ordering::AcqRel) == 1
    }

    pub(crate) fn get_data<T>(&self, data_id: DataId) -> Result<&FlowData<T>, FlowError>
    where
        T: Send + Sync + 'static,
    {
        self.datas[data_id.0]
            .as_any()
            .downcast_ref::<FlowData<T>>()
            .ok_or_else(|| FlowError::FlowDataDownCastError(data_id.0, type_name::<T>()))
    }

    // publish data
    pub(crate) fn publish_data<T>(&self, data_id: DataId, val: T) -> Result<(), FlowError>
    where
        T: Send + Sync + 'static,
    {
        let data = self.datas[data_id.0]
            .as_any()
            .downcast_ref::<FlowData<T>>()
            .ok_or_else(|| FlowError::FlowDataDownCastError(data_id.0, type_name::<T>()))?;
        let _ = data.set_data(val)?;
        Ok(())
    }

    pub(crate) fn on_data_published(&self, data_id: DataId) -> Result<(), FlowError> {
        if !self.datas[data_id.0].is_ready() {
            return Err(FlowError::PublishDataWhileNotReady(data_id.0));
        }
        let meta = &self.flow_template.datas[data_id.0];
        for dep_id in meta.successors.iter() {
            if self.dep_satisfied(*dep_id) {
                let depdef = &self.flow_template.dependencies[dep_id.0];
                if self.vertex_ready(depdef.src) {
                    // spawn a task to run vertex
                    self.try_spawn(depdef.src);
                }
            }
        }
        Ok(())
    }

    fn try_spawn(&self, vertex_id: VertexId) {
        if let Some(me) = self.self_ref.get() {
            match me.upgrade() {
                Some(arc) => arc.spawn(vertex_id),
                None => {
                    eprintln!("FlowRuntime has been dropped");
                    return
                }
            }
        }
    }

    fn spawn(self: &Arc<Self>, vertex_id: VertexId) {
        if self.vertices[vertex_id.0].activated
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err() {
            eprintln!("vertex: {} is already activated", self.flow_template.vertex_name(vertex_id));
            return;
        }
        self.flow_run_waiter.on_task_created();
        let vertex_def = &self.flow_template.vertices[vertex_id.0];
        let runtime = self.clone();
        let vertex_id = vertex_id.0;
        let _ = tokio::spawn(async move {
            let vertex_def = &runtime.flow_template.vertices[vertex_id];
            let accessor = RuntimeDataAccessor { flow_runtime: &runtime };
            // processor可以被多个runtime共享
            let processor = vertex_def.processor.clone();
            if let Err(exec_error) = processor(accessor).await {
                runtime.flow_run_waiter.on_task_failed(exec_error);
            } else {
                runtime.flow_run_waiter.on_task_done();
            }
        });
    }

    /// 等待flow运行完毕或传递出必须终止运行的致命错误
    pub async fn wait(&self) -> Result<(), FlowError> {
        self.flow_run_waiter.wait_for_done().await
    }
}

pub struct RuntimeDataAccessor<'r> {
    flow_runtime: &'r FlowRuntime,
}

impl<'r> RuntimeDataAccessor<'r> {
    /// get reference to Node's dependent data
    pub(crate) fn get_dependent_data<T>(&self, data_handle: &DataHandle<T>) -> Result<&FlowData<T>, FlowError>
    where
        T: Send + Sync + 'static,
    {
        self.flow_runtime.get_data(data_handle.id())
    }

    /// publish a data exactly once, trigger successors' state updating
    pub(crate) fn publish_data<T>(&self, data_handle: &DataHandle<T>, val: T) -> Result<(), FlowError>
    where
        T: Send + Sync + 'static,
    {
        self.flow_runtime.publish_data(data_handle.id(), val)?;
        self.flow_runtime.on_data_published(data_handle.id())?;
        Ok(())
    }
}