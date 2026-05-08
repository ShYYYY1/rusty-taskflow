use crate::tf::{component_registry::FlowContext, errors::FlowError, flow::{Flow, TaskId}, task::{TaskInput, TaskOutput}};
use std::{any::Any, future::Future, pin::Pin, sync::Arc};

/// Converts a heterogeneous, type-erased iterator of upstream outputs
/// (`Arc<dyn Any + Send + Sync>`) into a concrete, strongly-typed tuple.
///
/// Implemented for the unit type `()` (source tasks with no inputs) and for
/// tuples of the form `(Arc<A>, Arc<B>, ...)` up to a fixed arity. Users
/// rarely implement this trait directly — the `#[sync_task]` / `#[async_task]`
/// proc macros pick the right implementation based on the `run` signature.
pub trait FromAnyIter: Sized + Send + Sync + 'static {
    fn from_any_iter(inputs: &mut dyn Iterator<Item = Arc<dyn Any + Send + Sync>>) -> Result<Self, FlowError>;
}

/// User-facing synchronous task trait.
///
/// You normally implement this indirectly through the `#[sync_task]` proc macro.
/// The macro translates a natural inherent `fn run(self, a: &A, b: &B) -> C` into
/// an impl of this trait with the correct tuple input type.
///
/// ## Accessing shared components
///
/// Every task execution is handed a `&FlowContext` that carries the process-wide
/// component registry (see [`crate::FlowContext`]). Declare it as the first
/// non-`self` parameter of your `run` function and the proc macro will forward
/// it automatically:
///
/// ```ignore
/// #[sync_task]
/// impl MyTask {
///     fn run(self, ctx: &FlowContext, a: &u8) -> u8 {
///         let db = ctx.get_singleton_component::<Db>("db").unwrap();
///         // ...
///         *a
///     }
/// }
/// ```
///
/// If you do not need the context, simply omit the parameter — the macro wires
/// a no-op forwarding.
pub trait SyncTask {
    type Input: Send + Sync + 'static;
    type Output;
    fn run(self, ctx: &FlowContext, input: TaskInput<Self::Input>) -> TaskOutput<Self::Output>;
}

/// User-facing asynchronous task trait. See [`SyncTask`] for the same rules
/// around `FlowContext` injection and macro-based authoring.
pub trait AsyncTask {
    type Input: Send + Sync + 'static;
    type Output;
    fn run(
        self,
        ctx: &FlowContext,
        input: TaskInput<Self::Input>,
    ) -> impl Future<Output = TaskOutput<Self::Output>> + Send;
}

impl<T> AsyncTask for T
where T: SyncTask + Send
{
    type Input = T::Input;
    type Output = T::Output;
    fn run(
        self,
        ctx: &FlowContext,
        input: TaskInput<Self::Input>,
    ) -> impl Future<Output = TaskOutput<Self::Output>> + Send {
        async move { T::run(self, ctx, input) }
    }
}

/// Runtime-erased task entry point used by the scheduler. End users should not
/// implement this — the blanket `TaskAdapter` implementation bridges it to
/// [`AsyncTask`] / [`SyncTask`].
///
/// `ctx` is passed as an `Arc<FlowContext>` so the produced future can own its
/// share of the context independently of the caller. The future borrows
/// `&ctx` only locally inside its body; the borrow never escapes the future.
pub trait InvocableTask {
    fn invoke(
        self: Box<Self>,
        ctx: Arc<FlowContext>,
        input: &mut dyn Iterator<Item = Arc<dyn Any + Send + Sync>>,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Any + Send + Sync>, FlowError>> + Send>>;
}

pub trait IntoDependencies<InputType> {
    fn register(self, flow: &mut Flow, target: &TaskId);
}
