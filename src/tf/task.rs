use std::{any::Any, future::Future, pin::Pin, sync::Arc};

use crate::tf::{errors::FlowError, traits::{AsyncTask, FromAnyVec, InvocableTask}};

pub(crate) struct TaskInput<T = ()>(pub(crate) T);
pub(crate) struct TaskOutput<T = ()>(pub(crate) T);

type Inputs = Vec<Arc<dyn Any + Send + Sync>>;

// 0-tuple: source task
impl FromAnyVec for () {
    fn from_any_vec(inputs: Vec<Arc<dyn Any + Send + Sync>>) -> Result<Self, FlowError> {
        if !inputs.is_empty() {
            return Err(FlowError::SourceTaskHasNoInput(inputs.len()));
        }
        Ok(())
    }
}

macro_rules! impl_from_any_vec {
    ($($idx:tt : $T:ident),+) => {
        impl<$($T: Send + Sync + 'static),+> FromAnyVec for ($(Arc<$T>,)+) {
            fn from_any_vec(mut inputs: Vec<Arc<dyn Any + Send + Sync>>) -> Result<Self, FlowError> {
                const ARITY: usize = impl_from_any_vec!(@count $($T),+);
                if inputs.len() != ARITY {
                    return Err(FlowError::TaskInputsNumError(ARITY, inputs.len()));
                }
                inputs.reverse();
                Ok(($({
                        let arc = inputs.pop().unwrap();
                        arc.downcast::<$T>()
                            .map_err(|_| {
                                FlowError::TaskInputTypeDowncastError(
                                    format!("input[{}]: downcast to {} failed", $idx, std::any::type_name::<$T>()))
                            })?
                    },
                )+))
            }
        }
    };
    (@count $($T:ident),+) => { [$(impl_from_any_vec!(@replace $T ())),+].len() };
    (@replace $_t:ident $val:expr) => { $val };
}

// impl_from_any_vec! macro for generating FromAnyVec implementations for tuples, vector of 6 elements top supported
impl_from_any_vec!(0: A);
impl_from_any_vec!(0: A, 1: B);
impl_from_any_vec!(0: A, 1: B, 2: C);
impl_from_any_vec!(0: A, 1: B, 2: C, 3: D);
impl_from_any_vec!(0: A, 1: B, 2: C, 3: D, 4: E);
impl_from_any_vec!(0: A, 1: B, 2: C, 3: D, 4: E, 5: F);

pub struct TaskAdapter<I, O, T>
where 
    T: AsyncTask<Input = I, Output = O>,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static
{
    task: T,
}

impl<I, O, T> TaskAdapter<I, O, T>
where 
    T: AsyncTask<Input = I, Output = O>,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static
{
    pub fn new(task: T) -> Self {
        Self { task: task }
    }
}

impl<I, O, T> InvocableTask for TaskAdapter<I, O, T>
where 
    T: AsyncTask<Input = I, Output = O> + Send + 'static,
    I: FromAnyVec,
    O: Send + Sync + 'static
{
    fn invoke(self: Box<Self>, input: Inputs) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Any + Send + Sync>, String>> + Send>> {
        let input_tup = match I::from_any_vec(input) {
            Ok(v) => v,
            Err(e) => return Box::pin(async move { Err(e.to_string()) }),
        };
        Box::pin(async move {
            let TaskOutput(out) = self.task.run(TaskInput(input_tup)).await;
            Ok(Arc::new(out) as Arc<dyn Any + Send + Sync>)
        })
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use taskflow_macros::{async_task, sync_task};

    use super::*;

    struct AddAndPrint;

    #[async_task]
    impl AddAndPrint {
        pub async fn run(self, a: &i32, b: &i32) -> i32 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            println!("add res: {}", a + b);
            a + b
        }
    }

    struct MultiplyAndPrint;

    #[sync_task]
    impl MultiplyAndPrint {
        fn run(self, a: i32, b: i32) -> i32 {
            println!("multiply res: {}", a * b);
            a * b
        }
    }

    #[tokio::test]
    async fn test_add_and_print() {
        let add_task = AddAndPrint;
        let add_typed_task = TaskAdapter::new(add_task);
        let multi_task = MultiplyAndPrint;
        let multi_typed_task = TaskAdapter::new(multi_task);
        let task_list: Vec<Box<dyn InvocableTask>> = vec![Box::new(add_typed_task), Box::new(multi_typed_task)];
        let a_input: Vec<Arc<dyn Any + Send + Sync>> = vec![Arc::new(100), Arc::new(3000)];
        let mut task_list_iter = task_list.into_iter();
        let a_fut = task_list_iter.next().unwrap().invoke(a_input);
        let res = tokio::spawn(a_fut).await.unwrap().unwrap().downcast::<i32>();
        assert_eq!(*res.unwrap(), 3100i32)
    }
}
