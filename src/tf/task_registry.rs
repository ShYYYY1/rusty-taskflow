use crate::tf::{traits::InvocableTask, task::TaskAdapter};

struct TaskFactory {
    name: &'static str,
    creator: fn() -> Box<dyn InvocableTask>
}

inventory::collect!(TaskFactory);


macro_rules! register_task_with_name {
    ($name: literal, $init: expr) => {
        inventory::submit! {
            TaskFactory {
                name: $name,
                creator: || Box::new(TaskAdapter::new($init())),
            }
        }
    };
    ($name: literal, $init: expr, $($arg: expr),+) => {
        inventory::submit! {
            TaskFactory {
                name: $name,
                creator: || Box::new(TaskAdapter::new($init($($arg),+)))
            }
        }
    };
}

pub fn create_invocable_task(name: &str) -> Option<Box<dyn InvocableTask>> {
    inventory::iter::<TaskFactory>
        .into_iter()
        .find(|task_factory| task_factory.name == name)
        .map(|task_factory| (task_factory.creator)())
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, num::ParseIntError, time::Duration};

    use taskflow_macros::{async_task, sync_task};

    use super::*;

    struct SourceTask1(u8);
    #[sync_task]
    impl SourceTask1 {
        fn new() -> Self {
            Self(10)
        }

        fn run(self) -> u8 {
            self.0
        }
    }
    register_task_with_name!("SourceTask1", SourceTask1::new);

    struct SourceTask2 {
        val1: u8,
        val2: String
    }
    #[async_task]
    impl SourceTask2 {
        fn new(v1: u8, v2: impl Into<String>) -> Self {
            Self { val1: v1, val2: v2.into() }
        }

        async fn run(self) -> u8 {
            let v = self.val2.parse::<u8>().unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.val1 + v
        }
    }
    register_task_with_name!("SourceTask2", SourceTask2::new, 10, "10");

    #[tokio::test]
    async fn test_registry() {
        let t1 = create_invocable_task("SourceTask1").unwrap();
        let t2 = create_invocable_task("SourceTask2").unwrap();
        let mut result = Vec::new();
        result.push(tokio::spawn(t1.invoke(VecDeque::new())));
        result.push(tokio::spawn(t2.invoke(VecDeque::new())));
        let mut results = Vec::new();
        for handle in result {
            let res = handle.await.unwrap().unwrap();
            results.push(res.downcast_ref::<u8>().cloned());
        }
        let sum = results.into_iter().filter_map(|ele| ele).sum::<u8>();
        assert_eq!(30, sum);
    }
}