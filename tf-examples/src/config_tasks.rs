use std::sync::atomic::{AtomicU64, Ordering};

use taskflow::{async_task, register_factory, register_singleton, sync_task, FlowContext};

/// A singleton that lives inside the `FlowContext` and exposes a tunable
/// multiplier that downstream tasks can read without having to receive it
/// through the DAG.
pub struct MultiplierConfig {
    pub factor: u64,
}

impl MultiplierConfig {
    pub fn new() -> Self {
        Self { factor: 3 }
    }
}

register_singleton!(MultiplierConfig, "multiplier_config", MultiplierConfig::new);

/// A factory component: every `create_component::<RequestId>("request_id")`
/// call returns a fresh, unique id. Demonstrates per-invocation component
/// construction as opposed to the shared-by-reference singleton above.
pub struct RequestId(pub u64);

impl RequestId {
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

register_factory!(RequestId, "request_id", RequestId::new);

pub struct FibSource1;

impl FibSource1 {
    pub fn new() -> Self {
        Self
    }
}

#[sync_task(path = "::taskflow")]
impl FibSource1 {
    fn run(self) ->u64 {
        10
    }
}

pub struct FibSource2;

impl FibSource2 {
    pub fn new() -> Self {
        Self
    }
}

#[sync_task(path = "::taskflow")]
impl FibSource2 {
    fn run(self) ->u64 {
        10
    }
}

pub struct Merger;

impl Merger {
    pub fn new() -> Self {
        Self
    }
}

#[sync_task(path = "::taskflow")]
impl Merger {
    fn run(self, v1: &u64, v2: &u64) ->u64 {
        println!("Merger output: {}", v1 + v2);
        v1 + v2
    }
}

pub struct Fib;
#[sync_task(path = "::taskflow")]
impl Fib {
    pub fn new() -> Self {
        Self
    }

    fn fib(v: &u64) ->u64 {
        if *v <= 1u64 {
            return *v;
        }
        Self::fib(&(*v - 1)) + Self::fib(&(*v - 2))
    }

    fn run(self, v: &u64) ->u64 {
        let res = Self::fib(v);
        println!("Fib result: {res}");
        res
    }
}

/// Uses the shared `MultiplierConfig` singleton from the `FlowContext` to
/// amplify its single input, and tags the log with a fresh `RequestId` pulled
/// from the factory. Note how the task signature declares
/// `ctx: &FlowContext` as the first non-`self` parameter — the proc macro
/// wires it from the scheduler automatically and does **not** treat it as a
/// DAG input.
pub struct Multiply;

impl Multiply {
    pub fn new() -> Self {
        Self
    }
}

#[sync_task(path = "::taskflow")]
impl Multiply {
    fn run(self, ctx: &FlowContext, v: &u64) -> u64 {
        let cfg = ctx
            .get_singleton_component::<MultiplierConfig>("multiplier_config")
            .expect("multiplier_config singleton must be registered");
        let req = ctx
            .create_component::<RequestId>("request_id")
            .expect("request_id factory must be registered");
        let result = cfg.factor * v;
        println!(
            "Multiply[req={}]: {} * {} = {}",
            req.0, cfg.factor, v, result
        );
        result
    }
}

pub struct FibInput;

#[sync_task(path = "::taskflow")]
impl FibInput {
    pub fn new() -> Self {
        Self
    }

    fn run(self) -> u64 {
        18
    }
}

pub struct AsyncPersistFib;

#[async_task(path = "::taskflow")]
impl AsyncPersistFib {
    pub fn new() -> Self {
        Self
    }

    async fn run(self, fib: &u64) -> u64 {
        let output_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("outputs");
        tokio::fs::create_dir_all(&output_dir)
            .await
            .expect("failed to create tf-examples/outputs");

        let output_file = output_dir.join("mixed_fib_result.txt");
        tokio::fs::write(&output_file, format!("fib_result={fib}\n"))
            .await
            .expect("failed to write mixed_fib_result.txt");

        println!("AsyncPersistFib wrote {}", output_file.display());
        *fib
    }
}

pub struct DoubleSink;

#[sync_task(path = "::taskflow")]
impl DoubleSink {
    pub fn new() -> Self {
        Self
    }

    fn run(self, value: &u64) -> u64 {
        value * 2
    }
}
