use taskflow::{async_task, sync_task};

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

pub struct Multiply {
    factor:u64,
}

impl Multiply {
    pub fn new() -> Self {
        Self { factor: 2 }
    }
}

#[sync_task(path = "::taskflow")]
impl Multiply {
    fn run(self, v: &u64) ->u64 {
        self.factor * v
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
