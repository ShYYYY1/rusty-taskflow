//! Performance benchmark tests for the Flow DAG executor.
//!
//! Run with:
//!   cargo test --release bench_ -- --nocapture --test-threads=1
//!
//! Categories:
//!   1. CPU-intensive  (fibonacci computation)
//!   2. IO-intensive   (async sleep)
//!   3. Mixed          (CPU + IO combined)
//!
//! Each test compares `taskflow::Flow` against a manual tokio baseline
//! to measure pure framework overhead.
//!
//! To compare against external DAG libraries (dagrs, dagx, etc.),
//! add them as [dev-dependencies] in Cargo.toml and extend the
//! individual test functions with equivalent graph constructions.

use std::time::{Duration, Instant};

use dagx::{DagRunner, TaskHandle};
use taskflow_macros::{async_task, sync_task};

use crate::tf::dependency::OutputWrapper;
use crate::tf::flow::Flow;

// =========================================================================
// Helpers
// =========================================================================

/// Clone an `OutputWrapper` to enable fan-out from a single task output.
fn dup<T>(out: &OutputWrapper<T>) -> OutputWrapper<T> {
    OutputWrapper::new(out.id.clone())
}

fn header(title: &str) {
    println!("\n{}", "=".repeat(72));
    println!("  {title}");
    println!("{}", "=".repeat(72));
}

#[derive(Clone, Copy, Debug)]
struct BenchStats {
    mean_ms: f64,
    median_ms: f64,
    p95_ms: f64,
    stddev_ms: f64,
}

impl BenchStats {
    fn from_samples(samples: &[f64]) -> Self {
        assert!(!samples.is_empty(), "samples must not be empty");

        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let n = sorted.len();
        let mean = sorted.iter().sum::<f64>() / n as f64;
        let median = if n % 2 == 1 {
            sorted[n / 2]
        } else {
            (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
        };
        let p95_idx = ((n * 95).div_ceil(100)).saturating_sub(1);
        let p95 = sorted[p95_idx];
        let variance = sorted
            .iter()
            .map(|v| {
                let d = *v - mean;
                d * d
            })
            .sum::<f64>()
            / n as f64;

        Self {
            mean_ms: mean,
            median_ms: median,
            p95_ms: p95,
            stddev_ms: variance.sqrt(),
        }
    }
}

#[derive(Clone, Debug)]
struct PhaseStats {
    build: BenchStats,
    exec: BenchStats,
    total: BenchStats,
}

impl PhaseStats {
    fn from_samples(build: &[f64], exec: &[f64], total: &[f64]) -> Self {
        Self {
            build: BenchStats::from_samples(build),
            exec: BenchStats::from_samples(exec),
            total: BenchStats::from_samples(total),
        }
    }
}

fn print_table_header() {
    println!("    +----------+-----------+-----------+-----------+-----------+-----------+");
    println!("    | engine   |   mean ms | median ms |    p95 ms |    std ms | ovhd vs b |");
    println!("    +----------+-----------+-----------+-----------+-----------+-----------+");
}

fn print_table_row(engine: &str, stats: BenchStats, overhead_pct: Option<f64>) {
    let overhead = overhead_pct
        .map(|v| format!("{v:+6.1}%"))
        .unwrap_or_else(|| "   -   ".to_string());
    println!(
        "    | {engine:<8} | {mean:>9.2} | {median:>9.2} | {p95:>9.2} | {std:>9.2} | {overhead:>9} |",
        engine = engine,
        mean = stats.mean_ms,
        median = stats.median_ms,
        p95 = stats.p95_ms,
        std = stats.stddev_ms,
        overhead = overhead,
    );
}


fn row(label: &str, tf: BenchStats, bl: BenchStats) {
    let tf_overhead = if bl.mean_ms > 0.01 {
        (tf.mean_ms - bl.mean_ms) / bl.mean_ms * 100.0
    } else {
        0.0
    };

    println!("  {label}");
    print_table_header();
    print_table_row("taskflow", tf, Some(tf_overhead));
    print_table_row("baseline", bl, None);
    println!("    +----------+-----------+-----------+-----------+-----------+-----------+");
}

fn row3(label: &str, tf: BenchStats, dx: BenchStats, bl: BenchStats) {
    fn pct(a: f64, b: f64) -> f64 {
        if b > 0.01 { (a - b) / b * 100.0 } else { 0.0 }
    }

    println!("  {label}");
    print_table_header();
    print_table_row("taskflow", tf, Some(pct(tf.mean_ms, bl.mean_ms)));
    print_table_row("dagx", dx, Some(pct(dx.mean_ms, bl.mean_ms)));
    print_table_row("baseline", bl, None);
    println!("    +----------+-----------+-----------+-----------+-----------+-----------+");
}

fn print_phase_table_header() {
    println!("    +----------+-----------+-----------+-----------+-----------+");
    println!("    | engine   | build ms  |  exec ms  | total ms  | ovhd vs b |");
    println!("    +----------+-----------+-----------+-----------+-----------+");
}

fn print_phase_row(engine: &str, stats: &PhaseStats, overhead_pct: Option<f64>) {
    let overhead = overhead_pct
        .map(|v| format!("{v:+6.1}%"))
        .unwrap_or_else(|| "   -   ".to_string());
    println!(
        "    | {engine:<8} | {build:>9.2} | {exec:>9.2} | {total:>9.2} | {overhead:>9} |",
        engine = engine,
        build = stats.build.mean_ms,
        exec = stats.exec.mean_ms,
        total = stats.total.mean_ms,
        overhead = overhead,
    );
}

fn row3_phase(label: &str, tf: &PhaseStats, dx: &PhaseStats, bl: &PhaseStats) {
    fn pct(a: f64, b: f64) -> f64 {
        if b > 0.01 { (a - b) / b * 100.0 } else { 0.0 }
    }

    println!("  {label}");
    print_phase_table_header();
    print_phase_row("taskflow", tf, Some(pct(tf.total.mean_ms, bl.total.mean_ms)));
    print_phase_row("dagx", dx, Some(pct(dx.total.mean_ms, bl.total.mean_ms)));
    print_phase_row("baseline", bl, None);
    println!("    +----------+-----------+-----------+-----------+-----------+");
}

async fn timed_value<F, Fut, T>(f: &mut F) -> (f64, T)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let t0 = Instant::now();
    let value = f().await;
    (t0.elapsed().as_secs_f64() * 1000.0, value)
}

async fn bench_three_with_value<TF, DF, BF, FutT, FutD, FutB, T, D, B>(
    warmup: usize,
    runs: usize,
    mut tf: TF,
    mut dx: DF,
    mut bl: BF,
) -> ((BenchStats, T), (BenchStats, D), (BenchStats, B))
where
    TF: FnMut() -> FutT,
    DF: FnMut() -> FutD,
    BF: FnMut() -> FutB,
    FutT: std::future::Future<Output = T>,
    FutD: std::future::Future<Output = D>,
    FutB: std::future::Future<Output = B>,
{
    let total = warmup + runs;
    let mut tf_samples = Vec::with_capacity(runs);
    let mut dx_samples = Vec::with_capacity(runs);
    let mut bl_samples = Vec::with_capacity(runs);
    let mut last_tf = None;
    let mut last_dx = None;
    let mut last_bl = None;

    for round in 0..total {
        let record = round >= warmup;
        match round % 3 {
            0 => {
                let (t, v) = timed_value(&mut tf).await;
                if record { tf_samples.push(t); }
                last_tf = Some(v);
                let (t, v) = timed_value(&mut dx).await;
                if record { dx_samples.push(t); }
                last_dx = Some(v);
                let (t, v) = timed_value(&mut bl).await;
                if record { bl_samples.push(t); }
                last_bl = Some(v);
            }
            1 => {
                let (t, v) = timed_value(&mut dx).await;
                if record { dx_samples.push(t); }
                last_dx = Some(v);
                let (t, v) = timed_value(&mut bl).await;
                if record { bl_samples.push(t); }
                last_bl = Some(v);
                let (t, v) = timed_value(&mut tf).await;
                if record { tf_samples.push(t); }
                last_tf = Some(v);
            }
            _ => {
                let (t, v) = timed_value(&mut bl).await;
                if record { bl_samples.push(t); }
                last_bl = Some(v);
                let (t, v) = timed_value(&mut tf).await;
                if record { tf_samples.push(t); }
                last_tf = Some(v);
                let (t, v) = timed_value(&mut dx).await;
                if record { dx_samples.push(t); }
                last_dx = Some(v);
            }
        }
    }

    (
        (BenchStats::from_samples(&tf_samples), last_tf.expect("taskflow result missing")),
        (BenchStats::from_samples(&dx_samples), last_dx.expect("dagx result missing")),
        (BenchStats::from_samples(&bl_samples), last_bl.expect("baseline result missing")),
    )
}

async fn bench_three_split_with_value<TF, DF, BF, FutT, FutD, FutB, T, D, B>(
    warmup: usize,
    runs: usize,
    mut tf: TF,
    mut dx: DF,
    mut bl: BF,
) -> ((PhaseStats, T), (PhaseStats, D), (PhaseStats, B))
where
    TF: FnMut() -> FutT,
    DF: FnMut() -> FutD,
    BF: FnMut() -> FutB,
    FutT: std::future::Future<Output = ((f64, f64), T)>,
    FutD: std::future::Future<Output = ((f64, f64), D)>,
    FutB: std::future::Future<Output = ((f64, f64), B)>,
{
    let total = warmup + runs;

    let mut tf_build = Vec::with_capacity(runs);
    let mut tf_exec = Vec::with_capacity(runs);
    let mut tf_total = Vec::with_capacity(runs);

    let mut dx_build = Vec::with_capacity(runs);
    let mut dx_exec = Vec::with_capacity(runs);
    let mut dx_total = Vec::with_capacity(runs);

    let mut bl_build = Vec::with_capacity(runs);
    let mut bl_exec = Vec::with_capacity(runs);
    let mut bl_total = Vec::with_capacity(runs);

    let mut last_tf = None;
    let mut last_dx = None;
    let mut last_bl = None;

    for round in 0..total {
        let record = round >= warmup;
        match round % 3 {
            0 => {
                let ((b, e), v) = tf().await;
                if record {
                    tf_build.push(b);
                    tf_exec.push(e);
                    tf_total.push(b + e);
                }
                last_tf = Some(v);

                let ((b, e), v) = dx().await;
                if record {
                    dx_build.push(b);
                    dx_exec.push(e);
                    dx_total.push(b + e);
                }
                last_dx = Some(v);

                let ((b, e), v) = bl().await;
                if record {
                    bl_build.push(b);
                    bl_exec.push(e);
                    bl_total.push(b + e);
                }
                last_bl = Some(v);
            }
            1 => {
                let ((b, e), v) = dx().await;
                if record {
                    dx_build.push(b);
                    dx_exec.push(e);
                    dx_total.push(b + e);
                }
                last_dx = Some(v);

                let ((b, e), v) = bl().await;
                if record {
                    bl_build.push(b);
                    bl_exec.push(e);
                    bl_total.push(b + e);
                }
                last_bl = Some(v);

                let ((b, e), v) = tf().await;
                if record {
                    tf_build.push(b);
                    tf_exec.push(e);
                    tf_total.push(b + e);
                }
                last_tf = Some(v);
            }
            _ => {
                let ((b, e), v) = bl().await;
                if record {
                    bl_build.push(b);
                    bl_exec.push(e);
                    bl_total.push(b + e);
                }
                last_bl = Some(v);

                let ((b, e), v) = tf().await;
                if record {
                    tf_build.push(b);
                    tf_exec.push(e);
                    tf_total.push(b + e);
                }
                last_tf = Some(v);

                let ((b, e), v) = dx().await;
                if record {
                    dx_build.push(b);
                    dx_exec.push(e);
                    dx_total.push(b + e);
                }
                last_dx = Some(v);
            }
        }
    }

    (
        (
            PhaseStats::from_samples(&tf_build, &tf_exec, &tf_total),
            last_tf.expect("taskflow result missing"),
        ),
        (
            PhaseStats::from_samples(&dx_build, &dx_exec, &dx_total),
            last_dx.expect("dagx result missing"),
        ),
        (
            PhaseStats::from_samples(&bl_build, &bl_exec, &bl_total),
            last_bl.expect("baseline result missing"),
        ),
    )
}

async fn bench_three<TF, DF, BF, FutT, FutD, FutB>(
    warmup: usize,
    runs: usize,
    mut tf: TF,
    mut dx: DF,
    mut bl: BF,
) -> (BenchStats, BenchStats, BenchStats)
where
    TF: FnMut() -> FutT,
    DF: FnMut() -> FutD,
    BF: FnMut() -> FutB,
    FutT: std::future::Future<Output = ()>,
    FutD: std::future::Future<Output = ()>,
    FutB: std::future::Future<Output = ()>,
{

    let total = warmup + runs;
    let mut tf_samples = Vec::with_capacity(runs);
    let mut dx_samples = Vec::with_capacity(runs);
    let mut bl_samples = Vec::with_capacity(runs);

    for round in 0..total {
        let record = round >= warmup;
        match round % 3 {
            0 => {
                let (t, _) = timed_value(&mut tf).await;
                if record { tf_samples.push(t); }
                let (t, _) = timed_value(&mut dx).await;
                if record { dx_samples.push(t); }
                let (t, _) = timed_value(&mut bl).await;
                if record { bl_samples.push(t); }
            }
            1 => {
                let (t, _) = timed_value(&mut dx).await;
                if record { dx_samples.push(t); }
                let (t, _) = timed_value(&mut bl).await;
                if record { bl_samples.push(t); }
                let (t, _) = timed_value(&mut tf).await;
                if record { tf_samples.push(t); }
            }
            _ => {
                let (t, _) = timed_value(&mut bl).await;
                if record { bl_samples.push(t); }
                let (t, _) = timed_value(&mut tf).await;
                if record { tf_samples.push(t); }
                let (t, _) = timed_value(&mut dx).await;
                if record { dx_samples.push(t); }
            }
        }
    }

    (
        BenchStats::from_samples(&tf_samples),
        BenchStats::from_samples(&dx_samples),
        BenchStats::from_samples(&bl_samples),
    )
}


async fn bench_two<TF, BF, FutT, FutB>(
    warmup: usize,
    runs: usize,
    mut tf: TF,
    mut bl: BF,
) -> (BenchStats, BenchStats)
where
    TF: FnMut() -> FutT,
    BF: FnMut() -> FutB,
    FutT: std::future::Future<Output = ()>,
    FutB: std::future::Future<Output = ()>,
{
    let total = warmup + runs;
    let mut tf_samples = Vec::with_capacity(runs);
    let mut bl_samples = Vec::with_capacity(runs);

    for round in 0..total {
        let record = round >= warmup;
        if round % 2 == 0 {
            let (t, _) = timed_value(&mut tf).await;
            if record { tf_samples.push(t); }
            let (t, _) = timed_value(&mut bl).await;
            if record { bl_samples.push(t); }
        } else {
            let (t, _) = timed_value(&mut bl).await;
            if record { bl_samples.push(t); }
            let (t, _) = timed_value(&mut tf).await;
            if record { tf_samples.push(t); }
        }
    }

    (BenchStats::from_samples(&tf_samples), BenchStats::from_samples(&bl_samples))
}

// =========================================================================
// CPU work
// =========================================================================

fn fib(n: u32) -> u64 {
    if n <= 1 {
        return n as u64;
    }
    fib(n - 1) + fib(n - 2)
}

#[derive(Clone)]
pub struct ComplexPayload {
    bytes: Vec<u8>,
    tag: u64,
}

fn make_complex_payload(seed: u8, len: usize) -> ComplexPayload {
    let bytes = (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect();
    ComplexPayload {
        bytes,
        tag: seed as u64,
    }
}

fn mutate_complex_payload(input: &ComplexPayload, salt: u8) -> ComplexPayload {
    let mut bytes = input.bytes.clone();
    for (i, b) in bytes.iter_mut().enumerate() {
        let bias = salt.wrapping_add((i % 17) as u8);
        *b = b.wrapping_add(bias);
    }
    let byte_mix = bytes
        .iter()
        .step_by(97)
        .fold(0u64, |acc, v| acc.wrapping_add(*v as u64));
    ComplexPayload {
        bytes,
        tag: input.tag.wrapping_mul(131).wrapping_add(byte_mix),
    }
}

fn merge_complex_payload(a: &ComplexPayload, b: &ComplexPayload) -> ComplexPayload {
    let bytes = a
        .bytes
        .iter()
        .zip(&b.bytes)
        .map(|(x, y)| x.wrapping_add(*y))
        .collect();
    ComplexPayload {
        bytes,
        tag: a.tag.wrapping_add(b.tag).wrapping_mul(31),
    }
}

fn complex_payload_score(input: &ComplexPayload) -> u64 {
    let sample_sum = input
        .bytes
        .iter()
        .step_by(113)
        .fold(0u64, |acc, v| acc.wrapping_add(*v as u64));
    input.tag.wrapping_add(sample_sum)
}

// =========================================================================
// Task definitions
// =========================================================================

// ---- CPU tasks ----

struct CpuSource(u32);
#[sync_task]
impl CpuSource {
    fn run(self) -> u64 {
        fib(self.0)
    }
}

struct CpuStep(u32);
#[sync_task]
impl CpuStep {
    fn run(self, v: &u64) -> u64 {
        fib(self.0).wrapping_add(*v)
    }
}

struct CpuAdd2;
#[sync_task]
impl CpuAdd2 {
    fn run(self, a: &u64, b: &u64) -> u64 {
        a.wrapping_add(*b)
    }
}

struct CpuAdd3;
#[sync_task]
impl CpuAdd3 {
    fn run(self, a: &u64, b: &u64, c: &u64) -> u64 {
        a.wrapping_add(*b).wrapping_add(*c)
    }
}

struct ComplexSource {
    seed: u8,
    len: usize,
}
#[sync_task]
impl ComplexSource {
    fn run(self) -> ComplexPayload {
        make_complex_payload(self.seed, self.len)
    }
}

struct ComplexStep {
    salt: u8,
}
#[sync_task]
impl ComplexStep {
    fn run(self, input: &ComplexPayload) -> ComplexPayload {
        mutate_complex_payload(input, self.salt)
    }
}

struct ComplexMerge2;
#[sync_task]
impl ComplexMerge2 {
    fn run(self, a: &ComplexPayload, b: &ComplexPayload) -> ComplexPayload {
        merge_complex_payload(a, b)
    }
}

struct ComplexScore;
#[sync_task]
impl ComplexScore {
    fn run(self, input: &ComplexPayload) -> u64 {
        complex_payload_score(input)
    }
}

struct CpuSourceBlocking(u32);
#[async_task]
impl CpuSourceBlocking {
    async fn run(self) -> u64 {
        tokio::task::spawn_blocking(move || fib(self.0))
            .await
            .unwrap()
    }
}

struct CpuStepBlocking(u32);
#[async_task]
impl CpuStepBlocking {
    async fn run(self, v: &u64) -> u64 {
        let input = *v;
        tokio::task::spawn_blocking(move || fib(self.0).wrapping_add(input))
            .await
            .unwrap()
    }
}

// ---- IO tasks ----

struct IoSource(u64);
#[async_task]
impl IoSource {
    async fn run(self) -> u64 {
        tokio::time::sleep(Duration::from_millis(self.0)).await;
        self.0
    }
}

struct IoStep(u64);
#[async_task]
impl IoStep {
    async fn run(self, v: &u64) -> u64 {
        tokio::time::sleep(Duration::from_millis(self.0)).await;
        v + 1
    }
}

struct IoAdd2(u64);
#[async_task]
impl IoAdd2 {
    async fn run(self, a: &u64, b: &u64) -> u64 {
        tokio::time::sleep(Duration::from_millis(self.0)).await;
        a + b
    }
}

// ---- Mixed tasks ----

struct MixedSource {
    fib_n: u32,
    sleep_ms: u64,
}
#[async_task]
impl MixedSource {
    async fn run(self) -> u64 {
        let v = fib(self.fib_n);
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        v
    }
}

struct MixedStep {
    fib_n: u32,
    sleep_ms: u64,
}
#[async_task]
impl MixedStep {
    async fn run(self, v: &u64) -> u64 {
        let r = fib(self.fib_n).wrapping_add(*v);
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        r
    }
}

struct MixedAdd2 {
    fib_n: u32,
    sleep_ms: u64,
}
#[async_task]
impl MixedAdd2 {
    async fn run(self, a: &u64, b: &u64) -> u64 {
        let r = fib(self.fib_n).wrapping_add(*a).wrapping_add(*b);
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        r
    }
}

// =========================================================================
// dagx task definitions (equivalent tasks for comparison)
// =========================================================================

mod dx {
    use std::time::Duration;

    use dagx::{task, Task};

    pub struct CpuSource(pub u32);
    #[task]
    impl CpuSource {
        async fn run(&self) -> u64 {
            super::fib(self.0)
        }
    }

    pub struct CpuStep(pub u32);
    #[task]
    impl CpuStep {
        async fn run(&self, v: &u64) -> u64 {
            super::fib(self.0).wrapping_add(*v)
        }
    }

    pub struct CpuSourceBlocking(pub u32);
    #[task]
    impl CpuSourceBlocking {
        async fn run(&self) -> u64 {
            let n = self.0;
            tokio::task::spawn_blocking(move || super::fib(n))
                .await
                .unwrap()
        }
    }

    pub struct CpuStepBlocking(pub u32);
    #[task]
    impl CpuStepBlocking {
        async fn run(&self, v: &u64) -> u64 {
            let n = self.0;
            let input = *v;
            tokio::task::spawn_blocking(move || super::fib(n).wrapping_add(input))
                .await
                .unwrap()
        }
    }

    pub struct CpuAdd2;
    #[task]
    impl CpuAdd2 {
        async fn run(&self, a: &u64, b: &u64) -> u64 {
            a.wrapping_add(*b)
        }
    }

    pub struct CpuAdd3;
    #[task]
    impl CpuAdd3 {
        async fn run(&self, a: &u64, b: &u64, c: &u64) -> u64 {
            a.wrapping_add(*b).wrapping_add(*c)
        }
    }

    pub struct ComplexSource {
        pub seed: u8,
        pub len: usize,
    }
    #[task]
    impl ComplexSource {
        async fn run(&self) -> super::ComplexPayload {
            super::make_complex_payload(self.seed, self.len)
        }
    }

    pub struct ComplexStep {
        pub salt: u8,
    }
    #[task]
    impl ComplexStep {
        async fn run(&self, input: &super::ComplexPayload) -> super::ComplexPayload {
            super::mutate_complex_payload(input, self.salt)
        }
    }

    pub struct ComplexMerge2;
    #[task]
    impl ComplexMerge2 {
        async fn run(&self, a: &super::ComplexPayload, b: &super::ComplexPayload) -> super::ComplexPayload {
            super::merge_complex_payload(a, b)
        }
    }

    pub struct ComplexScore;
    #[task]
    impl ComplexScore {
        async fn run(&self, input: &super::ComplexPayload) -> u64 {
            super::complex_payload_score(input)
        }
    }

    pub struct IoSource(pub u64);
    #[task]
    impl IoSource {
        async fn run(&self) -> u64 {
            tokio::time::sleep(Duration::from_millis(self.0)).await;
            self.0
        }
    }

    pub struct IoStep(pub u64);
    #[task]
    impl IoStep {
        async fn run(&self, v: &u64) -> u64 {
            tokio::time::sleep(Duration::from_millis(self.0)).await;
            v + 1
        }
    }

    pub struct IoAdd2(pub u64);
    #[task]
    impl IoAdd2 {
        async fn run(&self, a: &u64, b: &u64) -> u64 {
            tokio::time::sleep(Duration::from_millis(self.0)).await;
            a + b
        }
    }

    pub struct MixedSource {
        pub fib_n: u32,
        pub sleep_ms: u64,
    }
    #[task]
    impl MixedSource {
        async fn run(&self) -> u64 {
            let v = super::fib(self.fib_n);
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
            v
        }
    }

    pub struct MixedStep {
        pub fib_n: u32,
        pub sleep_ms: u64,
    }
    #[task]
    impl MixedStep {
        async fn run(&self, v: &u64) -> u64 {
            let r = super::fib(self.fib_n).wrapping_add(*v);
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
            r
        }
    }

    pub struct MixedAdd2 {
        pub fib_n: u32,
        pub sleep_ms: u64,
    }
    #[task]
    impl MixedAdd2 {
        async fn run(&self, a: &u64, b: &u64) -> u64 {
            let r = super::fib(self.fib_n).wrapping_add(*a).wrapping_add(*b);
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
            r
        }
    }
}

// =========================================================================
// Constants — tune for your machine
// =========================================================================

/// Fibonacci input for CPU tasks. fib(32) ~ 5 ms release, ~100 ms debug.
const FIB_N: u32 = 32;
/// Sleep duration (ms) for IO tasks.
const SLEEP_MS: u64 = 10;
/// Warmup rounds per scenario (discarded from statistics).
const BENCH_WARMUP: usize = 5;
/// Number of measured rounds per scenario.
const BENCH_REPEAT: usize = 60;

/// Payload size for complex input/output benchmark cases.
const COMPLEX_PAYLOAD_BYTES: usize = 512 * 1024;

fn print_cpu_stability_hint() {
    println!("  note: for lower jitter, lock CPU frequency/governor and pin process cores before running bench");
}

// =========================================================================
// CPU-intensive benchmarks
// =========================================================================

/// Linear chain: Source → Step → Step → ... → Step (all CPU-bound).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_cpu_chain() {
    header(&format!("CPU: Linear Chain  (fib({FIB_N}) per task)"));
    print_cpu_stability_hint();

    for n in [5usize, 10, 20] {
        let ((tf_phase, tf_val), (dx_phase, dx_val), (bl_phase, bl_val)) = bench_three_split_with_value(
            BENCH_WARMUP,
            BENCH_REPEAT,
            || async {
                let build_t0 = Instant::now();
                let mut flow = Flow::new();
                let mut prev = flow.commit_source_task("src", CpuSource(FIB_N));
                for i in 0..n {
                    prev = flow
                        .commit_task(format!("s{i}"), CpuStep(FIB_N))
                        .with_dependencies(prev);
                }
                let build_ms = build_t0.elapsed().as_secs_f64() * 1000.0;

                let exec_t0 = Instant::now();
                let out = flow.run(prev).await.unwrap();
                let exec_ms = exec_t0.elapsed().as_secs_f64() * 1000.0;
                ((build_ms, exec_ms), out)
            },
            || async {
                let build_t0 = Instant::now();
                let dag = DagRunner::new();
                let mut dprev: TaskHandle<u64> = (&dag.add_task(dx::CpuSource(FIB_N))).into();
                for _ in 0..n {
                    dprev = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&dprev);
                }
                let build_ms = build_t0.elapsed().as_secs_f64() * 1000.0;

                let exec_t0 = Instant::now();
                dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
                let out = dag.get(dprev).unwrap();
                let exec_ms = exec_t0.elapsed().as_secs_f64() * 1000.0;
                ((build_ms, exec_ms), out)
            },
            || async {
                let build_t0 = Instant::now();
                let build_ms = build_t0.elapsed().as_secs_f64() * 1000.0;

                let exec_t0 = Instant::now();
                let mut val = fib(FIB_N);
                for _ in 0..n {
                    let v = val;
                    val = tokio::spawn(async move { fib(FIB_N).wrapping_add(v) })
                        .await
                        .unwrap();
                }
                let exec_ms = exec_t0.elapsed().as_secs_f64() * 1000.0;
                ((build_ms, exec_ms), val)
            },
        )
        .await;

        assert_eq!(tf_val, bl_val);
        assert_eq!(dx_val, bl_val);
        row3(
            &format!("chain len={n}"),
            tf_phase.total,
            dx_phase.total,
            bl_phase.total,
        );
        row3_phase(
            &format!("chain len={n} (build/exec split)"),
            &tf_phase,
            &dx_phase,
            &bl_phase,
        );
    }
}

/// Fan-out from single source → 6 parallel CPU tasks → tree reduction.
///
/// ```text
///           S
///    / / |  |  \ \
///   P1 P2 P3 P4 P5 P6
///    \ /   \ /   \ /
///    A1    A2    A3
///       \  |  /
///       Final
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_cpu_fan_out() {
    header(&format!("CPU: Fan-out (1→6) + Tree Reduce  (fib({FIB_N}))"));
    print_cpu_stability_hint();

    let ((tf_phase, tf_val), (dx_phase, dx_val), (bl_phase, bl_val)) = bench_three_split_with_value(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let build_t0 = Instant::now();
            let mut flow = Flow::new();
            let s = flow.commit_source_task("src", CpuSource(FIB_N));
            let p1 = flow.commit_task("p1", CpuStep(FIB_N)).with_dependencies(dup(&s));
            let p2 = flow.commit_task("p2", CpuStep(FIB_N)).with_dependencies(dup(&s));
            let p3 = flow.commit_task("p3", CpuStep(FIB_N)).with_dependencies(dup(&s));
            let p4 = flow.commit_task("p4", CpuStep(FIB_N)).with_dependencies(dup(&s));
            let p5 = flow.commit_task("p5", CpuStep(FIB_N)).with_dependencies(dup(&s));
            let p6 = flow.commit_task("p6", CpuStep(FIB_N)).with_dependencies(s);
            let a1 = flow.commit_task("a1", CpuAdd2).with_dependencies((p1, p2));
            let a2 = flow.commit_task("a2", CpuAdd2).with_dependencies((p3, p4));
            let a3 = flow.commit_task("a3", CpuAdd2).with_dependencies((p5, p6));
            let fin = flow.commit_task("fin", CpuAdd3).with_dependencies((a1, a2, a3));
            let build_ms = build_t0.elapsed().as_secs_f64() * 1000.0;

            let exec_t0 = Instant::now();
            let out = flow.run(fin).await.unwrap();
            let exec_ms = exec_t0.elapsed().as_secs_f64() * 1000.0;
            ((build_ms, exec_ms), out)
        },
        || async {
            let build_t0 = Instant::now();
            let dag = DagRunner::new();
            let s = dag.add_task(dx::CpuSource(FIB_N));
            let p1 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s);
            let p2 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s);
            let p3 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s);
            let p4 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s);
            let p5 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s);
            let p6 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s);
            let a1 = dag.add_task(dx::CpuAdd2).depends_on((&p1, &p2));
            let a2 = dag.add_task(dx::CpuAdd2).depends_on((&p3, &p4));
            let a3 = dag.add_task(dx::CpuAdd2).depends_on((&p5, &p6));
            let fin = dag.add_task(dx::CpuAdd3).depends_on((&a1, &a2, &a3));
            let build_ms = build_t0.elapsed().as_secs_f64() * 1000.0;

            let exec_t0 = Instant::now();
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            let out = dag.get(fin).unwrap();
            let exec_ms = exec_t0.elapsed().as_secs_f64() * 1000.0;
            ((build_ms, exec_ms), out)
        },
        || async {
            let build_t0 = Instant::now();
            let build_ms = build_t0.elapsed().as_secs_f64() * 1000.0;

            let exec_t0 = Instant::now();
            let sv = fib(FIB_N);
            let handles: Vec<_> = (0..6)
                .map(|_| {
                    let v = sv;
                    tokio::spawn(async move { fib(FIB_N).wrapping_add(v) })
                })
                .collect();
            let vals: Vec<u64> = futures::future::join_all(handles)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let a1 = vals[0].wrapping_add(vals[1]);
            let a2 = vals[2].wrapping_add(vals[3]);
            let a3 = vals[4].wrapping_add(vals[5]);
            let out = a1.wrapping_add(a2).wrapping_add(a3);
            let exec_ms = exec_t0.elapsed().as_secs_f64() * 1000.0;
            ((build_ms, exec_ms), out)
        },
    )
    .await;

    assert_eq!(tf_val, bl_val);
    assert_eq!(dx_val, bl_val);
    row3(
        "fan-out=6, tree-reduce",
        tf_phase.total,
        dx_phase.total,
        bl_phase.total,
    );
    row3_phase(
        "fan-out=6, tree-reduce (build/exec split)",
        &tf_phase,
        &dx_phase,
        &bl_phase,
    );
}

/// Diamond pattern: two independent paths from two sources converge.
///
/// ```text
///   S1    S2
///    |    |
///   C1   C2
///    \  /
///    Merge
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_cpu_diamond() {
    header(&format!("CPU: Diamond  (fib({FIB_N}))"));

    let ((tf_ms, tf_val), (dx_ms, dx_val), (bl_ms, bl_val)) = bench_three_with_value(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();
            let s1 = flow.commit_source_task("s1", CpuSource(FIB_N));
            let s2 = flow.commit_source_task("s2", CpuSource(FIB_N));
            let c1 = flow.commit_task("c1", CpuStep(FIB_N)).with_dependencies(s1);
            let c2 = flow.commit_task("c2", CpuStep(FIB_N)).with_dependencies(s2);
            let merge = flow.commit_task("merge", CpuAdd2).with_dependencies((c1, c2));
            flow.run(merge).await.unwrap()
        },
        || async {
            let dag = DagRunner::new();
            let s1 = dag.add_task(dx::CpuSource(FIB_N));
            let s2 = dag.add_task(dx::CpuSource(FIB_N));
            let c1 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s1);
            let c2 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s2);
            let merge = dag.add_task(dx::CpuAdd2).depends_on((&c1, &c2));
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            dag.get(merge).unwrap()
        },
        || async {
            let (v1, v2) = tokio::join!(
                tokio::spawn(async { fib(FIB_N) }),
                tokio::spawn(async { fib(FIB_N) }),
            );
            let c1 = tokio::spawn(async move { fib(FIB_N).wrapping_add(v1.unwrap()) });
            let c2 = tokio::spawn(async move { fib(FIB_N).wrapping_add(v2.unwrap()) });
            let (r1, r2) = tokio::join!(c1, c2);
            r1.unwrap().wrapping_add(r2.unwrap())
        },
    )
    .await;

    assert_eq!(tf_val, bl_val);
    assert_eq!(dx_val, bl_val);
    row3("diamond (2 paths)", tf_ms, dx_ms, bl_ms);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_cpu_chain_spawn_blocking() {
    header(&format!("CPU: Linear Chain via spawn_blocking (fib({FIB_N}))"));
    print_cpu_stability_hint();

    for n in [5usize, 10, 20] {
        let ((tf_ms, tf_val), (dx_ms, dx_val), (bl_ms, bl_val)) = bench_three_with_value(
            BENCH_WARMUP,
            BENCH_REPEAT,
            || async {
                let mut flow = Flow::new();
                let mut prev = flow.commit_source_task("src", CpuSourceBlocking(FIB_N));
                for i in 0..n {
                    prev = flow
                        .commit_task(format!("s{i}"), CpuStepBlocking(FIB_N))
                        .with_dependencies(prev);
                }
                flow.run(prev).await.unwrap()
            },
            || async {
                let dag = DagRunner::new();
                let mut dprev: TaskHandle<u64> = (&dag.add_task(dx::CpuSourceBlocking(FIB_N))).into();
                for _ in 0..n {
                    dprev = dag.add_task(dx::CpuStepBlocking(FIB_N)).depends_on(&dprev);
                }
                dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
                dag.get(dprev).unwrap()
            },
            || async {
                let mut val = tokio::task::spawn_blocking(move || fib(FIB_N)).await.unwrap();
                for _ in 0..n {
                    let v = val;
                    val = tokio::task::spawn_blocking(move || fib(FIB_N).wrapping_add(v))
                        .await
                        .unwrap();
                }
                val
            },
        )
        .await;

        assert_eq!(tf_val, bl_val);
        assert_eq!(dx_val, bl_val);
        row3(&format!("chain len={n}"), tf_ms, dx_ms, bl_ms);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_cpu_fan_out_spawn_blocking() {
    header(&format!("CPU: Fan-out via spawn_blocking (fib({FIB_N}))"));
    print_cpu_stability_hint();

    let ((tf_ms, tf_val), (dx_ms, dx_val), (bl_ms, bl_val)) = bench_three_with_value(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();
            let s = flow.commit_source_task("src", CpuSourceBlocking(FIB_N));
            let p1 = flow.commit_task("p1", CpuStepBlocking(FIB_N)).with_dependencies(dup(&s));
            let p2 = flow.commit_task("p2", CpuStepBlocking(FIB_N)).with_dependencies(dup(&s));
            let p3 = flow.commit_task("p3", CpuStepBlocking(FIB_N)).with_dependencies(dup(&s));
            let p4 = flow.commit_task("p4", CpuStepBlocking(FIB_N)).with_dependencies(dup(&s));
            let p5 = flow.commit_task("p5", CpuStepBlocking(FIB_N)).with_dependencies(dup(&s));
            let p6 = flow.commit_task("p6", CpuStepBlocking(FIB_N)).with_dependencies(s);
            let a1 = flow.commit_task("a1", CpuAdd2).with_dependencies((p1, p2));
            let a2 = flow.commit_task("a2", CpuAdd2).with_dependencies((p3, p4));
            let a3 = flow.commit_task("a3", CpuAdd2).with_dependencies((p5, p6));
            let fin = flow.commit_task("fin", CpuAdd3).with_dependencies((a1, a2, a3));
            flow.run(fin).await.unwrap()
        },
        || async {
            let dag = DagRunner::new();
            let s = dag.add_task(dx::CpuSourceBlocking(FIB_N));
            let p1 = dag.add_task(dx::CpuStepBlocking(FIB_N)).depends_on(&s);
            let p2 = dag.add_task(dx::CpuStepBlocking(FIB_N)).depends_on(&s);
            let p3 = dag.add_task(dx::CpuStepBlocking(FIB_N)).depends_on(&s);
            let p4 = dag.add_task(dx::CpuStepBlocking(FIB_N)).depends_on(&s);
            let p5 = dag.add_task(dx::CpuStepBlocking(FIB_N)).depends_on(&s);
            let p6 = dag.add_task(dx::CpuStepBlocking(FIB_N)).depends_on(&s);
            let a1 = dag.add_task(dx::CpuAdd2).depends_on((&p1, &p2));
            let a2 = dag.add_task(dx::CpuAdd2).depends_on((&p3, &p4));
            let a3 = dag.add_task(dx::CpuAdd2).depends_on((&p5, &p6));
            let fin = dag.add_task(dx::CpuAdd3).depends_on((&a1, &a2, &a3));
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            dag.get(fin).unwrap()
        },
        || async {
            let sv = tokio::task::spawn_blocking(move || fib(FIB_N)).await.unwrap();
            let handles: Vec<_> = (0..6)
                .map(|_| {
                    let v = sv;
                    tokio::task::spawn_blocking(move || fib(FIB_N).wrapping_add(v))
                })
                .collect();
            let vals: Vec<u64> = futures::future::join_all(handles)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let a1 = vals[0].wrapping_add(vals[1]);
            let a2 = vals[2].wrapping_add(vals[3]);
            let a3 = vals[4].wrapping_add(vals[5]);
            a1.wrapping_add(a2).wrapping_add(a3)
        },
    )
    .await;

    assert_eq!(tf_val, bl_val);
    assert_eq!(dx_val, bl_val);
    row3("fan-out=6, tree-reduce", tf_ms, dx_ms, bl_ms);
}

// =========================================================================
// IO-intensive benchmarks
// =========================================================================

/// Linear chain of IO tasks (sequential sleeps).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_io_chain() {
    header(&format!("IO: Linear Chain  (sleep {SLEEP_MS}ms per task)"));

    for n in [5usize, 10, 20] {
        let (tf_ms, dx_ms, bl_ms) = bench_three(
            BENCH_WARMUP,
            BENCH_REPEAT,
            || async {
                let mut flow = Flow::new();
                let mut prev = flow.commit_source_task("src", IoSource(SLEEP_MS));
                for i in 0..n {
                    prev = flow
                        .commit_task(format!("io{i}"), IoStep(SLEEP_MS))
                        .with_dependencies(prev);
                }
                let _tf_val = flow.run(prev).await.unwrap();
            },
            || async {
                let dag = DagRunner::new();
                let mut dprev: TaskHandle<u64> = (&dag.add_task(dx::IoSource(SLEEP_MS))).into();
                for _ in 0..n {
                    dprev = dag.add_task(dx::IoStep(SLEEP_MS)).depends_on(&dprev);
                }
                dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
                let _dx_val: u64 = dag.get(dprev).unwrap();
            },
            || async {
                let mut val = {
                    tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                    SLEEP_MS
                };
                for _ in 0..n {
                    let v = val;
                    val = tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                        v + 1
                    })
                    .await
                    .unwrap();
                }
            },
        )
        .await;

        row3(&format!("chain len={n}"), tf_ms, dx_ms, bl_ms);
    }
}

/// 6 independent IO sources (parallel sleeps) → tree reduction.
/// Demonstrates that parallel IO tasks complete in ~1x sleep time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_io_parallel() {
    header(&format!(
        "IO: Parallel Sources (6x sleep {SLEEP_MS}ms) + Reduce"
    ));

    let (tf_ms, dx_ms, bl_ms) = bench_three(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();
            let s1 = flow.commit_source_task("s1", IoSource(SLEEP_MS));
            let s2 = flow.commit_source_task("s2", IoSource(SLEEP_MS));
            let s3 = flow.commit_source_task("s3", IoSource(SLEEP_MS));
            let s4 = flow.commit_source_task("s4", IoSource(SLEEP_MS));
            let s5 = flow.commit_source_task("s5", IoSource(SLEEP_MS));
            let s6 = flow.commit_source_task("s6", IoSource(SLEEP_MS));
            let a1 = flow.commit_task("a1", IoAdd2(SLEEP_MS)).with_dependencies((s1, s2));
            let a2 = flow.commit_task("a2", IoAdd2(SLEEP_MS)).with_dependencies((s3, s4));
            let a3 = flow.commit_task("a3", IoAdd2(SLEEP_MS)).with_dependencies((s5, s6));
            let fin = flow.commit_task("fin", CpuAdd3).with_dependencies((a1, a2, a3));
            let _tf_val = flow.run(fin).await.unwrap();
        },
        || async {
            let dag = DagRunner::new();
            let s1 = dag.add_task(dx::IoSource(SLEEP_MS));
            let s2 = dag.add_task(dx::IoSource(SLEEP_MS));
            let s3 = dag.add_task(dx::IoSource(SLEEP_MS));
            let s4 = dag.add_task(dx::IoSource(SLEEP_MS));
            let s5 = dag.add_task(dx::IoSource(SLEEP_MS));
            let s6 = dag.add_task(dx::IoSource(SLEEP_MS));
            let m1 = dag.add_task(dx::IoAdd2(SLEEP_MS)).depends_on((&s1, &s2));
            let m2 = dag.add_task(dx::IoAdd2(SLEEP_MS)).depends_on((&s3, &s4));
            let m3 = dag.add_task(dx::IoAdd2(SLEEP_MS)).depends_on((&s5, &s6));
            let fin = dag.add_task(dx::CpuAdd3).depends_on((&m1, &m2, &m3));
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            let _dx_val: u64 = dag.get(fin).unwrap();
        },
        || async {
            let handles: Vec<_> = (0..6)
                .map(|_| {
                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                        SLEEP_MS
                    })
                })
                .collect();
            let vals: Vec<u64> = futures::future::join_all(handles)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let a1 = vals[0] + vals[1];
            let a2 = vals[2] + vals[3];
            let a3 = vals[4] + vals[5];
            let _bl_val = a1 + a2 + a3;
        },
    )
    .await;

    row3("6 parallel + reduce", tf_ms, dx_ms, bl_ms);
    let expected_min = SLEEP_MS as f64;
    let expected_seq = (SLEEP_MS * 6) as f64;
    println!(
        "  (sequential would be ~{expected_seq}ms, parallel ideal ~{expected_min}ms)"
    );
}

// =========================================================================
// Mixed benchmarks
// =========================================================================

/// Two pipelines — one CPU-heavy, one IO-heavy — merge at the end.
///
/// ```text
///   CpuSrc → CpuStep → CpuStep ─┐
///                                 ├→ MixedMerge → Result
///   IoSrc  → IoStep  → IoStep  ─┘
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_mixed_two_pipelines() {
    header("Mixed: Two Pipelines (CPU + IO) → Merge");

    let (tf_ms, dx_ms, bl_ms) = bench_three(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();
            let cs = flow.commit_source_task("cs", CpuSource(FIB_N));
            let c1 = flow.commit_task("c1", CpuStep(FIB_N)).with_dependencies(cs);
            let c2 = flow.commit_task("c2", CpuStep(FIB_N)).with_dependencies(c1);
            let is = flow.commit_source_task("is", IoSource(SLEEP_MS));
            let i1 = flow.commit_task("i1", IoStep(SLEEP_MS)).with_dependencies(is);
            let i2 = flow.commit_task("i2", IoStep(SLEEP_MS)).with_dependencies(i1);
            let fin = flow
                .commit_task("fin", MixedAdd2 { fib_n: 20, sleep_ms: 1 })
                .with_dependencies((c2, i2));
            let _tf_val = flow.run(fin).await.unwrap();
        },
        || async {
            let dag = DagRunner::new();
            let cs = dag.add_task(dx::CpuSource(FIB_N));
            let c1 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&cs);
            let c2 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&c1);
            let is = dag.add_task(dx::IoSource(SLEEP_MS));
            let i1 = dag.add_task(dx::IoStep(SLEEP_MS)).depends_on(&is);
            let i2 = dag.add_task(dx::IoStep(SLEEP_MS)).depends_on(&i1);
            let fin = dag.add_task(dx::MixedAdd2 { fib_n: 20, sleep_ms: 1 }).depends_on((&c2, &i2));
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            let _dx_val: u64 = dag.get(fin).unwrap();
        },
        || async {
            let cpu_handle = tokio::spawn(async {
                let mut v = fib(FIB_N);
                v = fib(FIB_N).wrapping_add(v);
                v = fib(FIB_N).wrapping_add(v);
                v
            });
            let io_handle = tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                let mut v = SLEEP_MS;
                tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                v += 1;
                tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                v += 1;
                v
            });
            let (cpu_r, io_r) = tokio::join!(cpu_handle, io_handle);
            let _bl_val = fib(20)
                .wrapping_add(cpu_r.unwrap())
                .wrapping_add(io_r.unwrap());
        },
    )
    .await;

    row3("2 pipelines + merge", tf_ms, dx_ms, bl_ms);
}

/// Alternating CPU and IO steps in a single chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_mixed_alternating_chain() {
    header("Mixed: Alternating CPU/IO Chain");

    for n in [4usize, 8] {
        let (tf_ms, dx_ms, bl_ms) = bench_three(
            BENCH_WARMUP,
            BENCH_REPEAT,
            || async {
                let mut flow = Flow::new();
                let mut prev =
                    flow.commit_source_task("src", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
                for i in 0..n {
                    prev = flow
                        .commit_task(
                            format!("m{i}"),
                            MixedStep { fib_n: FIB_N, sleep_ms: SLEEP_MS },
                        )
                        .with_dependencies(prev);
                }
                let _tf_val = flow.run(prev).await.unwrap();
            },
            || async {
                let dag = DagRunner::new();
                let mut dprev: TaskHandle<u64> = (&dag.add_task(dx::MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS })).into();
                for _ in 0..n {
                    dprev = dag.add_task(dx::MixedStep { fib_n: FIB_N, sleep_ms: SLEEP_MS }).depends_on(&dprev);
                }
                dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
                let _dx_val: u64 = dag.get(dprev).unwrap();
            },
            || async {
                let mut val = fib(FIB_N);
                tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                for _ in 0..n {
                    let v = val;
                    val = tokio::spawn(async move {
                        let r = fib(FIB_N).wrapping_add(v);
                        tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                        r
                    })
                    .await
                    .unwrap();
                }
            },
        )
        .await;

        row3(&format!("alternating len={n}"), tf_ms, dx_ms, bl_ms);
    }
}

/// Complex DAG: 6 sources → 6 processors → 3 pair-merge → final reduce.
///
/// ```text
///   S1 S2 S3 S4 S5 S6       (layer 0: 6 mixed sources)
///   |  |  |  |  |  |
///   P1 P2 P3 P4 P5 P6       (layer 1: 6 CPU steps)
///    \ /   \ /   \ /
///    M1    M2    M3          (layer 2: 3 IO merges)
///       \  |  /
///       Final                (layer 3: CPU reduce)
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_mixed_complex_dag() {
    header("Mixed: Complex 6-Source DAG (CPU+IO)");

    let (tf_ms, dx_ms, bl_ms) = bench_three(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();

            let s1 = flow.commit_source_task("s1", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s2 = flow.commit_source_task("s2", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s3 = flow.commit_source_task("s3", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s4 = flow.commit_source_task("s4", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s5 = flow.commit_source_task("s5", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s6 = flow.commit_source_task("s6", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });

            let p1 = flow.commit_task("p1", CpuStep(FIB_N)).with_dependencies(s1);
            let p2 = flow.commit_task("p2", CpuStep(FIB_N)).with_dependencies(s2);
            let p3 = flow.commit_task("p3", CpuStep(FIB_N)).with_dependencies(s3);
            let p4 = flow.commit_task("p4", CpuStep(FIB_N)).with_dependencies(s4);
            let p5 = flow.commit_task("p5", CpuStep(FIB_N)).with_dependencies(s5);
            let p6 = flow.commit_task("p6", CpuStep(FIB_N)).with_dependencies(s6);

            let m1 = flow.commit_task("m1", IoAdd2(SLEEP_MS)).with_dependencies((p1, p2));
            let m2 = flow.commit_task("m2", IoAdd2(SLEEP_MS)).with_dependencies((p3, p4));
            let m3 = flow.commit_task("m3", IoAdd2(SLEEP_MS)).with_dependencies((p5, p6));

            let fin = flow.commit_task("fin", CpuAdd3).with_dependencies((m1, m2, m3));
            let _tf_val = flow.run(fin).await.unwrap();
        },
        || async {
            let dag = DagRunner::new();
            let s1 = dag.add_task(dx::MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s2 = dag.add_task(dx::MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s3 = dag.add_task(dx::MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s4 = dag.add_task(dx::MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s5 = dag.add_task(dx::MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s6 = dag.add_task(dx::MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let p1 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s1);
            let p2 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s2);
            let p3 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s3);
            let p4 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s4);
            let p5 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s5);
            let p6 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s6);
            let m1 = dag.add_task(dx::IoAdd2(SLEEP_MS)).depends_on((&p1, &p2));
            let m2 = dag.add_task(dx::IoAdd2(SLEEP_MS)).depends_on((&p3, &p4));
            let m3 = dag.add_task(dx::IoAdd2(SLEEP_MS)).depends_on((&p5, &p6));
            let fin = dag.add_task(dx::CpuAdd3).depends_on((&m1, &m2, &m3));
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            let _dx_val: u64 = dag.get(fin).unwrap();
        },
        || async {
            let src_handles: Vec<_> = (0..6)
                .map(|_| {
                    tokio::spawn(async {
                        let v = fib(FIB_N);
                        tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                        v
                    })
                })
                .collect();
            let src_vals: Vec<u64> = futures::future::join_all(src_handles)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let cpu_handles: Vec<_> = src_vals
                .into_iter()
                .map(|v| tokio::spawn(async move { fib(FIB_N).wrapping_add(v) }))
                .collect();
            let cpu_vals: Vec<u64> = futures::future::join_all(cpu_handles)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let merge_handles: Vec<_> = cpu_vals
                .chunks(2)
                .map(|pair| {
                    let (a, b) = (pair[0], pair[1]);
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                        a + b
                    })
                })
                .collect();
            let merge_vals: Vec<u64> = futures::future::join_all(merge_handles)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let _bl_val = merge_vals[0]
                .wrapping_add(merge_vals[1])
                .wrapping_add(merge_vals[2]);
        },
    )
    .await;

    row3("6-source 4-layer DAG", tf_ms, dx_ms, bl_ms);
}

// =========================================================================
// Complex payload benchmarks (large input/output objects)
// =========================================================================

/// Complex payload chain: source blob -> N transform steps -> checksum score.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_complex_payload_chain() {
    header(&format!(
        "Complex Payload: Linear Chain (blob={} bytes)",
        COMPLEX_PAYLOAD_BYTES
    ));

    for n in [3usize, 6, 10] {
        let ((tf_ms, tf_val), (dx_ms, dx_val), (bl_ms, bl_val)) = bench_three_with_value(
            BENCH_WARMUP,
            BENCH_REPEAT,
            || async {
                let mut flow = Flow::new();
                let mut prev = flow.commit_source_task(
                    "blob_src",
                    ComplexSource {
                        seed: 7,
                        len: COMPLEX_PAYLOAD_BYTES,
                    },
                );
                for i in 0..n {
                    prev = flow
                        .commit_task(
                            format!("blob_step_{i}"),
                            ComplexStep {
                                salt: (i as u8).wrapping_add(13),
                            },
                        )
                        .with_dependencies(prev);
                }
                let score = flow.commit_task("blob_score", ComplexScore).with_dependencies(prev);
                flow.run(score).await.unwrap()
            },
            || async {
                let dag = DagRunner::new();
                let mut prev: TaskHandle<ComplexPayload> = (&dag.add_task(dx::ComplexSource {
                    seed: 7,
                    len: COMPLEX_PAYLOAD_BYTES,
                }))
                    .into();
                for i in 0..n {
                    prev = dag
                        .add_task(dx::ComplexStep {
                            salt: (i as u8).wrapping_add(13),
                        })
                        .depends_on(&prev);
                }
                let score = dag.add_task(dx::ComplexScore).depends_on(&prev);
                dag.run(|fut| {
                    tokio::spawn(fut);
                })
                .await
                .unwrap();
                dag.get(score).unwrap()
            },
            || async {
                let mut payload = make_complex_payload(7, COMPLEX_PAYLOAD_BYTES);
                for i in 0..n {
                    payload = mutate_complex_payload(&payload, (i as u8).wrapping_add(13));
                }
                complex_payload_score(&payload)
            },
        )
        .await;

        assert_eq!(tf_val, bl_val);
        assert_eq!(dx_val, bl_val);
        row3(&format!("blob chain len={n}"), tf_ms, dx_ms, bl_ms);
    }
}

/// Complex payload fan-out: one large blob fan-outs to 6 branches, then merges to final score.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_complex_payload_fan_out() {
    header(&format!(
        "Complex Payload: Fan-out + Merge (blob={} bytes)",
        COMPLEX_PAYLOAD_BYTES
    ));

    let ((tf_ms, tf_val), (dx_ms, dx_val), (bl_ms, bl_val)) = bench_three_with_value(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();
            let s = flow.commit_source_task(
                "blob_src",
                ComplexSource {
                    seed: 11,
                    len: COMPLEX_PAYLOAD_BYTES,
                },
            );

            let p1 = flow
                .commit_task("p1", ComplexStep { salt: 1 })
                .with_dependencies(dup(&s));
            let p2 = flow
                .commit_task("p2", ComplexStep { salt: 2 })
                .with_dependencies(dup(&s));
            let p3 = flow
                .commit_task("p3", ComplexStep { salt: 3 })
                .with_dependencies(dup(&s));
            let p4 = flow
                .commit_task("p4", ComplexStep { salt: 4 })
                .with_dependencies(dup(&s));
            let p5 = flow
                .commit_task("p5", ComplexStep { salt: 5 })
                .with_dependencies(dup(&s));
            let p6 = flow
                .commit_task("p6", ComplexStep { salt: 6 })
                .with_dependencies(s);

            let m1 = flow.commit_task("m1", ComplexMerge2).with_dependencies((p1, p2));
            let m2 = flow.commit_task("m2", ComplexMerge2).with_dependencies((p3, p4));
            let m3 = flow.commit_task("m3", ComplexMerge2).with_dependencies((p5, p6));
            let m12 = flow.commit_task("m12", ComplexMerge2).with_dependencies((m1, m2));
            let fin_payload = flow
                .commit_task("mfin", ComplexMerge2)
                .with_dependencies((m12, m3));
            let score = flow
                .commit_task("score", ComplexScore)
                .with_dependencies(fin_payload);
            flow.run(score).await.unwrap()
        },
        || async {
            let dag = DagRunner::new();
            let s = dag.add_task(dx::ComplexSource {
                seed: 11,
                len: COMPLEX_PAYLOAD_BYTES,
            });
            let p1 = dag.add_task(dx::ComplexStep { salt: 1 }).depends_on(&s);
            let p2 = dag.add_task(dx::ComplexStep { salt: 2 }).depends_on(&s);
            let p3 = dag.add_task(dx::ComplexStep { salt: 3 }).depends_on(&s);
            let p4 = dag.add_task(dx::ComplexStep { salt: 4 }).depends_on(&s);
            let p5 = dag.add_task(dx::ComplexStep { salt: 5 }).depends_on(&s);
            let p6 = dag.add_task(dx::ComplexStep { salt: 6 }).depends_on(&s);

            let m1 = dag.add_task(dx::ComplexMerge2).depends_on((&p1, &p2));
            let m2 = dag.add_task(dx::ComplexMerge2).depends_on((&p3, &p4));
            let m3 = dag.add_task(dx::ComplexMerge2).depends_on((&p5, &p6));
            let m12 = dag.add_task(dx::ComplexMerge2).depends_on((&m1, &m2));
            let fin_payload = dag.add_task(dx::ComplexMerge2).depends_on((&m12, &m3));
            let score = dag.add_task(dx::ComplexScore).depends_on(&fin_payload);

            dag.run(|fut| {
                tokio::spawn(fut);
            })
            .await
            .unwrap();
            dag.get(score).unwrap()
        },
        || async {
            let src = std::sync::Arc::new(make_complex_payload(11, COMPLEX_PAYLOAD_BYTES));
            let handles: Vec<_> = (1u8..=6)
                .map(|salt| {
                    let src = src.clone();
                    tokio::spawn(async move { mutate_complex_payload(src.as_ref(), salt) })
                })
                .collect();

            let vals: Vec<_> = futures::future::join_all(handles)
                .await

                .into_iter()
                .map(|r| r.unwrap())
                .collect();

            let m1 = merge_complex_payload(&vals[0], &vals[1]);
            let m2 = merge_complex_payload(&vals[2], &vals[3]);
            let m3 = merge_complex_payload(&vals[4], &vals[5]);
            let m12 = merge_complex_payload(&m1, &m2);
            let fin = merge_complex_payload(&m12, &m3);
            complex_payload_score(&fin)
        },
    )
    .await;

    assert_eq!(tf_val, bl_val);
    assert_eq!(dx_val, bl_val);
    row3("blob fan-out=6 + merge", tf_ms, dx_ms, bl_ms);
}

// =========================================================================
// Stress tests (large-scale, run with --ignored)
// =========================================================================

/// Deep chain: 100 CPU tasks in sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_stress_deep_cpu_chain() {
    header(&format!("STRESS: Deep CPU Chain (100 tasks, fib({FIB_N}))"));

    let (tf_ms, bl_ms) = bench_two(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();
            let mut prev = flow.commit_source_task("src", CpuSource(FIB_N));
            for i in 0..99 {
                prev = flow
                    .commit_task(format!("s{i}"), CpuStep(FIB_N))
                    .with_dependencies(prev);
            }
            let _tf_val = flow.run(prev).await.unwrap();
        },
        || async {
            let mut val = fib(FIB_N);
            for _ in 0..99 {
                let v = val;
                val = tokio::spawn(async move { fib(FIB_N).wrapping_add(v) })
                    .await
                    .unwrap();
            }
        },
    )
    .await;

    row("chain len=100", tf_ms, bl_ms);
}

/// Deep chain: 100 IO tasks in sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_stress_deep_io_chain() {
    header(&format!("STRESS: Deep IO Chain (100 tasks, sleep {SLEEP_MS}ms)"));

    let (tf_ms, bl_ms) = bench_two(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();
            let mut prev = flow.commit_source_task("src", IoSource(SLEEP_MS));
            for i in 0..99 {
                prev = flow
                    .commit_task(format!("io{i}"), IoStep(SLEEP_MS))
                    .with_dependencies(prev);
            }
            let _tf_val = flow.run(prev).await.unwrap();
        },
        || async {
            let mut val = SLEEP_MS;
            tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
            for _ in 0..99 {
                let v = val;
                val = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                    v + 1
                })
                .await
                .unwrap();
            }
        },
    )
    .await;

    row("chain len=100", tf_ms, bl_ms);
}

/// Wide DAG: 6 mixed sources → 6 CPU steps → 3 IO merges →
/// 3 CPU steps → 3-way reduce. 22 tasks total.
///
/// ```text
///   S1 S2 S3 S4 S5 S6       (layer 0: mixed sources)
///   |  |  |  |  |  |
///   P1 P2 P3 P4 P5 P6       (layer 1: CPU steps)
///    \ /   \ /   \ /
///    M1    M2    M3          (layer 2: IO merges)
///    |     |     |
///    R1    R2    R3          (layer 3: CPU steps)
///       \  |  /
///       Final                (layer 4: reduce)
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_stress_wide_dag() {
    header("STRESS: Wide 5-Layer DAG (22 tasks, CPU+IO)");

    let (tf_ms, bl_ms) = bench_two(
        BENCH_WARMUP,
        BENCH_REPEAT,
        || async {
            let mut flow = Flow::new();

            let s1 = flow.commit_source_task("s1", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s2 = flow.commit_source_task("s2", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s3 = flow.commit_source_task("s3", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s4 = flow.commit_source_task("s4", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s5 = flow.commit_source_task("s5", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });
            let s6 = flow.commit_source_task("s6", MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS });

            let p1 = flow.commit_task("p1", CpuStep(FIB_N)).with_dependencies(s1);
            let p2 = flow.commit_task("p2", CpuStep(FIB_N)).with_dependencies(s2);
            let p3 = flow.commit_task("p3", CpuStep(FIB_N)).with_dependencies(s3);
            let p4 = flow.commit_task("p4", CpuStep(FIB_N)).with_dependencies(s4);
            let p5 = flow.commit_task("p5", CpuStep(FIB_N)).with_dependencies(s5);
            let p6 = flow.commit_task("p6", CpuStep(FIB_N)).with_dependencies(s6);

            let m1 = flow.commit_task("m1", IoAdd2(SLEEP_MS)).with_dependencies((p1, p2));
            let m2 = flow.commit_task("m2", IoAdd2(SLEEP_MS)).with_dependencies((p3, p4));
            let m3 = flow.commit_task("m3", IoAdd2(SLEEP_MS)).with_dependencies((p5, p6));

            let r1 = flow.commit_task("r1", CpuStep(FIB_N)).with_dependencies(m1);
            let r2 = flow.commit_task("r2", CpuStep(FIB_N)).with_dependencies(m2);
            let r3 = flow.commit_task("r3", CpuStep(FIB_N)).with_dependencies(m3);

            let fin = flow.commit_task("fin", CpuAdd3).with_dependencies((r1, r2, r3));
            let _tf_val = flow.run(fin).await.unwrap();
        },
        || async {
            let src_h: Vec<_> = (0..6)
                .map(|_| {
                    tokio::spawn(async {
                        let v = fib(FIB_N);
                        tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                        v
                    })
                })
                .collect();
            let sv: Vec<u64> = futures::future::join_all(src_h)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let cpu_h: Vec<_> = sv
                .into_iter()
                .map(|v| tokio::spawn(async move { fib(FIB_N).wrapping_add(v) }))
                .collect();
            let cv: Vec<u64> = futures::future::join_all(cpu_h)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let merge_h: Vec<_> = cv
                .chunks(2)
                .map(|p| {
                    let (a, b) = (p[0], p[1]);
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                        a + b
                    })
                })
                .collect();
            let mv: Vec<u64> = futures::future::join_all(merge_h)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let step_h: Vec<_> = mv
                .into_iter()
                .map(|v| tokio::spawn(async move { fib(FIB_N).wrapping_add(v) }))
                .collect();
            let rv: Vec<u64> = futures::future::join_all(step_h)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect();
            let _bl_val = rv[0].wrapping_add(rv[1]).wrapping_add(rv[2]);
        },
    )
    .await;

    row("22-task 5-layer DAG", tf_ms, bl_ms);
}
