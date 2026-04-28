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

fn row(label: &str, tf_ms: f64, bl_ms: f64) {
    let overhead = if bl_ms > 0.01 {
        (tf_ms - bl_ms) / bl_ms * 100.0
    } else {
        0.0
    };
    println!(
        "  {label:42} tf={tf_ms:>9.2}ms  base={bl_ms:>9.2}ms  ovhd={overhead:>+7.1}%"
    );
}

fn row3(label: &str, tf_ms: f64, dx_ms: f64, bl_ms: f64) {
    fn pct(a: f64, b: f64) -> f64 {
        if b > 0.01 { (a - b) / b * 100.0 } else { 0.0 }
    }
    println!("  {label}");
    println!(
        "    taskflow: {:>9.2}ms  ({:>+7.1}% vs baseline)",
        tf_ms,
        pct(tf_ms, bl_ms)
    );
    println!(
        "    dagx:     {:>9.2}ms  ({:>+7.1}% vs baseline)",
        dx_ms,
        pct(dx_ms, bl_ms)
    );
    println!("    baseline: {:>9.2}ms", bl_ms);
}

async fn avg_time_ms<F, Fut>(runs: usize, mut f: F) -> f64
where
    F: FnMut() -> Fut,
    Fut: std::future::Future <Output = ()>,
{
    let mut total = 0.0;
    for _ in 0..runs {
        let t0 = Instant::now();
        f().await;
        total += t0.elapsed().as_secs_f64() * 1000.0;
    }
    total / runs as f64
}

async fn avg_time_ms_with_value<T, F, Fut>(runs: usize, mut f: F) -> (f64, T)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future <Output = T>,
{
    let mut total = 0.0;
    let mut last = None;
    for _ in 0..runs {
        let t0 = Instant::now();
        let value = f().await;
        total += t0.elapsed().as_secs_f64() * 1000.0;
        last = Some(value);
    }
    (total / runs as f64, last.expect("runs must be > 0"))
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
    fn run(self, v: u64) -> u64 {
        fib(self.0).wrapping_add(v)
    }
}

struct CpuAdd2;
#[sync_task]
impl CpuAdd2 {
    fn run(self, a: u64, b: u64) -> u64 {
        a.wrapping_add(b)
    }
}

struct CpuAdd3;
#[sync_task]
impl CpuAdd3 {
    fn run(self, a: u64, b: u64, c: u64) -> u64 {
        a.wrapping_add(b).wrapping_add(c)
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
    async fn run(self, v: u64) -> u64 {
        tokio::time::sleep(Duration::from_millis(self.0)).await;
        v + 1
    }
}

struct IoAdd2(u64);
#[async_task]
impl IoAdd2 {
    async fn run(self, a: u64, b: u64) -> u64 {
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
    async fn run(self, v: u64) -> u64 {
        let r = fib(self.fib_n).wrapping_add(v);
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
    async fn run(self, a: u64, b: u64) -> u64 {
        let r = fib(self.fib_n).wrapping_add(a).wrapping_add(b);
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
/// Number of repetitions per scenario, average used for reporting.
const BENCH_REPEAT: usize = 5;

// =========================================================================
// CPU-intensive benchmarks
// =========================================================================

/// Linear chain: Source → Step → Step → ... → Step (all CPU-bound).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_cpu_chain() {
    header(&format!("CPU: Linear Chain  (fib({FIB_N}) per task)"));

    for n in [5usize, 10, 20] {
        // ---- taskflow ----
        let (tf_ms, tf_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
            let mut flow = Flow::new();
            let mut prev = flow.commit_source_task("src", CpuSource(FIB_N));
            for i in 0..n {
                prev = flow
                    .commit_task(format!("s{i}"), CpuStep(FIB_N))
                    .with_dependencies(prev);
            }
            flow.run(prev).await.unwrap()
        })
        .await;

        // ---- dagx ----
        let (dx_ms, dx_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
            let dag = DagRunner::new();
            let mut dprev: TaskHandle<u64> = (&dag.add_task(dx::CpuSource(FIB_N))).into();
            for _ in 0..n {
                dprev = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&dprev);
            }
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            dag.get(dprev).unwrap()
        })
        .await;

        // ---- baseline: manual tokio ----
        let (bl_ms, bl_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
            let mut val = fib(FIB_N);
            for _ in 0..n {
                let v = val;
                val = tokio::spawn(async move { fib(FIB_N).wrapping_add(v) })
                    .await
                    .unwrap();
            }
            val
        })
        .await;

        assert_eq!(tf_val, bl_val);
        assert_eq!(dx_val, bl_val);
        row3(&format!("chain len={n}"), tf_ms, dx_ms, bl_ms);
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

    // ---- taskflow ----
    let (tf_ms, tf_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
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
        flow.run(fin).await.unwrap()
    })
    .await;

    // ---- dagx ----
    let (dx_ms, dx_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
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
        dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
        dag.get(fin).unwrap()
    })
    .await;

    // ---- baseline ----
    let (bl_ms, bl_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
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
        a1.wrapping_add(a2).wrapping_add(a3)
    })
    .await;

    assert_eq!(tf_val, bl_val);
    assert_eq!(dx_val, bl_val);
    row3("fan-out=6, tree-reduce", tf_ms, dx_ms, bl_ms);
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

    // ---- taskflow ----
    let (tf_ms, tf_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
        let mut flow = Flow::new();
        let s1 = flow.commit_source_task("s1", CpuSource(FIB_N));
        let s2 = flow.commit_source_task("s2", CpuSource(FIB_N));
        let c1 = flow.commit_task("c1", CpuStep(FIB_N)).with_dependencies(s1);
        let c2 = flow.commit_task("c2", CpuStep(FIB_N)).with_dependencies(s2);
        let merge = flow.commit_task("merge", CpuAdd2).with_dependencies((c1, c2));
        flow.run(merge).await.unwrap()
    })
    .await;

    // ---- dagx ----
    let (dx_ms, dx_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
        let dag = DagRunner::new();
        let s1 = dag.add_task(dx::CpuSource(FIB_N));
        let s2 = dag.add_task(dx::CpuSource(FIB_N));
        let c1 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s1);
        let c2 = dag.add_task(dx::CpuStep(FIB_N)).depends_on(&s2);
        let merge = dag.add_task(dx::CpuAdd2).depends_on((&c1, &c2));
        dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
        dag.get(merge).unwrap()
    })
    .await;

    // ---- baseline ----
    let (bl_ms, bl_val) = avg_time_ms_with_value(BENCH_REPEAT, || async {
        let (v1, v2) = tokio::join!(
            tokio::spawn(async { fib(FIB_N) }),
            tokio::spawn(async { fib(FIB_N) }),
        );
        let c1 = tokio::spawn(async move { fib(FIB_N).wrapping_add(v1.unwrap()) });
        let c2 = tokio::spawn(async move { fib(FIB_N).wrapping_add(v2.unwrap()) });
        let (r1, r2) = tokio::join!(c1, c2);
        r1.unwrap().wrapping_add(r2.unwrap())
    })
    .await;

    assert_eq!(tf_val, bl_val);
    assert_eq!(dx_val, bl_val);
    row3("diamond (2 paths)", tf_ms, dx_ms, bl_ms);
}

// =========================================================================
// IO-intensive benchmarks
// =========================================================================

/// Linear chain of IO tasks (sequential sleeps).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_io_chain() {
    header(&format!("IO: Linear Chain  (sleep {SLEEP_MS}ms per task)"));

    for n in [5usize, 10, 20] {
        // ---- taskflow ----
        let tf_ms = avg_time_ms(BENCH_REPEAT, || async {
            let mut flow = Flow::new();
            let mut prev = flow.commit_source_task("src", IoSource(SLEEP_MS));
            for i in 0..n {
                prev = flow
                    .commit_task(format!("io{i}"), IoStep(SLEEP_MS))
                    .with_dependencies(prev);
            }
            let _tf_val = flow.run(prev).await.unwrap();
        })
        .await;

        // ---- dagx ----
        let dx_ms = avg_time_ms(BENCH_REPEAT, || async {
            let dag = DagRunner::new();
            let mut dprev: TaskHandle<u64> = (&dag.add_task(dx::IoSource(SLEEP_MS))).into();
            for _ in 0..n {
                dprev = dag.add_task(dx::IoStep(SLEEP_MS)).depends_on(&dprev);
            }
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            let _dx_val: u64 = dag.get(dprev).unwrap();
        })
        .await;

        // ---- baseline ----
        let bl_ms = avg_time_ms(BENCH_REPEAT, || async {
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
        })
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

    // ---- taskflow ----
    let tf_ms = avg_time_ms(BENCH_REPEAT, || async {
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
    })
    .await;

    // ---- dagx ----
    let dx_ms = avg_time_ms(BENCH_REPEAT, || async {
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
    })
    .await;

    // ---- baseline ----
    let bl_ms = avg_time_ms(BENCH_REPEAT, || async {
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
        // sequential tree reduce (minimal overhead)
        let a1 = vals[0] + vals[1];
        let a2 = vals[2] + vals[3];
        let a3 = vals[4] + vals[5];
        let _bl_val = a1 + a2 + a3;
    })
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

    // ---- taskflow ----
    let tf_ms = avg_time_ms(BENCH_REPEAT, || async {
        let mut flow = Flow::new();
        // CPU pipeline
        let cs = flow.commit_source_task("cs", CpuSource(FIB_N));
        let c1 = flow.commit_task("c1", CpuStep(FIB_N)).with_dependencies(cs);
        let c2 = flow.commit_task("c2", CpuStep(FIB_N)).with_dependencies(c1);
        // IO pipeline
        let is = flow.commit_source_task("is", IoSource(SLEEP_MS));
        let i1 = flow.commit_task("i1", IoStep(SLEEP_MS)).with_dependencies(is);
        let i2 = flow.commit_task("i2", IoStep(SLEEP_MS)).with_dependencies(i1);
        // merge
        let fin = flow
            .commit_task("fin", MixedAdd2 { fib_n: 20, sleep_ms: 1 })
            .with_dependencies((c2, i2));
        let _tf_val = flow.run(fin).await.unwrap();
    })
    .await;

    // ---- dagx ----
    let dx_ms = avg_time_ms(BENCH_REPEAT, || async {
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
    })
    .await;

    // ---- baseline ----
    let bl_ms = avg_time_ms(BENCH_REPEAT, || async {
        // CPU pipeline (sequential 3 tasks)
        let cpu_handle = tokio::spawn(async {
            let mut v = fib(FIB_N);
            v = fib(FIB_N).wrapping_add(v);
            v = fib(FIB_N).wrapping_add(v);
            v
        });
        // IO pipeline (sequential 3 tasks)
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
    })
    .await;

    row3("2 pipelines + merge", tf_ms, dx_ms, bl_ms);
}

/// Alternating CPU and IO steps in a single chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_mixed_alternating_chain() {
    header("Mixed: Alternating CPU/IO Chain");

    for n in [4usize, 8] {
        // ---- taskflow ----
        let tf_ms = avg_time_ms(BENCH_REPEAT, || async {
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
        })
        .await;

        // ---- dagx ----
        let dx_ms = avg_time_ms(BENCH_REPEAT, || async {
            let dag = DagRunner::new();
            let mut dprev: TaskHandle<u64> = (&dag.add_task(dx::MixedSource { fib_n: FIB_N, sleep_ms: SLEEP_MS })).into();
            for _ in 0..n {
                dprev = dag.add_task(dx::MixedStep { fib_n: FIB_N, sleep_ms: SLEEP_MS }).depends_on(&dprev);
            }
            dag.run(|fut| { tokio::spawn(fut); }).await.unwrap();
            let _dx_val: u64 = dag.get(dprev).unwrap();
        })
        .await;

        // ---- baseline ----
        let bl_ms = avg_time_ms(BENCH_REPEAT, || async {
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
        })
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

    // ---- taskflow ----
    let tf_ms = avg_time_ms(BENCH_REPEAT, || async {
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
    })
    .await;

    // ---- dagx ----
    let dx_ms = avg_time_ms(BENCH_REPEAT, || async {
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
    })
    .await;

    // ---- baseline ----
    let bl_ms = avg_time_ms(BENCH_REPEAT, || async {
        // layer 0: 6 mixed sources in parallel
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
        // layer 1: 6 CPU steps in parallel
        let cpu_handles: Vec<_> = src_vals
            .into_iter()
            .map(|v| tokio::spawn(async move { fib(FIB_N).wrapping_add(v) }))
            .collect();
        let cpu_vals: Vec<u64> = futures::future::join_all(cpu_handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        // layer 2: 3 IO merges in parallel
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
        // layer 3: final reduce
        let _bl_val = merge_vals[0]
            .wrapping_add(merge_vals[1])
            .wrapping_add(merge_vals[2]);
    })
    .await;

    row3("6-source 4-layer DAG", tf_ms, dx_ms, bl_ms);
}

// =========================================================================
// Stress tests (large-scale, run with --ignored)
// =========================================================================

/// Deep chain: 100 CPU tasks in sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_stress_deep_cpu_chain() {
    header(&format!("STRESS: Deep CPU Chain (100 tasks, fib({FIB_N}))"));

    let tf_ms = avg_time_ms(BENCH_REPEAT, || async {
        let mut flow = Flow::new();
        let mut prev = flow.commit_source_task("src", CpuSource(FIB_N));
        for i in 0..99 {
            prev = flow
                .commit_task(format!("s{i}"), CpuStep(FIB_N))
                .with_dependencies(prev);
        }
        let _tf_val = flow.run(prev).await.unwrap();
    })
    .await;

    let bl_ms = avg_time_ms(BENCH_REPEAT, || async {
        let mut val = fib(FIB_N);
        for _ in 0..99 {
            let v = val;
            val = tokio::spawn(async move { fib(FIB_N).wrapping_add(v) })
                .await
                .unwrap();
        }
    })
    .await;

    row("chain len=100", tf_ms, bl_ms);
}

/// Deep chain: 100 IO tasks in sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_stress_deep_io_chain() {
    header(&format!("STRESS: Deep IO Chain (100 tasks, sleep {SLEEP_MS}ms)"));

    let tf_ms = avg_time_ms(BENCH_REPEAT, || async {
        let mut flow = Flow::new();
        let mut prev = flow.commit_source_task("src", IoSource(SLEEP_MS));
        for i in 0..99 {
            prev = flow
                .commit_task(format!("io{i}"), IoStep(SLEEP_MS))
                .with_dependencies(prev);
        }
        let _tf_val = flow.run(prev).await.unwrap();
    })
    .await;

    let bl_ms = avg_time_ms(BENCH_REPEAT, || async {
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
    })
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

    // ---- taskflow ----
    let tf_ms = avg_time_ms(BENCH_REPEAT, || async {
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
    })
    .await;

    // ---- baseline ----
    let bl_ms = avg_time_ms(BENCH_REPEAT, || async {
        // layer 0: 6 mixed sources
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
        // layer 1: 6 CPU steps
        let cpu_h: Vec<_> = sv
            .into_iter()
            .map(|v| tokio::spawn(async move { fib(FIB_N).wrapping_add(v) }))
            .collect();
        let cv: Vec<u64> = futures::future::join_all(cpu_h)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        // layer 2: 3 IO merges
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
        // layer 3: 3 CPU steps
        let step_h: Vec<_> = mv
            .into_iter()
            .map(|v| tokio::spawn(async move { fib(FIB_N).wrapping_add(v) }))
            .collect();
        let rv: Vec<u64> = futures::future::join_all(step_h)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        // layer 4: reduce
        let _bl_val = rv[0].wrapping_add(rv[1]).wrapping_add(rv[2]);
    })
    .await;

    row("22-task 5-layer DAG", tf_ms, bl_ms);
}
