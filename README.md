# rusty-taskflow

[English](#english) | [中文](#中文)

---

## English

A high-performance, type-safe DAG (Directed Acyclic Graph) execution framework for Rust with configuration-driven flow definition.

### Features

- **Type-safe DAG orchestration**: Compile-time dependency validation with automatic topological ordering
- **Concurrent execution**: Tasks at the same layer run asynchronously in parallel
- **Unified sync/async model**: Support both `#[sync_task]` and `#[async_task]` with unified async execution
- **Configuration-driven**: Define flows in TOML, generate type-safe code at compile time
- **Multi-flow management**: Load and run multiple flows from a single application
- **Two execution modes**:
  - Build flow first, execute later with `sink_id`
  - Direct execution by path

### Performance Benchmarks

Benchmark methodology: 5 warmup rounds + 60 measurement rounds, round-robin execution order to minimize cache bias.

Compared against: **dagx**, **dagrs**, and **manual tokio baseline**.

#### CPU-bound Tasks (fib(32) per task)

| Scenario | taskflow | dagx | dagrs | baseline |
|----------|----------|------|-------|----------|
| Linear Chain (20 steps) | 78.63ms (+0.0%) | 78.79ms (+0.2%) | 80.91ms (+2.9%) | 78.61ms |
| Fan-out (1→6) + Tree Reduce | 12.99ms (-3.2%) | 12.89ms (-3.9%) | 13.45ms (+0.3%) | 13.41ms |
| Diamond (2 paths) | 7.76ms (-0.4%) | 7.82ms (+0.3%) | 7.77ms (-0.3%) | 7.79ms |
| spawn_blocking Chain (20) | 79.24ms (-0.0%) | 79.74ms (+0.6%) | 79.16ms (-0.1%) | 79.25ms |

#### IO-bound Tasks (10ms sleep per task)

| Scenario | taskflow | dagx | dagrs | baseline |
|----------|----------|------|-------|----------|
| Linear Chain (20 steps) | 250.94ms (-0.6%) | 250.56ms (-0.7%) | 250.72ms (-0.7%) | 252.38ms |
| Linear Chain (10 steps) | 131.10ms (-0.3%) | 131.65ms (+0.1%) | 131.67ms (+0.2%) | 131.47ms |

#### Mixed CPU+IO Tasks

| Scenario | taskflow | dagx | baseline |
|----------|----------|------|----------|
| Alternating Chain (8 steps) | 157.66ms (+1.5%) | 158.82ms (+2.3%) | 155.26ms |
| Complex 6-Source DAG | 43.66ms (-3.8%) | 44.44ms (-2.1%) | 45.40ms |
| Two Pipelines + Merge | 40.69ms (+10.1%) | 39.32ms (+6.4%) | 36.95ms |

**Result**: All frameworks within ±5% of baseline for most scenarios. taskflow achieves zero-overhead abstraction.

### Quick Start

#### 1. Define Task Operators

```rust
use taskflow::{sync_task, async_task};

pub struct FibInput;
#[sync_task(path = "::taskflow")]
impl FibInput {
    pub fn new() -> Self { Self }
    fn run(self) -> u64 { 18 }
}

pub struct AsyncPersistFib;
#[async_task(path = "::taskflow")]
impl AsyncPersistFib {
    pub fn new() -> Self { Self }
    async fn run(self, fib: &u64) -> u64 {
        tokio::fs::write("result.txt", format!("{fib}")).await.unwrap();
        *fib
    }
}
```

#### 2. Define Flow in TOML

```toml
[flow]
name = "mixed_fib_io"

[[flow.source]]
name = "FibInput"
dependencies = []
output = "fib_n"
builder = "crate::config_tasks::FibInput::new()"

[[flow.processor]]
name = "Fib"
dependencies = ["fib_n"]
output = "fib_value"
builder = "crate::config_tasks::Fib::new()"

[[flow.processor]]
name = "AsyncPersistFib"
dependencies = ["fib_value"]
output = "persisted_fib"
builder = "crate::config_tasks::AsyncPersistFib::new()"

[flow.sink]
name = "DoubleSink"
dependencies = ["persisted_fib"]
output = "mixed_fib_output"
builder = "crate::config_tasks::DoubleSink::new()"
```

#### 3. Execute Flow

```rust
// Option A: Build then execute
let (mut flow, sink_id) = build_flow_by_path(path).expect("build failed");
let output = flow.run_with_sink_id(sink_id).await.expect("run failed");

// Option B: Direct execution
let output = run_flow_by_path(path).await.expect("run failed");
```

### Project Structure

```
tf-examples/
├── configs/
│   ├── flows.toml              # Flow index
│   └── flows/*.toml            # Individual flow definitions
├── src/
│   ├── config_tasks.rs         # Task implementations
│   └── main.rs                 # Entry point
└── build.rs                    # Compile-time code generation
```

---

## 中文

高性能、类型安全的 Rust DAG（有向无环图）执行框架，支持配置驱动的流程定义。

### 核心特性

- **类型安全的 DAG 编排**：编译期依赖校验，自动拓扑排序
- **并发执行**：同层任务异步并行执行
- **sync/async 统一模型**：同时支持 `#[sync_task]` 和 `#[async_task]`，底层统一异步执行
- **配置驱动**：TOML 定义流程，编译期生成类型安全代码
- **多流程管理**：单应用加载运行多个流程
- **两种执行模式**：
  - 先构建后执行（通过 `sink_id`）
  - 按路径直接执行

### 性能基准测试

测试方法：5 轮预热 + 60 轮测量，轮询执行顺序以消除缓存偏差。

对比框架：**dagx**、**dagrs**、**手写 tokio 基线**。

#### CPU 密集型任务（每任务 fib(32)）

| 场景 | taskflow | dagx | dagrs | 基线 |
|------|----------|------|-------|------|
| 线性链（20步） | 78.63ms (+0.0%) | 78.79ms (+0.2%) | 80.91ms (+2.9%) | 78.61ms |
| 扇出（1→6）+ 树归约 | 12.99ms (-3.2%) | 12.89ms (-3.9%) | 13.45ms (+0.3%) | 13.41ms |
| 菱形（2路径） | 7.76ms (-0.4%) | 7.82ms (+0.3%) | 7.77ms (-0.3%) | 7.79ms |
| spawn_blocking 链（20） | 79.24ms (-0.0%) | 79.74ms (+0.6%) | 79.16ms (-0.1%) | 79.25ms |

#### IO 密集型任务（每任务 sleep 10ms）

| 场景 | taskflow | dagx | dagrs | 基线 |
|------|----------|------|-------|------|
| 线性链（20步） | 250.94ms (-0.6%) | 250.56ms (-0.7%) | 250.72ms (-0.7%) | 252.38ms |
| 线性链（10步） | 131.10ms (-0.3%) | 131.65ms (+0.1%) | 131.67ms (+0.2%) | 131.47ms |

#### 混合 CPU+IO 任务

| 场景 | taskflow | dagx | 基线 |
|------|----------|------|------|
| 交替链（8步） | 157.66ms (+1.5%) | 158.82ms (+2.3%) | 155.26ms |
| 复杂 6 源 DAG | 43.66ms (-3.8%) | 44.44ms (-2.1%) | 45.40ms |
| 双管道 + 合并 | 40.69ms (+10.1%) | 39.32ms (+6.4%) | 36.95ms |

**结论**：大多数场景下各框架与基线差异在 ±5% 以内。taskflow 实现零开销抽象。

### 快速开始

#### 1. 定义任务算子

```rust
use taskflow::{sync_task, async_task};

pub struct FibInput;
#[sync_task(path = "::taskflow")]
impl FibInput {
    pub fn new() -> Self { Self }
    fn run(self) -> u64 { 18 }
}

pub struct AsyncPersistFib;
#[async_task(path = "::taskflow")]
impl AsyncPersistFib {
    pub fn new() -> Self { Self }
    async fn run(self, fib: &u64) -> u64 {
        tokio::fs::write("result.txt", format!("{fib}")).await.unwrap();
        *fib
    }
}
```

#### 2. 用 TOML 描述流程

```toml
[flow]
name = "mixed_fib_io"

[[flow.source]]
name = "FibInput"
dependencies = []
output = "fib_n"
builder = "crate::config_tasks::FibInput::new()"

[[flow.processor]]
name = "Fib"
dependencies = ["fib_n"]
output = "fib_value"
builder = "crate::config_tasks::Fib::new()"

[[flow.processor]]
name = "AsyncPersistFib"
dependencies = ["fib_value"]
output = "persisted_fib"
builder = "crate::config_tasks::AsyncPersistFib::new()"

[flow.sink]
name = "DoubleSink"
dependencies = ["persisted_fib"]
output = "mixed_fib_output"
builder = "crate::config_tasks::DoubleSink::new()"
```

#### 3. 执行流程

```rust
// 方式 A：先构建后执行
let (mut flow, sink_id) = build_flow_by_path(path).expect("构建失败");
let output = flow.run_with_sink_id(sink_id).await.expect("执行失败");

// 方式 B：直接按路径执行
let output = run_flow_by_path(path).await.expect("执行失败");
```

### 项目结构

```
tf-examples/
├── configs/
│   ├── flows.toml              # 流程索引
│   └── flows/*.toml            # 单个流程定义
├── src/
│   ├── config_tasks.rs         # 任务实现
│   └── main.rs                 # 业务入口
└── build.rs                    # 编译期代码生成
```

### 运行示例

```bash
cargo run -p tf-examples
```

---

## License

MIT
