# rusty-taskflow

[English](#english) | [中文](#中文)

---

> **Status: Early Development** — This project is under active development. The API is functional but may change. Use in production at your own risk.

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

### Performance

**Zero-overhead abstraction** — Framework overhead is minimal compared to hand-written tokio baseline code.

Benchmark methodology: 5 warmup rounds + 60 measurement rounds, round-robin execution order to minimize cache bias.

| Scenario | Overhead vs Baseline |
|----------|---------------------|
| CPU Linear Chain (20 tasks, fib(32) each) | +0.0% |
| CPU Fan-out (1→6) + Tree Reduce | -3.2% |
| CPU Diamond (2 parallel paths) | -0.4% |
| IO Linear Chain (20 tasks, 10ms each) | -0.6% |
| Mixed CPU+IO Complex DAG | -3.8% |

All scenarios within ±5% of manual tokio implementation.

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

> **状态：早期开发中** — 本项目仍在积极开发阶段，API 可正常使用但可能发生变更，生产环境使用需自行评估风险。

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

### 性能

**零开销抽象** — 框架开销极低，与手写 tokio 基线代码几乎无差异。

测试方法：5 轮预热 + 60 轮测量，轮询执行顺序以消除缓存偏差。

| 场景 | 相对基线开销 |
|------|-------------|
| CPU 线性链（20 任务，每任务 fib(32)） | +0.0% |
| CPU 扇出（1→6）+ 树归约 | -3.2% |
| CPU 菱形（2 并行路径） | -0.4% |
| IO 线性链（20 任务，每任务 10ms） | -0.6% |
| 混合 CPU+IO 复杂 DAG | -3.8% |

所有场景与手写 tokio 实现差异在 ±5% 以内。

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
