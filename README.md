# rusty-taskflow

[English](#english) | [中文](#中文)

---

> **Status: Early Development** — This project is under active development. The API is functional but may change. Use in production at your own risk.

---

## English

A high-performance, type-safe DAG (Directed Acyclic Graph) execution framework for Rust with configuration-driven flow definition and a built-in component registry for shared infrastructure.

### Features

- **Type-safe DAG orchestration**: Compile-time dependency validation with automatic topological ordering
- **Concurrent execution**: Tasks at the same layer run asynchronously in parallel
- **Unified sync/async model**: Support both `#[sync_task]` and `#[async_task]` with unified async execution
- **Configuration-driven**: Define flows in TOML, generate type-safe code at compile time
- **Multi-flow management**: Load and run multiple flows from a single application
- **FlowContext component injection** : Declare `ctx: &FlowContext` in any task to pull shared singletons or per-call factory objects (DB clients, config, request IDs, ...) without threading them through DAG edges. Components are declared globally via `register_singleton!` / `register_factory!`, or imperatively via `FlowContext::insert_singleton` / `insert_factory` for tests.
- **Three execution modes**:
  - Build flow first, execute later with `sink_id`
  - Direct execution by path
  - Construct flow manually in Rust code via `Flow::new()` / `Flow::with_context()`

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

#### 1. Define task operators

A task is an inherent `impl` block annotated with `#[sync_task]` or `#[async_task]`. All DAG inputs must be shared references `&T` (the runtime stores upstream outputs as `Arc<T>` internally).

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

#### 2. Register shared components (optional)

Components exposed through `FlowContext` come in two flavors:

- **Singleton** — one instance per process, shared by reference.
- **Factory** — constructor registered by name, returns a fresh `Box<T>` per call.

```rust
use taskflow::{register_singleton, register_factory};

pub struct MultiplierConfig { pub factor: u64 }
impl MultiplierConfig { pub fn new() -> Self { Self { factor: 3 } } }
register_singleton!(MultiplierConfig, "multiplier_config", MultiplierConfig::new);

pub struct RequestId(pub u64);
impl RequestId {
    pub fn new() -> Self {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self(N.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}
register_factory!(RequestId, "request_id", RequestId::new);
```

#### 3. Consume components inside a task

Declare `ctx: &FlowContext` as the **first non-`self` parameter**. The proc macro wires the runtime context in and does *not* treat it as a DAG input:

```rust
use taskflow::{sync_task, FlowContext};

pub struct Multiply;
impl Multiply { pub fn new() -> Self { Self } }

#[sync_task(path = "::taskflow")]
impl Multiply {
    fn run(self, ctx: &FlowContext, v: &u64) -> u64 {
        let cfg = ctx.get_singleton_component::<MultiplierConfig>("multiplier_config").unwrap();
        let req = ctx.create_component::<RequestId>("request_id").unwrap();
        println!("Multiply[req={}]: {} * {}", req.0, cfg.factor, v);
        cfg.factor * v
    }
}
```

Tasks that don't need the context simply omit the parameter — no source changes required.

#### 4. Describe the flow in TOML (optional)

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

#### 5. Execute the flow

```rust
// Option A: TOML-defined flow, build then execute
let (mut flow, sink_id) = build_flow_by_path(path).expect("build failed");
let output = flow.run_with_sink_id(sink_id).await.expect("run failed");

// Option B: TOML-defined flow, direct execution
let output = run_flow_by_path(path).await.expect("run failed");

// Option C: Manual graph; FlowContext auto-populated from `register_*!` macros
use taskflow::tf::flow::Flow;
let mut flow = Flow::new();
let s1 = flow.commit_source_task("S1", FibSource1::new());
let s2 = flow.commit_source_task("S2", FibSource2::new());
let merged = flow.commit_task("Merger", Merger::new()).with_dependencies((s1, s2));
let fib   = flow.commit_task("Fib", Fib::new()).with_dependencies(merged);
let sink  = flow.commit_task("Multiply", Multiply::new()).with_dependencies(fib);
let output = flow.run(sink).await.expect("manual run failed");

// Option D: Inject a custom FlowContext (tests, mocking, dynamic wiring)
use std::sync::Arc;
use taskflow::FlowContext;

let mut ctx = FlowContext::new();
ctx.insert_singleton("multiplier_config", MultiplierConfig { factor: 100 });
ctx.insert_factory("request_id", RequestId::new);
let mut flow = Flow::with_context(Arc::new(ctx));
// ... commit tasks and run as usual ...
```

### Project Structure

```
tf-examples/
├── configs/
│   ├── flows.toml              # Flow index
│   └── flows/*.toml            # Individual flow definitions
├── src/
│   ├── config_tasks.rs         # Task implementations + component registration
│   └── main.rs                 # Entry point (demonstrates all four modes)
└── build.rs                    # Compile-time code generation
```

Run the bundled demo:

```bash
cargo run -p tf-examples
```

---

## 中文

> **状态：早期开发中** — 本项目仍在积极开发阶段，API 可正常使用但可能发生变更，生产环境使用需自行评估风险。

高性能、类型安全的 Rust DAG（有向无环图）执行框架，支持配置驱动的流程定义，并内置组件注册表用于共享基础设施。

### 核心特性

- **类型安全的 DAG 编排**：编译期依赖校验，自动拓扑排序
- **并发执行**：同层任务异步并行执行
- **sync/async 统一模型**：同时支持 `#[sync_task]` 和 `#[async_task]`，底层统一异步执行
- **配置驱动**：TOML 定义流程，编译期生成类型安全代码
- **多流程管理**：单应用加载运行多个流程
- **FlowContext 组件注入** ：任意 task 在 `run` 首参声明 `ctx: &FlowContext` 即可取用共享单例或工厂对象（数据库客户端、配置、请求 ID 等），无需再通过 DAG 依赖链传递。组件通过 `register_singleton!` / `register_factory!` 全局注册，或测试场景下通过 `FlowContext::insert_singleton` / `insert_factory` 运行时注入。
- **四种执行模式**：
  - 先构建后执行（通过 `sink_id`）
  - 按路径直接执行
  - 通过 `Flow::new()` 在 Rust 代码中手动构图（ctx 自动从 inventory 构建）
  - 通过 `Flow::with_context()` 注入自定义 ctx（测试/Mock）

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

task 就是一个带 `#[sync_task]` 或 `#[async_task]` 注解的 inherent `impl` 块。所有 DAG 输入参数必须是共享引用 `&T`（运行时内部用 `Arc<T>` 存储上游输出）。

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

#### 2. 注册共享组件（可选）

`FlowContext` 暴露两类组件：

- **Singleton**：进程级单实例，通过引用共享
- **Factory**：按名注册构造器，每次调用返回一个新的 `Box<T>`

```rust
use taskflow::{register_singleton, register_factory};

pub struct MultiplierConfig { pub factor: u64 }
impl MultiplierConfig { pub fn new() -> Self { Self { factor: 3 } } }
register_singleton!(MultiplierConfig, "multiplier_config", MultiplierConfig::new);

pub struct RequestId(pub u64);
impl RequestId {
    pub fn new() -> Self {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self(N.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}
register_factory!(RequestId, "request_id", RequestId::new);
```

#### 3. 在 task 中消费组件

将 `ctx: &FlowContext` 声明为 `run` 的**首个非 `self` 参数**，proc macro 会把运行时 ctx 自动注入，并且**不会**把它识别为 DAG 输入：

```rust
use taskflow::{sync_task, FlowContext};

pub struct Multiply;
impl Multiply { pub fn new() -> Self { Self } }

#[sync_task(path = "::taskflow")]
impl Multiply {
    fn run(self, ctx: &FlowContext, v: &u64) -> u64 {
        let cfg = ctx.get_singleton_component::<MultiplierConfig>("multiplier_config").unwrap();
        let req = ctx.create_component::<RequestId>("request_id").unwrap();
        println!("Multiply[req={}]: {} * {}", req.0, cfg.factor, v);
        cfg.factor * v
    }
}
```

不需要 ctx 的 task 原样不动，完全向后兼容。

#### 4. 用 TOML 描述流程（可选）

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

#### 5. 执行流程

```rust
// 方式 A：TOML 流程，先构建后执行
let (mut flow, sink_id) = build_flow_by_path(path).expect("构建失败");
let output = flow.run_with_sink_id(sink_id).await.expect("执行失败");

// 方式 B：TOML 流程，直接执行
let output = run_flow_by_path(path).await.expect("执行失败");

// 方式 C：手动构图，FlowContext 自动从 register_*! 宏初始化
use taskflow::tf::flow::Flow;
let mut flow = Flow::new();
let s1 = flow.commit_source_task("S1", FibSource1::new());
let s2 = flow.commit_source_task("S2", FibSource2::new());
let merged = flow.commit_task("Merger", Merger::new()).with_dependencies((s1, s2));
let fib   = flow.commit_task("Fib", Fib::new()).with_dependencies(merged);
let sink  = flow.commit_task("Multiply", Multiply::new()).with_dependencies(fib);
let output = flow.run(sink).await.expect("手动执行失败");

// 方式 D：注入自定义 FlowContext（测试 / Mock / 动态装配）
use std::sync::Arc;
use taskflow::FlowContext;

let mut ctx = FlowContext::new();
ctx.insert_singleton("multiplier_config", MultiplierConfig { factor: 100 });
ctx.insert_factory("request_id", RequestId::new);
let mut flow = Flow::with_context(Arc::new(ctx));
// ... 后续 commit_task / run 与常规流程一致 ...
```

### 项目结构

```
tf-examples/
├── configs/
│   ├── flows.toml              # 流程索引
│   └── flows/*.toml            # 单个流程定义
├── src/
│   ├── config_tasks.rs         # 任务实现 + 组件注册
│   └── main.rs                 # 业务入口（演示全部四种执行模式）
└── build.rs                    # 编译期代码生成
```

### 运行示例

```bash
cargo run -p tf-examples
```

输出会依次演示：
- TOML 流程（sink_id 与路径直连两种调用方式）
- 手动构图，自动拉取 inventory 中的 `MultiplierConfig`（factor=3）和 `RequestId` 工厂
- 同一张图改用 `Flow::with_context` 注入 `factor=100` 的自定义 ctx，结果随之变化

---

## Changelog

### FlowContext component injection

- `Flow::new()` now auto-populates a `FlowContext` from every
  `register_singleton!` / `register_factory!` declaration compiled into the
  binary.
- `Flow::with_context(Arc<FlowContext>)` accepts a custom context for tests
  and dynamic wiring; `FlowContext::insert_singleton` /
  `insert_factory` allow runtime insertion (including capturing closures for
  factories).
- `#[sync_task]` / `#[async_task]` detect a leading `ctx: &FlowContext`
  parameter and forward the runtime context automatically. Tasks that do not
  declare one are unchanged — fully backward-compatible.
- `ComponentEntry::Factory` is now stored as `Box<dyn Fn>` so that both
  inventory-registered factories and runtime-inserted capturing factories
  share a single code path. The extra vtable indirection is dominated by the
  `Box::new(T)` allocation that every factory performs anyway.

---

## License

MIT
