mod config_tasks;

use std::sync::Arc;

use rusty_taskflow::{tf::flow::Flow, FlowContext};

// add generated .rs to compile
include!(concat!(env!("OUT_DIR"), "/generated_typed_flows.rs"));

fn expect_u64(output: &std::sync::Arc<dyn std::any::Any + Send + Sync>, context: &str) -> u64 {
    output
        .downcast_ref::<u64>()
        .copied()
        .unwrap_or_else(|| panic!("{context}: unexpected sink output type, expected u64"))
}

/// Default path: `Flow::new()` auto-populates the `FlowContext` from every
/// `register_singleton!` / `register_factory!` declaration in the binary
/// (here: `MultiplierConfig` and `RequestId` in `config_tasks.rs`). The
/// `Multiply` task pulls both components via `ctx: &FlowContext` rather than
/// threading them through DAG edges.
async fn run_manual_flow() -> u64 {
    let mut flow = Flow::new();

    let left = flow.commit_source_task("FibSource1", config_tasks::FibSource1::new());
    let right = flow.commit_source_task("FibSource2", config_tasks::FibSource2::new());
    let merged = flow
        .commit_task("Merger", config_tasks::Merger::new())
        .with_dependencies((left, right));
    let fib = flow
        .commit_task("Fib", config_tasks::Fib::new())
        .with_dependencies(merged);
    let sink = flow
        .commit_task("Multiply", config_tasks::Multiply::new())
        .with_dependencies(fib);

    flow.run(sink).await.expect("manual flow execution failed")
}

/// Test/mocking path: build a custom `FlowContext` (ignoring the inventory
/// registration entirely) and inject it via `Flow::with_context`. Here we
/// override the `multiplier_config` singleton with a higher factor to show
/// that the same `Multiply` task picks up whatever ctx the flow is bound to.
async fn run_flow_with_custom_ctx() -> u64 {
    let mut ctx = FlowContext::new();
    ctx.insert_singleton(
        "multiplier_config",
        config_tasks::MultiplierConfig { factor: 100 },
    );
    ctx.insert_factory("request_id", config_tasks::RequestId::new);

    let mut flow = Flow::with_context(Arc::new(ctx));

    let left = flow.commit_source_task("FibSource1", config_tasks::FibSource1::new());
    let right = flow.commit_source_task("FibSource2", config_tasks::FibSource2::new());
    let merged = flow
        .commit_task("Merger", config_tasks::Merger::new())
        .with_dependencies((left, right));
    let fib = flow
        .commit_task("Fib", config_tasks::Fib::new())
        .with_dependencies(merged);
    let sink = flow
        .commit_task("Multiply", config_tasks::Multiply::new())
        .with_dependencies(fib);

    flow.run(sink).await.expect("custom-ctx flow failed")
}

#[tokio::main]
async fn main() {
    let first_path = GENERATED_FLOW_PATHS
        .first()
        .copied()
        .expect("no flow configured in configs/flows.toml");

    // mode 1: build first, run later via sink id
    let (mut flow, sink_task_id) = build_flow_by_path(first_path)
        .expect("missing generated flow builder for first configured path");
    let output_any = flow
        .run_with_sink_id(sink_task_id)
        .await
        .expect("flow execution with sink_id failed");
    let output = expect_u64(&output_any, "run_with_sink_id");
    println!("[sink_id] {first_path} => {output}");

    // mode 2: run directly by path
    let second_path = GENERATED_FLOW_PATHS
        .get(1)
        .copied()
        .expect("second flow not configured in configs/flows.toml");
    let direct_output_any = run_flow_by_path(second_path)
        .await
        .expect("direct path run failed");
    let direct_output = expect_u64(&direct_output_any, "run_flow_by_path");
    println!("[direct] {second_path} => {direct_output}");

    // mode 3: construct graph manually; FlowContext auto-populated from inventory
    let manual_output = run_manual_flow().await;
    println!("[manual] FibSource1+FibSource2 -> Merger -> Fib -> Multiply => {manual_output}");

    // mode 4: inject a custom FlowContext overriding the inventory defaults
    let custom_output = run_flow_with_custom_ctx().await;
    println!("[custom-ctx] same pipeline with factor=100 => {custom_output}");
}
