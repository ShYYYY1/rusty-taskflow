mod config_tasks;

use taskflow::tf::flow::Flow;

// add generated .rs to compile
include!(concat!(env!("OUT_DIR"), "/generated_typed_flows.rs"));

fn expect_u64(output: &std::sync::Arc<dyn std::any::Any + Send + Sync>, context: &str) -> u64 {
    output
        .downcast_ref::<u64>()
        .copied()
        .unwrap_or_else(|| panic!("{context}: unexpected sink output type, expected u64"))
}

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

    // mode 3: construct graph manually in Rust code
    let manual_output = run_manual_flow().await;
    println!("[manual] FibSource1+FibSource2 -> Merger -> Fib -> Multiply => {manual_output}");
}

