mod config_tasks;

include!(concat!(env!("OUT_DIR"), "/generated_typed_flows.rs"));

#[tokio::main]
async fn main() {
    let first_path = GENERATED_FLOW_PATHS
        .first()
        .copied()
        .expect("no flow configured in configs/flow.toml");

    let (mut flow, sink_task_id) = build_flow_by_path(first_path)
        .expect("missing generated flow builder for configured path");

    // run flow by sink_id
    let output_any = flow
        .run_with_sink_id(sink_task_id)
        .await
        .expect("flow execution failed");
    let output = output_any
        .downcast_ref::<u64>()
        .copied()
        .expect("unexpected sink output type");

    // run flow directly
    let directly_run_output = run_flow_by_path(first_path).await;

    println!("typed flow output for {first_path}: {output}");
}
