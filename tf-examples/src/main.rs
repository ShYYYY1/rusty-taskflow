mod config_tasks;

include!(concat!(env!("OUT_DIR"), "/generated_typed_flows.rs"));

fn main() {
    let first_path = GENERATED_FLOW_PATHS
        .first()
        .copied()
        .expect("no flow configured in configs/flow.toml");
    // build_typed_flow_by_path func is generated at compile time
    let _flow = build_typed_flow_by_path(first_path)
        .expect("missing generated flow builder for configured path");

    println!("typed flow builders generated for {first_path}");
}
