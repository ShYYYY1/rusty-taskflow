use taskflow_build::build_main;

// Auto-run before compilation.
// Generates GENERATED_FLOW_PATHS and typed flow build/run helpers from config files.
// Default config path: CARGO_MANIFEST_DIR/configs/flows.toml, cause panic when path is invalid
build_main!();

// use env variable, cause panic when env variable not set
// build_main!(env = "TASKFLOW_FLOWS_INDEX_PATH");