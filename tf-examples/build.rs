use std::{env, path::PathBuf};

// auto run before compilation

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is not set"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is not set"));

    let index_path = env::var("TASKFLOW_FLOW_INDEX_PATH")
        .map(PathBuf::from)
        .map(|path| if path.is_absolute() { path } else { manifest_dir.join(path) })
        .unwrap_or_else(|_| manifest_dir.join("configs/flow.toml"));

    println!("cargo:rerun-if-env-changed=TASKFLOW_FLOW_INDEX_PATH");
    println!("cargo:rerun-if-changed={}", index_path.display());

    taskflow_build::generate(&index_path, &manifest_dir, &out_dir)
        .unwrap_or_else(|err| panic!("failed to generate typed flow builders: {err}"));
}
