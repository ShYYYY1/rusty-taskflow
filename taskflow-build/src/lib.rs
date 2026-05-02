pub mod macros;

use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque}, env, fs, path::{Path, PathBuf}
};

#[derive(Debug, Deserialize)]
struct FlowPathIndex {
    flow_path: FlowPathList,
}

#[derive(Debug, Deserialize)]
struct FlowPathList {
    paths: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FlowFile {
    flow: FlowConfig,
}

#[derive(Debug, Deserialize)]
struct FlowConfig {
    source: Vec<TaskConfig>,
    processor: Vec<TaskConfig>,
    sink: TaskConfig,
}

#[derive(Debug, Deserialize)]
struct TaskConfig {
    name: String,
    dependencies: Vec<String>,
    output: String,
    builder: String,
}

#[derive(Debug)]
struct Node {
    name: String,
    dependencies: Vec<String>,
    output: String,
    builder: String,
    is_source: bool,
    is_sink: bool,
}

fn to_unix(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn normalize_for_concat(manifest_dir: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(manifest_dir).unwrap_or(path);
    let relative_unix = to_unix(relative);
    if relative_unix.starts_with('/') {
        relative_unix
    } else {
        format!("/{relative_unix}")
    }
}

fn sanitize_ident(raw: &str) -> String {
    let mut buf = String::with_capacity(raw.len() + 8);
    buf.push_str("out_");
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            buf.push(ch.to_ascii_lowercase());
        } else {
            buf.push('_');
        }
    }
    if buf
        .chars()
        .last()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(false)
    {
        buf.push('_');
    }
    buf
}

fn topo_sort(nodes: &[Node]) -> Result<Vec<usize>, String> {
    let mut output_to_index = HashMap::new();
    for (idx, node) in nodes.iter().enumerate() {
        if output_to_index.insert(node.output.clone(), idx).is_some() {
            return Err(format!(
                "duplicate output '{}' for task '{}'",
                node.output, node.name
            ));
        }
    }

    let mut indegree = vec![0usize; nodes.len()];
    let mut graph = vec![Vec::<usize>::new(); nodes.len()];

    for (idx, node) in nodes.iter().enumerate() {
        for dep in &node.dependencies {
            let from = output_to_index.get(dep).ok_or_else(|| {
                format!(
                    "task '{}' references unknown dependency output '{}'",
                    node.name, dep
                )
            })?;
            graph[*from].push(idx);
            indegree[idx] += 1;
        }
    }

    let mut queue = VecDeque::new();
    for (idx, deg) in indegree.iter().enumerate() {
        if *deg == 0 {
            queue.push_back(idx);
        }
    }

    let mut order = Vec::with_capacity(nodes.len());
    while let Some(cur) = queue.pop_front() {
        order.push(cur);
        for next in &graph[cur] {
            indegree[*next] -= 1;
            if indegree[*next] == 0 {
                queue.push_back(*next);
            }
        }
    }

    if order.len() != nodes.len() {
        return Err("flow dependency graph has cycle".to_string());
    }

    Ok(order)
}

fn render_flow_builder(func_name: &str, nodes: &[Node]) -> Result<String, String> {
    for node in nodes {
        if node.builder.trim().is_empty() {
            return Err(format!("task '{}' has empty builder expression", node.name));
        }
        if node.is_source && !node.dependencies.is_empty() {
            return Err(format!(
                "source task '{}' must not have dependencies",
                node.name
            ));
        }
        if !node.is_source && node.dependencies.is_empty() {
            return Err(format!(
                "non-source task '{}' must have at least one dependency",
                node.name
            ));
        }
    }

    let mut output_to_var = HashMap::new();
    let mut seen_vars = HashMap::new();
    for node in nodes {
        let sanitized = sanitize_ident(&node.output);
        if let Some(prev) = seen_vars.insert(sanitized.clone(), &node.output) {
            return Err(format!(
                "outputs '{}' and '{}' produce the same variable name '{sanitized}'",
                prev, node.output
            ));
        }
        output_to_var.insert(node.output.clone(), sanitized);
    }

    let order = topo_sort(nodes)?;

    let mut sink_var_name: Option<String> = None;
    let mut body = String::new();
    body.push_str(&format!(
        "fn {func_name}() -> (taskflow::tf::flow::Flow, taskflow::tf::flow::TaskId) {{\n    let mut flow = taskflow::tf::flow::Flow::new();\n"
    ));

    for idx in order {
        let node = &nodes[idx];
        let var_name = output_to_var
            .get(&node.output)
            .expect("output variable must exist");
        if node.is_source {
            body.push_str(&format!(
                "    let {var_name} = flow.commit_source_task(\"{}\", {});\n",
                node.name, node.builder
            ));
            continue;
        }

        let dependency_vars = node
            .dependencies
            .iter()
            .map(|dep| {
                output_to_var.get(dep).cloned().ok_or_else(|| {
                    format!(
                        "task '{}' references unknown dependency output '{}'",
                        node.name, dep
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let deps_expr = if dependency_vars.len() == 1 {
            dependency_vars[0].clone()
        } else {
            format!("({})", dependency_vars.join(", "))
        };

        body.push_str(&format!(
            "    let {var_name} = flow.commit_task(\"{}\", {}).with_dependencies({deps_expr});\n",
            node.name, node.builder
        ));

        if node.is_sink {
            sink_var_name = Some(var_name.clone());
        }
    }

    let sink_var_name = sink_var_name.ok_or_else(|| "sink task not found in flow nodes".to_string())?;
    body.push_str(&format!(
        "    let sink_task_id = {sink_var_name}.id.clone();\n    (flow, sink_task_id)\n}}\n"
    ));
    Ok(body)
}

fn render_flow_runner(func_name: &str, nodes: &[Node]) -> Result<String, String> {
    let mut output_to_var = HashMap::new();
    for node in nodes {
        output_to_var.insert(node.output.clone(), sanitize_ident(&node.output));
    }

    let order = topo_sort(nodes)?;
    let mut sink_var_name: Option<String> = None;

    let mut body = String::new();
    body.push_str(&format!(
        "async fn run_{func_name}() -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, taskflow::tf::errors::FlowError> {{\n    let mut flow = taskflow::tf::flow::Flow::new();\n"
    ));

    for idx in order {
        let node = &nodes[idx];
        let var_name = output_to_var
            .get(&node.output)
            .expect("output variable must exist");
        if node.is_source {
            body.push_str(&format!(
                "    let {var_name} = flow.commit_source_task(\"{}\", {});\n",
                node.name, node.builder
            ));
            continue;
        }

        let dependency_vars = node
            .dependencies
            .iter()
            .map(|dep| {
                output_to_var.get(dep).cloned().ok_or_else(|| {
                    format!(
                        "task '{}' references unknown dependency output '{}'",
                        node.name, dep
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let deps_expr = if dependency_vars.len() == 1 {
            dependency_vars[0].clone()
        } else {
            format!("({})", dependency_vars.join(", "))
        };

        body.push_str(&format!(
            "    let {var_name} = flow.commit_task(\"{}\", {}).with_dependencies({deps_expr});\n",
            node.name, node.builder
        ));

        if node.is_sink {
            sink_var_name = Some(var_name.clone());
        }
    }

    let sink_var_name = sink_var_name.ok_or_else(|| "sink task not found in flow nodes".to_string())?;
    body.push_str(&format!(
        "    let output = flow.run({sink_var_name}).await?;\n    Ok(std::sync::Arc::new(output) as std::sync::Arc<dyn std::any::Any + Send + Sync>)\n}}\n"
    ));

    Ok(body)
}

pub fn generate(index_path: &Path, manifest_dir: &Path, out_dir: &Path) -> Result<PathBuf, String> {
    let index_raw = fs::read_to_string(index_path)
        .map_err(|e| format!("failed to read flow index file {}: {e}", index_path.display()))?;
    let index: FlowPathIndex = toml::from_str(&index_raw)
        .map_err(|e| format!("failed to parse flow index {}: {e}", index_path.display()))?;

    let index_dir = index_path
        .parent()
        .ok_or_else(|| format!("flow index path has no parent: {}", index_path.display()))?;

    let mut path_entries = Vec::new();
    let mut match_arms = Vec::new();
    let mut run_match_arms = Vec::new();
    let mut builders = Vec::new();
    let mut runners = Vec::new();

    for (flow_idx, configured) in index.flow_path.paths.iter().enumerate() {
        let resolved = index_dir.join(configured);
        println!("cargo:rerun-if-changed={}", resolved.display());

        let flow_raw = fs::read_to_string(&resolved)
            .map_err(|e| format!("failed to read flow file {}: {e}", resolved.display()))?;
        let flow_file: FlowFile = toml::from_str(&flow_raw)
            .map_err(|e| format!("failed to parse {}: {e}", resolved.display()))?;

        let mut nodes = Vec::new();
        for task in flow_file.flow.source {
            nodes.push(Node {
                name: task.name,
                dependencies: task.dependencies,
                output: task.output,
                builder: task.builder,
                is_source: true,
                is_sink: false,
            });
        }
        for task in flow_file.flow.processor {
            nodes.push(Node {
                name: task.name,
                dependencies: task.dependencies,
                output: task.output,
                builder: task.builder,
                is_source: false,
                is_sink: false,
            });
        }
        nodes.push(Node {
            name: flow_file.flow.sink.name,
            dependencies: flow_file.flow.sink.dependencies,
            output: flow_file.flow.sink.output,
            builder: flow_file.flow.sink.builder,
            is_source: false,
            is_sink: true,
        });

        let func_name = format!("build_flow_{flow_idx}");
        let builder_src = render_flow_builder(&func_name, &nodes)
            .map_err(|e| format!("{}: {e}", resolved.display()))?;
        builders.push(builder_src);

        let runner_src = render_flow_runner(&func_name, &nodes)
            .map_err(|e| format!("{}: {e}", resolved.display()))?;
        runners.push(runner_src);

        let normalized = normalize_for_concat(manifest_dir, &resolved);
        let path_expr = format!("concat!(env!(\"CARGO_MANIFEST_DIR\"), \"{normalized}\")");
        path_entries.push(format!("    {path_expr}"));
        match_arms.push(format!("        {path_expr} => Some({func_name}()),"));
        run_match_arms.push(format!("        {path_expr} => run_{func_name}().await,"));
    }

    let generated = format!(
        "// @generated by taskflow-build. Do not edit manually.\n\
pub const GENERATED_FLOW_PATHS: &[&str] = &[\n{}\n];\n\
\n\
pub fn build_flow_by_path(path: &str) -> Option<(taskflow::tf::flow::Flow, taskflow::tf::flow::TaskId)> {{\n    match path {{\n{}\n        _ => None,\n    }}\n}}\n\
\n\
pub async fn run_flow_by_path(path: &str) -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, taskflow::tf::errors::FlowError> {{\n    match path {{\n{}\n        _ => Err(taskflow::tf::errors::FlowError::ConfigBuildError(format!(\"flow path '{{}}' is not generated\", path))),\n    }}\n}}\n\n{}\n\n{}\n",
        path_entries.join(",\n"),
        match_arms.join("\n"),
        run_match_arms.join("\n"),
        builders.join("\n"),
        runners.join("\n")
    );


    let out_file = out_dir.join("generated_typed_flows.rs");
    fs::write(&out_file, generated)
        .map_err(|e| format!("failed to write generated typed flow file {}: {e}", out_file.display()))?;
    Ok(out_file)
}

pub fn run_with_default() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is not set"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is not set"));

    // use CARGO_MANIFEST_DIR/configs/flows.toml by default
    let index_path = manifest_dir.join("configs/flows.toml");

    println!("cargo:rerun-if-env-changed=TASKFLOW_FLOW_INDEX_PATH");
    println!("cargo:rerun-if-changed={}", index_path.display());

    generate(&index_path, &manifest_dir, &out_dir)
        .unwrap_or_else(|err| panic!("failed to generate typed flow builders: {err}"));
}

pub fn run_with_env(env_key: &str) {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is not set"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is not set"));
    let env_path = env::var(env_key).expect(format!("{env_key} is not set").as_str());
    let index_path = PathBuf::from(env_path);
    let index_path = if index_path.is_absolute() { index_path } else { manifest_dir.join(index_path) };

    println!("cargo:rerun-if-env-changed=TASKFLOW_FLOW_INDEX_PATH");
    println!("cargo:rerun-if-changed={}", index_path.display());

    generate(&index_path, &manifest_dir, &out_dir)
        .unwrap_or_else(|err| panic!("failed to generate typed flow builders: {err}"));
}