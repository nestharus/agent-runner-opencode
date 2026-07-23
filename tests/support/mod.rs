//! Declared roles: orchestration, validator, parser, formatter, accessor, mapper, filter, predicate

use jsonschema::{Draft, JSONSchema};
use serde_json::{json, Map, Value};
use std::io::Write;
use std::process::{Command, Output, Stdio};

pub const CONTRACT: &str = "oulipoly.provider/v1";

pub fn invoke(subcommand: &str, params: Value) -> Output {
    invoke_with_host(subcommand, params, json!({}))
}

#[allow(dead_code)]
pub fn invoke_with_env(subcommand: &str, params: Value, env: &[(&str, &str)]) -> Output {
    let request = request_envelope(subcommand, params, json!({}));
    invoke_with_request_and_env(subcommand, request, env)
}

pub fn invoke_with_host(subcommand: &str, params: Value, host_overrides: Value) -> Output {
    let request = request_envelope(subcommand, params, host_overrides);
    invoke_with_request(subcommand, request)
}

#[allow(dead_code)]
pub fn invoke_with_host_and_env(
    subcommand: &str,
    params: Value,
    host_overrides: Value,
    env: &[(&str, &str)],
) -> Output {
    let request = request_envelope(subcommand, params, host_overrides);
    invoke_with_request_and_env(subcommand, request, env)
}

#[allow(dead_code)]
pub fn invoke_validated(subcommand: &str, params: Value, request_schema: &str) -> Output {
    invoke_validated_with_host(subcommand, params, json!({}), request_schema)
}

#[allow(dead_code)]
pub fn invoke_validated_with_host(
    subcommand: &str,
    params: Value,
    host_overrides: Value,
    request_schema: &str,
) -> Output {
    let request = validated_request_envelope(subcommand, params, host_overrides, request_schema);
    invoke_with_request(subcommand, request)
}

#[allow(dead_code)]
pub fn invoke_validated_with_host_and_env(
    subcommand: &str,
    params: Value,
    host_overrides: Value,
    request_schema: &str,
    env: &[(&str, &str)],
) -> Output {
    let request = validated_request_envelope(subcommand, params, host_overrides, request_schema);
    invoke_with_request_and_env(subcommand, request, env)
}

pub fn invoke_with_request(subcommand: &str, request_json: Value) -> Output {
    let stdin = request_stdin_bytes(&request_json);
    invoke_raw_stdin(subcommand, &stdin)
}

#[allow(dead_code)]
pub fn invoke_with_request_and_env(
    subcommand: &str,
    request_json: Value,
    env: &[(&str, &str)],
) -> Output {
    let stdin = request_stdin_bytes(&request_json);
    invoke_raw_stdin_with_env(subcommand, &stdin, env)
}

pub fn invoke_raw_stdin(subcommand: &str, stdin_bytes: &[u8]) -> Output {
    invoke_raw_stdin_with_env(subcommand, stdin_bytes, &[])
}

pub fn invoke_raw_stdin_with_env(
    subcommand: &str,
    stdin_bytes: &[u8],
    env: &[(&str, &str)],
) -> Output {
    let mut child = spawn_provider(subcommand, env);
    write_provider_stdin(&mut child, stdin_bytes);
    wait_provider(child)
}

pub fn request_envelope(subcommand: &str, params: Value, host_overrides: Value) -> Value {
    json!({
        "contract": CONTRACT,
        "request_id": format!("req-{subcommand}"),
        "provider_instance_id": "opencode-primary",
        "host": host_context(host_overrides),
        "params": params
    })
}

pub fn validated_request_envelope(
    subcommand: &str,
    params: Value,
    host_overrides: Value,
    request_schema: &str,
) -> Value {
    let request = request_envelope(subcommand, params, host_overrides);
    assert_valid_request_envelope(&request, request_schema);
    request
}

pub fn assert_valid_request_envelope(request: &Value, request_schema: &str) {
    assert_valid(request, request_schema);
}

pub fn host_context(host_overrides: Value) -> Value {
    let mut host = json!({
        "app": "oulipoly-agent-runner",
        "app_version": "0.0.0",
        "platform": "linux-x86_64",
        "working_directory": "/tmp",
        "config_root": "/tmp/config",
        "data_root": "/tmp/data",
        "env": { "TERM": "xterm-256color" }
    });
    if let (Some(host), Some(overrides)) = (host.as_object_mut(), host_overrides.as_object()) {
        for (key, value) in overrides {
            host.insert(key.clone(), value.clone());
        }
    }
    host
}

pub fn json_stdout(output: &Output) -> Value {
    assert_stderr_diagnostics_only(output);
    assert_stdout_present(output);
    parse_stdout_json(&output.stdout)
}

pub fn assert_stderr_diagnostics_only(output: &Output) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains(CONTRACT),
        "stderr must be diagnostics-only, not a contract stream: {stderr}"
    );
}

pub fn compile_contract_ref(schema_file: &str, def_name: &str) -> JSONSchema {
    let common = common_schema_doc();
    let schema_doc = contract_schema_doc(schema_file);
    let mut root = bundled_contract_schema(common, schema_doc, def_name);

    rewrite_external_refs(&mut root);
    compile_json_schema(&root)
}

pub fn bundled_contract_schema(common: Value, schema_doc: Value, def_name: &str) -> Value {
    let mut defs = common["$defs"].as_object().unwrap().clone();
    for (key, value) in schema_doc["$defs"].as_object().unwrap() {
        defs.insert(key.clone(), value.clone());
    }

    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": defs,
        "$ref": format!("#/$defs/{def_name}")
    })
}

pub fn rewrite_external_refs(value: &mut Value) {
    match value {
        Value::Object(map) => {
            rewrite_object_external_ref(map);
            for child in map.values_mut() {
                rewrite_external_refs(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_external_refs(item);
            }
        }
        _ => {}
    }
}

fn rewrite_object_external_ref(map: &mut Map<String, Value>) {
    if let Some(reference) = external_ref_string_mut(map) {
        rewrite_external_ref_string(reference);
    }
}

fn external_ref_string_mut(map: &mut Map<String, Value>) -> Option<&mut String> {
    match map.get_mut("$ref") {
        Some(Value::String(reference)) => Some(reference),
        _ => None,
    }
}

fn rewrite_external_ref_string(reference: &mut String) {
    if let Some(replacement) = external_ref_replacement(reference) {
        *reference = replacement;
    }
}

struct ExternalRef<'a> {
    document: &'a str,
    def_path: &'a str,
}

fn external_ref_replacement(reference: &str) -> Option<String> {
    let parsed = parse_external_ref(reference)?;
    if !is_schema_external_ref(&parsed) {
        return None;
    }
    Some(local_def_ref(parsed.def_path))
}

fn parse_external_ref(reference: &str) -> Option<ExternalRef<'_>> {
    reference
        .split_once("#/$defs/")
        .map(|(document, def_path)| ExternalRef { document, def_path })
}

fn is_schema_external_ref(reference: &ExternalRef<'_>) -> bool {
    reference.document.ends_with(".schema.json")
}

fn local_def_ref(def_path: &str) -> String {
    format!("#/$defs/{def_path}")
}

pub fn assert_valid(value: &Value, schema_id: &str) {
    let (schema_file, def_name) = schema_file_and_def(schema_id);
    let schema = compile_contract_ref(schema_file, def_name);
    if let Err(errors) = schema.validate(value) {
        contract_validation_failed(schema_id, validation_error_details(errors), value);
    };
}

fn request_stdin_bytes(request_json: &Value) -> Vec<u8> {
    request_json.to_string().into_bytes()
}

fn spawn_provider(subcommand: &str, env: &[(&str, &str)]) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_agent-runner-opencode"))
        .arg(subcommand)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_clear()
        .envs(env.iter().copied())
        .spawn()
        .unwrap()
}

fn write_provider_stdin(child: &mut std::process::Child, stdin_bytes: &[u8]) {
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(stdin_bytes)
        .unwrap();
}

fn wait_provider(child: std::process::Child) -> Output {
    child.wait_with_output().unwrap()
}

fn assert_stdout_present(output: &Output) {
    assert!(
        !output.stdout.is_empty(),
        "stdout must contain one contract JSON envelope; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn parse_stdout_json(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).unwrap()
}

fn common_schema_doc() -> Value {
    serde_json::from_str(include_str!("../../contract/v1/common.schema.json")).unwrap()
}

fn contract_schema_doc(schema_file: &str) -> Value {
    serde_json::from_str(contract_schema_text(schema_file)).unwrap()
}

fn contract_schema_text(schema_file: &str) -> &'static str {
    match schema_file {
        "common.schema.json" => include_str!("../../contract/v1/common.schema.json"),
        "describe.schema.json" => include_str!("../../contract/v1/describe.schema.json"),
        "schema.schema.json" => include_str!("../../contract/v1/schema.schema.json"),
        "discovery.schema.json" => include_str!("../../contract/v1/discovery.schema.json"),
        "settings.schema.json" => include_str!("../../contract/v1/settings.schema.json"),
        "setup.schema.json" => include_str!("../../contract/v1/setup.schema.json"),
        "policy.schema.json" => include_str!("../../contract/v1/policy.schema.json"),
        "terminal.schema.json" => include_str!("../../contract/v1/terminal.schema.json"),
        "launch.schema.json" => include_str!("../../contract/v1/launch.schema.json"),
        "quota.schema.json" => include_str!("../../contract/v1/quota.schema.json"),
        "session.schema.json" => include_str!("../../contract/v1/session.schema.json"),
        "rotation.schema.json" => include_str!("../../contract/v1/rotation.schema.json"),
        "migration.schema.json" => include_str!("../../contract/v1/migration.schema.json"),
        other => panic!("unhandled schema file: {other}"),
    }
}

fn compile_json_schema(root: &Value) -> JSONSchema {
    JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(root)
        .unwrap()
}

fn schema_file_and_def(schema_id: &str) -> (&str, &str) {
    schema_id
        .split_once("#/$defs/")
        .unwrap_or_else(|| panic!("schema id must be file.schema.json#/$defs/Name: {schema_id}"))
}

fn validation_error_details<'a>(
    errors: impl Iterator<Item = jsonschema::ValidationError<'a>>,
) -> String {
    errors
        .map(|err| err.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

fn contract_validation_failed(schema_id: &str, details: String, value: &Value) -> ! {
    panic!("contract validation failed for {schema_id}:\n{details}\nvalue:\n{value}");
}
