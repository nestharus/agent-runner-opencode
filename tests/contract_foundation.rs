//! Declared roles: orchestration, validator, parser, formatter, accessor, mapper, filter, predicate

mod support;

use jsonschema::{Draft, JSONSchema};
use serde_json::{json, Value};
use std::process::Command;
use support::{
    assert_valid, host_context, invoke, invoke_raw_stdin, invoke_with_request, json_stdout,
    CONTRACT,
};

#[test]
fn describe_response_conforms_and_sets_opencode_identity() {
    let output = invoke("describe", json!({}));
    assert_success(&output, "describe");
    let response = json_stdout(&output);
    assert_describe_response(&response);
}

#[test]
fn schema_response_conforms_and_returns_opencode_settings_v1() {
    let output = invoke("schema", json!({ "schema_id": "opencode.settings/v1" }));
    assert_success(&output, "schema");
    let response = json_stdout(&output);
    assert_settings_schema_response(&response);
}

#[test]
fn unknown_schema_id_returns_contract_error_envelope() {
    let output = invoke("schema", json!({ "schema_id": "unknown.settings/v1" }));
    assert_error_response(output, "unsupported", "unknown_schema");
}

#[test]
fn discovery_models_lists_gpt_variants() {
    let output = invoke("discovery.models", json!({}));
    assert_success(&output, "discovery.models");
    let response = json_stdout(&output);
    assert_discovery_models_response(&response);
}

#[test]
fn discovery_accounts_maps_shuffled_codex_auth() {
    let output = invoke("discovery.accounts", json!({}));
    assert_success(&output, "discovery.accounts");
    let response = json_stdout(&output);
    assert_discovery_accounts_response(&response);
}

#[test]
fn unknown_subcommand_returns_error_envelope() {
    assert_error_response(
        invoke("does.not_exist", json!({})),
        "unsupported",
        "unknown_subcommand",
    );
}

#[test]
fn invalid_json_stdin_returns_error_envelope() {
    assert_error_response(
        invoke_raw_stdin("describe", b"{not valid json"),
        "invalid_request",
        "invalid_json",
    );
}

#[test]
fn missing_params_returns_error_envelope() {
    let request = json!({
        "contract": CONTRACT,
        "request_id": "req-missing-params",
        "provider_instance_id": "opencode-primary",
        "host": host_context(json!({}))
    });
    assert_error_response(
        invoke_with_request("describe", request),
        "invalid_request",
        "missing_params",
    );
}

#[test]
fn invalid_request_envelope_returns_error_envelope() {
    let wrong_contract = json!({
        "contract": "oulipoly.provider/v0",
        "request_id": "req-wrong-contract",
        "provider_instance_id": "opencode-primary",
        "host": host_context(json!({})),
        "params": {}
    });
    assert_invalid_request_response(invoke_with_request("describe", wrong_contract));

    let missing_host = json!({
        "contract": CONTRACT,
        "request_id": "req-missing-host",
        "provider_instance_id": "opencode-primary",
        "params": {}
    });
    assert_invalid_request_response(invoke_with_request("describe", missing_host));

    let invalid_host = json!({
        "contract": CONTRACT,
        "request_id": "req-invalid-host",
        "provider_instance_id": "opencode-primary",
        "host": {
            "app": "",
            "unexpected": true
        },
        "params": {}
    });
    assert_invalid_request_response(invoke_with_request("describe", invalid_host));
}

#[test]
fn no_host_crate_linkage_excludes_oulipoly_provider() {
    let output = cargo_metadata_output();
    assert_cargo_metadata_success(&output);
    let metadata = cargo_metadata_json(&output.stdout);
    assert_no_host_crate_dependency(&metadata);
}

fn cargo_metadata_output() -> std::process::Output {
    Command::new(cargo_binary())
        .args(cargo_metadata_args())
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap()
}

fn cargo_binary() -> std::ffi::OsString {
    std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into())
}

fn cargo_metadata_args() -> [&'static str; 5] {
    ["metadata", "--format-version", "1", "--locked", "--offline"]
}

fn assert_cargo_metadata_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "cargo metadata failed {:?}; stderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_success(output: &std::process::Output, label: &str) {
    assert!(
        output.status.success(),
        "{label} exited {:?}; stderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_describe_response(response: &Value) {
    assert_valid(response, "describe.schema.json#/$defs/DescribeResponse");
    assert_valid(
        &response["result"],
        "describe.schema.json#/$defs/DescribeResult",
    );
    assert_describe_identity(&response["result"]);
}

fn assert_describe_identity(result: &Value) {
    assert_eq!(result["provider_id"], "opencode");
    assert_eq!(result["settings_schema_id"], "opencode.settings/v1");
    assert_eq!(result["capabilities"]["setup_brain"], false);
    for key in described_true_capabilities() {
        assert_eq!(result["capabilities"][key], true, "capability {key}");
    }
}

fn described_true_capabilities() -> [&'static str; 11] {
    [
        "launch",
        "policy",
        "quota",
        "session",
        "session_enumerate",
        "terminal",
        "rotation",
        "discovery",
        "settings",
        "setup",
        "migration",
    ]
}

fn assert_settings_schema_response(response: &Value) {
    assert_valid(response, "schema.schema.json#/$defs/SchemaResponse");
    assert_valid(
        &response["result"],
        "schema.schema.json#/$defs/SchemaResult",
    );
    let result = &response["result"];
    assert_eq!(result["schema_id"], "opencode.settings/v1");
    assert_embedded_schema_id_absolute(&result["schema"]);
    compile_standalone_schema(&result["schema"]);
}

fn assert_embedded_schema_id_absolute(schema: &Value) {
    if let Some(schema_id) = schema.get("$id") {
        assert!(
            schema_id.as_str().is_some_and(|id| id.contains("://")),
            "embedded schema $id must be absolute when present"
        );
    }
}

fn compile_standalone_schema(schema: &Value) {
    JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(schema)
        .unwrap();
}

fn assert_discovery_models_response(response: &Value) {
    assert_valid(
        response,
        "discovery.schema.json#/$defs/DiscoveryModelsResponse",
    );
    assert_valid(
        &response["result"],
        "discovery.schema.json#/$defs/DiscoveryModelsResult",
    );
    let models = response["result"]["models"]
        .as_array()
        .expect("models array");
    assert_eq!(models.len(), 5);
    for (alias, effort) in expected_model_variants() {
        assert_model_variant(models, alias, effort);
    }
}

fn expected_model_variants() -> [(&'static str, &'static str); 5] {
    [
        ("gpt-low", "low"),
        ("gpt-medium", "medium"),
        ("gpt-high", "high"),
        ("gpt-xhigh", "xhigh"),
        ("gpt-max", "max"),
    ]
}

fn assert_model_variant(models: &[Value], alias: &str, effort: &str) {
    let model = find_by_field(models, "name", alias);
    assert_eq!(model["provider_model"], "openai/gpt-5.6-sol", "{alias}");
    assert_eq!(
        model["provider_args"],
        json!(["-m", "openai/gpt-5.6-sol", "--variant", effort]),
        "{alias} provider args"
    );
}

fn assert_discovery_accounts_response(response: &Value) {
    assert_valid(
        response,
        "discovery.schema.json#/$defs/DiscoveryAccountsResponse",
    );
    assert_valid(
        &response["result"],
        "discovery.schema.json#/$defs/DiscoveryAccountsResult",
    );
    let accounts = response["result"]["accounts"]
        .as_array()
        .expect("accounts array");
    assert_eq!(accounts.len(), 5);
    for expected in expected_account_mappings() {
        assert_account_mapping(accounts, expected);
    }
}

fn expected_account_mappings() -> [(&'static str, u64, &'static str, &'static str, &'static str); 5]
{
    [
        ("opencode1", 1, "~/.codex/auth.json", "codex1", "781db66f"),
        ("opencode2", 2, "~/.codex5/auth.json", "codex5", "27f8ea6e"),
        ("opencode3", 3, "~/.codex2/auth.json", "codex2", "60238f0b"),
        ("opencode4", 4, "~/.codex3/auth.json", "codex3", "9d764739"),
        ("opencode5", 5, "~/.codex4/auth.json", "codex4", "835bbc4d"),
    ]
}

fn cargo_metadata_json(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).unwrap()
}

fn assert_no_host_crate_dependency(metadata: &Value) {
    let packages = metadata["packages"].as_array().expect("metadata packages");
    for package in packages {
        assert_no_host_package(package);
    }
}

fn assert_no_host_package(package: &Value) {
    assert_ne!(
        package["name"], "oulipoly-provider",
        "host crate oulipoly-provider must not be in the build graph: {package}"
    );
    assert_no_oulipoly_provider_path(&package["manifest_path"]);
    for dependency in package["dependencies"].as_array().into_iter().flatten() {
        assert_no_host_dependency(dependency);
    }
}

fn assert_no_host_dependency(dependency: &Value) {
    assert_ne!(
        dependency["name"], "oulipoly-provider",
        "host crate oulipoly-provider must not be declared as a dependency: {dependency}"
    );
    assert_no_oulipoly_provider_path(&dependency["path"]);
}

fn assert_error_response(output: std::process::Output, category: &str, code: &str) -> Value {
    assert_error_output(&output, category, code);
    let response = json_stdout(&output);
    assert_error_envelope(&response, category, code);
    response
}

fn assert_error_output(output: &std::process::Output, category: &str, code: &str) {
    assert!(
        !output.status.success(),
        "expected nonzero exit for {category}/{code}"
    );
}

fn assert_error_envelope(response: &Value, category: &str, code: &str) {
    assert_valid(response, "common.schema.json#/$defs/ErrorResponseEnvelope");
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["category"], category);
    assert_eq!(response["error"]["code"], code);
}

fn assert_invalid_request_response(output: std::process::Output) -> Value {
    assert_invalid_request_output(&output);
    let response = json_stdout(&output);
    assert_invalid_request_envelope(&response);
    response
}

fn assert_invalid_request_output(output: &std::process::Output) {
    assert!(
        !output.status.success(),
        "expected nonzero exit for invalid request envelope"
    );
}

fn assert_invalid_request_envelope(response: &Value) {
    assert_valid(response, "common.schema.json#/$defs/ErrorResponseEnvelope");
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["category"], "invalid_request");
}

fn assert_no_oulipoly_provider_path(value: &Value) {
    if let Some(path) = value.as_str() {
        assert!(
            !is_oulipoly_provider_path(&normalized_path(path)),
            "host crate oulipoly-provider path must not be in the build graph: {path}"
        );
    }
}

fn normalized_path(path: &str) -> String {
    path.replace('\\', "/")
}

fn is_oulipoly_provider_path(path: &str) -> bool {
    path.contains("/crates/oulipoly-provider")
}

fn find_by_field<'a>(items: &'a [Value], field: &str, expected: &str) -> &'a Value {
    find_optional_by_field(items, field, expected)
        .unwrap_or_else(|| panic!("missing item with {field}={expected}: {items:?}"))
}

fn find_optional_by_field<'a>(
    items: &'a [Value],
    field: &str,
    expected: &str,
) -> Option<&'a Value> {
    items
        .iter()
        .find(|item| item_field_matches(item, field, expected))
}

fn item_field_matches(item: &Value, field: &str, expected: &str) -> bool {
    item[field] == expected
}

fn assert_account_mapping(
    accounts: &[Value],
    (wrapper, index, auth_path, tag, hash): (&str, u64, &str, &str, &str),
) {
    let account = find_by_field(accounts, "id", wrapper);
    assert_eq!(account["opencode_wrapper"], wrapper);
    assert_eq!(account["opencode_index"], index);
    assert_eq!(account["codex_auth_path"], auth_path);
    assert_eq!(account["codex_account_tag"], tag);
    assert_eq!(account["codex_account_hash"], hash);

    let quota_source = &account["quota_source"];
    assert_eq!(quota_source["kind"], "codex_auth");
    assert_eq!(quota_source["auth_path"], auth_path);
    assert_eq!(quota_source["account_tag"], tag);
    assert_eq!(quota_source["account_hash"], hash);
}
