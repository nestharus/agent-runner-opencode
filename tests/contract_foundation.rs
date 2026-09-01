//! Declared roles: orchestration, validator, parser, formatter, accessor, mapper, filter, predicate

mod support;

use jsonschema::{Draft, JSONSchema};
use serde_json::{json, Value};
use std::process::Command;
use support::{
    assert_valid, host_context, invoke, invoke_raw_stdin, invoke_with_host, invoke_with_request,
    json_stdout, CONTRACT,
};

#[test]
fn describe_response_conforms_and_sets_opencode_identity() {
    let output = invoke("describe", json!({}));
    assert_success(&output, "describe");
    let response = json_stdout(&output);
    assert_describe_response(&response);
}

#[test]
fn describe_advertises_prompt_acceptance_only_when_selected_by_the_host() {
    let unselected = json_stdout(&invoke("describe", json!({})));
    assert!(unselected["result"]["capabilities"]
        .get("prompt_acceptance_v1")
        .is_none());

    let declined = json_stdout(&invoke_with_host(
        "describe",
        json!({}),
        json!({"env": {"OULIPOLY_HOST_PROMPT_ACCEPTANCE_V1": "0"}}),
    ));
    assert!(declined["result"]["capabilities"]
        .get("prompt_acceptance_v1")
        .is_none());

    let selected = json_stdout(&invoke_with_host(
        "describe",
        json!({}),
        json!({"env": {"OULIPOLY_HOST_PROMPT_ACCEPTANCE_V1": "1"}}),
    ));
    assert_eq!(
        selected["result"]["capabilities"]["prompt_acceptance_v1"],
        true
    );
    assert_describe_response(&selected);
}

#[test]
fn describe_advertises_launch_output_only_when_selected_by_the_host() {
    let legacy = json_stdout(&invoke("describe", json!({})));
    assert!(legacy["result"]["capabilities"]
        .get("launch_output_v1")
        .is_none());

    let selected = json_stdout(&invoke_with_host(
        "describe",
        json!({}),
        json!({"env": {"OULIPOLY_HOST_LAUNCH_OUTPUT_V1": "1"}}),
    ));
    assert_eq!(selected["result"]["capabilities"]["launch_output_v1"], true);
    assert_describe_response(&selected);
}

#[test]
fn describe_advertises_session_turn_pages_only_when_selected_by_the_host() {
    let unselected = json_stdout(&invoke("describe", json!({})));
    assert!(unselected["result"]["capabilities"]
        .get("session_turn_pages_v1")
        .is_none());

    let selected = json_stdout(&invoke_with_host(
        "describe",
        json!({}),
        json!({"env": {"OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"}}),
    ));
    assert_eq!(
        selected["result"]["capabilities"]["session_turn_pages_v1"],
        true
    );
    assert_describe_response(&selected);
}

#[test]
fn schema_response_conforms_and_returns_opencode_settings_v1() {
    let output = invoke("schema", json!({ "schema_id": "opencode.settings/v1" }));
    assert_success(&output, "schema");
    let response = json_stdout(&output);
    assert_settings_schema_response(&response);
}

#[test]
fn schema_response_exposes_native_identity_rebind_protocol() {
    let output = invoke(
        "schema",
        json!({ "schema_id": "opencode.native-identity-rebind/v1" }),
    );
    assert_success(&output, "native identity rebind schema");
    let response = json_stdout(&output);
    assert_valid(&response, "schema.schema.json#/$defs/SchemaResponse");
    assert_eq!(
        response["result"]["schema_id"],
        "opencode.native-identity-rebind/v1"
    );
    assert_eq!(
        response["result"]["schema"]["$id"],
        "https://schemas.oulipoly.dev/opencode/native-identity-rebind/v1"
    );
    assert_eq!(
        response["result"]["schema"]["x-protocol"],
        "opencode.native-identity-rebind/v1"
    );
    assert!(response["result"].get("ui").is_none());
}

#[test]
fn schema_response_exposes_rotation_decision_protocol() {
    let output = invoke(
        "schema",
        json!({ "schema_id": "opencode.rotation-decision/v1" }),
    );
    assert_success(&output, "rotation decision schema");
    let response = json_stdout(&output);
    assert_valid(&response, "schema.schema.json#/$defs/SchemaResponse");
    assert_eq!(
        response["result"]["schema_id"],
        "opencode.rotation-decision/v1"
    );
    assert_eq!(
        response["result"]["schema"]["$id"],
        "https://schemas.oulipoly.dev/opencode/rotation-decision/v1"
    );
    assert_eq!(
        response["result"]["schema"]["x-protocol"],
        "opencode.rotation-decision/v1"
    );
    assert!(response["result"].get("ui").is_none());
}

#[test]
fn pinned_contract_snapshot_matches_every_recorded_upstream_digest() {
    let mut checked = 0;
    for line in include_str!("../contract/v1/UPSTREAM.md").lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 2 || !fields[1].ends_with(".schema.json") {
            continue;
        }
        let bytes = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("contract/v1")
                .join(fields[1]),
        )
        .expect("read pinned contract schema");
        assert_eq!(
            agent_runner_opencode::encoding::sha256_hex(&bytes),
            fields[0],
            "{} diverged from the recorded Agent Runner snapshot",
            fields[1]
        );
        checked += 1;
    }
    assert_eq!(checked, 13, "all pinned contract schemas must be checked");
    let schema_count =
        std::fs::read_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("contract/v1"))
            .expect("read pinned contract directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.ends_with(".schema.json"))
            })
            .count();
    assert_eq!(schema_count, checked, "unrecorded contract schema found");
}

#[test]
fn every_advertised_model_settings_value_is_semantically_valid() {
    for values in schema_valid_settings_examples() {
        let output = invoke("settings.validate", json!({ "values": values }));
        assert_success(&output, "settings.validate advertised model");
        let response = json_stdout(&output);
        assert_eq!(response["result"]["valid"], true, "{response}");
        assert_eq!(response["result"]["diagnostics"], json!([]), "{response}");
    }
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
fn discovery_accounts_maps_native_opencode_auth() {
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
    let schema = compile_standalone_schema(&result["schema"]);
    assert_settings_schema_catalog(&result["schema"]);
    for values in schema_valid_settings_examples() {
        assert!(
            schema.is_valid(&values),
            "advertised schema must accept catalog settings value: {values}"
        );
    }
}

fn assert_embedded_schema_id_absolute(schema: &Value) {
    if let Some(schema_id) = schema.get("$id") {
        assert!(
            schema_id.as_str().is_some_and(|id| id.contains("://")),
            "embedded schema $id must be absolute when present"
        );
    }
}

fn compile_standalone_schema(schema: &Value) -> JSONSchema {
    JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(schema)
        .unwrap()
}

fn assert_settings_schema_catalog(schema: &Value) {
    let variants = schema["properties"]["model"]["oneOf"]
        .as_array()
        .expect("model oneOf");
    assert_eq!(variants.len(), 8);
    assert!(variants.iter().any(|variant| {
        variant["properties"]["selection"]["const"] == "requested"
            && variant["required"] == json!(["selection"])
    }));
    let tuples = variants
        .iter()
        .filter(|variant| variant["properties"].get("name").is_some())
        .map(|variant| {
            (
                variant["properties"]["name"]["const"]
                    .as_str()
                    .expect("model name"),
                variant["properties"]["provider_model"]["const"]
                    .as_str()
                    .expect("provider model"),
                variant["properties"]["variant"]["const"]
                    .as_str()
                    .expect("model variant"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(tuples, expected_model_variants());
    assert_eq!(
        schema["properties"]["model"]["default"],
        json!({
            "name": "gpt-high",
            "provider_model": "openai/gpt-5.6-sol",
            "variant": "high"
        })
    );
}

fn schema_valid_settings_examples() -> Vec<Value> {
    let mut examples = expected_model_variants()
        .into_iter()
        .map(|(name, provider_model, variant)| {
            json!({
                "provider": "opencode",
                "profile": "opencode1",
                "wrapper": "opencode1",
                "model": {
                    "name": name,
                    "provider_model": provider_model,
                    "variant": variant
                },
                "quota": {
                    "source": "opencode_auth",
                    "auth_path": "~/.local/share/opencode/auth.json",
                    "probe": "native_chatgpt_usage"
                },
                "launch": {
                    "format": "json",
                    "dangerously_skip_permissions": true
                }
            })
        })
        .collect::<Vec<_>>();
    let mut requested = examples[0].clone();
    requested["model"] = json!({ "selection": "requested" });
    examples.push(requested);
    examples
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
    assert_eq!(models.len(), 7);
    for (alias, provider_model, effort) in expected_model_variants() {
        assert_model_variant(models, alias, provider_model, effort);
    }
}

fn expected_model_variants() -> [(&'static str, &'static str, &'static str); 7] {
    [
        ("gpt-low", "openai/gpt-5.6-sol", "low"),
        ("gpt-medium", "openai/gpt-5.6-sol", "medium"),
        ("gpt-high", "openai/gpt-5.6-sol", "high"),
        ("gpt-xhigh", "openai/gpt-5.6-sol", "xhigh"),
        ("gpt-max", "openai/gpt-5.6-sol", "max"),
        ("gpt-luna-low", "openai/gpt-5.6-luna", "low"),
        ("gpt-luna-max", "openai/gpt-5.6-luna", "max"),
    ]
}

fn assert_model_variant(models: &[Value], alias: &str, provider_model: &str, effort: &str) {
    let model = find_by_field(models, "name", alias);
    assert_eq!(model["provider_model"], provider_model, "{alias}");
    assert_eq!(
        model["provider_args"],
        json!(["-m", provider_model, "--variant", effort]),
        "{alias} provider args"
    );
    assert_eq!(
        model["account_eligibility"], "uniform_all_declared_accounts",
        "{alias} account eligibility policy"
    );
    assert_eq!(
        model["eligible_accounts"],
        json!([
            "opencode1",
            "opencode2",
            "opencode3",
            "opencode4",
            "opencode5"
        ]),
        "{alias} eligible accounts"
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
        (
            "opencode1",
            1,
            "~/.local/share/opencode/auth.json",
            "opencode1",
            "b7590111",
        ),
        (
            "opencode2",
            2,
            "~/.opencode2/opencode/auth.json",
            "opencode2",
            "6dadfdf6",
        ),
        (
            "opencode3",
            3,
            "~/.opencode3/opencode/auth.json",
            "opencode3",
            "00d3e164",
        ),
        (
            "opencode4",
            4,
            "~/.opencode4/opencode/auth.json",
            "opencode4",
            "d2b0bb16",
        ),
        (
            "opencode5",
            5,
            "~/.opencode5/opencode/auth.json",
            "opencode5",
            "7aee8329",
        ),
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
    assert_eq!(account["quota_auth_path"], auth_path);
    assert_eq!(account["account_tag"], tag);
    assert_eq!(account["account_hash"], hash);

    let quota_source = &account["quota_source"];
    assert_eq!(quota_source["kind"], "opencode_auth");
    assert_eq!(quota_source["auth_path"], auth_path);
    assert_eq!(quota_source["probe"], "native_chatgpt_usage");
    assert_eq!(quota_source["account_tag"], tag);
    assert_eq!(quota_source["account_hash"], hash);
}
