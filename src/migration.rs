//! Declared roles: mapper, validator

use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use serde_json::{json, Value};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub fn plan_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    Ok(json!({
        "actions": planned_actions(&params),
        "warnings": migration_warnings(&params),
        "requires_backup": params.get("live_config_root").and_then(Value::as_str).is_some(),
        "confirmation": {
            "required": true,
            "reason": "migration.apply writes provider-owned artifacts only and does not cut over live gpt-* routes"
        }
    }))
}

pub fn apply_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    ensure_confirmation(&params, request_id)?;
    let artifact_root = artifact_root(host, &params, request_id)?;
    fs::create_dir_all(&artifact_root).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "migration_artifact_dir_failed",
            format!("failed to create provider-owned artifact directory: {err}"),
        )
    })?;
    let actions = planned_actions(&params);
    let summary = artifact_summary(&params, &actions);
    let path = artifact_root.join("opencode-provider-migration-summary.json");
    write_artifact(&path, &summary, request_id)?;
    let bytes = fs::read(&path).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "migration_artifact_read_failed",
            format!("failed to read provider-owned artifact: {err}"),
        )
    })?;
    Ok(json!({
        "applied_actions": actions,
        "artifacts": [{"kind": "file", "path": path.to_string_lossy(), "sha256": sha256_hex(&bytes)}],
        "warnings": migration_warnings(&params),
        "outcome": {
            "status": "provider_artifacts_written",
            "live_cutover": false,
            "artifact_root": artifact_root.to_string_lossy()
        }
    }))
}

fn planned_actions(params: &Value) -> Vec<Value> {
    vec![
        json!({
            "kind": "analyze_legacy_opencode",
            "target_provider": string_param(params, "target_provider").unwrap_or("agent-runner-opencode"),
            "scope": string_param(params, "scope").unwrap_or("provider_owned"),
        }),
        json!({
            "kind": "write_provider_owned_artifact",
            "artifact": "opencode-provider-migration-summary.json",
        }),
    ]
}

fn migration_warnings(params: &Value) -> Vec<Value> {
    let mut warnings = vec![json!(
        "live providers.toml and gpt-* model TOML cutover is intentionally not performed"
    )];
    if string_param(params, "scope") != Some("provider_owned") {
        warnings.push(json!(
            "non-provider-owned scope requested; provider will still emit artifacts only"
        ));
    }
    warnings
}

fn ensure_confirmation(params: &Value, request_id: &str) -> Result<(), ProviderFailure> {
    if params
        .pointer("/confirmation/approved")
        .and_then(Value::as_bool)
        == Some(true)
    {
        return Ok(());
    }
    Err(ProviderFailure::invalid_request(
        request_id,
        "migration_confirmation_required",
        "migration.apply requires confirmation.approved=true",
    ))
}

fn artifact_root(
    host: &HostContext,
    params: &Value,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    if let Some(root) = string_param(params, "artifact_root") {
        return Ok(PathBuf::from(root));
    }
    let Some(config_root) = host
        .config_root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
    else {
        return Err(ProviderFailure::invalid_request(
            request_id,
            "missing_artifact_root",
            "migration.apply requires params.artifact_root or host.config_root",
        ));
    };
    Ok(PathBuf::from(config_root).join("opencode-provider-migration-artifacts"))
}

fn artifact_summary(params: &Value, actions: &[Value]) -> Value {
    json!({
        "schema": "opencode.provider_migration/v1",
        "target_provider": string_param(params, "target_provider").unwrap_or("agent-runner-opencode"),
        "scope": string_param(params, "scope").unwrap_or("provider_owned"),
        "legacy": legacy_summary(params.get("legacy").unwrap_or(&Value::Null)),
        "actions": actions,
        "live_cutover": false,
    })
}

fn legacy_summary(legacy: &Value) -> Value {
    json!({
        "has_providers_toml": legacy.get("providers_toml").and_then(Value::as_str).is_some(),
        "model_count": legacy.get("models").and_then(Value::as_object).map(|models| models.len()).unwrap_or(0),
    })
}

fn write_artifact(path: &PathBuf, value: &Value, request_id: &str) -> Result<(), ProviderFailure> {
    let bytes = serde_json::to_vec(value).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "migration_artifact_serialize_failed",
            format!("failed to serialize migration artifact: {err}"),
        )
    })?;
    let mut file = fs::File::create(path).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "migration_artifact_create_failed",
            format!("failed to create provider-owned migration artifact: {err}"),
        )
    })?;
    file.write_all(&bytes).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "migration_artifact_write_failed",
            format!("failed to write provider-owned migration artifact: {err}"),
        )
    })
}

fn string_param<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}
