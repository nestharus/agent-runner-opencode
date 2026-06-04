//! Declared roles: mapper, validator

use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use serde_json::{json, Value};
use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};

const PROVIDER_DIR: &str = "agent-runner-opencode";
const MIGRATION_DIR: &str = "migration";
const LEGACY_PROVIDER_ARTIFACT_DIR: &str = "provider-owned-migration-artifacts";

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
    let config_root = config_root(host, request_id)?;
    let artifact_root = artifact_root(&config_root, &params, request_id)?;
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
    ensure_canonical_contained(&path, &config_root, request_id)?;
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
    config_root: &Path,
    params: &Value,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let allowed_roots = provider_owned_artifact_roots(config_root);
    if let Some(root) = string_param(params, "artifact_root") {
        let requested = requested_artifact_root(config_root, root, request_id)?;
        ensure_provider_owned_artifact_root(&requested, &allowed_roots, config_root, request_id)?;
        return Ok(requested);
    }
    ensure_provider_owned_artifact_root(
        &allowed_roots[0],
        &allowed_roots,
        config_root,
        request_id,
    )?;
    Ok(allowed_roots[0].clone())
}

fn config_root(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
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
    Ok(PathBuf::from(config_root))
}

fn provider_owned_artifact_roots(config_root: &Path) -> Vec<PathBuf> {
    vec![
        config_root.join(PROVIDER_DIR).join(MIGRATION_DIR),
        config_root.join(LEGACY_PROVIDER_ARTIFACT_DIR),
    ]
}

fn requested_artifact_root(
    config_root: &Path,
    root: &str,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let path = PathBuf::from(root);
    let requested = if path.is_absolute() {
        path
    } else {
        config_root.join(path)
    };
    if has_parent_component(&requested) || is_forbidden_live_route_path(&requested) {
        return Err(invalid_artifact_root(request_id));
    }
    Ok(requested)
}

fn ensure_provider_owned_artifact_root(
    requested: &Path,
    allowed_roots: &[PathBuf],
    config_root: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let requested = normalized_absolute_path(requested, request_id)?;
    for root in allowed_roots {
        let root = normalized_absolute_path(root, request_id)?;
        if requested.starts_with(&root) {
            ensure_canonical_contained(&requested, config_root, request_id)?;
            return Ok(());
        }
    }
    Err(invalid_artifact_root(request_id))
}

fn ensure_canonical_contained(
    requested: &Path,
    config_root: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let canonical_root = canonical_path(config_root, request_id)?;
    let canonical_requested = canonical_create_path(requested, request_id)?;
    if canonical_requested.starts_with(canonical_root) {
        return Ok(());
    }
    Err(invalid_artifact_root(request_id))
}

fn normalized_absolute_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str())
            }
            Component::CurDir => {}
            Component::ParentDir => return Err(invalid_artifact_root(request_id)),
        }
    }
    Ok(normalized)
}

fn canonical_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    fs::canonicalize(path).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "migration_artifact_root_canonicalize_failed",
            format!("failed to canonicalize migration artifact root: {err}"),
        )
    })
}

fn canonical_create_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let existing = existing_ancestor(path).ok_or_else(|| invalid_artifact_root(request_id))?;
    let mut canonical = canonical_path(existing, request_id)?;
    let suffix = path
        .strip_prefix(existing)
        .map_err(|_| invalid_artifact_root(request_id))?;
    for component in suffix.components() {
        match component {
            Component::Normal(part) => canonical.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err(invalid_artifact_root(request_id));
            }
        }
    }
    Ok(canonical)
}

fn existing_ancestor(path: &Path) -> Option<&Path> {
    path.ancestors().find(|ancestor| ancestor.exists())
}

fn has_parent_component(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}

fn is_forbidden_live_route_path(path: &Path) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some("providers.toml")
        || path
            .components()
            .any(|component| component.as_os_str() == "models")
        || path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("gpt-") && name.ends_with(".toml"))
}

fn invalid_artifact_root(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "artifact_root_outside_provider_root",
        "migration.apply artifact_root must stay under a provider-owned migration root",
    )
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
