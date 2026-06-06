//! Declared roles: mapper, validator, orchestration, accessor, formatter, predicate

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
    fs::create_dir_all(&artifact_root)
        .map_err(|err| migration_artifact_dir_failure(request_id, err))?;
    let actions = planned_actions(&params);
    let summary = artifact_summary(&params, &actions);
    let path = artifact_root.join("opencode-provider-migration-summary.json");
    ensure_canonical_contained(&path, &config_root, request_id)?;
    write_artifact(&path, &summary, request_id)?;
    let bytes = read_artifact(&path, request_id)?;
    Ok(migration_apply_result(
        actions,
        migration_apply_artifacts(&path, &bytes),
        migration_warnings(&params),
        &artifact_root,
    ))
}

fn migration_apply_result(
    applied_actions: Vec<Value>,
    artifacts: Vec<Value>,
    warnings: Vec<Value>,
    artifact_root: &Path,
) -> Value {
    json!({
        "applied_actions": applied_actions,
        "artifacts": artifacts,
        "warnings": warnings,
        "outcome": migration_apply_outcome(artifact_root)
    })
}

fn migration_apply_artifacts(path: &Path, bytes: &[u8]) -> Vec<Value> {
    vec![migration_apply_artifact(path, bytes)]
}

fn migration_apply_artifact(path: &Path, bytes: &[u8]) -> Value {
    json!({"kind": "file", "path": path_string(path), "sha256": sha256_hex(bytes)})
}

fn migration_apply_outcome(artifact_root: &Path) -> Value {
    json!({
        "status": "provider_artifacts_written",
        "live_cutover": false,
        "artifact_root": path_string(artifact_root)
    })
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
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
    Err(migration_confirmation_required_failure(request_id))
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
    let requested = requested_artifact_root_path(config_root, root);
    ensure_requested_artifact_root(&requested, request_id)?;
    Ok(requested)
}

fn requested_artifact_root_path(config_root: &Path, root: &str) -> PathBuf {
    let path = PathBuf::from(root);
    if path.is_absolute() {
        path
    } else {
        config_root.join(path)
    }
}

fn ensure_requested_artifact_root(
    requested: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if is_invalid_requested_artifact_root(requested) {
        return Err(invalid_artifact_root(request_id));
    }
    Ok(())
}

fn is_invalid_requested_artifact_root(path: &Path) -> bool {
    has_parent_component(path) || is_forbidden_live_route_path(path)
}

fn ensure_provider_owned_artifact_root(
    requested: &Path,
    allowed_roots: &[PathBuf],
    config_root: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let requested = validated_normalized_absolute_path(requested, request_id)?;
    for root in allowed_roots {
        let root = validated_normalized_absolute_path(root, request_id)?;
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

fn validated_normalized_absolute_path(
    path: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    ensure_no_parent_component(path, request_id)?;
    Ok(normalized_absolute_path(path))
}

fn normalized_absolute_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        push_normalized_component(&mut normalized, component);
    }
    normalized
}

fn push_normalized_component(normalized: &mut PathBuf, component: Component<'_>) {
    match component {
        Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
            normalized.push(component.as_os_str())
        }
        Component::CurDir | Component::ParentDir => {}
    }
}

fn ensure_no_parent_component(path: &Path, request_id: &str) -> Result<(), ProviderFailure> {
    if has_parent_component(path) {
        return Err(invalid_artifact_root(request_id));
    }
    Ok(())
}

fn canonical_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    fs::canonicalize(path)
        .map_err(|err| migration_artifact_root_canonicalize_failure(request_id, err))
}

fn canonical_create_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let existing = existing_ancestor_path(path, request_id)?;
    let suffix = create_path_suffix(path, existing, request_id)?;
    ensure_create_path_suffix(suffix, request_id)?;
    let mut canonical = canonical_path(existing, request_id)?;
    append_create_path_suffix(&mut canonical, suffix);
    Ok(canonical)
}

fn existing_ancestor_path<'a>(
    path: &'a Path,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    existing_ancestor(path).ok_or_else(|| invalid_artifact_root(request_id))
}

fn create_path_suffix<'a>(
    path: &'a Path,
    existing: &'a Path,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    path.strip_prefix(existing)
        .map_err(|_| invalid_artifact_root(request_id))
}

fn ensure_create_path_suffix(suffix: &Path, request_id: &str) -> Result<(), ProviderFailure> {
    for component in suffix.components() {
        if !is_create_path_suffix_component(component) {
            return Err(invalid_artifact_root(request_id));
        }
    }
    Ok(())
}

fn is_create_path_suffix_component(component: Component<'_>) -> bool {
    matches!(component, Component::Normal(_) | Component::CurDir)
}

fn append_create_path_suffix(canonical: &mut PathBuf, suffix: &Path) {
    for component in suffix.components() {
        push_create_path_component(canonical, component);
    }
}

fn push_create_path_component(canonical: &mut PathBuf, component: Component<'_>) {
    if let Component::Normal(part) = component {
        canonical.push(part);
    }
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

fn write_artifact(path: &Path, value: &Value, request_id: &str) -> Result<(), ProviderFailure> {
    let bytes = artifact_bytes(value, request_id)?;
    let mut file = create_artifact_file(path, request_id)?;
    file.write_all(&bytes)
        .map_err(|err| migration_artifact_write_failure(request_id, err))
}

fn string_param<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn migration_artifact_dir_failure(request_id: &str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "migration_artifact_dir_failed",
        format!("failed to create provider-owned artifact directory: {err}"),
    )
}

fn read_artifact(path: &Path, request_id: &str) -> Result<Vec<u8>, ProviderFailure> {
    fs::read(path).map_err(|err| migration_artifact_read_failure(request_id, err))
}

fn migration_artifact_read_failure(request_id: &str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "migration_artifact_read_failed",
        format!("failed to read provider-owned artifact: {err}"),
    )
}

fn migration_confirmation_required_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "migration_confirmation_required",
        "migration.apply requires confirmation.approved=true",
    )
}

fn migration_artifact_root_canonicalize_failure(
    request_id: &str,
    err: std::io::Error,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "migration_artifact_root_canonicalize_failed",
        format!("failed to canonicalize migration artifact root: {err}"),
    )
}

fn artifact_bytes(value: &Value, request_id: &str) -> Result<Vec<u8>, ProviderFailure> {
    serde_json::to_vec(value).map_err(|err| migration_artifact_serialize_failure(request_id, err))
}

fn migration_artifact_serialize_failure(
    request_id: &str,
    err: serde_json::Error,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "migration_artifact_serialize_failed",
        format!("failed to serialize migration artifact: {err}"),
    )
}

fn create_artifact_file(path: &Path, request_id: &str) -> Result<fs::File, ProviderFailure> {
    fs::File::create(path).map_err(|err| migration_artifact_create_failure(request_id, err))
}

fn migration_artifact_create_failure(request_id: &str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "migration_artifact_create_failed",
        format!("failed to create provider-owned migration artifact: {err}"),
    )
}

fn migration_artifact_write_failure(request_id: &str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "migration_artifact_write_failed",
        format!("failed to write provider-owned migration artifact: {err}"),
    )
}
