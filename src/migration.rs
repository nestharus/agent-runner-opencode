//! Declared roles: mapper, validator, orchestration, accessor, formatter, predicate

use crate::activity::ActivityTargets;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use crate::path_guard;
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
    durable_fs::create_directories(&artifact_root)
        .map_err(|err| migration_artifact_dir_failure(request_id, err))?;
    let actions = planned_actions(&params);
    let summary = artifact_summary(&params, &actions);
    let bytes = artifact_bytes(&summary, request_id)?;
    let path = artifact_root.join(format!(
        "opencode-provider-migration-summary-{}.json",
        sha256_hex(&bytes)
    ));
    ensure_canonical_contained(&path, &config_root, request_id)?;
    write_artifact(&path, &bytes, request_id)?;
    Ok(migration_apply_result(
        actions,
        migration_apply_artifacts(&path, &bytes),
        migration_warnings(&params),
        &artifact_root,
    ))
}

pub(crate) fn activity_targets(params: &Value, result: Option<&Value>) -> ActivityTargets {
    let mut targets = ActivityTargets::default();
    if let Some(provider) = string_param(params, "target_provider") {
        targets.attempted("provider", provider, "params.target_provider");
    }
    targets.attempted(
        "migration_input_digest",
        sha256_hex(params.to_string().as_bytes()),
        "params.content_sha256",
    );
    if let Some(artifacts) = result
        .and_then(|result| result.get("artifacts"))
        .and_then(Value::as_array)
    {
        for artifact in artifacts {
            if let Some(digest) = artifact
                .get("sha256")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                targets.generated("migration_artifact", digest, "result.artifacts[].sha256");
            }
        }
    }
    targets
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
            "artifact": "opencode-provider-migration-summary-<sha256>.json",
            "publication": "content_addressed_atomic",
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
    path_guard::confined_target(config_root, requested)
        .map(|_| ())
        .map_err(|_| invalid_artifact_root(request_id))
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
    let legacy = params.get("legacy").unwrap_or(&Value::Null);
    json!({
        "schema": "opencode.provider_migration/v1",
        "target_provider": string_param(params, "target_provider").unwrap_or("agent-runner-opencode"),
        "scope": string_param(params, "scope").unwrap_or("provider_owned"),
        "legacy": legacy_summary(legacy),
        "legacy_input_sha256": sha256_hex(legacy.to_string().as_bytes()),
        "actions": actions,
        "live_cutover": false,
    })
}

fn legacy_summary(legacy: &Value) -> Value {
    let providers_toml = legacy.get("providers_toml").and_then(Value::as_str);
    json!({
        "has_providers_toml": providers_toml.is_some(),
        "providers_toml_sha256": providers_toml.map(|value| sha256_hex(value.as_bytes())),
        "models": legacy_model_identities(legacy),
        "model_count": legacy.get("models").and_then(Value::as_object).map(|models| models.len()).unwrap_or(0),
    })
}

fn legacy_model_identities(legacy: &Value) -> Vec<Value> {
    legacy
        .get("models")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .map(|(name, value)| {
            let raw = value.as_str().unwrap_or_default();
            let parsed = raw.parse::<toml::Value>().ok();
            json!({
                "file": name,
                "sha256": sha256_hex(raw.as_bytes()),
                "name": parsed.as_ref().and_then(|value| value.get("name")).and_then(toml::Value::as_str),
                "provider": parsed.as_ref().and_then(|value| value.get("provider")).and_then(toml::Value::as_str),
                "model": parsed.as_ref().and_then(|value| value.get("model")).and_then(toml::Value::as_str),
            })
        })
        .collect()
}

fn write_artifact(path: &Path, bytes: &[u8], request_id: &str) -> Result<(), ProviderFailure> {
    let parent = path.parent().ok_or_else(|| {
        migration_artifact_create_failure(
            request_id,
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "artifact has no parent"),
        )
    })?;
    match durable_fs::read_file(path) {
        Ok(existing) if existing == bytes => return Ok(()),
        Ok(_) => {
            return Err(migration_artifact_write_failure(
                request_id,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "content-addressed migration artifact contains different bytes",
                ),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(migration_artifact_write_failure(request_id, error)),
    }
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| migration_artifact_create_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .write_all(bytes)
        .map_err(|error| migration_artifact_write_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(|error| migration_artifact_write_failure(request_id, error))?;
    match temporary.persist_noclobber(path) {
        Ok(_) => {}
        Err(error) if fs::read(path).is_ok_and(|existing| existing == bytes) => {}
        Err(error) => return Err(migration_artifact_write_failure(request_id, error.error)),
    }
    durable_fs::sync_directory(parent)
        .map_err(|error| migration_artifact_write_failure(request_id, error))
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

fn migration_confirmation_required_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "migration_confirmation_required",
        "migration.apply requires confirmation.approved=true",
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
