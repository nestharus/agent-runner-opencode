//! Declared roles: mapper, validator, predicate, filter, formatter

use crate::account::profile_for_settings_id;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use crate::opencode::{self, OpencodeExportError, OpencodeImportError};
use serde_json::{json, Value};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn assess_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let requirements = requirements(&params);
    let met = requirements_met(&requirements);
    let facts_allow = facts_allow_rotation(&params);
    let allowed = met && facts_allow;
    Ok(assess_result(allowed, &requirements, met, facts_allow))
}

pub fn materialize_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let chain_id = required_string(&params, "chain_id", request_id)?;
    let source_provider = required_string(&params, "source_provider", request_id)?;
    let target_provider = required_string(&params, "target_provider", request_id)?;
    let source_session_id = required_string(&params, "source_session_id", request_id)?;
    let source_account = profile_for_settings_id(source_provider)
        .ok_or_else(|| unknown_rotation_account(request_id, "source", source_provider))?;
    let target_account = profile_for_settings_id(target_provider)
        .ok_or_else(|| unknown_rotation_account(request_id, "target", target_provider))?;
    let working_directory = rotation_working_directory(host, request_id)?;
    let native = opencode::export(source_session_id, source_account)
        .map_err(|error| rotation_export_failure(request_id, source_session_id, error))?;
    validate_rotation_export(&native, source_session_id, request_id)?;
    let boundary = crate::session::rotation_boundary_timestamp(&native)
        .ok_or_else(|| rotation_boundary_missing(request_id, source_session_id))?;
    let artifact_bytes = serde_json::to_vec(native.native_json())
        .map_err(|error| rotation_artifact_failure(request_id, error))?;
    let artifact_path = rotation_artifact_path(host, &artifact_bytes, request_id)?;
    write_artifact_atomic(&artifact_path, &artifact_bytes)
        .map_err(|error| rotation_artifact_failure(request_id, error))?;
    let target_session_id =
        opencode::import_session(&artifact_path, target_account, working_directory)
            .map_err(|error| rotation_import_failure(request_id, target_provider, error))?;
    if target_session_id != source_session_id {
        return Err(rotation_import_session_mismatch(
            request_id,
            source_session_id,
            &target_session_id,
        ));
    }
    let artifact = rotation_artifact(&artifact_path, &artifact_bytes);
    let host_state_plan = host_state_plan(HostStatePlanInput {
        chain_id,
        source_provider,
        target_provider,
        source_session_id,
        target_session_id: &target_session_id,
        transition_reason: transition_reason(&params),
        boundary: &boundary,
        artifact: &artifact,
    });
    Ok(json!({
        "changed": true,
        "target_provider_session_id": target_session_id,
        "artifacts": [artifact],
        "host_state_plan": host_state_plan,
    }))
}

fn requirements(params: &Value) -> Vec<Value> {
    params
        .get("requirements")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn requirements_met(requirements: &[Value]) -> bool {
    !requirements.is_empty()
        && requirements.iter().all(|requirement| {
            requirement
                .get("met")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
}

fn facts_allow_rotation(params: &Value) -> bool {
    let quota = params
        .pointer("/facts/quota/available")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let exportable = params
        .pointer("/facts/session/exportable")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let target = params
        .pointer("/facts/settings/target_profile_present")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    quota && exportable && target
}

fn score(requirements: &[Value], facts_allow: bool) -> u64 {
    if requirements.is_empty() {
        return u64::from(facts_allow) * 100;
    }
    let met = met_requirement_count(requirements);
    (met * 100) / requirements.len() as u64
}

fn assess_reason(allowed: bool, requirements_met: bool, facts_allow: bool) -> &'static str {
    match (allowed, requirements_met, facts_allow) {
        (true, _, _) => {
            "rotation requirements are satisfied; provider can return a host-applied plan"
        }
        (false, true, true) => "rotation was denied by provider policy",
        (false, false, _) => "one or more rotation requirements are not met",
        (false, _, false) => "provider facts do not permit safe rotation materialization",
    }
}

struct HostStatePlanInput<'a> {
    chain_id: &'a str,
    source_provider: &'a str,
    target_provider: &'a str,
    source_session_id: &'a str,
    target_session_id: &'a str,
    transition_reason: &'a str,
    boundary: &'a str,
    artifact: &'a Value,
}

fn host_state_plan(input: HostStatePlanInput<'_>) -> Value {
    json!({
        "schema_version": 1,
        "operation": "rotation.materialize",
        "chain_id": input.chain_id,
        "source_provider": input.source_provider,
        "target_provider": input.target_provider,
        "source_session_id": input.source_session_id,
        "target_session_id": input.target_session_id,
        "transition_reason": input.transition_reason,
        "segments": [
            {
                "provider": input.source_provider,
                "session_id": input.source_session_id,
                "ended_at": input.boundary
            },
            {
                "provider": input.target_provider,
                "session_id": input.target_session_id,
                "started_at": input.boundary
            }
        ],
        "artifacts": [input.artifact]
    })
}

fn transition_reason(params: &Value) -> &'static str {
    match params.get("transition_reason").and_then(Value::as_str) {
        Some("quota_threshold") => "quota_threshold",
        Some("exhausted") => "exhausted",
        _ => "manual",
    }
}

fn required_string<'a>(
    params: &'a Value,
    key: &str,
    request_id: &str,
) -> Result<&'a str, ProviderFailure> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "rotation_params_invalid",
                format!("rotation.materialize requires {key}"),
            )
        })
}

fn assess_result(
    allowed: bool,
    requirements: &[Value],
    requirements_met: bool,
    facts_allow: bool,
) -> Value {
    json!({
        "allowed": allowed,
        "score": score(requirements, facts_allow),
        "reason": assess_reason(allowed, requirements_met, facts_allow),
        "requirements": requirements,
    })
}

fn met_requirement_count(requirements: &[Value]) -> u64 {
    requirements
        .iter()
        .filter(|requirement| requirement_met(requirement))
        .count() as u64
}

fn requirement_met(requirement: &Value) -> bool {
    requirement.get("met").and_then(Value::as_bool) == Some(true)
}

fn rotation_artifact_path(
    host: &HostContext,
    artifact_bytes: &[u8],
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = host
        .data_root
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "rotation_data_root_missing",
                "rotation.materialize requires host.data_root",
            )
        })?;
    let artifact_id = sha256_hex(artifact_bytes);
    Ok(Path::new(data_root)
        .join("provider-artifacts")
        .join("opencode")
        .join("rotation")
        .join(format!("{artifact_id}.json")))
}

fn rotation_working_directory<'a>(
    host: &'a HostContext,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    let path = host
        .working_directory
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "rotation_working_directory_missing",
                "rotation.materialize requires host.working_directory",
            )
        })?;
    if !path.is_absolute() || !path.is_dir() {
        return Err(ProviderFailure::invalid_request(
            request_id,
            "rotation_working_directory_invalid",
            "rotation.materialize host.working_directory must be an existing absolute directory",
        ));
    }
    Ok(path)
}

fn write_artifact_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "artifact path has no parent",
        )
    })?;
    fs::create_dir_all(parent)?;
    set_private_directory_permissions(parent)?;
    match fs::read(path) {
        Ok(existing) if existing == bytes => return Ok(()),
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "content-addressed artifact does not match its digest path",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = path.with_extension(format!("json.{}.{nonce}.tmp", std::process::id()));
    let mut file = private_artifact_file(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    match fs::rename(&temporary, path) {
        Ok(()) => Ok(()),
        Err(error) if fs::read(path).is_ok_and(|existing| existing == bytes) => {
            let _ = fs::remove_file(&temporary);
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(error)
        }
    }
}

fn private_artifact_file(path: &Path) -> std::io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn validate_rotation_export(
    native: &opencode::OpencodeExport,
    source_session_id: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if native.info.id != source_session_id
        || native
            .messages
            .iter()
            .any(|message| message.info.session_id.as_deref() != Some(source_session_id))
    {
        return Err(rotation_export_session_mismatch(
            request_id,
            source_session_id,
        ));
    }
    Ok(())
}

fn rotation_artifact(path: &Path, bytes: &[u8]) -> Value {
    json!({
        "kind": "file",
        "path": path.display().to_string(),
        "sha256": sha256_hex(bytes),
    })
}

fn unknown_rotation_account(request_id: &str, role: &str, provider: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "rotation_account_unknown",
        format!("rotation {role} account is unknown: {provider}"),
    )
}

fn rotation_export_failure(
    request_id: &str,
    session_id: &str,
    error: OpencodeExportError,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_export_failed",
        format!("failed to export source session {session_id}: {error:?}"),
    )
}

fn rotation_export_session_mismatch(request_id: &str, source_session_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_export_session_mismatch",
        format!("OpenCode export does not belong to source session {source_session_id}"),
    )
}

fn rotation_artifact_failure(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_artifact_failed",
        format!("failed to persist rotation artifact: {error}"),
    )
}

fn rotation_import_failure(
    request_id: &str,
    target_provider: &str,
    error: OpencodeImportError,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_import_failed",
        format!("failed to import session into {target_provider}: {error:?}"),
    )
}

fn rotation_import_session_mismatch(
    request_id: &str,
    source_session_id: &str,
    target_session_id: &str,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_import_session_mismatch",
        format!(
            "OpenCode import returned session {target_session_id} instead of {source_session_id}"
        ),
    )
}

fn rotation_boundary_missing(request_id: &str, source_session_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_boundary_missing",
        format!("source session {source_session_id} has no exported turns"),
    )
}

#[cfg(test)]
mod tests {
    use super::validate_rotation_export;

    #[test]
    fn rotation_rejects_message_without_source_session_identity() {
        let native = crate::opencode::parse_export_stdout(
            br#"{
                "info": {"id": "ses_source", "title": "source"},
                "messages": [{
                    "info": {
                        "id": "msg_source",
                        "role": "user",
                        "time": {"created": 1782864000000}
                    },
                    "parts": []
                }]
            }"#,
        )
        .expect("native export fixture");

        assert!(validate_rotation_export(&native, "ses_source", "request-test").is_err());
    }
}
