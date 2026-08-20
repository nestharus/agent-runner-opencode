//! Declared roles: mapper, validator, predicate, filter, formatter

use crate::account::{profile_for_account_reference, AccountProfile};
use crate::activity::ActivityTargets;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use crate::opencode::{self, OpencodeExportError, OpencodeImportError};
use crate::path_guard;
use crate::runtime_selection::resolve_runtime_selection;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const AUTHORIZATION_TTL: Duration = Duration::from_secs(10 * 60);
const ROTATION_STATE_DIR: &str = "provider-state/opencode/rotation";

pub fn assess_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
    provider_instance_id: &str,
) -> Result<Value, ProviderFailure> {
    let requirements = requirements(&params);
    let met = requirements_met(&requirements);
    let facts_allow = facts_allow_rotation(&params);
    let binding = rotation_binding(&params, host, provider_instance_id, request_id)?;
    validate_rotation_accounts(host, &binding, request_id)?;
    let allowed = met
        && facts_allow
        && binding.source_provider_id != binding.target_provider_id
        && binding.source_account.opencode_wrapper != binding.target_account.opencode_wrapper;
    let authorization = persist_assessment_decision(host, &binding, allowed, request_id)?;
    Ok(assess_result(
        allowed,
        &requirements,
        met,
        facts_allow,
        authorization.as_ref(),
    ))
}

pub fn materialize_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
    provider_instance_id: &str,
) -> Result<Value, ProviderFailure> {
    let binding = rotation_binding(&params, host, provider_instance_id, request_id)?;
    let working_directory = rotation_working_directory(host, request_id)?;
    let _lock = acquire_rotation_lock(host, request_id)?;
    if let Some(result) = read_materialization_receipt(host, &binding, request_id)? {
        return Ok(result);
    }
    if let Some(mut operation) = read_rotation_operation(host, &binding, request_id)? {
        if operation.phase == RotationOperationPhase::Prepared {
            reconcile_prepared_operation(host, &params, &binding, &mut operation, request_id)?;
        }
        return finalize_rotation_operation(host, &binding, &operation, request_id);
    }
    let authorization = require_fresh_authorization(host, &binding, request_id)?;
    let native = opencode::export(&binding.source_session_id, binding.source_account)
        .map_err(|error| rotation_export_failure(request_id, &binding.source_session_id, error))?;
    validate_rotation_export(&native, &binding.source_session_id, request_id)?;
    let boundary = crate::session::rotation_boundary_timestamp(&native)
        .ok_or_else(|| rotation_boundary_missing(request_id, &binding.source_session_id))?;
    let artifact_bytes = serde_json::to_vec(native.native_json())
        .map_err(|error| rotation_artifact_failure(request_id, error))?;
    let artifact_path = rotation_artifact_path(host, &artifact_bytes, request_id)?;
    write_artifact_atomic(&artifact_path, &artifact_bytes)
        .map_err(|error| rotation_artifact_failure(request_id, error))?;
    let mut operation = RotationOperation {
        schema_version: 1,
        binding_sha256: binding_digest(&binding),
        binding: binding_value(&binding),
        authorization_id: authorization["authorization_id"]
            .as_str()
            .expect("validated authorization id")
            .to_string(),
        assessment_request_id: authorization["assessment_request_id"]
            .as_str()
            .expect("validated assessment request id")
            .to_string(),
        materialization_request_id: request_id.to_string(),
        artifact_path: artifact_path.display().to_string(),
        artifact_sha256: sha256_hex(&artifact_bytes),
        boundary,
        prepared_at_unix_ms: now_unix_ms(),
        phase: RotationOperationPhase::Prepared,
        target_session_id: None,
        imported_at_unix_ms: None,
    };
    write_rotation_operation(host, &binding, &operation, request_id)?;
    let target_session_id =
        opencode::import_session(&artifact_path, binding.target_account, working_directory)
            .map_err(|error| {
                rotation_import_failure(request_id, &binding.target_provider_id, error)
            })?;
    operation.phase = RotationOperationPhase::Imported;
    operation.target_session_id = Some(target_session_id);
    operation.imported_at_unix_ms = Some(now_unix_ms());
    write_rotation_operation(host, &binding, &operation, request_id)?;
    finalize_rotation_operation(host, &binding, &operation, request_id)
}

pub(crate) fn activity_targets(params: &Value, result: Option<&Value>) -> ActivityTargets {
    let mut targets = ActivityTargets::default();
    append_attempted_string(
        &mut targets,
        params,
        "chain_id",
        "rotation_chain",
        "params.chain_id",
    );
    append_attempted_string(
        &mut targets,
        params,
        "source_provider",
        "provider",
        "params.source_provider",
    );
    append_attempted_string(
        &mut targets,
        params,
        "target_provider",
        "provider",
        "params.target_provider",
    );
    append_rotation_account_targets(
        &mut targets,
        params,
        "source_account",
        "params.source_account",
    );
    append_rotation_account_targets(
        &mut targets,
        params,
        "target_account",
        "params.target_account",
    );
    append_attempted_string(
        &mut targets,
        params,
        "source_session_id",
        "provider_session",
        "params.source_session_id",
    );
    append_attempted_string(
        &mut targets,
        params,
        "settings_id",
        "settings_record",
        "params.settings_id",
    );
    append_attempted_string(
        &mut targets,
        params,
        "model_name",
        "model_alias",
        "params.model_name",
    );
    if let Some(target_session_id) = result
        .and_then(|result| result.get("target_provider_session_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        targets.generated(
            "provider_session",
            target_session_id,
            "result.target_provider_session_id",
        );
    }
    targets
}

fn append_attempted_string(
    targets: &mut ActivityTargets,
    params: &Value,
    field: &str,
    kind: &'static str,
    provenance: &'static str,
) {
    if let Some(value) = optional_string(params, field) {
        targets.attempted(kind, value, provenance);
    }
}

fn append_rotation_account_targets(
    targets: &mut ActivityTargets,
    params: &Value,
    field: &str,
    provenance: &'static str,
) {
    let Some(reference) = optional_string(params, field) else {
        return;
    };
    targets.attempted("account", reference, provenance);
    if let Some(profile) = profile_for_account_reference(reference) {
        targets.resolved(
            "account",
            profile.opencode_wrapper,
            format!("{provenance}.catalog"),
        );
    }
}

fn requirements(params: &Value) -> Vec<Value> {
    params
        .get("requirements")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn requirements_met(requirements: &[Value]) -> bool {
    requirements.is_empty()
        || requirements.iter().all(|requirement| {
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

#[derive(Clone)]
struct RotationBinding {
    chain_id: String,
    source_provider_id: String,
    target_provider_id: String,
    source_account: &'static AccountProfile,
    target_account: &'static AccountProfile,
    source_session_id: String,
    model_name: String,
    settings_record_id: String,
    transition_reason: String,
    provider_instance_id: String,
    host_app: String,
}

#[derive(Deserialize, Serialize)]
struct RotationOperation {
    schema_version: u32,
    binding_sha256: String,
    binding: Value,
    authorization_id: String,
    assessment_request_id: String,
    materialization_request_id: String,
    artifact_path: String,
    artifact_sha256: String,
    boundary: String,
    prepared_at_unix_ms: u64,
    phase: RotationOperationPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    imported_at_unix_ms: Option<u64>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RotationOperationPhase {
    Prepared,
    Imported,
}

fn rotation_binding(
    params: &Value,
    host: &HostContext,
    provider_instance_id: &str,
    request_id: &str,
) -> Result<RotationBinding, ProviderFailure> {
    let source_account_reference = required_string(params, "source_account", request_id)?;
    let target_account_reference = required_string(params, "target_account", request_id)?;
    Ok(RotationBinding {
        chain_id: required_string(params, "chain_id", request_id)?.to_string(),
        source_provider_id: required_string(params, "source_provider", request_id)?.to_string(),
        target_provider_id: required_string(params, "target_provider", request_id)?.to_string(),
        source_account: rotation_account(source_account_reference, request_id, "source")?,
        target_account: rotation_account(target_account_reference, request_id, "target")?,
        source_session_id: required_string(params, "source_session_id", request_id)?.to_string(),
        model_name: optional_string(params, "model_name")
            .unwrap_or("")
            .to_string(),
        settings_record_id: optional_string(params, "settings_id")
            .unwrap_or("")
            .to_string(),
        transition_reason: transition_reason(params).to_string(),
        provider_instance_id: provider_instance_id.to_string(),
        host_app: host.app.clone(),
    })
}

fn optional_string<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn binding_value(binding: &RotationBinding) -> Value {
    json!({
        "chain_id": binding.chain_id,
        "source_provider": binding.source_provider_id,
        "target_provider": binding.target_provider_id,
        "source_account": binding.source_account.opencode_wrapper,
        "target_account": binding.target_account.opencode_wrapper,
        "source_session_id": binding.source_session_id,
        "model_name": binding.model_name,
        "settings_id": binding.settings_record_id,
        "transition_reason": binding.transition_reason,
        "provider_instance_id": binding.provider_instance_id,
        "host_app": binding.host_app,
    })
}

fn binding_digest(binding: &RotationBinding) -> String {
    sha256_hex(binding_value(binding).to_string().as_bytes())
}

fn validate_rotation_accounts(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if binding.target_provider_id != binding.provider_instance_id {
        return Err(rotation_target_provider_mismatch(request_id));
    }
    if !binding.settings_record_id.is_empty() {
        let selection = resolve_runtime_selection(host, &binding.settings_record_id, request_id)?;
        if selection.account.opencode_wrapper != binding.target_account.opencode_wrapper {
            return Err(rotation_settings_account_mismatch(request_id));
        }
    }
    Ok(())
}

fn rotation_account(
    account_reference: &str,
    request_id: &str,
    role: &str,
) -> Result<&'static crate::account::AccountProfile, ProviderFailure> {
    profile_for_account_reference(account_reference)
        .ok_or_else(|| unknown_rotation_account(request_id, role, account_reference))
}

fn persist_assessment_decision(
    host: &HostContext,
    binding: &RotationBinding,
    allowed: bool,
    request_id: &str,
) -> Result<Option<Value>, ProviderFailure> {
    let path = authorization_path(host, binding, request_id)?;
    if !allowed {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(rotation_state_failure(request_id, error)),
        }
        return Ok(None);
    }
    let issued_at_unix_ms = now_unix_ms();
    let digest = binding_digest(binding);
    let authorization_id =
        sha256_hex(format!("{digest}\0{request_id}\0{issued_at_unix_ms}").as_bytes());
    let record = json!({
        "schema_version": 1,
        "authorization_id": authorization_id,
        "binding_sha256": digest,
        "binding": binding_value(binding),
        "assessment_request_id": request_id,
        "issued_at_unix_ms": issued_at_unix_ms,
        "expires_at_unix_ms": issued_at_unix_ms.saturating_add(AUTHORIZATION_TTL.as_millis() as u64),
        "allowed": true,
    });
    write_private_json_atomic(&path, &record)
        .map_err(|error| rotation_state_failure(request_id, error))?;
    Ok(Some(json!({
        "kind": "provider_authorization",
        "met": true,
        "authorization_id": record["authorization_id"],
        "binding_sha256": record["binding_sha256"],
        "expires_at_unix_ms": record["expires_at_unix_ms"],
    })))
}

fn require_fresh_authorization(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let path = authorization_path(host, binding, request_id)?;
    let bytes = durable_fs::read_file(&path)
        .map_err(|error| rotation_authorization_failure(request_id, error))?;
    let record: Value = serde_json::from_slice(&bytes)
        .map_err(|error| rotation_authorization_failure(request_id, error))?;
    let expected = binding_digest(binding);
    let valid = record["allowed"] == true
        && record["binding_sha256"].as_str() == Some(expected.as_str())
        && record["binding"] == binding_value(binding)
        && record["expires_at_unix_ms"]
            .as_u64()
            .is_some_and(|expires| expires >= now_unix_ms());
    if !valid {
        return Err(rotation_authorization_invalid(request_id));
    }
    Ok(record)
}

fn acquire_rotation_lock(
    host: &HostContext,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let root = rotation_state_root(host, request_id)?;
    let lock_path =
        confined_rotation_state_target(host, &root.join("materialize.lock"), request_id)?;
    durable_fs::create_private_directories(&root)
        .map_err(|error| rotation_state_failure(request_id, error))?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .map_err(|error| rotation_state_failure(request_id, error))?;
    lock.lock_exclusive()
        .map_err(|error| rotation_state_failure(request_id, error))?;
    Ok(lock)
}

fn authorization_path(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let path = rotation_state_root(host, request_id)?
        .join("authorizations")
        .join(format!("{}.json", binding_digest(binding)));
    confined_rotation_state_target(host, &path, request_id)
}

fn materialization_receipt_path(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let path = rotation_state_root(host, request_id)?
        .join("materializations")
        .join(format!("{}.json", binding_digest(binding)));
    confined_rotation_state_target(host, &path, request_id)
}

fn rotation_operation_path(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let path = rotation_state_root(host, request_id)?
        .join("operations")
        .join(format!("{}.json", binding_digest(binding)));
    confined_rotation_state_target(host, &path, request_id)
}

fn rotation_state_root(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let data_root = rotation_data_root(host, request_id)?;
    let path = data_root.join(ROTATION_STATE_DIR);
    confined_rotation_state_target(host, &path, request_id)
}

fn rotation_data_root<'a>(
    host: &'a HostContext,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    host.data_root
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "rotation_data_root_missing",
                "rotation operations require host.data_root",
            )
        })
}

fn confined_rotation_state_target(
    host: &HostContext,
    target: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = rotation_data_root(host, request_id)?;
    path_guard::confined_target(data_root, target)
        .map_err(|error| rotation_state_failure(request_id, error))
}

fn read_materialization_receipt(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<Option<Value>, ProviderFailure> {
    let path = materialization_receipt_path(host, binding, request_id)?;
    let bytes = match durable_fs::read_file(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(rotation_state_failure(request_id, error)),
    };
    let receipt: Value = serde_json::from_slice(&bytes)
        .map_err(|error| rotation_state_failure(request_id, error))?;
    if receipt["binding_sha256"].as_str() != Some(binding_digest(binding).as_str())
        || receipt["binding"] != binding_value(binding)
    {
        return Err(rotation_authorization_invalid(request_id));
    }
    Ok(receipt.get("result").cloned())
}

fn read_rotation_operation(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<Option<RotationOperation>, ProviderFailure> {
    let path = rotation_operation_path(host, binding, request_id)?;
    let bytes = match durable_fs::read_file(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(rotation_state_failure(request_id, error)),
    };
    let operation: RotationOperation = serde_json::from_slice(&bytes)
        .map_err(|error| rotation_operation_invalid(request_id, error))?;
    validate_rotation_operation(host, binding, &operation, request_id)?;
    Ok(Some(operation))
}

fn validate_rotation_operation(
    host: &HostContext,
    binding: &RotationBinding,
    operation: &RotationOperation,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let phase_valid = match operation.phase {
        RotationOperationPhase::Prepared => {
            operation.target_session_id.is_none() && operation.imported_at_unix_ms.is_none()
        }
        RotationOperationPhase::Imported => {
            operation
                .target_session_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                && operation.imported_at_unix_ms.is_some()
        }
    };
    if operation.schema_version != 1
        || operation.binding_sha256 != binding_digest(binding)
        || operation.binding != binding_value(binding)
        || operation.authorization_id.trim().is_empty()
        || operation.assessment_request_id.trim().is_empty()
        || operation.materialization_request_id.trim().is_empty()
        || operation.boundary.trim().is_empty()
        || !phase_valid
    {
        return Err(rotation_operation_invalid(
            request_id,
            "operation identity or phase is inconsistent",
        ));
    }
    read_rotation_operation_artifact(host, operation, request_id).map(|_| ())
}

fn read_rotation_operation_artifact(
    host: &HostContext,
    operation: &RotationOperation,
    request_id: &str,
) -> Result<Vec<u8>, ProviderFailure> {
    let data_root = rotation_data_root(host, request_id)?;
    let path = path_guard::confined_target(data_root, Path::new(&operation.artifact_path))
        .map_err(|error| rotation_operation_invalid(request_id, error))?;
    let bytes = durable_fs::read_file(&path)
        .map_err(|error| rotation_operation_invalid(request_id, error))?;
    if sha256_hex(&bytes) != operation.artifact_sha256 {
        return Err(rotation_operation_invalid(
            request_id,
            "operation artifact digest does not match durable preparation",
        ));
    }
    Ok(bytes)
}

fn write_rotation_operation(
    host: &HostContext,
    binding: &RotationBinding,
    operation: &RotationOperation,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    write_private_json_atomic(
        &rotation_operation_path(host, binding, request_id)?,
        &serde_json::to_value(operation)
            .map_err(|error| rotation_state_failure(request_id, error))?,
    )
    .map_err(|error| rotation_state_failure(request_id, error))
}

fn reconcile_prepared_operation(
    host: &HostContext,
    params: &Value,
    binding: &RotationBinding,
    operation: &mut RotationOperation,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let supplied_target = optional_string(params, "recovery_target_session_id");
    let candidate_session_id = supplied_target.unwrap_or(&binding.source_session_id);
    let target = match opencode::export(candidate_session_id, binding.target_account) {
        Ok(target) => target,
        Err(error) => {
            return Err(rotation_recovery_required(
                request_id,
                binding,
                operation,
                supplied_target,
                Some(format!("target export failed: {error:?}")),
            ));
        }
    };
    if target.info.id != candidate_session_id
        || target
            .messages
            .iter()
            .any(|message| message.info.session_id.as_deref() != Some(candidate_session_id))
    {
        return Err(rotation_recovery_required(
            request_id,
            binding,
            operation,
            supplied_target,
            Some("target export does not carry the proposed target session identity".to_string()),
        ));
    }
    let source_bytes = read_rotation_operation_artifact(host, operation, request_id)?;
    let source: Value = serde_json::from_slice(&source_bytes)
        .map_err(|error| rotation_operation_invalid(request_id, error))?;
    if normalized_rotation_session(source)
        != normalized_rotation_session(target.native_json().clone())
    {
        return Err(rotation_recovery_required(
            request_id,
            binding,
            operation,
            supplied_target,
            Some("target export content does not match the prepared source artifact".to_string()),
        ));
    }
    operation.phase = RotationOperationPhase::Imported;
    operation.target_session_id = Some(candidate_session_id.to_string());
    operation.imported_at_unix_ms = Some(now_unix_ms());
    write_rotation_operation(host, binding, operation, request_id)
}

fn normalized_rotation_session(mut native: Value) -> Value {
    if let Some(info) = native.get_mut("info").and_then(Value::as_object_mut) {
        info.insert("id".to_string(), Value::String("<session>".to_string()));
        info.remove("directory");
    }
    normalize_nested_session_ids(&mut native);
    native
}

fn normalize_nested_session_ids(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if object.contains_key("sessionID") {
                object.insert(
                    "sessionID".to_string(),
                    Value::String("<session>".to_string()),
                );
            }
            for value in object.values_mut() {
                normalize_nested_session_ids(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize_nested_session_ids(value);
            }
        }
        _ => {}
    }
}

fn finalize_rotation_operation(
    host: &HostContext,
    binding: &RotationBinding,
    operation: &RotationOperation,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    if operation.phase != RotationOperationPhase::Imported {
        return Err(rotation_operation_invalid(
            request_id,
            "only an imported operation can be finalized",
        ));
    }
    let target_session_id = operation
        .target_session_id
        .as_deref()
        .expect("validated imported operation has a target session id");
    let artifact_bytes = read_rotation_operation_artifact(host, operation, request_id)?;
    let artifact_path = Path::new(&operation.artifact_path);
    let artifact = rotation_artifact(artifact_path, &artifact_bytes);
    let decision_artifact = write_rotation_decision_receipt(host, binding, operation, request_id)?;
    let host_state_plan = host_state_plan(HostStatePlanInput {
        chain_id: &binding.chain_id,
        source_provider: &binding.source_provider_id,
        target_provider: &binding.target_provider_id,
        source_session_id: &binding.source_session_id,
        target_session_id,
        transition_reason: &binding.transition_reason,
        boundary: &operation.boundary,
        artifacts: [&artifact, &decision_artifact],
    });
    let result = json!({
        "changed": true,
        "target_provider_session_id": target_session_id,
        "artifacts": [artifact, decision_artifact],
        "host_state_plan": host_state_plan,
    });
    write_materialization_receipt(
        host,
        binding,
        &result,
        &operation.materialization_request_id,
        request_id,
    )?;
    Ok(result)
}

fn write_materialization_receipt(
    host: &HostContext,
    binding: &RotationBinding,
    result: &Value,
    materialization_request_id: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let receipt = json!({
        "schema_version": 1,
        "binding_sha256": binding_digest(binding),
        "binding": binding_value(binding),
        "materialization_request_id": materialization_request_id,
        "finalization_request_id": request_id,
        "recorded_at_unix_ms": now_unix_ms(),
        "result": result,
    });
    write_private_json_atomic(
        &materialization_receipt_path(host, binding, request_id)?,
        &receipt,
    )
    .map_err(|error| rotation_state_failure(request_id, error))
}

fn write_rotation_decision_receipt(
    host: &HostContext,
    binding: &RotationBinding,
    operation: &RotationOperation,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let target_session_id = operation
        .target_session_id
        .as_deref()
        .expect("validated imported operation has a target session id");
    let identity_matched = target_session_id == binding.source_session_id;
    let record = json!({
        "schema_version": 1,
        "operation": "rotation.materialize",
        "binding_sha256": binding_digest(binding),
        "binding": binding_value(binding),
        "authorization_id": operation.authorization_id,
        "assessment_request_id": operation.assessment_request_id,
        "materialization_request_id": operation.materialization_request_id,
        "expected_target_session_id": binding.source_session_id,
        "actual_target_session_id": target_session_id,
        "identity_matched": identity_matched,
        "outcome": if identity_matched { "settled" } else { "recoverable_identity_change" },
        "recorded_at_unix_ms": operation.imported_at_unix_ms,
    });
    let bytes =
        serde_json::to_vec(&record).map_err(|error| rotation_state_failure(request_id, error))?;
    let digest = sha256_hex(&bytes);
    let candidate = rotation_state_root(host, request_id)?
        .join("decisions")
        .join(format!("{digest}.json"));
    let path = confined_rotation_state_target(host, &candidate, request_id)?;
    write_private_bytes_atomic(&path, &bytes)
        .map_err(|error| rotation_state_failure(request_id, error))?;
    Ok(json!({
        "kind": "file",
        "path": path.display().to_string(),
        "sha256": digest,
    }))
}

fn write_private_json_atomic(path: &Path, value: &Value) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    write_private_bytes_atomic(path, &bytes)
}

fn write_private_bytes_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "state path has no parent")
    })?;
    durable_fs::create_private_directories(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.as_file_mut().write_all(bytes)?;
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    durable_fs::sync_directory(parent)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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
    artifacts: [&'a Value; 2],
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
        "artifacts": input.artifacts
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
    authorization: Option<&Value>,
) -> Value {
    let score = score(requirements, facts_allow);
    let mut requirements = requirements.to_vec();
    if let Some(authorization) = authorization {
        requirements.push(authorization.clone());
    }
    json!({
        "allowed": allowed,
        "score": score,
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
    let data_root = rotation_data_root(host, request_id)?;
    let artifact_id = sha256_hex(artifact_bytes);
    let path = data_root
        .join("provider-artifacts")
        .join("opencode")
        .join("rotation")
        .join(format!("{artifact_id}.json"));
    path_guard::confined_target(data_root, &path)
        .map_err(|error| rotation_artifact_failure(request_id, error))
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
    durable_fs::create_private_directories(parent)?;
    match fs::read(path) {
        Ok(existing) if existing == bytes => return durable_fs::sync_directory(parent),
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
        Ok(()) => durable_fs::sync_directory(parent),
        Err(error) if fs::read(path).is_ok_and(|existing| existing == bytes) => {
            let _ = fs::remove_file(&temporary);
            durable_fs::sync_directory(parent)
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

fn rotation_settings_account_mismatch(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "rotation_settings_account_mismatch",
        "rotation settings_id must identify a persisted record for target_provider",
    )
}

fn rotation_target_provider_mismatch(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "rotation_target_provider_mismatch",
        "rotation target_provider must equal the invoked provider_instance_id",
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

fn rotation_state_failure(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_state_failed",
        format!("failed to maintain provider-owned rotation state: {error}"),
    )
}

fn rotation_operation_invalid(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_operation_invalid",
        format!("durable rotation operation state is invalid: {error}"),
    )
}

fn rotation_recovery_required(
    request_id: &str,
    binding: &RotationBinding,
    operation: &RotationOperation,
    supplied_target: Option<&str>,
    observation: Option<String>,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "rotation_recovery_required",
        "a prepared rotation may already have imported the target session; automatic re-import is blocked until the target identity is reconciled",
        json!({
            "binding_sha256": operation.binding_sha256,
            "target_account": binding.target_account.opencode_wrapper,
            "expected_target_session_id": binding.source_session_id,
            "supplied_recovery_target_session_id": supplied_target,
            "prepared_artifact_path": operation.artifact_path,
            "recovery": "retry with recovery_target_session_id after confirming the imported target session; if import never occurred, import the prepared artifact once and supply the resulting session id",
            "observation": observation,
        }),
    )
}

fn rotation_authorization_failure(
    request_id: &str,
    error: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "rotation_authorization_required",
        format!("rotation.materialize requires a fresh matching rotation.assess decision: {error}"),
    )
}

fn rotation_authorization_invalid(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "rotation_authorization_invalid",
        "rotation assessment authorization is expired or does not match the materialization",
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
