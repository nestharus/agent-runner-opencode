//! Declared roles: orchestration, mapper, parser, validator, accessor, predicate, formatter
//! adapter_declarations:
//!   - component: src/quota.rs
//!     role: adapter
//!     Translates:
//!       - opencode auth source profile to QuotaSourceResult
//!       - provider-owned QuotaObservation to QuotaProbeWindow
//!       - opencode CLI-owned auth refresh boundary to QuotaRefreshAuthResult

use crate::account::{profile_for_wrapper_reference, AccountProfile};
use crate::activity::ActivityTargets;
use crate::durable_fs;
use crate::encoding::{bounded_text, now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope};
use crate::opencode::{OpencodeAuthEffect, OpencodeAuthObservation};
use crate::path_guard;
use crate::quota_adapter::{self, QuotaObservation, QuotaObservationFailure, QuotaWindow};
use crate::runtime_selection::{
    append_resolved_activity_targets, resolve_runtime_selection, RuntimeSelection,
};
use chrono::DateTime;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const QUOTA_REFRESH_STATE_DIR: &str = "provider-state/opencode/quota/auth-refresh";
const QUOTA_REFRESH_SCHEMA_VERSION: u32 = 1;

#[derive(Deserialize)]
struct QuotaBaseParams {
    settings_id: String,
}

#[derive(Deserialize)]
struct QuotaRefreshAuthParams {
    settings_id: String,
}

#[derive(Deserialize, Serialize)]
struct QuotaRefreshOperation {
    schema_version: u32,
    operation: String,
    request_id: String,
    binding_sha256: String,
    binding: Value,
    phase: QuotaRefreshOperationPhase,
    prepared_at_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    native_effect_admitted_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    committed_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum QuotaRefreshOperationPhase {
    Prepared,
    NativeEffectAdmitted,
    ReconciliationRequired,
    Committed,
}

#[derive(Clone, Copy)]
pub(crate) enum Command {
    Source,
    Probe,
    RefreshAuth,
}

pub(crate) fn handle(command: Command, request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let RequestEnvelope {
        host,
        params,
        request_id,
        provider_instance_id,
        ..
    } = request;
    match command {
        Command::Source => source_params(&host, params, &request_id),
        Command::Probe => probe_params(&host, params, &request_id),
        Command::RefreshAuth => {
            refresh_auth_params(&host, params, &request_id, provider_instance_id.as_deref())
        }
    }
}

pub(crate) fn activity_targets(
    host: &HostContext,
    params: &Value,
    result: Option<&Value>,
    request_id: &str,
) -> ActivityTargets {
    let mut targets = ActivityTargets::default();
    let settings_id = params
        .get("settings_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if let Some(settings_id) = settings_id {
        targets.attempted("settings_record", settings_id, "params.settings_id");
        if result.is_some() {
            append_resolved_activity_targets(
                &mut targets,
                host,
                settings_id,
                request_id,
                "runtime_selection.settings_record",
            );
        }
    }
    targets
}

pub fn source_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params = parse_base_params(params, request_id)?;
    let account = account_from_settings_record(host, &params.settings_id, request_id)?;
    let auth_path = resolved_auth_path(account);
    Ok(source_result(account, &auth_path))
}

pub fn probe_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params = parse_base_params(params, request_id)?;
    let account = account_from_settings_record(host, &params.settings_id, request_id)?;
    Ok(probe_account(account))
}

pub fn refresh_auth_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
    provider_instance_id: Option<&str>,
) -> Result<Value, ProviderFailure> {
    let parsed = parse_refresh_params(params.clone(), request_id)?;
    let params_sha256 = sha256_hex(params.to_string().as_bytes());
    let attempted_identity_sha256 = quota_refresh_attempt_identity_sha256(
        &params_sha256,
        &parsed.settings_id,
        provider_instance_id,
        &host.app,
    );
    let _request_lock = acquire_quota_refresh_request_lock(host, request_id)?;
    let (mut operation, account, auth_path) = match read_quota_refresh_operation(host, request_id)?
    {
        Some(operation) => {
            validate_quota_refresh_operation(&operation, request_id)?;
            if !quota_refresh_attempt_matches(
                &operation,
                &params_sha256,
                &parsed.settings_id,
                provider_instance_id,
                &host.app,
            ) {
                return Err(quota_refresh_request_conflict(
                    request_id,
                    &attempted_identity_sha256,
                    &operation,
                ));
            }
            match operation.phase {
                QuotaRefreshOperationPhase::Committed => {
                    return Ok(operation
                        .result
                        .expect("validated committed quota refresh has a result"));
                }
                QuotaRefreshOperationPhase::NativeEffectAdmitted => {
                    let mut unresolved = operation;
                    unresolved.phase = QuotaRefreshOperationPhase::ReconciliationRequired;
                    write_quota_refresh_operation(host, &unresolved, request_id)?;
                    return Err(quota_refresh_reconciliation_required(
                        request_id,
                        &unresolved,
                    ));
                }
                QuotaRefreshOperationPhase::ReconciliationRequired => {
                    return Err(quota_refresh_reconciliation_required(
                        request_id, &operation,
                    ));
                }
                QuotaRefreshOperationPhase::Prepared => {}
            }
            let (account, auth_path) = quota_refresh_operation_route(&operation, request_id)?;
            (operation, account, auth_path)
        }
        None => {
            let selection = resolve_runtime_selection(host, &parsed.settings_id, request_id)?;
            let account = selection.account;
            let auth_path = resolved_auth_path(account);
            let binding = quota_refresh_binding(
                &params_sha256,
                &selection,
                &auth_path,
                provider_instance_id,
                &host.app,
            );
            let operation = QuotaRefreshOperation {
                schema_version: QUOTA_REFRESH_SCHEMA_VERSION,
                operation: "quota.refresh_auth".to_string(),
                request_id: request_id.to_string(),
                binding_sha256: quota_refresh_binding_sha256(&binding),
                binding,
                phase: QuotaRefreshOperationPhase::Prepared,
                prepared_at_unix_ms: now_unix_ms(),
                native_effect_admitted_at_unix_ms: None,
                committed_at_unix_ms: None,
                result: None,
            };
            write_quota_refresh_operation(host, &operation, request_id)?;
            (operation, account, auth_path)
        }
    };
    let _account_lock = acquire_quota_refresh_account_lock(host, account, &auth_path, request_id)?;
    operation.phase = QuotaRefreshOperationPhase::NativeEffectAdmitted;
    operation.native_effect_admitted_at_unix_ms = Some(now_unix_ms());
    write_quota_refresh_operation(host, &operation, request_id)?;
    let checked_at_unix_ms = now_unix_ms();
    let refresh = run_account_auth_refresh(account, &auth_path);
    let refreshed = refresh_succeeded(&refresh);
    let available = refresh
        .as_ref()
        .is_ok_and(OpencodeAuthObservation::command_succeeded)
        && refresh_available(account);
    let result = refresh_auth_result(
        refreshed,
        available,
        checked_at_unix_ms,
        refresh_detail(refresh.as_ref()),
    );
    operation.phase = QuotaRefreshOperationPhase::Committed;
    operation.committed_at_unix_ms = Some(now_unix_ms());
    operation.result = Some(result.clone());
    write_quota_refresh_operation(host, &operation, request_id)?;
    Ok(result)
}

fn quota_refresh_binding(
    params_sha256: &str,
    selection: &RuntimeSelection,
    auth_path: &Path,
    provider_instance_id: Option<&str>,
    host_app: &str,
) -> Value {
    json!({
        "operation": "quota.refresh_auth",
        "params_sha256": params_sha256,
        "settings_id": selection.settings_id,
        "settings_version": selection.settings_version,
        "account": selection.account.opencode_wrapper,
        "account_index": selection.account.opencode_index,
        "auth_source_path": auth_path.display().to_string(),
        "provider_instance_id": provider_instance_id,
        "host_app": host_app,
    })
}

fn quota_refresh_attempt_identity_sha256(
    params_sha256: &str,
    settings_id: &str,
    provider_instance_id: Option<&str>,
    host_app: &str,
) -> String {
    sha256_hex(
        json!({
            "operation": "quota.refresh_auth",
            "params_sha256": params_sha256,
            "settings_id": settings_id,
            "provider_instance_id": provider_instance_id,
            "host_app": host_app,
        })
        .to_string()
        .as_bytes(),
    )
}

fn quota_refresh_attempt_matches(
    operation: &QuotaRefreshOperation,
    params_sha256: &str,
    settings_id: &str,
    provider_instance_id: Option<&str>,
    host_app: &str,
) -> bool {
    operation.binding["params_sha256"].as_str() == Some(params_sha256)
        && operation.binding["settings_id"].as_str() == Some(settings_id)
        && operation.binding["provider_instance_id"] == json!(provider_instance_id)
        && operation.binding["host_app"].as_str() == Some(host_app)
}

fn quota_refresh_operation_route(
    operation: &QuotaRefreshOperation,
    request_id: &str,
) -> Result<(&'static AccountProfile, PathBuf), ProviderFailure> {
    let account_name = operation.binding["account"]
        .as_str()
        .ok_or_else(|| quota_refresh_operation_invalid(request_id, "binding account is missing"))?;
    let account = profile_for_wrapper_reference(account_name).ok_or_else(|| {
        quota_refresh_operation_invalid(request_id, "binding account is not declared")
    })?;
    if account.opencode_wrapper != account_name
        || operation.binding["account_index"].as_u64() != Some(u64::from(account.opencode_index))
    {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "binding account identity is inconsistent",
        ));
    }
    let auth_path = operation.binding["auth_source_path"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            quota_refresh_operation_invalid(request_id, "binding auth source path is missing")
        })?;
    Ok((account, auth_path))
}

fn quota_refresh_binding_sha256(binding: &Value) -> String {
    sha256_hex(binding.to_string().as_bytes())
}

fn acquire_quota_refresh_request_lock(
    host: &HostContext,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let name = sha256_hex(request_id.as_bytes());
    acquire_quota_refresh_lock(host, &format!("locks/requests/{name}.lock"), request_id)
}

fn acquire_quota_refresh_account_lock(
    host: &HostContext,
    account: &AccountProfile,
    auth_path: &Path,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let name = sha256_hex(
        format!(
            "{}\0{}\0{}",
            account.opencode_wrapper,
            account.opencode_index,
            auth_path.display()
        )
        .as_bytes(),
    );
    acquire_quota_refresh_lock(host, &format!("locks/accounts/{name}.lock"), request_id)
}

fn acquire_quota_refresh_lock(
    host: &HostContext,
    relative: &str,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let root = quota_refresh_state_root(host, request_id)?;
    let path = confined_quota_refresh_target(host, &root.join(relative), request_id)?;
    let parent = path
        .parent()
        .expect("quota refresh lock always has a parent");
    durable_fs::create_private_directories(parent)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    lock.lock_exclusive()
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    Ok(lock)
}

fn quota_refresh_operation_path(
    host: &HostContext,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let name = sha256_hex(request_id.as_bytes());
    let root = quota_refresh_state_root(host, request_id)?;
    confined_quota_refresh_target(
        host,
        &root.join(format!("requests/{name}.json")),
        request_id,
    )
}

fn quota_refresh_state_root(
    host: &HostContext,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = quota_refresh_data_root(host, request_id)?;
    confined_quota_refresh_target(host, &data_root.join(QUOTA_REFRESH_STATE_DIR), request_id)
}

fn quota_refresh_data_root<'a>(
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
                "quota_refresh_data_root_missing",
                "quota.refresh_auth requires host.data_root for durable request custody",
            )
        })
}

fn confined_quota_refresh_target(
    host: &HostContext,
    target: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    path_guard::confined_target(quota_refresh_data_root(host, request_id)?, target)
        .map_err(|error| quota_refresh_state_failure(request_id, error))
}

fn read_quota_refresh_operation(
    host: &HostContext,
    request_id: &str,
) -> Result<Option<QuotaRefreshOperation>, ProviderFailure> {
    let path = quota_refresh_operation_path(host, request_id)?;
    let bytes = match durable_fs::read_file(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(quota_refresh_state_failure(request_id, error)),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| quota_refresh_operation_invalid(request_id, error))
}

fn validate_quota_refresh_operation(
    operation: &QuotaRefreshOperation,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let phase_valid = match operation.phase {
        QuotaRefreshOperationPhase::Prepared => {
            operation.native_effect_admitted_at_unix_ms.is_none()
                && operation.committed_at_unix_ms.is_none()
                && operation.result.is_none()
        }
        QuotaRefreshOperationPhase::NativeEffectAdmitted
        | QuotaRefreshOperationPhase::ReconciliationRequired => {
            operation.native_effect_admitted_at_unix_ms.is_some()
                && operation.committed_at_unix_ms.is_none()
                && operation.result.is_none()
        }
        QuotaRefreshOperationPhase::Committed => {
            operation.native_effect_admitted_at_unix_ms.is_some()
                && operation.committed_at_unix_ms.is_some()
                && operation.result.is_some()
        }
    };
    if operation.schema_version != QUOTA_REFRESH_SCHEMA_VERSION
        || operation.operation != "quota.refresh_auth"
        || operation.request_id != request_id
        || operation.binding_sha256 != quota_refresh_binding_sha256(&operation.binding)
        || operation.binding_sha256.trim().is_empty()
        || !phase_valid
    {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "operation identity, binding, or phase is inconsistent",
        ));
    }
    Ok(())
}

fn write_quota_refresh_operation(
    host: &HostContext,
    operation: &QuotaRefreshOperation,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let path = quota_refresh_operation_path(host, request_id)?;
    let parent = path
        .parent()
        .expect("quota refresh operation always has a parent");
    durable_fs::create_private_directories(parent)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    let bytes = serde_json::to_vec_pretty(operation)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    temporary
        .write_all(&bytes)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    temporary
        .persist(&path)
        .map_err(|error| quota_refresh_state_failure(request_id, error.error))?;
    durable_fs::sync_directory(parent)
        .map_err(|error| quota_refresh_state_failure(request_id, error))
}

fn quota_refresh_request_conflict(
    request_id: &str,
    attempted_identity_sha256: &str,
    operation: &QuotaRefreshOperation,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "quota_refresh_request_conflict",
        "quota.refresh_auth request_id is already bound to a different operation identity",
        json!({
            "attempted_request_identity_sha256": attempted_identity_sha256,
            "committed_binding_sha256": operation.binding_sha256,
            "committed_phase": quota_refresh_phase_name(operation.phase),
        }),
    )
}

fn quota_refresh_reconciliation_required(
    request_id: &str,
    operation: &QuotaRefreshOperation,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "quota_refresh_reconciliation_required",
        "a prior quota.refresh_auth invocation admitted the native auth effect but did not commit an observation; automatic replay is unsafe",
        json!({
            "binding_sha256": operation.binding_sha256,
            "phase": quota_refresh_phase_name(operation.phase),
            "settings_id": operation.binding["settings_id"],
            "settings_version": operation.binding["settings_version"],
            "account": operation.binding["account"],
            "auth_source_path": operation.binding["auth_source_path"],
            "recovery": "inspect the bound credential source and submit a new request_id only after reconciling the prior native auth attempt",
        }),
    )
}

fn quota_refresh_phase_name(phase: QuotaRefreshOperationPhase) -> &'static str {
    match phase {
        QuotaRefreshOperationPhase::Prepared => "prepared",
        QuotaRefreshOperationPhase::NativeEffectAdmitted => "native_effect_admitted",
        QuotaRefreshOperationPhase::ReconciliationRequired => "reconciliation_required",
        QuotaRefreshOperationPhase::Committed => "committed",
    }
}

fn quota_refresh_state_failure(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "quota_refresh_state_failed",
        format!("failed to preserve quota.refresh_auth request state: {error}"),
    )
}

fn quota_refresh_operation_invalid(
    request_id: &str,
    error: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "quota_refresh_operation_invalid",
        format!("durable quota.refresh_auth request state is invalid: {error}"),
    )
}

fn source_result(account: &AccountProfile, auth_path: &Path) -> Value {
    let has_source = auth_has_source(auth_path);
    let source_id = readable_source_id(has_source, account, auth_path);
    source_result_json(has_source, source_id)
}

fn source_result_json(has_source: bool, source_id: Option<String>) -> Value {
    let mut result = serde_json::Map::new();
    result.insert("has_source".to_string(), json!(has_source));
    result.insert("freshness".to_string(), json!(source_freshness(has_source)));
    if let Some(source_id) = source_id {
        result.insert("source_id".to_string(), json!(source_id));
    }
    Value::Object(result)
}

fn readable_source_id(
    has_source: bool,
    account: &AccountProfile,
    auth_path: &Path,
) -> Option<String> {
    has_source.then(|| source_id(account, auth_path))
}

fn probe_account(account: &AccountProfile) -> Value {
    let auth_path = resolved_auth_path(account);
    if !auth_has_source(&auth_path) {
        return unreadable_auth_probe_result();
    }
    probe_observation_result(run_probe(&auth_path))
}

fn unreadable_auth_probe_result() -> Value {
    unavailable_result("native opencode auth source is missing or unreadable".to_string())
}

fn run_probe(auth_path: &Path) -> Result<QuotaObservation, QuotaObservationFailure> {
    quota_adapter::observe_quota(auth_path)
}

fn probe_observation_result(
    observation: Result<QuotaObservation, QuotaObservationFailure>,
) -> Value {
    match observation {
        Ok(observation) => available_probe_result(&observation.windows),
        Err(failure) => unavailable_result(quota_observation_failure_detail(failure)),
    }
}

fn quota_observation_failure_detail(failure: QuotaObservationFailure) -> String {
    if failure.needs_auth_refresh() {
        return format!(
            "{} (authentication refresh required; invoke quota.refresh_auth with a new request_id before probing again)",
            failure.detail
        );
    }
    failure.detail
}

fn unavailable_result(detail: String) -> Value {
    json!({
        "available": false,
        "checked_at_unix_ms": now_unix_ms(),
        "windows": [],
        "detail": detail,
    })
}

fn quota_windows(windows: &[QuotaWindow]) -> Vec<Value> {
    windows.iter().map(quota_window).collect()
}

fn quota_window(window: &QuotaWindow) -> Value {
    let mut result = serde_json::Map::new();
    if let Some(name) = &window.name {
        result.insert("name".to_string(), json!(name));
    }
    result.insert(
        "remaining_ratio".to_string(),
        json!(((100.0 - window.used_percent) / 100.0).clamp(0.0, 1.0)),
    );
    result.insert(
        "resets_at_unix_ms".to_string(),
        json!(epoch_ms(&window.resets_at)),
    );
    Value::Object(result)
}

fn parse_base_params(params: Value, request_id: &str) -> Result<QuotaBaseParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_quota_params_failure(request_id, err))
}

fn parse_refresh_params(
    params: Value,
    request_id: &str,
) -> Result<QuotaRefreshAuthParams, ProviderFailure> {
    serde_json::from_value(params)
        .map_err(|err| invalid_quota_refresh_params_failure(request_id, err))
}

fn account_from_settings_record(
    host: &HostContext,
    settings_id: &str,
    request_id: &str,
) -> Result<&'static AccountProfile, ProviderFailure> {
    resolve_runtime_selection(host, settings_id, request_id).map(|selection| selection.account)
}

fn resolved_auth_path(account: &AccountProfile) -> PathBuf {
    expand_tilde(account.quota_auth_path())
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(relative) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(relative);
        }
    }
    PathBuf::from(path)
}

fn auth_is_readable(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() || !has_read_permission(&metadata) {
        return false;
    }
    fs::File::open(path).is_ok()
}

fn auth_has_source(path: &Path) -> bool {
    auth_is_readable(path)
}

#[cfg(unix)]
fn has_read_permission(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o444 != 0
}

#[cfg(not(unix))]
fn has_read_permission(_metadata: &fs::Metadata) -> bool {
    true
}

fn source_id(account: &AccountProfile, auth_path: &Path) -> String {
    format!(
        "{}:{}:{}:{}",
        account.quota_source_kind(),
        account.opencode_wrapper,
        account.opencode_index,
        auth_path.to_string_lossy()
    )
}

fn opencode_command_failure_detail(output: &crate::shell::ShellOutput) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = bounded_text(stderr.trim(), 500);
    if stderr.is_empty() {
        return format!("opencode auth list exited with status {}", output.status);
    }
    format!(
        "opencode auth list exited with status {}: {stderr}",
        output.status
    )
}

fn refresh_available(account: &AccountProfile) -> bool {
    quota_observation_without_refresh(account).is_ok()
}

fn quota_observation_without_refresh(
    account: &AccountProfile,
) -> Result<QuotaObservation, QuotaObservationFailure> {
    run_probe(&resolved_auth_path(account))
}

fn run_account_auth_refresh(
    account: &AccountProfile,
    auth_path: &Path,
) -> std::io::Result<OpencodeAuthObservation> {
    crate::opencode::observe_auth_list(account, auth_path)
}

fn refresh_succeeded(refresh: &std::io::Result<OpencodeAuthObservation>) -> bool {
    refresh
        .as_ref()
        .is_ok_and(OpencodeAuthObservation::credentials_refreshed)
}

fn refresh_detail(refresh: Result<&OpencodeAuthObservation, &std::io::Error>) -> String {
    match refresh {
        Ok(observation) if observation.command_succeeded() => {
            auth_effect_detail(observation.effect).to_string()
        }
        Ok(observation) => format!(
            "opencode auth list failed: {}",
            opencode_command_failure_detail(&observation.output)
        ),
        Err(err) => format!("failed to run opencode auth list: {err}"),
    }
}

fn auth_effect_detail(effect: OpencodeAuthEffect) -> &'static str {
    match effect {
        OpencodeAuthEffect::CredentialsChanged => {
            "the selected credential source changed during opencode auth list"
        }
        OpencodeAuthEffect::CredentialsUnchanged => {
            "opencode auth list completed without an observed credential change"
        }
        OpencodeAuthEffect::CredentialStateUnobservable => {
            "opencode auth list completed but credential state could not be compared"
        }
    }
}

fn refresh_auth_result(
    refreshed: bool,
    available: bool,
    checked_at_unix_ms: u64,
    detail: String,
) -> Value {
    json!({
        "refreshed": refreshed,
        "available": available,
        "checked_at_unix_ms": checked_at_unix_ms,
        "detail": detail,
    })
}

fn source_freshness(has_source: bool) -> &'static str {
    if has_source {
        "auth_readable"
    } else {
        "auth_missing_or_unreadable"
    }
}

fn available_probe_result(windows: &[QuotaWindow]) -> Value {
    json!({
        "available": true,
        "checked_at_unix_ms": now_unix_ms(),
        "windows": quota_windows(windows),
    })
}

fn invalid_quota_params_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_quota_params",
        format!("quota params are invalid: {err}"),
    )
}

fn invalid_quota_refresh_params_failure(
    request_id: &str,
    err: serde_json::Error,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_quota_refresh_auth_params",
        format!("quota.refresh_auth params are invalid: {err}"),
    )
}

fn epoch_ms(rfc3339: &str) -> i64 {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("quota observation resets_at was validated before projection")
        .timestamp_millis()
}
