//! Declared roles: orchestration, parser, validator, accessor, predicate, formatter
//! intrinsic_surface_declarations:
//!   - component: src/quota_auth_refresh.rs
//!     role: intrinsic-surface
//!     Domain: durable credential-refresh effect custody
//!     Owns:
//!       - request and credential-effect serialization
//!       - durable actor settlement and exact retry
//!       - credential reconciliation and terminal replay

use crate::account::{profile_for_wrapper_reference, AccountProfile};
use crate::durable_fs;
use crate::encoding::{bounded_text_bytes, now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure};
use crate::native_process::{terminate_process_group_actor, ProcessGroupActor};
use crate::native_runtime::{self, NativeRuntimeContext};
use crate::opencode::{
    OpencodeAuthEffect, OpencodeAuthFailure, OpencodeAuthObservation,
    PreparedOpencodeAuthObservation,
};
use crate::operation_bounds;
use crate::path_guard;
use crate::quota::{resolved_auth_path, run_probe};
use crate::quota_adapter::{QuotaObservation, QuotaObservationFailure};
use crate::quota_observer::{self, QuotaObserverContext};
use crate::request_custody::{ActiveReservation, CustodyError, RequestCustody};
use crate::runtime_selection::{resolve_runtime_selection, RuntimeSelection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const QUOTA_REFRESH_STATE_DIR: &str = "provider-state/opencode/quota/auth-refresh";
const QUOTA_REFRESH_SCHEMA_VERSION: u32 = 1;
const QUOTA_REFRESH_OPERATION_TIMEOUT: Duration = Duration::from_secs(20);
const QUOTA_REFRESH_CLEANUP_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const QUOTA_REFRESH_ORPHAN_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_ACTIVE_QUOTA_REFRESH_REQUEST_RECORDS: usize = 64;
const MAX_QUOTA_REFRESH_REPLAY_RECORDS: usize = 4096;
const MAX_QUOTA_REFRESH_STATE_BYTES: usize = 256 * 1024;
const MAX_QUOTA_REFRESH_RECONCILIATION_DETAIL_BYTES: usize = 500;

#[derive(Deserialize)]
struct QuotaRefreshAuthParams {
    settings_id: String,
    context: Option<QuotaRefreshAuthContext>,
}

#[derive(Deserialize)]
struct QuotaRefreshAuthContext {
    reconciliation: Option<QuotaRefreshAuthReconciliation>,
    #[serde(flatten)]
    _extra: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
struct QuotaRefreshAuthReconciliation {
    disposition: String,
    credential_source_sha256: String,
}

impl QuotaRefreshAuthParams {
    fn reconciliation(&self) -> Option<&QuotaRefreshAuthReconciliation> {
        self.context
            .as_ref()
            .and_then(|context| context.reconciliation.as_ref())
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actor_process_group_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actor_process_group_incarnation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actor_terminal_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    committed_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reconciliation: Option<Value>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum QuotaRefreshOperationPhase {
    Prepared,
    NativeEffectAdmitted,
    ReconciliationRequired,
    Committed,
}
pub fn refresh_auth_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
    provider_instance_id: Option<&str>,
) -> Result<Value, ProviderFailure> {
    let parsed = parse_refresh_params(params.clone(), request_id)?;
    let params_sha256 = quota_refresh_binding_params_sha256(&params);
    let attempted_identity_sha256 = quota_refresh_attempt_identity_sha256(
        &params_sha256,
        &parsed.settings_id,
        provider_instance_id,
        &host.app,
    );
    let _request_lock =
        acquire_quota_refresh_request_lock(host, &attempted_identity_sha256, request_id)?;
    let (mut operation, account, runtime, observer, auth_path) =
        match read_quota_refresh_operation(host, request_id)? {
            Some(mut operation) => {
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
                        settle_quota_refresh_actor(&mut unresolved, request_id)?;
                        require_quota_refresh_reconciliation(
                            &mut unresolved,
                            "provider_interrupted_after_effect_admission",
                            "unobserved",
                            "the provider did not preserve a terminal credential observation",
                        );
                        write_quota_refresh_operation(host, &unresolved, request_id)?;
                        return Err(quota_refresh_reconciliation_required(
                            request_id,
                            &unresolved,
                        ));
                    }
                    QuotaRefreshOperationPhase::ReconciliationRequired => {
                        if settle_quota_refresh_actor(&mut operation, request_id)? {
                            write_quota_refresh_operation(host, &operation, request_id)?;
                        }
                        if let Some(reconciliation) = parsed.reconciliation() {
                            return reconcile_quota_refresh_operation(
                                host,
                                operation,
                                reconciliation,
                                request_id,
                            );
                        }
                        return Err(quota_refresh_reconciliation_required(
                            request_id, &operation,
                        ));
                    }
                    QuotaRefreshOperationPhase::Prepared => {
                        if parsed.reconciliation().is_some() {
                            return Err(quota_refresh_reconciliation_not_required(request_id));
                        }
                    }
                }
                let operation =
                    upgrade_prepared_quota_refresh_runtime_binding(host, operation, request_id)?;
                let (account, runtime, observer, auth_path) =
                    quota_refresh_operation_route(host, &operation, request_id)?;
                (operation, account, runtime, observer, auth_path)
            }
            None => {
                if parsed.reconciliation().is_some() {
                    return Err(quota_refresh_reconciliation_not_required(request_id));
                }
                let selection = resolve_runtime_selection(host, &parsed.settings_id, request_id)?;
                let account = selection.account;
                let runtime = native_runtime::resolve_for_account(host, account, request_id)?;
                let observer = quota_observer::resolve(host, account, request_id)?;
                let auth_path = resolved_auth_path(account, &runtime);
                let binding = quota_refresh_binding(
                    &params_sha256,
                    &selection,
                    &runtime,
                    &observer,
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
                    actor_process_group_id: None,
                    actor_process_group_incarnation: None,
                    actor_terminal_at_unix_ms: None,
                    committed_at_unix_ms: None,
                    result: None,
                    reconciliation: None,
                };
                write_quota_refresh_operation(host, &operation, request_id)?;
                (operation, account, runtime, observer, auth_path)
            }
        };
    let _effect_lock = acquire_quota_refresh_effect_lock(host, &auth_path, request_id)?;
    let native_timeout = quota_refresh_operation_timeout(host, request_id)?;
    let checked_at_unix_ms = now_unix_ms();
    let refresh = match prepare_account_auth_refresh(&runtime, &auth_path) {
        Ok(prepared) => {
            publish_quota_refresh_actor(host, &mut operation, prepared.actor(), request_id)?;
            match prepared.observe_leader(native_timeout) {
                Ok(pending) => {
                    settle_quota_refresh_actor(&mut operation, request_id)?;
                    pending.observe_terminal_credentials()
                }
                Err(error) => {
                    settle_quota_refresh_actor(&mut operation, request_id)?;
                    Err(error)
                }
            }
        }
        Err(error) => Err(error),
    };
    if let Some(reconciliation) = refresh_reconciliation(&refresh) {
        require_quota_refresh_reconciliation(
            &mut operation,
            reconciliation.reason,
            reconciliation.credential_effect,
            &reconciliation.detail,
        );
        write_quota_refresh_operation(host, &operation, request_id)?;
        return Err(quota_refresh_reconciliation_required(
            request_id, &operation,
        ));
    }
    let refreshed = refresh_succeeded(&refresh);
    let available = refresh
        .as_ref()
        .is_ok_and(OpencodeAuthObservation::observation_succeeded)
        && refresh_available(account, &runtime, &observer);
    let result = refresh_auth_result(
        refreshed,
        available,
        checked_at_unix_ms,
        refresh_detail(refresh.as_ref()),
    );
    operation.phase = QuotaRefreshOperationPhase::Committed;
    operation.committed_at_unix_ms = Some(now_unix_ms());
    operation.result = Some(result.clone());
    operation.reconciliation = None;
    write_quota_refresh_operation(host, &operation, request_id)?;
    Ok(result)
}

fn quota_refresh_binding_params_sha256(params: &Value) -> String {
    let mut binding_params = params.clone();
    let remove_empty_context = if let Some(context) = binding_params
        .get_mut("context")
        .and_then(Value::as_object_mut)
    {
        context.remove("reconciliation");
        context.is_empty()
    } else {
        false
    };
    if remove_empty_context {
        binding_params
            .as_object_mut()
            .expect("quota refresh params are validated as an object")
            .remove("context");
    }
    sha256_hex(binding_params.to_string().as_bytes())
}

fn reconcile_quota_refresh_operation(
    host: &HostContext,
    mut operation: QuotaRefreshOperation,
    reconciliation: &QuotaRefreshAuthReconciliation,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    if reconciliation.disposition != "accept_current_credentials"
        || !is_sha256_hex(&reconciliation.credential_source_sha256)
    {
        return Err(invalid_quota_refresh_reconciliation_failure(request_id));
    }
    let (account, runtime, observer, auth_path) =
        quota_refresh_operation_route(host, &operation, request_id)?;
    let _effect_lock = acquire_quota_refresh_effect_lock(host, &auth_path, request_id)?;
    let credential_bytes =
        durable_fs::read_file_bounded(&auth_path, durable_fs::MAX_AUTH_FILE_BYTES)
            .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    let observed_sha256 = sha256_hex(&credential_bytes);
    if observed_sha256 != reconciliation.credential_source_sha256 {
        return Err(quota_refresh_reconciliation_mismatch(
            request_id,
            &reconciliation.credential_source_sha256,
            &observed_sha256,
        ));
    }
    let checked_at_unix_ms = now_unix_ms();
    let refreshed = operation
        .reconciliation
        .as_ref()
        .and_then(|evidence| evidence.get("credential_effect"))
        .and_then(Value::as_str)
        == Some("credentials_changed");
    let available = refresh_available(account, &runtime, &observer);
    let result = refresh_auth_result(
        refreshed,
        available,
        checked_at_unix_ms,
        "the caller reconciled and accepted the current bound credential source".to_string(),
    );
    let evidence = operation.reconciliation.get_or_insert_with(|| {
        json!({
            "reason": "manual_reconciliation",
            "credential_effect": "unobserved",
            "detail": "the caller supplied authoritative reconciliation for an admitted auth effect",
            "observed_at_unix_ms": checked_at_unix_ms,
        })
    });
    evidence["resolution"] = json!({
        "disposition": reconciliation.disposition,
        "credential_source_sha256": observed_sha256,
        "accepted_at_unix_ms": checked_at_unix_ms,
    });
    operation.phase = QuotaRefreshOperationPhase::Committed;
    operation.committed_at_unix_ms = Some(checked_at_unix_ms);
    operation.result = Some(result.clone());
    write_quota_refresh_operation(host, &operation, request_id)?;
    Ok(result)
}

fn upgrade_prepared_quota_refresh_runtime_binding(
    host: &HostContext,
    mut operation: QuotaRefreshOperation,
    request_id: &str,
) -> Result<QuotaRefreshOperation, ProviderFailure> {
    if operation.binding["native_runtime_identity_sha256"]
        .as_str()
        .is_some()
        && operation.binding["quota_observer_identity_sha256"]
            .as_str()
            .is_some()
    {
        return Ok(operation);
    }
    let account_name = operation.binding["account"]
        .as_str()
        .ok_or_else(|| quota_refresh_operation_invalid(request_id, "binding account is missing"))?;
    let account = profile_for_wrapper_reference(account_name).ok_or_else(|| {
        quota_refresh_operation_invalid(request_id, "binding account is not declared")
    })?;
    if account.opencode_wrapper != account_name {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "binding account identity is inconsistent",
        ));
    }
    let runtime = native_runtime::resolve_for_account(host, account, request_id)?;
    let observer = quota_observer::resolve(host, account, request_id)?;
    let expected_auth_path = resolved_auth_path(account, &runtime);
    if operation.binding["auth_source_path"].as_str()
        != Some(expected_auth_path.to_string_lossy().as_ref())
    {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "legacy prepared binding auth source does not match the native runtime context",
        ));
    }
    operation.binding["native_runtime_identity_sha256"] = json!(runtime.identity_sha256());
    operation.binding["quota_observer_identity_sha256"] = json!(observer.identity_sha256());
    operation.binding_sha256 = quota_refresh_binding_sha256(&operation.binding);
    write_quota_refresh_operation(host, &operation, request_id)?;
    Ok(operation)
}

fn quota_refresh_binding(
    params_sha256: &str,
    selection: &RuntimeSelection,
    runtime: &NativeRuntimeContext,
    observer: &QuotaObserverContext,
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
        "native_runtime_identity_sha256": runtime.identity_sha256(),
        "quota_observer_identity_sha256": observer.identity_sha256(),
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
    host: &HostContext,
    operation: &QuotaRefreshOperation,
    request_id: &str,
) -> Result<
    (
        &'static AccountProfile,
        NativeRuntimeContext,
        QuotaObserverContext,
        PathBuf,
    ),
    ProviderFailure,
> {
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
    let runtime = native_runtime::resolve_for_account(host, account, request_id)?;
    if operation.binding["native_runtime_identity_sha256"].as_str()
        != Some(runtime.identity_sha256())
    {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "binding native runtime identity is inconsistent",
        ));
    }
    let observer = quota_observer::resolve(host, account, request_id)?;
    if operation.binding["quota_observer_identity_sha256"].as_str()
        != Some(observer.identity_sha256())
    {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "binding quota observer identity is inconsistent",
        ));
    }
    Ok((account, runtime, observer, auth_path))
}

fn quota_refresh_binding_sha256(binding: &Value) -> String {
    sha256_hex(binding.to_string().as_bytes())
}

fn acquire_quota_refresh_request_lock(
    host: &HostContext,
    reservation_binding_sha256: &str,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let name = sha256_hex(request_id.as_bytes());
    let root = quota_refresh_state_root(host, request_id)?;
    let lock_root = confined_quota_refresh_target(host, &root.join("locks/requests"), request_id)?;
    durable_fs::create_private_directories(&lock_root)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    let capacity_lock = open_quota_refresh_lock_file(&lock_root.join(".capacity.lock"))
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    lock_quota_refresh_file(host, &capacity_lock, request_id)?;
    let lock_path = lock_root.join(format!("{name}.lock"));
    let state_path = quota_refresh_operation_path(host, request_id)?;
    let custody = quota_refresh_custody(host, &root, &lock_root, request_id)?;
    let lock_exists = lock_path.exists();
    let state_exists = state_path.exists();
    let replay_owner_exists = custody
        .replay_owner_exists(&state_path)
        .map_err(|error| quota_refresh_custody_failure(request_id, error))?;
    let active = maintain_quota_refresh_capacity(&custody, &lock_path, request_id)?;
    let mut active_reservation = custody
        .active_reservation(&state_path, reservation_binding_sha256)
        .map_err(|error| quota_refresh_custody_failure(request_id, error))?;
    if active_reservation == ActiveReservation::Unbound
        && !lock_exists
        && !state_exists
        && !replay_owner_exists
    {
        custody
            .bind_unbound_active(&state_path, reservation_binding_sha256)
            .map_err(|error| quota_refresh_custody_failure(request_id, error))?;
        active_reservation = ActiveReservation::Matching;
    }
    if active_reservation == ActiveReservation::Conflicting {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "the active request reservation belongs to different quota.refresh_auth inputs",
        ));
    }
    let active_marker_exists = active_reservation != ActiveReservation::Absent;
    let resumes_pre_state_reservation =
        active_reservation == ActiveReservation::Matching && !state_exists && !replay_owner_exists;
    let observed_existing =
        lock_exists || state_exists || replay_owner_exists || active_marker_exists;
    let reserved = !observed_existing;
    if reserved && active >= MAX_ACTIVE_QUOTA_REFRESH_REQUEST_RECORDS {
        return Err(quota_refresh_state_capacity_exceeded(request_id));
    }
    if reserved {
        custody
            .reserve_active(&state_path, reservation_binding_sha256)
            .map_err(|error| quota_refresh_custody_failure(request_id, error))?;
    }
    let replay_pin = if observed_existing {
        Some(
            custody
                .pin_existing(&state_path)
                .map_err(|error| quota_refresh_custody_failure(request_id, error))?,
        )
    } else {
        None
    };
    let lock = match open_quota_refresh_lock_file(&lock_path) {
        Ok(lock) => lock,
        Err(error) => {
            if reserved {
                custody
                    .remove_active_marker(&state_path)
                    .map_err(|cleanup| quota_refresh_custody_failure(request_id, cleanup))?;
            }
            return Err(quota_refresh_state_failure(request_id, error));
        }
    };
    drop(capacity_lock);
    if let Err(error) = lock_quota_refresh_file(host, &lock, request_id) {
        drop(lock);
        if reserved {
            retire_orphan_quota_refresh_request(
                &custody,
                &lock_root,
                &state_path,
                &lock_path,
                request_id,
            )?;
        }
        return Err(error);
    }
    drop(replay_pin);
    custody
        .release_pin_after_lock(&state_path)
        .map_err(|error| quota_refresh_custody_failure(request_id, error))?;
    if observed_existing && !resumes_pre_state_reservation && !state_path.exists() {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "an observed request replay is still retiring its durable state",
        ));
    }
    Ok(lock)
}

struct QuotaRefreshEffectLock {
    _credential_path_lock: fs::File,
    _credential_file_lock: Option<fs::File>,
}

fn acquire_quota_refresh_effect_lock(
    host: &HostContext,
    auth_path: &Path,
    request_id: &str,
) -> Result<QuotaRefreshEffectLock, ProviderFailure> {
    let canonical_auth_path = canonical_credential_path(auth_path)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    let parent = canonical_auth_path.parent().ok_or_else(|| {
        quota_refresh_state_failure(request_id, "credential source has no parent directory")
    })?;
    let path_lock = open_quota_refresh_lock_file(
        &parent.join(".oulipoly-agent-runner-opencode-quota-refresh.lock"),
    )
    .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    lock_quota_refresh_file(host, &path_lock, request_id)?;
    let credential_file_lock = match OpenOptions::new().read(true).write(true).open(auth_path) {
        Ok(lock) => {
            lock_quota_refresh_file(host, &lock, request_id)?;
            Some(lock)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(quota_refresh_state_failure(request_id, error)),
    };
    Ok(QuotaRefreshEffectLock {
        _credential_path_lock: path_lock,
        _credential_file_lock: credential_file_lock,
    })
}

fn canonical_credential_path(auth_path: &Path) -> std::io::Result<PathBuf> {
    match fs::canonicalize(auth_path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = auth_path.parent().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "credential source has no parent directory",
                )
            })?;
            let file_name = auth_path.file_name().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "credential source has no file name",
                )
            })?;
            fs::canonicalize(parent).map(|parent| parent.join(file_name))
        }
        Err(error) => Err(error),
    }
}

fn open_quota_refresh_lock_file(path: &Path) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn maintain_quota_refresh_capacity(
    custody: &RequestCustody,
    current_lock_path: &Path,
    request_id: &str,
) -> Result<usize, ProviderFailure> {
    custody
        .maintain(current_lock_path, quota_refresh_bytes_are_replay)
        .map_err(|error| quota_refresh_custody_failure(request_id, error))
}

fn quota_refresh_bytes_are_replay(bytes: &[u8]) -> Result<bool, String> {
    let operation: QuotaRefreshOperation =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    Ok(operation.phase == QuotaRefreshOperationPhase::Committed)
}

fn quota_refresh_custody(
    host: &HostContext,
    root: &Path,
    lock_root: &Path,
    request_id: &str,
) -> Result<RequestCustody, ProviderFailure> {
    let request_root = confined_quota_refresh_target(host, &root.join("requests"), request_id)?;
    durable_fs::create_private_directories(&request_root)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    Ok(RequestCustody::new_fixed(
        request_root,
        lock_root.to_path_buf(),
        root.join(".custody-v2"),
        MAX_QUOTA_REFRESH_STATE_BYTES,
        MAX_ACTIVE_QUOTA_REFRESH_REQUEST_RECORDS,
        MAX_QUOTA_REFRESH_REPLAY_RECORDS,
        QUOTA_REFRESH_ORPHAN_RETENTION,
    ))
}

fn retire_orphan_quota_refresh_request(
    custody: &RequestCustody,
    lock_root: &Path,
    state_path: &Path,
    lock_path: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let capacity = open_quota_refresh_lock_file(&lock_root.join(".capacity.lock"))
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    if !operation_bounds::lock_exclusive_for(&capacity, QUOTA_REFRESH_CLEANUP_LOCK_TIMEOUT)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?
    {
        return Err(quota_refresh_lock_timeout(request_id));
    }
    if state_path.exists() {
        return Ok(());
    }
    let lock = open_quota_refresh_lock_file(lock_path)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    match fs2::FileExt::try_lock_exclusive(&lock) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
        Err(error) => return Err(quota_refresh_state_failure(request_id, error)),
    }
    if state_path.exists() {
        return Ok(());
    }
    custody
        .remove_active_marker(state_path)
        .map_err(|error| quota_refresh_custody_failure(request_id, error))?;
    match fs::remove_file(lock_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(quota_refresh_state_failure(request_id, error)),
    }
    durable_fs::sync_directory(lock_root)
        .map_err(|error| quota_refresh_state_failure(request_id, error))
}

fn quota_refresh_custody_failure(request_id: &str, error: CustodyError) -> ProviderFailure {
    match error {
        CustodyError::Capacity => quota_refresh_state_capacity_exceeded(request_id),
        CustodyError::Invalid(error) => quota_refresh_operation_invalid(request_id, error),
        CustodyError::Migration(error) => quota_refresh_operation_invalid(request_id, error),
        CustodyError::Io(error) => quota_refresh_state_failure(request_id, error),
    }
}

fn lock_quota_refresh_file(
    host: &HostContext,
    lock: &fs::File,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let timeout = quota_refresh_operation_timeout(host, request_id)?;
    if !operation_bounds::lock_exclusive_for(lock, timeout)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?
    {
        return Err(quota_refresh_lock_timeout(request_id));
    }
    Ok(())
}

fn quota_refresh_operation_timeout(
    host: &HostContext,
    request_id: &str,
) -> Result<Duration, ProviderFailure> {
    operation_bounds::remaining_timeout(host.deadline_unix_ms, QUOTA_REFRESH_OPERATION_TIMEOUT)
        .ok_or_else(|| quota_refresh_deadline_exceeded(request_id))
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
    let bytes = match durable_fs::read_file_bounded(&path, MAX_QUOTA_REFRESH_STATE_BYTES) {
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
    let actor_absent = operation.actor_process_group_id.is_none()
        && operation.actor_process_group_incarnation.is_none()
        && operation.actor_terminal_at_unix_ms.is_none();
    let actor_live_or_unsettled = operation.actor_process_group_id.is_some_and(|id| id > 0)
        && operation
            .actor_process_group_incarnation
            .as_ref()
            .is_some_and(|incarnation| !incarnation.trim().is_empty())
        && operation.actor_terminal_at_unix_ms.is_none();
    let actor_terminal = operation.actor_process_group_id.is_some_and(|id| id > 0)
        && operation
            .actor_process_group_incarnation
            .as_ref()
            .is_some_and(|incarnation| !incarnation.trim().is_empty())
        && operation.actor_terminal_at_unix_ms.is_some();
    let reconciliation_valid = operation
        .reconciliation
        .as_ref()
        .is_none_or(valid_quota_refresh_reconciliation);
    let phase_valid = match operation.phase {
        QuotaRefreshOperationPhase::Prepared => {
            operation.native_effect_admitted_at_unix_ms.is_none()
                && actor_absent
                && operation.committed_at_unix_ms.is_none()
                && operation.result.is_none()
                && operation.reconciliation.is_none()
        }
        QuotaRefreshOperationPhase::NativeEffectAdmitted => {
            operation.native_effect_admitted_at_unix_ms.is_some()
                && actor_live_or_unsettled
                && operation.committed_at_unix_ms.is_none()
                && operation.result.is_none()
                && operation.reconciliation.is_none()
        }
        QuotaRefreshOperationPhase::ReconciliationRequired => {
            operation.native_effect_admitted_at_unix_ms.is_some()
                && actor_terminal
                && operation.committed_at_unix_ms.is_none()
                && operation.result.is_none()
                && operation
                    .reconciliation
                    .as_ref()
                    .is_none_or(|evidence| evidence.get("resolution").is_none())
        }
        QuotaRefreshOperationPhase::Committed => {
            ((operation.native_effect_admitted_at_unix_ms.is_some() && actor_terminal)
                || (operation.native_effect_admitted_at_unix_ms.is_none() && actor_absent))
                && operation.committed_at_unix_ms.is_some()
                && operation.result.is_some()
                && operation.reconciliation.as_ref().is_none_or(|evidence| {
                    evidence
                        .get("resolution")
                        .is_some_and(valid_quota_refresh_resolution)
                })
        }
    };
    if operation.schema_version != QUOTA_REFRESH_SCHEMA_VERSION
        || operation.operation != "quota.refresh_auth"
        || operation.request_id != request_id
        || operation.binding_sha256 != quota_refresh_binding_sha256(&operation.binding)
        || operation.binding_sha256.trim().is_empty()
        || !reconciliation_valid
        || !phase_valid
    {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "operation identity, binding, or phase is inconsistent",
        ));
    }
    Ok(())
}

fn valid_quota_refresh_reconciliation(reconciliation: &Value) -> bool {
    reconciliation
        .get("reason")
        .and_then(Value::as_str)
        .is_some_and(|reason| !reason.trim().is_empty())
        && matches!(
            reconciliation
                .get("credential_effect")
                .and_then(Value::as_str),
            Some("credentials_changed" | "unobservable" | "unobserved")
        )
        && reconciliation
            .get("detail")
            .and_then(Value::as_str)
            .is_some_and(|detail| {
                !detail.trim().is_empty()
                    && detail.len() <= MAX_QUOTA_REFRESH_RECONCILIATION_DETAIL_BYTES
            })
        && reconciliation
            .get("observed_at_unix_ms")
            .and_then(Value::as_u64)
            .is_some()
        && reconciliation
            .get("resolution")
            .is_none_or(valid_quota_refresh_resolution)
}

fn valid_quota_refresh_resolution(resolution: &Value) -> bool {
    resolution.get("disposition").and_then(Value::as_str) == Some("accept_current_credentials")
        && resolution
            .get("credential_source_sha256")
            .and_then(Value::as_str)
            .is_some_and(is_sha256_hex)
        && resolution
            .get("accepted_at_unix_ms")
            .and_then(Value::as_u64)
            .is_some()
}

fn write_quota_refresh_operation(
    host: &HostContext,
    operation: &QuotaRefreshOperation,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    validate_quota_refresh_operation(operation, request_id)?;
    let path = quota_refresh_operation_path(host, request_id)?;
    let parent = path
        .parent()
        .expect("quota refresh operation always has a parent");
    durable_fs::create_private_directories(parent)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    let bytes = serde_json::to_vec_pretty(operation)
        .map_err(|error| quota_refresh_state_failure(request_id, error))?;
    if bytes.len() > MAX_QUOTA_REFRESH_STATE_BYTES {
        return Err(quota_refresh_state_capacity_exceeded(request_id));
    }
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
            "reconciliation_evidence": operation.reconciliation,
            "recovery": "inspect the bound credential source, then retry this original request with params.context.reconciliation.disposition=accept_current_credentials and params.context.reconciliation.credential_source_sha256 set to the lowercase SHA-256 of the credential file you accepted",
        }),
    )
}

fn quota_refresh_reconciliation_not_required(request_id: &str) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "quota_refresh_reconciliation_not_required",
        "quota.refresh_auth reconciliation is accepted only for the original reconciliation-required request",
        json!({}),
    )
}

fn quota_refresh_actor_cleanup_failed(
    request_id: &str,
    operation: &QuotaRefreshOperation,
    error: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "quota_refresh_actor_cleanup_failed",
        "the provider could not terminate and prove discharge of the exact native auth actor recorded by this request; no credential reconciliation was accepted",
        json!({
            "binding_sha256": operation.binding_sha256,
            "actor_process_group_id": operation.actor_process_group_id,
            "actor_process_group_incarnation": operation.actor_process_group_incarnation,
            "failure": error.to_string(),
            "recovery": "retry this unchanged quota.refresh_auth request; the provider will safely ignore a recycled process-group identity and will not accept credential state until the recorded actor is terminal",
        }),
    )
}

fn invalid_quota_refresh_reconciliation_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_quota_refresh_reconciliation",
        "reconciliation requires disposition=accept_current_credentials and the lowercase SHA-256 of the current bound credential source",
    )
}

fn quota_refresh_reconciliation_mismatch(
    request_id: &str,
    supplied_sha256: &str,
    observed_sha256: &str,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "quota_refresh_reconciliation_mismatch",
        "the bound credential source changed after the caller's reconciliation observation",
        json!({
            "supplied_credential_source_sha256": supplied_sha256,
            "observed_credential_source_sha256": observed_sha256,
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

fn quota_refresh_lock_timeout(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "quota_refresh_lock_timeout",
        "quota refresh lock could not be acquired before the operation deadline",
    )
}

fn quota_refresh_state_capacity_exceeded(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "quota_refresh_state_capacity_exceeded",
        format!(
            "durable quota.refresh_auth custody has reached its supported {MAX_ACTIVE_QUOTA_REFRESH_REQUEST_RECORDS}-request active bound or {MAX_QUOTA_REFRESH_REPLAY_RECORDS}-request completed replay bound; reconcile incomplete requests or allow the bounded recent-replay pool to retire its oldest completion"
        ),
    )
}

fn quota_refresh_deadline_exceeded(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "quota_refresh_deadline_exceeded",
        "quota refresh deadline was reached before the next native effect",
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

fn parse_refresh_params(
    params: Value,
    request_id: &str,
) -> Result<QuotaRefreshAuthParams, ProviderFailure> {
    serde_json::from_value(params)
        .map_err(|err| invalid_quota_refresh_params_failure(request_id, err))
}

fn opencode_command_failure_detail(output: &crate::shell::ShellOutput) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = bounded_text_bytes(stderr.trim(), MAX_QUOTA_REFRESH_RECONCILIATION_DETAIL_BYTES);
    if stderr.is_empty() {
        return format!("opencode auth list exited with status {}", output.status);
    }
    format!(
        "opencode auth list exited with status {}: {stderr}",
        output.status
    )
}
fn refresh_available(
    account: &AccountProfile,
    runtime: &NativeRuntimeContext,
    observer: &QuotaObserverContext,
) -> bool {
    quota_observation_without_refresh(account, runtime, observer).is_ok()
}

fn quota_observation_without_refresh(
    account: &AccountProfile,
    runtime: &NativeRuntimeContext,
    observer: &QuotaObserverContext,
) -> Result<QuotaObservation, QuotaObservationFailure> {
    run_probe(&resolved_auth_path(account, runtime), observer)
}

fn prepare_account_auth_refresh(
    runtime: &NativeRuntimeContext,
    auth_path: &Path,
) -> Result<PreparedOpencodeAuthObservation, OpencodeAuthFailure> {
    crate::opencode::prepare_auth_list(runtime, auth_path)
}

fn publish_quota_refresh_actor(
    host: &HostContext,
    operation: &mut QuotaRefreshOperation,
    actor: &ProcessGroupActor,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    operation.phase = QuotaRefreshOperationPhase::NativeEffectAdmitted;
    operation.native_effect_admitted_at_unix_ms = Some(now_unix_ms());
    operation.actor_process_group_id = Some(actor.process_group_id);
    operation.actor_process_group_incarnation = Some(actor.incarnation.clone());
    operation.actor_terminal_at_unix_ms = None;
    write_quota_refresh_operation(host, operation, request_id)
}

/// Settle the durably recorded auth actor while the caller owns the exact
/// request lock. If the original in-process supervisor was lost, that lock is
/// the exclusive successor authority for terminating the recorded incarnation.
fn settle_quota_refresh_actor(
    operation: &mut QuotaRefreshOperation,
    request_id: &str,
) -> Result<bool, ProviderFailure> {
    if operation.actor_terminal_at_unix_ms.is_some() {
        return Ok(false);
    }
    let (Some(process_group_id), Some(incarnation)) = (
        operation.actor_process_group_id,
        operation.actor_process_group_incarnation.as_ref(),
    ) else {
        return Err(quota_refresh_operation_invalid(
            request_id,
            "an effect-admitted quota refresh has no durable native actor incarnation",
        ));
    };
    let actor = ProcessGroupActor {
        process_group_id,
        incarnation: incarnation.clone(),
    };
    terminate_process_group_actor(&actor)
        .map_err(|error| quota_refresh_actor_cleanup_failed(request_id, operation, error))?;
    operation.actor_terminal_at_unix_ms = Some(now_unix_ms());
    Ok(true)
}

fn refresh_succeeded(refresh: &Result<OpencodeAuthObservation, OpencodeAuthFailure>) -> bool {
    refresh
        .as_ref()
        .is_ok_and(OpencodeAuthObservation::credentials_refreshed)
}

struct RefreshReconciliation {
    reason: &'static str,
    credential_effect: &'static str,
    detail: String,
}

fn refresh_reconciliation(
    refresh: &Result<OpencodeAuthObservation, OpencodeAuthFailure>,
) -> Option<RefreshReconciliation> {
    match refresh {
        Ok(observation)
            if observation.effect == OpencodeAuthEffect::CredentialStateUnobservable =>
        {
            Some(RefreshReconciliation {
                reason: "credential_state_unobservable",
                credential_effect: "unobservable",
                detail: refresh_detail(Ok(observation)),
            })
        }
        Ok(observation)
            if observation.effect == OpencodeAuthEffect::CredentialsChanged
                && !observation.observation_succeeded() =>
        {
            Some(RefreshReconciliation {
                reason: if observation.output_exceeded_bound {
                    "credential_changed_with_oversized_output"
                } else {
                    "credential_changed_with_command_failure"
                },
                credential_effect: "credentials_changed",
                detail: refresh_detail(Ok(observation)),
            })
        }
        Err(error) if error.effect_was_possible() => Some(RefreshReconciliation {
            reason: if error.kind() == std::io::ErrorKind::TimedOut {
                "native_auth_timed_out"
            } else {
                "native_auth_observation_failed"
            },
            credential_effect: "unobserved",
            detail: refresh_detail(Err(error)),
        }),
        Ok(_) | Err(_) => None,
    }
}

fn refresh_detail(refresh: Result<&OpencodeAuthObservation, &OpencodeAuthFailure>) -> String {
    match refresh {
        Ok(observation) if observation.observation_succeeded() => {
            auth_effect_detail(observation.effect).to_string()
        }
        Ok(observation) if observation.output_exceeded_bound => format!(
            "opencode auth list output exceeded the supported bound; {}",
            auth_effect_detail(observation.effect)
        ),
        Ok(observation) => format!(
            "opencode auth list failed: {}; {}",
            opencode_command_failure_detail(&observation.output),
            auth_effect_detail(observation.effect)
        ),
        Err(err) => format!("failed to run opencode auth list: {err}"),
    }
}

fn require_quota_refresh_reconciliation(
    operation: &mut QuotaRefreshOperation,
    reason: &str,
    credential_effect: &str,
    detail: &str,
) {
    operation.phase = QuotaRefreshOperationPhase::ReconciliationRequired;
    operation.reconciliation = Some(json!({
        "reason": reason,
        "credential_effect": credential_effect,
        "detail": bounded_text_bytes(detail, MAX_QUOTA_REFRESH_RECONCILIATION_DETAIL_BYTES),
        "observed_at_unix_ms": now_unix_ms(),
    }));
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

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}
#[cfg(test)]
mod custody_tests {
    use super::*;
    use crate::native_process::{actor_for_child, configure_process_group};
    use std::process::{Command as ProcessCommand, Stdio};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn exact_refresh_retry_resumes_pre_lock_reservation_at_active_capacity() {
        let directory = tempfile::tempdir().expect("quota custody directory");
        let host = HostContext {
            app: "test".to_string(),
            app_version: None,
            platform: None,
            working_directory: None,
            config_root: None,
            data_root: Some(directory.path().to_string_lossy().into_owned()),
            env: None,
            deadline_unix_ms: None,
        };
        let request_id = "request-exact-retry";
        let root = quota_refresh_state_root(&host, request_id).expect("quota state root");
        let request_root = root.join("requests");
        let lock_root = root.join("locks/requests");
        fs::create_dir_all(&request_root).expect("quota request root");
        fs::create_dir_all(&lock_root).expect("quota lock root");
        let custody = quota_refresh_custody(&host, &root, &lock_root, request_id)
            .expect("quota request custody");
        assert_eq!(
            custody
                .maintain(
                    &lock_root.join("initialize.lock"),
                    quota_refresh_bytes_are_replay,
                )
                .expect("initialize quota custody"),
            0
        );
        let target_stem = sha256_hex(request_id.as_bytes());
        let reservation_binding = sha256_hex(b"quota exact retry inputs");
        let target_state = request_root.join(format!("{target_stem}.json"));
        custody
            .reserve_active(&target_state, &reservation_binding)
            .expect("reserve exact quota request first");
        for index in 1..MAX_ACTIVE_QUOTA_REFRESH_REQUEST_RECORDS {
            let binding = format!("{index:064x}");
            custody
                .reserve_active(&request_root.join(format!("{index:064x}.json")), &binding)
                .expect("fill quota active reservations");
        }
        let overflow_binding = format!("{:064x}", MAX_ACTIVE_QUOTA_REFRESH_REQUEST_RECORDS + 1);
        assert!(matches!(
            custody.reserve_active(
                &request_root.join(format!(
                    "{:064x}.json",
                    MAX_ACTIVE_QUOTA_REFRESH_REQUEST_RECORDS + 1
                )),
                &overflow_binding,
            ),
            Err(CustodyError::Capacity)
        ));

        let lock = acquire_quota_refresh_request_lock(&host, &reservation_binding, request_id)
            .expect("exact refresh retry resumes its pre-lock reservation at capacity");
        assert!(!target_state.exists());
        drop(lock);
        let target_lock = lock_root.join(format!("{target_stem}.lock"));
        retire_orphan_quota_refresh_request(
            &custody,
            &lock_root,
            &target_state,
            &target_lock,
            request_id,
        )
        .expect("retire resumed pre-state quota reservation");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn terminal_auth_actor_is_durably_settled_once() {
        let mut command = ProcessCommand::new("/bin/sleep");
        command.arg("30");
        configure_process_group(&mut command);
        let mut actor_child = command.spawn().expect("spawn quota actor");
        let actor = actor_for_child(&actor_child).expect("identify quota actor");
        let mut operation =
            quota_custody_operation(1, QuotaRefreshOperationPhase::NativeEffectAdmitted);
        operation.actor_process_group_id = Some(actor.process_group_id);
        operation.actor_process_group_incarnation = Some(actor.incarnation);
        operation.actor_terminal_at_unix_ms = None;

        actor_child.kill().expect("terminate quota actor");
        actor_child.wait().expect("reap quota actor");
        assert!(settle_quota_refresh_actor(&mut operation, "request-1")
            .expect("terminal actor permits credential reconciliation"));
        assert!(operation.actor_terminal_at_unix_ms.is_some());
        assert!(!settle_quota_refresh_actor(&mut operation, "request-1")
            .expect("terminal settlement is idempotent"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn quota_retry_terminates_a_recorded_group_after_its_leader_exits() {
        let mut command = ProcessCommand::new("/bin/sh");
        command
            .args(["-c", "sleep 30 </dev/null >/dev/null 2>&1 & exit 0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        configure_process_group(&mut command);
        let mut child = command.spawn().expect("spawn quota group leader");
        let actor = actor_for_child(&child).expect("identify quota process group");
        let recorded_actor = actor.clone();
        assert!(child.wait().expect("observe direct leader exit").success());
        let mut operation =
            quota_custody_operation(1, QuotaRefreshOperationPhase::NativeEffectAdmitted);
        operation.actor_process_group_id = Some(actor.process_group_id);
        operation.actor_process_group_incarnation = Some(actor.incarnation);
        operation.actor_terminal_at_unix_ms = None;

        assert!(settle_quota_refresh_actor(&mut operation, "request-1")
            .expect("exact retry terminates the orphaned process group"));
        assert!(operation.actor_terminal_at_unix_ms.is_some());
        assert!(
            crate::native_process::actor_is_terminal_or_recycled(&recorded_actor)
                .expect("verify quota actor terminality")
        );
    }

    #[test]
    fn committed_history_does_not_consume_active_refresh_capacity() {
        let directory = tempfile::tempdir().expect("quota custody directory");
        let data_root = directory.path().to_string_lossy().into_owned();
        let host = HostContext {
            app: "test".to_string(),
            app_version: None,
            platform: None,
            working_directory: None,
            config_root: None,
            data_root: Some(data_root),
            env: None,
            deadline_unix_ms: None,
        };
        let root = quota_refresh_state_root(&host, "request-test").expect("quota state root");
        let request_root = root.join("requests");
        let lock_root = root.join("locks/requests");
        fs::create_dir_all(&request_root).expect("quota request root");
        fs::create_dir_all(&lock_root).expect("quota lock root");
        let completed_records = 2;
        for index in 0..completed_records {
            let stem = format!("{index:064x}");
            fs::write(lock_root.join(format!("{stem}.lock")), b"").expect("quota request lock");
            let operation = QuotaRefreshOperation {
                schema_version: QUOTA_REFRESH_SCHEMA_VERSION,
                operation: "quota.refresh_auth".to_string(),
                request_id: format!("request-{index}"),
                binding_sha256: "binding".to_string(),
                binding: json!({}),
                phase: QuotaRefreshOperationPhase::Committed,
                prepared_at_unix_ms: 1,
                native_effect_admitted_at_unix_ms: Some(2),
                actor_process_group_id: Some(123),
                actor_process_group_incarnation: Some("test:actor:1".to_string()),
                actor_terminal_at_unix_ms: Some(3),
                committed_at_unix_ms: Some(3),
                result: Some(json!({"available": true})),
                reconciliation: None,
            };
            fs::write(
                request_root.join(format!("{stem}.json")),
                serde_json::to_vec(&operation).expect("serialize quota operation"),
            )
            .expect("committed quota record");
        }

        let custody = RequestCustody::new_fixed(
            request_root,
            lock_root.clone(),
            root.join(".custody-v2"),
            MAX_QUOTA_REFRESH_STATE_BYTES,
            1,
            completed_records,
            QUOTA_REFRESH_ORPHAN_RETENTION,
        );
        let active =
            maintain_quota_refresh_capacity(&custody, &lock_root.join("new.lock"), "request-test")
                .expect("classify bounded quota custody");

        assert_eq!(active, 0);
        let replay = fs::read_dir(root.join(".custody-v2/replay"))
            .expect("quota replay ring")
            .count();
        assert_eq!(replay, completed_records);

        let replay_parses = AtomicUsize::new(0);
        let active = custody
            .maintain(&lock_root.join("new.lock"), |bytes| {
                replay_parses.fetch_add(1, Ordering::Relaxed);
                quota_refresh_bytes_are_replay(bytes)
            })
            .expect("maintain indexed quota custody");
        assert_eq!(active, 0);
        assert_eq!(
            replay_parses.load(Ordering::Relaxed),
            0,
            "steady-state admission must not parse replay payloads"
        );
    }

    #[test]
    fn interrupted_replay_handoff_is_idempotent_for_quota_requests() {
        let directory = tempfile::tempdir().expect("quota custody directory");
        let request_root = directory.path().join("requests");
        let lock_root = directory.path().join("locks");
        fs::create_dir_all(&request_root).expect("quota request root");
        fs::create_dir_all(&lock_root).expect("quota lock root");
        let custody = RequestCustody::new_fixed(
            request_root.clone(),
            lock_root.clone(),
            directory.path().join(".custody-v2"),
            MAX_QUOTA_REFRESH_STATE_BYTES,
            2,
            2,
            QUOTA_REFRESH_ORPHAN_RETENTION,
        );
        let first = format!("{:064x}", 1);
        let first_state = request_root.join(format!("{first}.json"));
        let first_lock = lock_root.join(format!("{first}.lock"));
        fs::write(&first_lock, b"").expect("first quota request lock");
        fs::write(
            &first_state,
            serde_json::to_vec(&quota_custody_operation(
                1,
                QuotaRefreshOperationPhase::Prepared,
            ))
            .expect("serialize active quota operation"),
        )
        .expect("first active quota state");
        assert_eq!(
            custody
                .maintain(
                    &lock_root.join("current.lock"),
                    quota_refresh_bytes_are_replay
                )
                .expect("initialize quota custody"),
            1
        );

        fs::write(
            &first_state,
            serde_json::to_vec(&quota_custody_operation(
                1,
                QuotaRefreshOperationPhase::Committed,
            ))
            .expect("serialize terminal quota operation"),
        )
        .expect("complete first quota state");
        assert!(custody
            .publish_replay_without_retiring_active(&first_state, &lock_root.join("current.lock"))
            .expect("publish quota replay before simulated interruption"));
        assert_eq!(
            custody
                .maintain(
                    &lock_root.join("current.lock"),
                    quota_refresh_bytes_are_replay
                )
                .expect("resume interrupted quota handoff"),
            0
        );
        assert_eq!(quota_replay_references(directory.path(), &first), 1);
        let recovered: QuotaRefreshOperation =
            serde_json::from_slice(&fs::read(&first_state).expect("recover first quota terminal"))
                .expect("parse recovered quota terminal");
        assert!(recovered.phase == QuotaRefreshOperationPhase::Committed);

        for index in 2..=3 {
            let stem = format!("{index:064x}");
            let state = request_root.join(format!("{stem}.json"));
            fs::write(lock_root.join(format!("{stem}.lock")), b"")
                .expect("later quota request lock");
            fs::write(
                &state,
                serde_json::to_vec(&quota_custody_operation(
                    index,
                    QuotaRefreshOperationPhase::Committed,
                ))
                .expect("serialize later quota operation"),
            )
            .expect("later terminal quota state");
            custody
                .reserve_active(&state, &stem)
                .expect("reserve later quota request");
            assert_eq!(
                custody
                    .maintain(
                        &lock_root.join("current.lock"),
                        quota_refresh_bytes_are_replay
                    )
                    .expect("place later quota replay"),
                0
            );
        }
        assert!(
            !first_state.exists(),
            "the oldest quota replay is evicted once"
        );
        assert_eq!(quota_replay_references(directory.path(), &first), 0);
        assert_eq!(
            quota_replay_references(directory.path(), &format!("{:064x}", 2)),
            1
        );
        assert_eq!(
            quota_replay_references(directory.path(), &format!("{:064x}", 3)),
            1
        );
    }

    fn quota_custody_operation(
        index: usize,
        phase: QuotaRefreshOperationPhase,
    ) -> QuotaRefreshOperation {
        let effect_admitted = phase != QuotaRefreshOperationPhase::Prepared;
        let actor_terminal = phase == QuotaRefreshOperationPhase::Committed;
        QuotaRefreshOperation {
            schema_version: QUOTA_REFRESH_SCHEMA_VERSION,
            operation: "quota.refresh_auth".to_string(),
            request_id: format!("request-{index}"),
            binding_sha256: "binding".to_string(),
            binding: json!({}),
            phase,
            prepared_at_unix_ms: 1,
            native_effect_admitted_at_unix_ms: effect_admitted.then_some(2),
            actor_process_group_id: effect_admitted.then_some(123),
            actor_process_group_incarnation: effect_admitted.then(|| format!("test:actor:{index}")),
            actor_terminal_at_unix_ms: actor_terminal.then_some(3),
            committed_at_unix_ms: (phase == QuotaRefreshOperationPhase::Committed).then_some(3),
            result: (phase == QuotaRefreshOperationPhase::Committed)
                .then(|| json!({"available": true})),
            reconciliation: None,
        }
    }

    fn quota_replay_references(root: &Path, stem: &str) -> usize {
        fs::read_dir(root.join(".custody-v2/replay"))
            .expect("quota replay ring")
            .map(|entry| entry.expect("quota replay entry").path())
            .filter_map(|path| fs::read(path).ok())
            .filter_map(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .filter(|record| record["request_sha256"].as_str() == Some(stem))
            .count()
    }
}
