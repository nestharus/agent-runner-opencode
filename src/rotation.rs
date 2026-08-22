//! Declared roles: mapper, validator, predicate, filter, formatter

use crate::account::{profile_for_account_reference, AccountProfile};
use crate::activity::ActivityTargets;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use crate::native_process::{actor_is_terminal_or_recycled, ProcessGroupActor};
use crate::native_runtime;
use crate::opencode::{self, OpencodeExportError};
use crate::operation_bounds;
use crate::path_guard;
use crate::runtime_selection::resolve_runtime_selection;
use crate::schema::ROTATION_DECISION_SCHEMA_ID;
use crate::settings;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const AUTHORIZATION_TTL: Duration = Duration::from_secs(10 * 60);
const ROTATION_REPLAY_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const ROTATION_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_ROTATION_ARTIFACT_BYTES: usize = opencode::MAX_EXPORT_OUTPUT_BYTES;
const MAX_ROTATION_STATE_BYTES: usize = 1024 * 1024;
const MAX_ROTATION_LIVE_RECORDS: usize = 64;
const ROTATION_BINDING_LOCK_STRIPES: u8 = 64;
const ROTATION_RESERVATION_RETENTION: Duration = Duration::from_secs(2 * 60);
const MAX_ROTATION_ARTIFACT_RECORDS: usize = MAX_ROTATION_LIVE_RECORDS * 2;
const MAX_ROTATION_COMPATIBILITY_RECORDS: usize = 4096;
const ROTATION_STATE_DIR: &str = "provider-state/opencode/rotation";
const ROTATION_OPERATION_SCHEMA_VERSION: u32 = 2;

#[derive(Default)]
struct RotationCapacity {
    authorizations: usize,
    materializations: usize,
    operations: usize,
    reservations: usize,
    artifacts: usize,
    decisions: usize,
}

struct RotationReservationGuard {
    path: PathBuf,
    armed: bool,
}

impl RotationReservationGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RotationReservationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if fs::remove_file(&self.path).is_ok() {
            if let Some(parent) = self.path.parent() {
                let _ = durable_fs::sync_directory(parent);
            }
        }
    }
}

struct RotationBudget {
    started: Instant,
    host_deadline_unix_ms: Option<u64>,
}

impl RotationBudget {
    fn new(host: &HostContext) -> Self {
        Self {
            started: Instant::now(),
            host_deadline_unix_ms: host.deadline_unix_ms,
        }
    }

    fn remaining(&self, request_id: &str) -> Result<Duration, ProviderFailure> {
        let provider_remaining = ROTATION_OPERATION_TIMEOUT.saturating_sub(self.started.elapsed());
        if provider_remaining.is_zero() {
            return Err(rotation_deadline_exceeded(request_id));
        }
        operation_bounds::remaining_timeout(self.host_deadline_unix_ms, provider_remaining)
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| rotation_deadline_exceeded(request_id))
    }

    fn checkpoint(&self, request_id: &str) -> Result<(), ProviderFailure> {
        self.remaining(request_id).map(|_| ())
    }
}

pub fn assess_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
    provider_instance_id: &str,
) -> Result<Value, ProviderFailure> {
    let budget = RotationBudget::new(host);
    let requirements = requirements(&params);
    let met = requirements_met(&requirements);
    let facts_allow = facts_allow_rotation(&params);
    let binding = assessment_rotation_binding(&params, host, provider_instance_id, request_id)?;
    let allowed = met
        && facts_allow
        && binding.source_provider_id != binding.target_provider_id
        && binding.source_account.opencode_wrapper != binding.target_account.opencode_wrapper;
    let _binding_lock = acquire_rotation_binding_lock(host, &binding, &budget, request_id)?;
    let _capacity_lock = acquire_rotation_capacity_lock(host, &budget, request_id)?;
    budget.checkpoint(request_id)?;
    let capacity = maintain_rotation_capacity(host, &binding, &budget, request_id)?;
    if allowed
        && !authorization_path(host, &binding, request_id)?.exists()
        && capacity.authorizations >= MAX_ROTATION_LIVE_RECORDS
    {
        return Err(rotation_state_capacity_exceeded(
            request_id,
            "authorizations",
        ));
    }
    let authorization = persist_assessment_decision(host, &binding, allowed, request_id)?;
    budget.checkpoint(request_id)?;
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
    let budget = RotationBudget::new(host);
    let binding =
        materialization_rotation_binding(&params, host, provider_instance_id, request_id)?;
    let _binding_lock = acquire_rotation_binding_lock(host, &binding, &budget, request_id)?;
    let capacity_lock = acquire_rotation_capacity_lock(host, &budget, request_id)?;
    budget.checkpoint(request_id)?;
    let capacity = maintain_rotation_capacity(host, &binding, &budget, request_id)?;
    if let Some(result) = read_materialization_receipt(host, &binding, request_id)? {
        budget.checkpoint(request_id)?;
        return Ok(result);
    }
    budget.checkpoint(request_id)?;
    if let Some(mut operation) = read_rotation_operation(host, &binding, request_id)? {
        drop(capacity_lock);
        budget.checkpoint(request_id)?;
        validate_rotation_settings_for_operation(host, &params, &binding, &operation, request_id)?;
        if operation.phase == RotationOperationPhase::Prepared {
            let target_runtime = native_runtime::resolve_for_account_with_timeout(
                host,
                binding.target_account,
                budget.remaining(request_id)?,
                request_id,
            )?;
            execute_or_reconcile_prepared_operation(
                host,
                &params,
                &binding,
                &target_runtime,
                &mut operation,
                &budget,
                request_id,
            )?;
        }
        return finalize_rotation_operation(
            host, &params, &binding, &operation, &budget, request_id,
        );
    }
    let reservation_path = rotation_reservation_path(host, &binding, request_id)?;
    let reservation_exists = reservation_path.exists();
    if !reservation_exists
        && capacity
            .materializations
            .saturating_add(capacity.operations)
            .saturating_add(capacity.reservations)
            >= MAX_ROTATION_LIVE_RECORDS
    {
        return Err(rotation_state_capacity_exceeded(
            request_id,
            "reservations, operations, and materialization receipts",
        ));
    }
    if !reservation_exists
        && (capacity.artifacts.saturating_add(capacity.reservations)
            >= MAX_ROTATION_ARTIFACT_RECORDS
            || capacity.decisions.saturating_add(capacity.reservations)
                >= MAX_ROTATION_ARTIFACT_RECORDS)
    {
        return Err(rotation_state_capacity_exceeded(
            request_id,
            "artifacts or decisions",
        ));
    }
    let authorization = require_fresh_authorization(host, &binding, request_id)?;
    validate_rotation_settings_before_effect(host, &binding, request_id)?;
    let mut reservation =
        persist_rotation_reservation(&reservation_path, &binding, request_id, reservation_exists)?;
    drop(capacity_lock);
    budget.checkpoint(request_id)?;
    let working_directory = rotation_working_directory(host, request_id)?;
    let source_runtime = native_runtime::resolve_for_account_with_timeout(
        host,
        binding.source_account,
        budget.remaining(request_id)?,
        request_id,
    )?;
    budget.checkpoint(request_id)?;
    let target_runtime = native_runtime::resolve_for_account_with_timeout(
        host,
        binding.target_account,
        budget.remaining(request_id)?,
        request_id,
    )?;
    budget.checkpoint(request_id)?;
    let native = opencode::export_with_timeout(
        &binding.source_session_id,
        &source_runtime,
        budget.remaining(request_id)?,
    )
    .map_err(|error| rotation_export_failure(request_id, &binding.source_session_id, error))?;
    budget.checkpoint(request_id)?;
    validate_rotation_export(&native, &binding.source_session_id, request_id)?;
    let boundary = crate::session::rotation_boundary_timestamp(&native)
        .ok_or_else(|| rotation_boundary_missing(request_id, &binding.source_session_id))?;
    let artifact_bytes = serde_json::to_vec(native.native_json())
        .map_err(|error| rotation_artifact_failure(request_id, error))?;
    if artifact_bytes.len() > MAX_ROTATION_ARTIFACT_BYTES {
        return Err(rotation_artifact_capacity_exceeded(
            request_id,
            artifact_bytes.len(),
        ));
    }
    budget.checkpoint(request_id)?;
    let artifact_path = rotation_artifact_path(host, &artifact_bytes, request_id)?;
    write_artifact_atomic(&artifact_path, &artifact_bytes)
        .map_err(|error| rotation_artifact_failure(request_id, error))?;
    budget.checkpoint(request_id)?;
    let mut operation = RotationOperation {
        schema_version: ROTATION_OPERATION_SCHEMA_VERSION,
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
        import_actor_process_group_id: None,
        import_actor_process_group_incarnation: None,
        import_actor_terminal_at_unix_ms: None,
        target_session_id: None,
        import_candidate_session_id: None,
        imported_at_unix_ms: None,
    };
    let operation_capacity_lock = acquire_rotation_capacity_lock(host, &budget, request_id)?;
    write_rotation_operation(host, &binding, &operation, request_id)?;
    remove_rotation_record(&reservation_path, request_id)?;
    reservation.disarm();
    drop(operation_capacity_lock);
    budget.checkpoint(request_id)?;
    admit_and_observe_import(
        host,
        &binding,
        &target_runtime,
        &mut operation,
        working_directory,
        &budget,
        request_id,
    )?;
    finalize_rotation_operation(host, &params, &binding, &operation, &budget, request_id)
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
    settings_selection: Option<RotationSettingsSelection>,
    transition_reason: String,
    provider_instance_id: String,
    host_app: String,
}

#[derive(Clone)]
struct RotationSettingsSelection {
    record_id: String,
    record_version: String,
    account: &'static AccountProfile,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RotationSettingsReconciliation {
    settings_id: String,
    settings_version: String,
    settings_account: String,
    target_provider_session_id: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    import_actor_process_group_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    import_actor_process_group_incarnation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    import_actor_terminal_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    import_candidate_session_id: Option<String>,
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
    let source_account = rotation_account(source_account_reference, request_id, "source")?;
    let target_account = rotation_account(target_account_reference, request_id, "target")?;
    Ok(RotationBinding {
        chain_id: required_string(params, "chain_id", request_id)?.to_string(),
        source_provider_id: required_string(params, "source_provider", request_id)?.to_string(),
        target_provider_id: required_string(params, "target_provider", request_id)?.to_string(),
        source_account,
        target_account,
        source_session_id: required_string(params, "source_session_id", request_id)?.to_string(),
        model_name: optional_string(params, "model_name")
            .unwrap_or("")
            .to_string(),
        settings_selection: None,
        transition_reason: transition_reason(params).to_string(),
        provider_instance_id: provider_instance_id.to_string(),
        host_app: host.app.clone(),
    })
}

fn assessment_rotation_binding(
    params: &Value,
    host: &HostContext,
    provider_instance_id: &str,
    request_id: &str,
) -> Result<RotationBinding, ProviderFailure> {
    let mut binding = rotation_binding(params, host, provider_instance_id, request_id)?;
    if let Some(settings_id) = optional_string(params, "settings_id") {
        let selection = resolve_runtime_selection(host, settings_id, request_id)?;
        binding.settings_selection = Some(RotationSettingsSelection {
            record_id: selection.settings_id,
            record_version: selection.settings_version,
            account: selection.account,
        });
    }
    validate_rotation_accounts(&binding, request_id)?;
    Ok(binding)
}

fn materialization_rotation_binding(
    params: &Value,
    host: &HostContext,
    provider_instance_id: &str,
    request_id: &str,
) -> Result<RotationBinding, ProviderFailure> {
    let mut binding = rotation_binding(params, host, provider_instance_id, request_id)?;
    if let Some(settings_id) = optional_string(params, "settings_id") {
        let settings_version = required_string(params, "settings_version", request_id)?;
        let settings_account = rotation_account(
            required_string(params, "settings_account", request_id)?,
            request_id,
            "settings",
        )?;
        binding.settings_selection = Some(RotationSettingsSelection {
            record_id: settings_id.to_string(),
            record_version: settings_version.to_string(),
            account: settings_account,
        });
    } else if optional_string(params, "settings_version").is_some()
        || optional_string(params, "settings_account").is_some()
    {
        return Err(rotation_settings_binding_invalid(
            request_id,
            "settings_version and settings_account require settings_id",
        ));
    }
    validate_rotation_accounts(&binding, request_id)?;
    Ok(binding)
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
        "settings_selection": rotation_settings_selection_value(binding.settings_selection.as_ref()),
        "transition_reason": binding.transition_reason,
        "provider_instance_id": binding.provider_instance_id,
        "host_app": binding.host_app,
    })
}

fn rotation_settings_selection_value(selection: Option<&RotationSettingsSelection>) -> Value {
    selection.map_or(Value::Null, |selection| {
        json!({
            "settings_id": selection.record_id,
            "settings_version": selection.record_version,
            "settings_account": selection.account.opencode_wrapper,
        })
    })
}

fn binding_digest(binding: &RotationBinding) -> String {
    sha256_hex(binding_value(binding).to_string().as_bytes())
}

fn validate_rotation_accounts(
    binding: &RotationBinding,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if binding.target_provider_id != binding.provider_instance_id {
        return Err(rotation_target_provider_mismatch(request_id));
    }
    if let Some(selection) = &binding.settings_selection {
        if selection.account.opencode_wrapper != binding.target_account.opencode_wrapper {
            return Err(rotation_settings_account_mismatch(request_id));
        }
    }
    Ok(())
}

fn validate_rotation_settings_before_effect(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let Some(expected) = binding.settings_selection.as_ref() else {
        return Ok(());
    };
    let _settings_lock = settings::acquire_store_lock(host, request_id)?;
    match observe_rotation_settings_selection(host, expected, request_id) {
        Ok(observed) if rotation_settings_selection_matches(expected, &observed) => Ok(()),
        observed => Err(rotation_settings_selection_changed(
            request_id, binding, observed,
        )),
    }
}

fn validate_rotation_settings_for_operation(
    host: &HostContext,
    params: &Value,
    binding: &RotationBinding,
    operation: &RotationOperation,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let (settings_lock, _) =
        settle_rotation_settings_selection(host, params, binding, operation, request_id)?;
    drop(settings_lock);
    Ok(())
}

fn settle_rotation_settings_selection(
    host: &HostContext,
    params: &Value,
    binding: &RotationBinding,
    operation: &RotationOperation,
    request_id: &str,
) -> Result<(Option<fs::File>, Option<RotationSettingsSelection>), ProviderFailure> {
    let Some(expected) = binding.settings_selection.as_ref() else {
        if params.get("settings_reconciliation").is_some() {
            return Err(rotation_settings_binding_invalid(
                request_id,
                "settings_reconciliation requires an assessment-bound settings selection",
            ));
        }
        return Ok((None, None));
    };
    let settings_lock = settings::acquire_store_lock(host, request_id)?;
    let observed = observe_rotation_settings_selection(host, expected, request_id);
    if observed
        .as_ref()
        .is_ok_and(|observed| rotation_settings_selection_matches(expected, observed))
    {
        return Ok((Some(settings_lock), Some(expected.clone())));
    }
    let reconciliation =
        parse_rotation_settings_reconciliation(params, request_id)?.ok_or_else(|| {
            rotation_settings_reconciliation_required(
                request_id,
                binding,
                operation,
                observed.as_ref().map_err(String::as_str),
            )
        })?;
    let reconciliation_account = rotation_account(
        &reconciliation.settings_account,
        request_id,
        "settings reconciliation",
    )?;
    let reconciled = RotationSettingsSelection {
        record_id: reconciliation.settings_id,
        record_version: reconciliation.settings_version,
        account: reconciliation_account,
    };
    if reconciled.account.opencode_wrapper != binding.target_account.opencode_wrapper {
        return Err(rotation_settings_reconciliation_required(
            request_id,
            binding,
            operation,
            observed.as_ref().map_err(String::as_str),
        ));
    }
    let expected_target_session_id = match operation.phase {
        RotationOperationPhase::Prepared => optional_string(params, "recovery_target_session_id")
            .or(operation.import_candidate_session_id.as_deref())
            .unwrap_or(&binding.source_session_id),
        RotationOperationPhase::Imported => operation
            .target_session_id
            .as_deref()
            .expect("validated imported rotation has a target session id"),
    };
    if reconciliation.target_provider_session_id != expected_target_session_id {
        return Err(rotation_settings_reconciliation_required(
            request_id,
            binding,
            operation,
            observed.as_ref().map_err(String::as_str),
        ));
    }
    let reconciled_observation = observe_rotation_settings_selection(host, &reconciled, request_id);
    if !reconciled_observation
        .as_ref()
        .is_ok_and(|observed| rotation_settings_selection_matches(&reconciled, observed))
    {
        return Err(rotation_settings_reconciliation_required(
            request_id,
            binding,
            operation,
            reconciled_observation.as_ref().map_err(String::as_str),
        ));
    }
    Ok((Some(settings_lock), Some(reconciled)))
}

fn parse_rotation_settings_reconciliation(
    params: &Value,
    request_id: &str,
) -> Result<Option<RotationSettingsReconciliation>, ProviderFailure> {
    params
        .get("settings_reconciliation")
        .cloned()
        .map(|value| {
            serde_json::from_value(value).map_err(|error| {
                rotation_settings_binding_invalid(
                    request_id,
                    format!("settings_reconciliation is invalid: {error}"),
                )
            })
        })
        .transpose()
}

fn observe_rotation_settings_selection(
    host: &HostContext,
    expected: &RotationSettingsSelection,
    request_id: &str,
) -> Result<RotationSettingsSelection, String> {
    match resolve_runtime_selection(host, &expected.record_id, request_id) {
        Ok(selection) => Ok(RotationSettingsSelection {
            record_id: selection.settings_id,
            record_version: selection.settings_version,
            account: selection.account,
        }),
        Err(failure) => Err(format!("{}: {}", failure.code, failure.message)),
    }
}

fn rotation_settings_selection_matches(
    expected: &RotationSettingsSelection,
    observed: &RotationSettingsSelection,
) -> bool {
    expected.record_id == observed.record_id
        && expected.record_version == observed.record_version
        && expected.account.opencode_wrapper == observed.account.opencode_wrapper
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
        let parent = path
            .parent()
            .expect("rotation authorization always has a parent");
        durable_fs::create_private_directories(parent)
            .map_err(|error| rotation_state_failure(request_id, error))?;
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(rotation_state_failure(request_id, error)),
        }
        durable_fs::sync_directory(parent)
            .map_err(|error| rotation_state_failure(request_id, error))?;
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
        "settings_selection": rotation_settings_selection_value(binding.settings_selection.as_ref()),
    })))
}

fn require_fresh_authorization(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let path = authorization_path(host, binding, request_id)?;
    let bytes = durable_fs::read_file_bounded(&path, MAX_ROTATION_STATE_BYTES)
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

fn acquire_rotation_capacity_lock(
    host: &HostContext,
    budget: &RotationBudget,
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
    let timeout = budget.remaining(request_id)?;
    if !operation_bounds::lock_exclusive_for(&lock, timeout)
        .map_err(|error| rotation_state_failure(request_id, error))?
    {
        return Err(rotation_lock_timeout(request_id));
    }
    Ok(lock)
}

fn acquire_rotation_binding_lock(
    host: &HostContext,
    binding: &RotationBinding,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let lock = open_rotation_binding_lock(host, binding, request_id)?;
    let timeout = budget.remaining(request_id)?;
    if !operation_bounds::lock_exclusive_for(&lock, timeout)
        .map_err(|error| rotation_state_failure(request_id, error))?
    {
        return Err(rotation_lock_timeout(request_id));
    }
    Ok(lock)
}

fn open_rotation_binding_lock(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let digest = binding_digest(binding);
    open_rotation_binding_lock_for_digest(host, &digest, request_id)
}

fn open_rotation_binding_lock_for_digest(
    host: &HostContext,
    digest: &str,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(rotation_state_failure(
            request_id,
            "rotation binding digest is not hexadecimal SHA-256",
        ));
    }
    let prefix = u8::from_str_radix(&digest[..2], 16)
        .expect("binding digest always starts with one hexadecimal byte");
    let stripe = prefix % ROTATION_BINDING_LOCK_STRIPES;
    let root = rotation_state_root(host, request_id)?;
    let lock_root = confined_rotation_state_target(host, &root.join("binding-locks"), request_id)?;
    durable_fs::create_private_directories(&lock_root)
        .map_err(|error| rotation_state_failure(request_id, error))?;
    let lock_path = confined_rotation_state_target(
        host,
        &lock_root.join(format!("stripe-{stripe:02}.lock")),
        request_id,
    )?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .map_err(|error| rotation_state_failure(request_id, error))
}

fn maintain_rotation_capacity(
    host: &HostContext,
    binding: &RotationBinding,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<RotationCapacity, ProviderFailure> {
    let root = rotation_state_root(host, request_id)?;
    let current_digest = binding_digest(binding);
    let mut capacity = RotationCapacity::default();

    for path in rotation_collection_paths(&root, "authorizations", request_id)? {
        budget.checkpoint(request_id)?;
        if path.file_stem().and_then(|stem| stem.to_str()) != Some(current_digest.as_str())
            && rotation_record_expired(&path, AUTHORIZATION_TTL)
        {
            remove_rotation_record(&path, request_id)?;
        } else {
            capacity.authorizations += 1;
        }
    }

    for path in rotation_collection_paths(&root, "materializations", request_id)? {
        budget.checkpoint(request_id)?;
        if path.file_stem().and_then(|stem| stem.to_str()) != Some(current_digest.as_str())
            && rotation_record_expired(&path, ROTATION_REPLAY_RETENTION)
        {
            remove_rotation_record(&path, request_id)?;
        } else {
            capacity.materializations += 1;
        }
    }

    let materialization_root = root.join("materializations");
    let operation_root = root.join("operations");
    for path in rotation_collection_paths(&root, "reservations", request_id)? {
        budget.checkpoint(request_id)?;
        let digest = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| rotation_state_failure(request_id, "reservation has no digest"))?;
        let successor = path
            .file_name()
            .map(|name| operation_root.join(name))
            .is_some_and(|operation| operation.is_file());
        let stale = digest != current_digest
            && rotation_record_expired(&path, ROTATION_RESERVATION_RETENTION);
        let abandoned = if stale {
            let lock = open_rotation_binding_lock_for_digest(host, digest, request_id)?;
            match fs2::FileExt::try_lock_exclusive(&lock) {
                Ok(()) => true,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(error) => return Err(rotation_state_failure(request_id, error)),
            }
        } else {
            false
        };
        if successor || abandoned {
            remove_rotation_record(&path, request_id)?;
        } else {
            capacity.reservations += 1;
        }
    }
    for path in rotation_collection_paths(&root, "operations", request_id)? {
        budget.checkpoint(request_id)?;
        let successor = path
            .file_name()
            .map(|name| materialization_root.join(name))
            .is_some_and(|receipt| receipt.is_file());
        if successor {
            remove_rotation_record(&path, request_id)?;
        } else {
            capacity.operations += 1;
        }
    }

    let references = rotation_live_artifact_references(host, &root, budget, request_id)?;
    let artifact_root = confined_rotation_state_target(
        host,
        &rotation_data_root(host, request_id)?
            .join("provider-artifacts")
            .join("opencode")
            .join("rotation"),
        request_id,
    )?;
    capacity.artifacts =
        maintain_rotation_artifact_collection(&artifact_root, &references, budget, request_id)?;
    capacity.decisions = maintain_rotation_artifact_collection(
        &root.join("decisions"),
        &references,
        budget,
        request_id,
    )?;

    for (name, observed) in [
        ("authorizations", capacity.authorizations),
        ("materialization receipts", capacity.materializations),
        ("operations", capacity.operations),
        ("reservations", capacity.reservations),
    ] {
        if observed > MAX_ROTATION_LIVE_RECORDS {
            return Err(rotation_state_capacity_exceeded(request_id, name));
        }
    }
    if capacity
        .materializations
        .saturating_add(capacity.operations)
        .saturating_add(capacity.reservations)
        > MAX_ROTATION_LIVE_RECORDS
    {
        return Err(rotation_state_capacity_exceeded(
            request_id,
            "reservations, operations, and materialization receipts",
        ));
    }
    for (name, observed) in [
        ("artifacts", capacity.artifacts),
        ("decisions", capacity.decisions),
    ] {
        if observed > MAX_ROTATION_ARTIFACT_RECORDS {
            return Err(rotation_state_capacity_exceeded(request_id, name));
        }
    }
    Ok(capacity)
}

fn rotation_collection_paths(
    root: &Path,
    relative: &str,
    request_id: &str,
) -> Result<Vec<PathBuf>, ProviderFailure> {
    let directory = root.join(relative);
    rotation_directory_paths(&directory, relative, request_id)
}

fn rotation_directory_paths(
    directory: &Path,
    collection_name: &str,
    request_id: &str,
) -> Result<Vec<PathBuf>, ProviderFailure> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(rotation_state_failure(request_id, error)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| rotation_state_failure(request_id, error))?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        if !entry
            .file_type()
            .map_err(|error| rotation_state_failure(request_id, error))?
            .is_file()
        {
            return Err(rotation_state_failure(
                request_id,
                format!(
                    "rotation collection entry is not a file: {}",
                    path.display()
                ),
            ));
        }
        paths.push(path);
        if paths.len() > MAX_ROTATION_COMPATIBILITY_RECORDS {
            return Err(rotation_state_capacity_exceeded(
                request_id,
                collection_name,
            ));
        }
    }
    Ok(paths)
}

fn rotation_record_expired(path: &Path, retention: Duration) -> bool {
    fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age >= retention)
}

fn remove_rotation_record(path: &Path, request_id: &str) -> Result<(), ProviderFailure> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(rotation_state_failure(request_id, error)),
    }
    durable_fs::sync_directory(
        path.parent()
            .expect("rotation record always has a parent directory"),
    )
    .map_err(|error| rotation_state_failure(request_id, error))
}

fn rotation_live_artifact_references(
    host: &HostContext,
    root: &Path,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<HashSet<PathBuf>, ProviderFailure> {
    let mut references = HashSet::new();
    for path in rotation_collection_paths(root, "operations", request_id)? {
        budget.checkpoint(request_id)?;
        let bytes = durable_fs::read_file_bounded(&path, MAX_ROTATION_STATE_BYTES)
            .map_err(|error| rotation_state_failure(request_id, error))?;
        let operation: RotationOperation = serde_json::from_slice(&bytes)
            .map_err(|error| rotation_operation_invalid(request_id, error))?;
        references.insert(confined_rotation_state_target(
            host,
            Path::new(&operation.artifact_path),
            request_id,
        )?);
    }
    for path in rotation_collection_paths(root, "materializations", request_id)? {
        budget.checkpoint(request_id)?;
        let bytes = durable_fs::read_file_bounded(&path, MAX_ROTATION_STATE_BYTES)
            .map_err(|error| rotation_state_failure(request_id, error))?;
        let receipt: Value = serde_json::from_slice(&bytes)
            .map_err(|error| rotation_state_failure(request_id, error))?;
        if let Some(artifacts) = receipt
            .pointer("/result/artifacts")
            .and_then(Value::as_array)
        {
            for artifact in artifacts {
                if let Some(path) = artifact.get("path").and_then(Value::as_str) {
                    references.insert(confined_rotation_state_target(
                        host,
                        Path::new(path),
                        request_id,
                    )?);
                }
            }
        }
    }
    Ok(references)
}

fn maintain_rotation_artifact_collection(
    directory: &Path,
    references: &HashSet<PathBuf>,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<usize, ProviderFailure> {
    let paths = rotation_directory_paths(directory, "artifacts", request_id)?;
    let mut retained = 0_usize;
    for path in paths {
        budget.checkpoint(request_id)?;
        if !references.contains(&path) && rotation_record_expired(&path, ROTATION_REPLAY_RETENTION)
        {
            remove_rotation_record(&path, request_id)?;
        } else {
            retained += 1;
        }
    }
    Ok(retained)
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

fn rotation_reservation_path(
    host: &HostContext,
    binding: &RotationBinding,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let path = rotation_state_root(host, request_id)?
        .join("reservations")
        .join(format!("{}.json", binding_digest(binding)));
    confined_rotation_state_target(host, &path, request_id)
}

fn persist_rotation_reservation(
    path: &Path,
    binding: &RotationBinding,
    request_id: &str,
    existing: bool,
) -> Result<RotationReservationGuard, ProviderFailure> {
    let digest = binding_digest(binding);
    if existing {
        let bytes = durable_fs::read_file_bounded(path, MAX_ROTATION_STATE_BYTES)
            .map_err(|error| rotation_state_failure(request_id, error))?;
        let reservation: Value = serde_json::from_slice(&bytes)
            .map_err(|error| rotation_state_failure(request_id, error))?;
        if reservation["schema_version"] != 1
            || reservation["binding_sha256"].as_str() != Some(digest.as_str())
            || reservation["binding"] != binding_value(binding)
        {
            return Err(rotation_state_failure(
                request_id,
                "rotation reservation identity is inconsistent",
            ));
        }
    } else {
        write_private_json_atomic(
            path,
            &json!({
                "schema_version": 1,
                "binding_sha256": digest,
                "binding": binding_value(binding),
                "materialization_request_id": request_id,
                "reserved_at_unix_ms": now_unix_ms(),
            }),
        )
        .map_err(|error| rotation_state_failure(request_id, error))?;
    }
    Ok(RotationReservationGuard {
        path: path.to_path_buf(),
        armed: true,
    })
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
    let bytes = match durable_fs::read_file_bounded(&path, MAX_ROTATION_STATE_BYTES) {
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
    let bytes = match durable_fs::read_file_bounded(&path, MAX_ROTATION_STATE_BYTES) {
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
    let actor_identity_present = operation
        .import_actor_process_group_id
        .is_some_and(|value| value > 0)
        && operation
            .import_actor_process_group_incarnation
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
    let actor_identity_absent = operation.import_actor_process_group_id.is_none()
        && operation.import_actor_process_group_incarnation.is_none()
        && operation.import_actor_terminal_at_unix_ms.is_none();
    let phase_valid = match operation.phase {
        RotationOperationPhase::Prepared => {
            operation.target_session_id.is_none()
                && operation.imported_at_unix_ms.is_none()
                && (actor_identity_absent || actor_identity_present)
                && operation
                    .import_candidate_session_id
                    .as_deref()
                    .is_none_or(|value| !value.trim().is_empty())
        }
        RotationOperationPhase::Imported => {
            operation
                .target_session_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                && operation.import_candidate_session_id.is_none()
                && operation.imported_at_unix_ms.is_some()
                && actor_identity_present
                && operation.import_actor_terminal_at_unix_ms.is_some()
        }
    };
    if operation.schema_version != ROTATION_OPERATION_SCHEMA_VERSION
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
    let bytes = durable_fs::read_file_bounded(&path, MAX_ROTATION_ARTIFACT_BYTES)
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

fn execute_or_reconcile_prepared_operation(
    host: &HostContext,
    params: &Value,
    binding: &RotationBinding,
    target_runtime: &native_runtime::NativeRuntimeContext,
    operation: &mut RotationOperation,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if operation.import_actor_process_group_id.is_none() {
        let working_directory = rotation_working_directory(host, request_id)?;
        return admit_and_observe_import(
            host,
            binding,
            target_runtime,
            operation,
            working_directory,
            budget,
            request_id,
        );
    }
    require_rotation_import_actor_terminal(operation, request_id)?;
    write_rotation_operation(host, binding, operation, request_id)?;
    reconcile_prepared_operation(
        host,
        params,
        binding,
        target_runtime,
        operation,
        budget,
        request_id,
    )
}

fn admit_and_observe_import(
    host: &HostContext,
    binding: &RotationBinding,
    target_runtime: &native_runtime::NativeRuntimeContext,
    operation: &mut RotationOperation,
    working_directory: &Path,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let import_timeout = budget.remaining(request_id)?;
    let prepared = opencode::prepare_import_session(
        Path::new(&operation.artifact_path),
        target_runtime,
        working_directory,
    )
    .map_err(|error| {
        rotation_recovery_required(
            request_id,
            binding,
            operation,
            None,
            Some(format!(
                "import could not be prepared before native effect: {error:?}"
            )),
        )
    })?;
    publish_rotation_import_actor(host, binding, operation, prepared.actor(), request_id)?;
    let observed = prepared.observe(import_timeout);
    operation.import_actor_terminal_at_unix_ms = Some(now_unix_ms());
    if let Err(error) = write_rotation_operation(host, binding, operation, request_id) {
        return Err(rotation_recovery_required(
            request_id,
            binding,
            operation,
            None,
            Some(format!(
                "native import actor became terminal but its terminal proof could not be persisted: {error:?}"
            )),
        ));
    }
    let target_session_id = match observed {
        Ok(target_session_id) => target_session_id,
        Err(error) => {
            return Err(rotation_recovery_required(
                request_id,
                binding,
                operation,
                None,
                Some(format!(
                    "import outcome could not be settled from native output: {error:?}"
                )),
            ));
        }
    };
    validate_and_record_imported_target(
        host,
        binding,
        target_runtime,
        operation,
        &target_session_id,
        true,
        budget,
        request_id,
    )
}

fn publish_rotation_import_actor(
    host: &HostContext,
    binding: &RotationBinding,
    operation: &mut RotationOperation,
    actor: &ProcessGroupActor,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    operation.import_actor_process_group_id = Some(actor.process_group_id);
    operation.import_actor_process_group_incarnation = Some(actor.incarnation.clone());
    operation.import_actor_terminal_at_unix_ms = None;
    if let Err(error) = write_rotation_operation(host, binding, operation, request_id) {
        operation.import_actor_process_group_id = None;
        operation.import_actor_process_group_incarnation = None;
        return Err(error);
    }
    Ok(())
}

fn require_rotation_import_actor_terminal(
    operation: &mut RotationOperation,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if operation.import_actor_terminal_at_unix_ms.is_some() {
        return Ok(());
    }
    let (Some(process_group_id), Some(incarnation)) = (
        operation.import_actor_process_group_id,
        operation.import_actor_process_group_incarnation.as_ref(),
    ) else {
        return Err(rotation_operation_invalid(
            request_id,
            "an effect-admitted rotation import has no durable native actor incarnation",
        ));
    };
    let actor = ProcessGroupActor {
        process_group_id,
        incarnation: incarnation.clone(),
    };
    match actor_is_terminal_or_recycled(&actor) {
        Ok(true) => {
            operation.import_actor_terminal_at_unix_ms = Some(now_unix_ms());
            Ok(())
        }
        Ok(false) => Err(rotation_import_actor_active(request_id, operation)),
        Err(error) => Err(rotation_import_actor_unverifiable(
            request_id, operation, error,
        )),
    }
}

fn reconcile_prepared_operation(
    host: &HostContext,
    params: &Value,
    binding: &RotationBinding,
    target_runtime: &native_runtime::NativeRuntimeContext,
    operation: &mut RotationOperation,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let supplied_target = optional_string(params, "recovery_target_session_id");
    let durable_candidate = operation.import_candidate_session_id.clone();
    let candidate_session_id = supplied_target
        .or(durable_candidate.as_deref())
        .unwrap_or(&binding.source_session_id);
    let preserve_candidate = durable_candidate.is_none() && supplied_target.is_some();
    validate_and_record_imported_target(
        host,
        binding,
        target_runtime,
        operation,
        candidate_session_id,
        preserve_candidate,
        budget,
        request_id,
    )
}

#[allow(clippy::too_many_arguments)]
fn validate_and_record_imported_target(
    host: &HostContext,
    binding: &RotationBinding,
    target_runtime: &native_runtime::NativeRuntimeContext,
    operation: &mut RotationOperation,
    candidate_session_id: &str,
    preserve_candidate: bool,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if preserve_candidate {
        preserve_import_candidate(host, binding, operation, candidate_session_id, request_id)?;
    }
    let verification_timeout = match budget.remaining(request_id) {
        Ok(timeout) => timeout,
        Err(_) => {
            return Err(rotation_recovery_required(
                request_id,
                binding,
                operation,
                Some(candidate_session_id),
                Some("rotation budget expired before target export validation".to_string()),
            ));
        }
    };
    let target = match opencode::export_with_timeout(
        candidate_session_id,
        target_runtime,
        verification_timeout,
    ) {
        Ok(target) => target,
        Err(error) => {
            return Err(rotation_recovery_required(
                request_id,
                binding,
                operation,
                Some(candidate_session_id),
                Some(format!("target export failed: {error:?}")),
            ));
        }
    };
    if budget.checkpoint(request_id).is_err() {
        return Err(rotation_recovery_required(
            request_id,
            binding,
            operation,
            Some(candidate_session_id),
            Some("rotation budget expired after target export validation".to_string()),
        ));
    }
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
            Some(candidate_session_id),
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
            Some(candidate_session_id),
            Some("target export content does not match the prepared source artifact".to_string()),
        ));
    }
    operation.phase = RotationOperationPhase::Imported;
    operation.target_session_id = Some(candidate_session_id.to_string());
    operation.import_candidate_session_id = None;
    operation.imported_at_unix_ms = Some(now_unix_ms());
    if let Err(error) = write_rotation_operation(host, binding, operation, request_id) {
        operation.phase = RotationOperationPhase::Prepared;
        operation.target_session_id = None;
        operation.import_candidate_session_id = Some(candidate_session_id.to_string());
        operation.imported_at_unix_ms = None;
        return Err(rotation_recovery_required(
            request_id,
            binding,
            operation,
            Some(candidate_session_id),
            Some(format!(
                "validated target could not be durably advanced to imported: {error:?}"
            )),
        ));
    }
    budget.checkpoint(request_id)
}

fn preserve_import_candidate(
    host: &HostContext,
    binding: &RotationBinding,
    operation: &mut RotationOperation,
    candidate_session_id: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if operation.import_candidate_session_id.as_deref() == Some(candidate_session_id) {
        return Ok(());
    }
    if let Some(existing) = operation.import_candidate_session_id.as_deref() {
        return Err(rotation_recovery_required(
            request_id,
            binding,
            operation,
            Some(existing),
            Some("native import candidate conflicts with durable operation custody".to_string()),
        ));
    }
    operation.import_candidate_session_id = Some(candidate_session_id.to_string());
    if let Err(error) = write_rotation_operation(host, binding, operation, request_id) {
        operation.import_candidate_session_id = None;
        return Err(rotation_recovery_required(
            request_id,
            binding,
            operation,
            Some(candidate_session_id),
            Some(format!(
                "native import candidate could not be durably preserved: {error:?}"
            )),
        ));
    }
    Ok(())
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
    params: &Value,
    binding: &RotationBinding,
    operation: &RotationOperation,
    budget: &RotationBudget,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    budget.checkpoint(request_id)?;
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
    budget.checkpoint(request_id)?;
    let artifact_path = Path::new(&operation.artifact_path);
    let artifact = rotation_artifact(artifact_path, &artifact_bytes);
    let (settings_lock, settled_settings_selection) =
        settle_rotation_settings_selection(host, params, binding, operation, request_id)?;
    let decision_artifact = write_rotation_decision_receipt(
        host,
        binding,
        settled_settings_selection.as_ref(),
        operation,
        request_id,
    )?;
    budget.checkpoint(request_id)?;
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
    let capacity_lock = acquire_rotation_capacity_lock(host, budget, request_id)?;
    write_materialization_receipt(
        host,
        binding,
        &result,
        &operation.materialization_request_id,
        request_id,
    )?;
    budget.checkpoint(request_id)?;
    remove_rotation_record(
        &rotation_operation_path(host, binding, request_id)?,
        request_id,
    )?;
    remove_rotation_record(&authorization_path(host, binding, request_id)?, request_id)?;
    drop(capacity_lock);
    drop(settings_lock);
    budget.checkpoint(request_id)?;
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
    settled_settings_selection: Option<&RotationSettingsSelection>,
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
        "schema_id": ROTATION_DECISION_SCHEMA_ID,
        "operation": "rotation.materialize",
        "binding_sha256": binding_digest(binding),
        "binding": binding_value(binding),
        "authorized_settings_selection": rotation_settings_selection_value(binding.settings_selection.as_ref()),
        "settled_settings_selection": rotation_settings_selection_value(settled_settings_selection),
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
    if bytes.len() > MAX_ROTATION_STATE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("rotation state exceeds supported {MAX_ROTATION_STATE_BYTES}-byte bound"),
        ));
    }
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
    match durable_fs::read_file_bounded(path, bytes.len()) {
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
        Err(error)
            if durable_fs::read_file_bounded(path, bytes.len())
                .is_ok_and(|existing| existing == bytes) =>
        {
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

fn rotation_settings_binding_invalid(
    request_id: &str,
    message: impl Into<String>,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "rotation_settings_binding_invalid",
        message.into(),
    )
}

fn rotation_settings_selection_changed(
    request_id: &str,
    binding: &RotationBinding,
    observed: Result<RotationSettingsSelection, String>,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "rotation_settings_selection_changed",
        "the assessment-bound settings selection changed before rotation effect admission; request a new assessment",
        json!({
            "authorized_settings_selection": rotation_settings_selection_value(binding.settings_selection.as_ref()),
            "observed_settings_selection": observed
                .as_ref()
                .map_or_else(|failure| json!({ "unavailable": failure }), |selection| rotation_settings_selection_value(Some(selection))),
        }),
    )
}

fn rotation_settings_reconciliation_required(
    request_id: &str,
    binding: &RotationBinding,
    operation: &RotationOperation,
    observed: Result<&RotationSettingsSelection, &str>,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "rotation_settings_reconciliation_required",
        "the assessment-bound settings selection changed after rotation effect admission; preserve the imported session and reconcile a current settings route to the imported target account before retrying",
        json!({
            "authorized_settings_selection": rotation_settings_selection_value(binding.settings_selection.as_ref()),
            "observed_settings_selection": observed
                .map_or_else(|failure| json!({ "unavailable": failure }), |selection| rotation_settings_selection_value(Some(selection))),
            "imported_target_account": binding.target_account.opencode_wrapper,
            "imported_target_provider_session_id": operation
                .target_session_id
                .as_ref()
                .or(operation.import_candidate_session_id.as_ref()),
            "operation_phase": operation.phase,
            "recovery": "retry the same materialization with settings_reconciliation naming an exact current settings ID/version/account for the imported target account and the imported target session; do not repeat import",
        }),
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
    if let OpencodeExportError::OutputTooLarge {
        stream,
        maximum_bytes,
    } = &error
    {
        return ProviderFailure::invalid_request(
            request_id,
            "rotation_export_capacity_exceeded",
            format!(
                "source session {session_id} export {stream} exceeds the supported {maximum_bytes}-byte bound"
            ),
        );
    }
    ProviderFailure::internal(
        request_id,
        "rotation_export_failed",
        format!("failed to export source session {session_id}: {error:?}"),
    )
}

fn rotation_artifact_capacity_exceeded(request_id: &str, observed_bytes: usize) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "rotation_artifact_capacity_exceeded",
        format!(
            "serialized rotation artifact is {observed_bytes} bytes; the supported maximum is {MAX_ROTATION_ARTIFACT_BYTES} bytes"
        ),
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

fn rotation_lock_timeout(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_lock_timeout",
        "rotation lock could not be acquired before the operation deadline",
    )
}

fn rotation_deadline_exceeded(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_deadline_exceeded",
        "the end-to-end rotation operation deadline was reached during bounded coordination or native work",
    )
}

fn rotation_state_failure(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_state_failed",
        format!("failed to maintain provider-owned rotation state: {error}"),
    )
}

fn rotation_state_capacity_exceeded(request_id: &str, collection: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "rotation_state_capacity_exceeded",
        format!(
            "provider-owned rotation {collection} reached its supported bounded custody; reconcile incomplete rotations or wait for the 24-hour replay window to expire"
        ),
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
            "import_actor_process_group_id": operation.import_actor_process_group_id,
            "import_actor_process_group_incarnation": operation.import_actor_process_group_incarnation,
            "import_actor_terminal_at_unix_ms": operation.import_actor_terminal_at_unix_ms,
            "recovery": "retry with recovery_target_session_id after confirming the imported target session; if import never occurred, import the prepared artifact once and supply the resulting session id",
            "observation": observation,
        }),
    )
}

fn rotation_import_actor_active(
    request_id: &str,
    operation: &RotationOperation,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "rotation_import_actor_active",
        "rotation recovery is blocked while the exact native import actor remains live",
        json!({
            "import_actor_process_group_id": operation.import_actor_process_group_id,
            "import_actor_process_group_incarnation": operation.import_actor_process_group_incarnation,
            "required_action": "retry the unchanged materialization after the native import actor is terminal or recycled",
        }),
    )
}

fn rotation_import_actor_unverifiable(
    request_id: &str,
    operation: &RotationOperation,
    error: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "rotation_import_actor_unverifiable",
        format!(
            "could not verify terminal custody for rotation import actor {:?}/{:?}: {error}",
            operation.import_actor_process_group_id,
            operation.import_actor_process_group_incarnation
        ),
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
    use super::*;
    use crate::native_process::{actor_for_child, configure_process_group};
    use std::process::Command;

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

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn rotation_recovery_waits_for_the_exact_import_actor() {
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        configure_process_group(&mut command);
        let mut child = command.spawn().expect("spawn rotation import actor");
        let actor = actor_for_child(&child).expect("identify rotation import actor");
        let mut operation = RotationOperation {
            schema_version: ROTATION_OPERATION_SCHEMA_VERSION,
            binding_sha256: "binding".to_string(),
            binding: json!({}),
            authorization_id: "authorization".to_string(),
            assessment_request_id: "assessment".to_string(),
            materialization_request_id: "materialization".to_string(),
            artifact_path: "/tmp/artifact".to_string(),
            artifact_sha256: "artifact".to_string(),
            boundary: "boundary".to_string(),
            prepared_at_unix_ms: 1,
            phase: RotationOperationPhase::Prepared,
            import_actor_process_group_id: Some(actor.process_group_id),
            import_actor_process_group_incarnation: Some(actor.incarnation),
            import_actor_terminal_at_unix_ms: None,
            target_session_id: None,
            import_candidate_session_id: None,
            imported_at_unix_ms: None,
        };

        let active = require_rotation_import_actor_terminal(&mut operation, "request-test")
            .expect_err("live import actor must block rotation recovery");
        assert_eq!(active.code, "rotation_import_actor_active");
        assert!(operation.import_actor_terminal_at_unix_ms.is_none());

        child.kill().expect("terminate rotation import actor");
        child.wait().expect("reap rotation import actor");
        require_rotation_import_actor_terminal(&mut operation, "request-test")
            .expect("terminal import actor permits rotation recovery");
        assert!(operation.import_actor_terminal_at_unix_ms.is_some());
    }
}
