//! Declared roles: accessor, mapper, orchestration, validator, predicate, filter, formatter, parser

use crate::account::{profile_for_wrapper_reference, ACCOUNTS};
use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope};
use crate::native_runtime;
use crate::operation_bounds;
use crate::quota_observer;
use crate::schema::NATIVE_IDENTITY_REBIND_SCHEMA_ID;
use crate::settings::{self, SettingsTransitionReadiness};
use crate::shell;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const SETUP_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const NATIVE_IDENTITY_REBIND_PROTOCOL: &str = "opencode.native-identity-rebind/v1";
const NATIVE_IDENTITY_REBIND_DRAIN_MS: u64 = 20_000;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum NativeIdentityRebindRequest {
    Plan {
        protocol: String,
        profiles: Vec<String>,
    },
    Seal {
        protocol: String,
        operation_id: String,
        profile: String,
        prior_binding: NativeIdentityBindingEvidence,
        host_handoff: NativeIdentityRebindSealHandoff,
    },
    Observe {
        protocol: String,
        operation_id: String,
        profile: String,
        prior_binding: NativeIdentityBindingEvidence,
        disposition: NativeIdentityRebindDisposition,
        host_handoff: NativeIdentityRebindObservationHandoff,
    },
    Release {
        protocol: String,
        operation_id: String,
        observation_id: String,
        profile: String,
        prior_binding: NativeIdentityBindingEvidence,
        observed_binding: NativeIdentityBindingEvidence,
        disposition: NativeIdentityRebindDisposition,
        host_handoff: NativeIdentityRebindReleaseHandoff,
    },
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityBindingEvidence {
    native_runtime_state_sha256: Option<String>,
    quota_observer_state_sha256: Option<String>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NativeIdentityRebindDisposition {
    Committed,
    RolledBack,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindSealHandoff {
    ordinary_admission_blocked: bool,
    obligations_reconciled: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindObservationHandoff {
    ordinary_admission_blocked: bool,
    validation_launch_completed: bool,
    validation_quota_probe_completed: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindReleaseHandoff {
    ordinary_admission_reopened: bool,
}

impl NativeIdentityRebindDisposition {
    fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Command {
    Detect,
    InstallPlan,
    SyncPlan,
    BrainTurn,
}

pub(crate) fn handle(command: Command, request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let RequestEnvelope {
        host,
        params,
        request_id,
        ..
    } = request;
    match command {
        Command::Detect => detect_params(&host, params, &request_id),
        Command::InstallPlan => install_plan_params(&host, params, &request_id),
        Command::SyncPlan => sync_plan_params(&host, params, &request_id),
        Command::BrainTurn => Err(brain_unsupported(request_id)),
    }
}

pub fn detect_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let data_root = string_param(&params, "data_root").or(host.data_root.as_deref());
    let profile_root = string_param(&params, "profile_root");
    let opencode = executable_evidence("opencode", host.deadline_unix_ms);
    let curl = executable_evidence("curl", host.deadline_unix_ms);
    let profiles = profile_evidence(data_root, profile_root, host.deadline_unix_ms);
    let settings_store = settings::transition_readiness(host, request_id);
    let installed = setup_installed(&opencode, &curl, &profiles, &settings_store);
    Ok(detect_result(
        opencode,
        curl,
        profiles,
        settings_store,
        installed,
    ))
}

pub fn install_plan_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let target = string_param(&params, "target").unwrap_or("local");
    let settings_store = settings::transition_readiness(host, request_id);
    Ok(install_plan_result(target, &settings_store))
}

pub fn sync_plan_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let desired = desired_profiles(&params);
    let mut operations = sync_operations(&desired);
    if let Some(rebind) = parse_native_identity_rebind(&params, request_id)? {
        operations.extend(native_identity_rebind_operations(host, rebind, request_id)?);
    }
    let settings_store = settings::transition_readiness(host, request_id);
    let diagnostics = sync_diagnostics(&params, &settings_store);
    Ok(sync_plan_result(operations, diagnostics))
}

pub fn brain_unsupported(request_id: String) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "setup_brain_unsupported",
        "opencode provider does not implement setup_brain.turn; describe advertises setup_brain=false",
    )
}

fn executable_evidence(program: &str, deadline_unix_ms: Option<u64>) -> Value {
    let path = find_on_path(program);
    let version = match (
        &path,
        operation_bounds::remaining_timeout(deadline_unix_ms, SETUP_PROBE_TIMEOUT),
    ) {
        (Some(path), Some(timeout)) => {
            let program = path.to_string_lossy().into_owned();
            if path.metadata().is_ok_and(|metadata| {
                metadata.len() as usize > durable_fs::MAX_BOUND_EXECUTABLE_BYTES
            }) {
                return json!({
                    "program": program,
                    "present": true,
                    "path": path.to_string_lossy().into_owned(),
                    "version": {
                        "present": true,
                        "ready": false,
                        "error": format!(
                            "executable exceeds the supported {}-byte identity bound",
                            durable_fs::MAX_BOUND_EXECUTABLE_BYTES
                        ),
                    },
                });
            }
            let mut command = shell::command(&program);
            command
                .arg("--version")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let output = command
                .spawn()
                .and_then(|child| ChildCustody::new(child).wait_with_output_timeout(timeout));
            match output {
                Ok(Some(output)) => json!({
                    "present": true,
                    "status": output.status.code().unwrap_or(1),
                    "ready": output.status.success(),
                }),
                Ok(None) => json!({
                    "present": true,
                    "ready": false,
                    "timed_out": true,
                    "error": format!("{program} --version exceeded the setup probe deadline"),
                }),
                Err(err) => json!({
                    "present": true,
                    "ready": false,
                    "error": err.to_string(),
                }),
            }
        }
        (Some(_), None) => json!({
            "present": true,
            "ready": false,
            "timed_out": true,
            "error": "host deadline expired before the setup probe",
        }),
        (None, _) => json!({
            "present": false,
            "ready": false,
            "error": format!("{program} was not found in PATH"),
        }),
    };
    json!({
        "program": program,
        "present": path.is_some(),
        "path": path.map(|path| path.to_string_lossy().into_owned()),
        "version": version,
    })
}

fn profile_evidence(
    data_root: Option<&str>,
    profile_root: Option<&str>,
    deadline_unix_ms: Option<u64>,
) -> Vec<Value> {
    ACCOUNTS
        .iter()
        .map(|account| {
            let wrapper = executable_evidence(account.opencode_wrapper, deadline_unix_ms);
            let auth_present = opencode_auth_file_present(account.opencode_auth_path);
            let wrapper_ready = evidence_ready(&wrapper);
            json!({
                "profile": account.opencode_wrapper,
                "wrapper": account.opencode_wrapper,
                "wrapper_present": evidence_present(&wrapper),
                "wrapper_ready": wrapper_ready,
                "wrapper_path": wrapper.get("path").cloned().unwrap_or(Value::Null),
                "wrapper_version": wrapper.get("version").cloned().unwrap_or(Value::Null),
                "opencode_auth_path": account.opencode_auth_path,
                "opencode_auth_present": auth_present,
                "profile_ready": wrapper_ready && auth_present,
                "data_root": data_root,
                "profile_root": profile_root,
                "quota_probe": account.quota_probe_kind(),
            })
        })
        .collect()
}

fn auth_summary() -> String {
    let present = ACCOUNTS
        .iter()
        .map(|account| {
            let state = if opencode_auth_file_present(account.opencode_auth_path) {
                "present"
            } else {
                "missing"
            };
            format!(
                "{}:{}:{}",
                account.opencode_wrapper, state, account.opencode_auth_path
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("OpenCode auth metadata only; {present}; quota probe native_chatgpt_usage")
}

fn setup_warnings(
    opencode: &Value,
    curl: &Value,
    profiles: &[Value],
    settings_store: &SettingsTransitionReadiness,
) -> Vec<Value> {
    let mut warnings = Vec::new();
    if !evidence_ready(opencode) {
        warnings.push(json!(
            "opencode executable probe did not complete successfully"
        ));
    }
    if !evidence_ready(curl) {
        warnings.push(json!("curl executable probe did not complete successfully"));
    }
    for profile in profiles {
        if profile.get("profile_ready").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let name = profile
            .get("profile")
            .and_then(Value::as_str)
            .unwrap_or("unknown profile");
        let wrapper_ready = profile
            .get("wrapper_ready")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let auth_present = profile
            .get("opencode_auth_present")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        warnings.push(json!(format!(
            "{name} is not ready: wrapper_ready={wrapper_ready}, opencode_auth_present={auth_present}"
        )));
    }
    if let Some(message) = settings_store.blocking_message() {
        warnings.push(json!(message));
    }
    warnings
}

fn sync_diagnostics(params: &Value, settings_store: &SettingsTransitionReadiness) -> Vec<Value> {
    let mut diagnostics = desired_profile_diagnostics(params);
    if params.get("settings_schema_id").and_then(Value::as_str) != Some("opencode.settings/v1") {
        diagnostics.push(settings_schema_mismatch_diagnostic());
    }
    if let Some(message) = settings_store.blocking_message() {
        diagnostics.push(json!({
            "severity": "error",
            "path": "host.config_root",
            "message": message,
            "code": "settings_transition_blocked",
        }));
    }
    diagnostics
}

fn desired_profile_diagnostics(params: &Value) -> Vec<Value> {
    profile_reference_diagnostics(params, "desired_profiles")
}

fn profile_reference_diagnostics(params: &Value, field: &str) -> Vec<Value> {
    params
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|value| match value.as_str() {
            Some(reference) if profile_for_wrapper_reference(reference).is_none() => {
                Some(unknown_profile_diagnostic(field, reference))
            }
            None => Some(invalid_profile_type_diagnostic(field)),
            _ => None,
        })
        .collect()
}

fn wrapper_names() -> Vec<&'static str> {
    ACCOUNTS
        .iter()
        .map(|account| account.opencode_wrapper)
        .collect()
}

fn string_param<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn find_on_path(program: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|directory| directory.join(program))
            .find(|candidate| candidate.is_file())
    })
}

fn expand_tilde(path: &str) -> PathBuf {
    match (path.strip_prefix("~/"), std::env::var_os("HOME")) {
        (Some(relative), Some(home)) => Path::new(&home).join(relative),
        _ => PathBuf::from(path),
    }
}

fn setup_installed(
    opencode: &Value,
    curl: &Value,
    profiles: &[Value],
    settings_store: &SettingsTransitionReadiness,
) -> bool {
    evidence_ready(opencode)
        && evidence_ready(curl)
        && settings_store.ready
        && profiles
            .iter()
            .all(|profile| profile.get("profile_ready").and_then(Value::as_bool) == Some(true))
}

fn evidence_present(evidence: &Value) -> bool {
    evidence
        .get("present")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn evidence_ready(evidence: &Value) -> bool {
    evidence_present(evidence)
        && evidence.pointer("/version/ready").and_then(Value::as_bool) == Some(true)
}

fn detect_result(
    opencode: Value,
    curl: Value,
    profiles: Vec<Value>,
    settings_store: SettingsTransitionReadiness,
    installed: bool,
) -> Value {
    let warnings = setup_warnings(&opencode, &curl, &profiles, &settings_store);
    json!({
        "installed": installed,
        "binary": {
            "opencode": opencode,
            "curl": curl,
        },
        "auth": auth_summary(),
        "profiles": profiles,
        "warnings": warnings,
    })
}

fn install_plan_result(target: &str, settings_store: &SettingsTransitionReadiness) -> Value {
    json!({
        "steps": [
            {
                "kind": "verify_settings_transition",
                "target": target,
                "blocking": !settings_store.ready,
                "settings_store": settings_store.evidence(),
            },
            {"kind": "verify_tool", "target": target, "command": "opencode --version"},
            {"kind": "verify_tool", "target": target, "command": "curl --version"},
            {"kind": "verify_wrappers", "target": target, "wrappers": wrapper_names()},
            {"kind": "prepare_provider_settings", "schema_id": "opencode.settings/v1"}
        ]
    })
}

fn sync_plan_result(operations: Vec<Value>, diagnostics: Vec<Value>) -> Value {
    json!({ "operations": operations, "diagnostics": diagnostics })
}

fn desired_profiles(params: &Value) -> Vec<String> {
    let Some(values) = params
        .get("desired_profiles")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
    else {
        return default_profiles();
    };
    values
        .iter()
        .filter_map(Value::as_str)
        .filter_map(profile_for_wrapper_reference)
        .map(|account| account.opencode_wrapper.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn default_profiles() -> Vec<String> {
    ACCOUNTS
        .iter()
        .map(|account| account.opencode_wrapper)
        .map(str::to_string)
        .collect()
}

fn sync_operations(desired: &[String]) -> Vec<Value> {
    desired
        .iter()
        .map(|profile| {
            json!({
                "kind": "ensure_profile",
                "profile": profile,
                "schema_id": "opencode.settings/v1"
            })
        })
        .collect()
}

fn parse_native_identity_rebind(
    params: &Value,
    request_id: &str,
) -> Result<Option<NativeIdentityRebindRequest>, ProviderFailure> {
    params
        .get("native_identity_rebind")
        .cloned()
        .map(|value| {
            serde_json::from_value(value).map_err(|error| {
                invalid_native_identity_rebind(
                    request_id,
                    format!("maintenance request does not match its protocol: {error}"),
                )
            })
        })
        .transpose()
}

fn native_identity_rebind_operations(
    host: &HostContext,
    request: NativeIdentityRebindRequest,
    request_id: &str,
) -> Result<Vec<Value>, ProviderFailure> {
    match request {
        NativeIdentityRebindRequest::Plan { protocol, profiles } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            if profiles.is_empty() || profiles.len() > ACCOUNTS.len() {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "plan profiles must contain between one and five canonical profiles",
                ));
            }
            if profiles.iter().collect::<BTreeSet<_>>().len() != profiles.len() {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "plan profiles must not contain duplicates",
                ));
            }
            profiles
                .into_iter()
                .map(|profile| {
                    let account = canonical_rebind_profile(&profile, request_id)?;
                    let prior_binding =
                        native_identity_binding_evidence(host, account, request_id, false)?;
                    Ok(native_identity_rebind_operation(
                        account.opencode_wrapper,
                        &prior_binding,
                        &prior_binding,
                        "awaiting_host_drain",
                        None,
                        None,
                        Some("seal"),
                    ))
                })
                .collect()
        }
        NativeIdentityRebindRequest::Seal {
            protocol,
            operation_id,
            profile,
            prior_binding,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_binding_evidence(&prior_binding, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id =
                native_identity_rebind_operation_id(account.opencode_wrapper, &prior_binding);
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile and prior identity evidence",
                ));
            }
            if !host_handoff.ordinary_admission_blocked || !host_handoff.obligations_reconciled {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "cutover sealing requires blocked ordinary admission and reconciled provider obligations",
                ));
            }
            let sealed_binding =
                native_identity_binding_evidence(host, account, request_id, false)?;
            if sealed_binding != prior_binding {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "provider identity state changed during the host drain; request a new plan while admission remains blocked",
                ));
            }
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                &prior_binding,
                &sealed_binding,
                "awaiting_cutover",
                None,
                None,
                Some("observe"),
            )])
        }
        NativeIdentityRebindRequest::Observe {
            protocol,
            operation_id,
            profile,
            prior_binding,
            disposition,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_binding_evidence(&prior_binding, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id =
                native_identity_rebind_operation_id(account.opencode_wrapper, &prior_binding);
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile and prior identity evidence",
                ));
            }
            let observed_binding =
                native_identity_binding_evidence(host, account, request_id, true)?;
            let validation_window_complete = host_handoff.ordinary_admission_blocked
                && host_handoff.validation_launch_completed
                && host_handoff.validation_quota_probe_completed;
            let (phase, diagnostic) = match disposition {
                NativeIdentityRebindDisposition::Committed
                    if validation_window_complete
                        && binding_component_rebound(
                            prior_binding.native_runtime_state_sha256.as_deref(),
                            observed_binding.native_runtime_state_sha256.as_deref(),
                        )
                        && binding_component_rebound(
                            prior_binding.quota_observer_state_sha256.as_deref(),
                            observed_binding.quota_observer_state_sha256.as_deref(),
                        ) =>
                {
                    ("awaiting_host_release", None)
                }
                NativeIdentityRebindDisposition::RolledBack
                    if validation_window_complete && observed_binding == prior_binding =>
                {
                    ("awaiting_host_release", None)
                }
                NativeIdentityRebindDisposition::Committed => (
                    "rejected",
                    Some("commit observation requires ordinary admission to remain blocked, completion of the two cutover-validation capabilities, and two newly admitted provider identity records"),
                ),
                NativeIdentityRebindDisposition::RolledBack => (
                    "rejected",
                    Some("rollback observation requires ordinary admission to remain blocked, completion of the two cutover-validation capabilities, and exact restoration of both prior identity records"),
                ),
            };
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                &prior_binding,
                &observed_binding,
                phase,
                diagnostic,
                Some(disposition),
                (phase == "awaiting_host_release").then_some("release"),
            )])
        }
        NativeIdentityRebindRequest::Release {
            protocol,
            operation_id,
            observation_id,
            profile,
            prior_binding,
            observed_binding,
            disposition,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_binding_evidence(&prior_binding, request_id)?;
            validate_native_identity_binding_evidence(&observed_binding, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id =
                native_identity_rebind_operation_id(account.opencode_wrapper, &prior_binding);
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile and prior identity evidence",
                ));
            }
            let expected_observation_id = native_identity_rebind_observation_id(
                &operation_id,
                &observed_binding,
                disposition,
            );
            if observation_id != expected_observation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "observation_id does not bind the supplied operation, disposition, and observed identity evidence",
                ));
            }
            let current_binding =
                native_identity_binding_evidence(host, account, request_id, true)?;
            let (phase, diagnostic) = if !host_handoff.ordinary_admission_reopened {
                (
                    "rejected",
                    Some("release acknowledgment requires the host to reopen ordinary admission"),
                )
            } else if current_binding != observed_binding {
                (
                    "rejected",
                    Some("provider identity state changed after observation and before host release acknowledgment"),
                )
            } else {
                match disposition {
                    NativeIdentityRebindDisposition::Committed => ("completed", None),
                    NativeIdentityRebindDisposition::RolledBack => ("rolled_back", None),
                }
            };
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                &prior_binding,
                &current_binding,
                phase,
                diagnostic,
                Some(disposition),
                None,
            )])
        }
    }
}

fn native_identity_binding_evidence(
    host: &HostContext,
    account: &crate::account::AccountProfile,
    request_id: &str,
    require_valid_identity: bool,
) -> Result<NativeIdentityBindingEvidence, ProviderFailure> {
    let native_runtime_state_sha256 = if require_valid_identity {
        native_runtime::validated_persisted_state_sha256(host, account, request_id)?
    } else {
        native_runtime::persisted_state_sha256(host, account, request_id)?
    };
    let quota_observer_state_sha256 = if require_valid_identity {
        quota_observer::validated_persisted_state_sha256(host, account, request_id)?
    } else {
        quota_observer::persisted_state_sha256(host, account, request_id)?
    };
    Ok(NativeIdentityBindingEvidence {
        native_runtime_state_sha256,
        quota_observer_state_sha256,
    })
}

fn native_identity_rebind_operation(
    profile: &str,
    prior_binding: &NativeIdentityBindingEvidence,
    observed_binding: &NativeIdentityBindingEvidence,
    phase: &str,
    diagnostic: Option<&str>,
    disposition: Option<NativeIdentityRebindDisposition>,
    next_action: Option<&str>,
) -> Value {
    let operation_id = native_identity_rebind_operation_id(profile, prior_binding);
    let mut operation = json!({
        "kind": "native_identity_rebind",
        "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
        "schema_id": NATIVE_IDENTITY_REBIND_SCHEMA_ID,
        "operation_id": operation_id,
        "profile": profile,
        "phase": phase,
        "maximum_drain_ms": NATIVE_IDENTITY_REBIND_DRAIN_MS,
        "prior_binding": prior_binding,
        "observed_binding": observed_binding,
        "responsibilities": [
            {
                "actor": "host",
                "action": "block ordinary capability admission for this profile, bound every in-flight request to the declared drain interval, and keep ordinary admission blocked through provider observation",
                "completion": "seal and observe assert host_handoff.ordinary_admission_blocked=true"
            },
            {
                "actor": "operator",
                "action": "reconcile every nonterminal launch, rotation, and quota-refresh obligation before cutover",
                "completion": "the seal request asserts host_handoff.obligations_reconciled=true"
            },
            {
                "actor": "host",
                "action": "while ordinary admission remains blocked, authorize exactly one operation-bound validation launch and quota probe, then reopen ordinary admission only after provider observation",
                "completion": "observe asserts both validation capabilities completed; release asserts ordinary admission reopened"
            },
            {
                "actor": "operator",
                "action": "stage replacement dependencies and preserve both prior provider identity records for rollback",
                "completion": "both observed provider identity records differ from the plan-bound prior records, or both prior records are restored"
            },
            {
                "actor": "provider",
                "action": "bind the request to the plan identity and observe both provider-owned identity records",
                "completion": "observation emits an observation-bound release request; release returns completed, rolled_back, or rejected"
            }
        ],
        "implementation_evidence": {
            "provider_state_records": [
                format!("native-runtimes/{profile}.json"),
                format!("quota-observers/{profile}.json")
            ]
        }
    });
    match next_action {
        Some("seal") => {
            operation["next_request"] = json!({
                "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
                "action": "seal",
                "operation_id": operation_id,
                "profile": profile,
                "prior_binding": prior_binding,
                "host_handoff": {
                    "ordinary_admission_blocked": true,
                    "obligations_reconciled": true
                }
            });
        }
        Some("observe") => {
            operation["next_request"] = json!({
                "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
                "action": "observe",
                "operation_id": operation_id,
                "profile": profile,
                "prior_binding": prior_binding,
                "disposition": "committed",
                "host_handoff": {
                    "ordinary_admission_blocked": true,
                    "validation_launch_completed": true,
                    "validation_quota_probe_completed": true
                }
            });
        }
        Some("release") => {
            let disposition = disposition.expect("release follows a typed observation");
            let observation_id =
                native_identity_rebind_observation_id(&operation_id, observed_binding, disposition);
            operation["observation_id"] = json!(observation_id);
            operation["disposition"] = json!(disposition.as_str());
            operation["next_request"] = json!({
                "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
                "action": "release",
                "operation_id": operation_id,
                "observation_id": observation_id,
                "profile": profile,
                "prior_binding": prior_binding,
                "observed_binding": observed_binding,
                "disposition": disposition.as_str(),
                "host_handoff": {
                    "ordinary_admission_reopened": true
                }
            });
        }
        Some(_) => unreachable!("native rebind operation has a fixed next-action set"),
        None => {}
    }
    if let Some(disposition) = disposition {
        let observation_id =
            native_identity_rebind_observation_id(&operation_id, observed_binding, disposition);
        operation["observation_id"] = json!(observation_id);
        operation["disposition"] = json!(disposition.as_str());
    }
    if let Some(diagnostic) = diagnostic {
        operation["diagnostic"] = json!(diagnostic);
    }
    operation
}

fn native_identity_rebind_observation_id(
    operation_id: &str,
    observed_binding: &NativeIdentityBindingEvidence,
    disposition: NativeIdentityRebindDisposition,
) -> String {
    sha256_hex(
        json!({
            "operation_id": operation_id,
            "observed_binding": observed_binding,
            "disposition": disposition.as_str(),
        })
        .to_string()
        .as_bytes(),
    )
}

fn native_identity_rebind_operation_id(
    profile: &str,
    prior_binding: &NativeIdentityBindingEvidence,
) -> String {
    sha256_hex(
        json!({
            "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
            "profile": profile,
            "prior_binding": prior_binding,
        })
        .to_string()
        .as_bytes(),
    )
}

fn binding_component_rebound(prior: Option<&str>, observed: Option<&str>) -> bool {
    observed.is_some() && observed != prior
}

fn canonical_rebind_profile(
    profile: &str,
    request_id: &str,
) -> Result<&'static crate::account::AccountProfile, ProviderFailure> {
    profile_for_wrapper_reference(profile)
        .filter(|account| account.opencode_wrapper == profile)
        .ok_or_else(|| {
            invalid_native_identity_rebind(
                request_id,
                format!("native identity rebind requires a canonical profile: {profile}"),
            )
        })
}

fn require_native_identity_rebind_protocol(
    protocol: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if protocol == NATIVE_IDENTITY_REBIND_PROTOCOL {
        return Ok(());
    }
    Err(invalid_native_identity_rebind(
        request_id,
        format!("unsupported native identity rebind protocol: {protocol}"),
    ))
}

fn validate_native_identity_binding_evidence(
    evidence: &NativeIdentityBindingEvidence,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if evidence
        .native_runtime_state_sha256
        .as_deref()
        .is_none_or(valid_sha256)
        && evidence
            .quota_observer_state_sha256
            .as_deref()
            .is_none_or(valid_sha256)
    {
        return Ok(());
    }
    Err(invalid_native_identity_rebind(
        request_id,
        "prior identity evidence must contain lowercase SHA-256 values or null",
    ))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn invalid_native_identity_rebind(request_id: &str, message: impl Into<String>) -> ProviderFailure {
    ProviderFailure::invalid_request(request_id, "invalid_native_identity_rebind", message)
}

fn opencode_auth_file_present(path: &str) -> bool {
    expand_tilde(path).is_file()
}

fn settings_schema_mismatch_diagnostic() -> Value {
    json!({
        "severity": "warning",
        "path": "settings_schema_id",
        "message": "sync plan expects opencode.settings/v1 settings",
        "code": "settings_schema_mismatch",
    })
}

fn unknown_profile_diagnostic(field: &str, reference: &str) -> Value {
    json!({
        "severity": "error",
        "path": field,
        "message": format!("unknown OpenCode account wrapper reference: {reference}"),
        "code": "unknown_opencode_profile",
    })
}

fn invalid_profile_type_diagnostic(field: &str) -> Value {
    json!({
        "severity": "error",
        "path": field,
        "message": "OpenCode account wrapper references must be strings",
        "code": "invalid_opencode_profile",
    })
}
