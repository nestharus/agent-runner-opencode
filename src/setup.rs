//! Declared roles: accessor, mapper, orchestration, validator, predicate, filter, formatter, parser

use crate::account::{profile_for_wrapper_reference, ACCOUNTS};
use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope};
use crate::native_runtime;
use crate::operation_bounds;
use crate::path_guard;
use crate::quota_observer;
use crate::schema::NATIVE_IDENTITY_REBIND_SCHEMA_ID;
use crate::settings::{self, SettingsTransitionReadiness};
use crate::shell;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const SETUP_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const NATIVE_IDENTITY_REBIND_PROTOCOL: &str = "opencode.native-identity-rebind/v1";
const NATIVE_IDENTITY_REBIND_DRAIN_MS: u64 = 20_000;
const NATIVE_IDENTITY_REBIND_STATE_DIR: &str = "provider-state/opencode/native-identity-rebind";
const NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION: u32 = 1;
const NATIVE_IDENTITY_REBIND_STATE_BYTES: usize = 16 * 1024;
const NATIVE_IDENTITY_REBIND_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum NativeIdentityRebindRequest {
    Plan {
        protocol: String,
        targets: Vec<NativeIdentityRebindTarget>,
    },
    Seal {
        protocol: String,
        operation_id: String,
        profile: String,
        component: NativeIdentityRebindComponent,
        prior_evidence: NativeIdentityRebindEvidence,
        host_handoff: NativeIdentityRebindSealHandoff,
    },
    Observe {
        protocol: String,
        operation_id: String,
        profile: String,
        component: NativeIdentityRebindComponent,
        prior_evidence: NativeIdentityRebindEvidence,
        disposition: NativeIdentityRebindDisposition,
        host_handoff: NativeIdentityRebindObservationHandoff,
    },
    Release {
        protocol: String,
        operation_id: String,
        observation_id: String,
        profile: String,
        component: NativeIdentityRebindComponent,
        prior_evidence: NativeIdentityRebindEvidence,
        observed_evidence: NativeIdentityRebindEvidence,
        disposition: NativeIdentityRebindDisposition,
        host_handoff: NativeIdentityRebindReleaseHandoff,
    },
}

#[derive(Clone, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindTarget {
    profile: String,
    component: NativeIdentityRebindComponent,
}

#[derive(Clone, Copy, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum NativeIdentityRebindComponent {
    NativeRuntime,
    QuotaObserver,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindEvidence {
    component_identity_sha256: Option<String>,
    state_record_sha256: Option<String>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
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
    validation_capability_completed: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindReleaseHandoff {
    ordinary_admission_reopened: bool,
}

struct NativeIdentityRebindOperationView<'a> {
    component: NativeIdentityRebindComponent,
    prior_evidence: &'a NativeIdentityRebindEvidence,
    observed_evidence: &'a NativeIdentityRebindEvidence,
    phase: &'a str,
    diagnostic: Option<&'a str>,
    disposition: Option<NativeIdentityRebindDisposition>,
    next_action: Option<&'a str>,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindObservationRecord {
    schema_version: u32,
    operation_id: String,
    observation_id: String,
    profile: String,
    component: NativeIdentityRebindComponent,
    prior_evidence: NativeIdentityRebindEvidence,
    observed_evidence: NativeIdentityRebindEvidence,
    disposition: NativeIdentityRebindDisposition,
    phase: NativeIdentityRebindObservationPhase,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum NativeIdentityRebindObservationPhase {
    AwaitingHostRelease,
    Completed,
    RolledBack,
}

impl NativeIdentityRebindObservationPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingHostRelease => "awaiting_host_release",
            Self::Completed => "completed",
            Self::RolledBack => "rolled_back",
        }
    }
}

impl NativeIdentityRebindDisposition {
    fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
        }
    }
}

impl NativeIdentityRebindComponent {
    fn as_str(self) -> &'static str {
        match self {
            Self::NativeRuntime => "native_runtime",
            Self::QuotaObserver => "quota_observer",
        }
    }

    fn validation_capability(self) -> &'static str {
        match self {
            Self::NativeRuntime => "launch",
            Self::QuotaObserver => "quota_probe",
        }
    }

    fn provider_state_record(self, profile: &str) -> String {
        match self {
            Self::NativeRuntime => format!("native-runtimes/{profile}.json"),
            Self::QuotaObserver => format!("quota-observers/{profile}.json"),
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
    let required_settings_ids = required_settings_ids(&params, request_id)?;
    let opencode = executable_evidence("opencode", host.deadline_unix_ms);
    let curl = executable_evidence("curl", host.deadline_unix_ms);
    let profiles = profile_evidence(data_root, profile_root, host.deadline_unix_ms);
    let settings_store = settings::transition_readiness(host, request_id, &required_settings_ids);
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
    let required_settings_ids = required_settings_ids(&params, request_id)?;
    let settings_store = settings::transition_readiness(host, request_id, &required_settings_ids);
    Ok(install_plan_result(target, &settings_store))
}

pub fn sync_plan_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let required_settings_ids = required_settings_ids(&params, request_id)?;
    let desired = desired_profiles(&params);
    let mut operations = sync_operations(&desired);
    if let Some(rebind) = parse_native_identity_rebind(&params, request_id)? {
        operations.extend(native_identity_rebind_operations(host, rebind, request_id)?);
    }
    let settings_store = settings::transition_readiness(host, request_id, &required_settings_ids);
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

/// Provider setup defaults to the exact record IDs used by the installed
/// Agent Runner OpenCode provider names. A host that has adopted opaque record
/// IDs can instead declare one `settings_id` or the complete `settings_ids`
/// population whose activation must be proven before setup reports ready.
fn required_settings_ids(params: &Value, request_id: &str) -> Result<Vec<String>, ProviderFailure> {
    let one = params.get("settings_id");
    let many = params.get("settings_ids");
    if one.is_some() && many.is_some() {
        return Err(invalid_setup_settings_ids(
            request_id,
            "settings_id and settings_ids are mutually exclusive",
        ));
    }
    let declared = if let Some(value) = one {
        vec![non_empty_setup_settings_id(value, request_id)?.to_string()]
    } else if let Some(values) = many {
        let values = values.as_array().ok_or_else(|| {
            invalid_setup_settings_ids(request_id, "settings_ids must be an array")
        })?;
        if values.is_empty() || values.len() > settings::MAX_SETTINGS_ACTIVATION_IDS {
            return Err(invalid_setup_settings_ids(
                request_id,
                "settings_ids must contain between one and 4096 exact record IDs",
            ));
        }
        values
            .iter()
            .map(|value| non_empty_setup_settings_id(value, request_id).map(str::to_string))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        ACCOUNTS
            .iter()
            .map(|account| {
                if account.opencode_index == 1 {
                    "opencode".to_string()
                } else {
                    account.opencode_wrapper.to_string()
                }
            })
            .collect()
    };
    let unique = declared.into_iter().collect::<BTreeSet<_>>();
    if unique.is_empty() {
        return Err(invalid_setup_settings_ids(
            request_id,
            "at least one exact settings ID is required",
        ));
    }
    Ok(unique.into_iter().collect())
}

fn non_empty_setup_settings_id<'a>(
    value: &'a Value,
    request_id: &str,
) -> Result<&'a str, ProviderFailure> {
    value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            invalid_setup_settings_ids(request_id, "settings IDs must be non-empty strings")
        })
}

fn invalid_setup_settings_ids(request_id: &str, message: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(request_id, "invalid_setup_settings_ids", message)
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
            {
                "kind": "prepare_provider_settings",
                "schema_id": "opencode.settings/v1",
                "activation_operation": "settings.migrate",
                "activation_identity": "legacy_provider_table_key_to_exact_settings_record_id"
            }
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
        NativeIdentityRebindRequest::Plan { protocol, targets } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            if targets.is_empty() || targets.len() > ACCOUNTS.len() * 2 {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "plan targets must contain between one and ten component-scoped identities",
                ));
            }
            if targets.iter().collect::<BTreeSet<_>>().len() != targets.len() {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "plan targets must not contain duplicate profile/component identities",
                ));
            }
            targets
                .into_iter()
                .map(|target| {
                    let account = canonical_rebind_profile(&target.profile, request_id)?;
                    let prior_evidence = native_identity_evidence(
                        host,
                        account,
                        target.component,
                        request_id,
                        false,
                    )?;
                    Ok(native_identity_rebind_operation(
                        account.opencode_wrapper,
                        NativeIdentityRebindOperationView {
                            component: target.component,
                            prior_evidence: &prior_evidence,
                            observed_evidence: &prior_evidence,
                            phase: "awaiting_host_drain",
                            diagnostic: None,
                            disposition: None,
                            next_action: Some("seal"),
                        },
                    ))
                })
                .collect()
        }
        NativeIdentityRebindRequest::Seal {
            protocol,
            operation_id,
            profile,
            component,
            prior_evidence,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_evidence(&prior_evidence, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id = native_identity_rebind_operation_id(
                account.opencode_wrapper,
                component,
                &prior_evidence,
            );
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile, component, and prior identity evidence",
                ));
            }
            if !host_handoff.ordinary_admission_blocked || !host_handoff.obligations_reconciled {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "cutover sealing requires blocked ordinary admission and reconciled provider obligations",
                ));
            }
            let sealed_evidence =
                native_identity_evidence(host, account, component, request_id, false)?;
            if sealed_evidence != prior_evidence {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "selected provider identity state changed during the host drain; request a new component-scoped plan while admission remains blocked",
                ));
            }
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                NativeIdentityRebindOperationView {
                    component,
                    prior_evidence: &prior_evidence,
                    observed_evidence: &sealed_evidence,
                    phase: "awaiting_cutover",
                    diagnostic: None,
                    disposition: None,
                    next_action: Some("observe"),
                },
            )])
        }
        NativeIdentityRebindRequest::Observe {
            protocol,
            operation_id,
            profile,
            component,
            prior_evidence,
            disposition,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_evidence(&prior_evidence, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id = native_identity_rebind_operation_id(
                account.opencode_wrapper,
                component,
                &prior_evidence,
            );
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile, component, and prior identity evidence",
                ));
            }
            let observed_evidence =
                native_identity_evidence(host, account, component, request_id, true)?;
            let validation_window_complete = host_handoff.ordinary_admission_blocked
                && host_handoff.validation_capability_completed;
            let (phase, diagnostic) = match disposition {
                _ if validation_window_complete
                    && native_identity_rebind_disposition_matches(
                        disposition,
                        &prior_evidence,
                        &observed_evidence,
                    ) =>
                {
                    let admitted_phase = persist_native_identity_rebind_observation(
                        host,
                        account.opencode_wrapper,
                        component,
                        &prior_evidence,
                        &observed_evidence,
                        disposition,
                        request_id,
                    )?;
                    (admitted_phase.as_str(), None)
                }
                NativeIdentityRebindDisposition::Committed => (
                    "rejected",
                    Some("commit observation requires ordinary admission to remain blocked, completion of the selected component's validation capability, and a newly admitted identity record for that component"),
                ),
                NativeIdentityRebindDisposition::RolledBack => (
                    "rejected",
                    Some("rollback observation requires ordinary admission to remain blocked, completion of the selected component's validation capability, and exact restoration of that component's prior identity record"),
                ),
            };
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                NativeIdentityRebindOperationView {
                    component,
                    prior_evidence: &prior_evidence,
                    observed_evidence: &observed_evidence,
                    phase,
                    diagnostic,
                    disposition: Some(disposition),
                    next_action: (phase == "awaiting_host_release").then_some("release"),
                },
            )])
        }
        NativeIdentityRebindRequest::Release {
            protocol,
            operation_id,
            observation_id,
            profile,
            component,
            prior_evidence,
            observed_evidence,
            disposition,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_evidence(&prior_evidence, request_id)?;
            validate_native_identity_evidence(&observed_evidence, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id = native_identity_rebind_operation_id(
                account.opencode_wrapper,
                component,
                &prior_evidence,
            );
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile, component, and prior identity evidence",
                ));
            }
            let expected_observation_id = native_identity_rebind_observation_id(
                &operation_id,
                &observed_evidence,
                disposition,
            );
            if observation_id != expected_observation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "observation_id does not bind the supplied operation, disposition, and observed component identity",
                ));
            }
            let _observation_lock = acquire_native_identity_rebind_lock(
                host,
                account.opencode_wrapper,
                component,
                request_id,
            )?;
            let admitted_observation = read_native_identity_rebind_observation(
                host,
                account.opencode_wrapper,
                component,
                request_id,
            )?
            .ok_or_else(|| {
                invalid_native_identity_rebind(
                    request_id,
                    "release requires a provider-admitted awaiting_host_release observation",
                )
            })?;
            let expected_observation = NativeIdentityRebindObservationRecord {
                schema_version: NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION,
                operation_id: operation_id.clone(),
                observation_id: observation_id.clone(),
                profile: account.opencode_wrapper.to_string(),
                component,
                prior_evidence: prior_evidence.clone(),
                observed_evidence: observed_evidence.clone(),
                disposition,
                phase: admitted_observation.phase,
            };
            if !native_identity_rebind_observation_matches(
                &admitted_observation,
                &expected_observation,
            ) {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "release does not match the provider-admitted awaiting_host_release observation",
                ));
            }
            if !host_handoff.ordinary_admission_reopened {
                return Ok(vec![native_identity_rebind_operation(
                    account.opencode_wrapper,
                    NativeIdentityRebindOperationView {
                        component,
                        prior_evidence: &prior_evidence,
                        observed_evidence: &observed_evidence,
                        phase: "rejected",
                        diagnostic: Some(
                            "release acknowledgment requires the host to reopen ordinary admission",
                        ),
                        disposition: Some(disposition),
                        next_action: None,
                    },
                )]);
            }
            if admitted_observation.phase
                != NativeIdentityRebindObservationPhase::AwaitingHostRelease
            {
                return Ok(vec![native_identity_rebind_operation(
                    account.opencode_wrapper,
                    NativeIdentityRebindOperationView {
                        component,
                        prior_evidence: &prior_evidence,
                        observed_evidence: &admitted_observation.observed_evidence,
                        phase: admitted_observation.phase.as_str(),
                        diagnostic: None,
                        disposition: Some(disposition),
                        next_action: None,
                    },
                )]);
            }
            let current_evidence =
                native_identity_evidence(host, account, component, request_id, true)?;
            let (phase, diagnostic) = if current_evidence != observed_evidence {
                (
                    "rejected",
                    Some("selected provider identity changed after observation and before host release acknowledgment"),
                )
            } else if !native_identity_rebind_disposition_matches(
                disposition,
                &prior_evidence,
                &current_evidence,
            ) {
                (
                    "rejected",
                    Some("selected provider identity no longer satisfies the admitted observation disposition"),
                )
            } else {
                let terminal_phase = match disposition {
                    NativeIdentityRebindDisposition::Committed => {
                        NativeIdentityRebindObservationPhase::Completed
                    }
                    NativeIdentityRebindDisposition::RolledBack => {
                        NativeIdentityRebindObservationPhase::RolledBack
                    }
                };
                let terminal_observation = NativeIdentityRebindObservationRecord {
                    phase: terminal_phase,
                    ..admitted_observation
                };
                write_native_identity_rebind_observation(host, &terminal_observation, request_id)?;
                (terminal_phase.as_str(), None)
            };
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                NativeIdentityRebindOperationView {
                    component,
                    prior_evidence: &prior_evidence,
                    observed_evidence: &current_evidence,
                    phase,
                    diagnostic,
                    disposition: Some(disposition),
                    next_action: None,
                },
            )])
        }
    }
}

fn native_identity_evidence(
    host: &HostContext,
    account: &crate::account::AccountProfile,
    component: NativeIdentityRebindComponent,
    request_id: &str,
    require_valid_identity: bool,
) -> Result<NativeIdentityRebindEvidence, ProviderFailure> {
    let evidence = match (component, require_valid_identity) {
        (NativeIdentityRebindComponent::NativeRuntime, true) => {
            native_runtime::validated_persisted_identity_evidence(host, account, request_id)?
        }
        (NativeIdentityRebindComponent::NativeRuntime, false) => {
            native_runtime::persisted_identity_evidence(host, account, request_id)?
        }
        (NativeIdentityRebindComponent::QuotaObserver, true) => {
            quota_observer::validated_persisted_identity_evidence(host, account, request_id)?
        }
        (NativeIdentityRebindComponent::QuotaObserver, false) => {
            quota_observer::persisted_identity_evidence(host, account, request_id)?
        }
    };
    let (component_identity_sha256, state_record_sha256) = evidence
        .map(|(component_identity, state_record)| (Some(component_identity), Some(state_record)))
        .unwrap_or((None, None));
    Ok(NativeIdentityRebindEvidence {
        component_identity_sha256,
        state_record_sha256,
    })
}

fn native_identity_rebind_operation(
    profile: &str,
    view: NativeIdentityRebindOperationView<'_>,
) -> Value {
    let NativeIdentityRebindOperationView {
        component,
        prior_evidence,
        observed_evidence,
        phase,
        diagnostic,
        disposition,
        next_action,
    } = view;
    let operation_id = native_identity_rebind_operation_id(profile, component, prior_evidence);
    let validation_capability = component.validation_capability();
    let mut operation = json!({
        "kind": "native_identity_rebind",
        "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
        "schema_id": NATIVE_IDENTITY_REBIND_SCHEMA_ID,
        "operation_id": operation_id,
        "profile": profile,
        "component": component.as_str(),
        "phase": phase,
        "maximum_drain_ms": NATIVE_IDENTITY_REBIND_DRAIN_MS,
        "prior_evidence": prior_evidence,
        "observed_evidence": observed_evidence,
        "responsibilities": [
            {
                "actor": "host",
                "action": format!("block ordinary capability admission that consumes the selected {} identity, bound in-flight consumers to the drain interval, and keep ordinary admission blocked through provider observation", component.as_str()),
                "completion": "seal and observe assert host_handoff.ordinary_admission_blocked=true"
            },
            {
                "actor": "operator",
                "action": format!("reconcile every nonterminal obligation that consumes the selected {} identity before cutover", component.as_str()),
                "completion": "the seal request asserts host_handoff.obligations_reconciled=true"
            },
            {
                "actor": "host",
                "action": format!("while ordinary admission remains blocked, authorize exactly one operation-bound {validation_capability}, then reopen ordinary admission only after provider observation"),
                "completion": "observe asserts the selected validation capability completed; release asserts ordinary admission reopened"
            },
            {
                "actor": "operator",
                "action": format!("stage the replacement {} dependency and preserve its prior provider identity record for rollback", component.as_str()),
                "completion": "the observed component identity differs from the plan-bound prior identity, or the prior identity is restored"
            },
            {
                "actor": "provider",
                "action": "bind the request to the component-scoped plan identity and observe that provider-owned identity record",
                "completion": "observation emits an observation-bound release request; release returns completed, rolled_back, or rejected"
            }
        ],
        "implementation_evidence": {
            "provider_state_record": component.provider_state_record(profile)
        }
    });
    match next_action {
        Some("seal") => {
            operation["next_request"] = json!({
                "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
                "action": "seal",
                "operation_id": operation_id,
                "profile": profile,
                "component": component.as_str(),
                "prior_evidence": prior_evidence,
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
                "component": component.as_str(),
                "prior_evidence": prior_evidence,
                "disposition": "committed",
                "host_handoff": {
                    "ordinary_admission_blocked": true,
                    "validation_capability_completed": true
                }
            });
        }
        Some("release") => {
            let disposition = disposition.expect("release follows a typed observation");
            let observation_id = native_identity_rebind_observation_id(
                &operation_id,
                observed_evidence,
                disposition,
            );
            operation["observation_id"] = json!(observation_id);
            operation["disposition"] = json!(disposition.as_str());
            operation["next_request"] = json!({
                "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
                "action": "release",
                "operation_id": operation_id,
                "observation_id": observation_id,
                "profile": profile,
                "component": component.as_str(),
                "prior_evidence": prior_evidence,
                "observed_evidence": observed_evidence,
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
            native_identity_rebind_observation_id(&operation_id, observed_evidence, disposition);
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
    observed_evidence: &NativeIdentityRebindEvidence,
    disposition: NativeIdentityRebindDisposition,
) -> String {
    sha256_hex(
        json!({
            "operation_id": operation_id,
            "observed_evidence": observed_evidence,
            "disposition": disposition.as_str(),
        })
        .to_string()
        .as_bytes(),
    )
}

fn native_identity_rebind_disposition_matches(
    disposition: NativeIdentityRebindDisposition,
    prior_evidence: &NativeIdentityRebindEvidence,
    observed_evidence: &NativeIdentityRebindEvidence,
) -> bool {
    match disposition {
        NativeIdentityRebindDisposition::Committed => identity_component_rebound(
            prior_evidence.component_identity_sha256.as_deref(),
            observed_evidence.component_identity_sha256.as_deref(),
        ),
        NativeIdentityRebindDisposition::RolledBack => observed_evidence == prior_evidence,
    }
}

fn persist_native_identity_rebind_observation(
    host: &HostContext,
    profile: &str,
    component: NativeIdentityRebindComponent,
    prior_evidence: &NativeIdentityRebindEvidence,
    observed_evidence: &NativeIdentityRebindEvidence,
    disposition: NativeIdentityRebindDisposition,
    request_id: &str,
) -> Result<NativeIdentityRebindObservationPhase, ProviderFailure> {
    let _lock = acquire_native_identity_rebind_lock(host, profile, component, request_id)?;
    let operation_id = native_identity_rebind_operation_id(profile, component, prior_evidence);
    let observation = NativeIdentityRebindObservationRecord {
        schema_version: NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION,
        observation_id: native_identity_rebind_observation_id(
            &operation_id,
            observed_evidence,
            disposition,
        ),
        operation_id,
        profile: profile.to_string(),
        component,
        prior_evidence: prior_evidence.clone(),
        observed_evidence: observed_evidence.clone(),
        disposition,
        phase: NativeIdentityRebindObservationPhase::AwaitingHostRelease,
    };
    if let Some(existing) =
        read_native_identity_rebind_observation(host, profile, component, request_id)?
    {
        if native_identity_rebind_observation_matches(&existing, &observation) {
            return Ok(existing.phase);
        }
        if existing.phase == NativeIdentityRebindObservationPhase::AwaitingHostRelease {
            return Err(invalid_native_identity_rebind(
                request_id,
                "a different admitted observation still owns release for this profile/component identity",
            ));
        }
    }
    write_native_identity_rebind_observation(host, &observation, request_id)?;
    Ok(observation.phase)
}

fn write_native_identity_rebind_observation(
    host: &HostContext,
    observation: &NativeIdentityRebindObservationRecord,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let path = native_identity_rebind_observation_path(
        host,
        &observation.profile,
        observation.component,
        request_id,
    )?;
    let parent = path
        .parent()
        .expect("native identity rebind observation always has a parent");
    let bytes = serde_json::to_vec_pretty(&observation)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    if bytes.len() > NATIVE_IDENTITY_REBIND_STATE_BYTES {
        return Err(native_identity_rebind_state_failure(
            request_id,
            format!(
                "observation record exceeds supported {NATIVE_IDENTITY_REBIND_STATE_BYTES}-byte bound"
            ),
        ));
    }
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    temporary
        .write_all(&bytes)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    temporary
        .persist(&path)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error.error))?;
    durable_fs::sync_directory(parent)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))
}

fn native_identity_rebind_observation_matches(
    left: &NativeIdentityRebindObservationRecord,
    right: &NativeIdentityRebindObservationRecord,
) -> bool {
    left.schema_version == right.schema_version
        && left.operation_id == right.operation_id
        && left.observation_id == right.observation_id
        && left.profile == right.profile
        && left.component == right.component
        && left.prior_evidence == right.prior_evidence
        && left.observed_evidence == right.observed_evidence
        && left.disposition == right.disposition
}

fn read_native_identity_rebind_observation(
    host: &HostContext,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<Option<NativeIdentityRebindObservationRecord>, ProviderFailure> {
    let path = native_identity_rebind_observation_path(host, profile, component, request_id)?;
    let bytes = match durable_fs::read_file_bounded(&path, NATIVE_IDENTITY_REBIND_STATE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(native_identity_rebind_state_failure(request_id, error)),
    };
    let observation: NativeIdentityRebindObservationRecord = serde_json::from_slice(&bytes)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    let phase_matches_disposition = match observation.phase {
        NativeIdentityRebindObservationPhase::AwaitingHostRelease => true,
        NativeIdentityRebindObservationPhase::Completed => {
            observation.disposition == NativeIdentityRebindDisposition::Committed
        }
        NativeIdentityRebindObservationPhase::RolledBack => {
            observation.disposition == NativeIdentityRebindDisposition::RolledBack
        }
    };
    if observation.schema_version != NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION
        || observation.profile != profile
        || observation.component != component
        || !phase_matches_disposition
        || observation.operation_id
            != native_identity_rebind_operation_id(profile, component, &observation.prior_evidence)
        || observation.observation_id
            != native_identity_rebind_observation_id(
                &observation.operation_id,
                &observation.observed_evidence,
                observation.disposition,
            )
    {
        return Err(native_identity_rebind_state_failure(
            request_id,
            "persisted observation record is inconsistent",
        ));
    }
    Ok(Some(observation))
}

fn acquire_native_identity_rebind_lock(
    host: &HostContext,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let root = native_identity_rebind_state_root(host, request_id)?;
    durable_fs::create_private_directories(&root)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    let lock_path = confined_native_identity_rebind_target(
        host,
        &root.join(format!("{profile}-{}.lock", component.as_str())),
        request_id,
    )?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = options
        .open(lock_path)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    let timeout = operation_bounds::remaining_timeout(
        host.deadline_unix_ms,
        NATIVE_IDENTITY_REBIND_LOCK_TIMEOUT,
    )
    .ok_or_else(|| native_identity_rebind_lock_timeout(request_id))?;
    if !operation_bounds::lock_exclusive_for(&lock, timeout)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?
    {
        return Err(native_identity_rebind_lock_timeout(request_id));
    }
    Ok(lock)
}

fn native_identity_rebind_observation_path(
    host: &HostContext,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let root = native_identity_rebind_state_root(host, request_id)?;
    confined_native_identity_rebind_target(
        host,
        &root.join(format!("{profile}-{}.json", component.as_str())),
        request_id,
    )
}

fn native_identity_rebind_state_root(
    host: &HostContext,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = native_identity_rebind_data_root(host, request_id)?;
    confined_native_identity_rebind_target(
        host,
        &data_root.join(NATIVE_IDENTITY_REBIND_STATE_DIR),
        request_id,
    )
}

fn native_identity_rebind_data_root<'a>(
    host: &'a HostContext,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    host.data_root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| {
            invalid_native_identity_rebind(
                request_id,
                "native identity rebind requires host.data_root for durable observation custody",
            )
        })
}

fn confined_native_identity_rebind_target(
    host: &HostContext,
    target: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = native_identity_rebind_data_root(host, request_id)?;
    path_guard::confined_target(data_root, target)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))
}

fn native_identity_rebind_state_failure(
    request_id: &str,
    error: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "native_identity_rebind_state_failed",
        format!("native identity rebind observation custody failed: {error}"),
    )
}

fn native_identity_rebind_lock_timeout(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "native_identity_rebind_lock_timeout",
        "native identity rebind observation lock could not be acquired before the operation deadline",
    )
}

fn native_identity_rebind_operation_id(
    profile: &str,
    component: NativeIdentityRebindComponent,
    prior_evidence: &NativeIdentityRebindEvidence,
) -> String {
    sha256_hex(
        json!({
            "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
            "profile": profile,
            "component": component.as_str(),
            "prior_evidence": prior_evidence,
        })
        .to_string()
        .as_bytes(),
    )
}

fn identity_component_rebound(prior: Option<&str>, observed: Option<&str>) -> bool {
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

fn validate_native_identity_evidence(
    evidence: &NativeIdentityRebindEvidence,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let component_valid = evidence
        .component_identity_sha256
        .as_deref()
        .is_none_or(valid_sha256);
    let record_valid = evidence
        .state_record_sha256
        .as_deref()
        .is_none_or(valid_sha256);
    let presence_matches =
        evidence.component_identity_sha256.is_some() == evidence.state_record_sha256.is_some();
    if component_valid && record_valid && presence_matches {
        return Ok(());
    }
    Err(invalid_native_identity_rebind(
        request_id,
        "component evidence must contain paired lowercase semantic-identity and state-record SHA-256 values, or two null values",
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
