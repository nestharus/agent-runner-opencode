//! Declared roles: accessor, mapper, orchestration, validator, predicate, filter, formatter, parser

use crate::account::{profile_for_wrapper_reference, ACCOUNTS};
use crate::child_custody::ChildCustody;
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope};
use crate::operation_bounds;
use crate::shell;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const SETUP_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

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
        Command::InstallPlan => install_plan_params(params, &request_id),
        Command::SyncPlan => sync_plan_params(params, &request_id),
        Command::BrainTurn => Err(brain_unsupported(request_id)),
    }
}

pub fn detect_params(
    host: &HostContext,
    params: Value,
    _request_id: &str,
) -> Result<Value, ProviderFailure> {
    let data_root = string_param(&params, "data_root").or(host.data_root.as_deref());
    let profile_root = string_param(&params, "profile_root");
    let opencode = executable_evidence("opencode", host.deadline_unix_ms);
    let curl = executable_evidence("curl", host.deadline_unix_ms);
    let profiles = profile_evidence(data_root, profile_root, host.deadline_unix_ms);
    let installed = setup_installed(&opencode, &curl, &profiles);
    Ok(detect_result(opencode, curl, profiles, installed))
}

pub fn install_plan_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let target = string_param(&params, "target").unwrap_or("local");
    Ok(install_plan_result(target))
}

pub fn sync_plan_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let desired = desired_profiles(&params);
    let rebind = rebind_profiles(&params);
    let operations = sync_operations(&desired, &rebind);
    let diagnostics = sync_diagnostics(&params);
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

fn setup_warnings(opencode: &Value, curl: &Value, profiles: &[Value]) -> Vec<Value> {
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
    warnings
}

fn sync_diagnostics(params: &Value) -> Vec<Value> {
    let mut diagnostics = desired_profile_diagnostics(params);
    diagnostics.extend(profile_reference_diagnostics(params, "rebind_profiles"));
    if params.get("settings_schema_id").and_then(Value::as_str) != Some("opencode.settings/v1") {
        diagnostics.push(settings_schema_mismatch_diagnostic());
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

fn setup_installed(opencode: &Value, curl: &Value, profiles: &[Value]) -> bool {
    evidence_ready(opencode)
        && evidence_ready(curl)
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

fn detect_result(opencode: Value, curl: Value, profiles: Vec<Value>, installed: bool) -> Value {
    let warnings = setup_warnings(&opencode, &curl, &profiles);
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

fn install_plan_result(target: &str) -> Value {
    json!({
        "steps": [
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

fn sync_operations(desired: &[String], rebind: &[String]) -> Vec<Value> {
    let mut operations = desired
        .iter()
        .map(|profile| {
            json!({
                "kind": "ensure_profile",
                "profile": profile,
                "schema_id": "opencode.settings/v1"
            })
        })
        .collect::<Vec<_>>();
    operations.extend(rebind.iter().map(|profile| {
        json!({
            "kind": "rebind_native_identity",
            "profile": profile,
            "maximum_drain_ms": 20_000,
            "preconditions": [
                "stop new admission for this profile",
                "apply a host deadline of at most 20 seconds to in-flight provider work",
                "reconcile every nonterminal launch, rotation, and quota-refresh operation",
                "stage the replacement wrapper and curl without mutating admitted files in place"
            ],
            "state_files": [
                format!("provider-state/opencode/native-runtimes/{profile}.json"),
                format!("provider-state/opencode/quota-observers/{profile}.json")
            ],
            "cutover": "back up and remove both context files, then admit one probe and launch under the staged PATH/environment; restore the backups if admission fails"
        })
    }));
    operations
}

fn rebind_profiles(params: &Value) -> Vec<String> {
    params
        .get("rebind_profiles")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(Value::as_str)
        .filter_map(profile_for_wrapper_reference)
        .map(|account| account.opencode_wrapper.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
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
