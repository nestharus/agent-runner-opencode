//! Declared roles: accessor, mapper, orchestration, validator, predicate, filter, formatter, parser

use crate::account::{profile_for_wrapper_reference, ACCOUNTS};
use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope};
use crate::launch;
use crate::native_implementation_manifest;
use crate::native_runtime;
use crate::operation_bounds;
use crate::quota_observer;
use crate::settings::{self, SettingsTransitionReadiness};
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
    let curl = quota_transport_evidence(host.deadline_unix_ms);
    let profiles = profile_evidence(host, data_root, profile_root, &opencode, &curl, request_id);
    let settings_store = settings::transition_readiness(host, request_id, &required_settings_ids);
    let launch_custody = launch_custody_readiness(host, &params);
    let installed = setup_installed(&profiles, &settings_store, &launch_custody);
    Ok(detect_result(
        opencode,
        curl,
        profiles,
        settings_store,
        launch_custody,
        installed,
    ))
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn quota_transport_evidence(deadline_unix_ms: Option<u64>) -> Value {
    executable_evidence("curl", deadline_unix_ms)
}

#[cfg(not(all(feature = "contract-test-fixtures", debug_assertions)))]
fn quota_transport_evidence(_deadline_unix_ms: Option<u64>) -> Value {
    quota_observer::setup_transport_evidence()
}

pub fn install_plan_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let target = string_param(&params, "target").unwrap_or("local");
    let required_settings_ids = required_settings_ids(&params, request_id)?;
    let settings_store = settings::transition_readiness(host, request_id, &required_settings_ids);
    let launch_custody = launch_custody_readiness(host, &params);
    Ok(install_plan_result(target, &settings_store, launch_custody))
}

pub fn sync_plan_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let required_settings_ids = required_settings_ids(&params, request_id)?;
    let desired = desired_profiles(&params);
    let mut operations = sync_operations(&desired);
    if let Some(rebind_operations) =
        crate::native_identity_rebind::operations(host, &params, request_id)?
    {
        operations.extend(rebind_operations);
    }
    let settings_store = settings::transition_readiness(host, request_id, &required_settings_ids);
    let launch_custody = launch_custody_readiness(host, &params);
    let diagnostics = sync_diagnostics(&params, &settings_store, &launch_custody);
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
    executable_evidence_at(program, path, deadline_unix_ms)
}

fn executable_evidence_at(
    program: &str,
    path: Option<PathBuf>,
    deadline_unix_ms: Option<u64>,
) -> Value {
    let timeout = operation_bounds::remaining_timeout(deadline_unix_ms, SETUP_PROBE_TIMEOUT);
    let implementation = match (&path, timeout) {
        (Some(path), Some(_)) => native_implementation_evidence(program, path),
        _ => None,
    };
    let version = match (&path, &implementation, timeout) {
        (Some(path), Some(implementation), Some(timeout)) if implementation["ready"] == true => {
            let resolved_program = path.to_string_lossy().into_owned();
            let mut command = shell::command(&resolved_program);
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
                    "error": format!("{resolved_program} --version exceeded the setup probe deadline"),
                }),
                Err(err) => json!({
                    "present": true,
                    "ready": false,
                    "error": err.to_string(),
                }),
            }
        }
        (Some(_), _, None) => json!({
            "present": true,
            "ready": false,
            "timed_out": true,
            "error": "host deadline expired before the setup probe",
        }),
        (Some(_), _, Some(_)) => json!({
            "present": true,
            "ready": false,
            "error": "executable identity is not approved for this provider build",
        }),
        (None, _, _) => json!({
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
        "implementation": implementation,
    })
}

fn native_implementation_evidence(program: &str, path: &Path) -> Option<Value> {
    let expected_contract = match program {
        "opencode" => native_runtime::OPENCODE_NATIVE_CONTRACT_ID,
        "curl" => quota_observer::QUOTA_OBSERVER_CONTRACT,
        _ => return None,
    };
    let (sha256, byte_length) =
        match durable_fs::sha256_file_bounded(path, durable_fs::MAX_BOUND_EXECUTABLE_BYTES) {
            Ok(identity) => identity,
            Err(error) => {
                return Some(json!({
                    "ready": false,
                    "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
                    "error": error.to_string(),
                }));
            }
        };
    match native_implementation_manifest::approved_implementation(program, &sha256, byte_length) {
        Ok(Some(approved)) if approved.semantic_contract == expected_contract => Some(json!({
            "ready": true,
            "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
            "manifest_id": approved.id,
            "version": approved.version,
            "semantic_contract": approved.semantic_contract,
            "sha256": sha256,
            "byte_length": byte_length,
        })),
        Ok(Some(_)) => Some(json!({
            "ready": false,
            "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
            "sha256": sha256,
            "byte_length": byte_length,
            "error": "implementation manifest entry has the wrong semantic contract",
        })),
        Ok(None) => Some(json!({
            "ready": false,
            "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
            "sha256": sha256,
            "byte_length": byte_length,
            "error": "implementation identity is not in the reviewed manifest",
        })),
        Err(error) => Some(json!({
            "ready": false,
            "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
            "sha256": sha256,
            "byte_length": byte_length,
            "error": error,
        })),
    }
}

fn profile_evidence(
    host: &HostContext,
    data_root: Option<&str>,
    profile_root: Option<&str>,
    opencode: &Value,
    quota_transport: &Value,
    request_id: &str,
) -> Vec<Value> {
    let ambient_native_runtime_ready = evidence_ready(opencode);
    let ambient_quota_observer_ready = evidence_ready(quota_transport);
    let durable_identity_available = host
        .data_root
        .as_deref()
        .is_some_and(|root| !root.trim().is_empty());
    ACCOUNTS
        .iter()
        .map(|account| {
            let (
                native_runtime_ready,
                native_runtime_source,
                native_runtime_identity,
                auth_path,
                native_runtime_error,
            ) = if durable_identity_available {
                match native_runtime::resolve_existing_for_setup(host, account, request_id) {
                    Ok(Some(runtime)) => (
                        true,
                        "persisted",
                        Some(runtime.identity_sha256().to_string()),
                        runtime.expand_path(account.opencode_auth_path),
                        None,
                    ),
                    Ok(None) => (
                        ambient_native_runtime_ready,
                        "ambient_admission",
                        None,
                        expand_tilde(account.opencode_auth_path),
                        None,
                    ),
                    Err(error) => (
                        false,
                        "persisted",
                        None,
                        expand_tilde(account.opencode_auth_path),
                        Some(format!("{}: {}", error.code, error.message)),
                    ),
                }
            } else {
                (
                    ambient_native_runtime_ready,
                    "ambient_admission",
                    None,
                    expand_tilde(account.opencode_auth_path),
                    None,
                )
            };
            let (
                quota_observer_ready,
                quota_observer_source,
                quota_observer_identity,
                quota_observer_error,
            ) = if durable_identity_available {
                match quota_observer::resolve_existing_for_setup(host, account, request_id) {
                    Ok(Some(identity)) => (true, "persisted", Some(identity), None),
                    Ok(None) => (
                        ambient_quota_observer_ready,
                        "ambient_admission",
                        None,
                        None,
                    ),
                    Err(error) => (
                        false,
                        "persisted",
                        None,
                        Some(format!("{}: {}", error.code, error.message)),
                    ),
                }
            } else {
                (
                    ambient_quota_observer_ready,
                    "ambient_admission",
                    None,
                    None,
                )
            };
            let auth_present = auth_file_present(&auth_path);
            json!({
                "profile": account.opencode_wrapper,
                "logical_account": account.opencode_wrapper,
                "logical_account_present": true,
                "native_runtime": "opencode",
                "native_runtime_ready": native_runtime_ready,
                "native_runtime_identity_source": native_runtime_source,
                "native_runtime_identity_sha256": native_runtime_identity,
                "native_runtime_error": native_runtime_error,
                "quota_observer_ready": quota_observer_ready,
                "quota_observer_identity_source": quota_observer_source,
                "quota_observer_identity_sha256": quota_observer_identity,
                "quota_observer_error": quota_observer_error,
                "opencode_auth_path": account.opencode_auth_path,
                "effective_opencode_auth_path": auth_path.display().to_string(),
                "opencode_auth_present": auth_present,
                "profile_ready": native_runtime_ready && quota_observer_ready && auth_present,
                "data_root": data_root,
                "profile_root": profile_root,
                "quota_probe": account.quota_probe_kind(),
            })
        })
        .collect()
}

fn auth_summary(profiles: &[Value]) -> String {
    let present = profiles
        .iter()
        .map(|profile| {
            let state = if profile["opencode_auth_present"] == true {
                "present"
            } else {
                "missing"
            };
            format!(
                "{}:{}:{}",
                profile["profile"].as_str().unwrap_or("unknown"),
                state,
                profile["effective_opencode_auth_path"]
                    .as_str()
                    .unwrap_or("unknown")
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
    launch_custody: &Value,
) -> Vec<Value> {
    let mut warnings = Vec::new();
    if !evidence_ready(opencode) {
        warnings.push(json!(
            "opencode executable probe did not complete successfully"
        ));
    }
    if !evidence_ready(curl) {
        warnings.push(json!(
            "quota transport readiness probe did not complete successfully"
        ));
    }
    for profile in profiles {
        if profile.get("profile_ready").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let name = profile
            .get("profile")
            .and_then(Value::as_str)
            .unwrap_or("unknown profile");
        let native_runtime_ready = profile
            .get("native_runtime_ready")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let quota_observer_ready = profile
            .get("quota_observer_ready")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let auth_present = profile
            .get("opencode_auth_present")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        warnings.push(json!(format!(
            "{name} is not ready: native_runtime_ready={native_runtime_ready}, quota_observer_ready={quota_observer_ready}, opencode_auth_present={auth_present}"
        )));
    }
    if let Some(message) = settings_store.blocking_message() {
        warnings.push(json!(message));
    }
    if launch_custody["ready"] != true {
        warnings.push(json!({
            "code": "launch_custody_migration_required",
            "message": launch_custody["blocking_error"],
            "launch_custody": launch_custody,
        }));
    }
    warnings
}

fn sync_diagnostics(
    params: &Value,
    settings_store: &SettingsTransitionReadiness,
    launch_custody: &Value,
) -> Vec<Value> {
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
    if launch_custody["ready"] != true {
        diagnostics.push(json!({
            "severity": "error",
            "path": "host.data_root",
            "message": launch_custody["blocking_error"],
            "code": "launch_custody_migration_required",
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

fn profile_names() -> Vec<&'static str> {
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

fn launch_custody_readiness(host: &HostContext, params: &Value) -> Value {
    match string_param(params, "data_root").or(host.data_root.as_deref()) {
        Some(data_root) => launch::custody_transition_preflight(Path::new(data_root)),
        None => json!({
            "ready": false,
            "format": "unknown",
            "blocking_error": "setup cannot preflight launch custody because host.data_root is missing; provide the exact provider data root before install or sync",
            "runtime_active_population_limit": null,
        }),
    }
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
    profiles: &[Value],
    settings_store: &SettingsTransitionReadiness,
    launch_custody: &Value,
) -> bool {
    settings_store.ready
        && launch_custody["ready"] == true
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
        && evidence
            .get("implementation")
            .filter(|implementation| !implementation.is_null())
            .is_none_or(|implementation| implementation["ready"] == true)
}

fn detect_result(
    opencode: Value,
    curl: Value,
    profiles: Vec<Value>,
    settings_store: SettingsTransitionReadiness,
    launch_custody: Value,
    installed: bool,
) -> Value {
    let warnings = setup_warnings(
        &opencode,
        &curl,
        &profiles,
        &settings_store,
        &launch_custody,
    );
    json!({
        "installed": installed,
        "binary": {
            "opencode": opencode,
            "curl": curl,
        },
        "auth": auth_summary(&profiles),
        "profiles": profiles,
        "warnings": warnings,
    })
}

fn install_plan_result(
    target: &str,
    settings_store: &SettingsTransitionReadiness,
    launch_custody: Value,
) -> Value {
    let quota_transport = quota_transport_install_step(target);
    json!({
        "steps": [
            {
                "kind": "verify_settings_transition",
                "target": target,
                "blocking": !settings_store.ready,
                "settings_store": settings_store.evidence(),
            },
            {
                "kind": "verify_launch_custody_transition",
                "target": target,
                "blocking": launch_custody["ready"] != true,
                "launch_custody": launch_custody,
            },
            {
                "kind": "verify_reviewed_native_implementation",
                "target": target,
                "component": "opencode",
                "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
                "semantic_contract": native_runtime::OPENCODE_NATIVE_CONTRACT_ID,
                "post_admission_probe": "--version"
            },
            quota_transport,
            {
                "kind": "verify_logical_profiles",
                "target": target,
                "profiles": profile_names(),
                "native_runtime": "opencode",
                "auth_requirement": "per_profile"
            },
            {
                "kind": "prepare_provider_settings",
                "schema_id": "opencode.settings/v1",
                "activation_operation": "settings.migrate",
                "activation_identity": "legacy_provider_table_key_to_exact_settings_record_id"
            }
        ]
    })
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn quota_transport_install_step(target: &str) -> Value {
    json!({"kind": "verify_tool", "target": target, "command": "curl --version"})
}

#[cfg(not(all(feature = "contract-test-fixtures", debug_assertions)))]
fn quota_transport_install_step(target: &str) -> Value {
    json!({
        "kind": "verify_in_process_transport",
        "target": target,
        "contract": quota_observer::QUOTA_OBSERVER_CONTRACT,
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

fn auth_file_present(path: &Path) -> bool {
    path.is_file()
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

#[cfg(all(test, unix, not(feature = "contract-test-fixtures")))]
mod tests {
    use super::executable_evidence_at;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn setup_does_not_execute_an_unapproved_direct_implementation() {
        let directory = tempfile::tempdir().expect("create setup probe fixture");
        let sentinel = directory.path().join("unapproved-executed");
        let program = directory.path().join("opencode");
        fs::write(
            &program,
            format!(
                "#!/bin/sh\nprintf acted > '{}'\n",
                sentinel.to_string_lossy()
            ),
        )
        .expect("write unapproved executable");
        let mut permissions = fs::metadata(&program)
            .expect("unapproved executable metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&program, permissions).expect("mark unapproved fixture executable");

        let evidence = executable_evidence_at("opencode", Some(program), None);

        assert_eq!(evidence["version"]["ready"], false);
        assert_eq!(evidence["implementation"]["ready"], false);
        assert!(
            !sentinel.exists(),
            "manifest rejection must precede the setup version spawn"
        );
    }
}

#[cfg(test)]
mod identity_readiness_tests {
    use super::*;
    use std::fs;

    #[test]
    fn persisted_identity_disagreement_blocks_ambient_setup_readiness() {
        let directory = tempfile::tempdir().expect("setup identity fixture");
        let data_root = directory.path().to_string_lossy().into_owned();
        let runtime_root = directory
            .path()
            .join("provider-state/opencode/native-runtimes");
        let observer_root = directory
            .path()
            .join("provider-state/opencode/quota-observers");
        fs::create_dir_all(&runtime_root).expect("runtime identity root");
        fs::create_dir_all(&observer_root).expect("observer identity root");
        fs::write(runtime_root.join("opencode1.json"), br#"{}"#)
            .expect("incompatible persisted runtime");
        fs::write(observer_root.join("opencode1.json"), br#"{}"#)
            .expect("incompatible persisted observer");
        let host = HostContext {
            app: "test".to_string(),
            app_version: None,
            platform: None,
            working_directory: None,
            config_root: None,
            data_root: Some(data_root.clone()),
            env: None,
            deadline_unix_ms: None,
        };
        let ambient_ready = json!({
            "present": true,
            "version": { "ready": true },
            "implementation": { "ready": true },
        });

        let profiles = profile_evidence(
            &host,
            Some(&data_root),
            None,
            &ambient_ready,
            &ambient_ready,
            "setup-identity-test",
        );
        let profile = profiles
            .iter()
            .find(|profile| profile["profile"] == "opencode1")
            .expect("opencode1 profile evidence");

        assert_eq!(profile["native_runtime_identity_source"], "persisted");
        assert_eq!(profile["native_runtime_ready"], false);
        assert!(profile["native_runtime_error"].is_string());
        assert_eq!(profile["quota_observer_identity_source"], "persisted");
        assert_eq!(profile["quota_observer_ready"], false);
        assert!(profile["quota_observer_error"].is_string());
        assert_eq!(profile["profile_ready"], false);
    }

    #[test]
    fn setup_plans_block_an_oversized_launch_custody_transition() {
        let directory = tempfile::tempdir().expect("setup custody fixture");
        let data_root = directory.path().to_string_lossy().into_owned();
        let custody_root = directory
            .path()
            .join("provider-state/opencode/launch/requests/.custody-v2");
        fs::create_dir_all(&custody_root).expect("predecessor custody root");
        let empty_digest = "0".repeat(64);
        let slots = (0..513)
            .map(|_| {
                json!({
                    "occupied": 0,
                    "request_sha256": empty_digest,
                })
            })
            .collect::<Vec<_>>();
        fs::write(
            custody_root.join("schema.json"),
            serde_json::to_vec(&json!({
                "schema_version": 5,
                "active_policy": "elastic",
                "initial_active_slots": 64,
                "replay_slots": 4096,
            }))
            .expect("predecessor schema"),
        )
        .expect("write predecessor schema");
        fs::write(
            custody_root.join("active.json"),
            serde_json::to_vec(&json!({"next_probe": 0, "slots": slots}))
                .expect("predecessor active index"),
        )
        .expect("write predecessor active index");
        let host = HostContext {
            app: "test".to_string(),
            app_version: None,
            platform: None,
            working_directory: None,
            config_root: Some(
                directory
                    .path()
                    .join("config")
                    .to_string_lossy()
                    .into_owned(),
            ),
            data_root: Some(data_root),
            env: None,
            deadline_unix_ms: None,
        };

        let readiness = launch_custody_readiness(&host, &json!({}));
        assert_eq!(readiness["ready"], false);
        assert!(readiness["blocking_error"]
            .as_str()
            .is_some_and(|message| message.contains("do not delete provider sessions")));

        let plan = install_plan_params(&host, json!({"target": "local"}), "setup-custody-test")
            .expect("blocked install plan");
        let custody_step = plan["steps"]
            .as_array()
            .expect("install steps")
            .iter()
            .find(|step| step["kind"] == "verify_launch_custody_transition")
            .expect("launch custody preflight step");
        assert_eq!(custody_step["blocking"], true);

        let sync = sync_plan_params(
            &host,
            json!({
                "desired_profiles": [],
                "settings_schema_id": "opencode.settings/v1",
            }),
            "setup-custody-sync-test",
        )
        .expect("blocked sync plan");
        assert!(sync["diagnostics"]
            .as_array()
            .expect("sync diagnostics")
            .iter()
            .any(|diagnostic| diagnostic["code"] == "launch_custody_migration_required"));
    }
}
