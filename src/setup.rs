//! Declared roles: accessor, mapper, orchestration, validator, predicate, filter, formatter, parser

use crate::account::{profile_for_wrapper_reference, ACCOUNTS};
use crate::encoding::bounded_text;
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope};
use crate::shell;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

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
    let opencode = executable_evidence("opencode");
    let curl = executable_evidence("curl");
    let profiles = profile_evidence(data_root, profile_root);
    let installed = setup_installed(&opencode, &curl, &profiles);
    Ok(detect_result(opencode, curl, profiles, installed))
}

pub fn install_plan_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let target = string_param(&params, "target").unwrap_or("local");
    Ok(install_plan_result(target))
}

pub fn sync_plan_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let desired = desired_profiles(&params);
    let operations = sync_operations(&desired);
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

fn executable_evidence(program: &str) -> Value {
    let path = find_on_path(program);
    let version = match shell::run(&[program.to_string(), "--version".to_string()]) {
        Ok(output) => {
            let stdout = sanitized_command_output(&output.stdout, 500);
            let stderr = sanitized_command_output(&output.stderr, 500);
            json!({
                "present": true,
                "status": output.status,
                "ready": output.status == 0,
                "stdout_present": stdout.present,
                "stderr_present": stderr.present,
                "stdout_bytes": stdout.byte_len,
                "stderr_bytes": stderr.byte_len,
                "stdout": stdout.excerpt,
                "stderr": stderr.excerpt,
                "redacted": stdout.redacted || stderr.redacted,
            })
        }
        Err(err) => json!({
            "present": false,
            "error": redacted_excerpt(&err.to_string(), 300),
        }),
    };
    json!({
        "program": program,
        "present": path.is_some(),
        "path": path.map(|path| path.to_string_lossy().into_owned()),
        "version": version,
    })
}

struct SanitizedOutput {
    present: bool,
    byte_len: usize,
    excerpt: String,
    redacted: bool,
}

fn sanitized_command_output(bytes: &[u8], max_len: usize) -> SanitizedOutput {
    let text = String::from_utf8_lossy(bytes);
    let (redacted, changed) = redact_sensitive_text(&text);
    SanitizedOutput {
        present: !bytes.is_empty(),
        byte_len: bytes.len(),
        excerpt: bounded_text(redacted.trim(), max_len),
        redacted: changed,
    }
}

fn redacted_excerpt(text: &str, max_len: usize) -> String {
    let (redacted, _) = redact_sensitive_text(text);
    bounded_text(redacted.trim(), max_len)
}

fn redact_sensitive_text(text: &str) -> (String, bool) {
    let mut changed = false;
    let redacted = text
        .lines()
        .map(|line| {
            if line_contains_secret(line) {
                changed = true;
                "[redacted]".to_string()
            } else {
                line.chars()
                    .map(|ch| {
                        if ch.is_control() && ch != '\t' {
                            ' '
                        } else {
                            ch
                        }
                    })
                    .collect()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    (redacted, changed)
}

fn line_contains_secret(line: &str) -> bool {
    let lowered = line.to_ascii_lowercase();
    secret_keyword_present(&lowered) || token_shaped_fragment_present(line)
}

fn secret_keyword_present(lowered: &str) -> bool {
    [
        "api_key",
        "authorization",
        "bearer",
        "credential",
        "password",
        "private_key",
        "refresh",
        "secret",
        "token",
    ]
    .iter()
    .any(|needle| lowered.contains(needle))
}

fn token_shaped_fragment_present(line: &str) -> bool {
    line.split(|ch: char| !is_token_fragment_char(ch))
        .any(is_token_shaped_fragment)
}

fn is_token_fragment_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '+' | '/' | '=')
}

fn is_token_shaped_fragment(fragment: &str) -> bool {
    fragment.len() >= 32
        || fragment.starts_with("sk-")
        || fragment.starts_with("eyJ")
        || fragment.starts_with("ghp_")
        || fragment.starts_with("gho_")
        || fragment.starts_with("xox")
}

fn profile_evidence(data_root: Option<&str>, profile_root: Option<&str>) -> Vec<Value> {
    ACCOUNTS
        .iter()
        .map(|account| {
            let wrapper_path = find_on_path(account.opencode_wrapper);
            json!({
                "profile": account.opencode_wrapper,
                "wrapper": account.opencode_wrapper,
                "wrapper_present": wrapper_path.is_some(),
                "wrapper_path": wrapper_path.map(|path| path.to_string_lossy().into_owned()),
                "opencode_auth_path": account.opencode_auth_path,
                "opencode_auth_present": opencode_auth_file_present(account.opencode_auth_path),
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

fn setup_warnings(installed: bool) -> Vec<Value> {
    if installed {
        return Vec::new();
    }
    vec![json!(
        "one or more opencode setup prerequisites were not detected"
    )]
}

fn sync_diagnostics(params: &Value) -> Vec<Value> {
    let mut diagnostics = desired_profile_diagnostics(params);
    if params.get("settings_schema_id").and_then(Value::as_str) != Some("opencode.settings/v1") {
        diagnostics.push(settings_schema_mismatch_diagnostic());
    }
    diagnostics
}

fn desired_profile_diagnostics(params: &Value) -> Vec<Value> {
    params
        .get("desired_profiles")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|value| match value.as_str() {
            Some(reference) if profile_for_wrapper_reference(reference).is_none() => {
                Some(unknown_profile_diagnostic(reference))
            }
            None => Some(invalid_profile_type_diagnostic()),
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
    evidence_present(opencode) && evidence_present(curl) && any_wrapper_present(profiles)
}

fn evidence_present(evidence: &Value) -> bool {
    evidence
        .get("present")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn any_wrapper_present(profiles: &[Value]) -> bool {
    profiles
        .iter()
        .any(|profile| profile.get("wrapper_present").and_then(Value::as_bool) == Some(true))
}

fn detect_result(opencode: Value, curl: Value, profiles: Vec<Value>, installed: bool) -> Value {
    json!({
        "installed": installed,
        "binary": {
            "opencode": opencode,
            "curl": curl,
        },
        "auth": auth_summary(),
        "profiles": profiles,
        "warnings": setup_warnings(installed),
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

fn unknown_profile_diagnostic(reference: &str) -> Value {
    json!({
        "severity": "error",
        "path": "desired_profiles",
        "message": format!("unknown OpenCode account wrapper reference: {reference}"),
        "code": "unknown_opencode_profile",
    })
}

fn invalid_profile_type_diagnostic() -> Value {
    json!({
        "severity": "error",
        "path": "desired_profiles",
        "message": "OpenCode account wrapper references must be strings",
        "code": "invalid_opencode_profile",
    })
}
