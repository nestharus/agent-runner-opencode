//! Declared roles: accessor, mapper, orchestration, validator, predicate, filter, formatter, parser

use crate::account::ACCOUNTS;
use crate::encoding::bounded_text;
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope};
use crate::shell;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub fn handle(subcommand: &str, request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let RequestEnvelope {
        host,
        params,
        request_id,
        ..
    } = request;
    match subcommand {
        "setup.detect" => detect_params(&host, params, &request_id),
        "setup.install_plan" => install_plan_params(params, &request_id),
        "setup.sync_plan" => sync_plan_params(params, &request_id),
        "setup_brain.turn" => Err(brain_unsupported(request_id)),
        unknown => Err(unknown_setup_subcommand_failure(request_id, unknown)),
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
    let chatgpt_usage = executable_evidence("chatgpt-usage");
    let profiles = profile_evidence(data_root, profile_root);
    let installed = setup_installed(&opencode, &chatgpt_usage, &profiles);
    Ok(detect_result(opencode, chatgpt_usage, profiles, installed))
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
    executable_evidence_json(program, executable_probe(program))
}

struct ExecutableProbe {
    path: Option<PathBuf>,
    version: Value,
}

fn executable_probe(program: &str) -> ExecutableProbe {
    ExecutableProbe {
        path: find_on_path(program),
        version: command_output_evidence(program, &["--version"]),
    }
}

fn executable_evidence_json(program: &str, probe: ExecutableProbe) -> Value {
    json!({
        "program": program,
        "present": probe.path.is_some(),
        "path": probe.path.map(|path| path.to_string_lossy().into_owned()),
        "version": probe.version,
    })
}

fn command_output_evidence(program: &str, args: &[&str]) -> Value {
    let argv = command_argv(program, args);
    match shell::run(&argv) {
        Ok(output) => command_success_evidence(output),
        Err(err) => command_error_evidence(err),
    }
}

fn command_success_evidence(output: shell::ShellOutput) -> Value {
    let stdout = sanitized_command_output(&output.stdout, 500);
    let stderr = sanitized_command_output(&output.stderr, 500);
    command_success_json(output.status, stdout, stderr)
}

struct SanitizedOutput {
    present: bool,
    byte_len: usize,
    excerpt: String,
    redacted: bool,
}

fn sanitized_command_output(bytes: &[u8], max_len: usize) -> SanitizedOutput {
    let text = decoded_output_text(bytes);
    let (redacted, changed) = redact_sensitive_text(&text);
    sanitized_output(bytes, redacted.trim(), changed, max_len)
}

fn redacted_excerpt(text: &str, max_len: usize) -> String {
    let (redacted, _) = redact_sensitive_text(text);
    bounded_text(redacted.trim(), max_len)
}

fn redact_sensitive_text(text: &str) -> (String, bool) {
    let lines = redacted_lines(text);
    (redacted_text(&lines), any_redacted_line(&lines))
}

struct RedactedLine {
    text: String,
    changed: bool,
}

fn redacted_lines(text: &str) -> Vec<RedactedLine> {
    text.lines().map(redacted_line).collect()
}

fn redacted_line(line: &str) -> RedactedLine {
    let changed = line_contains_secret(line);
    RedactedLine {
        text: redacted_line_text(line, changed),
        changed,
    }
}

fn redacted_line_text(line: &str, changed: bool) -> String {
    if changed {
        redacted_placeholder()
    } else {
        printable_line(line)
    }
}

fn redacted_placeholder() -> String {
    "[redacted]".to_string()
}

fn redacted_text(lines: &[RedactedLine]) -> String {
    lines
        .iter()
        .map(|line| line.text.as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn any_redacted_line(lines: &[RedactedLine]) -> bool {
    lines.iter().any(|line| line.changed)
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

fn printable_line(line: &str) -> String {
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

fn profile_evidence(data_root: Option<&str>, profile_root: Option<&str>) -> Vec<Value> {
    ACCOUNTS
        .iter()
        .map(|account| profile_json(account, data_root, profile_root))
        .collect()
}

fn auth_summary() -> String {
    let present = auth_entries().join(", ");
    format!("codex auth metadata only; {present}; quota command chatgpt-usage")
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
    if params.get("settings_schema_id").and_then(Value::as_str) == Some("opencode.settings/v1") {
        return Vec::new();
    }
    vec![settings_schema_mismatch_diagnostic()]
}

fn wrapper_names() -> Vec<&'static str> {
    ACCOUNTS
        .iter()
        .map(|account| account.opencode_wrapper)
        .collect()
}

fn string_param<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    non_empty_param_string(param_string(raw_param(params, key)))
}

fn raw_param<'a>(params: &'a Value, key: &str) -> Option<&'a Value> {
    params.get(key)
}

fn param_string(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str)
}

fn non_empty_param_string(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.trim().is_empty())
}

fn find_on_path(program: &str) -> Option<PathBuf> {
    let path = path_env()?;
    first_existing_path_candidate(path_candidates(path_entries(&path), program))
}

fn path_env() -> Option<std::ffi::OsString> {
    std::env::var_os("PATH")
}

fn path_entries(path: &std::ffi::OsStr) -> Vec<PathBuf> {
    std::env::split_paths(path).collect()
}

fn path_candidates(entries: Vec<PathBuf>, program: &str) -> Vec<PathBuf> {
    entries
        .into_iter()
        .map(|dir| path_candidate(&dir, program))
        .collect()
}

fn path_candidate(dir: &Path, program: &str) -> PathBuf {
    dir.join(program)
}

fn first_existing_path_candidate(candidates: Vec<PathBuf>) -> Option<PathBuf> {
    candidates
        .into_iter()
        .find(|candidate| path_candidate_is_file(candidate))
}

fn path_candidate_is_file(candidate: &Path) -> bool {
    candidate.is_file()
}

fn expand_tilde(path: &str) -> PathBuf {
    let Some(relative) = tilde_relative(path) else {
        return literal_path(path);
    };
    let Some(home) = home_dir() else {
        return literal_path(path);
    };
    home_relative_path(&home, relative)
}

fn tilde_relative(path: &str) -> Option<&str> {
    path.strip_prefix("~/")
}

fn home_dir() -> Option<std::ffi::OsString> {
    std::env::var_os("HOME")
}

fn home_relative_path(home: &std::ffi::OsStr, relative: &str) -> PathBuf {
    Path::new(home).join(relative)
}

fn literal_path(path: &str) -> PathBuf {
    PathBuf::from(path)
}

fn unknown_setup_subcommand_failure(request_id: String, unknown: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "unknown_setup_subcommand",
        format!("unknown setup subcommand: {unknown}"),
    )
}

fn setup_installed(opencode: &Value, chatgpt_usage: &Value, profiles: &[Value]) -> bool {
    evidence_present(opencode) && evidence_present(chatgpt_usage) && any_wrapper_present(profiles)
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

fn detect_result(
    opencode: Value,
    chatgpt_usage: Value,
    profiles: Vec<Value>,
    installed: bool,
) -> Value {
    json!({
        "installed": installed,
        "binary": {
            "opencode": opencode,
            "chatgpt-usage": chatgpt_usage,
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
            {"kind": "verify_tool", "target": target, "command": "chatgpt-usage <codex-auth-path>"},
            {"kind": "verify_wrappers", "target": target, "wrappers": wrapper_names()},
            {"kind": "prepare_provider_settings", "schema_id": "opencode.settings/v1"}
        ]
    })
}

fn sync_plan_result(operations: Vec<Value>, diagnostics: Vec<Value>) -> Value {
    json!({ "operations": operations, "diagnostics": diagnostics })
}

fn desired_profiles(params: &Value) -> Vec<String> {
    desired_profile_values(params)
        .map(desired_profile_strings)
        .unwrap_or_else(default_profiles)
}

fn desired_profile_values(params: &Value) -> Option<&[Value]> {
    params
        .get("desired_profiles")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

fn desired_profile_strings(values: &[Value]) -> Vec<String> {
    owned_profile_strings(desired_profile_string_entries(values))
}

fn desired_profile_string_entries(values: &[Value]) -> Vec<&str> {
    values
        .iter()
        .filter_map(desired_profile_string_entry)
        .collect()
}

fn desired_profile_string_entry(value: &Value) -> Option<&str> {
    value.as_str()
}

fn owned_profile_strings(entries: Vec<&str>) -> Vec<String> {
    entries.into_iter().map(str::to_string).collect()
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
        .map(|profile| sync_operation(profile.as_str()))
        .collect()
}

fn sync_operation(profile: &str) -> Value {
    json!({"kind": "ensure_profile", "profile": profile, "schema_id": "opencode.settings/v1"})
}

fn command_argv(program: &str, args: &[&str]) -> Vec<String> {
    let mut argv = vec![program.to_string()];
    argv.extend(args.iter().map(|arg| (*arg).to_string()));
    argv
}

fn command_error_evidence(err: std::io::Error) -> Value {
    json!({
        "present": false,
        "error": redacted_excerpt(&err.to_string(), 300),
    })
}

fn command_success_json(status: i32, stdout: SanitizedOutput, stderr: SanitizedOutput) -> Value {
    json!({
        "present": true,
        "status": status,
        "ready": status == 0,
        "stdout_present": stdout.present,
        "stderr_present": stderr.present,
        "stdout_bytes": stdout.byte_len,
        "stderr_bytes": stderr.byte_len,
        "stdout": stdout.excerpt,
        "stderr": stderr.excerpt,
        "redacted": stdout.redacted || stderr.redacted,
    })
}

fn decoded_output_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn sanitized_output(
    bytes: &[u8],
    redacted: &str,
    changed: bool,
    max_len: usize,
) -> SanitizedOutput {
    SanitizedOutput {
        present: !bytes.is_empty(),
        byte_len: bytes.len(),
        excerpt: bounded_text(redacted, max_len),
        redacted: changed,
    }
}

fn profile_json(
    account: &crate::account::AccountProfile,
    data_root: Option<&str>,
    profile_root: Option<&str>,
) -> Value {
    profile_evidence_json(account, data_root, profile_root, profile_probe(account))
}

struct ProfileProbe {
    wrapper_path: Option<PathBuf>,
    codex_auth_present: bool,
}

fn profile_probe(account: &crate::account::AccountProfile) -> ProfileProbe {
    profile_probe_from_parts(
        find_on_path(account.opencode_wrapper),
        codex_auth_file_present(account.codex_auth_path),
    )
}

fn profile_probe_from_parts(
    wrapper_path: Option<PathBuf>,
    codex_auth_present: bool,
) -> ProfileProbe {
    ProfileProbe {
        wrapper_path,
        codex_auth_present,
    }
}

fn codex_auth_file_present(path: &str) -> bool {
    path_is_file(&expanded_auth_path(path))
}

fn expanded_auth_path(path: &str) -> PathBuf {
    expand_tilde(path)
}

fn path_is_file(path: &Path) -> bool {
    path.is_file()
}

fn profile_evidence_json(
    account: &crate::account::AccountProfile,
    data_root: Option<&str>,
    profile_root: Option<&str>,
    probe: ProfileProbe,
) -> Value {
    json!({
        "profile": account.opencode_wrapper,
        "wrapper": account.opencode_wrapper,
        "wrapper_present": probe.wrapper_path.is_some(),
        "wrapper_path": probe.wrapper_path.map(|path| path.to_string_lossy().into_owned()),
        "codex_auth_path": account.codex_auth_path,
        "codex_auth_present": probe.codex_auth_present,
        "data_root": data_root,
        "profile_root": profile_root,
        "quota_probe": "chatgpt-usage",
    })
}

fn auth_entries() -> Vec<String> {
    ACCOUNTS.iter().map(auth_entry).collect()
}

fn auth_entry(account: &crate::account::AccountProfile) -> String {
    format!(
        "{}:{}:{}",
        account.opencode_wrapper,
        auth_state(account.codex_auth_path),
        account.codex_auth_path
    )
}

fn auth_state(path: &str) -> &'static str {
    auth_state_label(codex_auth_file_present(path))
}

fn auth_state_label(present: bool) -> &'static str {
    if present {
        "present"
    } else {
        "missing"
    }
}

fn settings_schema_mismatch_diagnostic() -> Value {
    json!({
        "severity": "warning",
        "path": "settings_schema_id",
        "message": "sync plan expects opencode.settings/v1 settings",
        "code": "settings_schema_mismatch",
    })
}
