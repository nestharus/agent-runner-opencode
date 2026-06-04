//! Declared roles: accessor, mapper

use crate::account::ACCOUNTS;
use crate::encoding::bounded_text;
use crate::envelope::{HostContext, ProviderFailure};
use crate::shell;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub fn detect_params(
    host: &HostContext,
    params: Value,
    _request_id: &str,
) -> Result<Value, ProviderFailure> {
    let data_root = string_param(&params, "data_root").or(host.data_root.as_deref());
    let profile_root = string_param(&params, "profile_root");
    let opencode = executable_evidence("opencode");
    let chatgpt_usage = command_output_evidence("chatgpt-usage", &[]);
    let profiles = profile_evidence(data_root, profile_root);
    let installed = opencode
        .get("present")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && chatgpt_usage
            .get("present")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        && profiles
            .iter()
            .any(|profile| profile.get("wrapper_present").and_then(Value::as_bool) == Some(true));
    Ok(json!({
        "installed": installed,
        "binary": {
            "opencode": opencode,
            "chatgpt-usage": chatgpt_usage,
        },
        "auth": auth_summary(),
        "profiles": profiles,
        "warnings": setup_warnings(installed),
    }))
}

pub fn install_plan_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let target = string_param(&params, "target").unwrap_or("local");
    Ok(json!({
        "steps": [
            {"kind": "verify_tool", "target": target, "command": "opencode --version"},
            {"kind": "verify_tool", "target": target, "command": "chatgpt-usage <codex-auth-path>"},
            {"kind": "verify_wrappers", "target": target, "wrappers": wrapper_names()},
            {"kind": "prepare_provider_settings", "schema_id": "opencode.settings/v1"}
        ]
    }))
}

pub fn sync_plan_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let desired = params
        .get("desired_profiles")
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_else(|| {
            ACCOUNTS
                .iter()
                .map(|account| account.opencode_wrapper)
                .collect()
        });
    let operations = desired
        .iter()
        .map(|profile| json!({"kind": "ensure_profile", "profile": profile, "schema_id": "opencode.settings/v1"}))
        .collect::<Vec<_>>();
    let diagnostics = sync_diagnostics(&params);
    Ok(json!({ "operations": operations, "diagnostics": diagnostics }))
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
    let output = command_output_evidence(program, &["--version"]);
    json!({
        "program": program,
        "present": path.is_some(),
        "path": path.map(|path| path.to_string_lossy().into_owned()),
        "version": output,
    })
}

fn command_output_evidence(program: &str, args: &[&str]) -> Value {
    let mut argv = vec![program.to_string()];
    argv.extend(args.iter().map(|arg| (*arg).to_string()));
    match shell::run(&argv) {
        Ok(output) => command_success_evidence(output),
        Err(err) => json!({
            "present": false,
            "error": redacted_excerpt(&err.to_string(), 300),
        }),
    }
}

fn command_success_evidence(output: shell::ShellOutput) -> Value {
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
    let lines = text
        .lines()
        .map(|line| {
            if line_contains_secret(line) {
                changed = true;
                "[redacted]".to_string()
            } else {
                printable_line(line)
            }
        })
        .collect::<Vec<_>>();
    (lines.join("\n"), changed)
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
        .map(|account| {
            let wrapper_path = find_on_path(account.opencode_wrapper);
            json!({
                "profile": account.opencode_wrapper,
                "wrapper": account.opencode_wrapper,
                "wrapper_present": wrapper_path.is_some(),
                "wrapper_path": wrapper_path.map(|path| path.to_string_lossy().into_owned()),
                "codex_auth_path": account.codex_auth_path,
                "codex_auth_present": expand_tilde(account.codex_auth_path).is_file(),
                "data_root": data_root,
                "profile_root": profile_root,
                "quota_probe": "chatgpt-usage",
            })
        })
        .collect()
}

fn auth_summary() -> String {
    let present = ACCOUNTS
        .iter()
        .map(|account| {
            let state = if expand_tilde(account.codex_auth_path).is_file() {
                "present"
            } else {
                "missing"
            };
            format!(
                "{}:{state}:{}",
                account.opencode_wrapper, account.codex_auth_path
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
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
    vec![json!({
        "severity": "warning",
        "path": "settings_schema_id",
        "message": "sync plan expects opencode.settings/v1 settings",
        "code": "settings_schema_mismatch",
    })]
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
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(program);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(relative) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return Path::new(&home).join(relative);
        }
    }
    PathBuf::from(path)
}
