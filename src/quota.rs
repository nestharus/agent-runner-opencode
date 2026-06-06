//! Declared roles: orchestration, mapper, parser, validator, accessor, predicate, formatter
//! adapter_declarations:
//!   - component: src/quota.rs
//!     role: adapter
//!     Translates:
//!       - codex auth source profile to QuotaSourceResult
//!       - chatgpt-usage rolling windows to QuotaProbeWindow
//!       - codex CLI-owned auth refresh boundary to QuotaRefreshAuthResult

use crate::account::{profile_for_settings_id, AccountProfile};
use crate::codex::{self, ChatgptUsageWindow};
use crate::encoding::{bounded_text, now_unix_ms};
use crate::envelope::{ProviderFailure, RequestEnvelope};
use chrono::DateTime;
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

const REFRESH_DETAIL: &str = "codex-cli-owned refresh; agent-runner never mutates tokens";

#[derive(Deserialize)]
struct QuotaBaseParams {
    settings_id: String,
}

#[derive(Deserialize)]
struct QuotaRefreshAuthParams {
    settings_id: String,
}

pub fn handle(subcommand: &str, request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let RequestEnvelope {
        params, request_id, ..
    } = request;
    match subcommand {
        "quota.source" => source_params(params, &request_id),
        "quota.probe" => probe_params(params, &request_id),
        "quota.refresh_auth" => refresh_auth_params(params, &request_id),
        unknown => Err(unknown_quota_subcommand_failure(request_id, unknown)),
    }
}

pub fn source_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_base_params(params, request_id)?;
    let account = account_for_settings_id(&params.settings_id, request_id)?;
    let auth_path = resolved_auth_path(account);
    Ok(source_result(account, &auth_path))
}

pub fn probe_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_base_params(params, request_id)?;
    let account = account_for_settings_id(&params.settings_id, request_id)?;
    probe_account(account, request_id)
}

pub fn refresh_auth_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_refresh_params(params, request_id)?;
    let account = account_for_settings_id(&params.settings_id, request_id)?;
    let checked_at_unix_ms = now_unix_ms();
    let available = refresh_available(account, request_id);
    Ok(refresh_auth_result(available, checked_at_unix_ms))
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

fn probe_account(account: &AccountProfile, request_id: &str) -> Result<Value, ProviderFailure> {
    let auth_path = resolved_auth_path(account);
    if !auth_has_source(&auth_path) {
        return Ok(unreadable_auth_probe_result());
    }
    let output = run_probe_command(&auth_path, request_id)?;
    Ok(probe_output_result(&output))
}

fn unreadable_auth_probe_result() -> Value {
    unavailable_result("paired codex auth source is missing or unreadable".to_string())
}

fn run_probe_command(
    auth_path: &Path,
    request_id: &str,
) -> Result<crate::shell::ShellOutput, ProviderFailure> {
    codex::run_chatgpt_usage(auth_path).map_err(|err| quota_probe_spawn_failure(request_id, err))
}

fn probe_output_result(output: &crate::shell::ShellOutput) -> Value {
    if probe_command_failed(output) {
        return unavailable_result(command_failure_detail(output));
    }
    parsed_probe_output_result(&output.stdout)
}

fn probe_command_failed(output: &crate::shell::ShellOutput) -> bool {
    output.status != 0
}

fn parsed_probe_output_result(stdout: &[u8]) -> Value {
    match parse_probe_windows(stdout) {
        Ok(windows) => available_probe_result(&windows),
        Err(err) => unavailable_result(invalid_probe_detail(err)),
    }
}

fn unavailable_result(detail: String) -> Value {
    json!({
        "available": false,
        "checked_at_unix_ms": now_unix_ms(),
        "windows": [],
        "detail": detail,
    })
}

fn quota_windows(windows: &[ChatgptUsageWindow]) -> Vec<Value> {
    windows.iter().map(quota_window).collect()
}

fn quota_window(window: &ChatgptUsageWindow) -> Value {
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

fn account_for_settings_id(
    settings_id: &str,
    request_id: &str,
) -> Result<&'static AccountProfile, ProviderFailure> {
    profile_for_settings_id(settings_id)
        .ok_or_else(|| unknown_settings_id_failure(request_id, settings_id))
}

fn resolved_auth_path(account: &AccountProfile) -> PathBuf {
    expand_tilde(account.codex_auth_path)
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
        "codex-auth:{}:{}:{}",
        account.codex_account_tag,
        account.codex_account_hash,
        auth_path.to_string_lossy()
    )
}

fn command_failure_detail(output: &crate::shell::ShellOutput) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = bounded_text(stderr.trim(), 500);
    if stderr.is_empty() {
        return format!("chatgpt-usage exited with status {}", output.status);
    }
    format!(
        "chatgpt-usage exited with status {}: {stderr}",
        output.status
    )
}

fn refresh_available(account: &AccountProfile, request_id: &str) -> bool {
    probe_account(account, request_id)
        .ok()
        .and_then(|result| result.get("available").and_then(Value::as_bool))
        .unwrap_or(false)
}

fn refresh_auth_result(available: bool, checked_at_unix_ms: u64) -> Value {
    json!({
        "refreshed": false,
        "available": available,
        "checked_at_unix_ms": checked_at_unix_ms,
        "detail": REFRESH_DETAIL,
    })
}

fn source_freshness(has_source: bool) -> &'static str {
    if has_source {
        "auth_readable"
    } else {
        "auth_missing_or_unreadable"
    }
}

fn quota_probe_spawn_failure(request_id: &str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "quota_probe_spawn_failed",
        format!("failed to run chatgpt-usage: {err}"),
    )
}

fn parse_probe_windows(stdout: &[u8]) -> Result<Vec<ChatgptUsageWindow>, String> {
    codex::parse_chatgpt_usage_windows(stdout)
}

fn available_probe_result(windows: &[ChatgptUsageWindow]) -> Value {
    json!({
        "available": true,
        "checked_at_unix_ms": now_unix_ms(),
        "windows": quota_windows(windows),
    })
}

fn invalid_probe_detail(err: String) -> String {
    format!("chatgpt-usage output is invalid: {err}")
}

fn unknown_quota_subcommand_failure(request_id: String, unknown: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "unknown_quota_subcommand",
        format!("unknown quota subcommand: {unknown}"),
    )
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

fn unknown_settings_id_failure(request_id: &str, settings_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "unknown_settings_id",
        format!("unknown opencode settings_id: {settings_id}"),
    )
}

fn epoch_ms(rfc3339: &str) -> i64 {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("chatgpt-usage resets_at was validated before projection")
        .timestamp_millis()
}
