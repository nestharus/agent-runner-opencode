//! Declared roles: orchestration, mapper

use crate::account::{profile_for_settings_id, AccountProfile};
use crate::codex::{self, ChatgptUsageWindow};
use crate::encoding::{bounded_text, now_unix_ms};
use crate::envelope::ProviderFailure;
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
    let available = probe_account(account, request_id)
        .ok()
        .and_then(|result| result.get("available").and_then(Value::as_bool))
        .unwrap_or(false);
    Ok(json!({
        "refreshed": false,
        "available": available,
        "checked_at_unix_ms": checked_at_unix_ms,
        "detail": REFRESH_DETAIL,
    }))
}

fn source_result(account: &AccountProfile, auth_path: &Path) -> Value {
    let mut result = serde_json::Map::new();
    let has_source = auth_is_readable(auth_path);
    result.insert("has_source".to_string(), json!(has_source));
    result.insert(
        "freshness".to_string(),
        json!(if has_source {
            "auth_readable"
        } else {
            "auth_missing_or_unreadable"
        }),
    );
    if has_source {
        result.insert(
            "source_id".to_string(),
            json!(source_id(account, auth_path)),
        );
    }
    Value::Object(result)
}

fn probe_account(account: &AccountProfile, request_id: &str) -> Result<Value, ProviderFailure> {
    let auth_path = resolved_auth_path(account);
    if !auth_is_readable(&auth_path) {
        return Ok(unavailable_result(
            "paired codex auth source is missing or unreadable".to_string(),
        ));
    }
    let output = codex::run_chatgpt_usage(&auth_path).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "quota_probe_spawn_failed",
            format!("failed to run chatgpt-usage: {err}"),
        )
    })?;
    if output.status != 0 {
        return Ok(unavailable_result(command_failure_detail(&output)));
    }
    let windows = match codex::parse_chatgpt_usage_windows(&output.stdout) {
        Ok(windows) => windows,
        Err(err) => {
            return Ok(unavailable_result(format!(
                "chatgpt-usage output is invalid: {err}"
            )));
        }
    };
    Ok(json!({
        "available": true,
        "checked_at_unix_ms": now_unix_ms(),
        "windows": quota_windows(&windows),
    }))
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
    serde_json::from_value(params).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_quota_params",
            format!("quota params are invalid: {err}"),
        )
    })
}

fn parse_refresh_params(
    params: Value,
    request_id: &str,
) -> Result<QuotaRefreshAuthParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_quota_refresh_auth_params",
            format!("quota.refresh_auth params are invalid: {err}"),
        )
    })
}

fn account_for_settings_id(
    settings_id: &str,
    request_id: &str,
) -> Result<&'static AccountProfile, ProviderFailure> {
    profile_for_settings_id(settings_id).ok_or_else(|| {
        ProviderFailure::invalid_request(
            request_id,
            "unknown_settings_id",
            format!("unknown opencode settings_id: {settings_id}"),
        )
    })
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

fn epoch_ms(rfc3339: &str) -> i64 {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("chatgpt-usage resets_at was validated before projection")
        .timestamp_millis()
}
