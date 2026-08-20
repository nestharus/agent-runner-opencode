//! Declared roles: adapter, parser, validator, mapper, formatter
//! adapter_declarations:
//!   - component: src/quota_adapter.rs
//!     role: adapter
//!     Translates:
//!       - authenticated ChatGPT WHAM HTTP responses to QuotaObservation
//!       - explicit chatgpt-usage test-override stdout to QuotaObservation
//!       - source-specific transport and protocol failures to QuotaObservationFailure

use crate::shell::{self, ShellOutput};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Stdio;

const CHATGPT_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
const HTTP_STATUS_MARKER: &str = "__oulipoly_http_status__:";
const SCRIPT_OVERRIDE_ENV: &str = "AGENT_RUNNER_OPENCODE_USE_CHATGPT_USAGE_SCRIPT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaObservationSource {
    WhamApi,
    ScriptTestOverride,
}

#[derive(Debug)]
pub struct QuotaWindow {
    pub name: Option<String>,
    pub used_percent: f64,
    pub resets_at: String,
}

#[derive(Debug)]
pub struct QuotaObservation {
    pub source: QuotaObservationSource,
    pub windows: Vec<QuotaWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaFailureSource {
    AuthFile,
    WhamTransport,
    WhamHttp,
    WhamProtocol,
    ScriptOverrideTransport,
    ScriptOverrideExit,
    ScriptOverrideProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaRefreshAdvice {
    DoNotRefresh,
    RefreshAuthentication,
}

#[derive(Debug)]
pub struct QuotaObservationFailure {
    pub source: QuotaFailureSource,
    pub detail: String,
    refresh_advice: QuotaRefreshAdvice,
}

impl QuotaObservationFailure {
    pub fn needs_auth_refresh(&self) -> bool {
        self.refresh_advice == QuotaRefreshAdvice::RefreshAuthentication
    }
}

pub fn observe_quota(auth_path: &Path) -> Result<QuotaObservation, QuotaObservationFailure> {
    if script_override_enabled() {
        observe_script_override(auth_path)
    } else {
        observe_wham_api(auth_path)
    }
}

fn observe_script_override(auth_path: &Path) -> Result<QuotaObservation, QuotaObservationFailure> {
    let output = run_script_override(auth_path).map_err(|err| {
        failure(
            QuotaFailureSource::ScriptOverrideTransport,
            format!("chatgpt-usage test override failed to start: {err}"),
        )
    })?;
    if output.status != 0 {
        return Err(failure_with_refresh_advice(
            QuotaFailureSource::ScriptOverrideExit,
            shell_failure_detail("chatgpt-usage test override", &output),
            script_refresh_advice(&output),
        ));
    }
    let windows = parse_script_override_windows(&output.stdout).map_err(|detail| {
        failure(
            QuotaFailureSource::ScriptOverrideProtocol,
            format!("chatgpt-usage test override output is invalid: {detail}"),
        )
    })?;
    Ok(QuotaObservation {
        source: QuotaObservationSource::ScriptTestOverride,
        windows,
    })
}

fn observe_wham_api(auth_path: &Path) -> Result<QuotaObservation, QuotaObservationFailure> {
    let tokens = read_auth_tokens(auth_path)
        .map_err(|detail| failure(QuotaFailureSource::AuthFile, detail))?;
    let output = run_curl_usage(&tokens).map_err(|err| {
        failure(
            QuotaFailureSource::WhamTransport,
            format!("ChatGPT WHAM request failed to start: {err}"),
        )
    })?;
    if output.status != 0 {
        return Err(failure(
            QuotaFailureSource::WhamTransport,
            shell_failure_detail("ChatGPT WHAM curl transport", &output),
        ));
    }
    let (body, status) = split_http_body_and_status(&output.stdout)
        .map_err(|detail| failure(QuotaFailureSource::WhamProtocol, detail))?;
    let status_code = parse_http_status(status)
        .map_err(|detail| failure(QuotaFailureSource::WhamProtocol, detail))?;
    if !(200..300).contains(&status_code) {
        return Err(http_failure(status_code, body));
    }
    let parsed = parse_wham_usage_json(body).map_err(|err| {
        failure(
            QuotaFailureSource::WhamProtocol,
            format!("ChatGPT WHAM response must be JSON: {err}"),
        )
    })?;
    let windows = parse_wham_windows(&parsed)
        .map_err(|detail| failure(QuotaFailureSource::WhamProtocol, detail))?;
    Ok(QuotaObservation {
        source: QuotaObservationSource::WhamApi,
        windows,
    })
}

fn failure(source: QuotaFailureSource, detail: String) -> QuotaObservationFailure {
    QuotaObservationFailure {
        source,
        detail,
        refresh_advice: QuotaRefreshAdvice::DoNotRefresh,
    }
}

fn failure_with_refresh_advice(
    source: QuotaFailureSource,
    detail: String,
    refresh_advice: QuotaRefreshAdvice,
) -> QuotaObservationFailure {
    QuotaObservationFailure {
        source,
        detail,
        refresh_advice,
    }
}

fn http_failure(status: u16, body: &str) -> QuotaObservationFailure {
    QuotaObservationFailure {
        source: QuotaFailureSource::WhamHttp,
        detail: format!(
            "ChatGPT WHAM API returned HTTP {status}: {}",
            http_error_detail(body)
        ),
        refresh_advice: if status == 401 {
            QuotaRefreshAdvice::RefreshAuthentication
        } else {
            QuotaRefreshAdvice::DoNotRefresh
        },
    }
}

fn script_refresh_advice(output: &ShellOutput) -> QuotaRefreshAdvice {
    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    if stderr.contains("http 401")
        || stderr.contains("token is expired")
        || stderr.contains("authentication token is expired")
    {
        QuotaRefreshAdvice::RefreshAuthentication
    } else {
        QuotaRefreshAdvice::DoNotRefresh
    }
}

fn shell_failure_detail(participant: &str, output: &ShellOutput) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        return format!("{participant} exited with status {}", output.status);
    }
    format!(
        "{participant} exited with status {}: {stderr}",
        output.status
    )
}

fn script_override_enabled() -> bool {
    std::env::var_os(SCRIPT_OVERRIDE_ENV).is_some()
}

fn run_script_override(auth_path: &Path) -> std::io::Result<ShellOutput> {
    shell::run(&[
        "chatgpt-usage".to_string(),
        auth_path.to_string_lossy().into_owned(),
    ])
}

struct AuthTokens {
    access_token: String,
    account_id: String,
}

fn read_auth_tokens(path: &Path) -> Result<AuthTokens, String> {
    let raw = fs::read(path).map_err(|err| format!("failed to read OpenCode auth file: {err}"))?;
    let parsed: Value = serde_json::from_slice(&raw)
        .map_err(|err| format!("OpenCode auth file must be JSON: {err}"))?;
    let access_token = parsed
        .pointer("/openai/access")
        .and_then(Value::as_str)
        .and_then(nonempty_string)
        .ok_or_else(missing_auth_tokens_error)?;
    let account_id = parsed
        .pointer("/openai/accountId")
        .and_then(Value::as_str)
        .and_then(nonempty_string)
        .ok_or_else(missing_auth_tokens_error)?;
    Ok(AuthTokens {
        access_token,
        account_id,
    })
}

fn missing_auth_tokens_error() -> String {
    "OpenCode auth file is missing a ChatGPT access token or account id".to_string()
}

fn nonempty_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn curl_usage_argv() -> Vec<String> {
    vec![
        "curl".to_string(),
        "-sS".to_string(),
        "--max-time".to_string(),
        "20".to_string(),
        "-w".to_string(),
        format!("\n{HTTP_STATUS_MARKER}%{{http_code}}"),
        "-K".to_string(),
        "-".to_string(),
        CHATGPT_USAGE_URL.to_string(),
    ]
}

fn run_curl_usage(tokens: &AuthTokens) -> std::io::Result<ShellOutput> {
    let argv = curl_usage_argv();
    let (program, args) = argv
        .split_first()
        .expect("curl usage argv is constructed with a program");
    let mut child = shell::command(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .as_mut()
        .expect("curl stdin is piped")
        .write_all(curl_usage_config(tokens).as_bytes())?;
    let output = child.wait_with_output()?;
    Ok(ShellOutput {
        stdout: output.stdout,
        stderr: output.stderr,
        status: output.status.code().unwrap_or(1),
    })
}

fn curl_usage_config(tokens: &AuthTokens) -> String {
    format!(
        "header = \"Authorization: Bearer {}\"\nheader = \"ChatGPT-Account-Id: {}\"\n",
        curl_config_escape(&tokens.access_token),
        curl_config_escape(&tokens.account_id)
    )
}

fn curl_config_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn split_http_body_and_status(raw: &[u8]) -> Result<(&str, &str), String> {
    let text = std::str::from_utf8(raw)
        .map_err(|err| format!("ChatGPT WHAM response must be UTF-8: {err}"))?;
    let (body, status) = text
        .rsplit_once(HTTP_STATUS_MARKER)
        .ok_or_else(|| "curl output missing ChatGPT WHAM HTTP status marker".to_string())?;
    Ok((body.trim_end_matches('\n'), status.trim()))
}

fn parse_http_status(status: &str) -> Result<u16, String> {
    status
        .parse::<u16>()
        .map_err(|err| format!("ChatGPT WHAM HTTP status is invalid: {err}"))
}

fn parse_wham_usage_json(body: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str(body)
}

fn parse_wham_windows(parsed: &Value) -> Result<Vec<QuotaWindow>, String> {
    ["secondary_window", "primary_window"]
        .into_iter()
        .filter_map(|name| {
            parsed
                .pointer(&format!("/rate_limit/{name}"))
                .map(|window| parse_wham_window(name, window))
        })
        .collect()
}

fn parse_wham_window(name: &str, window: &Value) -> Result<QuotaWindow, String> {
    let reset_at = window
        .get("reset_at")
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("ChatGPT WHAM {name}.reset_at must be Unix seconds"))?;
    let used_percent = window
        .get("used_percent")
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("ChatGPT WHAM {name}.used_percent must be numeric"))?;
    validate_used_percent(name, used_percent)?;
    let resets_at = DateTime::<Utc>::from_timestamp(reset_at, 0)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Secs, true))
        .ok_or_else(|| format!("ChatGPT WHAM {name}.reset_at is out of range"))?;
    Ok(QuotaWindow {
        name: None,
        used_percent,
        resets_at,
    })
}

fn http_error_detail(body: &str) -> String {
    let parsed = serde_json::from_str::<Value>(body).ok();
    let detail = parsed.as_ref().and_then(|value| {
        value
            .pointer("/detail")
            .or_else(|| value.pointer("/error/message"))
            .and_then(Value::as_str)
    });
    detail
        .filter(|detail| !detail.trim().is_empty())
        .unwrap_or_else(|| body.trim())
        .to_string()
}

fn parse_script_override_windows(raw: &[u8]) -> Result<Vec<QuotaWindow>, String> {
    let parsed: Value =
        serde_json::from_slice(raw).map_err(|err| format!("stdout must be JSON: {err}"))?;
    let windows = parsed
        .get("windows")
        .and_then(Value::as_array)
        .ok_or_else(|| "windows must be an array".to_string())?;
    windows
        .iter()
        .enumerate()
        .map(|(index, window)| parse_script_override_window(index, window))
        .collect()
}

fn parse_script_override_window(index: usize, window: &Value) -> Result<QuotaWindow, String> {
    let object = window
        .as_object()
        .ok_or_else(|| format!("windows[{index}] must be an object"))?;
    let used_percent = object
        .get("used_percent")
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("windows[{index}].used_percent must be numeric"))?;
    validate_used_percent(&format!("windows[{index}]"), used_percent)?;
    let resets_at = object
        .get("resets_at")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("windows[{index}].resets_at must be a string"))?;
    DateTime::parse_from_rfc3339(resets_at)
        .map_err(|err| format!("windows[{index}].resets_at invalid RFC3339: {err}"))?;
    Ok(QuotaWindow {
        name: object
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string),
        used_percent,
        resets_at: resets_at.to_string(),
    })
}

fn validate_used_percent(label: &str, used_percent: f64) -> Result<(), String> {
    if (0.0..=100.0).contains(&used_percent) {
        return Ok(());
    }
    Err(format!("{label}.used_percent out of range: {used_percent}"))
}
