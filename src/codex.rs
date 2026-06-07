//! Declared roles: orchestration, parser, mapper, validator, formatter
//! adapter_declarations:
//!   - component: src/codex.rs
//!     role: adapter
//!     Translates:
//!       - chatgpt-usage rolling-window stdout JSON
//!       - chatgpt-usage auth-path argv boundary

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

#[derive(Debug)]
pub struct ChatgptUsageWindow {
    pub name: Option<String>,
    pub used_percent: f64,
    pub resets_at: String,
}

pub fn parse_chatgpt_usage_windows(raw: &[u8]) -> Result<Vec<ChatgptUsageWindow>, String> {
    let parsed = parse_usage_json(raw)?;
    parse_windows(usage_windows(&parsed)?)
}

pub fn run_chatgpt_usage(auth_path: &Path) -> std::io::Result<ShellOutput> {
    if chatgpt_usage_script_override_enabled() {
        return run_chatgpt_usage_script(auth_path);
    }
    run_chatgpt_usage_native(auth_path)
}

fn run_chatgpt_usage_script(auth_path: &Path) -> std::io::Result<ShellOutput> {
    let argv = chatgpt_usage_argv(auth_path);
    shell::run(&argv)
}

fn run_chatgpt_usage_native(auth_path: &Path) -> std::io::Result<ShellOutput> {
    let tokens = match read_auth_tokens(auth_path) {
        Ok(tokens) => tokens,
        Err(error) => return Ok(failed_output(3, error)),
    };
    let output = run_curl_usage(&tokens)?;
    Ok(project_curl_usage_output(output))
}

fn chatgpt_usage_script_override_enabled() -> bool {
    std::env::var_os(SCRIPT_OVERRIDE_ENV).is_some()
}

fn chatgpt_usage_argv(auth_path: &Path) -> Vec<String> {
    vec!["chatgpt-usage".to_string(), auth_path_arg(auth_path)]
}

fn auth_path_arg(auth_path: &Path) -> String {
    auth_path.to_string_lossy().into_owned()
}

struct AuthTokens {
    access_token: String,
    account_id: String,
}

fn read_auth_tokens(path: &Path) -> Result<AuthTokens, String> {
    let raw = read_auth_file(path).map_err(auth_file_read_error)?;
    let parsed = parse_auth_json(&raw).map_err(auth_file_json_error)?;
    required_auth_tokens(auth_tokens_from_json(&parsed))
}

fn read_auth_file(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    fs::read(path)
}

fn parse_auth_json(raw: &[u8]) -> Result<Value, serde_json::Error> {
    serde_json::from_slice(raw)
}

fn auth_tokens_from_json(parsed: &Value) -> Option<AuthTokens> {
    codex_auth_tokens(parsed).or_else(|| opencode_auth_tokens(parsed))
}

fn required_auth_tokens(tokens: Option<AuthTokens>) -> Result<AuthTokens, String> {
    tokens.ok_or_else(missing_auth_tokens_error)
}

fn auth_file_read_error(err: std::io::Error) -> String {
    format!("failed to read auth file: {err}")
}

fn auth_file_json_error(err: serde_json::Error) -> String {
    format!("auth file must be JSON: {err}")
}

fn missing_auth_tokens_error() -> String {
    "missing ChatGPT access token or account id in auth file".to_string()
}

fn codex_auth_tokens(parsed: &Value) -> Option<AuthTokens> {
    auth_tokens(
        parsed.pointer("/tokens/access_token")?.as_str()?,
        parsed.pointer("/tokens/account_id")?.as_str()?,
    )
}

fn opencode_auth_tokens(parsed: &Value) -> Option<AuthTokens> {
    auth_tokens(
        parsed.pointer("/openai/access")?.as_str()?,
        parsed.pointer("/openai/accountId")?.as_str()?,
    )
}

fn auth_tokens(access_token: &str, account_id: &str) -> Option<AuthTokens> {
    let access_token = nonempty_string(access_token)?;
    let account_id = nonempty_string(account_id)?;
    Some(AuthTokens {
        access_token,
        account_id,
    })
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
    Ok(shell_output_from_process(output))
}

fn shell_output_from_process(output: std::process::Output) -> ShellOutput {
    ShellOutput {
        stdout: output.stdout,
        stderr: output.stderr,
        status: output.status.code().unwrap_or(1),
    }
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

fn project_curl_usage_output(output: ShellOutput) -> ShellOutput {
    if output.status != 0 {
        return output;
    }
    match wham_usage_windows_stdout(&output.stdout) {
        Ok(stdout) => ShellOutput {
            stdout,
            stderr: Vec::new(),
            status: 0,
        },
        Err(error) => failed_output(4, error),
    }
}

fn wham_usage_windows_stdout(raw: &[u8]) -> Result<Vec<u8>, String> {
    let (body, status) = split_http_body_and_status(raw)?;
    if validate_success_http_status(status).is_err() {
        return Err(http_status_failure(status, body));
    }
    let parsed = parse_wham_usage_json(body).map_err(wham_usage_json_error)?;
    wham_usage_windows_bytes(&parsed).map_err(json_bytes_error)
}

fn split_http_body_and_status(raw: &[u8]) -> Result<(&str, &str), String> {
    let text = std::str::from_utf8(raw)
        .map_err(|err| format!("ChatGPT usage response must be UTF-8: {err}"))?;
    let (body, status) = text
        .rsplit_once(HTTP_STATUS_MARKER)
        .ok_or_else(|| "curl output missing HTTP status marker".to_string())?;
    Ok((body.trim_end_matches('\n'), status.trim()))
}

fn format_http_error(status: &str, body: &str) -> String {
    format!(
        "ChatGPT API returned HTTP {status}: {}",
        http_error_detail(body)
    )
}

fn http_error_detail(body: &str) -> String {
    let parsed = parse_http_error_body(body);
    let detail = parsed.as_ref().and_then(http_error_detail_value);
    if let Some(detail) = nonempty_http_error_detail(detail) {
        return owned_http_error_detail(detail);
    }
    trimmed_http_error_body(body)
}

fn validate_success_http_status(status: &str) -> Result<(), ()> {
    successful_http_status(status).then_some(()).ok_or(())
}

fn successful_http_status(status: &str) -> bool {
    status.starts_with('2')
}

fn http_status_failure(status: &str, body: &str) -> String {
    format_http_error(status, body)
}

fn parse_wham_usage_json(body: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str(body)
}

fn wham_usage_json_error(err: serde_json::Error) -> String {
    format!("ChatGPT usage response must be JSON: {err}")
}

fn wham_usage_windows_bytes(parsed: &Value) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&usage_windows_result(parsed))
}

fn json_bytes_error(err: serde_json::Error) -> String {
    err.to_string()
}

fn parse_http_error_body(body: &str) -> Option<Value> {
    serde_json::from_str::<Value>(body).ok()
}

fn http_error_detail_value(parsed: &Value) -> Option<&str> {
    parsed
        .pointer("/detail")
        .or_else(|| parsed.pointer("/error/message"))
        .and_then(Value::as_str)
}

fn nonempty_http_error_detail(detail: Option<&str>) -> Option<&str> {
    detail.filter(|detail| !detail.trim().is_empty())
}

fn owned_http_error_detail(detail: &str) -> String {
    detail.to_string()
}

fn trimmed_http_error_body(body: &str) -> String {
    body.trim().to_string()
}

fn usage_windows_result(parsed: &Value) -> Value {
    let windows = ["secondary_window", "primary_window"]
        .into_iter()
        .filter_map(|name| usage_window(parsed, name))
        .collect::<Vec<_>>();
    serde_json::json!({ "windows": windows })
}

fn usage_window(parsed: &Value, name: &str) -> Option<Value> {
    let window = parsed.pointer(&format!("/rate_limit/{name}"))?;
    let reset_at = window.get("reset_at")?.as_i64()?;
    Some(serde_json::json!({
        "used_percent": window.get("used_percent").and_then(Value::as_f64).unwrap_or(0.0),
        "resets_at": unix_seconds_to_rfc3339(reset_at)?,
    }))
}

fn unix_seconds_to_rfc3339(seconds: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(seconds, 0)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn failed_output(status: i32, message: String) -> ShellOutput {
    ShellOutput {
        stdout: Vec::new(),
        stderr: message.into_bytes(),
        status,
    }
}

fn parse_window(index: usize, window: &Value) -> Result<ChatgptUsageWindow, String> {
    let object = window_object(index, window)?;
    let used_percent = window_used_percent(index, object)?;
    validate_used_percent(index, used_percent)?;
    let resets_at = window_resets_at(index, object)?;
    validate_resets_at(index, resets_at)?;
    Ok(chatgpt_usage_window(object, used_percent, resets_at))
}

fn parse_usage_json(raw: &[u8]) -> Result<Value, String> {
    serde_json::from_slice(raw).map_err(invalid_usage_json_error)
}

fn parse_windows(windows: &[Value]) -> Result<Vec<ChatgptUsageWindow>, String> {
    windows
        .iter()
        .enumerate()
        .map(|(index, window)| parse_window(index, window))
        .collect()
}

fn usage_windows(parsed: &Value) -> Result<&[Value], String> {
    parsed
        .get("windows")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(missing_windows_error)
}

fn window_object(index: usize, window: &Value) -> Result<&serde_json::Map<String, Value>, String> {
    window.as_object().ok_or_else(|| window_object_error(index))
}

fn window_used_percent(
    index: usize,
    object: &serde_json::Map<String, Value>,
) -> Result<f64, String> {
    object
        .get("used_percent")
        .and_then(Value::as_f64)
        .ok_or_else(|| used_percent_error(index))
}

fn window_resets_at(index: usize, object: &serde_json::Map<String, Value>) -> Result<&str, String> {
    object
        .get("resets_at")
        .and_then(Value::as_str)
        .ok_or_else(|| resets_at_error(index))
}

fn chatgpt_usage_window(
    object: &serde_json::Map<String, Value>,
    used_percent: f64,
    resets_at: &str,
) -> ChatgptUsageWindow {
    ChatgptUsageWindow {
        name: object
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string),
        used_percent,
        resets_at: resets_at.to_string(),
    }
}

fn validate_used_percent(index: usize, used_percent: f64) -> Result<(), String> {
    if (0.0..=100.0).contains(&used_percent) {
        return Ok(());
    }
    Err(used_percent_range_error(index, used_percent))
}

fn validate_resets_at(index: usize, resets_at: &str) -> Result<(), String> {
    DateTime::parse_from_rfc3339(resets_at)
        .map(|_| ())
        .map_err(|err| resets_at_parse_error(index, err))
}

fn invalid_usage_json_error(err: serde_json::Error) -> String {
    format!("chatgpt-usage stdout must be JSON: {err}")
}

fn missing_windows_error() -> String {
    "chatgpt-usage windows must be an array".to_string()
}

fn window_object_error(index: usize) -> String {
    format!("windows[{index}] must be an object")
}

fn used_percent_error(index: usize) -> String {
    format!("windows[{index}].used_percent must be numeric")
}

fn used_percent_range_error(index: usize, used_percent: f64) -> String {
    format!("windows[{index}].used_percent out of range: {used_percent}")
}

fn resets_at_error(index: usize) -> String {
    format!("windows[{index}].resets_at must be a string")
}

fn resets_at_parse_error(index: usize, err: chrono::ParseError) -> String {
    format!("windows[{index}].resets_at invalid RFC3339: {err}")
}
