//! Declared roles: orchestration, parser, mapper, validator, formatter
//! adapter_declarations:
//!   - component: src/codex.rs
//!     role: adapter
//!     Translates:
//!       - chatgpt-usage rolling-window stdout JSON
//!       - chatgpt-usage auth-path argv boundary

use crate::shell::{self, ShellOutput};
use chrono::DateTime;
use serde_json::Value;
use std::path::Path;

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
    let argv = chatgpt_usage_argv(auth_path);
    shell::run(&argv)
}

fn chatgpt_usage_argv(auth_path: &Path) -> Vec<String> {
    vec!["chatgpt-usage".to_string(), auth_path_arg(auth_path)]
}

fn auth_path_arg(auth_path: &Path) -> String {
    auth_path.to_string_lossy().into_owned()
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
