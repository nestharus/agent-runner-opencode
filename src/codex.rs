//! Declared roles: adapter, parser
//! adapter_declarations:
//!   - component: src/codex.rs
//!     contract: chatgpt-usage windows stdout JSON
//!   - component: src/codex.rs
//!     contract: chatgpt-usage argv auth-path boundary

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
    let parsed: Value = serde_json::from_slice(raw)
        .map_err(|err| format!("chatgpt-usage stdout must be JSON: {err}"))?;
    let windows = parsed
        .get("windows")
        .and_then(Value::as_array)
        .ok_or_else(|| "chatgpt-usage windows must be an array".to_string())?;
    windows
        .iter()
        .enumerate()
        .map(|(index, window)| parse_window(index, window))
        .collect()
}

pub fn run_chatgpt_usage(auth_path: &Path) -> std::io::Result<ShellOutput> {
    let argv = vec![
        "chatgpt-usage".to_string(),
        auth_path.to_string_lossy().into_owned(),
    ];
    shell::run(&argv)
}

fn parse_window(index: usize, window: &Value) -> Result<ChatgptUsageWindow, String> {
    let object = window
        .as_object()
        .ok_or_else(|| format!("windows[{index}] must be an object"))?;
    let used_percent = object
        .get("used_percent")
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("windows[{index}].used_percent must be numeric"))?;
    validate_used_percent(index, used_percent)?;
    let resets_at = object
        .get("resets_at")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("windows[{index}].resets_at must be a string"))?;
    validate_resets_at(index, resets_at)?;
    Ok(ChatgptUsageWindow {
        name: object
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string),
        used_percent,
        resets_at: resets_at.to_string(),
    })
}

fn validate_used_percent(index: usize, used_percent: f64) -> Result<(), String> {
    if (0.0..=100.0).contains(&used_percent) {
        return Ok(());
    }
    Err(format!(
        "windows[{index}].used_percent out of range: {used_percent}"
    ))
}

fn validate_resets_at(index: usize, resets_at: &str) -> Result<(), String> {
    DateTime::parse_from_rfc3339(resets_at)
        .map(|_| ())
        .map_err(|err| format!("windows[{index}].resets_at invalid RFC3339: {err}"))
}
