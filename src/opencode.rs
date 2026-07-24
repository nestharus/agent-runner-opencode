//! Declared roles: orchestration, parser, accessor, filter, predicate, mapper, validator, formatter
//! adapter_declarations:
//!   - component: src/opencode.rs
//!     role: adapter
//!     Translates:
//!       - opencode run --format json event stream
//!       - opencode sessionID launch marker metadata
//!       - opencode event type/timestamp/part metadata
//!       - opencode export native session JSON

use crate::account::AccountProfile;
use crate::shell;
use crate::shell::ShellOutput;
use serde::Deserialize;
use serde_json::Value;
use std::path::Path;

#[derive(Default)]
pub struct EventParser {
    pending: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeEventMetadata {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "sessionID")]
    pub session_id: Option<String>,
    pub timestamp: u64,
    #[serde(default)]
    pub part: Value,
    #[serde(default)]
    pub error: Option<OpencodeEventError>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeEventError {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub data: OpencodeEventErrorData,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct OpencodeEventErrorData {
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OpencodeExport {
    pub info: OpencodeExportInfo,
    pub messages: Vec<OpencodeMessage>,
    native_json: Value,
}

#[derive(Deserialize)]
struct ParsedOpencodeExport {
    info: OpencodeExportInfo,
    #[serde(default)]
    messages: Vec<OpencodeMessage>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeExportInfo {
    pub id: String,
    pub title: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeMessage {
    pub info: OpencodeMessageInfo,
    #[serde(default)]
    pub parts: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeMessageInfo {
    pub id: String,
    pub role: String,
    #[serde(rename = "sessionID")]
    pub session_id: Option<String>,
    pub time: Option<OpencodeMessageTime>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeMessageTime {
    pub created: Option<u64>,
    pub completed: Option<u64>,
}

impl OpencodeExport {
    pub fn native_json(&self) -> &Value {
        &self.native_json
    }
}

#[derive(Debug)]
pub enum OpencodeExportError {
    Spawn(String),
    Failed { status: Option<i32>, stderr: String },
    InvalidJson(String),
}

#[derive(Debug)]
pub enum OpencodeSessionListError {
    Spawn(String),
    Failed { status: Option<i32>, stderr: String },
    InvalidJson(String),
}

#[derive(Debug)]
pub enum OpencodeImportError {
    Spawn(String),
    Failed { status: Option<i32>, stderr: String },
    MissingSessionId(String),
}

impl EventParser {
    pub fn ingest(&mut self, bytes: &[u8]) -> Vec<OpencodeEventMetadata> {
        self.pending.extend_from_slice(bytes);
        let lines = drain_complete_lines(&mut self.pending);
        parse_event_lines(&lines)
    }

    pub fn finish(&mut self) -> Vec<OpencodeEventMetadata> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let line = std::mem::take(&mut self.pending);
        parse_event_line(&line).into_iter().collect()
    }
}

pub fn first_session_id(events: &[OpencodeEventMetadata]) -> Option<String> {
    events.iter().find_map(|event| event.session_id.clone())
}

pub fn is_structured_error_event(event: &OpencodeEventMetadata) -> bool {
    event.event_type.as_str() == "error" && event.error.is_some()
}

pub fn export(
    session_id: &str,
    account: &AccountProfile,
) -> Result<OpencodeExport, OpencodeExportError> {
    let output = shell::command(account.opencode_wrapper)
        .arg("export")
        .arg(session_id)
        .output()
        .map_err(export_spawn_error)?;
    validate_export_status(&output)?;
    parse_export_stdout(&output.stdout)
}

pub fn session_list(
    limit: Option<usize>,
    account: &AccountProfile,
) -> Result<Vec<Value>, OpencodeSessionListError> {
    let mut command = shell::command(account.opencode_wrapper);
    command
        .arg("session")
        .arg("list")
        .arg("--format")
        .arg("json");
    if let Some(limit) = limit {
        command.arg("--max-count").arg(limit.to_string());
    }
    let output = command.output().map_err(session_list_spawn_error)?;
    validate_session_list_status(&output)?;
    parse_session_list_stdout(&output.stdout)
}

pub fn import_session(
    path: &Path,
    account: &AccountProfile,
    working_directory: &Path,
) -> Result<String, OpencodeImportError> {
    let output = shell::command(account.opencode_wrapper)
        .current_dir(working_directory)
        .arg("import")
        .arg(path)
        .output()
        .map_err(import_spawn_error)?;
    validate_import_status(&output)?;
    parse_import_stdout(&output.stdout)
}

pub fn refresh_auth(account: &AccountProfile) -> std::io::Result<ShellOutput> {
    crate::shell::run(&refresh_auth_argv(account))
}

fn refresh_auth_argv(account: &AccountProfile) -> Vec<String> {
    vec![
        account.opencode_wrapper.to_string(),
        "auth".to_string(),
        "list".to_string(),
    ]
}

pub fn parse_export_stdout(stdout: &[u8]) -> Result<OpencodeExport, OpencodeExportError> {
    let start = export_json_start(stdout)?;
    parse_export_json(&stdout[start..])
}

pub fn parse_session_list_stdout(stdout: &[u8]) -> Result<Vec<Value>, OpencodeSessionListError> {
    let start = session_list_json_start(stdout)?;
    parse_session_list_json(&stdout[start..])
}

pub fn parse_import_stdout(stdout: &[u8]) -> Result<String, OpencodeImportError> {
    let text = String::from_utf8_lossy(stdout);
    text.lines()
        .find_map(|line| line.trim().strip_prefix("Imported session: "))
        .map(str::trim)
        .filter(|session_id| !session_id.is_empty())
        .map(str::to_string)
        .ok_or_else(|| OpencodeImportError::MissingSessionId(text.into_owned()))
}

fn drain_complete_lines(pending: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let split_at = match pending.iter().rposition(|byte| *byte == b'\n') {
        Some(index) => index + 1,
        None => return Vec::new(),
    };
    let drained = pending.drain(..split_at).collect::<Vec<_>>();
    non_empty_lines(&drained)
}

fn parse_event_line(line: &[u8]) -> Option<OpencodeEventMetadata> {
    let event = parse_native_event(line)?;
    pinned_native_event(event)
}

fn is_pinned_native_event(event: &OpencodeEventMetadata) -> bool {
    is_pinned_part_event(event) || is_structured_error_event(event)
}

fn is_pinned_part_event(event: &OpencodeEventMetadata) -> bool {
    matches!(
        event.event_type.as_str(),
        "step_start" | "text" | "step_finish"
    ) && event.part.is_object()
}

fn parse_event_lines(lines: &[Vec<u8>]) -> Vec<OpencodeEventMetadata> {
    let parsed = parse_native_event_lines(lines);
    let parsed = valid_native_events(parsed);
    pinned_native_events(parsed)
}

fn parse_native_event_lines(lines: &[Vec<u8>]) -> Vec<Option<OpencodeEventMetadata>> {
    lines.iter().map(|line| parse_native_event(line)).collect()
}

fn valid_native_events(events: Vec<Option<OpencodeEventMetadata>>) -> Vec<OpencodeEventMetadata> {
    events.into_iter().flatten().collect()
}

fn pinned_native_events(events: Vec<OpencodeEventMetadata>) -> Vec<OpencodeEventMetadata> {
    events.into_iter().filter(is_pinned_native_event).collect()
}

fn export_spawn_error(err: std::io::Error) -> OpencodeExportError {
    OpencodeExportError::Spawn(err.to_string())
}

fn import_spawn_error(err: std::io::Error) -> OpencodeImportError {
    OpencodeImportError::Spawn(err.to_string())
}

fn session_list_spawn_error(err: std::io::Error) -> OpencodeSessionListError {
    OpencodeSessionListError::Spawn(err.to_string())
}

fn validate_export_status(output: &std::process::Output) -> Result<(), OpencodeExportError> {
    if output.status.success() {
        return Ok(());
    }
    Err(export_failed_error(output))
}

fn validate_session_list_status(
    output: &std::process::Output,
) -> Result<(), OpencodeSessionListError> {
    if output.status.success() {
        return Ok(());
    }
    Err(session_list_failed_error(output))
}

fn validate_import_status(output: &std::process::Output) -> Result<(), OpencodeImportError> {
    if output.status.success() {
        return Ok(());
    }
    Err(OpencodeImportError::Failed {
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn export_failed_error(output: &std::process::Output) -> OpencodeExportError {
    OpencodeExportError::Failed {
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

fn session_list_failed_error(output: &std::process::Output) -> OpencodeSessionListError {
    OpencodeSessionListError::Failed {
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

fn export_json_start(stdout: &[u8]) -> Result<usize, OpencodeExportError> {
    stdout
        .iter()
        .position(|byte| *byte == b'{')
        .ok_or_else(missing_export_json_error)
}

fn session_list_json_start(stdout: &[u8]) -> Result<usize, OpencodeSessionListError> {
    stdout
        .iter()
        .position(|byte| *byte == b'[')
        .ok_or_else(missing_session_list_json_error)
}

fn parse_export_json(bytes: &[u8]) -> Result<OpencodeExport, OpencodeExportError> {
    let native_json: Value = serde_json::from_slice(bytes).map_err(invalid_export_json_error)?;
    let parsed: ParsedOpencodeExport =
        serde_json::from_value(native_json.clone()).map_err(invalid_export_json_error)?;
    Ok(OpencodeExport {
        info: parsed.info,
        messages: parsed.messages,
        native_json,
    })
}

fn parse_session_list_json(bytes: &[u8]) -> Result<Vec<Value>, OpencodeSessionListError> {
    serde_json::from_slice(bytes).map_err(invalid_session_list_json_error)
}

fn missing_export_json_error() -> OpencodeExportError {
    OpencodeExportError::InvalidJson("missing JSON object".to_string())
}

fn missing_session_list_json_error() -> OpencodeSessionListError {
    OpencodeSessionListError::InvalidJson("missing JSON array".to_string())
}

fn invalid_export_json_error(err: serde_json::Error) -> OpencodeExportError {
    OpencodeExportError::InvalidJson(err.to_string())
}

fn invalid_session_list_json_error(err: serde_json::Error) -> OpencodeSessionListError {
    OpencodeSessionListError::InvalidJson(err.to_string())
}

fn non_empty_lines(drained: &[u8]) -> Vec<Vec<u8>> {
    let lines = byte_lines(drained);
    let lines = select_non_empty_lines(lines);
    owned_byte_lines(lines)
}

fn byte_lines(drained: &[u8]) -> Vec<&[u8]> {
    drained.split(|byte| *byte == b'\n').collect()
}

fn select_non_empty_lines(lines: Vec<&[u8]>) -> Vec<&[u8]> {
    lines
        .into_iter()
        .filter(|line| is_non_empty_line(line))
        .collect()
}

fn is_non_empty_line(line: &[u8]) -> bool {
    !line.trim_ascii().is_empty()
}

fn owned_byte_lines(lines: Vec<&[u8]>) -> Vec<Vec<u8>> {
    lines.into_iter().map(Vec::from).collect()
}

fn parse_native_event(line: &[u8]) -> Option<OpencodeEventMetadata> {
    serde_json::from_slice(line).ok()
}

fn pinned_native_event(event: OpencodeEventMetadata) -> Option<OpencodeEventMetadata> {
    is_pinned_native_event(&event).then_some(event)
}
