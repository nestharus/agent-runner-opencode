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
use rusqlite::{Connection, OpenFlags};
use serde::Deserialize;
use serde_json::Value;
use std::path::{Path, PathBuf};

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
    pub part: Value,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeExport {
    pub info: OpencodeExportInfo,
    #[serde(default)]
    pub messages: Vec<OpencodeMessage>,
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

#[derive(Debug)]
pub enum OpencodeExportError {
    Spawn(String),
    Failed { status: Option<i32>, stderr: String },
    InvalidJson(String),
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

pub fn export_with_sqlite_fallback(
    session_id: &str,
    account: &AccountProfile,
) -> Result<OpencodeExport, OpencodeExportError> {
    match export(session_id, account) {
        Ok(native) => Ok(native),
        Err(err) => export_error_with_sqlite_fallback(session_id, account, err),
    }
}

pub fn parse_export_stdout(stdout: &[u8]) -> Result<OpencodeExport, OpencodeExportError> {
    let start = export_json_start(stdout)?;
    parse_export_json(&stdout[start..])
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

fn validate_export_status(output: &std::process::Output) -> Result<(), OpencodeExportError> {
    if output.status.success() {
        return Ok(());
    }
    Err(export_failed_error(output))
}

fn export_failed_error(output: &std::process::Output) -> OpencodeExportError {
    OpencodeExportError::Failed {
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

fn parse_export_json(bytes: &[u8]) -> Result<OpencodeExport, OpencodeExportError> {
    serde_json::from_slice(bytes).map_err(invalid_export_json_error)
}

fn missing_export_json_error() -> OpencodeExportError {
    OpencodeExportError::InvalidJson("missing JSON object".to_string())
}

fn invalid_export_json_error(err: serde_json::Error) -> OpencodeExportError {
    OpencodeExportError::InvalidJson(err.to_string())
}

fn export_error_with_sqlite_fallback(
    session_id: &str,
    account: &AccountProfile,
    err: OpencodeExportError,
) -> Result<OpencodeExport, OpencodeExportError> {
    if !matches!(err, OpencodeExportError::InvalidJson(_)) {
        return Err(err);
    }
    sqlite_export(session_id, account).ok_or(err)
}

fn sqlite_export(session_id: &str, account: &AccountProfile) -> Option<OpencodeExport> {
    let db_path = sqlite_export_db_path(account)?;
    let conn = sqlite_export_connection(&db_path)?;
    let messages = sqlite_export_messages(&conn, session_id)?;
    if messages.is_empty() {
        return None;
    }
    Some(OpencodeExport {
        info: OpencodeExportInfo {
            id: session_id.to_string(),
            title: None,
        },
        messages,
    })
}

fn sqlite_export_db_path(account: &AccountProfile) -> Option<PathBuf> {
    Some(sqlite_export_base_dir(account)?.join("opencode.db"))
}

fn sqlite_export_base_dir(account: &AccountProfile) -> Option<PathBuf> {
    if account.opencode_index == 1 {
        return default_opencode_base_dir();
    }
    Some(
        home_dir()?
            .join(format!(".opencode{}", account.opencode_index))
            .join("opencode"),
    )
}

fn default_opencode_base_dir() -> Option<PathBuf> {
    let data_home = xdg_data_home().or_else(default_xdg_data_home)?;
    Some(data_home.join("opencode"))
}

fn default_xdg_data_home() -> Option<PathBuf> {
    Some(home_dir()?.join(".local/share"))
}

fn xdg_data_home() -> Option<PathBuf> {
    std::env::var_os("XDG_DATA_HOME").map(PathBuf::from)
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn sqlite_export_connection(path: &Path) -> Option<Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).ok()
}

fn sqlite_export_messages(conn: &Connection, session_id: &str) -> Option<Vec<OpencodeMessage>> {
    let mut stmt = conn
        .prepare(
            "SELECT id, session_id, time_created, data
             FROM message
             WHERE session_id = ?1
             ORDER BY time_created, id",
        )
        .ok()?;
    let rows = stmt
        .query_map([session_id], |row| {
            Ok(SqliteMessageRow {
                id: row.get(0)?,
                session_id: row.get(1)?,
                time_created: row.get(2)?,
                data: row.get(3)?,
            })
        })
        .ok()?;
    let messages = rows
        .filter_map(Result::ok)
        .filter_map(|row| sqlite_export_message(conn, row))
        .collect();
    Some(messages)
}

struct SqliteMessageRow {
    id: String,
    session_id: String,
    time_created: i64,
    data: String,
}

fn sqlite_export_message(conn: &Connection, row: SqliteMessageRow) -> Option<OpencodeMessage> {
    let data = sqlite_json(&row.data)?;
    let role = data.get("role")?.as_str()?.to_string();
    let parts = sqlite_export_parts(conn, &row.session_id, &row.id)?;
    Some(OpencodeMessage {
        info: OpencodeMessageInfo {
            id: row.id,
            role,
            session_id: Some(row.session_id),
            time: sqlite_message_time(&data, row.time_created),
        },
        parts,
    })
}

fn sqlite_message_time(data: &Value, time_created: i64) -> Option<OpencodeMessageTime> {
    let created = data
        .pointer("/time/created")
        .and_then(Value::as_u64)
        .or_else(|| u64::try_from(time_created).ok());
    let completed = data.pointer("/time/completed").and_then(Value::as_u64);
    (created.is_some() || completed.is_some()).then_some(OpencodeMessageTime { created, completed })
}

fn sqlite_export_parts(
    conn: &Connection,
    session_id: &str,
    message_id: &str,
) -> Option<Vec<Value>> {
    let mut stmt = conn
        .prepare(
            "SELECT data
             FROM part
             WHERE session_id = ?1 AND message_id = ?2
             ORDER BY time_created, id",
        )
        .ok()?;
    let rows = stmt
        .query_map([session_id, message_id], |row| row.get::<_, String>(0))
        .ok()?;
    Some(
        rows.filter_map(Result::ok)
            .filter_map(|data| sqlite_json(&data))
            .collect(),
    )
}

fn sqlite_json(data: &str) -> Option<Value> {
    serde_json::from_str(data).ok()
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
