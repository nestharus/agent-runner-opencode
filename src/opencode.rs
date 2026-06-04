//! Declared roles: adapter, parser, accessor
//! adapter_declarations:
//!   - component: src/opencode.rs
//!     role: adapter
//!     Translates:
//!       - opencode run --format json event stream
//!       - opencode sessionID launch marker metadata
//!       - opencode event type/timestamp/part metadata
//!       - opencode export native session JSON

use crate::account::AccountProfile;
use serde::Deserialize;
use serde_json::Value;
use std::process::Command;

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
        lines
            .iter()
            .filter_map(|line| parse_event_line(line))
            .collect()
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
    let output = Command::new(account.opencode_wrapper)
        .arg("export")
        .arg(session_id)
        .output()
        .map_err(|err| OpencodeExportError::Spawn(err.to_string()))?;
    if !output.status.success() {
        return Err(OpencodeExportError::Failed {
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }
    parse_export_stdout(&output.stdout)
}

pub fn parse_export_stdout(stdout: &[u8]) -> Result<OpencodeExport, OpencodeExportError> {
    let start = stdout
        .iter()
        .position(|byte| *byte == b'{')
        .ok_or_else(|| OpencodeExportError::InvalidJson("missing JSON object".to_string()))?;
    serde_json::from_slice(&stdout[start..])
        .map_err(|err| OpencodeExportError::InvalidJson(err.to_string()))
}

fn drain_complete_lines(pending: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let split_at = match pending.iter().rposition(|byte| *byte == b'\n') {
        Some(index) => index + 1,
        None => return Vec::new(),
    };
    let drained = pending.drain(..split_at).collect::<Vec<_>>();
    drained
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.trim_ascii().is_empty())
        .map(Vec::from)
        .collect()
}

fn parse_event_line(line: &[u8]) -> Option<OpencodeEventMetadata> {
    let event: OpencodeEventMetadata = serde_json::from_slice(line).ok()?;
    is_pinned_native_event(&event).then_some(event)
}

fn is_pinned_native_event(event: &OpencodeEventMetadata) -> bool {
    matches!(
        event.event_type.as_str(),
        "step_start" | "text" | "step_finish"
    ) && event.part.is_object()
}
