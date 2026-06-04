//! Declared roles: adapter, parser, accessor
//! adapter_declarations:
//!   - component: src/opencode.rs
//!     contract: opencode run --format json metadata events
//!   - component: src/opencode.rs
//!     contract: opencode sessionID extraction
//!   - component: src/opencode.rs
//!     contract: opencode event type and part metadata

use serde::Deserialize;
use serde_json::Value;

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
    pub timestamp: Option<u64>,
    pub part: Option<Value>,
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
    serde_json::from_slice(line).ok()
}
