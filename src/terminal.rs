//! Declared roles: mapper, parser, formatter, validator

use crate::encoding::{bounded_text, decode_base64};
use crate::envelope::ProviderFailure;
use serde::Deserialize;
use serde_json::{json, Value};

const TERMINAL_SIGNAL_EVIDENCE_MAX_LEN: usize = 160;

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessStatus {
    Exited { code: i32 },
    SignalTerminated { signal: i32 },
    SpawnError { reason: String },
    ProlongedSilence { reason: String },
    Cancelled,
    Unknown,
}

pub fn classify_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_classify_params(params, request_id)?;
    let stdout = decode_stream(&params.stdout_base64, request_id, "stdout_base64")?;
    let stderr = decode_stream(&params.stderr_base64, request_id, "stderr_base64")?;
    let signal = classify(&stdout, &stderr, &params.status, params.observed_at_unix_ms);
    Ok(json!({ "terminal_signal": signal }))
}

pub fn classify(
    _stdout: &[u8],
    _stderr: &[u8],
    status: &ProcessStatus,
    observed_at_unix_ms: u64,
) -> Value {
    terminal_signal(
        signal_kind(status),
        signal_evidence(status),
        observed_at_unix_ms,
    )
}

pub fn process_status_json(status: &ProcessStatus) -> Value {
    match status {
        ProcessStatus::Exited { code } => json!({ "kind": "exited", "code": code }),
        ProcessStatus::SignalTerminated { signal } => {
            json!({ "kind": "signal_terminated", "signal": signal })
        }
        ProcessStatus::SpawnError { reason } => {
            json!({ "kind": "spawn_error", "reason": reason })
        }
        ProcessStatus::ProlongedSilence { reason } => {
            json!({ "kind": "prolonged_silence", "reason": reason })
        }
        ProcessStatus::Cancelled => json!({ "kind": "cancelled" }),
        ProcessStatus::Unknown => json!({ "kind": "unknown" }),
    }
}

pub fn exit_code_for_status(status: &ProcessStatus) -> i32 {
    match status {
        ProcessStatus::Exited { code } => *code,
        ProcessStatus::SignalTerminated { signal } => 128 + *signal,
        ProcessStatus::ProlongedSilence { .. } => 124,
        ProcessStatus::Cancelled => 130,
        ProcessStatus::SpawnError { .. } | ProcessStatus::Unknown => 1,
    }
}

#[derive(Deserialize)]
struct TerminalClassifyParams {
    stdout_base64: String,
    stderr_base64: String,
    status: ProcessStatus,
    observed_at_unix_ms: u64,
}

fn parse_classify_params(
    params: Value,
    request_id: &str,
) -> Result<TerminalClassifyParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_terminal_params",
            format!("terminal.classify params are invalid: {err}"),
        )
    })
}

fn decode_stream(
    value: &str,
    request_id: &str,
    field: &'static str,
) -> Result<Vec<u8>, ProviderFailure> {
    decode_base64(value).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_base64",
            format!("{field} is not valid base64: {err}"),
        )
    })
}

fn signal_kind(status: &ProcessStatus) -> &'static str {
    match status {
        ProcessStatus::Exited { code: 0 } => "clean_exit",
        ProcessStatus::Exited { .. } => "nonzero_exit",
        ProcessStatus::SignalTerminated { .. } => "signal_exit",
        ProcessStatus::SpawnError { .. } => "spawn_error",
        ProcessStatus::ProlongedSilence { .. } => "prolonged_silence",
        ProcessStatus::Cancelled => "cancelled",
        ProcessStatus::Unknown => "unknown",
    }
}

fn signal_evidence(status: &ProcessStatus) -> String {
    match status {
        ProcessStatus::Exited { code } => format!("exit_code={code}"),
        ProcessStatus::SignalTerminated { signal } => format!("signal={signal}"),
        ProcessStatus::SpawnError { reason } | ProcessStatus::ProlongedSilence { reason } => {
            bounded_text(reason, TERMINAL_SIGNAL_EVIDENCE_MAX_LEN)
        }
        ProcessStatus::Cancelled => "cancelled".to_string(),
        ProcessStatus::Unknown => "unknown".to_string(),
    }
}

fn terminal_signal(kind: &str, evidence: String, observed_at_unix_ms: u64) -> Value {
    json!({
        "kind": kind,
        "evidence": evidence,
        "observed_at_unix_ms": observed_at_unix_ms,
    })
}
