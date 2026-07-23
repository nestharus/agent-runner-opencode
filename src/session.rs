//! Declared roles: orchestration, mapper, parser, validator, formatter, filter, accessor, predicate
//! intrinsic_surface_declarations:
//!   - component: src/session.rs
//!     role: intrinsic-surface
//!     Domain: canonical transcript surface
//!     Owns:
//!       - opencode export to provider session responses
//!       - canonical transcript byte serialization
//!       - session replace unsupported boundary
//!
//! adapter_declarations:
//!   - component: src/session.rs
//!     role: adapter
//!     Translates:
//!       - opencode export native session JSON to SessionReadTurnsResult
//!       - opencode launch sessionID evidence to SessionCaptureResult
//!       - opencode export native session JSON to oulipoly.canonical_transcript/v1
//!       - opencode absent transcript path to SessionLocateTranscriptResult
//!       - opencode unsupported transcript import to SessionReplaceResult boundary

use crate::account::profile_for_settings_id;
use crate::encoding::{encode_base64, sha256_hex};
use crate::envelope::{ProviderFailure, RequestEnvelope};
use crate::opencode::{
    self, OpencodeExport, OpencodeExportError, OpencodeMessage, OpencodeSessionListError,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::Path;

const CANONICAL_FORMAT: &str = "oulipoly.canonical_transcript/v1";
const NATIVE_FORMAT_ID: &str = "opencode.export/native-json";
const SOURCE_KIND: &str = "opencode.export";

#[derive(Deserialize)]
struct SessionParams {
    settings_id: String,
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct SessionCaptureParams {
    #[serde(rename = "settings_id")]
    _settings_id: String,
    session_id: Option<String>,
    launch: Option<SessionCaptureLaunch>,
    pinned_target: Option<String>,
    start_bound_provider_session_id: Option<String>,
    #[serde(flatten)]
    extra: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionCaptureLaunch {
    session: Option<SessionCaptureLaunchSession>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionCaptureLaunchSession {
    provider_session_id: Option<String>,
    #[serde(rename = "source")]
    _source: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionEnumerateParams {
    settings_id: String,
    limit: Option<usize>,
    cursor: Option<String>,
    include_cwd: Option<bool>,
    include_turn_count: Option<bool>,
    since_unix_ms: Option<u64>,
}

struct CapturedSession {
    provider_session_id: Option<String>,
    source: &'static str,
}

pub fn handle(subcommand: &str, request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let RequestEnvelope {
        params, request_id, ..
    } = request;
    match subcommand {
        "session.locate_transcript" => locate_transcript_params(params, &request_id),
        "session.read_turns" => read_turns_params(params, &request_id),
        "session.capture" => capture_params(params, &request_id),
        "session.enumerate" => enumerate_params(params, &request_id),
        "session.export" => export_params(params, &request_id),
        "session.replace" => replace_params(params, &request_id),
        unknown => Err(unknown_session_subcommand_failure(request_id, unknown)),
    }
}

pub fn locate_transcript_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    Ok(locate_transcript_result(params.session_id.as_deref()))
}

pub fn read_turns_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    let session_id = required_session_id(&params, request_id)?;
    let native = export_native(&params.settings_id, &session_id, request_id)?;
    let turns = native_turns(&native, &session_id)?;
    Ok(read_turns_result(turns))
}

pub fn capture_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_capture_params(params, request_id)?;
    let captured = captured_session_id(&params);
    let provider_session_id = captured.provider_session_id;
    let source = captured.source;
    Ok(capture_result(provider_session_id, source))
}

pub fn enumerate_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_enumerate_params(params, request_id)?;
    validate_enumerate_params(&params, request_id)?;
    let native = enumerate_native(&params.settings_id, params.limit, request_id)?;
    let (sessions, warnings) = enumerate_sessions(native, &params, request_id)?;
    Ok(enumerate_result(sessions, warnings))
}

pub fn export_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    let session_id = required_session_id(&params, request_id)?;
    let native = export_native(&params.settings_id, &session_id, request_id)?;
    let records = canonical_records(&native, &session_id)?;
    let bytes = canonical_jsonl(&records);
    Ok(export_result(&bytes, records.len()))
}

pub fn replace_params(_params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    Err(session_replace_unsupported_failure(request_id))
}

fn parse_session_params(params: Value, request_id: &str) -> Result<SessionParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_session_params_failure(request_id, err))
}

fn parse_capture_params(
    params: Value,
    request_id: &str,
) -> Result<SessionCaptureParams, ProviderFailure> {
    let params: SessionCaptureParams = serde_json::from_value(params)
        .map_err(|err| invalid_session_capture_params_failure(request_id, err))?;
    if params.extra.contains_key("evidence") {
        return Err(invalid_session_capture_params_failure(
            request_id,
            "the removed evidence field is unsupported",
        ));
    }
    Ok(params)
}

fn parse_enumerate_params(
    params: Value,
    request_id: &str,
) -> Result<SessionEnumerateParams, ProviderFailure> {
    serde_json::from_value(params)
        .map_err(|err| invalid_session_enumerate_params_failure(request_id, err))
}

fn validate_enumerate_params(
    params: &SessionEnumerateParams,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if params.limit == Some(0) {
        return Err(invalid_session_enumerate_limit_failure(request_id));
    }
    Ok(())
}

fn required_session_id(
    params: &SessionParams,
    request_id: &str,
) -> Result<String, ProviderFailure> {
    params
        .session_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| missing_session_id_failure(request_id))
}

fn export_native(
    settings_id: &str,
    session_id: &str,
    request_id: &str,
) -> Result<OpencodeExport, ProviderFailure> {
    let account = session_account(settings_id, request_id)?;
    let native = opencode::export(session_id, account)
        .map_err(|err| export_failure(request_id, session_id, err))?;
    validate_export_session_id(&native, session_id, request_id)?;
    validate_export_message_sessions(&native, session_id, request_id)?;
    Ok(native)
}

fn enumerate_native(
    settings_id: &str,
    limit: Option<usize>,
    request_id: &str,
) -> Result<Vec<Value>, ProviderFailure> {
    let account = session_account(settings_id, request_id)?;
    opencode::session_list(limit, account).map_err(|err| session_list_failure(request_id, err))
}

fn enumerate_sessions(
    native: Vec<Value>,
    params: &SessionEnumerateParams,
    request_id: &str,
) -> Result<(Vec<Value>, Vec<String>), ProviderFailure> {
    let mut warnings = enumerate_param_warnings(params);
    let mut sessions = Vec::new();
    for (index, entry) in native.iter().enumerate() {
        if let Some(session) =
            enumerate_session_entry(index, entry, params, request_id, &mut warnings)?
        {
            sessions.push(session);
        }
    }
    truncate_sessions(&mut sessions, params.limit);
    Ok((sessions, warnings))
}

fn enumerate_session_entry(
    index: usize,
    entry: &Value,
    params: &SessionEnumerateParams,
    request_id: &str,
    warnings: &mut Vec<String>,
) -> Result<Option<Value>, ProviderFailure> {
    let provider_session_id = required_enumerated_session_id(entry, index, request_id)?;
    let created_unix_ms = created_unix_ms(entry);
    let updated_unix_ms = updated_unix_ms(entry);
    if !matches_since_filter(created_unix_ms, updated_unix_ms, params.since_unix_ms) {
        return Ok(None);
    }
    if params.since_unix_ms.is_some() && created_unix_ms.is_none() && updated_unix_ms.is_none() {
        warnings.push(format!(
            "session {provider_session_id} has no timestamp; since_unix_ms filter could not be applied"
        ));
    }
    let include_cwd = params.include_cwd.unwrap_or(true);
    let include_turn_count = params.include_turn_count.unwrap_or(true);
    let cwd = include_cwd.then(|| enumerate_cwd(entry, index, &provider_session_id, warnings));
    let turn_count = include_turn_count.then(|| enumerated_turn_count(entry));
    Ok(Some(json!({
        "provider_session_id": provider_session_id,
        "title": string_field(entry, &["title"]),
        "cwd": cwd.flatten(),
        "created_unix_ms": created_unix_ms,
        "updated_unix_ms": updated_unix_ms,
        "turn_count": turn_count.flatten(),
        "source": {
            "kind": "opencode.session_list",
            "detail": "session list --format json"
        }
    })))
}

fn enumerate_param_warnings(params: &SessionEnumerateParams) -> Vec<String> {
    let mut warnings = Vec::new();
    if params
        .cursor
        .as_deref()
        .is_some_and(|cursor| !cursor.trim().is_empty())
    {
        warnings.push("cursor is ignored by opencode session.enumerate v1".to_string());
    }
    warnings
}

fn truncate_sessions(sessions: &mut Vec<Value>, limit: Option<usize>) {
    if let Some(limit) = limit {
        sessions.truncate(limit);
    }
}

fn required_enumerated_session_id(
    entry: &Value,
    index: usize,
    request_id: &str,
) -> Result<String, ProviderFailure> {
    string_field(entry, &["id", "sessionID", "sessionId", "session_id"])
        .ok_or_else(|| invalid_opencode_session_list_row_failure(request_id, index))
}

fn enumerate_cwd(
    entry: &Value,
    index: usize,
    provider_session_id: &str,
    warnings: &mut Vec<String>,
) -> Option<String> {
    let cwd = match raw_string_field(entry, &["directory", "cwd", "working_directory"]) {
        Some(cwd) => cwd,
        None => {
            warnings.push(format!(
                "session {provider_session_id} row {index} has no directory/cwd"
            ));
            return None;
        }
    };
    if cwd.trim().is_empty() {
        warnings.push(format!(
            "session {provider_session_id} row {index} has an empty directory/cwd"
        ));
        return None;
    }
    if !Path::new(cwd).is_absolute() {
        warnings.push(format!(
            "session {provider_session_id} row {index} has a non-absolute directory/cwd: {cwd}"
        ));
        return None;
    }
    Some(cwd.to_string())
}

fn matches_since_filter(
    created_unix_ms: Option<u64>,
    updated_unix_ms: Option<u64>,
    since_unix_ms: Option<u64>,
) -> bool {
    let Some(since_unix_ms) = since_unix_ms else {
        return true;
    };
    updated_unix_ms
        .or(created_unix_ms)
        .map(|unix_ms| unix_ms >= since_unix_ms)
        .unwrap_or(true)
}

fn created_unix_ms(entry: &Value) -> Option<u64> {
    integer_field(entry, &["created", "created_unix_ms", "createdUnixMs"])
        .or_else(|| nested_integer_field(entry, "time", &["created", "created_unix_ms"]))
}

fn updated_unix_ms(entry: &Value) -> Option<u64> {
    integer_field(entry, &["updated", "updated_unix_ms", "updatedUnixMs"])
        .or_else(|| nested_integer_field(entry, "time", &["updated", "updated_unix_ms"]))
}

fn enumerated_turn_count(entry: &Value) -> Option<u64> {
    integer_field(
        entry,
        &["turn_count", "turnCount", "message_count", "messageCount"],
    )
    .or_else(|| {
        entry
            .get("messages")
            .and_then(Value::as_array)
            .map(|messages| messages.len() as u64)
    })
}

fn string_field(entry: &Value, keys: &[&str]) -> Option<String> {
    raw_string_field(entry, keys)
        .and_then(|value| (!value.trim().is_empty()).then_some(value))
        .map(str::to_string)
}

fn raw_string_field<'a>(entry: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| entry.get(*key).and_then(Value::as_str))
}

fn nested_integer_field(entry: &Value, parent: &str, keys: &[&str]) -> Option<u64> {
    entry
        .get(parent)
        .and_then(|nested| integer_field(nested, keys))
}

fn integer_field(entry: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| entry.get(*key).and_then(unsigned_integer_value))
}

fn unsigned_integer_value(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
}

fn validate_export_session_id(
    native: &OpencodeExport,
    expected: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if native.info.id == expected {
        return Ok(());
    }
    Err(session_export_id_mismatch_failure(
        request_id,
        &native.info.id,
        expected,
    ))
}

fn validate_export_message_sessions(
    native: &OpencodeExport,
    expected: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    for message in &native.messages {
        match message.info.session_id.as_deref() {
            Some(session_id) if session_id == expected => {}
            Some(session_id) => {
                return Err(session_record_id_mismatch_failure(
                    request_id,
                    &message.info.id,
                    session_id,
                    expected,
                ));
            }
            None => {
                return Err(session_record_missing_session_id_failure(
                    request_id,
                    &message.info.id,
                ));
            }
        }
    }
    Ok(())
}

fn export_failure(request_id: &str, session_id: &str, err: OpencodeExportError) -> ProviderFailure {
    match err {
        OpencodeExportError::Spawn(message) => {
            opencode_export_unavailable_failure(request_id, session_id, message)
        }
        OpencodeExportError::Failed { status, stderr } => {
            session_export_failed_failure(request_id, session_id, status, &stderr)
        }
        OpencodeExportError::InvalidJson(message) => {
            invalid_opencode_export_failure(request_id, message)
        }
    }
}

fn session_list_failure(request_id: &str, err: OpencodeSessionListError) -> ProviderFailure {
    match err {
        OpencodeSessionListError::Spawn(message) => {
            opencode_session_list_unavailable_failure(request_id, message)
        }
        OpencodeSessionListError::Failed { status, stderr } => {
            session_list_failed_failure(request_id, status, &stderr)
        }
        OpencodeSessionListError::InvalidJson(message) => {
            invalid_opencode_session_list_failure(request_id, message)
        }
    }
}

fn native_turns(native: &OpencodeExport, session_id: &str) -> Result<Vec<Value>, ProviderFailure> {
    native
        .messages
        .iter()
        .map(|message| native_turn(message, session_id))
        .collect()
}

fn native_turn(message: &OpencodeMessage, session_id: &str) -> Result<Value, ProviderFailure> {
    Ok(json!({
        "session_id": session_id,
        "turn_id": stable_turn_id(message, session_id),
        "role": message.info.role,
        "timestamp": provider_turn_timestamp(message),
        "body": text_parts(message),
        "native": {
            "message_id": message.info.id,
            "session_id": message.info.session_id,
            "created_unix_ms": message.info.time.as_ref().and_then(|time| time.created),
            "completed_unix_ms": message.info.time.as_ref().and_then(|time| time.completed),
            "parts": message.parts,
        },
    }))
}

fn provider_turn_timestamp(message: &OpencodeMessage) -> String {
    message
        .info
        .time
        .as_ref()
        .and_then(|time| time.created.or(time.completed))
        .and_then(|milliseconds| i64::try_from(milliseconds).ok())
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn canonical_records(
    native: &OpencodeExport,
    session_id: &str,
) -> Result<Vec<Value>, ProviderFailure> {
    native
        .messages
        .iter()
        .map(|message| canonical_record(message, session_id, native.info.title.as_deref()))
        .collect()
}

fn canonical_record(
    message: &OpencodeMessage,
    session_id: &str,
    title: Option<&str>,
) -> Result<Value, ProviderFailure> {
    Ok(json!({
        "body": text_parts(message),
        "id": stable_turn_id(message, session_id),
        "metadata": {
            "native_message_id": message.info.id,
            "native_session_id": message.info.session_id,
            "native_title": title,
            "source_format": NATIVE_FORMAT_ID,
        },
        "role": message.info.role,
        "timestamp": message_timestamp(message),
        "type": "turn",
    }))
}

fn canonical_jsonl(records: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend_from_slice(record.to_string().as_bytes());
        bytes.push(b'\n');
    }
    bytes
}

fn stable_turn_id(message: &OpencodeMessage, session_id: &str) -> String {
    let order = message_order_key(message);
    let preimage = format!("opencode-turn\0{session_id}\0{}\0{order}", message.info.id);
    format!("turn_{}", sha256_hex(preimage.as_bytes()))
}

fn message_order_key(message: &OpencodeMessage) -> String {
    message
        .info
        .time
        .as_ref()
        .and_then(|time| time.created.or(time.completed))
        .map(|value| value.to_string())
        .unwrap_or_else(|| message.info.id.clone())
}

fn message_timestamp(message: &OpencodeMessage) -> String {
    message_order_key(message)
}

fn text_parts(message: &OpencodeMessage) -> Vec<Value> {
    let parts = native_text_parts(&message.parts);
    let texts = native_text_part_texts(parts);
    texts.into_iter().map(contract_text_part).collect()
}

fn native_text_parts(parts: &[Value]) -> Vec<&Value> {
    parts
        .iter()
        .filter(|part| is_native_text_part(part))
        .collect()
}

fn is_native_text_part(part: &Value) -> bool {
    part.get("type").and_then(Value::as_str) == Some("text")
}

fn native_text_part_texts(parts: Vec<&Value>) -> Vec<&str> {
    parts
        .into_iter()
        .filter_map(native_text_part_text)
        .collect()
}

fn native_text_part_text(part: &Value) -> Option<&str> {
    part.get("text").and_then(Value::as_str)
}

fn contract_text_part(text: &str) -> Value {
    json!({
        "type": "text",
        "text": text,
    })
}

fn captured_session_id(params: &SessionCaptureParams) -> CapturedSession {
    if let Some(provider_session_id) = launch_provider_session_id(params) {
        return CapturedSession {
            provider_session_id: Some(provider_session_id),
            source: "launch.session.provider_session_id",
        };
    }
    if let Some(provider_session_id) = bare_provider_session_id(params) {
        return CapturedSession {
            provider_session_id: Some(provider_session_id),
            source: "session_id",
        };
    }
    if let Some(provider_session_id) = pinned_target(params) {
        return CapturedSession {
            provider_session_id: Some(provider_session_id),
            source: "pinned_target",
        };
    }
    if let Some(provider_session_id) = start_bound_provider_session_id(params) {
        return CapturedSession {
            provider_session_id: Some(provider_session_id),
            source: "start_bound_provider_session_id",
        };
    }
    CapturedSession {
        provider_session_id: None,
        source: "none",
    }
}

fn non_empty_string(value: Option<&str>) -> Option<String> {
    value
        .filter(|text| !text.trim().is_empty())
        .map(str::to_string)
}

fn capture_artifacts(provider_session_id: Option<&str>) -> Vec<Value> {
    vec![json!({
        "kind": "opencode-session-export-source",
        "uri": source_id(provider_session_id),
    })]
}

fn source_id(session_id: Option<&str>) -> String {
    session_id
        .map(|id| format!("{SOURCE_KIND}:{id}"))
        .unwrap_or_else(|| SOURCE_KIND.to_string())
}

fn unknown_session_subcommand_failure(request_id: String, unknown: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "unknown_session_subcommand",
        format!("unknown session subcommand: {unknown}"),
    )
}

fn locate_transcript_result(session_id: Option<&str>) -> Value {
    json!({
        "located": false,
        "format_id": NATIVE_FORMAT_ID,
        "source_id": source_id(session_id),
        "require_existing_observed": false,
    })
}

fn read_turns_result(turns: Vec<Value>) -> Value {
    json!({
        "turn_count": turns.len(),
        "turns": turns,
        "complete": true,
    })
}

fn capture_result(provider_session_id: Option<String>, source: &'static str) -> Value {
    let artifacts = capture_artifacts(provider_session_id.as_deref());
    let source_id = source_id(provider_session_id.as_deref());
    json!({
        "artifacts": artifacts,
        "provider_session_id": provider_session_id,
        "state": {
            "format_id": NATIVE_FORMAT_ID,
            "source_id": source_id,
            "source": source,
        },
    })
}

fn enumerate_result(sessions: Vec<Value>, warnings: Vec<String>) -> Value {
    json!({
        "sessions": sessions,
        "complete": true,
        "next_cursor": null,
        "warnings": warnings,
    })
}

fn export_result(bytes: &[u8], turn_count: usize) -> Value {
    json!({
        "canonical_format": CANONICAL_FORMAT,
        "data_base64": encode_base64(bytes),
        "sha256": sha256_hex(bytes),
        "turn_count": turn_count,
    })
}

fn session_replace_unsupported_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "session_replace_unsupported",
        "opencode does not provide a stable transcript import or replace API",
    )
}

fn invalid_session_params_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_session_params",
        format!("session params are invalid: {err}"),
    )
}

fn invalid_session_capture_params_failure(
    request_id: &str,
    err: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_session_capture_params",
        format!("session.capture params are invalid: {err}"),
    )
}

fn invalid_session_enumerate_params_failure(
    request_id: &str,
    err: serde_json::Error,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_session_enumerate_params",
        format!("session.enumerate params are invalid: {err}"),
    )
}

fn invalid_session_enumerate_limit_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_session_enumerate_params",
        "session.enumerate limit must be greater than or equal to 1",
    )
}

fn missing_session_id_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "missing_session_id",
        "session params require non-empty session_id",
    )
}

fn session_account(
    settings_id: &str,
    request_id: &str,
) -> Result<&'static crate::account::AccountProfile, ProviderFailure> {
    profile_for_settings_id(settings_id)
        .ok_or_else(|| unknown_settings_id_failure(request_id, settings_id))
}

fn unknown_settings_id_failure(request_id: &str, settings_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "unknown_settings_id",
        format!("unknown opencode settings_id: {settings_id}"),
    )
}

fn session_export_id_mismatch_failure(
    request_id: &str,
    actual: &str,
    expected: &str,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "session_export_id_mismatch",
        format!("opencode export returned session_id {actual} instead of {expected}"),
    )
}

fn session_record_id_mismatch_failure(
    request_id: &str,
    message_id: &str,
    session_id: &str,
    expected: &str,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "session_record_id_mismatch",
        format!(
            "opencode message {message_id} belongs to session {session_id} instead of {expected}"
        ),
    )
}

fn session_record_missing_session_id_failure(
    request_id: &str,
    message_id: &str,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "session_record_missing_session_id",
        format!("opencode message {message_id} is missing info.sessionID"),
    )
}

fn opencode_export_unavailable_failure(
    request_id: &str,
    session_id: &str,
    message: String,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "opencode_export_unavailable",
        format!("failed to run opencode export for {session_id}: {message}"),
    )
}

fn session_export_failed_failure(
    request_id: &str,
    session_id: &str,
    status: Option<i32>,
    stderr: &str,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "session_export_failed",
        format!(
            "opencode export failed for {session_id} with status {:?}: {}",
            status,
            stderr.trim()
        ),
    )
}

fn invalid_opencode_export_failure(request_id: &str, message: String) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_opencode_export",
        format!("opencode export output was not valid native JSON: {message}"),
    )
}

fn opencode_session_list_unavailable_failure(request_id: &str, message: String) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "opencode_session_list_unavailable",
        format!("failed to run opencode session list: {message}"),
    )
}

fn session_list_failed_failure(
    request_id: &str,
    status: Option<i32>,
    stderr: &str,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "session_list_failed",
        format!(
            "opencode session list failed with status {:?}: {}",
            status,
            stderr.trim()
        ),
    )
}

fn invalid_opencode_session_list_failure(request_id: &str, message: String) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_opencode_session_list",
        format!("opencode session list output was not valid JSON: {message}"),
    )
}

fn invalid_opencode_session_list_row_failure(request_id: &str, index: usize) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_opencode_session_list",
        format!("opencode session list row {index} is missing a non-empty id"),
    )
}

fn launch_provider_session_id(params: &SessionCaptureParams) -> Option<String> {
    params
        .launch
        .as_ref()
        .and_then(|launch| launch.session.as_ref())
        .and_then(|session| non_empty_string(session.provider_session_id.as_deref()))
}

fn bare_provider_session_id(params: &SessionCaptureParams) -> Option<String> {
    non_empty_string(params.session_id.as_deref())
}

fn pinned_target(params: &SessionCaptureParams) -> Option<String> {
    non_empty_string(params.pinned_target.as_deref())
}

fn start_bound_provider_session_id(params: &SessionCaptureParams) -> Option<String> {
    non_empty_string(params.start_bound_provider_session_id.as_deref())
}
