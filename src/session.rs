//! Declared roles: orchestration, mapper
//! intrinsic_surface_declarations:
//!   - component: src/session.rs
//!     role: intrinsic-surface
//!     Domain: canonical transcript surface
//!     Owns:
//!       - opencode export to provider session responses
//!       - canonical transcript byte serialization
//!       - session replace unsupported boundary

use crate::account::profile_for_settings_id;
use crate::encoding::{encode_base64, sha256_hex};
use crate::envelope::ProviderFailure;
use crate::opencode::{self, OpencodeExport, OpencodeExportError, OpencodeMessage};
use serde::Deserialize;
use serde_json::{json, Value};

const CANONICAL_FORMAT: &str = "oulipoly.canonical_transcript/v1";
const NATIVE_FORMAT_ID: &str = "opencode.export/native-json";
const SOURCE_KIND: &str = "opencode.export";

#[derive(Deserialize)]
struct SessionParams {
    settings_id: String,
    session_id: Option<String>,
}

pub fn locate_transcript_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    Ok(json!({
        "located": false,
        "format_id": NATIVE_FORMAT_ID,
        "source_id": source_id(params.session_id.as_deref()),
        "require_existing_observed": false,
    }))
}

pub fn read_turns_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    let session_id = required_session_id(&params, request_id)?;
    let native = export_native(&params.settings_id, &session_id, request_id)?;
    let turns = native_turns(&native, &session_id)?;
    Ok(json!({
        "turn_count": turns.len(),
        "turns": turns,
        "complete": true,
    }))
}

pub fn capture_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    ensure_params_object(&params, request_id)?;
    let provider_session_id = captured_session_id(&params);
    Ok(json!({
        "artifacts": capture_artifacts(provider_session_id.as_deref()),
        "provider_session_id": provider_session_id,
        "state": {
            "format_id": NATIVE_FORMAT_ID,
            "source_id": source_id(captured_session_id(&params).as_deref()),
            "source": "launch.sessionID",
        },
    }))
}

pub fn export_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    let session_id = required_session_id(&params, request_id)?;
    let native = export_native(&params.settings_id, &session_id, request_id)?;
    let records = canonical_records(&native, &session_id)?;
    let bytes = canonical_jsonl(&records);
    Ok(json!({
        "canonical_format": CANONICAL_FORMAT,
        "data_base64": encode_base64(&bytes),
        "sha256": sha256_hex(&bytes),
        "turn_count": records.len(),
    }))
}

pub fn replace_params(_params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    Err(ProviderFailure::unsupported(
        request_id,
        "session_replace_unsupported",
        "opencode does not provide a stable transcript import or replace API",
    ))
}

fn parse_session_params(params: Value, request_id: &str) -> Result<SessionParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_session_params",
            format!("session params are invalid: {err}"),
        )
    })
}

fn ensure_params_object(params: &Value, request_id: &str) -> Result<(), ProviderFailure> {
    if params.is_object() {
        return Ok(());
    }
    Err(ProviderFailure::invalid_request(
        request_id,
        "invalid_session_params",
        "session params must be an object",
    ))
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
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "missing_session_id",
                "session params require non-empty session_id",
            )
        })
}

fn export_native(
    settings_id: &str,
    session_id: &str,
    request_id: &str,
) -> Result<OpencodeExport, ProviderFailure> {
    let account = profile_for_settings_id(settings_id).ok_or_else(|| {
        ProviderFailure::invalid_request(
            request_id,
            "unknown_settings_id",
            format!("unknown opencode settings_id: {settings_id}"),
        )
    })?;
    let native = opencode::export(session_id, account)
        .map_err(|err| export_failure(request_id, session_id, err))?;
    validate_export_session_id(&native, session_id, request_id)?;
    Ok(native)
}

fn validate_export_session_id(
    native: &OpencodeExport,
    expected: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if native.info.id == expected {
        return Ok(());
    }
    Err(ProviderFailure::invalid_request(
        request_id,
        "session_export_id_mismatch",
        format!(
            "opencode export returned session_id {} instead of {expected}",
            native.info.id
        ),
    ))
}

fn export_failure(request_id: &str, session_id: &str, err: OpencodeExportError) -> ProviderFailure {
    match err {
        OpencodeExportError::Spawn(message) => ProviderFailure::invalid_request(
            request_id,
            "opencode_export_unavailable",
            format!("failed to run opencode export for {session_id}: {message}"),
        ),
        OpencodeExportError::Failed { status, stderr } => ProviderFailure::invalid_request(
            request_id,
            "session_export_failed",
            format!(
                "opencode export failed for {session_id} with status {:?}: {}",
                status,
                stderr.trim()
            ),
        ),
        OpencodeExportError::InvalidJson(message) => ProviderFailure::invalid_request(
            request_id,
            "invalid_opencode_export",
            format!("opencode export output was not valid native JSON: {message}"),
        ),
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
        "id": stable_turn_id(message, session_id),
        "role": message.info.role,
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
    message
        .parts
        .iter()
        .filter_map(text_part)
        .collect::<Vec<_>>()
}

fn text_part(part: &Value) -> Option<Value> {
    let text = part.get("text").and_then(Value::as_str)?;
    Some(json!({
        "type": "text",
        "text": text,
    }))
}

fn captured_session_id(params: &Value) -> Option<String> {
    string_at(params, &["launch", "session", "provider_session_id"])
        .or_else(|| string_at(params, &["launch", "sessionID"]))
        .or_else(|| string_at(params, &["launch", "session_id"]))
        .or_else(|| string_at(params, &["evidence", "provider_session_id"]))
        .or_else(|| string_at(params, &["evidence", "sessionID"]))
        .or_else(|| string_at(params, &["session_id"]))
}

fn string_at(value: &Value, path: &[&str]) -> Option<String> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
        .and_then(Value::as_str)
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
