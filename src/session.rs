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

use crate::activity::ActivityTargets;
use crate::encoding::{encode_base64, sha256_hex};
use crate::envelope::{ProviderFailure, RequestEnvelope};
use crate::native_runtime::{self, NativeRuntimeContext};
use crate::opencode::{self, OpencodeExport, OpencodeExportError, OpencodeMessage};
use crate::runtime_selection::{append_resolved_activity_targets, resolve_runtime_selection};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::Path;

const CANONICAL_FORMAT: &str = "oulipoly.canonical_transcript/v1";
const NATIVE_FORMAT_ID: &str = "opencode.export/native-json";
const SOURCE_KIND: &str = "opencode.export";
const USER_OBSERVATION_PROJECTION: &str = "user_observation";
const MAX_OBSERVATION_BODY_TAIL: usize = 16;

#[derive(Deserialize)]
struct SessionParams {
    settings_id: String,
    session_id: Option<String>,
    turn_projection: Option<String>,
    body_tail_limit: Option<usize>,
}

#[derive(Deserialize)]
struct SessionCaptureParams {
    settings_id: String,
    session_id: Option<String>,
    launch: Option<SessionCaptureLaunch>,
    live_report: Option<SessionCaptureLiveReport>,
    pinned_target: Option<String>,
    start_bound_provider_session_id: Option<String>,
    #[serde(flatten)]
    extra: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionCaptureLiveReport {
    provider_session_id: String,
    invocation_uuid: String,
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

struct CapturedSession {
    provider_session_id: Option<String>,
    source: &'static str,
}

struct SessionIdentityCandidate {
    provider_session_id: String,
    source: &'static str,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Command {
    LocateTranscript,
    ReadTurns,
    Capture,
    Enumerate,
    Export,
    Replace,
}

pub(crate) struct SessionOutcome {
    pub result: Value,
}

impl SessionOutcome {
    fn new(result: Value) -> Self {
        Self { result }
    }
}

pub(crate) fn handle(
    command: Command,
    request: RequestEnvelope,
) -> Result<SessionOutcome, ProviderFailure> {
    let RequestEnvelope {
        host,
        params,
        request_id,
        ..
    } = request;
    match command {
        Command::LocateTranscript => {
            locate_transcript_params(params, &request_id).map(SessionOutcome::new)
        }
        Command::ReadTurns => {
            read_turns_params(&host, params, &request_id).map(SessionOutcome::new)
        }
        Command::Capture => capture_params(&host, params, &request_id).map(SessionOutcome::new),
        Command::Enumerate => {
            crate::session_enumeration::enumerate_params(&host, params, &request_id)
                .map(SessionOutcome::new)
        }
        Command::Export => export_params(&host, params, &request_id).map(SessionOutcome::new),
        Command::Replace => replace_params(params, &request_id).map(SessionOutcome::new),
    }
}

pub(crate) fn activity_targets(
    command: Command,
    host: &crate::envelope::HostContext,
    params: &Value,
    result: Option<&Value>,
    request_id: &str,
) -> ActivityTargets {
    let mut targets = ActivityTargets::default();
    let settings_id = params
        .get("settings_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if let Some(settings_id) = settings_id {
        targets.attempted("settings_record", settings_id, "params.settings_id");
    }
    if command == Command::Capture {
        append_capture_activity_candidates(&mut targets, params, request_id);
    } else if let Some(session_id) = params
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        targets.attempted("provider_session", session_id, "params.session_id");
    }
    let Some(result) = result else {
        return targets;
    };
    if let Some(settings_id) = settings_id.filter(|_| session_resolves_runtime(command, params)) {
        append_resolved_activity_targets(
            &mut targets,
            host,
            settings_id,
            request_id,
            "runtime_selection.settings_record",
        );
    }
    append_completed_session_activity_targets(&mut targets, command, params, result);
    targets
}

fn append_capture_activity_candidates(
    targets: &mut ActivityTargets,
    params: &Value,
    request_id: &str,
) {
    let Ok(params) = parse_capture_params(params.clone(), request_id) else {
        return;
    };
    let Ok(candidates) = session_identity_candidates(&params, request_id) else {
        return;
    };
    for candidate in candidates {
        targets.attempted(
            "provider_session",
            candidate.provider_session_id,
            format!("params.{}", candidate.source),
        );
    }
}

fn session_resolves_runtime(command: Command, params: &Value) -> bool {
    matches!(
        command,
        Command::ReadTurns | Command::Enumerate | Command::Export
    ) || (command == Command::Capture && params.get("live_report").is_some())
}

fn append_completed_session_activity_targets(
    targets: &mut ActivityTargets,
    command: Command,
    params: &Value,
    result: &Value,
) {
    if command == Command::Capture {
        if let Some(provider_session_id) = result
            .get("provider_session_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            let source = result
                .pointer("/state/source")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("provider_session_id");
            targets.resolved(
                "provider_session",
                provider_session_id,
                format!("result.{source}"),
            );
        }
        return;
    }
    if command == Command::Enumerate {
        if let Some(sessions) = result.get("sessions").and_then(Value::as_array) {
            for session in sessions {
                if let Some(provider_session_id) = session
                    .get("provider_session_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                {
                    targets.resolved(
                        "provider_session",
                        provider_session_id,
                        "result.sessions[].provider_session_id",
                    );
                }
            }
        }
        return;
    }
    if matches!(command, Command::ReadTurns | Command::Export) {
        if let Some(session_id) = params
            .get("session_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            targets.resolved("provider_session", session_id, "params.session_id");
        }
    }
}

pub fn locate_transcript_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    Ok(locate_transcript_result(params.session_id.as_deref()))
}

pub fn read_turns_params(
    host: &crate::envelope::HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    validate_turn_projection(&params, request_id)?;
    let session_id = required_session_id(&params, request_id)?;
    let native = export_native(host, &params.settings_id, &session_id, request_id)?;
    let turns = match params.turn_projection.as_deref() {
        Some(USER_OBSERVATION_PROJECTION) => {
            user_observation_turns(&native, &session_id, params.body_tail_limit.unwrap_or(4))?
        }
        _ => native_turns(&native, &session_id)?,
    };
    Ok(read_turns_result(turns))
}

pub fn capture_params(
    host: &crate::envelope::HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params = parse_capture_params(params, request_id)?;
    let captured = captured_session_id(&params, request_id)?;
    if let Some(captured) = capture_live_report(host, &params, request_id)? {
        return Ok(capture_result(
            Some(captured),
            "live_report.provider_session_id",
        ));
    }
    let provider_session_id = captured.provider_session_id;
    let source = captured.source;
    Ok(capture_result(provider_session_id, source))
}

fn capture_live_report(
    host: &crate::envelope::HostContext,
    params: &SessionCaptureParams,
    request_id: &str,
) -> Result<Option<String>, ProviderFailure> {
    let Some(report) = params.live_report.as_ref() else {
        return Ok(None);
    };
    let provider_session_id =
        non_empty_string(Some(&report.provider_session_id)).ok_or_else(|| {
            invalid_session_capture_params_failure(
                request_id,
                "live_report.provider_session_id must be non-empty",
            )
        })?;
    let invocation_uuid = non_empty_string(Some(&report.invocation_uuid)).ok_or_else(|| {
        invalid_session_capture_params_failure(
            request_id,
            "live_report.invocation_uuid must be non-empty",
        )
    })?;
    let envelope_invocation_uuid = params
        .extra
        .get("invocation_uuid")
        .and_then(Value::as_str)
        .and_then(|value| non_empty_string(Some(value)));
    if envelope_invocation_uuid.as_deref() != Some(invocation_uuid.as_str()) {
        return Err(invalid_session_capture_params_failure(
            request_id,
            "live_report.invocation_uuid must match invocation_uuid",
        ));
    }
    let native = export_native(host, &params.settings_id, &provider_session_id, request_id)?;
    validate_live_report_working_directory(&native, host.working_directory.as_deref(), request_id)?;
    Ok(Some(native.info.id))
}

fn validate_live_report_working_directory(
    native: &OpencodeExport,
    working_directory: Option<&str>,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let expected = non_empty_string(working_directory).ok_or_else(|| {
        invalid_session_capture_params_failure(
            request_id,
            "live reports require host.working_directory",
        )
    })?;
    let actual = non_empty_string(native.info.directory.as_deref()).ok_or_else(|| {
        invalid_session_capture_params_failure(
            request_id,
            "opencode export is missing info.directory for the live report",
        )
    })?;
    if Path::new(&actual) != Path::new(&expected) {
        return Err(invalid_session_capture_params_failure(
            request_id,
            format!(
                "live report workspace mismatch: opencode exported {actual}, runner requested {expected}"
            ),
        ));
    }
    Ok(())
}

pub fn export_params(
    host: &crate::envelope::HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params = parse_session_params(params, request_id)?;
    let session_id = required_session_id(&params, request_id)?;
    let native = export_native(host, &params.settings_id, &session_id, request_id)?;
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

fn validate_turn_projection(
    params: &SessionParams,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    match params.turn_projection.as_deref() {
        None => Ok(()),
        Some(USER_OBSERVATION_PROJECTION)
            if matches!(
                params.body_tail_limit,
                None | Some(1..=MAX_OBSERVATION_BODY_TAIL)
            ) =>
        {
            Ok(())
        }
        Some(USER_OBSERVATION_PROJECTION) => Err(invalid_session_params_message_failure(
            request_id,
            format!("body_tail_limit must be between 1 and {MAX_OBSERVATION_BODY_TAIL}"),
        )),
        Some(projection) => Err(invalid_session_params_message_failure(
            request_id,
            format!("unsupported turn_projection: {projection}"),
        )),
    }
}

fn export_native(
    host: &crate::envelope::HostContext,
    settings_id: &str,
    session_id: &str,
    request_id: &str,
) -> Result<OpencodeExport, ProviderFailure> {
    let runtime = session_runtime(host, settings_id, request_id)?;
    let native = opencode::export(session_id, &runtime)
        .map_err(|err| export_failure(request_id, session_id, err))?;
    validate_export_session_id(&native, session_id, request_id)?;
    validate_export_message_sessions(&native, session_id, request_id)?;
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
        OpencodeExportError::OutputTooLarge {
            stream,
            maximum_bytes,
        } => ProviderFailure::invalid_request(
            request_id,
            "opencode_export_capacity_exceeded",
            format!(
                "opencode export {stream} for {session_id} exceeds the supported {maximum_bytes}-byte bound"
            ),
        ),
        OpencodeExportError::TimedOut => ProviderFailure::invalid_request(
            request_id,
            "opencode_export_timeout",
            format!("opencode export timed out for {session_id}"),
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

fn user_observation_turns(
    native: &OpencodeExport,
    session_id: &str,
    body_tail_limit: usize,
) -> Result<Vec<Value>, ProviderFailure> {
    let messages = native
        .messages
        .iter()
        .filter(|message| message.info.role == "user")
        .collect::<Vec<_>>();
    let body_start = messages.len().saturating_sub(body_tail_limit);
    messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| user_observation_turn(message, session_id, index >= body_start))
        .collect()
}

fn user_observation_turn(
    message: &OpencodeMessage,
    session_id: &str,
    include_body: bool,
) -> Result<Value, ProviderFailure> {
    let mut turn = json!({
        "session_id": session_id,
        "turn_id": stable_turn_id(message, session_id),
        "role": message.info.role,
        "timestamp": provider_turn_timestamp(message),
    });
    if include_body {
        turn["body"] = Value::Array(text_parts(message));
    }
    Ok(turn)
}

fn native_turn(message: &OpencodeMessage, session_id: &str) -> Result<Value, ProviderFailure> {
    let model_identity = message.info.model_identity();
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
            "provider_id": model_identity.provider_id(),
            "model_id": model_identity.model_id(),
            "variant": model_identity.variant(),
            "parts": message.parts,
        },
    }))
}

fn provider_turn_timestamp(message: &OpencodeMessage) -> String {
    message_time_millis(message)
        .and_then(|milliseconds| i64::try_from(milliseconds).ok())
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn rotation_boundary_timestamp(native: &OpencodeExport) -> Option<String> {
    native
        .messages
        .iter()
        .map(|message| message_time_millis(message).unwrap_or_default())
        .max()
        .and_then(|milliseconds| i64::try_from(milliseconds).ok())
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn message_time_millis(message: &OpencodeMessage) -> Option<u64> {
    message
        .info
        .time
        .as_ref()
        .and_then(|time| time.created.or(time.completed))
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
    let model_identity = message.info.model_identity();
    Ok(json!({
        "body": text_parts(message),
        "id": stable_turn_id(message, session_id),
        "metadata": {
            "native_message_id": message.info.id,
            "native_session_id": message.info.session_id,
            "native_title": title,
            "provider_id": model_identity.provider_id(),
            "model_id": model_identity.model_id(),
            "variant": model_identity.variant(),
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
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .map(|text| {
            json!({
                "type": "text",
                "text": text,
            })
        })
        .collect()
}

fn captured_session_id(
    params: &SessionCaptureParams,
    request_id: &str,
) -> Result<CapturedSession, ProviderFailure> {
    let candidates = session_identity_candidates(params, request_id)?;
    validate_session_identity_candidates(&candidates, request_id)?;
    Ok(candidates
        .into_iter()
        .next()
        .map(|candidate| CapturedSession {
            provider_session_id: Some(candidate.provider_session_id),
            source: candidate.source,
        })
        .unwrap_or(CapturedSession {
            provider_session_id: None,
            source: "none",
        }))
}

fn session_identity_candidates(
    params: &SessionCaptureParams,
    request_id: &str,
) -> Result<Vec<SessionIdentityCandidate>, ProviderFailure> {
    let mut candidates = Vec::new();
    if let Some(report) = params.live_report.as_ref() {
        let provider_session_id =
            non_empty_string(Some(&report.provider_session_id)).ok_or_else(|| {
                invalid_session_capture_params_failure(
                    request_id,
                    "live_report.provider_session_id must be non-empty",
                )
            })?;
        candidates.push(session_identity_candidate(
            provider_session_id,
            "live_report.provider_session_id",
        ));
    }
    push_session_identity_candidate(
        &mut candidates,
        launch_provider_session_id(params),
        "launch.session.provider_session_id",
    );
    push_session_identity_candidate(
        &mut candidates,
        bare_provider_session_id(params),
        "session_id",
    );
    push_session_identity_candidate(&mut candidates, pinned_target(params), "pinned_target");
    push_session_identity_candidate(
        &mut candidates,
        start_bound_provider_session_id(params),
        "start_bound_provider_session_id",
    );
    Ok(candidates)
}

fn push_session_identity_candidate(
    candidates: &mut Vec<SessionIdentityCandidate>,
    provider_session_id: Option<String>,
    source: &'static str,
) {
    if let Some(provider_session_id) = provider_session_id {
        candidates.push(session_identity_candidate(provider_session_id, source));
    }
}

fn session_identity_candidate(
    provider_session_id: String,
    source: &'static str,
) -> SessionIdentityCandidate {
    SessionIdentityCandidate {
        provider_session_id,
        source,
    }
}

fn validate_session_identity_candidates(
    candidates: &[SessionIdentityCandidate],
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let Some(expected) = candidates.first() else {
        return Ok(());
    };
    if let Some(conflict) = candidates
        .iter()
        .skip(1)
        .find(|candidate| candidate.provider_session_id != expected.provider_session_id)
    {
        return Err(invalid_session_capture_params_failure(
            request_id,
            format!(
                "conflicting session evidence: {} disagrees with {}",
                conflict.source, expected.source
            ),
        ));
    }
    Ok(())
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
    invalid_session_params_message_failure(request_id, err)
}

fn invalid_session_params_message_failure(
    request_id: &str,
    err: impl std::fmt::Display,
) -> ProviderFailure {
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

fn missing_session_id_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "missing_session_id",
        "session params require non-empty session_id",
    )
}

pub(crate) fn session_runtime(
    host: &crate::envelope::HostContext,
    settings_id: &str,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    // Session storage is account-scoped; the settings record's model binding
    // deliberately does not constrain read/enumerate/export operations.
    let selection = resolve_runtime_selection(host, settings_id, request_id)?;
    native_runtime::resolve_for_account(host, selection.account, request_id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_observation_turn_omits_unselected_body() {
        let message: OpencodeMessage = serde_json::from_value(json!({
            "info": {
                "id": "message-1",
                "role": "user",
                "sessionID": "session-1",
                "time": { "created": 1 }
            },
            "parts": [{ "type": "text", "text": "body" }]
        }))
        .expect("user message");

        let without_body =
            user_observation_turn(&message, "session-1", false).expect("observation without body");
        let with_body =
            user_observation_turn(&message, "session-1", true).expect("observation with body");

        assert!(!without_body
            .as_object()
            .expect("observation object")
            .contains_key("body"));
        assert_eq!(with_body["body"][0]["text"], "body");
    }
}
