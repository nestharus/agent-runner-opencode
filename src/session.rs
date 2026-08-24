//! Declared roles: orchestration, mapper, parser, validator, formatter, filter, accessor, predicate
//! intrinsic_surface_declarations:
//!   - component: src/session.rs
//!     role: intrinsic-surface
//!     Domain: canonical transcript surface
//!     Owns:
//!       - opencode export to provider session responses
//!       - canonical transcript byte serialization
//!       - session replace unsupported boundary
//!   - component: src/session.rs
//!     role: intrinsic-surface
//!     Domain: durable session-enumeration snapshot custody
//!     Owns:
//!       - bounded native session population capture
//!       - request-bound immutable snapshot and initial-page replay
//!       - cursor identity, advancement, retention, and terminal claims
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
use crate::durable_fs;
use crate::encoding::{encode_base64, now_unix_ms, sha256_hex};
use crate::envelope::{ProviderFailure, RequestEnvelope};
use crate::native_runtime::{self, NativeRuntimeContext};
use crate::opencode::{
    self, OpencodeExport, OpencodeExportError, OpencodeMessage, OpencodeSessionDirectory,
    OpencodeSessionListError, OpencodeSessionListRow,
};
use crate::operation_bounds;
use crate::path_guard;
use crate::runtime_selection::{append_resolved_activity_targets, resolve_runtime_selection};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

const CANONICAL_FORMAT: &str = "oulipoly.canonical_transcript/v1";
const NATIVE_FORMAT_ID: &str = "opencode.export/native-json";
const SOURCE_KIND: &str = "opencode.export";
const USER_OBSERVATION_PROJECTION: &str = "user_observation";
const MAX_OBSERVATION_BODY_TAIL: usize = 16;
const SESSION_ENUMERATION_SNAPSHOT_DIR: &str =
    "provider-state/opencode/session-enumeration-snapshots";
const SESSION_ENUMERATION_SNAPSHOT_SCHEMA_VERSION: u32 = 4;
const SESSION_ENUMERATION_SNAPSHOT_TTL_MS: u64 = 15 * 60 * 1_000;
const SESSION_ENUMERATION_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ENUMERATED_SESSIONS: usize = 256;
const MAX_ENUMERATION_PAGE_SIZE: usize = 256;
const MAX_ENUMERATION_SNAPSHOTS: usize = 32;
const MAX_ENUMERATION_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
const MAX_ENUMERATION_ENTRY_BYTES: usize = 64 * 1024;
const MAX_ENUMERATION_MANIFEST_BYTES: usize = 32 * 1024;
const MAX_ENUMERATION_WARNINGS_BYTES: usize = 256 * 1024;
const MAX_ENUMERATION_WARNINGS: usize = 32;

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

#[derive(Deserialize, Serialize)]
struct EnumerationSnapshotManifest {
    schema_version: u32,
    snapshot_id: String,
    #[serde(default)]
    snapshot_instance_sha256: String,
    identity_sha256: String,
    total_sessions: usize,
    created_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    #[serde(default)]
    initial_request_sha256: String,
    #[serde(default)]
    initial_page_end: usize,
    #[serde(default)]
    initial_warnings_sha256: String,
    #[serde(default)]
    row_offsets: Vec<u64>,
    #[serde(default)]
    row_sha256: Vec<String>,
    #[serde(default)]
    next_cursor_offset: usize,
    #[serde(default)]
    last_page_claim_request_sha256: Option<String>,
    #[serde(default)]
    last_page_claim_start: Option<usize>,
    #[serde(default)]
    last_page_claim_end: Option<usize>,
    #[serde(default)]
    terminal_claim_request_sha256: Option<String>,
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
        Command::Enumerate => enumerate_params(&host, params, &request_id),
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

pub(crate) fn enumerate_params(
    host: &crate::envelope::HostContext,
    params: Value,
    request_id: &str,
) -> Result<SessionOutcome, ProviderFailure> {
    let params = parse_enumerate_params(params, request_id)?;
    validate_enumerate_params(&params, request_id)?;
    if let Some(cursor) = params.cursor.as_deref() {
        return load_enumeration_snapshot_page(host, &params, cursor, request_id)
            .map(enumerate_result);
    }
    if let Some(page) = replay_claimed_initial_snapshot_page(host, &params, request_id)? {
        return Ok(enumerate_result(page));
    }
    let native = enumerate_native(host, &params.settings_id, request_id)?;
    let page = enumerate_sessions(host, native, &params, request_id)?;
    Ok(enumerate_result(page))
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
    if !matches!(params.limit, None | Some(1..=MAX_ENUMERATION_PAGE_SIZE)) {
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

fn enumerate_native(
    host: &crate::envelope::HostContext,
    settings_id: &str,
    request_id: &str,
) -> Result<Vec<OpencodeSessionListRow>, ProviderFailure> {
    let runtime = session_runtime(host, settings_id, request_id)?;
    let timeout =
        operation_bounds::remaining_timeout(host.deadline_unix_ms, Duration::from_secs(20))
            .ok_or_else(|| session_list_timeout_failure(request_id))?;
    let sessions = opencode::session_list_with_timeout(
        Some(MAX_ENUMERATED_SESSIONS.saturating_add(1)),
        &runtime,
        timeout,
    )
    .map_err(|err| session_list_failure(request_id, err))?;
    if sessions.len() > MAX_ENUMERATED_SESSIONS {
        return Err(session_population_capacity_failure(request_id));
    }
    Ok(sessions)
}

struct EnumeratePage {
    sessions: Vec<Value>,
    warnings: Vec<String>,
    complete: bool,
    next_cursor: Option<String>,
}

fn enumerate_sessions(
    host: &crate::envelope::HostContext,
    native: Vec<OpencodeSessionListRow>,
    params: &SessionEnumerateParams,
    request_id: &str,
) -> Result<EnumeratePage, ProviderFailure> {
    let mut warnings = Vec::new();
    let mut sessions = Vec::new();
    for (index, entry) in native.iter().enumerate() {
        if let Some(session) = enumerate_session_entry(index, entry, params, &mut warnings) {
            sessions.push(session);
        }
    }
    paginate_sessions(host, sessions, params, warnings, request_id)
}

fn enumerate_session_entry(
    index: usize,
    entry: &OpencodeSessionListRow,
    params: &SessionEnumerateParams,
    warnings: &mut Vec<String>,
) -> Option<Value> {
    let provider_session_id = &entry.provider_session_id;
    let created_unix_ms = entry.created_unix_ms;
    let updated_unix_ms = entry.updated_unix_ms;
    if !matches_since_filter(created_unix_ms, updated_unix_ms, params.since_unix_ms) {
        return None;
    }
    if params.since_unix_ms.is_some() && created_unix_ms.is_none() && updated_unix_ms.is_none() {
        warnings.push(format!(
            "session {provider_session_id} has no timestamp; since_unix_ms filter could not be applied"
        ));
    }
    let include_cwd = params.include_cwd.unwrap_or(true);
    let include_turn_count = params.include_turn_count.unwrap_or(true);
    let cwd = include_cwd.then(|| enumerate_cwd(entry, index, warnings));
    let turn_count = include_turn_count.then_some(entry.turn_count);
    Some(json!({
        "provider_session_id": provider_session_id,
        "title": entry.title,
        "cwd": cwd.flatten(),
        "created_unix_ms": created_unix_ms,
        "updated_unix_ms": updated_unix_ms,
        "turn_count": turn_count.flatten(),
        "source": {
            "kind": "opencode.session_list",
            "detail": "session list --format json"
        }
    }))
}

fn paginate_sessions(
    host: &crate::envelope::HostContext,
    sessions: Vec<Value>,
    params: &SessionEnumerateParams,
    mut warnings: Vec<String>,
    request_id: &str,
) -> Result<EnumeratePage, ProviderFailure> {
    if warnings.len() > MAX_ENUMERATION_WARNINGS {
        let omitted = warnings.len() - MAX_ENUMERATION_WARNINGS;
        warnings.truncate(MAX_ENUMERATION_WARNINGS);
        warnings.push(format!(
            "{omitted} additional session enumeration warnings were omitted"
        ));
    }
    let limit = params.limit.unwrap_or(MAX_ENUMERATION_PAGE_SIZE);
    let end = limit.min(sessions.len());
    persist_enumeration_snapshot(host, sessions, params, warnings, end, request_id)
}

fn enumeration_request_identity(params: &SessionEnumerateParams) -> String {
    sha256_hex(
        json!({
            "settings_id": params.settings_id,
            "include_cwd": params.include_cwd.unwrap_or(true),
            "include_turn_count": params.include_turn_count.unwrap_or(true),
            "since_unix_ms": params.since_unix_ms,
        })
        .to_string()
        .as_bytes(),
    )
}

fn replay_claimed_initial_snapshot_page(
    host: &crate::envelope::HostContext,
    params: &SessionEnumerateParams,
    request_id: &str,
) -> Result<Option<EnumeratePage>, ProviderFailure> {
    if host
        .data_root
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        return Ok(None);
    }
    let root = enumeration_snapshot_root(host, request_id)?;
    let _lock = acquire_enumeration_snapshot_lock(host, &root, request_id)?;
    prune_enumeration_snapshots(host, &root, request_id)?;
    let initial_request_sha256 = enumeration_page_request_sha256(params, request_id);
    claimed_initial_snapshot_page_locked(host, &root, params, request_id, &initial_request_sha256)
}

fn claimed_initial_snapshot_page_locked(
    host: &crate::envelope::HostContext,
    root: &Path,
    params: &SessionEnumerateParams,
    request_id: &str,
    initial_request_sha256: &str,
) -> Result<Option<EnumeratePage>, ProviderFailure> {
    let snapshot_id = initial_enumeration_snapshot_id(initial_request_sha256);
    let snapshot_root =
        confined_enumeration_snapshot_target(host, &root.join(&snapshot_id), request_id)?;
    if !snapshot_root.exists() {
        return Ok(None);
    }
    let manifest = snapshot_manifest(&snapshot_root, request_id)?;
    let identity_sha256 = enumeration_request_identity(params);
    if manifest.schema_version != SESSION_ENUMERATION_SNAPSHOT_SCHEMA_VERSION
        || manifest.snapshot_id != snapshot_id
        || !is_sha256_hex(&manifest.snapshot_instance_sha256)
        || manifest.identity_sha256 != identity_sha256
        || manifest.total_sessions > MAX_ENUMERATED_SESSIONS
        || manifest.expires_at_unix_ms < now_unix_ms()
        || manifest.initial_request_sha256 != initial_request_sha256
        || manifest.initial_page_end > manifest.total_sessions
        || !is_sha256_hex(&manifest.initial_warnings_sha256)
        || !enumeration_snapshot_rows_are_valid(&manifest)
    {
        return Err(session_snapshot_failure(
            request_id,
            "the durable initial request claim is invalid",
        ));
    }
    let complete = manifest.initial_page_end == manifest.total_sessions;
    match manifest.terminal_claim_request_sha256.as_deref() {
        Some(owner) if complete && owner == initial_request_sha256 => {}
        Some(_) => return Err(session_snapshot_terminal_handoff_failure(request_id)),
        None if complete => {
            return Err(session_snapshot_failure(
                request_id,
                "the durable terminal initial request claim has no handoff owner",
            ))
        }
        None => {}
    }
    let exact_initial_retry = manifest.next_cursor_offset == manifest.initial_page_end
        && manifest.last_page_claim_request_sha256.as_deref() == Some(initial_request_sha256)
        && manifest.last_page_claim_start == Some(0)
        && manifest.last_page_claim_end == Some(manifest.initial_page_end);
    if !exact_initial_retry {
        return Err(session_snapshot_cursor_superseded_failure(request_id));
    }
    let warnings_bytes = read_enumeration_snapshot_file(
        &snapshot_root.join("warnings.json"),
        MAX_ENUMERATION_WARNINGS_BYTES,
    )
    .map_err(|error| session_snapshot_failure(request_id, error))?;
    if sha256_hex(&warnings_bytes) != manifest.initial_warnings_sha256 {
        return Err(session_snapshot_failure(
            request_id,
            "the durable initial warning projection does not match its manifest",
        ));
    }
    let warnings: Vec<String> = serde_json::from_slice(&warnings_bytes)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    if warnings.len() > MAX_ENUMERATION_WARNINGS.saturating_add(1) {
        return Err(session_snapshot_failure(
            request_id,
            "the durable initial warning projection exceeds its count bound",
        ));
    }
    let sessions = read_enumeration_snapshot_rows(
        host,
        &snapshot_root,
        &manifest,
        0,
        manifest.initial_page_end,
        request_id,
    )?;
    Ok(Some(EnumeratePage {
        sessions,
        warnings,
        complete,
        next_cursor: (!complete).then(|| {
            enumeration_cursor(
                &snapshot_id,
                &manifest.snapshot_instance_sha256,
                manifest.initial_page_end,
                manifest.total_sessions,
                &identity_sha256,
            )
        }),
    }))
}

fn initial_enumeration_snapshot_id(initial_request_sha256: &str) -> String {
    sha256_hex(
        [
            b"agent-runner-opencode.session-enumeration.initial.v1".as_slice(),
            &[0],
            initial_request_sha256.as_bytes(),
        ]
        .concat()
        .as_slice(),
    )
}

fn persist_enumeration_snapshot(
    host: &crate::envelope::HostContext,
    sessions: Vec<Value>,
    params: &SessionEnumerateParams,
    warnings: Vec<String>,
    first_page_end: usize,
    request_id: &str,
) -> Result<EnumeratePage, ProviderFailure> {
    let identity_sha256 = enumeration_request_identity(params);
    let initial_request_sha256 = enumeration_page_request_sha256(params, request_id);
    let mut encoded_rows = Vec::with_capacity(sessions.len());
    let mut encoded_bytes = 0_usize;
    for session in &sessions {
        let bytes = serde_json::to_vec(session)
            .map_err(|error| session_snapshot_failure(request_id, error))?;
        if bytes.len() > MAX_ENUMERATION_ENTRY_BYTES {
            return Err(session_snapshot_capacity_failure(
                request_id,
                "one projected session row exceeds its supported encoded-size bound",
            ));
        }
        encoded_bytes = encoded_bytes.saturating_add(bytes.len());
        if encoded_bytes > MAX_ENUMERATION_SNAPSHOT_BYTES {
            return Err(session_snapshot_capacity_failure(
                request_id,
                "the projected session snapshot exceeds its supported encoded-size bound",
            ));
        }
        encoded_rows.push(bytes);
    }
    let mut rows = Vec::with_capacity(encoded_bytes);
    let mut row_offsets = Vec::with_capacity(encoded_rows.len().saturating_add(1));
    let mut row_sha256 = Vec::with_capacity(encoded_rows.len());
    row_offsets.push(0);
    for encoded_row in &encoded_rows {
        rows.extend_from_slice(encoded_row);
        row_sha256.push(sha256_hex(encoded_row));
        row_offsets.push(rows.len() as u64);
    }
    let encoded_warnings = serde_json::to_vec(&warnings)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    if encoded_warnings.len() > MAX_ENUMERATION_WARNINGS_BYTES
        || rows.len().saturating_add(encoded_warnings.len()) > MAX_ENUMERATION_SNAPSHOT_BYTES
    {
        return Err(session_snapshot_capacity_failure(
            request_id,
            "the projected session warnings exceed the supported snapshot bound",
        ));
    }
    let snapshot_id = initial_enumeration_snapshot_id(&initial_request_sha256);
    let root = enumeration_snapshot_root(host, request_id)?;
    let _lock = acquire_enumeration_snapshot_lock(host, &root, request_id)?;
    let retained = prune_enumeration_snapshots(host, &root, request_id)?;
    if let Some(page) = claimed_initial_snapshot_page_locked(
        host,
        &root,
        params,
        request_id,
        &initial_request_sha256,
    )? {
        return Ok(page);
    }
    let snapshot_root =
        confined_enumeration_snapshot_target(host, &root.join(&snapshot_id), request_id)?;
    if snapshot_root.exists() {
        fs::remove_dir_all(&snapshot_root)
            .map_err(|error| session_snapshot_failure(request_id, error))?;
        durable_fs::sync_directory(&root)
            .map_err(|error| session_snapshot_failure(request_id, error))?;
    } else if retained >= MAX_ENUMERATION_SNAPSHOTS {
        return Err(session_snapshot_capacity_failure(
            request_id,
            "the active session enumeration snapshot limit is reached; retry after an earlier cursor expires",
        ));
    }
    durable_fs::create_private_directories(&snapshot_root)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    stage_enumeration_snapshot_file(&snapshot_root.join("rows.bin"), &rows)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    stage_enumeration_snapshot_file(&snapshot_root.join("warnings.json"), &encoded_warnings)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    durable_fs::sync_directory(&snapshot_root)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    let created_at_unix_ms = now_unix_ms();
    let snapshot_instance_sha256 = new_enumeration_snapshot_instance_sha256(&snapshot_root)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    let complete = first_page_end == sessions.len();
    let manifest = EnumerationSnapshotManifest {
        schema_version: SESSION_ENUMERATION_SNAPSHOT_SCHEMA_VERSION,
        snapshot_id: snapshot_id.clone(),
        snapshot_instance_sha256,
        identity_sha256: identity_sha256.clone(),
        total_sessions: sessions.len(),
        created_at_unix_ms,
        expires_at_unix_ms: created_at_unix_ms.saturating_add(SESSION_ENUMERATION_SNAPSHOT_TTL_MS),
        initial_request_sha256: initial_request_sha256.clone(),
        initial_page_end: first_page_end,
        initial_warnings_sha256: sha256_hex(&encoded_warnings),
        row_offsets,
        row_sha256,
        next_cursor_offset: first_page_end,
        last_page_claim_request_sha256: Some(initial_request_sha256.clone()),
        last_page_claim_start: Some(0),
        last_page_claim_end: Some(first_page_end),
        terminal_claim_request_sha256: complete.then_some(initial_request_sha256.clone()),
    };
    let bytes = encode_enumeration_snapshot_manifest(&manifest, request_id)?;
    write_enumeration_snapshot_file(&snapshot_root.join("manifest.json"), &bytes)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    Ok(EnumeratePage {
        sessions: sessions.into_iter().take(first_page_end).collect(),
        warnings,
        complete,
        next_cursor: (!complete).then(|| {
            enumeration_cursor(
                &snapshot_id,
                &manifest.snapshot_instance_sha256,
                first_page_end,
                manifest.total_sessions,
                &identity_sha256,
            )
        }),
    })
}

fn load_enumeration_snapshot_page(
    host: &crate::envelope::HostContext,
    params: &SessionEnumerateParams,
    cursor: &str,
    request_id: &str,
) -> Result<EnumeratePage, ProviderFailure> {
    let (snapshot_id, cursor_snapshot_instance, start, total, cursor_identity) =
        parse_enumeration_cursor(cursor, request_id)?;
    let expected_identity = enumeration_request_identity(params);
    if cursor_identity != expected_identity {
        return Err(invalid_session_enumerate_cursor_failure(
            request_id,
            "cursor does not match the requested settings and filters",
        ));
    }
    let root = enumeration_snapshot_root(host, request_id)?;
    let _lock = acquire_enumeration_snapshot_lock(host, &root, request_id)?;
    let snapshot_root =
        confined_enumeration_snapshot_target(host, &root.join(&snapshot_id), request_id)?;
    let mut manifest = snapshot_manifest(&snapshot_root, request_id).map_err(|_| {
        invalid_session_enumerate_cursor_failure(request_id, "snapshot is missing or invalid")
    })?;
    if manifest.schema_version != SESSION_ENUMERATION_SNAPSHOT_SCHEMA_VERSION
        || manifest.snapshot_id != snapshot_id
        || !is_sha256_hex(&manifest.snapshot_instance_sha256)
        || manifest.snapshot_instance_sha256 != cursor_snapshot_instance
        || manifest.identity_sha256 != expected_identity
        || manifest.total_sessions != total
        || manifest.expires_at_unix_ms < now_unix_ms()
        || !enumeration_snapshot_rows_are_valid(&manifest)
        || start >= total
    {
        return Err(invalid_session_enumerate_cursor_failure(
            request_id,
            "snapshot identity, lifetime, or offset is invalid",
        ));
    }
    let end = start
        .saturating_add(params.limit.unwrap_or(MAX_ENUMERATION_PAGE_SIZE))
        .min(total);
    let request_sha256 = enumeration_page_request_sha256(params, request_id);
    let exact_page_retry = manifest.last_page_claim_request_sha256.as_deref()
        == Some(request_sha256.as_str())
        && manifest.last_page_claim_start == Some(start)
        && manifest.last_page_claim_end == Some(end)
        && manifest.next_cursor_offset == end;
    let advances_cursor = manifest.next_cursor_offset == start;
    if let Some(owner) = manifest.terminal_claim_request_sha256.as_deref() {
        if owner != request_sha256.as_str() || !exact_page_retry {
            return Err(session_snapshot_terminal_handoff_failure(request_id));
        }
    } else if !advances_cursor && !exact_page_retry {
        return Err(session_snapshot_cursor_superseded_failure(request_id));
    }
    let sessions =
        read_enumeration_snapshot_rows(host, &snapshot_root, &manifest, start, end, request_id)?;
    let complete = end == total;
    if manifest.terminal_claim_request_sha256.is_some() && !complete {
        return Err(session_snapshot_terminal_handoff_failure(request_id));
    }
    if advances_cursor {
        manifest.next_cursor_offset = end;
        manifest.last_page_claim_request_sha256 = Some(request_sha256.clone());
        manifest.last_page_claim_start = Some(start);
        manifest.last_page_claim_end = Some(end);
    }
    if complete {
        match manifest.terminal_claim_request_sha256.as_deref() {
            Some(owner) if owner != request_sha256.as_str() => {
                return Err(session_snapshot_terminal_handoff_failure(request_id));
            }
            Some(_) => {}
            None => {
                manifest.terminal_claim_request_sha256 = Some(request_sha256.clone());
            }
        }
    }
    if advances_cursor {
        let bytes = encode_enumeration_snapshot_manifest(&manifest, request_id)?;
        write_enumeration_snapshot_file(&snapshot_root.join("manifest.json"), &bytes)
            .map_err(|error| session_snapshot_failure(request_id, error))?;
    }
    Ok(EnumeratePage {
        sessions,
        warnings: Vec::new(),
        complete,
        next_cursor: (!complete).then(|| {
            enumeration_cursor(
                &snapshot_id,
                &manifest.snapshot_instance_sha256,
                end,
                total,
                &expected_identity,
            )
        }),
    })
}

fn read_enumeration_snapshot_rows(
    host: &crate::envelope::HostContext,
    snapshot_root: &Path,
    manifest: &EnumerationSnapshotManifest,
    start: usize,
    end: usize,
    request_id: &str,
) -> Result<Vec<Value>, ProviderFailure> {
    if !enumeration_snapshot_rows_are_valid(manifest)
        || end > manifest.total_sessions
        || start > end
    {
        return Err(session_snapshot_failure(
            request_id,
            "the durable session row population does not match its manifest",
        ));
    }
    let path =
        confined_enumeration_snapshot_target(host, &snapshot_root.join("rows.bin"), request_id)?;
    let mut file =
        fs::File::open(path).map_err(|error| session_snapshot_failure(request_id, error))?;
    let expected_bytes = *manifest
        .row_offsets
        .last()
        .ok_or_else(|| session_snapshot_failure(request_id, "missing session row offsets"))?;
    let observed_bytes = file
        .metadata()
        .map_err(|error| session_snapshot_failure(request_id, error))?
        .len();
    if observed_bytes != expected_bytes {
        return Err(session_snapshot_failure(
            request_id,
            "the durable session row bytes do not match their manifest",
        ));
    }
    let page_start = manifest.row_offsets[start];
    let page_end = manifest.row_offsets[end];
    let page_length = usize::try_from(page_end.saturating_sub(page_start))
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    if page_length > MAX_ENUMERATION_SNAPSHOT_BYTES {
        return Err(session_snapshot_capacity_failure(
            request_id,
            "the requested durable session page exceeds its supported encoded-size bound",
        ));
    }
    file.seek(SeekFrom::Start(page_start))
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    let mut page = vec![0_u8; page_length];
    file.read_exact(&mut page)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    let mut sessions = Vec::with_capacity(end.saturating_sub(start));
    for index in start..end {
        let row_start = usize::try_from(manifest.row_offsets[index] - page_start)
            .map_err(|error| session_snapshot_failure(request_id, error))?;
        let row_end = usize::try_from(manifest.row_offsets[index + 1] - page_start)
            .map_err(|error| session_snapshot_failure(request_id, error))?;
        let encoded = &page[row_start..row_end];
        if sha256_hex(encoded) != manifest.row_sha256[index] {
            return Err(session_snapshot_failure(
                request_id,
                "one durable session row does not match its manifest",
            ));
        }
        sessions.push(
            serde_json::from_slice(encoded)
                .map_err(|error| session_snapshot_failure(request_id, error))?,
        );
    }
    Ok(sessions)
}

fn enumeration_snapshot_rows_are_valid(manifest: &EnumerationSnapshotManifest) -> bool {
    manifest.row_offsets.len() == manifest.total_sessions.saturating_add(1)
        && manifest.row_sha256.len() == manifest.total_sessions
        && manifest.row_offsets.first() == Some(&0)
        && manifest
            .row_offsets
            .last()
            .is_some_and(|bytes| *bytes <= MAX_ENUMERATION_SNAPSHOT_BYTES as u64)
        && manifest.row_offsets.windows(2).all(|offsets| {
            offsets[0] <= offsets[1]
                && offsets[1] - offsets[0] <= MAX_ENUMERATION_ENTRY_BYTES as u64
        })
        && manifest.row_sha256.iter().all(|hash| is_sha256_hex(hash))
}

fn new_enumeration_snapshot_instance_sha256(snapshot_root: &Path) -> std::io::Result<String> {
    let unique = tempfile::Builder::new()
        .prefix(".snapshot-instance-")
        .tempfile_in(snapshot_root)?;
    let instance_sha256 = sha256_hex(unique.path().to_string_lossy().as_bytes());
    unique.close()?;
    Ok(instance_sha256)
}

fn enumeration_page_request_sha256(params: &SessionEnumerateParams, request_id: &str) -> String {
    sha256_hex(
        json!({
            "request_id": request_id,
            "settings_id": params.settings_id,
            "limit": params.limit.unwrap_or(MAX_ENUMERATION_PAGE_SIZE),
            "cursor": params.cursor,
            "include_cwd": params.include_cwd.unwrap_or(true),
            "include_turn_count": params.include_turn_count.unwrap_or(true),
            "since_unix_ms": params.since_unix_ms,
        })
        .to_string()
        .as_bytes(),
    )
}

fn enumeration_cursor(
    snapshot_id: &str,
    snapshot_instance_sha256: &str,
    offset: usize,
    total: usize,
    identity_sha256: &str,
) -> String {
    format!("v3:{snapshot_id}:{snapshot_instance_sha256}:{offset}:{total}:{identity_sha256}")
}

fn parse_enumeration_cursor(
    cursor: &str,
    request_id: &str,
) -> Result<(String, String, usize, usize, String), ProviderFailure> {
    let fields = cursor.split(':').collect::<Vec<_>>();
    if fields.len() != 6
        || fields[0] != "v3"
        || !is_sha256_hex(fields[1])
        || !is_sha256_hex(fields[2])
        || !is_sha256_hex(fields[5])
    {
        return Err(invalid_session_enumerate_cursor_failure(
            request_id,
            "cursor is malformed",
        ));
    }
    let offset = fields[3].parse::<usize>().map_err(|_| {
        invalid_session_enumerate_cursor_failure(request_id, "cursor offset is not an integer")
    })?;
    let total = fields[4].parse::<usize>().map_err(|_| {
        invalid_session_enumerate_cursor_failure(request_id, "cursor total is not an integer")
    })?;
    if total > MAX_ENUMERATED_SESSIONS || offset >= total {
        return Err(invalid_session_enumerate_cursor_failure(
            request_id,
            "cursor range is outside the supported session population",
        ));
    }
    Ok((
        fields[1].to_string(),
        fields[2].to_string(),
        offset,
        total,
        fields[5].to_string(),
    ))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn enumeration_snapshot_root(
    host: &crate::envelope::HostContext,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = host
        .data_root
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| session_snapshot_data_root_failure(request_id))?;
    confined_enumeration_snapshot_target(
        host,
        &data_root.join(SESSION_ENUMERATION_SNAPSHOT_DIR),
        request_id,
    )
}

fn confined_enumeration_snapshot_target(
    host: &crate::envelope::HostContext,
    target: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = host
        .data_root
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| session_snapshot_data_root_failure(request_id))?;
    path_guard::confined_target(data_root, target)
        .map_err(|error| session_snapshot_failure(request_id, error))
}

fn acquire_enumeration_snapshot_lock(
    host: &crate::envelope::HostContext,
    root: &Path,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    durable_fs::create_private_directories(root)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    let lock_path =
        confined_enumeration_snapshot_target(host, &root.join(".snapshots.lock"), request_id)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    let timeout = operation_bounds::remaining_timeout(
        host.deadline_unix_ms,
        SESSION_ENUMERATION_LOCK_TIMEOUT,
    )
    .ok_or_else(|| session_snapshot_lock_timeout_failure(request_id))?;
    if !operation_bounds::lock_exclusive_for(&lock, timeout)
        .map_err(|error| session_snapshot_failure(request_id, error))?
    {
        return Err(session_snapshot_lock_timeout_failure(request_id));
    }
    Ok(lock)
}

fn prune_enumeration_snapshots(
    host: &crate::envelope::HostContext,
    root: &Path,
    request_id: &str,
) -> Result<usize, ProviderFailure> {
    let mut retained = 0_usize;
    let mut visited = 0_usize;
    for entry in fs::read_dir(root).map_err(|error| session_snapshot_failure(request_id, error))? {
        let entry = entry.map_err(|error| session_snapshot_failure(request_id, error))?;
        if !entry
            .file_type()
            .map_err(|error| session_snapshot_failure(request_id, error))?
            .is_dir()
        {
            continue;
        }
        visited += 1;
        if visited > MAX_ENUMERATION_SNAPSHOTS.saturating_add(1) {
            return Err(session_snapshot_capacity_failure(
                request_id,
                "the snapshot directory exceeds its supported entry bound",
            ));
        }
        let path = confined_enumeration_snapshot_target(host, &entry.path(), request_id)?;
        let expired_or_partial = snapshot_manifest(&path, request_id)
            .map(|manifest| {
                manifest.schema_version != SESSION_ENUMERATION_SNAPSHOT_SCHEMA_VERSION
                    || manifest.expires_at_unix_ms < now_unix_ms()
            })
            .unwrap_or(true);
        if expired_or_partial {
            fs::remove_dir_all(&path)
                .map_err(|error| session_snapshot_failure(request_id, error))?;
            durable_fs::sync_directory(root)
                .map_err(|error| session_snapshot_failure(request_id, error))?;
        } else {
            retained += 1;
        }
    }
    Ok(retained)
}

fn snapshot_manifest(
    snapshot_root: &Path,
    request_id: &str,
) -> Result<EnumerationSnapshotManifest, ProviderFailure> {
    let bytes = durable_fs::read_file_bounded(
        &snapshot_root.join("manifest.json"),
        MAX_ENUMERATION_MANIFEST_BYTES,
    )
    .map_err(|error| session_snapshot_failure(request_id, error))?;
    serde_json::from_slice(&bytes).map_err(|error| session_snapshot_failure(request_id, error))
}

fn encode_enumeration_snapshot_manifest(
    manifest: &EnumerationSnapshotManifest,
    request_id: &str,
) -> Result<Vec<u8>, ProviderFailure> {
    let bytes = serde_json::to_vec(manifest)
        .map_err(|error| session_snapshot_failure(request_id, error))?;
    if bytes.len() > MAX_ENUMERATION_MANIFEST_BYTES {
        return Err(session_snapshot_capacity_failure(
            request_id,
            "the session snapshot manifest exceeds its supported encoded-size bound",
        ));
    }
    Ok(bytes)
}

fn write_enumeration_snapshot_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    stage_enumeration_snapshot_file(path, bytes)?;
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("snapshot path has no parent"))?;
    durable_fs::sync_directory(parent)
}

fn stage_enumeration_snapshot_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("snapshot path has no parent"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    set_private_snapshot_file(path)
}

fn read_enumeration_snapshot_file(path: &Path, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(maximum_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("snapshot file exceeds supported {maximum_bytes}-byte bound"),
        ));
    }
    Ok(bytes)
}

#[cfg(unix)]
fn set_private_snapshot_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_snapshot_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn enumerate_cwd(
    entry: &OpencodeSessionListRow,
    index: usize,
    warnings: &mut Vec<String>,
) -> Option<String> {
    let provider_session_id = &entry.provider_session_id;
    let cwd = match &entry.directory {
        OpencodeSessionDirectory::Absolute(cwd) => return Some(cwd.clone()),
        OpencodeSessionDirectory::Missing => {
            warnings.push(format!(
                "session {provider_session_id} row {index} has no directory/cwd"
            ));
            return None;
        }
        OpencodeSessionDirectory::Invalid(cwd) => cwd,
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
        OpencodeSessionListError::InvalidRow { index, message } => {
            invalid_opencode_session_list_failure(
                request_id,
                format!("row {index} is invalid: {message}"),
            )
        }
        OpencodeSessionListError::OutputTooLarge {
            stream,
            maximum_bytes,
        } => ProviderFailure::invalid_request(
            request_id,
            "opencode_session_list_capacity_exceeded",
            format!(
                "opencode session list {stream} exceeds the supported {maximum_bytes}-byte bound"
            ),
        ),
        OpencodeSessionListError::TimedOut => session_list_timeout_failure(request_id),
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

fn enumerate_result(page: EnumeratePage) -> SessionOutcome {
    SessionOutcome {
        result: json!({
            "sessions": page.sessions,
            "complete": page.complete,
            "next_cursor": page.next_cursor,
            "warnings": page.warnings,
        }),
    }
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
        format!("session.enumerate limit must be between 1 and {MAX_ENUMERATION_PAGE_SIZE}"),
    )
}

fn invalid_session_enumerate_cursor_failure(request_id: &str, message: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_session_enumerate_cursor",
        format!("session.enumerate cursor is invalid: {message}"),
    )
}

fn missing_session_id_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "missing_session_id",
        "session params require non-empty session_id",
    )
}

fn session_runtime(
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
        format!("opencode session list output was not a valid typed observation: {message}"),
    )
}

fn session_list_timeout_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "opencode_session_list_timeout",
        "opencode session list did not complete within the supported operation deadline",
    )
}

fn session_population_capacity_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "opencode_session_population_capacity_exceeded",
        format!(
            "opencode session population exceeds the supported {MAX_ENUMERATED_SESSIONS}-session snapshot bound"
        ),
    )
}

fn session_snapshot_data_root_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "session_enumeration_data_root_missing",
        "paginated session enumeration requires host.data_root for its bounded continuation snapshot",
    )
}

fn session_snapshot_failure(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "session_enumeration_snapshot_failed",
        format!("bounded session enumeration snapshot failed: {error}"),
    )
}

fn session_snapshot_capacity_failure(
    request_id: &str,
    message: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "session_enumeration_snapshot_capacity_exceeded",
        message.to_string(),
    )
}

fn session_snapshot_lock_timeout_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "session_enumeration_snapshot_lock_timeout",
        "session enumeration snapshot lock could not be acquired before the operation deadline",
    )
}

fn session_snapshot_terminal_handoff_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "session_enumeration_snapshot_terminal_handoff_in_progress",
        "session enumeration snapshot already has a different terminal response handoff owner",
        json!({
            "required_action": "retry only the exact terminal continuation request until its response handoff completes",
        }),
    )
}

fn session_snapshot_cursor_superseded_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "session_enumeration_cursor_superseded",
        "session enumeration cursor has already advanced to a later page",
        json!({
            "required_action": "retry only the exact latest page request, continue from its returned cursor, or restart enumeration",
        }),
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
