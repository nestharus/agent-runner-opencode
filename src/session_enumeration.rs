//! Declared roles: orchestration, mapper, parser, validator, formatter, filter, accessor, predicate
//! intrinsic_surface_declarations:
//!   - component: src/session_enumeration.rs
//!     role: intrinsic-surface
//!     Domain: durable session-enumeration snapshot custody
//!     Owns:
//!       - bounded native session population capture
//!       - request-bound immutable snapshot and initial-page replay
//!       - cursor identity, advancement, retention, and terminal claims

use crate::durable_fs;
use crate::encoding::{now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure};
use crate::opencode::{
    self, OpencodeSessionDirectory, OpencodeSessionListError, OpencodeSessionListRow,
};
use crate::operation_bounds;
use crate::path_guard;
use crate::session::session_runtime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

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

struct EnumerationSnapshotRetention {
    retained: usize,
    oldest_terminal: Option<(u64, PathBuf)>,
}
pub(crate) fn enumerate_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
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
    let retention = prune_enumeration_snapshots(host, &root, request_id)?;
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
    } else if retention.retained >= MAX_ENUMERATION_SNAPSHOTS {
        if let Some((_, terminal)) = retention.oldest_terminal {
            fs::remove_dir_all(&terminal)
                .map_err(|error| session_snapshot_failure(request_id, error))?;
            durable_fs::sync_directory(&root)
                .map_err(|error| session_snapshot_failure(request_id, error))?;
        } else {
            return Err(session_snapshot_capacity_failure(
                request_id,
                "all retained session enumeration snapshots still own active cursors; retry after a cursor reaches its terminal page or expires",
            ));
        }
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
) -> Result<EnumerationSnapshotRetention, ProviderFailure> {
    let mut retained = 0_usize;
    let mut visited = 0_usize;
    let mut oldest_terminal: Option<(u64, PathBuf)> = None;
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
        let manifest = snapshot_manifest(&path, request_id);
        let expired_or_partial = manifest.as_ref().map_or(true, |manifest| {
            manifest.schema_version != SESSION_ENUMERATION_SNAPSHOT_SCHEMA_VERSION
                || manifest.expires_at_unix_ms < now_unix_ms()
        });
        if expired_or_partial {
            fs::remove_dir_all(&path)
                .map_err(|error| session_snapshot_failure(request_id, error))?;
            durable_fs::sync_directory(root)
                .map_err(|error| session_snapshot_failure(request_id, error))?;
        } else {
            retained += 1;
            let manifest = manifest.expect("retained snapshot has a readable manifest");
            if manifest.terminal_claim_request_sha256.is_some()
                && oldest_terminal.as_ref().is_none_or(|(created, candidate)| {
                    (manifest.created_at_unix_ms, path.as_path()) < (*created, candidate.as_path())
                })
            {
                oldest_terminal = Some((manifest.created_at_unix_ms, path));
            }
        }
    }
    Ok(EnumerationSnapshotRetention {
        retained,
        oldest_terminal,
    })
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
fn enumerate_result(page: EnumeratePage) -> Value {
    json!({
        "sessions": page.sessions,
        "complete": page.complete,
        "next_cursor": page.next_cursor,
        "warnings": page.warnings,
    })
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
