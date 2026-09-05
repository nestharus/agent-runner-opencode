//! Declared roles: orchestration, adapter, parser, validator, formatter, mapper
//! intrinsic_surface_declarations:
//!   - component: src/session_turn_pages.rs
//!     role: intrinsic-surface
//!     Domain: bounded OpenCode session-turn paging
//!     Owns:
//!       - strict oulipoly.session_turn_pages/v1 request modes
//!       - OpenCode SQLite message/part schema detection and keyset reads
//!       - source-generation-bound snapshot, page, and resume tokens
//!       - metadata-only live session capture

use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::encoding::{decode_base64, encode_base64, sha256_hex};
use crate::envelope::{success_response, HostContext, ProviderFailure, RequestEnvelope};
use crate::native_runtime::{self, NativeRuntimeContext};
use crate::operation_bounds;
use crate::path_guard;
use crate::runtime_selection::resolve_runtime_selection;
use chrono::{DateTime, SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const READ_PROTOCOL: &str = "oulipoly.session_turn_pages/v1";
const SOURCE_SCHEMA: &str = "opencode.sqlite.message-part/v1";
const TOKEN_VERSION: u8 = 2;
const TOKEN_PREFIX: &str = "stp2";
const LEGACY_TOKEN_PREFIX: &str = "stp1";
const TOKEN_DOMAIN: &[u8] = b"agent-runner-opencode.session-turn-token.v1\0";
const TOKEN_KEY_BYTES: usize = 32;
const TOKEN_KEY_PATH: &str = "provider-state/opencode/session-turn-token-auth-v1.key";
const DATABASE_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_DATABASE_PATH_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_DATABASE_SCHEMA_OUTPUT_BYTES: usize = 60 * 1024;
const MAX_DATABASE_QUERY_OUTPUT_BYTES: usize = 60 * 1024;
const MAX_DATABASE_STDERR_BYTES: usize = 64 * 1024;
const MAX_NATIVE_PAYLOAD_QUERY_BYTES: usize = 1024 * 1024;
const MAX_NATIVE_MESSAGE_QUERY_SOURCE_BYTES: usize = 20 * 1024;
const MAX_NATIVE_MESSAGE_QUERY_ROWS: usize = 128;
const MAX_NATIVE_DATABASE_BATCH_SOURCE_BYTES: usize = 12 * 1024;
const MAX_NATIVE_DATABASE_BATCH_ROWS: usize = 64;
const MAX_TURNS: usize = 256;
const MIN_RESPONSE_BYTES: usize = 1024;
const MAX_RESPONSE_BYTES: usize = 512 * 1024;
const MIN_SOURCE_BYTES: usize = 1;
const MAX_SOURCE_BYTES: usize = 8 * 1024 * 1024;
const MAX_INLINE_BODY_BYTES: usize = 64 * 1024;
const MAX_ID_BYTES: usize = 1024;
const MAX_ROLE_BYTES: usize = 64;
const MAX_TOKEN_BYTES: usize = 4096;
const MAX_MESSAGE_METADATA_BYTES: usize = 16 * 1024;
const MAX_PARTS_PER_MESSAGE: usize = 128;
const MAX_PAGE_PART_ROWS: usize = MAX_PARTS_PER_MESSAGE + 1;
const PART_TYPE_PREFIX_BYTES: usize = 128;
const MAX_OBSERVATION_SCAN_ROWS: usize = 256;
const MAX_SESSION_DIRECTORY_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TurnProjection {
    CanonicalIngest,
    UserObservation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum StartMode {
    Beginning,
    Tail,
    Continuation,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadTurnsParams {
    settings_id: String,
    session_id: String,
    read_protocol: String,
    turn_projection: TurnProjection,
    expected_delivery_nonce: Option<String>,
    start_mode: StartMode,
    after_token: Option<String>,
    snapshot_id: Option<String>,
    page_token: Option<String>,
    max_turns: usize,
    max_response_bytes: usize,
    max_source_bytes: usize,
    max_inline_body_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceKey {
    #[serde(rename = "t")]
    created: u64,
    #[serde(rename = "i")]
    id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TokenBinding {
    #[serde(rename = "p")]
    provider_instance_id: String,
    #[serde(rename = "s")]
    settings_id: String,
    #[serde(rename = "v")]
    settings_version: String,
    #[serde(rename = "n")]
    session_id: String,
    #[serde(rename = "c")]
    session_created: u64,
    #[serde(rename = "j")]
    projection: TurnProjection,
    #[serde(rename = "d")]
    expected_delivery_nonce: Option<String>,
    #[serde(rename = "r")]
    runtime_identity: String,
    #[serde(rename = "g")]
    source_generation: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PageBudgets {
    #[serde(rename = "t")]
    max_turns: usize,
    #[serde(rename = "r")]
    max_response_bytes: usize,
    #[serde(rename = "s")]
    max_source_bytes: usize,
    #[serde(rename = "b")]
    max_inline_body_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageTokenPayload {
    #[serde(rename = "v")]
    version: u8,
    #[serde(rename = "k")]
    kind: String,
    #[serde(rename = "b")]
    binding: TokenBinding,
    #[serde(rename = "l")]
    budgets: PageBudgets,
    #[serde(rename = "s")]
    snapshot_id: String,
    #[serde(rename = "h")]
    high_watermark: Option<SourceKey>,
    #[serde(rename = "x")]
    snapshot_guard: SnapshotGuard,
    #[serde(rename = "c")]
    cursor: Option<SourceKey>,
    #[serde(rename = "p")]
    page_index: u64,
    #[serde(rename = "q")]
    next_sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResumeTokenPayload {
    #[serde(rename = "v")]
    version: u8,
    #[serde(rename = "k")]
    kind: String,
    #[serde(rename = "b")]
    binding: TokenBinding,
    #[serde(rename = "p")]
    position: Option<SourceKey>,
}

struct PageState {
    snapshot_id: String,
    high_watermark: Option<SourceKey>,
    snapshot_guard: SnapshotGuard,
    cursor: Option<SourceKey>,
    page_index: u64,
    next_sequence: u64,
    budgets: PageBudgets,
    token_key: [u8; TOKEN_KEY_BYTES],
}

#[derive(Clone)]
struct SessionMetadata {
    id: String,
    directory: String,
    created: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceStamp {
    device: u64,
    inode: u64,
}

#[derive(Serialize)]
struct SourceFileRevision {
    #[serde(rename = "l")]
    len: u64,
    #[serde(rename = "m")]
    modified_seconds: i64,
    #[serde(rename = "n")]
    modified_nanoseconds: i64,
    #[serde(rename = "c")]
    changed_seconds: i64,
    #[serde(rename = "d")]
    changed_nanoseconds: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotGuard {
    #[serde(rename = "r")]
    revision: String,
    #[serde(rename = "m")]
    message_rowid: u64,
    #[serde(rename = "p")]
    part_rowid: u64,
}

struct SourceAdapter<'a> {
    host: &'a HostContext,
    runtime: NativeRuntimeContext,
    database_path: PathBuf,
    stamp: SourceStamp,
    schema_fingerprint: String,
    session: SessionMetadata,
    request_id: &'a str,
}

#[derive(Clone)]
struct MessageMetadata {
    key: SourceKey,
    updated: u64,
    data_bytes: usize,
}

#[derive(Clone)]
struct NativeMessage {
    metadata: MessageMetadata,
    role: String,
    parent_id: Option<String>,
    created: u64,
    completed: Option<u64>,
}

#[derive(Clone)]
struct PartMetadata {
    id: String,
    message_id: String,
    updated: u64,
    data_bytes: usize,
}

#[derive(Clone)]
struct ClassifiedPart {
    metadata: PartMetadata,
    part_type: String,
}

enum BodyProjection {
    Absent,
    Omitted {
        body_bytes: Option<usize>,
        body_sha256: Option<String>,
        canonical_text_sha256: Option<String>,
    },
    Inline {
        body: Vec<Value>,
        body_bytes: usize,
        body_sha256: String,
        canonical_text_sha256: String,
    },
}

struct SourceWork {
    maximum: usize,
    examined: usize,
}

impl SourceWork {
    fn new(maximum: usize) -> Self {
        Self {
            maximum,
            examined: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.maximum.saturating_sub(self.examined)
    }

    fn add(&mut self, bytes: usize, request_id: &str) -> Result<(), ProviderFailure> {
        let examined = self.examined.checked_add(bytes).ok_or_else(|| {
            source_budget_failure(request_id, "source byte accounting overflowed")
        })?;
        if examined > self.maximum {
            return Err(source_budget_failure(
                request_id,
                "the bounded OpenCode source read exceeded max_source_bytes",
            ));
        }
        self.examined = examined;
        Ok(())
    }
}

pub(crate) fn read_turns(request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let request_id = request.request_id.clone();
    require_selected_protocol(&request.host, &request_id)?;
    let provider_instance_id = request
        .provider_instance_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid_params_failure(&request_id, "provider_instance_id is required"))?
        .to_string();
    let params = parse_params(request.params, &request_id)?;
    validate_params(&params, &request_id)?;
    let selection = resolve_runtime_selection(&request.host, &params.settings_id, &request_id)?;
    let runtime =
        native_runtime::resolve_for_account(&request.host, selection.account, &request_id)?;
    let adapter = SourceAdapter::open(&request.host, runtime, &params.session_id, &request_id)?;
    let token_key = session_token_key(&request.host, &request_id)?;
    let binding = TokenBinding {
        provider_instance_id,
        settings_id: selection.settings_id,
        settings_version: selection.settings_version,
        session_id: adapter.session.id.clone(),
        session_created: adapter.session.created,
        projection: params.turn_projection,
        expected_delivery_nonce: params.expected_delivery_nonce.clone(),
        runtime_identity: adapter.runtime.identity_sha256().to_string(),
        source_generation: adapter.source_generation(),
    };
    let budgets = PageBudgets {
        max_turns: params.max_turns,
        max_response_bytes: params.max_response_bytes,
        max_source_bytes: params.max_source_bytes,
        max_inline_body_bytes: params.max_inline_body_bytes,
    };
    let mut work = SourceWork::new(params.max_source_bytes);
    let (state, tail_anchor) = page_state(
        &adapter,
        &binding,
        &budgets,
        token_key,
        &params,
        &mut work,
        &request_id,
    )?;
    let snapshot_guard = state.snapshot_guard.clone();
    let high_watermark = state.high_watermark.clone();
    let result = if tail_anchor {
        page_result(
            &binding,
            &state,
            state.cursor.clone(),
            Vec::new(),
            work.examined,
            false,
            &request_id,
        )?
    } else {
        read_page(&adapter, &binding, &budgets, state, &mut work, &request_id)?
    };
    adapter.require_snapshot_guard(&snapshot_guard, high_watermark.as_ref())?;
    adapter.verify_schema_current()?;
    ensure_response_within_budget(&request_id, &result, params.max_response_bytes)?;
    Ok(result)
}

pub(crate) fn capture_live_session(
    host: &HostContext,
    settings_id: &str,
    session_id: &str,
    working_directory: Option<&str>,
    request_id: &str,
) -> Result<String, ProviderFailure> {
    let expected = working_directory
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            capture_params_failure(request_id, "live reports require host.working_directory")
        })?;
    let selection = resolve_runtime_selection(host, settings_id, request_id)?;
    let runtime = native_runtime::resolve_for_account(host, selection.account, request_id)?;
    let adapter = SourceAdapter::open(host, runtime, session_id, request_id)?;
    if Path::new(&adapter.session.directory) != Path::new(expected) {
        return Err(capture_params_failure(
            request_id,
            format!(
                "live report workspace mismatch: OpenCode metadata reports {}, runner requested {expected}",
                adapter.session.directory
            ),
        ));
    }
    adapter.verify_schema_current()?;
    Ok(adapter.session.id)
}

fn require_selected_protocol(host: &HostContext, request_id: &str) -> Result<(), ProviderFailure> {
    if host
        .env
        .as_ref()
        .and_then(|env| env.get(crate::schema::HOST_SESSION_TURN_PAGES_V1_ENV))
        .map(String::as_str)
        == Some("1")
    {
        return Ok(());
    }
    Err(ProviderFailure::unsupported(
        request_id,
        "session_turn_pages_not_selected",
        "session.read_turns requires host selection OULIPOLY_HOST_SESSION_TURN_PAGES_V1=1",
    ))
}

fn parse_params(params: Value, request_id: &str) -> Result<ReadTurnsParams, ProviderFailure> {
    let object = params
        .as_object()
        .ok_or_else(|| invalid_params_failure(request_id, "params must be an object"))?;
    for field in [
        "settings_id",
        "session_id",
        "read_protocol",
        "turn_projection",
        "start_mode",
        "after_token",
        "snapshot_id",
        "page_token",
        "max_turns",
        "max_response_bytes",
        "max_source_bytes",
        "max_inline_body_bytes",
    ] {
        if !object.contains_key(field) {
            return Err(invalid_params_failure(
                request_id,
                format!("missing required paging field {field}"),
            ));
        }
    }
    serde_json::from_value(params)
        .map_err(|error| invalid_params_failure(request_id, error.to_string()))
}

fn validate_params(params: &ReadTurnsParams, request_id: &str) -> Result<(), ProviderFailure> {
    if params.read_protocol != READ_PROTOCOL {
        return Err(ProviderFailure::unsupported(
            request_id,
            "unsupported_session_read_protocol",
            format!("session.read_turns supports only read_protocol {READ_PROTOCOL}"),
        ));
    }
    for (label, value) in [
        ("settings_id", params.settings_id.as_str()),
        ("session_id", params.session_id.as_str()),
    ] {
        if value.trim().is_empty() || value.len() > MAX_ID_BYTES {
            return Err(invalid_params_failure(
                request_id,
                format!("{label} must contain 1..={MAX_ID_BYTES} UTF-8 bytes"),
            ));
        }
    }
    if !(1..=MAX_TURNS).contains(&params.max_turns) {
        return Err(invalid_params_failure(
            request_id,
            format!("max_turns must be between 1 and {MAX_TURNS}"),
        ));
    }
    if !(MIN_RESPONSE_BYTES..=MAX_RESPONSE_BYTES).contains(&params.max_response_bytes) {
        return Err(invalid_params_failure(
            request_id,
            format!(
                "max_response_bytes must be between {MIN_RESPONSE_BYTES} and {MAX_RESPONSE_BYTES}"
            ),
        ));
    }
    if !(MIN_SOURCE_BYTES..=MAX_SOURCE_BYTES).contains(&params.max_source_bytes) {
        return Err(invalid_params_failure(
            request_id,
            format!("max_source_bytes must be between {MIN_SOURCE_BYTES} and {MAX_SOURCE_BYTES}"),
        ));
    }
    if params.max_inline_body_bytes > MAX_INLINE_BODY_BYTES {
        return Err(invalid_params_failure(
            request_id,
            format!("max_inline_body_bytes must not exceed {MAX_INLINE_BODY_BYTES}"),
        ));
    }
    match (
        params.turn_projection,
        params.expected_delivery_nonce.as_deref(),
    ) {
        (TurnProjection::UserObservation, Some(nonce))
            if nonce.len() == 64
                && nonce
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) => {}
        (TurnProjection::UserObservation, _) => {
            return Err(invalid_params_failure(
                request_id,
                "user_observation requires expected_delivery_nonce as exactly 64 lowercase hexadecimal characters",
            ));
        }
        (TurnProjection::CanonicalIngest, None) => {}
        (TurnProjection::CanonicalIngest, Some(_)) => {
            return Err(invalid_params_failure(
                request_id,
                "canonical_ingest forbids expected_delivery_nonce",
            ));
        }
    }
    for token in [
        params.after_token.as_deref(),
        params.snapshot_id.as_deref(),
        params.page_token.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
            return Err(invalid_params_failure(
                request_id,
                format!("paging tokens must contain 1..={MAX_TOKEN_BYTES} bytes"),
            ));
        }
    }
    match (
        params.start_mode,
        params.after_token.as_ref(),
        params.snapshot_id.as_ref(),
        params.page_token.as_ref(),
    ) {
        (StartMode::Beginning, _, None, None)
        | (StartMode::Tail, None, None, None)
        | (StartMode::Continuation, None, Some(_), Some(_)) => {
            if params.start_mode == StartMode::Tail
                && params.turn_projection != TurnProjection::UserObservation
            {
                return Err(invalid_params_failure(
                    request_id,
                    "tail mode requires turn_projection user_observation",
                ));
            }
            Ok(())
        }
        _ => Err(invalid_params_failure(
            request_id,
            "paging mode must be beginning with optional after_token, tail without tokens, or continuation with snapshot_id and page_token only",
        )),
    }
}

fn page_state(
    adapter: &SourceAdapter<'_>,
    binding: &TokenBinding,
    budgets: &PageBudgets,
    token_key: [u8; TOKEN_KEY_BYTES],
    params: &ReadTurnsParams,
    work: &mut SourceWork,
    request_id: &str,
) -> Result<(PageState, bool), ProviderFailure> {
    if params.start_mode == StartMode::Continuation {
        let token: PageTokenPayload = decode_token(
            params.page_token.as_deref().expect("validated page token"),
            "page",
            &token_key,
            adapter.runtime.identity_sha256(),
            request_id,
        )?;
        if !same_token_scope(&token.binding, binding)
            || token.budgets != *budgets
            || params.snapshot_id.as_deref() != Some(token.snapshot_id.as_str())
        {
            return Err(invalid_token_failure(
                request_id,
                "page token does not match provider, settings, session, projection, budgets, or snapshot",
            ));
        }
        adapter.require_generation(&token.binding.source_generation)?;
        adapter.require_snapshot_guard(&token.snapshot_guard, token.high_watermark.as_ref())?;
        if let Some(high_watermark) = token.high_watermark.as_ref() {
            let message = adapter.load_message_at_key(high_watermark, work)?;
            if !message_is_immutable(&message) {
                return Err(snapshot_invalidated_failure(
                    request_id,
                    "snapshot high-watermark is no longer immutable",
                ));
            }
        }
        return Ok((
            PageState {
                snapshot_id: token.snapshot_id,
                high_watermark: token.high_watermark,
                snapshot_guard: token.snapshot_guard,
                cursor: token.cursor,
                page_index: token.page_index,
                next_sequence: token.next_sequence,
                budgets: token.budgets,
                token_key,
            },
            false,
        ));
    }

    let cursor = if let Some(token) = params.after_token.as_deref() {
        let token: ResumeTokenPayload = decode_token(
            token,
            "resume",
            &token_key,
            adapter.runtime.identity_sha256(),
            request_id,
        )?;
        if !same_token_scope(&token.binding, binding) {
            return Err(invalid_token_failure(
                request_id,
                "resume token does not match provider, settings, session, or projection",
            ));
        }
        adapter.require_generation(&token.binding.source_generation)?;
        token.position
    } else {
        None
    };
    let revision_before = adapter.source_revision()?;
    let (message_rowid, part_rowid) = adapter.rowid_frontiers()?;
    let high_watermark = adapter.immutable_tail(work)?;
    let revision_after = adapter.source_revision()?;
    if revision_before != revision_after {
        return Err(snapshot_invalidated_failure(
            request_id,
            "OpenCode source changed while capturing the immutable snapshot boundary",
        ));
    }
    let snapshot_guard = SnapshotGuard {
        revision: revision_after,
        message_rowid,
        part_rowid,
    };
    if cursor.as_ref().is_some_and(|cursor| {
        high_watermark
            .as_ref()
            .is_none_or(|high| source_key_cmp(cursor, high).is_gt())
    }) {
        return Err(snapshot_invalidated_failure(
            request_id,
            "resume position is beyond the current immutable source tail",
        ));
    }
    let snapshot_id = snapshot_id(
        binding,
        cursor.as_ref(),
        high_watermark.as_ref(),
        &snapshot_guard,
    );
    let tail_anchor = params.start_mode == StartMode::Tail;
    Ok((
        PageState {
            snapshot_id,
            high_watermark: high_watermark.clone(),
            snapshot_guard,
            cursor: if tail_anchor { high_watermark } else { cursor },
            page_index: 0,
            next_sequence: 0,
            budgets: budgets.clone(),
            token_key,
        },
        tail_anchor,
    ))
}

fn same_token_scope(left: &TokenBinding, right: &TokenBinding) -> bool {
    left.provider_instance_id == right.provider_instance_id
        && left.settings_id == right.settings_id
        && left.settings_version == right.settings_version
        && left.session_id == right.session_id
        && left.projection == right.projection
        && left.expected_delivery_nonce == right.expected_delivery_nonce
        && left.runtime_identity == right.runtime_identity
}

fn read_page(
    adapter: &SourceAdapter<'_>,
    binding: &TokenBinding,
    budgets: &PageBudgets,
    state: PageState,
    work: &mut SourceWork,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let Some(high_watermark) = state.high_watermark.as_ref() else {
        return page_result(
            binding,
            &state,
            None,
            Vec::new(),
            work.examined,
            false,
            request_id,
        );
    };
    let row_limit = match binding.projection {
        TurnProjection::CanonicalIngest => budgets.max_turns.saturating_add(1),
        TurnProjection::UserObservation => MAX_OBSERVATION_SCAN_ROWS,
    };
    let metadata =
        adapter.message_metadata_page(state.cursor.as_ref(), high_watermark, row_limit)?;
    if metadata.is_empty() {
        return page_result(
            binding,
            &state,
            state.cursor.clone(),
            Vec::new(),
            work.examined,
            false,
            request_id,
        );
    }

    let selected = select_message_payloads(
        &metadata,
        work.remaining().min(MAX_NATIVE_PAYLOAD_QUERY_BYTES),
        budgets.max_turns,
    );
    if selected.is_empty() {
        return Err(source_budget_failure(
            request_id,
            "max_source_bytes cannot admit the next bounded message metadata record",
        ));
    }
    let messages = adapter.load_messages(&selected, work)?;
    let relevant = messages
        .iter()
        .filter(|message| message_matches_projection(message, binding.projection))
        .take(budgets.max_turns)
        .map(|message| message.metadata.key.clone())
        .collect::<Vec<_>>();
    let parts = adapter.classified_parts(&relevant, work)?;
    let bodies = adapter.project_bodies(
        &relevant,
        &parts,
        binding.projection,
        binding.expected_delivery_nonce.as_deref(),
        budgets.max_inline_body_bytes,
        work,
    )?;

    let page_start_sequence = state.next_sequence;
    let mut turns = Vec::new();
    let mut cursor = state.cursor.clone();
    for message in &messages {
        if !message_matches_projection(message, binding.projection) {
            cursor = Some(message.metadata.key.clone());
            continue;
        }
        if turns.len() == budgets.max_turns {
            break;
        }
        let Some(body) = bodies.get(&message.metadata.key.id) else {
            break;
        };
        let sequence = page_start_sequence.saturating_add(turns.len() as u64);
        let turn = projected_turn(message, sequence, body, &adapter.session.id, &parts)?;
        let mut tentative = turns.clone();
        tentative.push(turn.clone());
        let tentative_result = page_result(
            binding,
            &state,
            Some(message.metadata.key.clone()),
            tentative,
            work.examined,
            false,
            request_id,
        )?;
        if response_fits(&tentative_result, budgets.max_response_bytes, request_id) {
            turns.push(turn);
            cursor = Some(message.metadata.key.clone());
            continue;
        }
        let omitted = omit_turn_body(turn);
        let mut tentative = turns.clone();
        tentative.push(omitted.clone());
        let tentative_result = page_result(
            binding,
            &state,
            Some(message.metadata.key.clone()),
            tentative,
            work.examined,
            false,
            request_id,
        )?;
        if response_fits(&tentative_result, budgets.max_response_bytes, request_id) {
            turns.push(omitted);
            cursor = Some(message.metadata.key.clone());
            continue;
        }
        if turns.is_empty() {
            return Err(turn_metadata_too_large_failure(request_id));
        }
        break;
    }
    if turns.is_empty() && cursor == state.cursor {
        return Err(source_budget_failure(
            request_id,
            "the page budgets cannot admit the next complete bounded turn",
        ));
    }
    let scan_progress = turns.is_empty() && cursor != state.cursor;
    page_result(
        binding,
        &state,
        cursor,
        turns,
        work.examined,
        scan_progress,
        request_id,
    )
}

fn select_message_payloads(
    metadata: &[MessageMetadata],
    source_bytes_remaining: usize,
    max_turns: usize,
) -> Vec<MessageMetadata> {
    let reserve = MAX_PARTS_PER_MESSAGE.saturating_mul(PART_TYPE_PREFIX_BYTES);
    let message_capacity = source_bytes_remaining
        .saturating_sub(reserve)
        .min(MAX_NATIVE_MESSAGE_QUERY_SOURCE_BYTES);
    let mut selected = Vec::new();
    let mut bytes = 0_usize;
    for row in metadata {
        if selected.len() >= MAX_NATIVE_MESSAGE_QUERY_ROWS {
            break;
        }
        let next = bytes.saturating_add(row.data_bytes);
        if next > message_capacity {
            break;
        }
        selected.push(row.clone());
        bytes = next;
        if selected.len() >= max_turns && metadata.len() <= max_turns.saturating_add(1) {
            break;
        }
    }
    selected
}

fn message_matches_projection(message: &NativeMessage, projection: TurnProjection) -> bool {
    projection == TurnProjection::CanonicalIngest || message.role == "user"
}

fn projected_turn(
    message: &NativeMessage,
    sequence: u64,
    body: &BodyProjection,
    session_id: &str,
    parts: &BTreeMap<String, Vec<ClassifiedPart>>,
) -> Result<Value, ProviderFailure> {
    let timestamp = i64::try_from(message.created)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let turn_id = stable_turn_id(session_id, &message.metadata.key.id);
    let parent_turn_id = message
        .parent_id
        .as_deref()
        .map(|parent_id| stable_turn_id(session_id, parent_id));
    let is_compaction_boundary = parts
        .get(&message.metadata.key.id)
        .is_some_and(|parts| parts.iter().any(|part| part.part_type == "compaction"));
    let (body_state, body_value, body_bytes, body_sha256, canonical_text_sha256) = match body {
        BodyProjection::Absent => ("absent", Value::Null, Value::Null, Value::Null, Value::Null),
        BodyProjection::Omitted {
            body_bytes,
            body_sha256,
            canonical_text_sha256,
        } => (
            "omitted_oversize",
            Value::Null,
            json!(body_bytes),
            json!(body_sha256),
            json!(canonical_text_sha256),
        ),
        BodyProjection::Inline {
            body,
            body_bytes,
            body_sha256,
            canonical_text_sha256,
        } => (
            "inline",
            Value::Array(body.clone()),
            json!(body_bytes),
            json!(body_sha256),
            json!(canonical_text_sha256),
        ),
    };
    Ok(json!({
        "session_id": session_id,
        "turn_id": turn_id,
        "snapshot_sequence": sequence,
        "timestamp": timestamp,
        "role": message.role,
        "parent_turn_id": parent_turn_id,
        "is_sidechain": false,
        "is_compaction_boundary": is_compaction_boundary,
        "body_state": body_state,
        "body": body_value,
        "body_bytes": body_bytes,
        "body_sha256": body_sha256,
        "canonical_text_sha256": canonical_text_sha256,
    }))
}

fn omit_turn_body(mut turn: Value) -> Value {
    if turn["body_state"] == "inline" {
        turn["body_state"] = json!("omitted_oversize");
        turn["body"] = Value::Null;
    }
    turn
}

fn page_result(
    binding: &TokenBinding,
    state: &PageState,
    cursor: Option<SourceKey>,
    turns: Vec<Value>,
    source_bytes_examined: usize,
    scan_progress: bool,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let complete = cursor == state.high_watermark;
    let next_sequence = state.next_sequence.saturating_add(turns.len() as u64);
    let next_page_token = if complete {
        None
    } else {
        Some(encode_token(
            &PageTokenPayload {
                version: TOKEN_VERSION,
                kind: "page".to_string(),
                binding: binding.clone(),
                budgets: state.budgets.clone(),
                snapshot_id: state.snapshot_id.clone(),
                high_watermark: state.high_watermark.clone(),
                snapshot_guard: state.snapshot_guard.clone(),
                cursor: cursor.clone(),
                page_index: state.page_index.saturating_add(1),
                next_sequence,
            },
            &state.token_key,
            binding.runtime_identity.as_str(),
            request_id,
        )?)
    };
    let resume_token = if complete {
        Some(encode_token(
            &ResumeTokenPayload {
                version: TOKEN_VERSION,
                kind: "resume".to_string(),
                binding: binding.clone(),
                position: state.high_watermark.clone(),
            },
            &state.token_key,
            binding.runtime_identity.as_str(),
            request_id,
        )?)
    } else {
        None
    };
    Ok(json!({
        "read_protocol": READ_PROTOCOL,
        "provider_instance_id": binding.provider_instance_id,
        "settings_id": binding.settings_id,
        "session_id": binding.session_id,
        "turn_projection": binding.projection,
        "snapshot_id": state.snapshot_id,
        "page_index": state.page_index,
        "page_start_sequence": state.next_sequence,
        "turns": turns,
        "page_turn_count": next_sequence.saturating_sub(state.next_sequence),
        "source_bytes_examined": source_bytes_examined,
        "scan_progress": scan_progress,
        "snapshot_complete": complete,
        "next_page_token": next_page_token,
        "resume_token": resume_token,
        "source_final": false,
        "warnings": [],
    }))
}

fn response_fits(result: &Value, maximum: usize, request_id: &str) -> bool {
    let response = success_response(request_id, result.clone());
    serde_json::to_vec(&response)
        .map(|bytes| bytes.len().saturating_add(1) <= maximum)
        .unwrap_or(false)
}

fn ensure_response_within_budget(
    request_id: &str,
    result: &Value,
    maximum: usize,
) -> Result<(), ProviderFailure> {
    let response = success_response(request_id, result.clone());
    let bytes = serde_json::to_vec(&response).map_err(|error| {
        ProviderFailure::internal(request_id, "session_page_encode_failed", error.to_string())
    })?;
    if bytes.len().saturating_add(1) <= maximum {
        return Ok(());
    }
    Err(ProviderFailure::internal(
        request_id,
        "session_page_response_budget_exceeded",
        "the final compact session page success envelope exceeds max_response_bytes",
    ))
}

impl<'a> SourceAdapter<'a> {
    fn open(
        host: &'a HostContext,
        runtime: NativeRuntimeContext,
        session_id: &str,
        request_id: &'a str,
    ) -> Result<Self, ProviderFailure> {
        let database_path = database_path(host, &runtime, request_id)?;
        let stamp = source_stamp(&database_path)
            .map_err(|error| source_unavailable_failure(request_id, error))?;
        let schema_rows = database_query(
            host,
            &runtime,
            schema_query(),
            MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
            request_id,
        )?;
        ensure_same_stamp(&database_path, &stamp, request_id)?;
        let schema_fingerprint = validate_source_schema(&schema_rows, request_id)?;
        let session = load_session_metadata(
            host,
            &runtime,
            &database_path,
            &stamp,
            session_id,
            request_id,
        )?;
        Ok(Self {
            host,
            runtime,
            database_path,
            stamp,
            schema_fingerprint,
            session,
            request_id,
        })
    }

    fn source_generation(&self) -> String {
        sha256_hex(
            json!({
                "schema": SOURCE_SCHEMA,
                "database_path": self.database_path,
                "device": self.stamp.device,
                "inode": self.stamp.inode,
                "schema_fingerprint": self.schema_fingerprint,
                "session_id": self.session.id,
                "session_created": self.session.created,
            })
            .to_string()
            .as_bytes(),
        )
    }

    fn source_revision(&self) -> Result<String, ProviderFailure> {
        let database = source_file_revision(&self.database_path).map_err(|error| {
            snapshot_invalidated_failure(
                self.request_id,
                format!("could not inspect OpenCode database revision: {error}"),
            )
        })?;
        let wal_path = PathBuf::from(format!("{}-wal", self.database_path.display()));
        let wal = match source_file_revision(&wal_path) {
            Ok(revision) => Some(revision),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(snapshot_invalidated_failure(
                    self.request_id,
                    format!("could not inspect OpenCode WAL revision: {error}"),
                ));
            }
        };
        let encoded = serde_json::to_vec(&(database, wal)).map_err(|error| {
            snapshot_invalidated_failure(
                self.request_id,
                format!("could not encode OpenCode source revision: {error}"),
            )
        })?;
        let digest = <Sha256 as sha2::Digest>::digest(&encoded);
        Ok(encode_base64(&digest[..16]))
    }

    fn rowid_frontiers(&self) -> Result<(u64, u64), ProviderFailure> {
        let rows = self.query(
            "SELECT COALESCE((SELECT MAX(rowid) FROM message),0) AS message_rowid,\
                    COALESCE((SELECT MAX(rowid) FROM part),0) AS part_rowid"
                .to_string(),
            MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
        )?;
        let row = rows.first().ok_or_else(|| {
            source_schema_failure(self.request_id, "OpenCode rowid frontier query was empty")
        })?;
        Ok((
            row_u64(row, "message_rowid", self.request_id)?,
            row_u64(row, "part_rowid", self.request_id)?,
        ))
    }

    fn require_snapshot_guard(
        &self,
        guard: &SnapshotGuard,
        high_watermark: Option<&SourceKey>,
    ) -> Result<(), ProviderFailure> {
        let revision = self.source_revision()?;
        if revision == guard.revision {
            return Ok(());
        }
        let (message_rowid, part_rowid) = self.rowid_frontiers()?;
        if message_rowid < guard.message_rowid
            || part_rowid < guard.part_rowid
            || (message_rowid == guard.message_rowid && part_rowid == guard.part_rowid)
        {
            return Err(snapshot_invalidated_failure(
                self.request_id,
                "OpenCode source changed without a bounded append beyond the snapshot frontier",
            ));
        }
        let Some(high) = high_watermark else {
            return Ok(());
        };
        let changed_message = self.query(
            format!(
                "SELECT id FROM message WHERE rowid>{} AND session_id={} AND \
                   (time_created<{} OR (time_created={} AND id<={})) LIMIT 1",
                guard.message_rowid,
                sql_string(&self.session.id),
                high.created,
                high.created,
                sql_string(&high.id),
            ),
            MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
        )?;
        if !changed_message.is_empty() {
            return Err(snapshot_invalidated_failure(
                self.request_id,
                "OpenCode inserted a backdated message inside the active snapshot",
            ));
        }
        let changed_part = self.query(
            format!(
                "SELECT p.id FROM part AS p LEFT JOIN message AS m ON m.id=p.message_id \
                 WHERE p.rowid>{} AND p.session_id={} AND \
                   (m.id IS NULL OR m.session_id!={} OR m.time_created<{} OR \
                    (m.time_created={} AND m.id<={})) LIMIT 1",
                guard.part_rowid,
                sql_string(&self.session.id),
                sql_string(&self.session.id),
                high.created,
                high.created,
                sql_string(&high.id),
            ),
            MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
        )?;
        if !changed_part.is_empty() {
            return Err(snapshot_invalidated_failure(
                self.request_id,
                "OpenCode inserted or replaced a part inside the active snapshot",
            ));
        }
        Ok(())
    }

    fn require_generation(&self, expected: &str) -> Result<(), ProviderFailure> {
        if expected == self.source_generation() {
            return Ok(());
        }
        Err(snapshot_invalidated_failure(
            self.request_id,
            "OpenCode source generation was replaced or changed incompatibly",
        ))
    }

    fn query(&self, sql: String, maximum: usize) -> Result<Vec<Value>, ProviderFailure> {
        ensure_same_stamp(&self.database_path, &self.stamp, self.request_id)?;
        let rows = database_query(self.host, &self.runtime, sql, maximum, self.request_id)?;
        ensure_same_stamp(&self.database_path, &self.stamp, self.request_id)?;
        Ok(rows)
    }

    fn verify_schema_current(&self) -> Result<(), ProviderFailure> {
        let rows = self.query(schema_query(), MAX_DATABASE_SCHEMA_OUTPUT_BYTES)?;
        let fingerprint = validate_source_schema(&rows, self.request_id)?;
        if fingerprint == self.schema_fingerprint {
            return Ok(());
        }
        Err(snapshot_invalidated_failure(
            self.request_id,
            "OpenCode source schema changed during the page read",
        ))
    }

    fn immutable_tail(&self, work: &mut SourceWork) -> Result<Option<SourceKey>, ProviderFailure> {
        // A later completed row does not seal an earlier incomplete assistant.
        // Find the first gap in source order, then anchor immediately before it.
        let mutable_rows = self.query(
            format!(
                "SELECT id,time_created,time_updated,length(CAST(data AS BLOB)) AS data_bytes \
                 FROM message INDEXED BY message_session_time_created_id_idx \
                 WHERE session_id={} AND \
                   CASE WHEN json_valid(data) THEN \
                     json_extract(data,'$.role')='assistant' AND \
                     COALESCE(json_type(data,'$.time.completed'),'')!='integer' \
                   ELSE 1 END \
                 ORDER BY time_created,id LIMIT 1",
                sql_string(&self.session.id)
            ),
            MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
        )?;
        let mutable = parse_message_metadata(mutable_rows, self.request_id)?;
        let high_rows = if let Some(gap) = mutable.first() {
            let message = self.load_message(gap, work)?;
            if message_is_immutable(&message) {
                return Err(snapshot_invalidated_failure(
                    self.request_id,
                    "OpenCode mutable-prefix boundary changed during snapshot capture",
                ));
            }
            self.query(
                format!(
                    "SELECT id,time_created,time_updated,length(CAST(data AS BLOB)) AS data_bytes \
                     FROM message INDEXED BY message_session_time_created_id_idx \
                     WHERE session_id={} AND \
                       (time_created<{} OR (time_created={} AND id<{})) \
                     ORDER BY time_created DESC,id DESC LIMIT 1",
                    sql_string(&self.session.id),
                    gap.key.created,
                    gap.key.created,
                    sql_string(&gap.key.id)
                ),
                MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
            )?
        } else {
            self.query(
                format!(
                    "SELECT id,time_created,time_updated,length(CAST(data AS BLOB)) AS data_bytes \
                     FROM message INDEXED BY message_session_time_created_id_idx \
                     WHERE session_id={} ORDER BY time_created DESC,id DESC LIMIT 1",
                    sql_string(&self.session.id)
                ),
                MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
            )?
        };
        let candidates = parse_message_metadata(high_rows, self.request_id)?;
        let Some(candidate) = candidates.first() else {
            return Ok(None);
        };
        let message = self.load_message(candidate, work)?;
        if !message_is_immutable(&message) {
            return Err(snapshot_invalidated_failure(
                self.request_id,
                "OpenCode immutable-prefix boundary changed during snapshot capture",
            ));
        }
        Ok(Some(candidate.key.clone()))
    }

    fn message_metadata_page(
        &self,
        cursor: Option<&SourceKey>,
        high: &SourceKey,
        limit: usize,
    ) -> Result<Vec<MessageMetadata>, ProviderFailure> {
        let after = cursor.map_or_else(
            || "1=1".to_string(),
            |cursor| {
                format!(
                    "(time_created>{} OR (time_created={} AND id>{}))",
                    cursor.created,
                    cursor.created,
                    sql_string(&cursor.id)
                )
            },
        );
        let before = format!(
            "(time_created<{} OR (time_created={} AND id<={}))",
            high.created,
            high.created,
            sql_string(&high.id)
        );
        let rows = self.query(
            format!(
                "SELECT id,time_created,time_updated,length(CAST(data AS BLOB)) AS data_bytes \
                 FROM message INDEXED BY message_session_time_created_id_idx \
                 WHERE session_id={} AND {after} AND {before} \
                 ORDER BY time_created,id LIMIT {}",
                sql_string(&self.session.id),
                limit.min(MAX_OBSERVATION_SCAN_ROWS)
            ),
            MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
        )?;
        parse_message_metadata(rows, self.request_id)
    }

    fn load_message(
        &self,
        metadata: &MessageMetadata,
        work: &mut SourceWork,
    ) -> Result<NativeMessage, ProviderFailure> {
        self.load_messages(std::slice::from_ref(metadata), work)?
            .into_iter()
            .next()
            .ok_or_else(|| snapshot_invalidated_failure(self.request_id, "message disappeared"))
    }

    fn load_message_at_key(
        &self,
        key: &SourceKey,
        work: &mut SourceWork,
    ) -> Result<NativeMessage, ProviderFailure> {
        let rows = self.query(
            format!(
                "SELECT id,time_created,time_updated,length(CAST(data AS BLOB)) AS data_bytes \
                 FROM message INDEXED BY message_session_time_created_id_idx \
                 WHERE session_id={} AND time_created={} AND id={} LIMIT 1",
                sql_string(&self.session.id),
                key.created,
                sql_string(&key.id)
            ),
            MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
        )?;
        let metadata = parse_message_metadata(rows, self.request_id)?;
        let metadata = metadata.first().ok_or_else(|| {
            snapshot_invalidated_failure(self.request_id, "snapshot high-watermark disappeared")
        })?;
        self.load_message(metadata, work)
    }

    fn load_messages(
        &self,
        metadata: &[MessageMetadata],
        work: &mut SourceWork,
    ) -> Result<Vec<NativeMessage>, ProviderFailure> {
        if metadata.is_empty() {
            return Ok(Vec::new());
        }
        let wanted = metadata
            .iter()
            .enumerate()
            .map(|(order, row)| {
                format!(
                    "({}, {}, {})",
                    sql_string(&row.key.id),
                    row.key.created,
                    order
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let rows = self.query(
            format!(
                "WITH wanted(id,time_created,source_order) AS (VALUES {wanted}) \
                 SELECT m.id,m.session_id,m.time_created,m.time_updated,\
                        length(CAST(m.data AS BLOB)) AS data_bytes,hex(CAST(m.data AS BLOB)) AS data_hex,\
                        wanted.source_order \
                 FROM wanted JOIN message AS m ON m.id=wanted.id AND m.time_created=wanted.time_created \
                 WHERE m.session_id={} ORDER BY wanted.source_order",
                sql_string(&self.session.id)
            ),
            MAX_DATABASE_QUERY_OUTPUT_BYTES,
        )?;
        if rows.len() != metadata.len() {
            return Err(snapshot_invalidated_failure(
                self.request_id,
                "one bounded OpenCode message disappeared or changed its source key",
            ));
        }
        let mut messages = Vec::with_capacity(rows.len());
        for (expected, row) in metadata.iter().zip(rows) {
            let observed = message_metadata_from_row(&row, self.request_id)?;
            if observed.key != expected.key
                || observed.updated != expected.updated
                || observed.data_bytes != expected.data_bytes
            {
                return Err(snapshot_invalidated_failure(
                    self.request_id,
                    "one bounded OpenCode message changed during metadata capture",
                ));
            }
            let bytes = decode_hex_field(&row, "data_hex", expected.data_bytes, self.request_id)?;
            work.add(bytes.len(), self.request_id)?;
            let data: Value = serde_json::from_slice(&bytes).map_err(|error| {
                source_schema_failure(
                    self.request_id,
                    format!("message {} data is invalid JSON: {error}", expected.key.id),
                )
            })?;
            messages.push(parse_native_message(
                expected.clone(),
                &data,
                self.request_id,
            )?);
        }
        Ok(messages)
    }

    fn classified_parts(
        &self,
        messages: &[SourceKey],
        work: &mut SourceWork,
    ) -> Result<BTreeMap<String, Vec<ClassifiedPart>>, ProviderFailure> {
        let mut result = BTreeMap::new();
        if messages.is_empty() {
            return Ok(result);
        }
        let wanted = messages
            .iter()
            .enumerate()
            .map(|(order, message)| format!("({}, {})", sql_string(&message.id), order))
            .collect::<Vec<_>>()
            .join(",");
        let rows = self.query(
            format!(
                "WITH wanted(message_id,source_order) AS (VALUES {wanted}) \
                 SELECT p.id,p.message_id,p.session_id,p.time_created,p.time_updated,\
                        length(CAST(p.data AS BLOB)) AS data_bytes,wanted.source_order \
                 FROM wanted JOIN part AS p INDEXED BY part_message_id_id_idx ON p.message_id=wanted.message_id \
                 WHERE p.session_id={} ORDER BY wanted.source_order,p.id LIMIT {}",
                sql_string(&self.session.id),
                MAX_PAGE_PART_ROWS + 1
            ),
            MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
        )?;
        let mut metadata = rows
            .into_iter()
            .map(|row| part_metadata_from_row(&row, self.request_id))
            .collect::<Result<Vec<_>, _>>()?;
        let truncated_message = (metadata.len() > MAX_PAGE_PART_ROWS)
            .then(|| metadata[MAX_PAGE_PART_ROWS].message_id.clone());
        metadata.truncate(MAX_PAGE_PART_ROWS);
        let mut counts = BTreeMap::<String, usize>::new();
        for part in &metadata {
            let count = counts.entry(part.message_id.clone()).or_default();
            *count += 1;
            if *count > MAX_PARTS_PER_MESSAGE {
                return Err(turn_metadata_too_large_failure(self.request_id));
            }
        }
        let mut prefix_rows = Vec::new();
        let mut prefix_bytes = 0_usize;
        for part in &metadata {
            if truncated_message.as_deref() == Some(part.message_id.as_str()) {
                continue;
            }
            let bytes = part.data_bytes.min(PART_TYPE_PREFIX_BYTES);
            if prefix_bytes.saturating_add(bytes) > work.remaining() {
                break;
            }
            prefix_bytes += bytes;
            prefix_rows.push(part.clone());
        }
        let prefixes = self.load_part_bytes(&prefix_rows, true)?;
        work.add(prefix_bytes, self.request_id)?;
        let prefixed_ids = prefixes.keys().cloned().collect::<BTreeSet<_>>();
        for message in messages {
            let expected = metadata
                .iter()
                .filter(|part| part.message_id == message.id)
                .collect::<Vec<_>>();
            if truncated_message.as_deref() == Some(message.id.as_str())
                || expected.iter().any(|part| !prefixed_ids.contains(&part.id))
            {
                break;
            }
            let classified = expected
                .into_iter()
                .map(|part| {
                    let prefix = prefixes.get(&part.id).ok_or_else(|| {
                        source_schema_failure(self.request_id, "missing bounded part prefix")
                    })?;
                    let part_type = part_type_from_prefix(prefix).ok_or_else(|| {
                        source_schema_failure(
                            self.request_id,
                            format!(
                                "part {} does not expose type within its bounded metadata prefix",
                                part.id
                            ),
                        )
                    })?;
                    Ok(ClassifiedPart {
                        metadata: (*part).clone(),
                        part_type,
                    })
                })
                .collect::<Result<Vec<_>, ProviderFailure>>()?;
            result.insert(message.id.clone(), classified);
        }
        Ok(result)
    }

    fn project_bodies(
        &self,
        messages: &[SourceKey],
        parts: &BTreeMap<String, Vec<ClassifiedPart>>,
        projection: TurnProjection,
        expected_delivery_nonce: Option<&str>,
        max_inline_body_bytes: usize,
        work: &mut SourceWork,
    ) -> Result<BTreeMap<String, BodyProjection>, ProviderFailure> {
        let mut projections = BTreeMap::new();
        let mut inline_parts = Vec::new();
        let mut inline_messages = BTreeSet::new();
        let mut body_source_bytes = 0_usize;
        for message in messages {
            let Some(message_parts) = parts.get(&message.id) else {
                break;
            };
            let text_parts = message_parts
                .iter()
                .filter(|part| part.part_type == "text")
                .collect::<Vec<_>>();
            if text_parts.is_empty() {
                projections.insert(message.id.clone(), BodyProjection::Absent);
                continue;
            }
            let native_bytes = text_parts.iter().fold(0_usize, |total, part| {
                total.saturating_add(part.metadata.data_bytes)
            });
            if body_source_bytes.saturating_add(native_bytes)
                > work.remaining().min(MAX_NATIVE_PAYLOAD_QUERY_BYTES)
            {
                projections.insert(
                    message.id.clone(),
                    BodyProjection::Omitted {
                        body_bytes: None,
                        body_sha256: None,
                        canonical_text_sha256: None,
                    },
                );
                continue;
            }
            body_source_bytes += native_bytes;
            inline_messages.insert(message.id.clone());
            inline_parts.extend(text_parts.into_iter().map(|part| part.metadata.clone()));
        }
        let bodies = self.load_part_bytes(&inline_parts, false)?;
        work.add(body_source_bytes, self.request_id)?;
        for message in messages {
            if !inline_messages.contains(&message.id) {
                continue;
            }
            let mut body = Vec::new();
            for part in parts
                .get(&message.id)
                .into_iter()
                .flatten()
                .filter(|part| part.part_type == "text")
            {
                let bytes = bodies.get(&part.metadata.id).ok_or_else(|| {
                    snapshot_invalidated_failure(self.request_id, "text part disappeared")
                })?;
                let data: Value = serde_json::from_slice(bytes).map_err(|error| {
                    source_schema_failure(
                        self.request_id,
                        format!("text part {} is invalid JSON: {error}", part.metadata.id),
                    )
                })?;
                if data.get("type").and_then(Value::as_str) != Some("text") {
                    return Err(snapshot_invalidated_failure(
                        self.request_id,
                        "part type changed after bounded metadata capture",
                    ));
                }
                let text = data.get("text").and_then(Value::as_str).ok_or_else(|| {
                    source_schema_failure(
                        self.request_id,
                        format!("text part {} has no string text", part.metadata.id),
                    )
                })?;
                body.push(json!({"type": "text", "text": text}));
            }
            if projection == TurnProjection::UserObservation {
                let persisted_text = normalized_text(&body);
                let launch_normalized_text =
                    crate::resume_observation::strip_quoted_launch_delivery_marker(&persisted_text);
                let expected_delivery_nonce = expected_delivery_nonce.expect(
                    "validated user observation token binding has an expected delivery nonce",
                );
                let logical_text = crate::resume_observation::strip_trailing_delivery_marker(
                    &launch_normalized_text,
                    expected_delivery_nonce,
                );
                if logical_text != persisted_text {
                    body = vec![json!({"type": "text", "text": logical_text})];
                }
            }
            #[derive(Serialize)]
            struct CanonicalTextBodyChunk<'a> {
                #[serde(rename = "type")]
                kind: &'static str,
                text: &'a str,
            }
            let canonical_body = body
                .iter()
                .filter_map(|part| {
                    part.get("text")
                        .and_then(Value::as_str)
                        .map(|text| CanonicalTextBodyChunk { kind: "text", text })
                })
                .collect::<Vec<_>>();
            let encoded = serde_json::to_vec(&canonical_body).map_err(|error| {
                ProviderFailure::internal(
                    self.request_id,
                    "session_body_encode_failed",
                    error.to_string(),
                )
            })?;
            let canonical_text = normalized_text(&body);
            let body_bytes = encoded.len();
            let body_sha256 = sha256_hex(&encoded);
            let canonical_text_sha256 = sha256_hex(canonical_text.as_bytes());
            if body_bytes > max_inline_body_bytes {
                projections.insert(
                    message.id.clone(),
                    BodyProjection::Omitted {
                        body_bytes: Some(body_bytes),
                        body_sha256: Some(body_sha256),
                        canonical_text_sha256: Some(canonical_text_sha256),
                    },
                );
                continue;
            }
            projections.insert(
                message.id.clone(),
                BodyProjection::Inline {
                    body,
                    body_bytes,
                    body_sha256,
                    canonical_text_sha256,
                },
            );
        }
        Ok(projections)
    }

    fn load_part_bytes(
        &self,
        parts: &[PartMetadata],
        prefix_only: bool,
    ) -> Result<BTreeMap<String, Vec<u8>>, ProviderFailure> {
        if parts.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut chunks = Vec::new();
        for (source_order, part) in parts.iter().enumerate() {
            let retained_bytes = if prefix_only {
                part.data_bytes.min(PART_TYPE_PREFIX_BYTES)
            } else {
                part.data_bytes
            };
            if retained_bytes == 0 {
                chunks.push((part, source_order, 0_usize, 0_usize));
                continue;
            }
            let mut offset = 0_usize;
            while offset < retained_bytes {
                let bytes = retained_bytes
                    .saturating_sub(offset)
                    .min(MAX_NATIVE_DATABASE_BATCH_SOURCE_BYTES);
                chunks.push((part, source_order, offset, bytes));
                offset = offset.saturating_add(bytes);
            }
        }

        let mut result = parts
            .iter()
            .map(|part| (part.id.clone(), Vec::new()))
            .collect::<BTreeMap<_, _>>();
        let mut cursor = 0_usize;
        while cursor < chunks.len() {
            let mut end = cursor;
            let mut source_bytes = 0_usize;
            while end < chunks.len() && end.saturating_sub(cursor) < MAX_NATIVE_DATABASE_BATCH_ROWS
            {
                let next = source_bytes.saturating_add(chunks[end].3);
                if end > cursor && next > MAX_NATIVE_DATABASE_BATCH_SOURCE_BYTES {
                    break;
                }
                source_bytes = next;
                end += 1;
            }
            let batch = &chunks[cursor..end];
            let wanted = batch
                .iter()
                .map(|(part, source_order, offset, bytes)| {
                    format!(
                        "({}, {}, {}, {}, {}, {}, {})",
                        sql_string(&part.id),
                        sql_string(&part.message_id),
                        part.updated,
                        part.data_bytes,
                        offset,
                        bytes,
                        source_order
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let rows = self.query(
                format!(
                    "WITH wanted(id,message_id,time_updated,data_bytes,chunk_offset,chunk_bytes,source_order) \
                     AS (VALUES {wanted}) \
                     SELECT p.id,p.message_id,p.time_updated,\
                            length(CAST(p.data AS BLOB)) AS data_bytes,\
                            wanted.chunk_offset,wanted.chunk_bytes,\
                            hex(substr(CAST(p.data AS BLOB),wanted.chunk_offset+1,wanted.chunk_bytes)) AS data_hex,\
                            wanted.source_order \
                     FROM wanted JOIN part AS p ON p.id=wanted.id AND p.message_id=wanted.message_id \
                     WHERE p.session_id={} AND p.time_updated=wanted.time_updated \
                       AND length(CAST(p.data AS BLOB))=wanted.data_bytes \
                     ORDER BY wanted.source_order,wanted.chunk_offset",
                    sql_string(&self.session.id)
                ),
                MAX_DATABASE_QUERY_OUTPUT_BYTES,
            )?;
            if rows.len() != batch.len() {
                return Err(snapshot_invalidated_failure(
                    self.request_id,
                    "one bounded OpenCode part disappeared or changed during chunked capture",
                ));
            }
            for ((expected, _, offset, bytes), row) in batch.iter().zip(rows) {
                let observed = part_metadata_from_row(&row, self.request_id)?;
                if observed.id != expected.id
                    || observed.message_id != expected.message_id
                    || observed.updated != expected.updated
                    || observed.data_bytes != expected.data_bytes
                    || row_usize(&row, "chunk_offset", self.request_id)? != *offset
                    || row_usize(&row, "chunk_bytes", self.request_id)? != *bytes
                {
                    return Err(snapshot_invalidated_failure(
                        self.request_id,
                        "one bounded OpenCode part changed identity during chunked capture",
                    ));
                }
                result
                    .get_mut(&expected.id)
                    .expect("part result was initialized")
                    .extend(decode_hex_field(&row, "data_hex", *bytes, self.request_id)?);
            }
            cursor = end;
        }
        for part in parts {
            let expected_bytes = if prefix_only {
                part.data_bytes.min(PART_TYPE_PREFIX_BYTES)
            } else {
                part.data_bytes
            };
            if result.get(&part.id).map(Vec::len) != Some(expected_bytes) {
                return Err(snapshot_invalidated_failure(
                    self.request_id,
                    "one bounded OpenCode part capture is incomplete",
                ));
            }
        }
        Ok(result)
    }
}

fn database_path(
    host: &HostContext,
    runtime: &NativeRuntimeContext,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let output = run_database_command(
        host,
        runtime,
        &["db", "path"],
        MAX_DATABASE_PATH_OUTPUT_BYTES,
        request_id,
    )?;
    let text = std::str::from_utf8(&output)
        .map_err(|error| source_unavailable_failure(request_id, error))?
        .trim();
    if text.is_empty()
        || text.len() > MAX_DATABASE_PATH_OUTPUT_BYTES
        || !Path::new(text).is_absolute()
    {
        return Err(source_unavailable_failure(
            request_id,
            "opencode db path did not return one bounded absolute path",
        ));
    }
    fs::canonicalize(text).map_err(|error| source_unavailable_failure(request_id, error))
}

fn database_query(
    host: &HostContext,
    runtime: &NativeRuntimeContext,
    sql: String,
    maximum_output: usize,
    request_id: &str,
) -> Result<Vec<Value>, ProviderFailure> {
    let output = run_database_command(
        host,
        runtime,
        &["db", sql.as_str(), "--format", "json"],
        maximum_output,
        request_id,
    )?;
    if output.iter().all(u8::is_ascii_whitespace) {
        return Ok(Vec::new());
    }
    parse_database_rows(&output).map_err(|error| {
        source_schema_failure(
            request_id,
            format!("opencode db output is invalid JSON: {error}"),
        )
    })
}

fn parse_database_rows(output: &[u8]) -> Result<Vec<Value>, serde_json::Error> {
    // Native OpenCode may print one-line startup notices before its formatted
    // JSON. Search line-boundary arrays from the end so a bracketed notice is
    // never mistaken for the database result.
    let candidates = output
        .split_inclusive(|byte| *byte == b'\n')
        .scan(0_usize, |offset, line| {
            let start = *offset;
            *offset = offset.saturating_add(line.len());
            Some((start, line))
        })
        .filter_map(|(line_start, line)| {
            line.iter()
                .position(|byte| !byte.is_ascii_whitespace())
                .filter(|position| line[*position] == b'[')
                .map(|position| line_start.saturating_add(position))
        })
        .collect::<Vec<_>>();

    for start in candidates.into_iter().rev() {
        if let Ok(rows) = serde_json::from_slice::<Vec<Value>>(&output[start..]) {
            return Ok(rows);
        }
    }
    serde_json::from_slice(output)
}

fn run_database_command(
    host: &HostContext,
    runtime: &NativeRuntimeContext,
    args: &[&str],
    maximum_output: usize,
    request_id: &str,
) -> Result<Vec<u8>, ProviderFailure> {
    let timeout =
        operation_bounds::remaining_timeout(host.deadline_unix_ms, DATABASE_COMMAND_TIMEOUT)
            .ok_or_else(|| {
                source_unavailable_failure(request_id, "OpenCode database deadline expired")
            })?;
    let mut command = runtime.command();
    let stable_cwd = runtime
        .stable_execution_env()
        .get("HOME")
        .filter(|value| Path::new(value.as_str()).is_absolute())
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    command
        .args(args)
        .current_dir(stable_cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command
        .spawn()
        .map_err(|error| source_unavailable_failure(request_id, error))?;
    let output = ChildCustody::new(child)
        .wait_with_bounded_output_timeout(timeout, maximum_output, MAX_DATABASE_STDERR_BYTES)
        .map_err(|error| source_unavailable_failure(request_id, error))?
        .ok_or_else(|| {
            source_unavailable_failure(request_id, "OpenCode database query timed out")
        })?;
    if output.stdout.len() > maximum_output || output.stderr.len() > MAX_DATABASE_STDERR_BYTES {
        return Err(source_capacity_failure(request_id));
    }
    if !output.status.success() {
        return Err(source_unavailable_failure(
            request_id,
            format!(
                "OpenCode database command exited with status {:?}: {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }
    Ok(output.stdout)
}

fn schema_query() -> String {
    "SELECT 'session' AS object_name,'column' AS object_kind,cid AS position,name,type,pk \
     FROM pragma_table_info('session') \
     UNION ALL SELECT 'message','column',cid,name,type,pk FROM pragma_table_info('message') \
     UNION ALL SELECT 'part','column',cid,name,type,pk FROM pragma_table_info('part') \
     UNION ALL SELECT 'message_session_time_created_id_idx','index',seqno,name,'',0 \
       FROM pragma_index_info('message_session_time_created_id_idx') \
     UNION ALL SELECT 'part_message_id_id_idx','index',seqno,name,'',0 \
       FROM pragma_index_info('part_message_id_id_idx') \
     ORDER BY object_kind,object_name,position"
        .to_string()
}

fn validate_source_schema(rows: &[Value], request_id: &str) -> Result<String, ProviderFailure> {
    let mut columns = BTreeMap::<String, BTreeMap<String, String>>::new();
    let mut indexes = BTreeMap::<String, Vec<(usize, String)>>::new();
    for row in rows {
        let object = row_string(row, "object_name", request_id)?;
        let kind = row_string(row, "object_kind", request_id)?;
        let name = row_string(row, "name", request_id)?;
        match kind.as_str() {
            "column" => {
                columns.entry(object).or_default().insert(
                    name,
                    row_string(row, "type", request_id)?.to_ascii_uppercase(),
                );
            }
            "index" => indexes
                .entry(object)
                .or_default()
                .push((row_usize(row, "position", request_id)?, name)),
            _ => {
                return Err(source_schema_failure(
                    request_id,
                    "unexpected schema row kind",
                ))
            }
        }
    }
    for (table, required) in [
        (
            "session",
            [
                ("id", "TEXT"),
                ("directory", "TEXT"),
                ("time_created", "INTEGER"),
            ]
            .as_slice(),
        ),
        (
            "message",
            [
                ("id", "TEXT"),
                ("session_id", "TEXT"),
                ("time_created", "INTEGER"),
                ("time_updated", "INTEGER"),
                ("data", "TEXT"),
            ]
            .as_slice(),
        ),
        (
            "part",
            [
                ("id", "TEXT"),
                ("message_id", "TEXT"),
                ("session_id", "TEXT"),
                ("time_created", "INTEGER"),
                ("time_updated", "INTEGER"),
                ("data", "TEXT"),
            ]
            .as_slice(),
        ),
    ] {
        let observed = columns.get(table).ok_or_else(|| {
            source_schema_failure(request_id, format!("required table {table} is absent"))
        })?;
        for (name, kind) in required {
            if observed.get(*name).map(String::as_str) != Some(*kind) {
                return Err(source_schema_failure(
                    request_id,
                    format!("required {table}.{name} {kind} column is absent"),
                ));
            }
        }
    }
    for (index, expected) in [
        (
            "message_session_time_created_id_idx",
            ["session_id", "time_created", "id"].as_slice(),
        ),
        ("part_message_id_id_idx", ["message_id", "id"].as_slice()),
    ] {
        let mut observed = indexes.remove(index).ok_or_else(|| {
            source_schema_failure(request_id, format!("required index {index} is absent"))
        })?;
        observed.sort_by_key(|(position, _)| *position);
        let names = observed
            .iter()
            .map(|(_, name)| name.as_str())
            .collect::<Vec<_>>();
        if names != expected {
            return Err(source_schema_failure(
                request_id,
                format!("required index {index} has incompatible columns"),
            ));
        }
    }
    serde_json::to_vec(rows)
        .map(|bytes| sha256_hex(&bytes))
        .map_err(|error| {
            ProviderFailure::internal(
                request_id,
                "session_source_schema_encode_failed",
                error.to_string(),
            )
        })
}

fn load_session_metadata(
    host: &HostContext,
    runtime: &NativeRuntimeContext,
    database_path: &Path,
    stamp: &SourceStamp,
    session_id: &str,
    request_id: &str,
) -> Result<SessionMetadata, ProviderFailure> {
    let rows = database_query(
        host,
        runtime,
        format!(
            "SELECT id,CASE WHEN length(CAST(directory AS BLOB))<={MAX_SESSION_DIRECTORY_BYTES} \
                    THEN directory ELSE NULL END AS directory,\
                    length(CAST(directory AS BLOB)) AS directory_bytes,time_created \
             FROM session WHERE id={} LIMIT 1",
            sql_string(session_id)
        ),
        MAX_DATABASE_SCHEMA_OUTPUT_BYTES,
        request_id,
    )?;
    ensure_same_stamp(database_path, stamp, request_id)?;
    let row = rows.first().ok_or_else(|| {
        ProviderFailure::invalid_request(
            request_id,
            "session_not_found",
            format!("OpenCode session {session_id} was not found"),
        )
    })?;
    if row_usize(row, "directory_bytes", request_id)? > MAX_SESSION_DIRECTORY_BYTES {
        return Err(turn_metadata_too_large_failure(request_id));
    }
    let id = row_string(row, "id", request_id)?;
    let directory = row_string(row, "directory", request_id)?;
    if id != session_id || id.len() > MAX_ID_BYTES || directory.is_empty() {
        return Err(source_schema_failure(
            request_id,
            "OpenCode session metadata identity or directory is invalid",
        ));
    }
    Ok(SessionMetadata {
        id,
        directory,
        created: row_u64(row, "time_created", request_id)?,
    })
}

fn parse_message_metadata(
    rows: Vec<Value>,
    request_id: &str,
) -> Result<Vec<MessageMetadata>, ProviderFailure> {
    rows.iter()
        .map(|row| message_metadata_from_row(row, request_id))
        .collect()
}

fn message_metadata_from_row(
    row: &Value,
    request_id: &str,
) -> Result<MessageMetadata, ProviderFailure> {
    let id = row_string(row, "id", request_id)?;
    let data_bytes = row_usize(row, "data_bytes", request_id)?;
    if id.is_empty() || id.len() > MAX_ID_BYTES || data_bytes > MAX_MESSAGE_METADATA_BYTES {
        return Err(turn_metadata_too_large_failure(request_id));
    }
    Ok(MessageMetadata {
        key: SourceKey {
            created: row_u64(row, "time_created", request_id)?,
            id,
        },
        updated: row_u64(row, "time_updated", request_id)?,
        data_bytes,
    })
}

fn parse_native_message(
    metadata: MessageMetadata,
    data: &Value,
    request_id: &str,
) -> Result<NativeMessage, ProviderFailure> {
    let role = data
        .get("role")
        .and_then(Value::as_str)
        .filter(|role| matches!(*role, "user" | "assistant"))
        .ok_or_else(|| source_schema_failure(request_id, "message role is unsupported"))?
        .to_string();
    if role.len() > MAX_ROLE_BYTES {
        return Err(turn_metadata_too_large_failure(request_id));
    }
    let created = data
        .pointer("/time/created")
        .and_then(Value::as_u64)
        .unwrap_or(metadata.key.created);
    if created != metadata.key.created {
        return Err(snapshot_invalidated_failure(
            request_id,
            "message source key disagrees with data.time.created",
        ));
    }
    let completed = data.pointer("/time/completed").and_then(Value::as_u64);
    let parent_id = data
        .get("parentID")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    if parent_id
        .as_deref()
        .is_some_and(|id| id.len() > MAX_ID_BYTES)
    {
        return Err(turn_metadata_too_large_failure(request_id));
    }
    Ok(NativeMessage {
        metadata,
        role,
        parent_id,
        created,
        completed,
    })
}

fn message_is_immutable(message: &NativeMessage) -> bool {
    message.role == "user" || message.completed.is_some()
}

fn part_metadata_from_row(row: &Value, request_id: &str) -> Result<PartMetadata, ProviderFailure> {
    let id = row_string(row, "id", request_id)?;
    let message_id = row_string(row, "message_id", request_id)?;
    if id.is_empty()
        || id.len() > MAX_ID_BYTES
        || message_id.is_empty()
        || message_id.len() > MAX_ID_BYTES
    {
        return Err(turn_metadata_too_large_failure(request_id));
    }
    Ok(PartMetadata {
        id,
        message_id,
        updated: row_u64(row, "time_updated", request_id)?,
        data_bytes: row_usize(row, "data_bytes", request_id)?,
    })
}

fn part_type_from_prefix(prefix: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(prefix).ok()?;
    let marker = "\"type\"";
    let start = text.find(marker)? + marker.len();
    let value = text[start..].trim_start().strip_prefix(':')?.trim_start();
    if !value.starts_with('"') {
        return None;
    }
    let mut escaped = false;
    for (offset, byte) in value.as_bytes()[1..].iter().enumerate() {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            return serde_json::from_str(&value[..=offset + 1]).ok();
        }
    }
    None
}

fn normalized_text(body: &[Value]) -> String {
    let combined = body
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<String>();
    combined
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .trim()
        .to_string()
}

fn stable_turn_id(session_id: &str, native_message_id: &str) -> String {
    let preimage = format!("opencode-turn\0{session_id}\0{native_message_id}");
    format!("turn_{}", sha256_hex(preimage.as_bytes()))
}

fn snapshot_id(
    binding: &TokenBinding,
    cursor: Option<&SourceKey>,
    high_watermark: Option<&SourceKey>,
    snapshot_guard: &SnapshotGuard,
) -> String {
    sha256_hex(
        serde_json::to_string(&json!({
            "binding": binding,
            "cursor": cursor,
            "high_watermark": high_watermark,
            "snapshot_guard": snapshot_guard,
        }))
        .expect("snapshot identity serialization is infallible")
        .as_bytes(),
    )
}

fn encode_token<T: Serialize>(
    value: &T,
    token_key: &[u8; TOKEN_KEY_BYTES],
    runtime_identity: &str,
    request_id: &str,
) -> Result<String, ProviderFailure> {
    let payload = serde_json::to_vec(value).map_err(|error| {
        ProviderFailure::internal(request_id, "session_token_encode_failed", error.to_string())
    })?;
    let encoded = encode_base64(&payload);
    let digest = token_digest(token_key, runtime_identity, &payload);
    let token = format!("{TOKEN_PREFIX}.{encoded}.{digest}");
    if token.len() > MAX_TOKEN_BYTES {
        return Err(ProviderFailure::internal(
            request_id,
            "session_token_capacity_exceeded",
            "bounded session page token exceeds its protocol field limit",
        ));
    }
    Ok(token)
}

fn session_token_key(
    host: &HostContext,
    request_id: &str,
) -> Result<[u8; TOKEN_KEY_BYTES], ProviderFailure> {
    let data_root = host
        .data_root
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| token_key_failure(request_id, "host.data_root is required"))?;
    let path = path_guard::confined_target(data_root, &data_root.join(TOKEN_KEY_PATH))
        .map_err(|error| token_key_failure(request_id, error))?;
    match read_token_key(&path) {
        Ok(key) => return Ok(key),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(token_key_failure(request_id, error)),
    }

    let parent = path
        .parent()
        .expect("session token key path always has a parent");
    durable_fs::create_private_directories(parent)
        .map_err(|error| token_key_failure(request_id, error))?;
    let mut key = [0_u8; TOKEN_KEY_BYTES];
    getrandom::fill(&mut key).map_err(|error| token_key_failure(request_id, error))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| token_key_failure(request_id, error))?;
    temporary
        .write_all(&key)
        .map_err(|error| token_key_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(|error| token_key_failure(request_id, error))?;
    match temporary.persist_noclobber(&path) {
        Ok(_) => {
            durable_fs::sync_directory(parent)
                .map_err(|error| token_key_failure(request_id, error))?;
            read_token_key(&path).map_err(|error| token_key_failure(request_id, error))
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_token_key(&path).map_err(|error| token_key_failure(request_id, error))
        }
        Err(error) => Err(token_key_failure(request_id, error.error)),
    }
}

fn read_token_key(path: &Path) -> std::io::Result<[u8; TOKEN_KEY_BYTES]> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "session token key is not a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "session token key permissions are not private",
            ));
        }
    }
    let bytes = durable_fs::read_file_bounded(path, TOKEN_KEY_BYTES)?;
    bytes.try_into().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("session token key must contain exactly {TOKEN_KEY_BYTES} bytes"),
        )
    })
}

fn decode_token<T: for<'de> Deserialize<'de>>(
    token: &str,
    expected_kind: &str,
    token_key: &[u8; TOKEN_KEY_BYTES],
    runtime_identity: &str,
    request_id: &str,
) -> Result<T, ProviderFailure> {
    if token.len() > MAX_TOKEN_BYTES {
        return Err(invalid_token_failure(
            request_id,
            "token exceeds its field bound",
        ));
    }
    if token.starts_with(&format!("{LEGACY_TOKEN_PREFIX}.")) {
        return Err(snapshot_invalidated_failure(
            request_id,
            "session paging token authority was upgraded",
        ));
    }
    let fields = token.split('.').collect::<Vec<_>>();
    if fields.len() != 3 || fields[0] != TOKEN_PREFIX || fields[2].len() != 64 {
        return Err(invalid_token_failure(request_id, "token is malformed"));
    }
    let payload = decode_base64(fields[1]).map_err(|error| {
        invalid_token_failure(request_id, format!("token payload is invalid: {error}"))
    })?;
    if encode_base64(&payload) != fields[1] {
        return Err(invalid_token_failure(
            request_id,
            "token payload encoding is not canonical",
        ));
    }
    if !verify_token_digest(token_key, runtime_identity, &payload, fields[2]) {
        return Err(invalid_token_failure(
            request_id,
            "token integrity check failed",
        ));
    }
    let value: Value = serde_json::from_slice(&payload).map_err(|error| {
        invalid_token_failure(request_id, format!("token JSON is invalid: {error}"))
    })?;
    if value.get("v").and_then(Value::as_u64) != Some(u64::from(TOKEN_VERSION))
        || value.get("k").and_then(Value::as_str) != Some(expected_kind)
    {
        return Err(invalid_token_failure(
            request_id,
            "token version or kind is invalid",
        ));
    }
    serde_json::from_value(value).map_err(|error| {
        invalid_token_failure(request_id, format!("token fields are invalid: {error}"))
    })
}

fn token_digest(
    token_key: &[u8; TOKEN_KEY_BYTES],
    runtime_identity: &str,
    payload: &[u8],
) -> String {
    token_mac(token_key, runtime_identity, payload)
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn verify_token_digest(
    token_key: &[u8; TOKEN_KEY_BYTES],
    runtime_identity: &str,
    payload: &[u8],
    encoded_digest: &str,
) -> bool {
    let Some(digest) = decode_hex_digest(encoded_digest) else {
        return false;
    };
    token_mac(token_key, runtime_identity, payload)
        .verify_slice(&digest)
        .is_ok()
}

fn token_mac(
    token_key: &[u8; TOKEN_KEY_BYTES],
    runtime_identity: &str,
    payload: &[u8],
) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(token_key).expect("HMAC accepts a 32-byte key");
    mac.update(TOKEN_DOMAIN);
    mac.update(runtime_identity.as_bytes());
    mac.update(&[0]);
    mac.update(payload);
    mac
}

fn decode_hex_digest(encoded: &str) -> Option<[u8; 32]> {
    if encoded.len() != 64
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let high = (pair[0] as char).to_digit(16)? as u8;
        let low = (pair[1] as char).to_digit(16)? as u8;
        digest[index] = (high << 4) | low;
    }
    Some(digest)
}

fn source_key_cmp(left: &SourceKey, right: &SourceKey) -> std::cmp::Ordering {
    (left.created, left.id.as_str()).cmp(&(right.created, right.id.as_str()))
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn row_string(row: &Value, field: &str, request_id: &str) -> Result<String, ProviderFailure> {
    row.get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            source_schema_failure(
                request_id,
                format!("database row field {field} is not a string"),
            )
        })
}

fn row_u64(row: &Value, field: &str, request_id: &str) -> Result<u64, ProviderFailure> {
    row.get(field).and_then(Value::as_u64).ok_or_else(|| {
        source_schema_failure(
            request_id,
            format!("database row field {field} is not an unsigned integer"),
        )
    })
}

fn row_usize(row: &Value, field: &str, request_id: &str) -> Result<usize, ProviderFailure> {
    usize::try_from(row_u64(row, field, request_id)?)
        .map_err(|error| source_schema_failure(request_id, error))
}

fn decode_hex_field(
    row: &Value,
    field: &str,
    expected_bytes: usize,
    request_id: &str,
) -> Result<Vec<u8>, ProviderFailure> {
    let encoded = row_string(row, field, request_id)?;
    if encoded.len() != expected_bytes.saturating_mul(2) {
        return Err(snapshot_invalidated_failure(
            request_id,
            format!("database field {field} changed length during bounded read"),
        ));
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_value(pair[0])?;
            let low = hex_value(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect::<Result<Vec<_>, ()>>()
        .map_err(|_| {
            source_schema_failure(
                request_id,
                format!("database field {field} is not hexadecimal"),
            )
        })
}

fn hex_value(byte: u8) -> Result<u8, ()> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(()),
    }
}

#[cfg(unix)]
fn source_file_revision(path: &Path) -> std::io::Result<SourceFileRevision> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "OpenCode source revision path is not a regular file",
        ));
    }
    Ok(SourceFileRevision {
        len: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

#[cfg(not(unix))]
fn source_file_revision(_path: &Path) -> std::io::Result<SourceFileRevision> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "bounded OpenCode source revision identity currently requires Unix metadata",
    ))
}

#[cfg(unix)]
fn source_stamp(path: &Path) -> std::io::Result<SourceStamp> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "OpenCode database path is not a regular file",
        ));
    }
    Ok(SourceStamp {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn source_stamp(_path: &Path) -> std::io::Result<SourceStamp> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "bounded OpenCode SQLite paging currently requires Unix file generation identity",
    ))
}

fn ensure_same_stamp(
    path: &Path,
    expected: &SourceStamp,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let observed =
        source_stamp(path).map_err(|error| snapshot_invalidated_failure(request_id, error))?;
    if &observed == expected {
        return Ok(());
    }
    Err(snapshot_invalidated_failure(
        request_id,
        "OpenCode database source was replaced",
    ))
}

fn invalid_params_failure(request_id: &str, message: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_session_read_turns_params",
        format!("session.read_turns paging params are invalid: {message}"),
    )
}

fn invalid_token_failure(request_id: &str, message: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_session_page_token",
        format!("session.read_turns token is invalid: {message}"),
    )
}

fn token_key_failure(request_id: &str, message: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "session_token_authority_unavailable",
        format!("provider-owned session token authority is unavailable: {message}"),
    )
}

fn snapshot_invalidated_failure(
    request_id: &str,
    message: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::retryable_conflict(
        request_id,
        "snapshot_invalidated",
        message.to_string(),
        json!({"required_action": "restart from the last durable resume token or source beginning"}),
    )
}

fn source_unavailable_failure(
    request_id: &str,
    message: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "opencode_session_source_unavailable",
        format!("bounded OpenCode session source is unavailable: {message}"),
    )
}

fn source_schema_failure(request_id: &str, message: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "opencode_session_source_schema_unsupported",
        format!("OpenCode SQLite source does not match {SOURCE_SCHEMA}: {message}"),
    )
}

fn source_capacity_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "opencode_session_source_output_exceeded",
        "bounded OpenCode database query exceeded its internal output limit",
    )
}

fn source_budget_failure(request_id: &str, message: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "session_source_budget_exceeded",
        message.to_string(),
    )
}

fn turn_metadata_too_large_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "turn_metadata_too_large",
        "one OpenCode turn exceeds the bounded paging metadata envelope",
    )
}

fn capture_params_failure(request_id: &str, message: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_session_capture_params",
        format!("session.capture params are invalid: {message}"),
    )
}
