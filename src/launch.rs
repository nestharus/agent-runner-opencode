//! Declared roles: orchestration, formatter, parser, mapper, validator, accessor, filter, predicate
//! adapter_declarations:
//!   - component: src/launch.rs
//!     role: adapter
//!     Translates:
//!       - opencode process lifecycle to contract/v1 launch NDJSON
//!       - opencode stdout/stderr bytes to LaunchStdoutEvent/LaunchStderrEvent
//!       - opencode sessionID metadata to LaunchMarkerEvent
//!       - declared params.env entries and host-linkage env to env-cleared child env
//!       - process terminal status to LaunchExitEvent

use crate::activity::ActivityTargets;
use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::encoding::{bounded_text, decode_base64, encode_base64, now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure, CONTRACT};
use crate::opencode::{self, first_session_id, EventParser, OpencodeEventMetadata};
use crate::path_guard;
use crate::policy;
use crate::resume_observation::{
    self, DurableResumeObservationRequest, ResumeObservation, ResumeObservationRequest,
};
use crate::terminal::{classify, exit_code_for_status, process_status_json, ProcessStatus};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const TERMINATION_GRACE: Duration = Duration::from_millis(100);
const COMPLETED_RESUME_GRACE: Duration = Duration::from_millis(500);
const RESUME_COMPLETION_PROBE_DELAY: Duration = Duration::from_secs(1);
const RESUME_COMPLETION_PROBE_INTERVAL: Duration = Duration::from_millis(500);
const MAX_RESUME_COMPLETION_PROBES: usize = 3;
const DRAIN_COMPLETION_GRACE: Duration = Duration::from_millis(500);
const DRAIN_CHANNEL_CAPACITY: usize = 32;
const TERMINAL_CAPTURE_LIMIT: usize = 1024 * 1024;
const DEFERRED_EVENT_LIMIT: usize = 1024 * 1024;
const BASE_LAUNCH_ENV_PASSTHROUGH_KEYS: &[&str] = &["PATH", "HOME"];
// Step-6a host-linkage contract: these runner bindings must survive env_clear.
const HOST_LINKAGE_ENV_KEYS: &[&str] = &[
    "OULIPOLY_DATA_DIR",
    "OULIPOLY_PARENT_INVOCATION",
    "AGENT_BASH_AGENT_RUNNER_BIN",
];
const OPENCODE_SESSION_FLAG: &str = "--session";
const OPENCODE_RUN_ARG: &str = "run";
const POLICY_MANAGED_FLAGS_WITH_VALUE: &[&str] = &["--format", "-m", "--variant"];
const POLICY_MANAGED_FLAGS_WITHOUT_VALUE: &[&str] = &["--dangerously-skip-permissions"];
const PRODUCED_ASSISTANT_RESPONSE_MARKER: &str = "oulipoly.produced_assistant_response";
const SUBMITTED_USER_TURN_MARKER: &str = "oulipoly.submitted_user_turn";
const RESUME_COMPLETION_UNRESOLVED_MARKER: &str = "oulipoly.resume_completion_unresolved";
const PROVIDER_SESSION_MARKER: &str = "oulipoly.provider_session";
const TERMINAL_SIGNAL_EVIDENCE_MAX_LEN: usize = 160;
const LAUNCH_STATE_DIR: &str = "provider-state/opencode/launch/requests";
pub const OPENCODE_PROMPT_ARG_BYTE_CEILING: usize = 64 * 1024;

#[derive(Deserialize)]
struct LaunchParams {
    settings_id: String,
    mode: String,
    model: policy::PolicyModelRequest,
    argv: Vec<String>,
    working_directory: String,
    env: Option<BTreeMap<String, String>>,
    stdin: Option<BytePayload>,
    session: Option<LaunchSession>,
}

#[derive(Deserialize)]
struct LaunchSession {
    known_provider_session_id: Option<String>,
    start_mode: Option<String>,
    #[serde(flatten)]
    _extra: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
struct BytePayload {
    encoding: String,
    data: String,
}

enum DrainMessage {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    StdoutDone,
    StderrDone,
    ReadError { stdout: bool, message: String },
}

#[derive(Serialize, Deserialize)]
struct LaunchRequestState {
    schema_version: u32,
    operation_kind: LaunchOperationKind,
    request_id: String,
    binding_sha256: String,
    prompt_sha256: Option<String>,
    recovery: LaunchRecoveryIdentity,
    phase: LaunchRequestPhase,
    provider_session_id: Option<String>,
    terminal_status: Option<Value>,
    prepared_at_unix_ms: u64,
    observed_at_unix_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LaunchOperationKind {
    NewSession,
    Resume,
}

#[derive(Deserialize)]
struct LaunchOperationDiscriminator {
    operation_kind: LaunchOperationKind,
}

#[derive(Clone, Serialize, Deserialize)]
struct LaunchRecoveryIdentity {
    program: String,
    passthrough_env: BTreeMap<String, String>,
    declared_env_sha256: String,
    working_directory: String,
    provider_id: String,
    model_id: String,
    effort: String,
}

struct LaunchRecoveryContext {
    identity: LaunchRecoveryIdentity,
    declared_env: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LaunchRequestPhase {
    Prepared,
    SessionObserved,
    TerminalWithoutSession,
}

struct LaunchRequestGuard {
    state_path: PathBuf,
    state: LaunchRequestState,
    _lock: fs::File,
}

#[derive(Serialize, Deserialize)]
struct ResumeLaunchRequestState {
    schema_version: u32,
    operation_kind: LaunchOperationKind,
    request_id: String,
    binding_sha256: String,
    observation: DurableResumeObservationRequest,
    recovery: LaunchRecoveryIdentity,
    phase: ResumeLaunchRequestPhase,
    terminal_status: Option<Value>,
    prepared_at_unix_ms: u64,
    observed_at_unix_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResumeLaunchRequestPhase {
    Prepared,
    SubmissionObserved,
    CompletionObserved,
    Unresolved,
    TerminalWithoutSubmission,
}

struct ResumeLaunchRequestGuard {
    state_path: PathBuf,
    state: ResumeLaunchRequestState,
    declared_env: BTreeMap<String, String>,
    _lock: fs::File,
}

enum PreparedLaunchRecovery {
    NoEffectObserved,
    SessionObserved(String),
    Ambiguous(Vec<String>),
}

pub(crate) struct LaunchOutcome {
    pub exit_code: i32,
    pub activity_targets: ActivityTargets,
}

pub(crate) fn stream<W: Write>(
    request_id: &str,
    host: &HostContext,
    params: Value,
    writer: &mut W,
) -> Result<LaunchOutcome, ProviderFailure> {
    let raw_params = params.clone();
    let params = parse_launch_params(params, request_id)?;
    let effective = match launch_argv(&params, host, request_id)? {
        PolicyLaunch::Accepted(effective) => *effective,
        PolicyLaunch::Rejected(reason) => {
            return stream_policy_rejection(request_id, writer, reason).map(|exit_code| {
                LaunchOutcome {
                    exit_code,
                    activity_targets: ActivityTargets::default(),
                }
            })
        }
    };
    let new_session = known_provider_session_id(&params).is_none();
    let binding_sha256 =
        launch_request_binding_sha256(host, &raw_params, &effective.route_evidence);
    let prompt_sha256 = effective
        .prompt
        .as_deref()
        .map(|prompt| sha256_hex(prompt.as_bytes()));
    let recovery = launch_recovery_context(
        &effective.argv,
        &params.working_directory,
        &effective.env,
        &effective.route,
    );
    let resume_durable_identity = effective
        .resume_observation_request
        .as_ref()
        .map(ResumeObservationRequest::durable_identity);
    let mut state = LaunchState::new(
        request_id,
        host.deadline_unix_ms,
        effective.resume_observation_request,
        effective.route_evidence.clone(),
        None,
    );
    let child_stdin = match prepared_child_stdin(effective.stdin.as_deref()) {
        Ok(stdin) => stdin,
        Err(error) => {
            state.emit_route_evidence(writer)?;
            state.final_status = Some(spawn_error_status(error));
            state.mark_drains_done();
            return state.finish(writer).map(|exit_code| LaunchOutcome {
                exit_code,
                activity_targets: effective.activity_targets,
            });
        }
    };
    let (launch_request, resume_launch_request) = if new_session {
        (
            Some(LaunchRequestGuard::prepare(
                host,
                request_id,
                binding_sha256,
                prompt_sha256,
                recovery,
            )?),
            None,
        )
    } else {
        let observation = resume_durable_identity.ok_or_else(|| {
            launch_state_invalid(
                request_id,
                "resumed-session launch has no durable observation identity",
            )
        })?;
        (
            None,
            Some(ResumeLaunchRequestGuard::prepare(
                host,
                request_id,
                binding_sha256,
                observation,
                recovery,
            )?),
        )
    };
    if let Err(failure) = state.emit_route_evidence(writer) {
        if let Some(launch_request) = launch_request {
            launch_request.abandon_before_spawn()?;
        }
        if let Some(resume_launch_request) = resume_launch_request {
            resume_launch_request.abandon_before_spawn()?;
        }
        return Err(failure);
    }
    if let Some(launch_request) = launch_request {
        state.attach_launch_request(launch_request);
    }
    if let Some(resume_launch_request) = resume_launch_request {
        state.attach_resume_launch_request(resume_launch_request);
    }
    let child = match spawn_child(
        &effective.argv,
        &params.working_directory,
        &effective.env,
        child_stdin,
    ) {
        Ok(child) => child,
        Err(err) => {
            if let Some(launch_request) = state.launch_request.take() {
                launch_request.abandon_before_spawn()?;
            }
            if let Some(resume_launch_request) = state.resume_launch_request.take() {
                resume_launch_request.abandon_before_spawn()?;
            }
            state.final_status = Some(spawn_error_status(err));
            state.mark_drains_done();
            return state.finish(writer).map(|exit_code| LaunchOutcome {
                exit_code,
                activity_targets: effective.activity_targets,
            });
        }
    };
    let mut custody = ChildCustody::with_cleanup(child, |child| {
        let _ = terminate_child(child);
    });
    stream_child(custody.child_mut(), &mut state, writer).map(|exit_code| LaunchOutcome {
        exit_code,
        activity_targets: effective.activity_targets,
    })
}

pub(crate) fn attempted_activity_targets(params: &Value) -> ActivityTargets {
    let mut targets = ActivityTargets::default();
    let settings_id = params
        .get("settings_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if let Some(settings_id) = settings_id {
        targets.attempted("settings_record", settings_id, "params.settings_id");
    }
    if let Some(model_name) = params
        .pointer("/model/name")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        targets.attempted("model_alias", model_name, "params.model.name");
    }
    if let Some(provider_args) = params.pointer("/model/provider_args") {
        targets.provider_args(provider_args);
    }
    if let Some(provider_session_id) = params
        .pointer("/session/known_provider_session_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        targets.attempted(
            "provider_session",
            provider_session_id,
            "params.session.known_provider_session_id",
        );
    }
    targets
}

fn parse_launch_params(params: Value, request_id: &str) -> Result<LaunchParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_launch_params_failure(request_id, err))
}

struct EffectiveLaunch {
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    stdin: Option<Vec<u8>>,
    prompt: Option<String>,
    resume_observation_request: Option<ResumeObservationRequest>,
    route: policy::PolicyRouteIdentity,
    route_evidence: Value,
    activity_targets: ActivityTargets,
}

enum PolicyLaunch {
    Accepted(Box<EffectiveLaunch>),
    Rejected(String),
}

fn launch_argv(
    params: &LaunchParams,
    host: &HostContext,
    request_id: &str,
) -> Result<PolicyLaunch, ProviderFailure> {
    validate_launch_session(params, request_id)?;
    let stdin = policy_stdin_for_launch(params.stdin.as_ref(), request_id)?;
    let decision = policy::evaluate_launch(
        host,
        policy::PolicyLaunchRequest {
            settings_id: params.settings_id.clone(),
            mode: params.mode.clone(),
            model: params.model.clone(),
            argv: params.argv.clone(),
            env: params.env.clone(),
            stdin,
        },
        request_id,
    )?;
    match decision {
        policy::PolicyDecision::Accepted(plan) => Ok(PolicyLaunch::Accepted(Box::new(
            effective_launch(params, plan, host.deadline_unix_ms, request_id)?,
        ))),
        policy::PolicyDecision::Rejected(plan) => {
            Ok(PolicyLaunch::Rejected(policy_rejection_reason(&plan)))
        }
    }
}

fn effective_launch(
    params: &LaunchParams,
    plan: policy::PolicyLaunchPlan,
    deadline_unix_ms: Option<u64>,
    request_id: &str,
) -> Result<EffectiveLaunch, ProviderFailure> {
    let policy::PolicyLaunchPlan {
        argv,
        env,
        stdin,
        prompt,
        diagnostics: _,
        markers,
        route,
    } = plan;
    validate_policy_argv(&argv, request_id)?;
    let stdin = stdin.map(String::into_bytes);
    let argv = resume_argv(
        params,
        argv,
        stdin.as_deref(),
        prompt.as_deref(),
        request_id,
    )?;
    let resume_observation_request = resume_observation_request(
        params,
        stdin.as_deref(),
        prompt.as_deref(),
        &argv,
        deadline_unix_ms,
        &route,
    );
    let mut activity_targets = ActivityTargets::default();
    policy::append_route_activity_targets(&mut activity_targets, &route);
    let argv = split_oversized_prompt_argv(argv, request_id)?;
    Ok(EffectiveLaunch {
        argv,
        env,
        stdin,
        prompt,
        resume_observation_request,
        route,
        route_evidence: json!(markers),
        activity_targets,
    })
}

fn split_oversized_prompt_argv(
    mut argv: Vec<String>,
    request_id: &str,
) -> Result<Vec<String>, ProviderFailure> {
    let Some((message_start, has_boundary)) = opencode_message_region(&argv) else {
        return Ok(argv);
    };
    if !argv[message_start..]
        .iter()
        .any(|arg| arg.len() >= OPENCODE_PROMPT_ARG_BYTE_CEILING)
    {
        return Ok(argv);
    }
    let tokens = argv[message_start..]
        .iter()
        .flat_map(|arg| arg.split(' '))
        .map(|token| validate_prompt_token(token, request_id))
        .collect::<Result<Vec<_>, _>>()?;
    argv.truncate(message_start);
    if !has_boundary {
        argv.push("--".to_string());
    }
    argv.extend(tokens);
    Ok(argv)
}

fn validate_prompt_token(token: &str, request_id: &str) -> Result<String, ProviderFailure> {
    if token.len() >= OPENCODE_PROMPT_ARG_BYTE_CEILING {
        return Err(oversized_prompt_token_failure(request_id));
    }
    Ok(token.to_string())
}

fn oversized_prompt_token_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "oversized_prompt_token",
        format!(
            "opencode positional message contains a token of at least {} UTF-8 bytes without an ASCII space; cannot keep each positional message argument below the {}-byte ceiling",
            OPENCODE_PROMPT_ARG_BYTE_CEILING,
            OPENCODE_PROMPT_ARG_BYTE_CEILING,
        ),
    )
}

fn resume_argv(
    params: &LaunchParams,
    mut argv: Vec<String>,
    stdin: Option<&[u8]>,
    prompt: Option<&str>,
    request_id: &str,
) -> Result<Vec<String>, ProviderFailure> {
    let Some(session_id) = known_provider_session_id(params) else {
        return Ok(argv);
    };
    require_resume_payload_reaches_child(&argv, stdin, prompt, request_id)?;
    let insert_at = resume_session_insert_index(&argv);
    upsert_session_arg(&mut argv, session_id, insert_at);
    Ok(argv)
}

fn resume_observation_request(
    params: &LaunchParams,
    stdin: Option<&[u8]>,
    prompt: Option<&str>,
    argv: &[String],
    deadline_unix_ms: Option<u64>,
    route: &policy::PolicyRouteIdentity,
) -> Option<ResumeObservationRequest> {
    let session_id = known_provider_session_id(params)?;
    let prompt = submitted_resume_payload(argv, stdin, prompt)?;
    Some(ResumeObservationRequest::new(
        route.account_wrapper.clone(),
        session_id.to_string(),
        prompt,
        now_unix_ms(),
        deadline_unix_ms,
        route.provider_id.clone(),
        route.model_id.clone(),
        route.effort.to_string(),
    ))
}

fn known_provider_session_id(params: &LaunchParams) -> Option<&str> {
    params
        .session
        .as_ref()
        .and_then(|session| session.known_provider_session_id.as_deref())
        .filter(|session_id| !session_id.trim().is_empty())
}

fn validate_launch_session(params: &LaunchParams, request_id: &str) -> Result<(), ProviderFailure> {
    let Some(session) = params.session.as_ref() else {
        return Ok(());
    };
    match (
        session
            .known_provider_session_id
            .as_deref()
            .filter(|session_id| !session_id.trim().is_empty()),
        session.start_mode.as_deref(),
    ) {
        (None, None) => Ok(()),
        (Some(_), Some("resume")) => Ok(()),
        (Some(_), Some("create")) => Err(ProviderFailure::unsupported(
            request_id,
            "launch_session_create_unsupported",
            "OpenCode cannot start a new session with a caller-selected provider session id",
        )),
        _ => Err(ProviderFailure::invalid_request(
            request_id,
            "invalid_launch_session",
            "launch session requires a non-empty known_provider_session_id paired with start_mode resume or create",
        )),
    }
}

fn require_resume_payload_reaches_child(
    argv: &[String],
    stdin: Option<&[u8]>,
    prompt: Option<&str>,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if submitted_resume_payload(argv, stdin, prompt).is_some() {
        return Ok(());
    }
    Err(empty_resume_payload_failure(request_id))
}

fn submitted_resume_payload(
    argv: &[String],
    stdin: Option<&[u8]>,
    prompt: Option<&str>,
) -> Option<String> {
    stdin
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .filter(|text| !text.trim().is_empty())
        .map(str::to_string)
        .or_else(|| prompt_arg_payload(argv, prompt))
        .or_else(|| argv_payload_after_resume_session_insert_index(argv).map(str::to_string))
}

fn prompt_arg_payload(argv: &[String], prompt: Option<&str>) -> Option<String> {
    let prompt = prompt.filter(|text| !text.trim().is_empty())?;
    argv.iter()
        .any(|arg| arg == prompt)
        .then(|| prompt.to_string())
}

fn argv_payload_after_resume_session_insert_index(argv: &[String]) -> Option<&str> {
    let mut index = resume_session_insert_index(argv);
    while index < argv.len() {
        if argv[index] == OPENCODE_SESSION_FLAG {
            index = index.saturating_add(2);
            continue;
        }
        if !argv[index].trim().is_empty() {
            return Some(&argv[index]);
        }
        index += 1;
    }
    None
}

fn opencode_message_region(argv: &[String]) -> Option<(usize, bool)> {
    let mut index = policy_managed_opencode_prefix_end(argv)?;
    if let Some(boundary) = argv[index..].iter().position(|arg| arg == "--") {
        return Some((index + boundary + 1, true));
    }
    if argv.get(index).map(String::as_str) == Some(OPENCODE_SESSION_FLAG) {
        index = index.saturating_add(2).min(argv.len());
    }
    Some((index, false))
}

fn resume_session_insert_index(argv: &[String]) -> usize {
    policy_managed_opencode_prefix_end(argv).unwrap_or(argv.len())
}

fn policy_managed_opencode_prefix_end(argv: &[String]) -> Option<usize> {
    let mut index = argv.iter().position(|arg| arg == OPENCODE_RUN_ARG)? + 1;
    while index < argv.len() {
        let arg = argv[index].as_str();
        if POLICY_MANAGED_FLAGS_WITH_VALUE.contains(&arg) {
            index = index.saturating_add(2);
        } else if POLICY_MANAGED_FLAGS_WITHOUT_VALUE.contains(&arg) {
            index += 1;
        } else {
            break;
        }
    }
    Some(index.min(argv.len()))
}

fn upsert_session_arg(argv: &mut Vec<String>, session_id: &str, insert_at: usize) {
    if let Some(index) = argv.iter().position(|arg| arg == OPENCODE_SESSION_FLAG) {
        set_existing_session_arg(argv, index, session_id);
    } else {
        insert_session_arg(argv, insert_at, session_id);
    }
}

fn set_existing_session_arg(argv: &mut Vec<String>, index: usize, session_id: &str) {
    if index + 1 < argv.len() {
        argv[index + 1] = session_id.to_string();
    } else {
        argv.insert(index + 1, session_id.to_string());
    }
}

fn insert_session_arg(argv: &mut Vec<String>, insert_at: usize, session_id: &str) {
    argv.insert(insert_at, OPENCODE_SESSION_FLAG.to_string());
    argv.insert(insert_at + 1, session_id.to_string());
}

fn empty_resume_payload_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "empty_resume_payload",
        "resume launch has a known provider session but no non-empty prompt payload reaches child argv or stdin",
    )
}

fn policy_rejection_reason(plan: &policy::PolicyRejection) -> String {
    let diagnostics = plan.diagnostics_json();
    format!("policy.evaluate rejected launch params; diagnostics={diagnostics}")
}

fn policy_stdin_for_launch(
    input: Option<&BytePayload>,
    request_id: &str,
) -> Result<Option<String>, ProviderFailure> {
    let Some(bytes) = optional_stdin_bytes(input, request_id)? else {
        return Ok(None);
    };
    stdin_utf8_text(bytes, request_id).map(Some)
}

fn optional_stdin_bytes(
    input: Option<&BytePayload>,
    request_id: &str,
) -> Result<Option<Vec<u8>>, ProviderFailure> {
    input
        .map(|input| decode_byte_payload(input, request_id))
        .transpose()
}

fn stdin_utf8_text(bytes: Vec<u8>, request_id: &str) -> Result<String, ProviderFailure> {
    String::from_utf8(bytes).map_err(|err| invalid_stdin_utf8_failure(request_id, err))
}

fn decode_byte_payload(
    payload: &BytePayload,
    request_id: &str,
) -> Result<Vec<u8>, ProviderFailure> {
    match payload.encoding.as_str() {
        "base64" => decode_base64_payload(payload, request_id),
        "utf8" => Ok(utf8_payload_bytes(payload)),
        other => Err(invalid_stdin_encoding_failure(request_id, other)),
    }
}

fn decode_base64_payload(
    payload: &BytePayload,
    request_id: &str,
) -> Result<Vec<u8>, ProviderFailure> {
    decode_base64(&payload.data).map_err(|err| invalid_stdin_base64_failure(request_id, err))
}

fn utf8_payload_bytes(payload: &BytePayload) -> Vec<u8> {
    payload.data.as_bytes().to_vec()
}

fn spawn_child(
    argv: &[String],
    working_directory: &str,
    env: &BTreeMap<String, String>,
    stdin: Stdio,
) -> std::io::Result<Child> {
    let mut command = child_command(argv, working_directory, stdin);
    command.env_clear();
    let child_env = child_env(env);
    command.envs(child_env.iter());
    configure_process_group(&mut command);
    command.spawn()
}

fn child_command(argv: &[String], working_directory: &str, stdin: Stdio) -> Command {
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .current_dir(working_directory)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(stdin);
    command
}

fn prepared_child_stdin(stdin: Option<&[u8]>) -> std::io::Result<Stdio> {
    let Some(stdin) = stdin else {
        return Ok(Stdio::null());
    };
    let mut staged = tempfile::tempfile()?;
    staged.write_all(stdin)?;
    staged.sync_all()?;
    staged.seek(SeekFrom::Start(0))?;
    Ok(Stdio::from(staged))
}

fn child_env(declared: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut env = launch_passthrough_env();
    // Explicitly declared values take precedence over ambient passthrough.
    env.extend(declared.clone());
    env
}

fn launch_passthrough_env() -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for key in BASE_LAUNCH_ENV_PASSTHROUGH_KEYS
        .iter()
        .chain(HOST_LINKAGE_ENV_KEYS.iter())
    {
        if let Ok(value) = std::env::var(key) {
            env.insert((*key).to_string(), value);
        }
    }
    env
}

fn stream_child<W: Write>(
    child: &mut Child,
    state: &mut LaunchState,
    writer: &mut W,
) -> Result<i32, ProviderFailure> {
    let receiver = start_drains(child);
    run_supervision_loop(child, &receiver, state, writer)?;
    state.finish(writer)
}

fn start_drains(child: &mut Child) -> Receiver<DrainMessage> {
    let (sender, receiver) = mpsc::sync_channel(DRAIN_CHANNEL_CAPACITY);
    if let Some(stdout) = child.stdout.take() {
        spawn_drain(stdout, sender.clone(), true);
    }
    if let Some(stderr) = child.stderr.take() {
        spawn_drain(stderr, sender, false);
    }
    receiver
}

fn spawn_drain<R: Read + Send + 'static>(
    reader: R,
    sender: SyncSender<DrainMessage>,
    stdout: bool,
) {
    std::thread::spawn(move || drain_reader(reader, sender, stdout));
}

fn drain_reader<R: Read>(mut reader: R, sender: SyncSender<DrainMessage>, stdout: bool) {
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                let bytes = buffer[..count].to_vec();
                let message = if stdout {
                    DrainMessage::Stdout(bytes)
                } else {
                    DrainMessage::Stderr(bytes)
                };
                if sender.send(message).is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = sender.send(DrainMessage::ReadError {
                    stdout,
                    message: error.to_string(),
                });
                return;
            }
        }
    }
    let done = if stdout {
        DrainMessage::StdoutDone
    } else {
        DrainMessage::StderrDone
    };
    let _ = sender.send(done);
}

fn retain_terminal_tail(target: &mut Vec<u8>, bytes: &[u8], truncated: &mut bool) {
    if bytes.len() >= TERMINAL_CAPTURE_LIMIT {
        target.clear();
        target.extend_from_slice(&bytes[bytes.len() - TERMINAL_CAPTURE_LIMIT..]);
        *truncated = true;
        return;
    }
    let overflow = target
        .len()
        .saturating_add(bytes.len())
        .saturating_sub(TERMINAL_CAPTURE_LIMIT);
    if overflow > 0 {
        target.drain(..overflow);
        *truncated = true;
    }
    target.extend_from_slice(bytes);
}

fn run_supervision_loop<W: Write>(
    child: &mut Child,
    receiver: &Receiver<DrainMessage>,
    state: &mut LaunchState,
    writer: &mut W,
) -> Result<(), ProviderFailure> {
    while !state.is_complete() {
        capture_child_exit(child, state)?;
        enforce_deadline(child, state)?;
        state.probe_completed_resume();
        complete_terminal_resume(child, state);
        close_lingering_process_group(child, state);
        match receiver.recv_timeout(state.wait_duration()) {
            Ok(message) => state.handle_drain_message(message, writer)?,
            Err(mpsc::RecvTimeoutError::Timeout) => state.heartbeat(writer)?,
            Err(mpsc::RecvTimeoutError::Disconnected) => state.mark_drains_done(),
        }
    }
    Ok(())
}

fn complete_terminal_resume(child: &mut Child, state: &mut LaunchState) {
    if state.final_status.is_some() || !state.completed_resume_grace_elapsed() {
        return;
    }
    if let Some(status) = terminate_child(child) {
        state.record_forced_exit(process_status_from_exit(status));
    }
}

fn close_lingering_process_group(child: &mut Child, state: &mut LaunchState) {
    if state.drains_done() || !state.child_exit_grace_elapsed() {
        return;
    }
    let _ = terminate_child(child);
}

fn enforce_deadline(child: &mut Child, state: &mut LaunchState) -> Result<(), ProviderFailure> {
    if state.final_status.is_some() || !state.deadline_reached() {
        return Ok(());
    }
    let _ = terminate_child(child);
    state.final_status = Some(deadline_status());
    Ok(())
}

fn capture_child_exit(child: &mut Child, state: &mut LaunchState) -> Result<(), ProviderFailure> {
    if state.final_status.is_some() {
        return Ok(());
    }
    if let Some(status) = child
        .try_wait()
        .map_err(|err| spawn_failure(&state.request_id, "try_wait", err))?
    {
        state.record_child_exit(process_status_from_exit(status));
    }
    Ok(())
}

fn stream_policy_rejection<W: Write>(
    request_id: &str,
    writer: &mut W,
    reason: String,
) -> Result<i32, ProviderFailure> {
    let mut state = LaunchState::new(request_id, None, None, json!([]), None);
    state.final_status = Some(policy_rejection_status(reason));
    state.mark_drains_done();
    state.finish(writer)
}

impl LaunchRequestGuard {
    fn prepare(
        host: &HostContext,
        request_id: &str,
        binding_sha256: String,
        prompt_sha256: Option<String>,
        recovery: LaunchRecoveryContext,
    ) -> Result<Self, ProviderFailure> {
        let root = launch_state_root(host, request_id)?;
        durable_fs::create_private_directories(&root)
            .map_err(|error| launch_state_failure(request_id, error))?;
        let key = sha256_hex(request_id.as_bytes());
        let state_path =
            confined_launch_state_target(host, &root.join(format!("{key}.json")), request_id)?;
        let lock_path =
            confined_launch_state_target(host, &root.join(format!("{key}.lock")), request_id)?;
        let lock = open_launch_state_lock(&lock_path)
            .map_err(|error| launch_state_failure(request_id, error))?;
        lock.lock_exclusive()
            .map_err(|error| launch_state_failure(request_id, error))?;
        match durable_fs::read_file(&state_path) {
            Ok(bytes) => {
                validate_launch_operation_kind(
                    &bytes,
                    LaunchOperationKind::NewSession,
                    request_id,
                    &binding_sha256,
                )?;
                let mut state: LaunchRequestState = serde_json::from_slice(&bytes)
                    .map_err(|error| launch_state_invalid(request_id, error))?;
                validate_launch_request_state(&state, request_id)?;
                if state.binding_sha256 != binding_sha256 {
                    return Err(launch_request_reuse_conflict(
                        request_id,
                        &binding_sha256,
                        &state,
                    ));
                }
                match state.phase {
                    LaunchRequestPhase::Prepared => {
                        validate_launch_recovery_context(&state.recovery, &recovery, request_id)?;
                        match recover_prepared_launch(&state, &recovery.declared_env, request_id)? {
                            PreparedLaunchRecovery::NoEffectObserved => {
                                remove_launch_request_state(&state_path, request_id)?;
                            }
                            PreparedLaunchRecovery::SessionObserved(provider_session_id) => {
                                state.phase = LaunchRequestPhase::SessionObserved;
                                state.provider_session_id = Some(provider_session_id);
                                state.terminal_status = None;
                                state.observed_at_unix_ms = Some(now_unix_ms());
                                write_launch_request_state(&state_path, &state, request_id)?;
                                return Err(launch_session_reconciliation_required(
                                    request_id, &state,
                                ));
                            }
                            PreparedLaunchRecovery::Ambiguous(candidates) => {
                                return Err(launch_session_recovery_required(
                                    request_id, &state, candidates,
                                ));
                            }
                        }
                    }
                    LaunchRequestPhase::SessionObserved => {
                        return Err(launch_session_reconciliation_required(request_id, &state));
                    }
                    LaunchRequestPhase::TerminalWithoutSession => {
                        return Err(launch_terminal_reconciliation_required(request_id, &state));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(launch_state_failure(request_id, error)),
        }
        let state = LaunchRequestState {
            schema_version: 4,
            operation_kind: LaunchOperationKind::NewSession,
            request_id: request_id.to_string(),
            binding_sha256,
            prompt_sha256,
            recovery: recovery.identity,
            phase: LaunchRequestPhase::Prepared,
            provider_session_id: None,
            terminal_status: None,
            prepared_at_unix_ms: now_unix_ms(),
            observed_at_unix_ms: None,
        };
        write_launch_request_state(&state_path, &state, request_id)?;
        Ok(Self {
            state_path,
            state,
            _lock: lock,
        })
    }

    fn observe_session(&mut self, provider_session_id: &str) -> Result<(), ProviderFailure> {
        if self.state.phase == LaunchRequestPhase::SessionObserved {
            if self.state.provider_session_id.as_deref() == Some(provider_session_id) {
                return Ok(());
            }
            return Err(ProviderFailure::conflict(
                &self.state.request_id,
                "launch_generated_session_conflict",
                "one launch request produced conflicting provider session identities",
                json!({
                    "committed_provider_session_id": self.state.provider_session_id,
                    "observed_provider_session_id": provider_session_id,
                }),
            ));
        }
        self.state.phase = LaunchRequestPhase::SessionObserved;
        self.state.provider_session_id = Some(provider_session_id.to_string());
        self.state.terminal_status = None;
        self.state.observed_at_unix_ms = Some(now_unix_ms());
        write_launch_request_state(&self.state_path, &self.state, &self.state.request_id)
    }

    fn observe_terminal_without_session(
        &mut self,
        terminal_status: Value,
    ) -> Result<(), ProviderFailure> {
        if self.state.phase == LaunchRequestPhase::SessionObserved {
            return Ok(());
        }
        self.state.phase = LaunchRequestPhase::TerminalWithoutSession;
        self.state.provider_session_id = None;
        self.state.terminal_status = Some(terminal_status);
        self.state.observed_at_unix_ms = Some(now_unix_ms());
        write_launch_request_state(&self.state_path, &self.state, &self.state.request_id)
    }

    fn abandon_before_spawn(self) -> Result<(), ProviderFailure> {
        remove_launch_request_state(&self.state_path, &self.state.request_id)
    }
}

impl ResumeLaunchRequestGuard {
    fn prepare(
        host: &HostContext,
        request_id: &str,
        binding_sha256: String,
        observation: DurableResumeObservationRequest,
        recovery: LaunchRecoveryContext,
    ) -> Result<Self, ProviderFailure> {
        let root = launch_state_root(host, request_id)?;
        durable_fs::create_private_directories(&root)
            .map_err(|error| launch_state_failure(request_id, error))?;
        let key = sha256_hex(request_id.as_bytes());
        let state_path =
            confined_launch_state_target(host, &root.join(format!("{key}.json")), request_id)?;
        let lock_path =
            confined_launch_state_target(host, &root.join(format!("{key}.lock")), request_id)?;
        let lock = open_launch_state_lock(&lock_path)
            .map_err(|error| launch_state_failure(request_id, error))?;
        lock.lock_exclusive()
            .map_err(|error| launch_state_failure(request_id, error))?;
        match durable_fs::read_file(&state_path) {
            Ok(bytes) => {
                validate_launch_operation_kind(
                    &bytes,
                    LaunchOperationKind::Resume,
                    request_id,
                    &binding_sha256,
                )?;
                let mut state: ResumeLaunchRequestState = serde_json::from_slice(&bytes)
                    .map_err(|error| launch_state_invalid(request_id, error))?;
                validate_resume_launch_request_state(&state, request_id)?;
                if state.binding_sha256 != binding_sha256 {
                    return Err(resume_launch_request_reuse_conflict(
                        request_id,
                        &binding_sha256,
                        &state,
                    ));
                }
                validate_launch_recovery_context(&state.recovery, &recovery, request_id)?;
                match state.phase {
                    ResumeLaunchRequestPhase::SubmissionObserved
                    | ResumeLaunchRequestPhase::CompletionObserved => {
                        return Err(resume_launch_reconciliation_required(request_id, &state));
                    }
                    ResumeLaunchRequestPhase::Prepared
                    | ResumeLaunchRequestPhase::Unresolved
                    | ResumeLaunchRequestPhase::TerminalWithoutSubmission => {
                        let recovered = observe_durable_resume(
                            &state.observation,
                            &state.recovery,
                            &recovery.declared_env,
                        );
                        if !recovered.available {
                            state.phase = ResumeLaunchRequestPhase::Unresolved;
                            state.observed_at_unix_ms = Some(now_unix_ms());
                            write_launch_request_state(&state_path, &state, request_id)?;
                            return Err(resume_launch_recovery_unavailable(request_id, &state));
                        }
                        if recovered.submitted_user_turn.is_some() {
                            state.phase = if recovered.completion_observed() {
                                ResumeLaunchRequestPhase::CompletionObserved
                            } else {
                                ResumeLaunchRequestPhase::SubmissionObserved
                            };
                            state.observed_at_unix_ms = Some(now_unix_ms());
                            write_launch_request_state(&state_path, &state, request_id)?;
                            return Err(resume_launch_reconciliation_required(request_id, &state));
                        }
                        remove_launch_request_state(&state_path, request_id)?;
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(launch_state_failure(request_id, error)),
        }
        let state = ResumeLaunchRequestState {
            schema_version: 4,
            operation_kind: LaunchOperationKind::Resume,
            request_id: request_id.to_string(),
            binding_sha256,
            observation,
            recovery: recovery.identity,
            phase: ResumeLaunchRequestPhase::Prepared,
            terminal_status: None,
            prepared_at_unix_ms: now_unix_ms(),
            observed_at_unix_ms: None,
        };
        write_launch_request_state(&state_path, &state, request_id)?;
        Ok(Self {
            state_path,
            state,
            declared_env: recovery.declared_env,
            _lock: lock,
        })
    }

    fn observe(&self) -> ResumeObservation {
        observe_durable_resume(
            &self.state.observation,
            &self.state.recovery,
            &self.declared_env,
        )
    }

    fn settle(
        &mut self,
        observation: &ResumeObservation,
        completion_observed: bool,
        terminal_status: Value,
    ) -> Result<(), ProviderFailure> {
        self.state.phase = if completion_observed {
            ResumeLaunchRequestPhase::CompletionObserved
        } else if !observation.available {
            ResumeLaunchRequestPhase::Unresolved
        } else if observation.submitted_user_turn.is_some() {
            ResumeLaunchRequestPhase::SubmissionObserved
        } else {
            ResumeLaunchRequestPhase::TerminalWithoutSubmission
        };
        self.state.terminal_status = Some(terminal_status);
        self.state.observed_at_unix_ms = Some(now_unix_ms());
        write_launch_request_state(&self.state_path, &self.state, &self.state.request_id)
    }

    fn unresolved_marker(&self) -> Value {
        json!({
            "state": "resume_observation_unavailable",
            "provider_session_id": self.state.observation.session_id,
            "prompt_sha256": self.state.observation.payload_sha256,
            "required_action": "reconcile the provider session before retrying the submitted turn",
        })
    }

    fn abandon_before_spawn(self) -> Result<(), ProviderFailure> {
        remove_launch_request_state(&self.state_path, &self.state.request_id)
    }
}

fn remove_launch_request_state(path: &Path, request_id: &str) -> Result<(), ProviderFailure> {
    match fs::remove_file(path) {
        Ok(()) => durable_fs::sync_directory(
            path.parent()
                .expect("launch state path always has a parent"),
        )
        .map_err(|error| launch_state_failure(request_id, error)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(launch_state_failure(request_id, error)),
    }
}

fn launch_request_binding_sha256(
    host: &HostContext,
    params: &Value,
    route_evidence: &Value,
) -> String {
    sha256_hex(
        json!({
            "host_app": host.app,
            "params": params,
            "route": route_evidence,
        })
        .to_string()
        .as_bytes(),
    )
}

fn launch_recovery_context(
    argv: &[String],
    working_directory: &str,
    declared_env: &BTreeMap<String, String>,
    route: &policy::PolicyRouteIdentity,
) -> LaunchRecoveryContext {
    let passthrough_env = launch_passthrough_env();
    let mut effective_env = passthrough_env.clone();
    effective_env.extend(declared_env.clone());
    LaunchRecoveryContext {
        identity: LaunchRecoveryIdentity {
            program: resolve_launch_program(&argv[0], working_directory, &effective_env),
            passthrough_env,
            declared_env_sha256: launch_environment_sha256(declared_env),
            working_directory: working_directory.to_string(),
            provider_id: route.provider_id.clone(),
            model_id: route.model_id.clone(),
            effort: route.effort.clone(),
        },
        declared_env: declared_env.clone(),
    }
}

fn launch_environment_sha256(env: &BTreeMap<String, String>) -> String {
    sha256_hex(
        serde_json::to_vec(env)
            .expect("launch environment serialization cannot fail")
            .as_slice(),
    )
}

fn validate_launch_recovery_context(
    identity: &LaunchRecoveryIdentity,
    recovery: &LaunchRecoveryContext,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if identity.declared_env_sha256 == launch_environment_sha256(&recovery.declared_env) {
        return Ok(());
    }
    Err(launch_recovery_unavailable(
        request_id,
        "the exact retry did not reproduce the declared child environment",
    ))
}

fn observe_durable_resume(
    observation: &DurableResumeObservationRequest,
    recovery: &LaunchRecoveryIdentity,
    declared_env: &BTreeMap<String, String>,
) -> ResumeObservation {
    let mut env = recovery.passthrough_env.clone();
    env.extend(declared_env.clone());
    resume_observation::observe_durable(
        observation,
        &recovery.program,
        &recovery.working_directory,
        &env,
    )
}

fn resolve_launch_program(
    program: &str,
    working_directory: &str,
    env: &BTreeMap<String, String>,
) -> String {
    let path = Path::new(program);
    if path.is_absolute() {
        return program.to_string();
    }
    if path.components().count() > 1 {
        return Path::new(working_directory)
            .join(path)
            .to_string_lossy()
            .into_owned();
    }
    env.get("PATH")
        .and_then(|path| {
            std::env::split_paths(path)
                .map(|directory| directory.join(program))
                .find(|candidate| candidate.is_file())
        })
        .map(|candidate| candidate.to_string_lossy().into_owned())
        .unwrap_or_else(|| program.to_string())
}

fn recover_prepared_launch(
    state: &LaunchRequestState,
    declared_env: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<PreparedLaunchRecovery, ProviderFailure> {
    let output = launch_recovery_command(
        state,
        declared_env,
        &["session", "list", "--format", "json"],
    )
    .output()
    .map_err(|error| launch_recovery_unavailable(request_id, error))?;
    if !output.status.success() {
        return Err(launch_recovery_unavailable(
            request_id,
            format!(
                "session list exited {:?}: {}",
                output.status.code(),
                bounded_text(&String::from_utf8_lossy(&output.stderr), 500)
            ),
        ));
    }
    let sessions = opencode::parse_session_list_stdout(&output.stdout)
        .map_err(|error| launch_recovery_unavailable(request_id, format!("{error:?}")))?;
    let mut candidates = sessions
        .iter()
        .filter(|entry| launch_recovery_session_is_plausible(entry, state))
        .filter_map(launch_recovery_session_id)
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();
    if candidates.is_empty() {
        return Ok(PreparedLaunchRecovery::NoEffectObserved);
    }
    let Some(prompt_sha256) = state.prompt_sha256.as_deref() else {
        return Ok(PreparedLaunchRecovery::Ambiguous(candidates));
    };
    let mut matched = candidates
        .iter()
        .filter(|session_id| {
            recovered_session_matches_request(state, declared_env, session_id, prompt_sha256)
                .unwrap_or(false)
        })
        .cloned()
        .collect::<Vec<_>>();
    matched.sort();
    matched.dedup();
    match matched.as_slice() {
        [session_id] => Ok(PreparedLaunchRecovery::SessionObserved(session_id.clone())),
        [] => Ok(PreparedLaunchRecovery::Ambiguous(candidates)),
        _ => Ok(PreparedLaunchRecovery::Ambiguous(matched)),
    }
}

fn launch_recovery_command(
    state: &LaunchRequestState,
    declared_env: &BTreeMap<String, String>,
    args: &[&str],
) -> Command {
    let mut command = Command::new(&state.recovery.program);
    let mut env = state.recovery.passthrough_env.clone();
    env.extend(declared_env.clone());
    command
        .args(args)
        .current_dir(&state.recovery.working_directory)
        .env_clear()
        .envs(env);
    command
}

fn launch_recovery_session_is_plausible(entry: &Value, state: &LaunchRequestState) -> bool {
    let directory_matches =
        launch_recovery_string(entry, &["directory", "cwd", "working_directory"])
            .is_none_or(|directory| directory == state.recovery.working_directory);
    let timestamp =
        launch_recovery_integer(entry, &["updated", "updated_unix_ms", "updatedUnixMs"])
            .or_else(|| {
                launch_recovery_integer(entry, &["created", "created_unix_ms", "createdUnixMs"])
            })
            .or_else(|| {
                entry.get("time").and_then(|time| {
                    launch_recovery_integer(time, &["updated", "updated_unix_ms"])
                        .or_else(|| launch_recovery_integer(time, &["created", "created_unix_ms"]))
                })
            });
    directory_matches
        && timestamp
            .is_none_or(|timestamp| timestamp >= state.prepared_at_unix_ms.saturating_sub(5_000))
}

fn launch_recovery_session_id(entry: &Value) -> Option<String> {
    launch_recovery_string(entry, &["id", "sessionID", "sessionId", "session_id"])
        .map(str::to_string)
}

fn launch_recovery_string<'a>(entry: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| entry.get(*key).and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
}

fn launch_recovery_integer(entry: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| entry.get(*key).and_then(Value::as_u64))
}

fn recovered_session_matches_request(
    state: &LaunchRequestState,
    declared_env: &BTreeMap<String, String>,
    session_id: &str,
    prompt_sha256: &str,
) -> Result<bool, ProviderFailure> {
    let output = launch_recovery_command(state, declared_env, &["export", session_id])
        .output()
        .map_err(|error| launch_recovery_unavailable(&state.request_id, error))?;
    if !output.status.success() {
        return Ok(false);
    }
    let export = opencode::parse_export_stdout(&output.stdout)
        .map_err(|error| launch_recovery_unavailable(&state.request_id, format!("{error:?}")))?;
    if export.info.id != session_id {
        return Ok(false);
    }
    Ok(export.messages.iter().any(|message| {
        let (provider_id, model_id, effort) = message.info.model_identity();
        message.info.role == "user"
            && message.info.session_id.as_deref() == Some(session_id)
            && provider_id == Some(state.recovery.provider_id.as_str())
            && model_id == Some(state.recovery.model_id.as_str())
            && effort == Some(state.recovery.effort.as_str())
            && message
                .info
                .time
                .as_ref()
                .and_then(|time| time.created)
                .is_some_and(|created| created >= state.prepared_at_unix_ms.saturating_sub(5_000))
            && message.parts.iter().any(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| sha256_hex(text.as_bytes()) == prompt_sha256)
            })
    }))
}

fn launch_state_root(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let data_root = launch_data_root(host, request_id)?;
    confined_launch_state_target(host, &data_root.join(LAUNCH_STATE_DIR), request_id)
}

fn launch_data_root<'a>(
    host: &'a HostContext,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    host.data_root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "launch_data_root_missing",
                "launch requires host.data_root for durable request-to-operation binding",
            )
        })
}

fn confined_launch_state_target(
    host: &HostContext,
    target: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    path_guard::confined_target(launch_data_root(host, request_id)?, target)
        .map_err(|error| launch_state_failure(request_id, error))
}

fn open_launch_state_lock(path: &Path) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn write_launch_request_state<T: Serialize>(
    path: &Path,
    state: &T,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let parent = path
        .parent()
        .expect("launch request state path always has a parent");
    durable_fs::create_private_directories(parent)
        .map_err(|error| launch_state_failure(request_id, error))?;
    let bytes =
        serde_json::to_vec(state).map_err(|error| launch_state_invalid(request_id, error))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| launch_state_failure(request_id, error))?;
    temporary
        .write_all(&bytes)
        .map_err(|error| launch_state_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(|error| launch_state_failure(request_id, error))?;
    temporary
        .persist(path)
        .map_err(|error| launch_state_failure(request_id, error.error))?;
    durable_fs::sync_directory(parent).map_err(|error| launch_state_failure(request_id, error))
}

fn validate_launch_operation_kind(
    bytes: &[u8],
    expected: LaunchOperationKind,
    request_id: &str,
    attempted_binding_sha256: &str,
) -> Result<(), ProviderFailure> {
    let discriminator: LaunchOperationDiscriminator =
        serde_json::from_slice(bytes).map_err(|error| launch_state_invalid(request_id, error))?;
    if discriminator.operation_kind == expected {
        return Ok(());
    }
    Err(ProviderFailure::conflict(
        request_id,
        "launch_request_conflict",
        "launch request_id already names a different durable launch operation",
        json!({
            "attempted_operation_kind": expected,
            "committed_operation_kind": discriminator.operation_kind,
            "attempted_binding_sha256": attempted_binding_sha256,
        }),
    ))
}

fn validate_launch_request_state(
    state: &LaunchRequestState,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let phase_valid = match state.phase {
        LaunchRequestPhase::Prepared => {
            state.provider_session_id.is_none()
                && state.terminal_status.is_none()
                && state.observed_at_unix_ms.is_none()
        }
        LaunchRequestPhase::SessionObserved => {
            state
                .provider_session_id
                .as_deref()
                .is_some_and(|session_id| !session_id.trim().is_empty())
                && state.terminal_status.is_none()
                && state.observed_at_unix_ms.is_some()
        }
        LaunchRequestPhase::TerminalWithoutSession => {
            state.provider_session_id.is_none()
                && state.terminal_status.as_ref().is_some_and(Value::is_object)
                && state.observed_at_unix_ms.is_some()
        }
    };
    if state.schema_version == 4
        && state.operation_kind == LaunchOperationKind::NewSession
        && state.request_id == request_id
        && !state.binding_sha256.trim().is_empty()
        && !state.recovery.program.trim().is_empty()
        && !state.recovery.declared_env_sha256.trim().is_empty()
        && !state.recovery.working_directory.trim().is_empty()
        && !state.recovery.provider_id.trim().is_empty()
        && !state.recovery.model_id.trim().is_empty()
        && !state.recovery.effort.trim().is_empty()
        && phase_valid
    {
        return Ok(());
    }
    Err(launch_state_invalid(
        request_id,
        "launch request state identity or phase is inconsistent",
    ))
}

fn validate_resume_launch_request_state(
    state: &ResumeLaunchRequestState,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let phase_valid = match state.phase {
        ResumeLaunchRequestPhase::Prepared => {
            state.terminal_status.is_none() && state.observed_at_unix_ms.is_none()
        }
        ResumeLaunchRequestPhase::SubmissionObserved
        | ResumeLaunchRequestPhase::CompletionObserved
        | ResumeLaunchRequestPhase::Unresolved
        | ResumeLaunchRequestPhase::TerminalWithoutSubmission => {
            state.observed_at_unix_ms.is_some()
                && state.terminal_status.as_ref().is_none_or(Value::is_object)
        }
    };
    let observation = &state.observation;
    if state.schema_version == 4
        && state.operation_kind == LaunchOperationKind::Resume
        && state.request_id == request_id
        && !state.binding_sha256.trim().is_empty()
        && !observation.account_wrapper.trim().is_empty()
        && !observation.session_id.trim().is_empty()
        && !observation.payload_sha256.trim().is_empty()
        && !observation.provider_id.trim().is_empty()
        && !observation.model_id.trim().is_empty()
        && !observation.variant.trim().is_empty()
        && !state.recovery.program.trim().is_empty()
        && !state.recovery.declared_env_sha256.trim().is_empty()
        && !state.recovery.working_directory.trim().is_empty()
        && state.recovery.provider_id == observation.provider_id
        && state.recovery.model_id == observation.model_id
        && state.recovery.effort == observation.variant
        && phase_valid
    {
        return Ok(());
    }
    Err(launch_state_invalid(
        request_id,
        "durable resume launch state identity or phase is inconsistent",
    ))
}

fn launch_state_failure(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "launch_state_failed",
        format!("durable launch request state failed: {error}"),
    )
}

fn launch_state_invalid(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "launch_state_invalid",
        format!("durable launch request state is invalid: {error}"),
    )
}

fn launch_recovery_unavailable(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "launch_session_recovery_unavailable",
        format!("could not reconcile a prepared new-session launch: {error}"),
    )
}

fn launch_request_reuse_conflict(
    request_id: &str,
    attempted_binding_sha256: &str,
    state: &LaunchRequestState,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_request_conflict",
        "launch request_id already names a different durable new-session operation",
        json!({
            "attempted_binding_sha256": attempted_binding_sha256,
            "committed_binding_sha256": state.binding_sha256,
            "provider_session_id": state.provider_session_id,
        }),
    )
}

fn launch_session_reconciliation_required(
    request_id: &str,
    state: &LaunchRequestState,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_session_reconciliation_required",
        "new-session launch request already has durable state and will not spawn independent work",
        json!({
            "phase": state.phase,
            "binding_sha256": state.binding_sha256,
            "prompt_sha256": state.prompt_sha256,
            "provider_session_id": state.provider_session_id,
            "terminal_status": state.terminal_status,
            "required_action": "reconcile the bound provider session before deciding whether to resume",
        }),
    )
}

fn launch_session_recovery_required(
    request_id: &str,
    state: &LaunchRequestState,
    candidate_session_ids: Vec<String>,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_session_recovery_required",
        "prepared launch recovery found sessions that require authoritative reconciliation",
        json!({
            "phase": state.phase,
            "binding_sha256": state.binding_sha256,
            "prompt_sha256": state.prompt_sha256,
            "candidate_provider_session_ids": candidate_session_ids,
            "required_action": "inspect the candidate native sessions and resume only the one that owns this request",
        }),
    )
}

fn launch_terminal_reconciliation_required(
    request_id: &str,
    state: &LaunchRequestState,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_terminal_reconciliation_required",
        "new-session launch request already reached a durable terminal without observing a provider session",
        json!({
            "phase": state.phase,
            "binding_sha256": state.binding_sha256,
            "prompt_sha256": state.prompt_sha256,
            "terminal_status": state.terminal_status,
            "required_action": "reconcile the durable terminal before deciding whether a distinct request should retry",
        }),
    )
}

fn resume_launch_request_reuse_conflict(
    request_id: &str,
    attempted_binding_sha256: &str,
    state: &ResumeLaunchRequestState,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_request_conflict",
        "launch request_id already names a different durable resumed-session operation",
        json!({
            "attempted_binding_sha256": attempted_binding_sha256,
            "committed_binding_sha256": state.binding_sha256,
            "provider_session_id": state.observation.session_id,
        }),
    )
}

fn resume_launch_reconciliation_required(
    request_id: &str,
    state: &ResumeLaunchRequestState,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_resume_reconciliation_required",
        "resumed-session launch request has durable delivery evidence and will not resubmit the turn",
        json!({
            "phase": state.phase,
            "binding_sha256": state.binding_sha256,
            "prompt_sha256": state.observation.payload_sha256,
            "provider_session_id": state.observation.session_id,
            "terminal_status": state.terminal_status,
            "required_action": "reconcile the bound provider session before deciding whether to submit another turn",
        }),
    )
}

fn resume_launch_recovery_unavailable(
    request_id: &str,
    state: &ResumeLaunchRequestState,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "launch_resume_recovery_unavailable",
        format!(
            "could not authoritatively reconcile prepared resumed-session launch for provider session {}",
            state.observation.session_id
        ),
    )
}

struct LaunchState {
    request_id: String,
    seq: u64,
    stdout_done: bool,
    stderr_done: bool,
    final_status: Option<ProcessStatus>,
    child_exit_at: Option<Instant>,
    forced_exit_status: Option<ProcessStatus>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
    integrity_failures: Vec<String>,
    parser: EventParser,
    last_opencode_event: Option<OpencodeEventMetadata>,
    completed_resume_at: Option<Instant>,
    next_resume_completion_probe: Option<Instant>,
    resume_completion_probes: usize,
    unresolved_resume_completion: Option<Value>,
    session_id: Option<String>,
    resume_observation_request: Option<ResumeObservationRequest>,
    deadline_unix_ms: Option<u64>,
    next_heartbeat: Instant,
    route_evidence: Value,
    launch_request: Option<LaunchRequestGuard>,
    resume_launch_request: Option<ResumeLaunchRequestGuard>,
    projection_admitted: bool,
    deferred_events: Vec<Value>,
    deferred_event_bytes: usize,
}

impl LaunchState {
    fn new(
        request_id: &str,
        deadline_unix_ms: Option<u64>,
        resume_observation_request: Option<ResumeObservationRequest>,
        route_evidence: Value,
        launch_request: Option<LaunchRequestGuard>,
    ) -> Self {
        let next_resume_completion_probe = resume_observation_request
            .as_ref()
            .map(|_| Instant::now() + RESUME_COMPLETION_PROBE_DELAY);
        Self {
            request_id: request_id.to_string(),
            seq: 1,
            stdout_done: false,
            stderr_done: false,
            final_status: None,
            child_exit_at: None,
            forced_exit_status: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
            integrity_failures: Vec::new(),
            parser: EventParser::default(),
            last_opencode_event: None,
            completed_resume_at: None,
            next_resume_completion_probe,
            resume_completion_probes: 0,
            unresolved_resume_completion: None,
            session_id: None,
            resume_observation_request,
            deadline_unix_ms,
            next_heartbeat: Instant::now() + HEARTBEAT_INTERVAL,
            route_evidence,
            projection_admitted: launch_request.is_none(),
            launch_request,
            resume_launch_request: None,
            deferred_events: Vec::new(),
            deferred_event_bytes: 0,
        }
    }

    fn attach_launch_request(&mut self, launch_request: LaunchRequestGuard) {
        self.launch_request = Some(launch_request);
        self.projection_admitted = false;
    }

    fn attach_resume_launch_request(&mut self, launch_request: ResumeLaunchRequestGuard) {
        self.resume_launch_request = Some(launch_request);
        self.projection_admitted = false;
    }

    fn handle_drain_message<W: Write>(
        &mut self,
        message: DrainMessage,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        match message {
            DrainMessage::Stdout(bytes) => self.stdout_bytes(&bytes, writer),
            DrainMessage::Stderr(bytes) => self.stderr_bytes(&bytes, writer),
            DrainMessage::StdoutDone => {
                self.stdout_done = true;
                Ok(())
            }
            DrainMessage::StderrDone => {
                self.stderr_done = true;
                Ok(())
            }
            DrainMessage::ReadError { stdout, message } => {
                if stdout {
                    self.stdout_done = true;
                } else {
                    self.stderr_done = true;
                }
                self.integrity_failures.push(format!(
                    "{} pipe read failed: {message}",
                    if stdout { "stdout" } else { "stderr" }
                ));
                Ok(())
            }
        }
    }

    fn stdout_bytes<W: Write>(
        &mut self,
        bytes: &[u8],
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.record_stdout(bytes);
        let session = self.session_from_stdout(bytes);
        self.admit_generated_session(session.as_deref(), writer)?;
        self.project_stdout_bytes(bytes, writer)?;
        self.capture_session_marker(session, writer)
    }

    fn stderr_bytes<W: Write>(
        &mut self,
        bytes: &[u8],
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.record_stderr(bytes);
        self.project_stderr_bytes(bytes, writer)
    }

    fn record_stdout(&mut self, bytes: &[u8]) {
        retain_terminal_tail(&mut self.stdout, bytes, &mut self.stdout_truncated);
    }

    fn record_stderr(&mut self, bytes: &[u8]) {
        retain_terminal_tail(&mut self.stderr, bytes, &mut self.stderr_truncated);
    }

    fn project_stdout_bytes<W: Write>(
        &mut self,
        bytes: &[u8],
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.stream_bytes("stdout", bytes, writer)
    }

    fn project_stderr_bytes<W: Write>(
        &mut self,
        bytes: &[u8],
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.stream_bytes("stderr", bytes, writer)
    }

    fn stream_bytes<W: Write>(
        &mut self,
        kind: &str,
        bytes: &[u8],
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.write_event(
            stream_bytes_event(&self.request_id, self.seq, kind, bytes),
            writer,
        )
    }

    fn capture_session<W: Write>(
        &mut self,
        session: Option<String>,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.admit_generated_session(session.as_deref(), writer)?;
        self.capture_session_marker(session, writer)
    }

    fn capture_session_marker<W: Write>(
        &mut self,
        session: Option<String>,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        if self.has_session_marker() {
            return Ok(());
        }
        if let Some(session_id) = session {
            self.record_session_id(&session_id);
            self.write_session_marker(&session_id, writer)?;
        }
        Ok(())
    }

    fn admit_generated_session<W: Write>(
        &mut self,
        provider_session_id: Option<&str>,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.persist_generated_session(provider_session_id)?;
        if provider_session_id.is_some() && self.launch_request.is_some() {
            self.admit_projection(writer)?;
        }
        Ok(())
    }

    fn persist_generated_session(
        &mut self,
        provider_session_id: Option<&str>,
    ) -> Result<(), ProviderFailure> {
        let (Some(launch_request), Some(provider_session_id)) =
            (self.launch_request.as_mut(), provider_session_id)
        else {
            return Ok(());
        };
        launch_request.observe_session(provider_session_id)
    }

    fn admit_projection<W: Write>(&mut self, writer: &mut W) -> Result<(), ProviderFailure> {
        if self.projection_admitted {
            return Ok(());
        }
        self.projection_admitted = true;
        self.deferred_event_bytes = 0;
        for event in std::mem::take(&mut self.deferred_events) {
            self.write_event(event, writer)?;
        }
        Ok(())
    }

    fn has_session_marker(&self) -> bool {
        self.session_id.is_some()
    }

    fn record_session_id(&mut self, session_id: &str) {
        self.session_id = Some(session_id.to_string());
    }

    fn write_session_marker<W: Write>(
        &mut self,
        session_id: &str,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.marker(session_marker_name(session_id), writer)?;
        self.marker_with_value(
            PROVIDER_SESSION_MARKER.to_string(),
            json!({
                "provider_session_id": session_id,
                "source": "opencode.run.format_json",
            }),
            writer,
        )
    }

    fn session_from_stdout(&mut self, bytes: &[u8]) -> Option<String> {
        let events = self.parser.ingest(bytes);
        self.record_parser_errors();
        self.record_opencode_events(&events);
        first_session_id(&events)
    }

    fn capture_session_from_parser_tail<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        let session = self.session_from_parser_tail();
        self.capture_session(session, writer)
    }

    fn session_from_parser_tail(&mut self) -> Option<String> {
        let events = self.parser.finish();
        self.record_parser_errors();
        self.record_opencode_events(&events);
        first_session_id(&events)
    }

    fn record_opencode_events(&mut self, events: &[OpencodeEventMetadata]) {
        if let Some(event) = events.last() {
            self.last_opencode_event = Some(event.clone());
        }
    }

    fn record_parser_errors(&mut self) {
        self.integrity_failures.extend(
            self.parser
                .take_errors()
                .into_iter()
                .map(|error| format!("native event parse failed: {error}")),
        );
    }

    fn completed_resume_grace_elapsed(&self) -> bool {
        self.completed_resume_at
            .is_some_and(|completed_at| completed_at.elapsed() >= COMPLETED_RESUME_GRACE)
    }

    fn probe_completed_resume(&mut self) {
        let Some(next_probe) = self.next_resume_completion_probe else {
            return;
        };
        if self.completed_resume_at.is_some()
            || self.resume_completion_probes >= MAX_RESUME_COMPLETION_PROBES
            || Instant::now() < next_probe
        {
            return;
        }
        self.next_resume_completion_probe = Some(Instant::now() + RESUME_COMPLETION_PROBE_INTERVAL);
        self.probe_completed_resume_now();
    }

    fn probe_completed_resume_now(&mut self) {
        if self.completed_resume_at.is_some()
            || self.resume_completion_probes >= MAX_RESUME_COMPLETION_PROBES
        {
            return;
        }
        self.resume_completion_probes += 1;
        let Some(request) = self.resume_observation_request.as_ref() else {
            return;
        };
        let observation = self
            .resume_launch_request
            .as_ref()
            .map(ResumeLaunchRequestGuard::observe)
            .unwrap_or_else(|| resume_observation::observe(request));
        if observation.completion_observed() {
            self.completed_resume_at = Some(Instant::now());
        }
    }

    fn marker<W: Write>(&mut self, name: String, writer: &mut W) -> Result<(), ProviderFailure> {
        self.marker_with_value(name, json!(true), writer)
    }

    fn marker_with_value<W: Write>(
        &mut self,
        name: String,
        value: Value,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.write_event(
            marker_event(&self.request_id, self.seq, name, value),
            writer,
        )
    }

    fn heartbeat<W: Write>(&mut self, writer: &mut W) -> Result<(), ProviderFailure> {
        if self.final_status.is_some() {
            return Ok(());
        }
        self.next_heartbeat = Instant::now() + HEARTBEAT_INTERVAL;
        self.write_event(heartbeat_event(&self.request_id, self.seq), writer)
    }

    fn finish<W: Write>(&mut self, writer: &mut W) -> Result<i32, ProviderFailure> {
        self.capture_session_from_parser_tail(writer)?;
        self.admit_terminal_without_session(writer)?;
        let final_resume_observation = self
            .resume_launch_request
            .as_ref()
            .map(ResumeLaunchRequestGuard::observe)
            .or_else(|| {
                self.resume_observation_request
                    .as_ref()
                    .map(resume_observation::observe)
            });
        let completion_observed = self.completed_resume_at.is_some()
            || final_resume_observation
                .as_ref()
                .is_some_and(|observation| observation.completion_observed());
        let submitted_user_turn = final_resume_observation
            .as_ref()
            .and_then(|observation| observation.submitted_user_turn.as_ref());
        self.confirm_submitted_user_turn(submitted_user_turn, writer)?;
        self.confirm_produced_assistant_response(completion_observed, writer)?;
        self.retain_unresolved_resume_completion(
            final_resume_observation.as_ref(),
            completion_observed,
            writer,
        )?;
        self.settle_resume_launch_request(
            final_resume_observation.as_ref(),
            completion_observed,
            writer,
        )?;
        self.emit_integrity_evidence(writer)?;
        let status = self.finished_status();
        let signal = self.terminal_signal_for(&status);
        let event = self.exit_event(&status, signal);
        self.write_event(event, writer)?;
        Ok(provider_exit_code(&status))
    }

    fn admit_terminal_without_session<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        if self.projection_admitted {
            return Ok(());
        }
        if self.resume_launch_request.is_some() {
            return Ok(());
        }
        let status = self.finished_status();
        if let Some(launch_request) = self.launch_request.as_mut() {
            launch_request.observe_terminal_without_session(process_status_json(&status))?;
        }
        self.admit_projection(writer)
    }

    fn confirm_submitted_user_turn<W: Write>(
        &mut self,
        marker_value: Option<&Value>,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        let Some(marker_value) = marker_value else {
            return Ok(());
        };
        self.marker_with_value(
            SUBMITTED_USER_TURN_MARKER.to_string(),
            marker_value.clone(),
            writer,
        )
    }

    fn confirm_produced_assistant_response<W: Write>(
        &mut self,
        completion_observed: bool,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        if !completion_observed {
            return Ok(());
        }
        self.marker(PRODUCED_ASSISTANT_RESPONSE_MARKER.to_string(), writer)
    }

    fn retain_unresolved_resume_completion<W: Write>(
        &mut self,
        observation: Option<&ResumeObservation>,
        completion_observed: bool,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        if !completion_observed && observation.is_some_and(|observation| !observation.available) {
            if let Some(unresolved) = self
                .resume_launch_request
                .as_ref()
                .map(ResumeLaunchRequestGuard::unresolved_marker)
            {
                self.unresolved_resume_completion = Some(unresolved.clone());
                return self.marker_with_value(
                    RESUME_COMPLETION_UNRESOLVED_MARKER.to_string(),
                    unresolved,
                    writer,
                );
            }
        }
        let submitted_user_turn = observation
            .and_then(|observation| observation.submitted_user_turn.as_ref())
            .filter(|_| !completion_observed);
        let Some(submitted_user_turn) = submitted_user_turn else {
            return Ok(());
        };
        let unresolved = json!({
            "state": "submitted_user_turn_without_completed_assistant_response",
            "provider_session_id": submitted_user_turn["provider_session_id"],
            "prompt_sha256": submitted_user_turn["prompt_sha256"],
            "required_action": "reconcile the provider session before retrying the submitted turn",
        });
        self.unresolved_resume_completion = Some(unresolved.clone());
        self.marker_with_value(
            RESUME_COMPLETION_UNRESOLVED_MARKER.to_string(),
            unresolved,
            writer,
        )
    }

    fn settle_resume_launch_request<W: Write>(
        &mut self,
        observation: Option<&ResumeObservation>,
        completion_observed: bool,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        let Some(observation) = observation else {
            if self.resume_launch_request.is_some() {
                return Err(launch_state_invalid(
                    &self.request_id,
                    "durable resumed-session launch has no final observation identity",
                ));
            }
            return Ok(());
        };
        let status = process_status_json(&self.finished_status());
        let Some(launch_request) = self.resume_launch_request.as_mut() else {
            return Ok(());
        };
        launch_request.settle(observation, completion_observed, status)?;
        self.admit_projection(writer)
    }

    fn finished_status(&self) -> ProcessStatus {
        let status = self.final_status.clone().unwrap_or(ProcessStatus::Unknown);
        if is_clean_exit_status(&status)
            && (!self.integrity_failures.is_empty() || self.unresolved_resume_completion.is_some())
        {
            return ProcessStatus::Unknown;
        }
        status
    }

    fn terminal_signal_for(&self, status: &ProcessStatus) -> Value {
        if matches!(status, ProcessStatus::Unknown) && self.unresolved_resume_completion.is_some() {
            let evidence = if self
                .unresolved_resume_completion
                .as_ref()
                .is_some_and(|value| value["state"] == "resume_observation_unavailable")
            {
                "resume delivery observation is unavailable; submission outcome remains unconfirmed"
            } else {
                "resume submission confirmed; assistant response completion remains unconfirmed"
            };
            return json!({
                "kind": "unknown",
                "evidence": evidence,
                "observed_at_unix_ms": now_unix_ms(),
            });
        }
        if let Some(signal) = self.final_opencode_error_signal(status) {
            return signal;
        }
        classify(&self.stdout, &self.stderr, status, now_unix_ms())
    }

    fn final_opencode_error_signal(&self, status: &ProcessStatus) -> Option<Value> {
        if !is_clean_exit_status(status) {
            return None;
        }
        let event = self.last_opencode_event.as_ref()?;
        if !opencode::is_structured_error_event(event) {
            return None;
        }
        Some(provider_error_terminal_signal(event))
    }

    fn exit_event(&self, status: &ProcessStatus, signal: Value) -> Value {
        let mut event = launch_exit_event(&self.request_id, self.seq, status, signal);
        attach_session_to_exit(&mut event, self.session_id.as_deref());
        event
    }

    fn write_event<W: Write>(
        &mut self,
        mut event: Value,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        if !self.projection_admitted {
            let event_bytes = event.to_string().len();
            if self.deferred_event_bytes.saturating_add(event_bytes) <= DEFERRED_EVENT_LIMIT {
                self.deferred_event_bytes += event_bytes;
                self.deferred_events.push(event);
            } else if !self
                .integrity_failures
                .iter()
                .any(|failure| failure == "pre-session event projection buffer exhausted")
            {
                self.integrity_failures
                    .push("pre-session event projection buffer exhausted".to_string());
            }
            return Ok(());
        }
        assign_event_seq(&mut event, self.seq);
        write_ndjson_event(&self.request_id, writer, &event)?;
        self.advance_seq();
        Ok(())
    }

    fn advance_seq(&mut self) {
        self.seq += 1;
    }

    fn wait_duration(&self) -> Duration {
        self.deadline_wait_duration()
            .min(self.heartbeat_wait_duration())
    }

    fn heartbeat_wait_duration(&self) -> Duration {
        self.next_heartbeat
            .saturating_duration_since(Instant::now())
    }

    fn deadline_wait_duration(&self) -> Duration {
        let Some(deadline) = self.deadline_unix_ms else {
            return HEARTBEAT_INTERVAL;
        };
        Duration::from_millis(deadline.saturating_sub(now_unix_ms()))
    }

    fn deadline_reached(&self) -> bool {
        self.deadline_unix_ms
            .is_some_and(|deadline| now_unix_ms() >= deadline)
    }

    fn is_complete(&self) -> bool {
        self.final_status.is_some() && self.stdout_done && self.stderr_done
    }

    fn drains_done(&self) -> bool {
        self.stdout_done && self.stderr_done
    }

    fn record_child_exit(&mut self, status: ProcessStatus) {
        self.final_status = Some(status);
        self.child_exit_at.get_or_insert_with(Instant::now);
    }

    fn record_forced_exit(&mut self, status: ProcessStatus) {
        self.forced_exit_status = Some(status.clone());
        self.record_child_exit(status);
    }

    fn child_exit_grace_elapsed(&self) -> bool {
        self.child_exit_at
            .is_some_and(|exited_at| exited_at.elapsed() >= DRAIN_COMPLETION_GRACE)
    }

    fn emit_route_evidence<W: Write>(&mut self, writer: &mut W) -> Result<(), ProviderFailure> {
        self.marker_with_value(
            "oulipoly.launch_route".to_string(),
            self.route_evidence.clone(),
            writer,
        )
    }

    fn emit_integrity_evidence<W: Write>(&mut self, writer: &mut W) -> Result<(), ProviderFailure> {
        if self.stdout_truncated || self.stderr_truncated {
            self.marker_with_value(
                "oulipoly.terminal_capture_truncated".to_string(),
                json!({
                    "stdout": self.stdout_truncated,
                    "stderr": self.stderr_truncated,
                    "retained_tail_bytes_per_stream": TERMINAL_CAPTURE_LIMIT,
                }),
                writer,
            )?;
        }
        if !self.integrity_failures.is_empty() {
            self.marker_with_value(
                "oulipoly.launch_evidence_loss".to_string(),
                json!({ "failures": self.integrity_failures.clone() }),
                writer,
            )?;
        }
        if let Some(status) = self.forced_exit_status.as_ref() {
            self.marker_with_value(
                "oulipoly.response_cleanup_process_status".to_string(),
                process_status_json(status),
                writer,
            )?;
        }
        Ok(())
    }

    fn mark_drains_done(&mut self) {
        self.stdout_done = true;
        self.stderr_done = true;
    }
}

fn invalid_launch_params_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_launch_params",
        format!("launch params are invalid: {err}"),
    )
}

fn validate_policy_argv(argv: &[String], request_id: &str) -> Result<(), ProviderFailure> {
    if argv.is_empty() {
        return Err(empty_policy_argv_failure(request_id));
    }
    Ok(())
}

fn empty_policy_argv_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "empty_policy_argv",
        "policy.evaluate returned no launch argv",
    )
}

fn invalid_stdin_utf8_failure(
    request_id: &str,
    err: std::string::FromUtf8Error,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_stdin_utf8",
        format!("launch stdin must be UTF-8 at the policy boundary: {err}"),
    )
}

fn invalid_stdin_base64_failure(request_id: &str, err: String) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_stdin_base64",
        format!("launch stdin base64 is invalid: {err}"),
    )
}

fn invalid_stdin_encoding_failure(request_id: &str, encoding: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_stdin_encoding",
        format!("unsupported launch stdin encoding: {encoding}"),
    )
}

fn session_marker_name(session_id: &str) -> String {
    format!("opencode.sessionID.{session_id}")
}

fn assign_event_seq(event: &mut Value, seq: u64) {
    event["seq"] = json!(seq);
}

fn write_ndjson_event<W: Write>(
    request_id: &str,
    writer: &mut W,
    event: &Value,
) -> Result<(), ProviderFailure> {
    write_json_event(request_id, writer, event)?;
    write_event_newline(request_id, writer)?;
    flush_event_writer(request_id, writer)
}

fn write_json_event<W: Write>(
    request_id: &str,
    writer: &mut W,
    event: &Value,
) -> Result<(), ProviderFailure> {
    serde_json::to_writer(writer, event).map_err(|error| json_write_failure(request_id, error))
}

fn write_event_newline<W: Write>(request_id: &str, writer: &mut W) -> Result<(), ProviderFailure> {
    writer
        .write_all(b"\n")
        .map_err(|error| write_failure(request_id, error))
}

fn flush_event_writer<W: Write>(request_id: &str, writer: &mut W) -> Result<(), ProviderFailure> {
    writer
        .flush()
        .map_err(|error| write_failure(request_id, error))
}

fn deadline_status() -> ProcessStatus {
    ProcessStatus::ProlongedSilence {
        reason: "no output before host deadline".to_string(),
    }
}

fn spawn_error_status(err: std::io::Error) -> ProcessStatus {
    ProcessStatus::SpawnError {
        reason: err.to_string(),
    }
}

fn policy_rejection_status(reason: String) -> ProcessStatus {
    ProcessStatus::SpawnError { reason }
}

fn stream_bytes_event(request_id: &str, seq: u64, kind: &str, bytes: &[u8]) -> Value {
    json!({
        "contract": CONTRACT,
        "request_id": request_id,
        "seq": seq,
        "time_unix_ms": now_unix_ms(),
        "kind": kind,
        "data_base64": encode_base64(bytes),
    })
}

fn marker_event(request_id: &str, seq: u64, name: String, value: Value) -> Value {
    json!({
        "contract": CONTRACT,
        "request_id": request_id,
        "seq": seq,
        "time_unix_ms": now_unix_ms(),
        "kind": "marker",
        "name": name,
        "value": value,
    })
}

fn heartbeat_event(request_id: &str, seq: u64) -> Value {
    json!({
        "contract": CONTRACT,
        "request_id": request_id,
        "seq": seq,
        "time_unix_ms": now_unix_ms(),
        "kind": "heartbeat",
        "detail": "child still running",
    })
}

fn process_status_from_exit(status: ExitStatus) -> ProcessStatus {
    if let Some(code) = status.code() {
        return ProcessStatus::Exited { code };
    }
    signal_status(status)
}

fn is_clean_exit_status(status: &ProcessStatus) -> bool {
    matches!(status, ProcessStatus::Exited { code: 0 })
}

fn provider_error_terminal_signal(event: &OpencodeEventMetadata) -> Value {
    json!({
        "kind": "unknown",
        "evidence": provider_error_signal_evidence(event),
        "observed_at_unix_ms": event.timestamp,
    })
}

fn provider_error_signal_evidence(event: &OpencodeEventMetadata) -> String {
    bounded_text(
        &format!(
            "provider error: opencode {}: {}",
            opencode_error_name(event),
            opencode_error_message(event)
        ),
        TERMINAL_SIGNAL_EVIDENCE_MAX_LEN,
    )
}

fn opencode_error_name(event: &OpencodeEventMetadata) -> &str {
    nonblank_text_or(raw_opencode_error_name(event), "unknown")
}

fn raw_opencode_error_name(event: &OpencodeEventMetadata) -> Option<&str> {
    event.error.as_ref().and_then(|error| error.name.as_deref())
}

fn opencode_error_message(event: &OpencodeEventMetadata) -> &str {
    nonblank_text_or(raw_opencode_error_message(event), "unknown")
}

fn raw_opencode_error_message(event: &OpencodeEventMetadata) -> Option<&str> {
    event.error.as_ref().and_then(opencode_error_message_value)
}

fn opencode_error_message_value(error: &opencode::OpencodeEventError) -> Option<&str> {
    error.data.message.as_deref().or(error.message.as_deref())
}

fn nonblank_text_or<'a>(value: Option<&'a str>, fallback: &'a str) -> &'a str {
    value
        .filter(|text| !text.trim().is_empty())
        .unwrap_or(fallback)
}

fn launch_exit_event(request_id: &str, seq: u64, status: &ProcessStatus, signal: Value) -> Value {
    json!({
        "contract": CONTRACT,
        "request_id": request_id,
        "seq": seq,
        "time_unix_ms": now_unix_ms(),
        "kind": "exit",
        "status": process_status_json(status),
        "terminal_signal": signal,
    })
}

fn attach_session_to_exit(event: &mut Value, session_id: Option<&str>) {
    if let Some(session_id) = session_id {
        event["session"] = json!({
            "provider_session_id": session_id,
            "source": "opencode.run.format_json",
        });
    }
}

fn provider_exit_code(status: &ProcessStatus) -> i32 {
    exit_code_for_status(status)
}

#[cfg(unix)]
fn signal_status(status: ExitStatus) -> ProcessStatus {
    use std::os::unix::process::ExitStatusExt;
    status
        .signal()
        .map(|signal| ProcessStatus::SignalTerminated { signal })
        .unwrap_or(ProcessStatus::Unknown)
}

#[cfg(not(unix))]
fn signal_status(_status: ExitStatus) -> ProcessStatus {
    ProcessStatus::Unknown
}

fn spawn_failure(request_id: &str, context: &'static str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "launch_supervision_error",
        format!("{context} failed while supervising opencode: {err}"),
    )
}

fn write_failure(request_id: &str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "launch_write_error",
        format!("failed to write launch event: {err}"),
    )
}

fn json_write_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "launch_write_error",
        format!("failed to write launch event: {err}"),
    )
}

#[cfg(target_os = "linux")]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    let parent_pid = unsafe { getpid() };
    unsafe {
        command.pre_exec(move || set_current_process_group_with_parent_death(parent_pid));
    }
}

#[cfg(target_os = "linux")]
fn set_current_process_group_with_parent_death(parent_pid: i32) -> std::io::Result<()> {
    set_current_process_group()?;
    if unsafe { prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { getppid() } != parent_pid {
        return Err(std::io::Error::other(
            "provider parent exited before child custody was established",
        ));
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    unsafe {
        command.pre_exec(set_current_process_group);
    }
}

#[cfg(unix)]
fn set_current_process_group() -> std::io::Result<()> {
    if process_group_setup_failed(unsafe { setpgid(0, 0) }) {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn process_group_setup_failed(result: i32) -> bool {
    result == -1
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_child(child: &mut Child) -> Option<ExitStatus> {
    let pgid = child_process_group_id(child);
    send_process_group_signal(pgid, SIGTERM);
    std::thread::sleep(TERMINATION_GRACE);
    send_process_group_signal(pgid, SIGKILL);
    child.wait().ok()
}

#[cfg(unix)]
fn child_process_group_id(child: &Child) -> i32 {
    -(child.id() as i32)
}

#[cfg(unix)]
fn send_process_group_signal(pgid: i32, signal: i32) {
    unsafe {
        let _ = kill(pgid, signal);
    }
}

#[cfg(not(unix))]
fn terminate_child(child: &mut Child) -> Option<ExitStatus> {
    let _ = child.kill();
    child.wait().ok()
}

#[cfg(unix)]
const SIGTERM: i32 = 15;

#[cfg(unix)]
const SIGKILL: i32 = 9;

#[cfg(target_os = "linux")]
const PR_SET_PDEATHSIG: i32 = 1;

#[cfg(unix)]
extern "C" {
    fn setpgid(pid: i32, pgid: i32) -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
}

#[cfg(target_os = "linux")]
extern "C" {
    fn getpid() -> i32;
    fn getppid() -> i32;
    fn prctl(option: i32, arg2: i32, arg3: usize, arg4: usize, arg5: usize) -> i32;
}
