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

use crate::account::profile_for_wrapper_reference;
use crate::activity::ActivityTargets;
use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::encoding::{bounded_text, decode_base64, encode_base64, now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure, CONTRACT};
#[cfg(unix)]
use crate::native_process::GatedCommand;
use crate::native_process::{
    process_group_incarnation as launch_process_incarnation,
    process_group_is_live as launch_process_group_is_live,
    terminate_process_group_child as terminate_child, ExecGate as LaunchExecGate,
};
use crate::native_runtime;
use crate::opencode::{self, first_session_id, EventParser, OpencodeEventMetadata};
use crate::operation_bounds;
use crate::path_guard;
use crate::policy;
use crate::request_custody::{ActiveReservation, CustodyError, RequestCustody};
use crate::resume_observation::{
    self, DurableResumeObservationRequest, ResumeObservation, ResumeObservationRequest,
    RouteIdentity as ResumeRouteIdentity,
};
use crate::terminal::{classify, exit_code_for_status, process_status_json, ProcessStatus};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const COMPLETED_RESUME_GRACE: Duration = Duration::from_millis(500);
const DRAIN_COMPLETION_GRACE: Duration = Duration::from_millis(500);
const DRAIN_CHANNEL_CAPACITY: usize = 32;
const DEFERRED_EVENT_LIMIT: usize = 1024 * 1024;
const MAX_LAUNCH_INTEGRITY_FAILURES: usize = 32;
const MAX_LAUNCH_INTEGRITY_FAILURE_DETAIL_BYTES: usize = 512;
const MAX_LAUNCH_INTEGRITY_EVIDENCE_BYTES: usize = 32 * 1024;
const LAUNCH_RECOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_LAUNCH_RECOVERY_CANDIDATES: usize = 8;
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
const LAUNCH_ORPHAN_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const LAUNCH_STATE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const LAUNCH_STATE_SCHEMA_VERSION: u32 = 10;
const MAX_ACTIVE_LAUNCH_REQUEST_RECORDS: usize = 64;
const MAX_LAUNCH_REPLAY_RECORDS: usize = 4096;
const MAX_LAUNCH_STATE_BYTES: usize = 256 * 1024;
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
    request_identity_sha256: String,
    binding_sha256: String,
    prompt_sha256: Option<String>,
    #[serde(default)]
    delivery_nonce: Option<String>,
    recovery: LaunchRecoveryIdentity,
    phase: LaunchRequestPhase,
    actor_process_group_id: Option<u32>,
    #[serde(default)]
    actor_process_group_incarnation: Option<String>,
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
    #[serde(default)]
    program_sha256: String,
    #[serde(default)]
    native_contract_id: String,
    #[serde(default)]
    fixed_args: Vec<String>,
    #[serde(default)]
    implementation_manifest_id: String,
    #[serde(default)]
    implementation_version: String,
    #[serde(default)]
    program_stamp: native_runtime::NativeProgramStamp,
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
    request_identity_sha256: String,
    binding_sha256: String,
    observation: DurableResumeObservationRequest,
    recovery: LaunchRecoveryIdentity,
    phase: ResumeLaunchRequestPhase,
    actor_process_group_id: Option<u32>,
    #[serde(default)]
    actor_process_group_incarnation: Option<String>,
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
    _lock: fs::File,
}

enum PreparedLaunchRecovery {
    NoEffectObserved,
    SessionObserved(String),
    Ambiguous(Vec<String>),
}

/// The shared conclusion of interpreting an existing durable launch request.
///
/// Every other conclusion is a terminal `ProviderFailure` that tells the caller
/// to reconcile or retry later. This one result authorizes the phase-specific
/// caller to retire the old state and, when appropriate, admit a fresh actor.
enum ExistingLaunchRetryOutcome {
    AuthoritativeNoEffect,
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
    validate_launch_authority(&params, request_id)?;
    let new_session = known_provider_session_id(&params).is_none();
    let request_identity_sha256 = launch_request_identity_sha256(host, &raw_params);
    let declared_env = params.env.clone().unwrap_or_default();
    preflight_existing_launch_replay(
        host,
        request_id,
        &request_identity_sha256,
        new_session,
        &declared_env,
    )?;
    let effective = match launch_argv(&params, host, request_id, &request_identity_sha256)? {
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
    let binding_sha256 =
        launch_request_binding_sha256(host, &raw_params, &effective.route_evidence);
    let prompt_sha256 = effective
        .prompt
        .as_deref()
        .map(|prompt| sha256_hex(prompt.as_bytes()));
    let recovery = effective.recovery;
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
            Some(LaunchRequestGuard::admit_after_policy(
                host,
                request_id,
                request_identity_sha256,
                binding_sha256,
                prompt_sha256,
                effective.delivery_nonce,
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
            Some(ResumeLaunchRequestGuard::admit_after_policy(
                host,
                request_id,
                request_identity_sha256,
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
    let (child, launch_exec_gate) = match spawn_child(
        &effective.argv,
        &params.working_directory,
        &effective.execution_env,
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
    let actor_process_group_id = custody.child_mut().id();
    let actor_process_group_incarnation = launch_process_incarnation(actor_process_group_id)
        .map_err(|error| {
            spawn_failure(
                request_id,
                "identify durably registered launch actor",
                error,
            )
        })?;
    if let Some(launch_request) = state.launch_request.as_mut() {
        launch_request.observe_actor(actor_process_group_id, &actor_process_group_incarnation)?;
    }
    if let Some(launch_request) = state.resume_launch_request.as_mut() {
        launch_request.observe_actor(actor_process_group_id, &actor_process_group_incarnation)?;
    }
    if let Some(launch_exec_gate) = launch_exec_gate {
        launch_exec_gate.release().map_err(|error| {
            spawn_failure(request_id, "release durably registered launch actor", error)
        })?;
    }
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
    execution_env: BTreeMap<String, String>,
    stdin: Option<Vec<u8>>,
    prompt: Option<String>,
    delivery_nonce: Option<String>,
    resume_observation_request: Option<ResumeObservationRequest>,
    route_evidence: Value,
    activity_targets: ActivityTargets,
    recovery: LaunchRecoveryContext,
}

enum PolicyLaunch {
    Accepted(Box<EffectiveLaunch>),
    Rejected(String),
}

fn launch_argv(
    params: &LaunchParams,
    host: &HostContext,
    request_id: &str,
    request_identity_sha256: &str,
) -> Result<PolicyLaunch, ProviderFailure> {
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
            effective_launch(params, plan, host, request_id, request_identity_sha256)?,
        ))),
        policy::PolicyDecision::Rejected(plan) => {
            Ok(PolicyLaunch::Rejected(policy_rejection_reason(&plan)))
        }
    }
}

fn effective_launch(
    params: &LaunchParams,
    plan: policy::PolicyLaunchPlan,
    host: &HostContext,
    request_id: &str,
    request_identity_sha256: &str,
) -> Result<EffectiveLaunch, ProviderFailure> {
    let policy::PolicyLaunchPlan {
        mut argv,
        env,
        stdin,
        prompt,
        diagnostics: _,
        markers,
        route,
    } = plan;
    validate_policy_argv(&argv, request_id)?;
    let account = profile_for_wrapper_reference(&route.account_wrapper).ok_or_else(|| {
        ProviderFailure::internal(
            request_id,
            "native_runtime_account_invalid",
            "accepted launch route does not name a declared canonical account",
        )
    })?;
    let configured_program = argv.first().map(String::as_str).unwrap_or_default();
    let native_runtime =
        native_runtime::resolve_for_launch(host, account, configured_program, &env, request_id)?;
    let execution_env = native_runtime.execution_environment(&env);
    if let Some(program) = argv.first_mut() {
        *program = native_runtime.program().to_string();
    }
    argv.splice(1..1, native_runtime.fixed_args().iter().cloned());
    let recovery =
        launch_recovery_context(&native_runtime, &params.working_directory, &env, &route);
    let argv = resume_argv(
        params,
        argv,
        stdin.as_deref().map(str::as_bytes),
        request_id,
    )?;
    let submitted_payload = submitted_launch_payload(&argv, stdin.as_deref().map(str::as_bytes));
    let delivery_nonce = Some(launch_delivery_nonce(
        host,
        request_id,
        request_identity_sha256,
    ));
    let (argv, stdin) = attach_launch_delivery_marker(
        argv,
        stdin,
        delivery_nonce
            .as_deref()
            .expect("provider-authored launch delivery nonce"),
    );
    let resume_observation_request =
        resume_observation_request(params, submitted_payload, delivery_nonce.as_deref(), &route);
    let mut activity_targets = ActivityTargets::default();
    policy::append_route_activity_targets(&mut activity_targets, &route);
    let argv = split_oversized_prompt_argv(argv, request_id)?;
    Ok(EffectiveLaunch {
        argv,
        execution_env,
        stdin: stdin.map(String::into_bytes),
        prompt,
        delivery_nonce,
        resume_observation_request,
        route_evidence: json!(markers),
        activity_targets,
        recovery,
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
    request_id: &str,
) -> Result<Vec<String>, ProviderFailure> {
    let Some(session_id) = known_provider_session_id(params) else {
        return Ok(argv);
    };
    require_resume_payload_reaches_child(&argv, stdin, request_id)?;
    let insert_at = resume_session_insert_index(&argv);
    upsert_session_arg(&mut argv, session_id, insert_at);
    Ok(argv)
}

fn resume_observation_request(
    params: &LaunchParams,
    submitted_payload: Option<String>,
    delivery_nonce: Option<&str>,
    route: &policy::PolicyRouteIdentity,
) -> Option<ResumeObservationRequest> {
    let session_id = known_provider_session_id(params)?;
    let prompt = submitted_payload?;
    let delivery_nonce = delivery_nonce?;
    Some(ResumeObservationRequest::new(
        route.account_wrapper.clone(),
        session_id.to_string(),
        prompt,
        delivery_nonce.to_string(),
        now_unix_ms(),
        ResumeRouteIdentity {
            provider_id: route.provider_id.clone(),
            model_id: route.model_id.clone(),
            variant: route.effort.to_string(),
        },
    ))
}

fn known_provider_session_id(params: &LaunchParams) -> Option<&str> {
    params
        .session
        .as_ref()
        .and_then(|session| session.known_provider_session_id.as_deref())
        .filter(|session_id| !session_id.trim().is_empty())
}

fn validate_launch_authority(
    params: &LaunchParams,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if let Some(arg) = policy::forbidden_user_launch_arg(&params.model, &params.argv) {
        return Err(ProviderFailure::invalid_request(
            request_id,
            "native_launch_control_forbidden",
            format!(
                "provider-managed native OpenCode control {arg} is forbidden in the user launch suffix before --; use typed launch fields or place literal message text after --"
            ),
        ));
    }
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
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if has_unbounded_option_shaped_message_arg(argv) {
        return Err(ProviderFailure::invalid_request(
            request_id,
            "ambiguous_resume_payload",
            "resume launch with option-shaped positional values must place -- before the message so its exact native identity can be retained",
        ));
    }
    if submitted_launch_payload(argv, stdin).is_some() {
        return Ok(());
    }
    Err(empty_resume_payload_failure(request_id))
}

fn submitted_launch_payload(argv: &[String], stdin: Option<&[u8]>) -> Option<String> {
    stdin
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .filter(|text| !text.trim().is_empty())
        .map(str::to_string)
        .or_else(|| opencode_argv_payload(argv))
}

fn launch_delivery_nonce(
    host: &HostContext,
    request_id: &str,
    request_identity_sha256: &str,
) -> String {
    sha256_hex(
        [
            b"agent-runner-opencode.launch.delivery.v1".as_slice(),
            &[0],
            host.app.as_bytes(),
            &[0],
            host.data_root.as_deref().unwrap_or_default().as_bytes(),
            &[0],
            request_id.as_bytes(),
            &[0],
            request_identity_sha256.as_bytes(),
        ]
        .concat()
        .as_slice(),
    )
}

fn attach_launch_delivery_marker(
    mut argv: Vec<String>,
    mut stdin: Option<String>,
    delivery_nonce: &str,
) -> (Vec<String>, Option<String>) {
    // Product decision: the pinned native boundary has no out-of-band,
    // crash-surviving request identity. Every admitted payload therefore
    // carries this provider-authored item so response-loss recovery can remain
    // request-local and at-most-once. It is intentionally model-visible and is
    // not claimed to preserve byte-exact caller-payload semantics; README and
    // the native-state contract define the affected actors and fidelity cost.
    let marker = resume_observation::delivery_marker(delivery_nonce);
    if stdin
        .as_ref()
        .is_some_and(|payload| !payload.trim().is_empty())
    {
        let payload = stdin.as_mut().expect("checked launch stdin payload");
        if !payload.ends_with('\n') {
            payload.push('\n');
        }
        payload.push('\n');
        payload.push_str(&marker);
        payload.push('\n');
        return (argv, stdin);
    }
    argv.push(marker);
    (argv, stdin)
}

fn opencode_argv_payload(argv: &[String]) -> Option<String> {
    let (message_start, _) = opencode_message_region(argv)?;
    let message_args = &argv[message_start..];
    if has_unbounded_option_shaped_message_arg(argv) {
        return None;
    }
    let payload = match message_args {
        [single] => single.clone(),
        multiple => multiple
            .iter()
            .map(|arg| {
                if arg.contains(' ') {
                    format!("\"{}\"", arg.replace('"', "\\\""))
                } else {
                    arg.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
    };
    (!payload.trim().is_empty()).then_some(payload)
}

fn has_unbounded_option_shaped_message_arg(argv: &[String]) -> bool {
    opencode_message_region(argv).is_some_and(|(message_start, has_boundary)| {
        !has_boundary && argv[message_start..].iter().any(|arg| arg.starts_with('-'))
    })
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
    let option_end = argv
        .iter()
        .position(|arg| arg == "--")
        .unwrap_or(argv.len());
    if let Some(index) = argv[..option_end]
        .iter()
        .position(|arg| arg == OPENCODE_SESSION_FLAG)
    {
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

#[cfg(unix)]
fn spawn_child(
    argv: &[String],
    working_directory: &str,
    env: &BTreeMap<String, String>,
    stdin: Stdio,
) -> std::io::Result<(Child, Option<LaunchExecGate>)> {
    let (program, args) = argv.split_first().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "launch argv is empty")
    })?;
    let mut command = GatedCommand::new(program, args)?;
    command
        .command_mut()
        .current_dir(working_directory)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(stdin)
        .env_clear()
        .envs(env);
    command.spawn().map(|(child, gate)| (child, Some(gate)))
}

#[cfg(not(unix))]
fn spawn_child(
    _argv: &[String],
    _working_directory: &str,
    _env: &BTreeMap<String, String>,
    _stdin: Stdio,
) -> std::io::Result<(Child, Option<LaunchExecGate>)> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "native launch requires Unix process-group custody",
    ))
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

fn run_supervision_loop<W: Write>(
    child: &mut Child,
    receiver: &Receiver<DrainMessage>,
    state: &mut LaunchState,
    writer: &mut W,
) -> Result<(), ProviderFailure> {
    while !state.is_complete() {
        capture_child_exit(child, state)?;
        enforce_deadline(child, state)?;
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

/// Resolve settings-independent exact-retry state before consulting mutable
/// policy or settings. The later admission phase repeats the file lookup while
/// holding the creating lock, but both phases delegate the durable state
/// meaning to the same operation-specific interpreters below.
fn preflight_existing_launch_replay(
    host: &HostContext,
    request_id: &str,
    request_identity_sha256: &str,
    new_session: bool,
    declared_env: &BTreeMap<String, String>,
) -> Result<(), ProviderFailure> {
    let root = launch_state_root(host, request_id)?;
    match fs::metadata(&root) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(launch_state_failure(
                request_id,
                "launch request state root is not a directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(launch_state_failure(request_id, error)),
    }
    durable_fs::create_private_directories(&root)
        .map_err(|error| launch_state_failure(request_id, error))?;
    let key = sha256_hex(request_id.as_bytes());
    let state_path =
        confined_launch_state_target(host, &root.join(format!("{key}.json")), request_id)?;
    match fs::metadata(&state_path) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            return Err(launch_state_failure(
                request_id,
                "launch state is not a file",
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(launch_state_failure(request_id, error)),
    }
    let lock = acquire_launch_request_lock(
        host,
        &root,
        &state_path,
        false,
        request_identity_sha256,
        request_id,
    )?;
    let bytes = durable_fs::read_file_bounded(&state_path, MAX_LAUNCH_STATE_BYTES)
        .map_err(|error| launch_state_failure(request_id, error))?;
    let expected_kind = if new_session {
        LaunchOperationKind::NewSession
    } else {
        LaunchOperationKind::Resume
    };
    validate_launch_operation_kind(&bytes, expected_kind, request_id, request_identity_sha256)?;
    let outcome = if new_session {
        interpret_existing_new_session_retry(
            host,
            &bytes,
            &state_path,
            request_id,
            request_identity_sha256,
            declared_env,
        )?
    } else {
        let state = decode_existing_resume_retry(&bytes, request_id, request_identity_sha256)?;
        interpret_existing_resume_retry(state, &state_path, request_id, declared_env)?
    };
    match outcome {
        ExistingLaunchRetryOutcome::AuthoritativeNoEffect => {
            remove_launch_request_state(&state_path, request_id)?;
            drop(lock);
            retire_orphan_launch_request_lock(&state_path, request_id)
        }
    }
}

/// Interpret the durable state machine for one existing new-session request.
/// Callers own when the state was discovered and what to do with an
/// authoritative no-effect result; this function is the single authority for
/// the stored phases and their recovery observations.
fn interpret_existing_new_session_retry(
    host: &HostContext,
    bytes: &[u8],
    state_path: &Path,
    request_id: &str,
    request_identity_sha256: &str,
    declared_env: &BTreeMap<String, String>,
) -> Result<ExistingLaunchRetryOutcome, ProviderFailure> {
    let mut state: LaunchRequestState =
        serde_json::from_slice(bytes).map_err(|error| launch_state_invalid(request_id, error))?;
    validate_launch_request_state(&state, request_id)?;
    if state.schema_version == 5 {
        state.delivery_nonce = None;
    }
    if state.request_identity_sha256 != request_identity_sha256 {
        return Err(launch_request_reuse_conflict(
            request_id,
            request_identity_sha256,
            &state,
        ));
    }
    match state.phase {
        LaunchRequestPhase::SessionObserved => {
            Err(launch_session_reconciliation_required(request_id, &state))
        }
        LaunchRequestPhase::TerminalWithoutSession => {
            Err(launch_terminal_reconciliation_required(request_id, &state))
        }
        LaunchRequestPhase::Prepared => {
            validate_launch_recovery_environment(&state.recovery, declared_env, request_id)?;
            require_prior_actor_terminal(
                state.actor_process_group_id,
                state.actor_process_group_incarnation.as_deref(),
                request_id,
                &state.binding_sha256,
            )?;
            match recover_prepared_launch(host, &state, declared_env, request_id)? {
                PreparedLaunchRecovery::NoEffectObserved => {
                    Ok(ExistingLaunchRetryOutcome::AuthoritativeNoEffect)
                }
                PreparedLaunchRecovery::SessionObserved(provider_session_id) => {
                    state.phase = LaunchRequestPhase::SessionObserved;
                    state.provider_session_id = Some(provider_session_id);
                    state.terminal_status = None;
                    state.observed_at_unix_ms = Some(now_unix_ms());
                    write_launch_request_state(state_path, &state, request_id)?;
                    Err(launch_session_reconciliation_required(request_id, &state))
                }
                PreparedLaunchRecovery::Ambiguous(candidates) => Err(
                    launch_session_recovery_required(request_id, &state, candidates),
                ),
            }
        }
    }
}

/// Interpret the durable state machine for one existing resumed-turn request.
/// Like the new-session interpreter, this is independent of whether the caller
/// is doing the early replay preflight or the locked post-policy admission.
fn decode_existing_resume_retry(
    bytes: &[u8],
    request_id: &str,
    request_identity_sha256: &str,
) -> Result<ResumeLaunchRequestState, ProviderFailure> {
    let mut state: ResumeLaunchRequestState =
        serde_json::from_slice(bytes).map_err(|error| launch_state_invalid(request_id, error))?;
    validate_resume_launch_request_state(&state, request_id)?;
    if state.schema_version == 5 {
        state.observation.delivery_nonce = None;
    }
    if state.request_identity_sha256 != request_identity_sha256 {
        return Err(resume_launch_request_reuse_conflict(
            request_id,
            request_identity_sha256,
            &state,
        ));
    }
    Ok(state)
}

fn interpret_existing_resume_retry(
    mut state: ResumeLaunchRequestState,
    state_path: &Path,
    request_id: &str,
    declared_env: &BTreeMap<String, String>,
) -> Result<ExistingLaunchRetryOutcome, ProviderFailure> {
    match state.phase {
        ResumeLaunchRequestPhase::CompletionObserved => {
            Err(resume_launch_reconciliation_required(request_id, &state))
        }
        ResumeLaunchRequestPhase::SubmissionObserved | ResumeLaunchRequestPhase::Unresolved => {
            validate_launch_recovery_environment(&state.recovery, declared_env, request_id)?;
            let prior_phase = state.phase;
            require_prior_actor_terminal(
                state.actor_process_group_id,
                state.actor_process_group_incarnation.as_deref(),
                request_id,
                &state.binding_sha256,
            )?;
            let recovered = observe_durable_resume(
                &state.observation,
                &state.recovery,
                declared_env,
                request_id,
            )?;
            if recovered.completion_observed() {
                state.phase = ResumeLaunchRequestPhase::CompletionObserved;
                state.observed_at_unix_ms = Some(now_unix_ms());
                write_launch_request_state(state_path, &state, request_id)?;
                return Err(resume_launch_reconciliation_required(request_id, &state));
            }
            if recovered.submitted_user_turn.is_some() {
                state.phase = ResumeLaunchRequestPhase::SubmissionObserved;
                state.observed_at_unix_ms = Some(now_unix_ms());
                write_launch_request_state(state_path, &state, request_id)?;
                return Err(resume_launch_reconciliation_required(request_id, &state));
            }
            if prior_phase == ResumeLaunchRequestPhase::Unresolved && recovered.available {
                return Ok(ExistingLaunchRetryOutcome::AuthoritativeNoEffect);
            }
            if prior_phase == ResumeLaunchRequestPhase::SubmissionObserved {
                return Err(resume_launch_reconciliation_required(request_id, &state));
            }
            Err(resume_launch_recovery_unavailable(request_id, &state))
        }
        ResumeLaunchRequestPhase::Prepared
        | ResumeLaunchRequestPhase::TerminalWithoutSubmission => {
            validate_launch_recovery_environment(&state.recovery, declared_env, request_id)?;
            if state.phase == ResumeLaunchRequestPhase::Prepared {
                require_prior_actor_terminal(
                    state.actor_process_group_id,
                    state.actor_process_group_incarnation.as_deref(),
                    request_id,
                    &state.binding_sha256,
                )?;
            }
            let recovered = observe_durable_resume(
                &state.observation,
                &state.recovery,
                declared_env,
                request_id,
            )?;
            if !recovered.available {
                state.phase = ResumeLaunchRequestPhase::Unresolved;
                state.observed_at_unix_ms = Some(now_unix_ms());
                write_launch_request_state(state_path, &state, request_id)?;
                return Err(resume_launch_recovery_unavailable(request_id, &state));
            }
            if recovered.submitted_user_turn.is_some() {
                state.phase = if recovered.completion_observed() {
                    ResumeLaunchRequestPhase::CompletionObserved
                } else {
                    ResumeLaunchRequestPhase::SubmissionObserved
                };
                state.observed_at_unix_ms = Some(now_unix_ms());
                write_launch_request_state(state_path, &state, request_id)?;
                return Err(resume_launch_reconciliation_required(request_id, &state));
            }
            Ok(ExistingLaunchRetryOutcome::AuthoritativeNoEffect)
        }
    }
}

impl LaunchRequestGuard {
    /// Recheck exact-retry state while holding the creating request lock after
    /// policy has selected the immutable route, then publish a fresh prepared
    /// record only when the shared interpreter proves no prior effect.
    fn admit_after_policy(
        host: &HostContext,
        request_id: &str,
        request_identity_sha256: String,
        binding_sha256: String,
        prompt_sha256: Option<String>,
        delivery_nonce: Option<String>,
        recovery: LaunchRecoveryContext,
    ) -> Result<Self, ProviderFailure> {
        let root = launch_state_root(host, request_id)?;
        durable_fs::create_private_directories(&root)
            .map_err(|error| launch_state_failure(request_id, error))?;
        let key = sha256_hex(request_id.as_bytes());
        let state_path =
            confined_launch_state_target(host, &root.join(format!("{key}.json")), request_id)?;
        let lock = acquire_launch_request_lock(
            host,
            &root,
            &state_path,
            true,
            &request_identity_sha256,
            request_id,
        )?;
        match durable_fs::read_file_bounded(&state_path, MAX_LAUNCH_STATE_BYTES) {
            Ok(bytes) => {
                validate_launch_operation_kind(
                    &bytes,
                    LaunchOperationKind::NewSession,
                    request_id,
                    &binding_sha256,
                )?;
                let ExistingLaunchRetryOutcome::AuthoritativeNoEffect =
                    interpret_existing_new_session_retry(
                        host,
                        &bytes,
                        &state_path,
                        request_id,
                        &request_identity_sha256,
                        &recovery.declared_env,
                    )?;
                remove_launch_request_state(&state_path, request_id)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(launch_state_failure(request_id, error)),
        }
        let state = LaunchRequestState {
            schema_version: LAUNCH_STATE_SCHEMA_VERSION,
            operation_kind: LaunchOperationKind::NewSession,
            request_id: request_id.to_string(),
            request_identity_sha256,
            binding_sha256,
            prompt_sha256,
            delivery_nonce,
            recovery: recovery.identity,
            phase: LaunchRequestPhase::Prepared,
            actor_process_group_id: None,
            actor_process_group_incarnation: None,
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

    fn observe_actor(
        &mut self,
        process_group_id: u32,
        process_group_incarnation: &str,
    ) -> Result<(), ProviderFailure> {
        observe_launch_actor(
            &mut self.state.actor_process_group_id,
            &mut self.state.actor_process_group_incarnation,
            process_group_id,
            process_group_incarnation,
            &self.state.request_id,
            &self.state.binding_sha256,
        )?;
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
        let state_path = self.state_path.clone();
        let request_id = self.state.request_id.clone();
        remove_launch_request_state(&state_path, &request_id)?;
        drop(self);
        retire_orphan_launch_request_lock(&state_path, &request_id)
    }
}

impl ResumeLaunchRequestGuard {
    /// Recheck exact-retry state while holding the creating request lock after
    /// policy has selected the immutable route, then publish a fresh prepared
    /// record only when the shared interpreter proves no prior effect.
    fn admit_after_policy(
        host: &HostContext,
        request_id: &str,
        request_identity_sha256: String,
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
        let lock = acquire_launch_request_lock(
            host,
            &root,
            &state_path,
            true,
            &request_identity_sha256,
            request_id,
        )?;
        match durable_fs::read_file_bounded(&state_path, MAX_LAUNCH_STATE_BYTES) {
            Ok(bytes) => {
                validate_launch_operation_kind(
                    &bytes,
                    LaunchOperationKind::Resume,
                    request_id,
                    &binding_sha256,
                )?;
                let state =
                    decode_existing_resume_retry(&bytes, request_id, &request_identity_sha256)?;
                validate_launch_recovery_environment(
                    &state.recovery,
                    &recovery.declared_env,
                    request_id,
                )?;
                let ExistingLaunchRetryOutcome::AuthoritativeNoEffect =
                    interpret_existing_resume_retry(
                        state,
                        &state_path,
                        request_id,
                        &recovery.declared_env,
                    )?;
                remove_launch_request_state(&state_path, request_id)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(launch_state_failure(request_id, error)),
        }
        let state = ResumeLaunchRequestState {
            schema_version: LAUNCH_STATE_SCHEMA_VERSION,
            operation_kind: LaunchOperationKind::Resume,
            request_id: request_id.to_string(),
            request_identity_sha256,
            binding_sha256,
            observation,
            recovery: recovery.identity,
            phase: ResumeLaunchRequestPhase::Prepared,
            actor_process_group_id: None,
            actor_process_group_incarnation: None,
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

    fn observe_actor(
        &mut self,
        process_group_id: u32,
        process_group_incarnation: &str,
    ) -> Result<(), ProviderFailure> {
        observe_launch_actor(
            &mut self.state.actor_process_group_id,
            &mut self.state.actor_process_group_incarnation,
            process_group_id,
            process_group_incarnation,
            &self.state.request_id,
            &self.state.binding_sha256,
        )?;
        write_launch_request_state(&self.state_path, &self.state, &self.state.request_id)
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
        let state_path = self.state_path.clone();
        let request_id = self.state.request_id.clone();
        remove_launch_request_state(&state_path, &request_id)?;
        drop(self);
        retire_orphan_launch_request_lock(&state_path, &request_id)
    }
}

fn remove_launch_request_state(path: &Path, request_id: &str) -> Result<(), ProviderFailure> {
    let root = path
        .parent()
        .expect("launch state path always has a parent");
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(launch_state_failure(request_id, error)),
    }
    durable_fs::sync_directory(root).map_err(|error| launch_state_failure(request_id, error))
}

fn retire_orphan_launch_request_lock(
    state_path: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let root = state_path
        .parent()
        .expect("launch state path always has a parent");
    let capacity = open_launch_state_lock(&root.join(".capacity.lock"))
        .map_err(|error| launch_state_failure(request_id, error))?;
    if !operation_bounds::lock_exclusive_for(&capacity, LAUNCH_STATE_LOCK_TIMEOUT)
        .map_err(|error| launch_state_failure(request_id, error))?
    {
        return Err(launch_state_lock_timeout(request_id));
    }
    if state_path.exists() {
        return Ok(());
    }
    let lock_path = state_path.with_extension("lock");
    let lock = open_launch_state_lock(&lock_path)
        .map_err(|error| launch_state_failure(request_id, error))?;
    match fs2::FileExt::try_lock_exclusive(&lock) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
        Err(error) => return Err(launch_state_failure(request_id, error)),
    }
    if state_path.exists() {
        return Ok(());
    }
    launch_request_custody(root)
        .remove_active_marker(state_path)
        .map_err(|error| launch_custody_failure(request_id, error))?;
    match fs::remove_file(&lock_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(launch_state_failure(request_id, error)),
    }
    durable_fs::sync_directory(root).map_err(|error| launch_state_failure(request_id, error))
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

fn launch_request_identity_sha256(host: &HostContext, params: &Value) -> String {
    sha256_hex(
        json!({
            "host_app": host.app,
            "params": params,
        })
        .to_string()
        .as_bytes(),
    )
}

fn launch_recovery_context(
    runtime: &native_runtime::NativeRuntimeContext,
    working_directory: &str,
    declared_env: &BTreeMap<String, String>,
    route: &policy::PolicyRouteIdentity,
) -> LaunchRecoveryContext {
    LaunchRecoveryContext {
        identity: LaunchRecoveryIdentity {
            program: runtime.program().to_string(),
            program_sha256: runtime.program_sha256().to_string(),
            native_contract_id: runtime.native_contract_id().to_string(),
            fixed_args: runtime.fixed_args().to_vec(),
            implementation_manifest_id: runtime.implementation_manifest_id().to_string(),
            implementation_version: runtime.implementation_version().to_string(),
            program_stamp: runtime.program_stamp().clone(),
            passthrough_env: runtime.stable_execution_env().clone(),
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

fn validate_launch_recovery_environment(
    identity: &LaunchRecoveryIdentity,
    declared_env: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if identity.declared_env_sha256 == launch_environment_sha256(declared_env) {
        return Ok(());
    }
    Err(launch_recovery_unavailable(
        request_id,
        "the exact retry did not reproduce the declared child environment",
    ))
}

fn validate_launch_recovery_implementation(
    recovery: &LaunchRecoveryIdentity,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if !launch_recovery_identity_is_manifest_bound(recovery) {
        return Err(launch_recovery_unavailable(
            request_id,
            "the durable request predates the pinned direct OpenCode recovery contract; reconcile it without invoking an unbound implementation",
        ));
    }
    let program = Path::new(&recovery.program);
    let validation = if recovery.program_stamp.is_complete() {
        native_runtime::validate_pinned_program(
            program,
            &recovery.program_sha256,
            &recovery.implementation_manifest_id,
            &recovery.implementation_version,
            &recovery.native_contract_id,
            &recovery.program_stamp,
        )
    } else {
        native_runtime::validate_predecessor_pinned_program(
            program,
            &recovery.program_sha256,
            &recovery.implementation_manifest_id,
            &recovery.implementation_version,
            &recovery.native_contract_id,
        )
    };
    validation.map_err(|error| launch_recovery_unavailable(request_id, error))
}

fn launch_recovery_identity_is_manifest_bound(recovery: &LaunchRecoveryIdentity) -> bool {
    recovery.native_contract_id == native_runtime::OPENCODE_NATIVE_CONTRACT_ID
        && recovery
            .fixed_args
            .iter()
            .map(String::as_str)
            .eq(native_runtime::OPENCODE_NATIVE_FIXED_ARGS.iter().copied())
        && !recovery.program_sha256.trim().is_empty()
        && !recovery.implementation_manifest_id.trim().is_empty()
        && !recovery.implementation_version.trim().is_empty()
        && Path::new(&recovery.program).is_absolute()
}

fn launch_recovery_identity_is_current(recovery: &LaunchRecoveryIdentity) -> bool {
    launch_recovery_identity_is_manifest_bound(recovery) && recovery.program_stamp.is_complete()
}

fn observe_durable_resume(
    observation: &DurableResumeObservationRequest,
    recovery: &LaunchRecoveryIdentity,
    declared_env: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<ResumeObservation, ProviderFailure> {
    validate_launch_recovery_implementation(recovery, request_id)?;
    let mut env = recovery.passthrough_env.clone();
    env.extend(declared_env.clone());
    Ok(resume_observation::observe_durable(
        observation,
        &recovery.program,
        &recovery.fixed_args,
        &recovery.working_directory,
        &env,
    ))
}

fn observe_launch_actor(
    committed_process_group_id: &mut Option<u32>,
    committed_incarnation: &mut Option<String>,
    observed_process_group_id: u32,
    observed_incarnation: &str,
    request_id: &str,
    binding_sha256: &str,
) -> Result<(), ProviderFailure> {
    match (
        *committed_process_group_id,
        committed_incarnation.as_deref(),
    ) {
        (Some(existing_process_group_id), Some(existing_incarnation))
            if existing_process_group_id == observed_process_group_id
                && existing_incarnation == observed_incarnation =>
        {
            Ok(())
        }
        (Some(existing_process_group_id), existing_incarnation) => Err(ProviderFailure::conflict(
            request_id,
            "launch_actor_conflict",
            "one launch request observed conflicting native process-group incarnations",
            json!({
                "binding_sha256": binding_sha256,
                "committed_process_group_id": existing_process_group_id,
                "committed_process_group_incarnation": existing_incarnation,
                "observed_process_group_id": observed_process_group_id,
                "observed_process_group_incarnation": observed_incarnation,
            }),
        )),
        (None, None) => {
            *committed_process_group_id = Some(observed_process_group_id);
            *committed_incarnation = Some(observed_incarnation.to_string());
            Ok(())
        }
        (None, Some(_)) => Err(launch_state_invalid(
            request_id,
            "launch actor incarnation exists without a process-group identity",
        )),
    }
}

fn require_prior_actor_terminal(
    process_group_id: Option<u32>,
    process_group_incarnation: Option<&str>,
    request_id: &str,
    binding_sha256: &str,
) -> Result<(), ProviderFailure> {
    let Some(process_group_id) = process_group_id else {
        // The exec gate cannot release the native command until actor
        // publication succeeds. Losing the provider before publication closes
        // the gate pipe, so an actor-less prepared record has admitted no
        // native effect and may proceed to authoritative native recovery.
        return Ok(());
    };
    if !launch_process_group_is_live(process_group_id) {
        return Ok(());
    }
    let Some(expected_incarnation) = process_group_incarnation else {
        return Err(launch_actor_reconciliation_required(
            request_id,
            binding_sha256,
            Some(process_group_id),
            "a predecessor launch record has a live process-group number but no durable actor incarnation",
        ));
    };
    match launch_process_incarnation(process_group_id) {
        Ok(current_incarnation) if current_incarnation != expected_incarnation => Ok(()),
        Ok(_) => Err(launch_actor_reconciliation_required(
            request_id,
            binding_sha256,
            Some(process_group_id),
            "the previously admitted native process-group incarnation is still alive",
        )),
        Err(error) if !launch_process_group_is_live(process_group_id) => Ok(()),
        Err(error) => Err(launch_actor_reconciliation_required(
            request_id,
            binding_sha256,
            Some(process_group_id),
            &format!("the live process group leader incarnation could not be verified: {error}"),
        )),
    }
}

fn recover_prepared_launch(
    host: &HostContext,
    state: &LaunchRequestState,
    declared_env: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<PreparedLaunchRecovery, ProviderFailure> {
    let started = Instant::now();
    validate_launch_recovery_implementation(&state.recovery, request_id)?;
    let env = launch_recovery_environment(state, declared_env);
    let sessions = opencode::session_list_with_launch_context(
        &state.recovery.program,
        &state.recovery.fixed_args,
        &state.recovery.working_directory,
        &env,
        None,
        launch_recovery_remaining(host, started, request_id)?,
    )
    .map_err(|error| launch_recovery_unavailable(request_id, format!("{error:?}")))?;
    let mut candidates = sessions
        .iter()
        .filter(|entry| launch_recovery_session_is_plausible(entry, state))
        .map(|entry| entry.provider_session_id.clone())
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();
    if candidates.is_empty() {
        return Ok(PreparedLaunchRecovery::NoEffectObserved);
    }
    if candidates.len() > MAX_LAUNCH_RECOVERY_CANDIDATES {
        return Ok(PreparedLaunchRecovery::Ambiguous(candidates));
    }
    let Some(delivery_nonce) = state.delivery_nonce.as_deref() else {
        return Ok(PreparedLaunchRecovery::Ambiguous(candidates));
    };
    let mut matched = Vec::new();
    for session_id in &candidates {
        if recovered_session_matches_request(
            host,
            started,
            state,
            &env,
            session_id,
            delivery_nonce,
        )? {
            matched.push(session_id.clone());
        }
    }
    matched.sort();
    matched.dedup();
    match matched.as_slice() {
        [session_id] => Ok(PreparedLaunchRecovery::SessionObserved(session_id.clone())),
        [] => Ok(PreparedLaunchRecovery::Ambiguous(candidates)),
        _ => Ok(PreparedLaunchRecovery::Ambiguous(matched)),
    }
}

fn launch_recovery_environment(
    state: &LaunchRequestState,
    declared_env: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut env = state.recovery.passthrough_env.clone();
    env.extend(declared_env.clone());
    env
}

fn launch_recovery_remaining(
    host: &HostContext,
    started: Instant,
    request_id: &str,
) -> Result<Duration, ProviderFailure> {
    let remaining = LAUNCH_RECOVERY_TIMEOUT.saturating_sub(started.elapsed());
    operation_bounds::remaining_timeout(host.deadline_unix_ms, remaining)
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| launch_recovery_unavailable(request_id, "recovery deadline was reached"))
}

fn launch_recovery_session_is_plausible(
    entry: &opencode::OpencodeSessionListRow,
    state: &LaunchRequestState,
) -> bool {
    let directory_matches = match &entry.directory {
        opencode::OpencodeSessionDirectory::Absolute(directory) => {
            directory == &state.recovery.working_directory
        }
        opencode::OpencodeSessionDirectory::Missing
        | opencode::OpencodeSessionDirectory::Invalid(_) => true,
    };
    let timestamp = entry.updated_unix_ms.or(entry.created_unix_ms);
    directory_matches
        && timestamp
            .is_none_or(|timestamp| timestamp >= state.prepared_at_unix_ms.saturating_sub(5_000))
}

fn recovered_session_matches_request(
    host: &HostContext,
    started: Instant,
    state: &LaunchRequestState,
    env: &BTreeMap<String, String>,
    session_id: &str,
    delivery_nonce: &str,
) -> Result<bool, ProviderFailure> {
    let export = opencode::export_with_launch_context(
        session_id,
        &state.recovery.program,
        &state.recovery.fixed_args,
        &state.recovery.working_directory,
        env,
        launch_recovery_remaining(host, started, &state.request_id)?,
    )
    .map_err(|error| launch_recovery_unavailable(&state.request_id, format!("{error:?}")))?;
    if export.info.id != session_id {
        return Ok(false);
    }
    Ok(recovered_session_export_matches_request(
        &export,
        state,
        session_id,
        delivery_nonce,
    ))
}

fn recovered_session_export_matches_request(
    export: &opencode::OpencodeExport,
    state: &LaunchRequestState,
    session_id: &str,
    delivery_nonce: &str,
) -> bool {
    export.messages.iter().any(|message| {
        let model_identity = message.info.model_identity();
        message.info.role == "user"
            && message.info.session_id.as_deref() == Some(session_id)
            && model_identity.provider_id() == Some(state.recovery.provider_id.as_str())
            && model_identity.model_id() == Some(state.recovery.model_id.as_str())
            && model_identity.variant() == Some(state.recovery.effort.as_str())
            && message
                .info
                .time
                .as_ref()
                .and_then(|time| time.created)
                .is_some_and(|created| created >= state.prepared_at_unix_ms.saturating_sub(5_000))
            && resume_observation::message_has_delivery_nonce(message, delivery_nonce)
    })
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

fn acquire_launch_request_lock(
    host: &HostContext,
    root: &Path,
    state_path: &Path,
    admit_new: bool,
    reservation_binding_sha256: &str,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let capacity_lock = acquire_launch_capacity_lock(root, host, request_id)?;
    let lock_path = state_path.with_extension("lock");
    let custody = launch_request_custody(root);
    let lock_exists = lock_path.exists();
    let state_exists = state_path.exists();
    let replay_owner_exists = custody
        .replay_owner_exists(state_path)
        .map_err(|error| launch_custody_failure(request_id, error))?;
    let active = maintain_launch_request_capacity(&custody, &lock_path, request_id)?;
    let mut active_reservation = custody
        .active_reservation(state_path, reservation_binding_sha256)
        .map_err(|error| launch_custody_failure(request_id, error))?;
    if active_reservation == ActiveReservation::Unbound
        && admit_new
        && !lock_exists
        && !state_exists
        && !replay_owner_exists
    {
        custody
            .bind_unbound_active(state_path, reservation_binding_sha256)
            .map_err(|error| launch_custody_failure(request_id, error))?;
        active_reservation = ActiveReservation::Matching;
    }
    if active_reservation == ActiveReservation::Conflicting {
        return Err(launch_state_invalid(
            request_id,
            "the active request reservation belongs to different launch inputs",
        ));
    }
    let active_marker_exists = active_reservation != ActiveReservation::Absent;
    let resumes_pre_state_reservation = admit_new
        && active_reservation == ActiveReservation::Matching
        && !state_exists
        && !replay_owner_exists;
    let observed_existing =
        lock_exists || state_exists || replay_owner_exists || active_marker_exists;
    let reserved = admit_new && !observed_existing;
    if reserved && active >= MAX_ACTIVE_LAUNCH_REQUEST_RECORDS {
        return Err(launch_state_capacity_exceeded(request_id));
    }
    if reserved {
        custody
            .reserve_active(state_path, reservation_binding_sha256)
            .map_err(|error| launch_custody_failure(request_id, error))?;
    }
    let replay_pin = if observed_existing {
        Some(
            custody
                .pin_existing(state_path)
                .map_err(|error| launch_custody_failure(request_id, error))?,
        )
    } else {
        None
    };
    let lock = match open_launch_state_lock(&lock_path) {
        Ok(lock) => lock,
        Err(error) => {
            if reserved {
                custody
                    .remove_active_marker(state_path)
                    .map_err(|cleanup| launch_custody_failure(request_id, cleanup))?;
            }
            return Err(launch_state_failure(request_id, error));
        }
    };
    drop(capacity_lock);
    let acquired =
        match operation_bounds::remaining_timeout(host.deadline_unix_ms, LAUNCH_STATE_LOCK_TIMEOUT)
        {
            Some(timeout) => operation_bounds::lock_exclusive_for(&lock, timeout)
                .map_err(|error| launch_state_failure(request_id, error))?,
            None => match fs2::FileExt::try_lock_exclusive(&lock) {
                Ok(()) => true,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(error) => return Err(launch_state_failure(request_id, error)),
            },
        };
    if !acquired {
        drop(lock);
        if reserved {
            retire_orphan_launch_request_lock(state_path, request_id)?;
        }
        return Err(launch_state_lock_timeout(request_id));
    }
    drop(replay_pin);
    custody
        .release_pin_after_lock(state_path)
        .map_err(|error| launch_custody_failure(request_id, error))?;
    if observed_existing && !resumes_pre_state_reservation && !state_path.exists() {
        return Err(launch_state_invalid(
            request_id,
            "an observed request replay is still retiring its durable state",
        ));
    }
    Ok(lock)
}

fn acquire_launch_capacity_lock(
    root: &Path,
    host: &HostContext,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    durable_fs::create_private_directories(root)
        .map_err(|error| launch_state_failure(request_id, error))?;
    let path = root.join(".capacity.lock");
    let lock =
        open_launch_state_lock(&path).map_err(|error| launch_state_failure(request_id, error))?;
    let timeout =
        operation_bounds::remaining_timeout(host.deadline_unix_ms, LAUNCH_STATE_LOCK_TIMEOUT)
            .unwrap_or(Duration::ZERO);
    if !operation_bounds::lock_exclusive_for(&lock, timeout)
        .map_err(|error| launch_state_failure(request_id, error))?
    {
        return Err(launch_state_lock_timeout(request_id));
    }
    Ok(lock)
}

fn maintain_launch_request_capacity(
    custody: &RequestCustody,
    current_lock_path: &Path,
    request_id: &str,
) -> Result<usize, ProviderFailure> {
    custody
        .maintain(current_lock_path, launch_request_bytes_are_replay)
        .map_err(|error| launch_custody_failure(request_id, error))
}

fn launch_request_bytes_are_replay(bytes: &[u8]) -> Result<bool, String> {
    let state: Value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    Ok(matches!(
        state.get("phase").and_then(Value::as_str),
        Some(
            "session_observed"
                | "terminal_without_session"
                | "completion_observed"
                | "terminal_without_submission"
        )
    ))
}

fn launch_request_custody(root: &Path) -> RequestCustody {
    RequestCustody::new(
        root.to_path_buf(),
        root.to_path_buf(),
        root.join(".custody-v2"),
        MAX_LAUNCH_STATE_BYTES,
        MAX_ACTIVE_LAUNCH_REQUEST_RECORDS,
        MAX_LAUNCH_REPLAY_RECORDS,
        LAUNCH_ORPHAN_RETENTION,
    )
}

fn launch_custody_failure(request_id: &str, error: CustodyError) -> ProviderFailure {
    match error {
        CustodyError::Capacity => launch_state_capacity_exceeded(request_id),
        CustodyError::Invalid(error) => launch_state_invalid(request_id, error),
        CustodyError::Io(error) => launch_state_failure(request_id, error),
    }
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
    if bytes.len() > MAX_LAUNCH_STATE_BYTES {
        return Err(launch_state_capacity_exceeded(request_id));
    }
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
    let actor_published = state.actor_process_group_id.is_some_and(|id| id > 0)
        && (state.schema_version < 7
            || state
                .actor_process_group_incarnation
                .as_deref()
                .is_some_and(|incarnation| !incarnation.trim().is_empty()));
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
                && actor_published
                && state.terminal_status.is_none()
                && state.observed_at_unix_ms.is_some()
        }
        LaunchRequestPhase::TerminalWithoutSession => {
            state.provider_session_id.is_none()
                && actor_published
                && state.terminal_status.as_ref().is_some_and(Value::is_object)
                && state.observed_at_unix_ms.is_some()
        }
    };
    let schema_valid = match state.schema_version {
        5 => state.actor_process_group_incarnation.is_none(),
        6 => {
            state
                .delivery_nonce
                .as_deref()
                .is_some_and(|nonce| !nonce.trim().is_empty())
                && state.actor_process_group_incarnation.is_none()
        }
        7 | 8 | 9 | LAUNCH_STATE_SCHEMA_VERSION => {
            state
                .delivery_nonce
                .as_deref()
                .is_some_and(|nonce| !nonce.trim().is_empty())
                && match (
                    state.actor_process_group_id,
                    state.actor_process_group_incarnation.as_deref(),
                ) {
                    (None, None) => true,
                    (Some(id), Some(incarnation)) => id > 0 && !incarnation.trim().is_empty(),
                    _ => false,
                }
        }
        _ => false,
    };
    if schema_valid
        && state.operation_kind == LaunchOperationKind::NewSession
        && state.request_id == request_id
        && !state.request_identity_sha256.trim().is_empty()
        && !state.binding_sha256.trim().is_empty()
        && !state.recovery.program.trim().is_empty()
        && !state.recovery.declared_env_sha256.trim().is_empty()
        && !state.recovery.working_directory.trim().is_empty()
        && !state.recovery.provider_id.trim().is_empty()
        && !state.recovery.model_id.trim().is_empty()
        && !state.recovery.effort.trim().is_empty()
        && (state.schema_version < LAUNCH_STATE_SCHEMA_VERSION
            || launch_recovery_identity_is_current(&state.recovery))
        && state.actor_process_group_id.is_none_or(|id| id > 0)
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
    let actor_published = state.actor_process_group_id.is_some_and(|id| id > 0)
        && (state.schema_version < 7
            || state
                .actor_process_group_incarnation
                .as_deref()
                .is_some_and(|incarnation| !incarnation.trim().is_empty()));
    let phase_valid = match state.phase {
        ResumeLaunchRequestPhase::Prepared => {
            state.terminal_status.is_none() && state.observed_at_unix_ms.is_none()
        }
        ResumeLaunchRequestPhase::SubmissionObserved
        | ResumeLaunchRequestPhase::CompletionObserved
        | ResumeLaunchRequestPhase::Unresolved
        | ResumeLaunchRequestPhase::TerminalWithoutSubmission => {
            actor_published
                && state.observed_at_unix_ms.is_some()
                && state.terminal_status.as_ref().is_none_or(Value::is_object)
        }
    };
    let observation = &state.observation;
    let schema_valid = match state.schema_version {
        5 => state.actor_process_group_incarnation.is_none(),
        6 => {
            observation
                .delivery_nonce
                .as_deref()
                .is_some_and(|nonce| !nonce.trim().is_empty())
                && state.actor_process_group_incarnation.is_none()
        }
        7 | 8 | 9 | LAUNCH_STATE_SCHEMA_VERSION => {
            observation
                .delivery_nonce
                .as_deref()
                .is_some_and(|nonce| !nonce.trim().is_empty())
                && match (
                    state.actor_process_group_id,
                    state.actor_process_group_incarnation.as_deref(),
                ) {
                    (None, None) => true,
                    (Some(id), Some(incarnation)) => id > 0 && !incarnation.trim().is_empty(),
                    _ => false,
                }
        }
        _ => false,
    };
    if schema_valid
        && state.operation_kind == LaunchOperationKind::Resume
        && state.request_id == request_id
        && !state.request_identity_sha256.trim().is_empty()
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
        && (state.schema_version < LAUNCH_STATE_SCHEMA_VERSION
            || launch_recovery_identity_is_current(&state.recovery))
        && state.actor_process_group_id.is_none_or(|id| id > 0)
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

fn launch_state_capacity_exceeded(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "launch_state_capacity_exceeded",
        format!(
            "durable launch custody has reached its supported {MAX_ACTIVE_LAUNCH_REQUEST_RECORDS}-request active bound or {MAX_LAUNCH_REPLAY_RECORDS}-request completed replay bound; reconcile unresolved requests or allow the bounded recent-replay pool to retire its oldest completion"
        ),
    )
}

fn launch_state_lock_timeout(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "launch_state_lock_timeout",
        "durable launch state lock could not be acquired before the operation deadline",
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
    attempted_request_identity_sha256: &str,
    state: &LaunchRequestState,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_request_conflict",
        "launch request_id already names a different durable new-session operation",
        json!({
            "attempted_request_identity_sha256": attempted_request_identity_sha256,
            "committed_request_identity_sha256": state.request_identity_sha256,
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

fn launch_actor_reconciliation_required(
    request_id: &str,
    binding_sha256: &str,
    process_group_id: Option<u32>,
    reason: &str,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_actor_reconciliation_required",
        "prepared launch still has an unresolved native actor and will not spawn independent work",
        json!({
            "binding_sha256": binding_sha256,
            "process_group_id": process_group_id,
            "reason": reason,
            "required_action": "confirm the prior native actor is terminal and reconcile any provider effect before submitting a distinct request",
        }),
    )
}

fn resume_launch_request_reuse_conflict(
    request_id: &str,
    attempted_request_identity_sha256: &str,
    state: &ResumeLaunchRequestState,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "launch_request_conflict",
        "launch request_id already names a different durable resumed-session operation",
        json!({
            "attempted_request_identity_sha256": attempted_request_identity_sha256,
            "committed_request_identity_sha256": state.request_identity_sha256,
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
    integrity_failures: Vec<String>,
    integrity_failures_omitted: u64,
    parser: EventParser,
    last_opencode_event: Option<OpencodeEventMetadata>,
    completed_resume_at: Option<Instant>,
    stream_resume_observation: Option<ResumeObservation>,
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
        Self {
            request_id: request_id.to_string(),
            seq: 1,
            stdout_done: false,
            stderr_done: false,
            final_status: None,
            child_exit_at: None,
            forced_exit_status: None,
            integrity_failures: Vec::new(),
            integrity_failures_omitted: 0,
            parser: EventParser::default(),
            last_opencode_event: None,
            completed_resume_at: None,
            stream_resume_observation: None,
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
                self.record_integrity_failure(format!(
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
        self.project_stderr_bytes(bytes, writer)
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
        if let Some(request) = self.resume_observation_request.as_ref() {
            for event in events {
                if let Some(observation) = resume_observation::observe_stream_event(request, event)
                {
                    let completion_observed = observation.completion_observed();
                    if completion_observed
                        || !self
                            .stream_resume_observation
                            .as_ref()
                            .is_some_and(ResumeObservation::completion_observed)
                    {
                        self.stream_resume_observation = Some(observation);
                    }
                    if completion_observed {
                        self.completed_resume_at.get_or_insert_with(Instant::now);
                    }
                }
            }
        }
        if let Some(event) = events.last() {
            self.last_opencode_event = Some(event.clone());
        }
    }

    fn record_parser_errors(&mut self) {
        let failures = self.parser.take_failure_summary();
        for error in failures.representative_details {
            self.record_integrity_failure(format!("native event parse failed: {error}"));
        }
        self.integrity_failures_omitted = self
            .integrity_failures_omitted
            .saturating_add(failures.omitted_count);
    }

    fn record_integrity_failure(&mut self, failure: String) {
        if self.integrity_failures.len() >= MAX_LAUNCH_INTEGRITY_FAILURES {
            self.integrity_failures_omitted = self.integrity_failures_omitted.saturating_add(1);
            return;
        }
        self.integrity_failures
            .push(bounded_integrity_failure_detail(failure));
    }

    fn integrity_evidence_value(&self) -> Option<Value> {
        if self.integrity_failures.is_empty() && self.integrity_failures_omitted == 0 {
            return None;
        }
        let evidence = json!({
            "failures": self.integrity_failures,
            "retained_failure_count": self.integrity_failures.len(),
            "omitted_failure_count": self.integrity_failures_omitted,
        });
        if evidence.to_string().len() <= MAX_LAUNCH_INTEGRITY_EVIDENCE_BYTES {
            return Some(evidence);
        }
        Some(json!({
            "failures": ["launch integrity evidence exceeded its encoded bound"],
            "retained_failure_count": 0,
            "omitted_failure_count": self
                .integrity_failures_omitted
                .saturating_add(self.integrity_failures.len() as u64),
        }))
    }

    fn completed_resume_grace_elapsed(&self) -> bool {
        self.completed_resume_at
            .is_some_and(|completed_at| completed_at.elapsed() >= COMPLETED_RESUME_GRACE)
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
        let final_resume_observation = self.stream_resume_observation.clone().or_else(|| {
            self.resume_observation_request
                .as_ref()
                .map(|_| resume_observation::unconfirmed_observation())
        });
        let completion_observed = final_resume_observation
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
            && (!self.integrity_failures.is_empty()
                || self.integrity_failures_omitted > 0
                || self.unresolved_resume_completion.is_some())
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
        classify(status, now_unix_ms())
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
                self.record_integrity_failure(
                    "pre-session event projection buffer exhausted".to_string(),
                );
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
        if let Some(evidence) = self.integrity_evidence_value() {
            self.marker_with_value(
                "oulipoly.launch_evidence_loss".to_string(),
                evidence,
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

fn bounded_integrity_failure_detail(mut failure: String) -> String {
    if failure.len() <= MAX_LAUNCH_INTEGRITY_FAILURE_DETAIL_BYTES {
        return failure;
    }
    let mut end = MAX_LAUNCH_INTEGRITY_FAILURE_DETAIL_BYTES.saturating_sub(3);
    while !failure.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    failure.truncate(end);
    failure.push_str("...");
    failure
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

#[cfg(test)]
mod streaming_tests {
    use super::*;

    #[test]
    fn launch_integrity_evidence_has_lifetime_count_detail_and_encoding_bounds() {
        let mut state = LaunchState::new("request-integrity", None, None, json!({}), None);
        for index in 0..MAX_LAUNCH_INTEGRITY_FAILURES + 17 {
            state.record_integrity_failure(format!("{index}:{}", "é".repeat(1024)));
        }

        assert_eq!(
            state.integrity_failures.len(),
            MAX_LAUNCH_INTEGRITY_FAILURES
        );
        assert_eq!(state.integrity_failures_omitted, 17);
        assert!(state
            .integrity_failures
            .iter()
            .all(|failure| failure.len() <= MAX_LAUNCH_INTEGRITY_FAILURE_DETAIL_BYTES));
        let evidence = state
            .integrity_evidence_value()
            .expect("bounded integrity evidence");
        assert_eq!(
            evidence["retained_failure_count"],
            MAX_LAUNCH_INTEGRITY_FAILURES
        );
        assert_eq!(evidence["omitted_failure_count"], 17);
        assert!(
            serde_json::to_vec(&evidence)
                .expect("encode integrity evidence")
                .len()
                <= MAX_LAUNCH_INTEGRITY_EVIDENCE_BYTES
        );
    }

    #[test]
    fn launch_integrity_evidence_preserves_parser_failure_multiplicity() {
        let mut state = LaunchState::new("request-parser-integrity", None, None, json!({}), None);

        assert!(state
            .session_from_stdout(&b"not-json\n".repeat(9))
            .is_none());

        assert_eq!(state.integrity_failures.len(), 4);
        assert_eq!(state.integrity_failures_omitted, 5);
        let evidence = state
            .integrity_evidence_value()
            .expect("parser integrity evidence");
        assert_eq!(evidence["retained_failure_count"], 4);
        assert_eq!(evidence["omitted_failure_count"], 5);
    }
}

#[cfg(test)]
mod custody_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn exact_retry_resumes_pre_lock_reservation_at_active_capacity() {
        let directory = tempfile::tempdir().expect("launch custody directory");
        let custody = launch_request_custody(directory.path());
        assert_eq!(
            custody
                .maintain(
                    &directory.path().join("initialize.lock"),
                    launch_request_bytes_are_replay,
                )
                .expect("initialize launch custody"),
            0
        );
        let target_binding = format!("{:064x}", 1);
        let target_state = directory.path().join(format!("{:064x}.json", 1));
        for index in 1..=MAX_ACTIVE_LAUNCH_REQUEST_RECORDS {
            let binding = format!("{index:064x}");
            custody
                .reserve_active(
                    &directory.path().join(format!("{index:064x}.json")),
                    &binding,
                )
                .expect("fill launch active reservations");
        }
        let overflow_binding = format!("{:064x}", MAX_ACTIVE_LAUNCH_REQUEST_RECORDS + 1);
        assert!(matches!(
            custody.reserve_active(
                &directory.path().join(format!(
                    "{:064x}.json",
                    MAX_ACTIVE_LAUNCH_REQUEST_RECORDS + 1
                )),
                &overflow_binding,
            ),
            Err(CustodyError::Capacity)
        ));
        let host = HostContext {
            app: "test".to_string(),
            app_version: None,
            platform: None,
            working_directory: None,
            config_root: None,
            data_root: None,
            env: None,
            deadline_unix_ms: None,
        };

        let lock = acquire_launch_request_lock(
            &host,
            directory.path(),
            &target_state,
            true,
            &target_binding,
            "request-exact-retry",
        )
        .expect("exact retry resumes its pre-lock reservation at capacity");
        assert!(!target_state.exists());
        drop(lock);
        retire_orphan_launch_request_lock(&target_state, "request-exact-retry")
            .expect("retire resumed pre-state launch reservation");
    }

    #[test]
    fn completed_history_does_not_consume_active_launch_capacity() {
        let directory = tempfile::tempdir().expect("launch custody directory");
        let completed_records = 2;
        for index in 0..completed_records {
            let stem = format!("{index:064x}");
            fs::write(directory.path().join(format!("{stem}.lock")), b"")
                .expect("launch request lock");
            fs::write(
                directory.path().join(format!("{stem}.json")),
                br#"{"phase":"session_observed"}"#,
            )
            .expect("completed launch record");
        }

        let custody = RequestCustody::new(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            directory.path().join(".custody-v2"),
            MAX_LAUNCH_STATE_BYTES,
            1,
            completed_records,
            LAUNCH_ORPHAN_RETENTION,
        );
        let active = maintain_launch_request_capacity(
            &custody,
            &directory.path().join("new.lock"),
            "request-test",
        )
        .expect("classify bounded launch custody");

        assert_eq!(active, 0);
        let replay = fs::read_dir(directory.path().join(".custody-v2/replay"))
            .expect("replay ring")
            .count();
        assert_eq!(replay, completed_records);

        let temporary_residue = directory.path().join(".custody-v2/.write-tmp/residue");
        fs::write(&temporary_residue, b"partial").expect("simulate interrupted publication");
        let replay_parses = AtomicUsize::new(0);
        let active = custody
            .maintain(&directory.path().join("new.lock"), |bytes| {
                replay_parses.fetch_add(1, Ordering::Relaxed);
                launch_request_bytes_are_replay(bytes)
            })
            .expect("maintain indexed launch custody");
        assert_eq!(active, 0);
        assert_eq!(
            replay_parses.load(Ordering::Relaxed),
            0,
            "steady-state admission must not parse replay payloads"
        );
        assert!(!temporary_residue.exists());
    }

    #[test]
    fn interrupted_replay_handoff_is_idempotent_for_launch_requests() {
        let directory = tempfile::tempdir().expect("launch custody directory");
        let custody = RequestCustody::new(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            directory.path().join(".custody-v2"),
            MAX_LAUNCH_STATE_BYTES,
            2,
            2,
            LAUNCH_ORPHAN_RETENTION,
        );
        let first = format!("{:064x}", 1);
        let first_state = directory.path().join(format!("{first}.json"));
        let first_lock = directory.path().join(format!("{first}.lock"));
        fs::write(&first_lock, b"").expect("first launch request lock");
        fs::write(&first_state, br#"{"phase":"prepared"}"#).expect("first active launch state");
        assert_eq!(
            custody
                .maintain(
                    &directory.path().join("current.lock"),
                    launch_request_bytes_are_replay
                )
                .expect("initialize launch custody"),
            1
        );

        fs::write(&first_state, br#"{"phase":"session_observed"}"#)
            .expect("complete first launch state");
        assert!(custody
            .publish_replay_without_retiring_active(
                &first_state,
                &directory.path().join("current.lock")
            )
            .expect("publish replay before simulated interruption"));
        assert_eq!(
            custody
                .maintain(
                    &directory.path().join("current.lock"),
                    launch_request_bytes_are_replay
                )
                .expect("resume interrupted launch handoff"),
            0
        );
        assert_eq!(
            replay_references(directory.path(), ".custody-v2", &first),
            1
        );
        assert_eq!(
            fs::read_to_string(&first_state).expect("recover first launch terminal"),
            r#"{"phase":"session_observed"}"#
        );

        for index in 2..=3 {
            let stem = format!("{index:064x}");
            let state = directory.path().join(format!("{stem}.json"));
            fs::write(directory.path().join(format!("{stem}.lock")), b"")
                .expect("later launch request lock");
            fs::write(&state, br#"{"phase":"session_observed"}"#)
                .expect("later completed launch state");
            custody
                .reserve_active(&state, &stem)
                .expect("reserve later launch request");
            assert_eq!(
                custody
                    .maintain(
                        &directory.path().join("current.lock"),
                        launch_request_bytes_are_replay
                    )
                    .expect("place later launch replay"),
                0
            );
        }
        assert!(!first_state.exists(), "the oldest replay is evicted once");
        assert_eq!(
            replay_references(directory.path(), ".custody-v2", &first),
            0
        );
        assert_eq!(
            replay_references(directory.path(), ".custody-v2", &format!("{:064x}", 2)),
            1
        );
        assert_eq!(
            replay_references(directory.path(), ".custody-v2", &format!("{:064x}", 3)),
            1
        );
    }

    fn replay_references(root: &Path, index_name: &str, stem: &str) -> usize {
        fs::read_dir(root.join(index_name).join("replay"))
            .expect("replay ring")
            .map(|entry| entry.expect("replay entry").path())
            .filter_map(|path| fs::read(path).ok())
            .filter_map(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .filter(|record| record["request_sha256"].as_str() == Some(stem))
            .count()
    }

    #[test]
    fn steady_state_admission_classifies_one_active_payload() {
        let directory = tempfile::tempdir().expect("launch custody directory");
        let active_records = MAX_ACTIVE_LAUNCH_REQUEST_RECORDS;
        for index in 0..active_records {
            let stem = format!("{index:064x}");
            fs::write(directory.path().join(format!("{stem}.lock")), b"")
                .expect("launch request lock");
            fs::write(
                directory.path().join(format!("{stem}.json")),
                br#"{"phase":"prepared"}"#,
            )
            .expect("active launch record");
        }

        let custody = launch_request_custody(directory.path());
        maintain_launch_request_capacity(
            &custody,
            &directory.path().join("new.lock"),
            "request-test",
        )
        .expect("migrate bounded launch custody");

        let active_parses = AtomicUsize::new(0);
        let active = custody
            .maintain(&directory.path().join("new.lock"), |bytes| {
                active_parses.fetch_add(1, Ordering::Relaxed);
                launch_request_bytes_are_replay(bytes)
            })
            .expect("maintain compact active custody");
        assert_eq!(active, active_records);
        assert_eq!(
            active_parses.load(Ordering::Relaxed),
            1,
            "steady-state admission must classify at most one active payload"
        );
    }

    #[test]
    fn exact_replay_pin_prevents_handoff_eviction() {
        let directory = tempfile::tempdir().expect("launch custody directory");
        let first = format!("{:064x}", 1);
        let first_state = directory.path().join(format!("{first}.json"));
        fs::write(directory.path().join(format!("{first}.lock")), b"").expect("first replay lock");
        fs::write(&first_state, br#"{"phase":"session_observed"}"#).expect("first replay state");
        let custody = RequestCustody::new(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            directory.path().join(".pin-test-custody"),
            MAX_LAUNCH_STATE_BYTES,
            2,
            1,
            LAUNCH_ORPHAN_RETENTION,
        );
        assert_eq!(
            custody
                .maintain(
                    &directory.path().join("new.lock"),
                    launch_request_bytes_are_replay
                )
                .expect("initialize one-slot replay ring"),
            0
        );
        let pin = custody
            .pin_existing(&first_state)
            .expect("pin exact replay");

        let second = format!("{:064x}", 2);
        let second_state = directory.path().join(format!("{second}.json"));
        fs::write(directory.path().join(format!("{second}.lock")), b"")
            .expect("second replay lock");
        fs::write(&second_state, br#"{"phase":"session_observed"}"#).expect("second replay state");
        custody
            .reserve_active(&second_state, &second)
            .expect("reserve second completion");
        assert_eq!(
            custody
                .maintain(
                    &directory.path().join("new.lock"),
                    launch_request_bytes_are_replay
                )
                .expect("pinned replay remains in ring"),
            1
        );
        assert!(first_state.exists());

        drop(pin);
        custody
            .release_pin_after_lock(&first_state)
            .expect("last exact waiter retires its pin");
        assert!(!directory
            .path()
            .join(format!(".pin-test-custody/pins/{first}.pin"))
            .exists());
        assert_eq!(
            custody
                .maintain(
                    &directory.path().join("new.lock"),
                    launch_request_bytes_are_replay
                )
                .expect("retire unpinned replay"),
            0
        );
        assert!(!first_state.exists());
        assert!(second_state.exists());
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::native_process::configure_process_group;
    use std::process::Command;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn launch_actor_recovery_distinguishes_a_recycled_process_group() {
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        configure_process_group(&mut command);
        let mut actor = command.spawn().expect("spawn isolated launch actor");
        let process_group_id = actor.id();
        let incarnation =
            launch_process_incarnation(process_group_id).expect("read launch actor incarnation");

        let same_actor = require_prior_actor_terminal(
            Some(process_group_id),
            Some(&incarnation),
            "request-a",
            "binding-a",
        );
        let recycled_actor = require_prior_actor_terminal(
            Some(process_group_id),
            Some(&format!("{incarnation}:different")),
            "request-a",
            "binding-a",
        );

        actor.kill().expect("terminate isolated launch actor");
        actor.wait().expect("reap isolated launch actor");
        assert!(same_actor.is_err(), "the admitted incarnation remains live");
        assert!(
            recycled_actor.is_ok(),
            "a live group with a different leader incarnation is unrelated"
        );
    }

    #[test]
    fn new_session_recovery_does_not_claim_an_identical_sibling_prompt() {
        let export = opencode::parse_export_stdout(
            serde_json::to_vec(&json!({
                "info": {"id": "session-b", "title": "sibling session"},
                "messages": [{
                    "info": {
                        "id": "message-b",
                        "role": "user",
                        "sessionID": "session-b",
                        "model": {
                            "providerID": "openai",
                            "modelID": "gpt-5.6-luna",
                            "variant": "low"
                        },
                        "time": {"created": 20}
                    },
                    "parts": [{
                        "type": "text",
                        "text": "identical prompt\n\n[OULIPOLY-DELIVERY request-a]\n\n[OULIPOLY-DELIVERY request-b]"
                    }]
                }]
            }))
            .expect("serialize sibling session export")
            .as_slice(),
        )
        .expect("parse sibling session export");
        let state = LaunchRequestState {
            schema_version: 6,
            operation_kind: LaunchOperationKind::NewSession,
            request_id: "request-a".to_string(),
            request_identity_sha256: "identity-a".to_string(),
            binding_sha256: "binding-a".to_string(),
            prompt_sha256: Some(sha256_hex(b"identical prompt")),
            delivery_nonce: Some("request-a".to_string()),
            recovery: LaunchRecoveryIdentity {
                program: "opencode1".to_string(),
                program_sha256: String::new(),
                native_contract_id: String::new(),
                fixed_args: Vec::new(),
                implementation_manifest_id: String::new(),
                implementation_version: String::new(),
                program_stamp: native_runtime::NativeProgramStamp::default(),
                passthrough_env: BTreeMap::new(),
                declared_env_sha256: "environment".to_string(),
                working_directory: "/tmp".to_string(),
                provider_id: "openai".to_string(),
                model_id: "gpt-5.6-luna".to_string(),
                effort: "low".to_string(),
            },
            phase: LaunchRequestPhase::Prepared,
            actor_process_group_id: None,
            actor_process_group_incarnation: None,
            provider_session_id: None,
            terminal_status: None,
            prepared_at_unix_ms: 20,
            observed_at_unix_ms: None,
        };
        let invalid_directory_observation = opencode::OpencodeSessionListRow {
            provider_session_id: "session-b".to_string(),
            title: None,
            directory: opencode::OpencodeSessionDirectory::Invalid("relative/path".to_string()),
            created_unix_ms: Some(20),
            updated_unix_ms: None,
            turn_count: None,
        };

        assert!(
            launch_recovery_session_is_plausible(&invalid_directory_observation, &state),
            "invalid directory evidence must remain an unknown candidate rather than becoming authoritative absence"
        );

        assert!(!recovered_session_export_matches_request(
            &export,
            &state,
            "session-b",
            "request-a"
        ));
        assert!(recovered_session_export_matches_request(
            &export,
            &state,
            "session-b",
            "request-b"
        ));
    }
}
