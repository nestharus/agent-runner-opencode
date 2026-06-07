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

use crate::account::profile_for_settings_id;
use crate::encoding::{bounded_text, decode_base64, encode_base64, now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure, CONTRACT};
use crate::opencode::{
    self, first_session_id, EventParser, OpencodeEventMetadata, OpencodeExport, OpencodeMessage,
};
use crate::policy;
use crate::terminal::{classify, exit_code_for_status, process_status_json, ProcessStatus};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
const TERMINATION_GRACE: Duration = Duration::from_millis(100);
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
const SUBMITTED_USER_TURN_MARKER: &str = "oulipoly.submitted_user_turn";
const SUBMITTED_USER_TURN_SOURCE: &str = "opencode.export";
const DELIVERY_NONCE_PREFIX: &str = "[OULIPOLY-DELIVERY ";
const DELIVERY_NONCE_SUFFIX: char = ']';
const TERMINAL_SIGNAL_EVIDENCE_MAX_LEN: usize = 160;

#[derive(Deserialize)]
struct LaunchParams {
    settings_id: String,
    mode: String,
    model: Value,
    argv: Vec<String>,
    working_directory: String,
    env: Option<BTreeMap<String, String>>,
    stdin: Option<BytePayload>,
    session: Option<Value>,
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
}

pub fn stream<W: Write>(
    request_id: &str,
    host: &HostContext,
    params: Value,
    writer: &mut W,
) -> Result<i32, ProviderFailure> {
    let params = parse_launch_params(params, request_id)?;
    let effective = match launch_argv(&params, request_id)? {
        PolicyLaunch::Accepted(effective) => effective,
        PolicyLaunch::Rejected(reason) => {
            return stream_policy_rejection(request_id, writer, reason)
        }
    };
    let mut child = match spawn_child(
        &effective.argv,
        &params.working_directory,
        &effective.env,
        effective.stdin.as_ref(),
    ) {
        Ok(child) => child,
        Err(err) => return stream_spawn_error(request_id, writer, err),
    };
    stream_child(
        request_id,
        host,
        &mut child,
        effective.resume_confirmation,
        writer,
    )
}

fn parse_launch_params(params: Value, request_id: &str) -> Result<LaunchParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_launch_params_failure(request_id, err))
}

struct EffectiveLaunch {
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    stdin: Option<Vec<u8>>,
    _prompt: Option<String>,
    resume_confirmation: Option<ResumeConfirmation>,
}

#[derive(Clone)]
struct ResumeConfirmation {
    settings_id: String,
    session_id: String,
    prompt: String,
    delivery_nonce: Option<String>,
}

enum PolicyLaunch {
    Accepted(EffectiveLaunch),
    Rejected(String),
}

fn launch_argv(params: &LaunchParams, request_id: &str) -> Result<PolicyLaunch, ProviderFailure> {
    let policy_params = policy_params_for_launch(params, request_id)?;
    let result = policy::evaluate(policy_params, request_id)?;
    if !policy_result_accepted(&result) {
        return Ok(PolicyLaunch::Rejected(policy_rejection_reason(&result)));
    }
    Ok(PolicyLaunch::Accepted(effective_launch(
        params, result, request_id,
    )?))
}

fn effective_launch(
    params: &LaunchParams,
    result: Value,
    request_id: &str,
) -> Result<EffectiveLaunch, ProviderFailure> {
    let argv = validated_policy_argv(&result, request_id)?;
    project_effective_launch(params, &result, argv, request_id)
}

fn validated_policy_argv(result: &Value, request_id: &str) -> Result<Vec<String>, ProviderFailure> {
    let argv = policy_argv(result);
    validate_policy_argv(&argv, request_id)?;
    Ok(argv)
}

fn project_effective_launch(
    params: &LaunchParams,
    result: &Value,
    argv: Vec<String>,
    request_id: &str,
) -> Result<EffectiveLaunch, ProviderFailure> {
    let stdin = policy_stdin(result);
    let prompt = policy_prompt(result);
    let argv = resume_argv(
        params,
        argv,
        stdin.as_deref(),
        prompt.as_deref(),
        request_id,
    )?;
    let resume_confirmation =
        resume_confirmation(params, stdin.as_deref(), prompt.as_deref(), &argv);
    Ok(EffectiveLaunch {
        argv,
        env: effective_env_from_policy(result, request_id)?,
        stdin,
        _prompt: prompt,
        resume_confirmation,
    })
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

fn resume_confirmation(
    params: &LaunchParams,
    stdin: Option<&[u8]>,
    prompt: Option<&str>,
    argv: &[String],
) -> Option<ResumeConfirmation> {
    let session_id = known_provider_session_id(params)?;
    let prompt = submitted_resume_payload(argv, stdin, prompt)?;
    let delivery_nonce = delivery_nonce_from_prompt(&prompt);
    Some(ResumeConfirmation {
        settings_id: params.settings_id.clone(),
        session_id: session_id.to_string(),
        prompt,
        delivery_nonce,
    })
}

fn known_provider_session_id(params: &LaunchParams) -> Option<&str> {
    nonblank_optional_text(raw_known_provider_session_id(params))
}

fn raw_known_provider_session_id(params: &LaunchParams) -> Option<&str> {
    params
        .session
        .as_ref()
        .and_then(|session| session.get("known_provider_session_id"))
        .and_then(Value::as_str)
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
    stdin_payload_text(stdin)
        .or_else(|| prompt_arg_payload(argv, prompt))
        .or_else(|| argv_payload_after_resume_session_insert_index(argv).map(str::to_string))
}

fn stdin_payload_text(stdin: Option<&[u8]>) -> Option<String> {
    let bytes = stdin_payload_bytes(stdin)?;
    payload_string(bytes)
}

fn stdin_payload_bytes(stdin: Option<&[u8]>) -> Option<&[u8]> {
    nonempty_payload_bytes(stdin?)
}

fn nonempty_payload_bytes(bytes: &[u8]) -> Option<&[u8]> {
    (!bytes_are_empty_payload(bytes)).then_some(bytes)
}

fn bytes_are_empty_payload(bytes: &[u8]) -> bool {
    payload_text_or_bytes_are_empty(bytes, payload_utf8_text(bytes))
}

fn payload_text_or_bytes_are_empty(bytes: &[u8], text: Option<&str>) -> bool {
    text.map_or_else(|| bytes.is_empty(), text_is_blank)
}

fn payload_string(bytes: &[u8]) -> Option<String> {
    payload_utf8_text(bytes).map(owned_text)
}

fn payload_utf8_text(bytes: &[u8]) -> Option<&str> {
    std::str::from_utf8(bytes).ok()
}

fn owned_text(text: &str) -> String {
    text.to_string()
}

fn nonblank_optional_text(value: Option<&str>) -> Option<&str> {
    value.filter(|text| is_nonblank_text(text))
}

fn is_nonblank_text(text: &str) -> bool {
    !text_is_blank(text)
}

fn text_is_blank(text: &str) -> bool {
    text.trim().is_empty()
}

fn prompt_arg_payload(argv: &[String], prompt: Option<&str>) -> Option<String> {
    let prompt = nonempty_prompt(prompt)?;
    argv.iter()
        .any(|arg| arg == prompt)
        .then(|| prompt.to_string())
}

fn nonempty_prompt(prompt: Option<&str>) -> Option<&str> {
    nonblank_optional_text(prompt)
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

fn effective_env_from_policy(
    result: &Value,
    request_id: &str,
) -> Result<BTreeMap<String, String>, ProviderFailure> {
    let Some(env) = policy_env_object(result) else {
        return Ok(BTreeMap::new());
    };
    policy_env_entries(env, request_id)
}

fn policy_env_object(result: &Value) -> Option<&serde_json::Map<String, Value>> {
    result.get("env").and_then(Value::as_object)
}

fn policy_env_entries(
    env: &serde_json::Map<String, Value>,
    request_id: &str,
) -> Result<BTreeMap<String, String>, ProviderFailure> {
    env.iter()
        .map(|(key, value)| policy_env_entry(key, value, request_id))
        .collect()
}

fn policy_rejection_reason(result: &Value) -> String {
    let diagnostics = policy_diagnostics(result);
    format!("policy.evaluate rejected launch params; diagnostics={diagnostics}")
}

fn policy_params_for_launch(
    params: &LaunchParams,
    request_id: &str,
) -> Result<policy::PolicyEvaluateParams, ProviderFailure> {
    let value = launch_policy_value(params, request_id)?;
    parse_launch_policy_params(value, request_id)
}

fn parse_launch_policy_params(
    value: Value,
    request_id: &str,
) -> Result<policy::PolicyEvaluateParams, ProviderFailure> {
    serde_json::from_value(value)
        .map_err(|err| invalid_launch_policy_params_failure(request_id, err))
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
    stdin: Option<&Vec<u8>>,
) -> std::io::Result<Child> {
    let mut command = child_command(argv, working_directory, stdin.is_some());
    command.env_clear();
    let child_env = child_env(env);
    command.envs(child_env_pairs(&child_env));
    configure_process_group(&mut command);
    let mut child = command.spawn()?;
    write_child_stdin(&mut child, stdin)?;
    Ok(child)
}

fn child_command(argv: &[String], working_directory: &str, stdin_present: bool) -> Command {
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .current_dir(working_directory)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(child_stdin(stdin_present));
    command
}

fn child_stdin(stdin_present: bool) -> Stdio {
    if stdin_present {
        Stdio::piped()
    } else {
        Stdio::null()
    }
}

fn child_env_pairs(env: &BTreeMap<String, String>) -> Vec<(&str, &str)> {
    env.iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

fn child_env(declared: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut env = pass_through_env();
    env.extend(allowed_declared_env(declared));
    env
}

fn pass_through_env() -> BTreeMap<String, String> {
    pass_through_env_map(pass_through_env_entries())
}

fn pass_through_env_entries() -> Vec<(String, String)> {
    present_pass_through_env_entries(optional_pass_through_env_entries())
}

fn optional_pass_through_env_entries() -> Vec<Option<(String, String)>> {
    BASE_LAUNCH_ENV_PASSTHROUGH_KEYS
        .iter()
        .chain(HOST_LINKAGE_ENV_KEYS.iter())
        .map(|key| pass_through_env_entry(key))
        .collect()
}

fn present_pass_through_env_entries(
    entries: Vec<Option<(String, String)>>,
) -> Vec<(String, String)> {
    entries.into_iter().flatten().collect()
}

fn pass_through_env_map(entries: Vec<(String, String)>) -> BTreeMap<String, String> {
    entries.into_iter().collect()
}

fn pass_through_env_entry(key: &str) -> Option<(String, String)> {
    ambient_env_value(key).map(|value| env_pair(key, value))
}

fn ambient_env_value(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn env_pair(key: &str, value: String) -> (String, String) {
    (key.to_string(), value)
}

fn write_child_stdin(child: &mut Child, stdin: Option<&Vec<u8>>) -> std::io::Result<()> {
    if let (Some(input), Some(mut child_stdin)) = (stdin, child.stdin.take()) {
        child_stdin.write_all(input)?;
    }
    Ok(())
}

fn stream_child<W: Write>(
    request_id: &str,
    host: &HostContext,
    child: &mut Child,
    resume_confirmation: Option<ResumeConfirmation>,
    writer: &mut W,
) -> Result<i32, ProviderFailure> {
    let mut state = LaunchState::new(request_id, host.deadline_unix_ms, resume_confirmation);
    let receiver = start_drains(child);
    run_supervision_loop(child, &receiver, &mut state, writer)?;
    state.finish(writer)
}

fn start_drains(child: &mut Child) -> Receiver<DrainMessage> {
    let (sender, receiver) = mpsc::channel();
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
    sender: mpsc::Sender<DrainMessage>,
    stdout: bool,
) {
    std::thread::spawn(move || drain_reader(reader, sender, stdout));
}

fn drain_reader<R: Read>(mut reader: R, sender: mpsc::Sender<DrainMessage>, stdout: bool) {
    let mut buffer = drain_buffer();
    while let Ok(count) = read_drain_chunk(&mut reader, &mut buffer) {
        if drain_read_complete(count) {
            break;
        }
        let message = drain_chunk_message(stdout, drain_chunk(&buffer, count));
        if drain_send_failed(send_drain_message(&sender, message)) {
            return;
        }
    }
    send_drain_done(&sender, stdout);
}

fn drain_buffer() -> [u8; 8192] {
    [0_u8; 8192]
}

fn read_drain_chunk<R: Read>(reader: &mut R, buffer: &mut [u8]) -> std::io::Result<usize> {
    reader.read(buffer)
}

fn drain_read_complete(count: usize) -> bool {
    count == 0
}

fn drain_chunk(buffer: &[u8], count: usize) -> &[u8] {
    &buffer[..count]
}

fn drain_chunk_message(stdout: bool, chunk: &[u8]) -> DrainMessage {
    drain_bytes_message(stdout, drain_chunk_bytes(chunk))
}

fn drain_chunk_bytes(chunk: &[u8]) -> Vec<u8> {
    chunk.to_vec()
}

fn send_drain_message(
    sender: &mpsc::Sender<DrainMessage>,
    message: DrainMessage,
) -> Result<(), mpsc::SendError<DrainMessage>> {
    sender.send(message)
}

fn drain_send_failed(result: Result<(), mpsc::SendError<DrainMessage>>) -> bool {
    result.is_err()
}

fn send_drain_done(sender: &mpsc::Sender<DrainMessage>, stdout: bool) {
    let _ = send_drain_message(sender, drain_done_message(stdout));
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
        match receiver.recv_timeout(state.wait_duration()) {
            Ok(message) => state.handle_drain_message(message, writer)?,
            Err(mpsc::RecvTimeoutError::Timeout) => state.heartbeat(writer)?,
            Err(mpsc::RecvTimeoutError::Disconnected) => state.mark_drains_done(),
        }
    }
    Ok(())
}

fn enforce_deadline(child: &mut Child, state: &mut LaunchState) -> Result<(), ProviderFailure> {
    if state.final_status.is_some() || !state.deadline_reached() {
        return Ok(());
    }
    terminate_child(child);
    let _ = child.wait();
    state.final_status = Some(deadline_status());
    Ok(())
}

fn capture_child_exit(child: &mut Child, state: &mut LaunchState) -> Result<(), ProviderFailure> {
    if state.final_status.is_some() {
        return Ok(());
    }
    if let Some(status) = child
        .try_wait()
        .map_err(|err| spawn_failure("try_wait", err))?
    {
        state.final_status = Some(process_status_from_exit(status));
    }
    Ok(())
}

fn stream_spawn_error<W: Write>(
    request_id: &str,
    writer: &mut W,
    err: std::io::Error,
) -> Result<i32, ProviderFailure> {
    let mut state = LaunchState::new(request_id, None, None);
    state.final_status = Some(spawn_error_status(err));
    state.mark_drains_done();
    state.finish(writer)
}

fn stream_policy_rejection<W: Write>(
    request_id: &str,
    writer: &mut W,
    reason: String,
) -> Result<i32, ProviderFailure> {
    let mut state = LaunchState::new(request_id, None, None);
    state.final_status = Some(policy_rejection_status(reason));
    state.mark_drains_done();
    state.finish(writer)
}

struct LaunchState {
    request_id: String,
    seq: u64,
    stdout_done: bool,
    stderr_done: bool,
    final_status: Option<ProcessStatus>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    parser: EventParser,
    last_opencode_event: Option<OpencodeEventMetadata>,
    session_id: Option<String>,
    resume_confirmation: Option<ResumeConfirmation>,
    deadline_unix_ms: Option<u64>,
    next_heartbeat: Instant,
}

impl LaunchState {
    fn new(
        request_id: &str,
        deadline_unix_ms: Option<u64>,
        resume_confirmation: Option<ResumeConfirmation>,
    ) -> Self {
        Self {
            request_id: request_id.to_string(),
            seq: 1,
            stdout_done: false,
            stderr_done: false,
            final_status: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            parser: EventParser::default(),
            last_opencode_event: None,
            session_id: None,
            resume_confirmation,
            deadline_unix_ms,
            next_heartbeat: Instant::now() + HEARTBEAT_INTERVAL,
        }
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
        }
    }

    fn stdout_bytes<W: Write>(
        &mut self,
        bytes: &[u8],
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.record_stdout(bytes);
        self.project_stdout_bytes(bytes, writer)?;
        self.capture_session_from_stdout(bytes, writer)
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
        self.stdout.extend_from_slice(bytes);
    }

    fn record_stderr(&mut self, bytes: &[u8]) {
        self.stderr.extend_from_slice(bytes);
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
        if self.has_session_marker() {
            return Ok(());
        }
        if let Some(session_id) = session {
            self.record_session_id(&session_id);
            self.write_session_marker(&session_id, writer)?;
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
        self.marker(session_marker_name(session_id), writer)
    }

    fn capture_session_from_stdout<W: Write>(
        &mut self,
        bytes: &[u8],
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        let session = self.session_from_stdout(bytes);
        self.capture_session(session, writer)
    }

    fn session_from_stdout(&mut self, bytes: &[u8]) -> Option<String> {
        let events = self.parser.ingest(bytes);
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
        self.record_opencode_events(&events);
        first_session_id(&events)
    }

    fn record_opencode_events(&mut self, events: &[OpencodeEventMetadata]) {
        if let Some(event) = events.last() {
            self.last_opencode_event = Some(event.clone());
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
        self.confirm_submitted_user_turn(writer)?;
        let status = self.finished_status();
        let signal = self.terminal_signal_for(&status);
        let event = self.exit_event(&status, signal);
        self.write_event(event, writer)?;
        Ok(provider_exit_code(&status))
    }

    fn confirm_submitted_user_turn<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        let Some(marker_value) = self.submitted_user_turn_marker_value() else {
            return Ok(());
        };
        self.marker_with_value(SUBMITTED_USER_TURN_MARKER.to_string(), marker_value, writer)
    }

    fn submitted_user_turn_marker_value(&self) -> Option<Value> {
        let confirmation = self.resume_confirmation.as_ref()?;
        submitted_user_turn_marker_value(confirmation)
    }

    fn finished_status(&self) -> ProcessStatus {
        self.final_status.clone().unwrap_or(ProcessStatus::Unknown)
    }

    fn terminal_signal_for(&self, status: &ProcessStatus) -> Value {
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
        assign_event_seq(&mut event, self.seq);
        write_ndjson_event(writer, &event)?;
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

    fn mark_drains_done(&mut self) {
        self.stdout_done = true;
        self.stderr_done = true;
    }
}

fn submitted_user_turn_marker_value(confirmation: &ResumeConfirmation) -> Option<Value> {
    let native = export_for_resume_confirmation(confirmation)?;
    if !export_session_matches_confirmation(&native, confirmation) {
        return None;
    }
    let message = submitted_user_turn_message(&native.messages, confirmation)?;
    Some(submitted_user_turn_marker(confirmation, message))
}

fn export_for_resume_confirmation(confirmation: &ResumeConfirmation) -> Option<OpencodeExport> {
    let account = profile_for_settings_id(&confirmation.settings_id)?;
    opencode::export(&confirmation.session_id, account).ok()
}

fn export_session_matches_confirmation(
    native: &OpencodeExport,
    confirmation: &ResumeConfirmation,
) -> bool {
    native.info.id.as_str() == confirmation.session_id.as_str()
}

fn submitted_user_turn_message<'a>(
    messages: &'a [OpencodeMessage],
    confirmation: &ResumeConfirmation,
) -> Option<&'a OpencodeMessage> {
    messages
        .iter()
        .find(|message| submitted_user_message_matches(message, confirmation))
}

fn submitted_user_turn_marker(
    confirmation: &ResumeConfirmation,
    message: &OpencodeMessage,
) -> Value {
    let marker = submitted_user_turn_marker_base(confirmation, message);
    marker_with_delivery_nonce(marker, confirmation.delivery_nonce.as_deref())
}

fn submitted_user_turn_marker_base(
    confirmation: &ResumeConfirmation,
    message: &OpencodeMessage,
) -> Value {
    json!({
        "provider_session_id": confirmation.session_id.as_str(),
        "prompt_sha256": sha256_hex(confirmation.prompt.as_bytes()),
        "source": SUBMITTED_USER_TURN_SOURCE,
        "message_id": message.info.id.as_str(),
    })
}

fn marker_with_delivery_nonce(mut marker: Value, delivery_nonce: Option<&str>) -> Value {
    if let Some(delivery_nonce) = delivery_nonce {
        marker["delivery_nonce"] = json!(delivery_nonce);
    }
    marker
}

fn submitted_user_message_matches(
    message: &OpencodeMessage,
    confirmation: &ResumeConfirmation,
) -> bool {
    message.info.role.as_str() == "user"
        && message.info.session_id.as_deref() == Some(confirmation.session_id.as_str())
        && message_confirms_resume_payload(message, confirmation)
}

fn message_confirms_resume_payload(
    message: &OpencodeMessage,
    confirmation: &ResumeConfirmation,
) -> bool {
    if let Some(delivery_nonce) = confirmation.delivery_nonce.as_deref() {
        return message_contains_delivery_nonce(message, delivery_nonce);
    }
    message_has_exact_text_part(message, &confirmation.prompt)
}

fn message_has_exact_text_part(message: &OpencodeMessage, prompt: &str) -> bool {
    message.parts.iter().any(|part| {
        part.get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| text == prompt)
    })
}

fn message_contains_delivery_nonce(message: &OpencodeMessage, delivery_nonce: &str) -> bool {
    let marker = delivery_marker(delivery_nonce);
    message_string_fields_contain(message, &marker)
}

fn message_string_fields_contain(message: &OpencodeMessage, needle: &str) -> bool {
    let fields = message_string_fields(message);
    string_fields_contain_needle(&fields, needle)
        || text_contains_needle(&joined_string_fields(&fields), needle)
}

fn message_string_fields(message: &OpencodeMessage) -> Vec<&str> {
    message.parts.iter().flat_map(value_string_fields).collect()
}

fn value_string_fields(value: &Value) -> Vec<&str> {
    if let Some(text) = value_text(value) {
        return single_string_field(text);
    }
    if let Some(values) = value_array(value) {
        return array_string_fields(values);
    }
    if let Some(values) = value_object(value) {
        return object_string_fields(values);
    }
    empty_string_fields()
}

fn value_text(value: &Value) -> Option<&str> {
    value.as_str()
}

fn value_array(value: &Value) -> Option<&Vec<Value>> {
    value.as_array()
}

fn value_object(value: &Value) -> Option<&serde_json::Map<String, Value>> {
    value.as_object()
}

fn single_string_field(text: &str) -> Vec<&str> {
    vec![text]
}

fn empty_string_fields() -> Vec<&'static str> {
    Vec::new()
}

fn array_string_fields(values: &[Value]) -> Vec<&str> {
    values.iter().flat_map(value_string_fields).collect()
}

fn object_string_fields(values: &serde_json::Map<String, Value>) -> Vec<&str> {
    values.values().flat_map(value_string_fields).collect()
}

fn string_fields_contain_needle(fields: &[&str], needle: &str) -> bool {
    fields
        .iter()
        .any(|field| text_contains_needle(field, needle))
}

fn joined_string_fields(fields: &[&str]) -> String {
    fields.concat()
}

fn text_contains_needle(text: &str, needle: &str) -> bool {
    text.contains(needle)
}

fn delivery_nonce_from_prompt(prompt: &str) -> Option<String> {
    let start = prompt.find(DELIVERY_NONCE_PREFIX)? + DELIVERY_NONCE_PREFIX.len();
    let tail = &prompt[start..];
    let end = tail.find(DELIVERY_NONCE_SUFFIX)?;
    let nonce = tail[..end].trim();
    (!nonce.is_empty()).then(|| nonce.to_string())
}

fn delivery_marker(delivery_nonce: &str) -> String {
    format!("{DELIVERY_NONCE_PREFIX}{delivery_nonce}{DELIVERY_NONCE_SUFFIX}")
}

fn invalid_launch_params_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_launch_params",
        format!("launch params are invalid: {err}"),
    )
}

fn policy_result_accepted(result: &Value) -> bool {
    result.get("accepted").and_then(Value::as_bool) == Some(true)
}

fn policy_argv(result: &Value) -> Vec<String> {
    owned_strings(policy_argv_strings(result))
}

fn policy_argv_strings(result: &Value) -> Vec<&str> {
    let Some(values) = policy_argv_values(result) else {
        return Vec::new();
    };
    policy_argv_string_refs(policy_argv_string_values(values))
}

fn policy_argv_values(result: &Value) -> Option<&Vec<Value>> {
    result.get("argv").and_then(Value::as_array)
}

fn policy_argv_string_values(values: &[Value]) -> Vec<&Value> {
    values
        .iter()
        .filter(|value| policy_value_is_string(value))
        .collect()
}

fn policy_value_is_string(value: &Value) -> bool {
    value.as_str().is_some()
}

fn policy_argv_string_refs(values: Vec<&Value>) -> Vec<&str> {
    values.into_iter().map(policy_string_ref).collect()
}

fn policy_string_ref(value: &Value) -> &str {
    value.as_str().expect("filtered policy argv string")
}

fn owned_strings(values: Vec<&str>) -> Vec<String> {
    values.into_iter().map(ToOwned::to_owned).collect()
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

fn policy_stdin(result: &Value) -> Option<Vec<u8>> {
    policy_stdin_text(result).map(stdin_text_bytes)
}

fn policy_stdin_text(result: &Value) -> Option<&str> {
    result.get("stdin").and_then(Value::as_str)
}

fn stdin_text_bytes(stdin: &str) -> Vec<u8> {
    stdin.as_bytes().to_vec()
}

fn policy_prompt(result: &Value) -> Option<String> {
    result
        .get("prompt")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn policy_env_entry(
    key: &str,
    value: &Value,
    request_id: &str,
) -> Result<(String, String), ProviderFailure> {
    let value = policy_env_string(value, request_id)?;
    Ok(policy_env_pair(key, value))
}

fn policy_env_string<'a>(value: &'a Value, request_id: &str) -> Result<&'a str, ProviderFailure> {
    value
        .as_str()
        .ok_or_else(|| invalid_policy_env_failure(request_id))
}

fn policy_env_pair(key: &str, value: &str) -> (String, String) {
    (key.to_string(), value.to_string())
}

fn invalid_policy_env_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_policy_env",
        "policy.evaluate returned a non-string env value",
    )
}

fn policy_diagnostics(result: &Value) -> Value {
    result
        .get("diagnostics")
        .cloned()
        .unwrap_or_else(|| json!([]))
}

fn launch_policy_value(params: &LaunchParams, request_id: &str) -> Result<Value, ProviderFailure> {
    let stdin = policy_stdin_for_launch(params.stdin.as_ref(), request_id)?;
    Ok(launch_policy_json(params, stdin))
}

fn launch_policy_json(params: &LaunchParams, stdin: Option<String>) -> Value {
    json!({
        "settings_id": params.settings_id.clone(),
        "mode": params.mode.clone(),
        "model": params.model.clone(),
        "launch": {
            "argv": params.argv.clone(),
            "env": params.env.clone(),
            "stdin": stdin,
        }
    })
}

fn invalid_launch_policy_params_failure(
    request_id: &str,
    err: serde_json::Error,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_launch_policy_params",
        format!("launch params could not be evaluated by policy: {err}"),
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

fn allowed_declared_env(declared: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    owned_env_pairs(allowed_declared_env_entries(declared))
}

fn allowed_declared_env_entries(declared: &BTreeMap<String, String>) -> Vec<(&String, &String)> {
    declared
        .iter()
        .filter(|(key, _)| allowed_declared_env_key(key))
        .collect()
}

fn allowed_declared_env_key(key: &str) -> bool {
    !policy::is_forbidden_env_key(key)
}

fn owned_env_pairs(entries: Vec<(&String, &String)>) -> BTreeMap<String, String> {
    entries.into_iter().map(owned_env_pair).collect()
}

fn owned_env_pair(entry: (&String, &String)) -> (String, String) {
    (entry.0.clone(), entry.1.clone())
}

fn session_marker_name(session_id: &str) -> String {
    format!("opencode.sessionID.{session_id}")
}

fn assign_event_seq(event: &mut Value, seq: u64) {
    event["seq"] = json!(seq);
}

fn write_ndjson_event<W: Write>(writer: &mut W, event: &Value) -> Result<(), ProviderFailure> {
    write_json_event(writer, event)?;
    write_event_newline(writer)?;
    flush_event_writer(writer)
}

fn write_json_event<W: Write>(writer: &mut W, event: &Value) -> Result<(), ProviderFailure> {
    serde_json::to_writer(writer, event).map_err(json_write_failure)
}

fn write_event_newline<W: Write>(writer: &mut W) -> Result<(), ProviderFailure> {
    writer.write_all(b"\n").map_err(write_failure)
}

fn flush_event_writer<W: Write>(writer: &mut W) -> Result<(), ProviderFailure> {
    writer.flush().map_err(write_failure)
}

fn drain_bytes_message(stdout: bool, bytes: Vec<u8>) -> DrainMessage {
    if stdout {
        DrainMessage::Stdout(bytes)
    } else {
        DrainMessage::Stderr(bytes)
    }
}

fn drain_done_message(stdout: bool) -> DrainMessage {
    if stdout {
        DrainMessage::StdoutDone
    } else {
        DrainMessage::StderrDone
    }
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
    nonblank_optional_text(value).unwrap_or(fallback)
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

fn spawn_failure(context: &'static str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        "req-launch",
        "launch_supervision_error",
        format!("{context} failed while supervising opencode: {err}"),
    )
}

fn write_failure(err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        "req-launch",
        "launch_write_error",
        format!("failed to write launch event: {err}"),
    )
}

fn json_write_failure(err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::internal(
        "req-launch",
        "launch_write_error",
        format!("failed to write launch event: {err}"),
    )
}

#[cfg(unix)]
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
fn terminate_child(child: &mut Child) {
    let pgid = child_process_group_id(child);
    send_process_group_signal(pgid, SIGTERM);
    std::thread::sleep(TERMINATION_GRACE);
    if child_still_running(child) {
        send_process_group_signal(pgid, SIGKILL);
    }
}

#[cfg(unix)]
fn child_process_group_id(child: &Child) -> i32 {
    -(child.id() as i32)
}

#[cfg(unix)]
fn child_still_running(child: &mut Child) -> bool {
    child.try_wait().ok().flatten().is_none()
}

#[cfg(unix)]
fn send_process_group_signal(pgid: i32, signal: i32) {
    unsafe {
        let _ = kill(pgid, signal);
    }
}

#[cfg(not(unix))]
fn terminate_child(child: &mut Child) {
    let _ = child.kill();
}

#[cfg(unix)]
const SIGTERM: i32 = 15;

#[cfg(unix)]
const SIGKILL: i32 = 9;

#[cfg(unix)]
extern "C" {
    fn setpgid(pid: i32, pgid: i32) -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
}
