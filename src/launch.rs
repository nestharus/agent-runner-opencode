//! Declared roles: orchestration, adapter, formatter, parser, mapper

use crate::encoding::{decode_base64, encode_base64, now_unix_ms};
use crate::envelope::{HostContext, ProviderFailure, CONTRACT};
use crate::opencode::{first_session_id, EventParser};
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

#[derive(Deserialize)]
struct LaunchParams {
    settings_id: String,
    mode: String,
    model: Value,
    argv: Vec<String>,
    working_directory: String,
    env: Option<BTreeMap<String, String>>,
    stdin: Option<BytePayload>,
    #[serde(rename = "session")]
    _session: Option<Value>,
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
    stream_child(request_id, host, &mut child, writer)
}

fn parse_launch_params(params: Value, request_id: &str) -> Result<LaunchParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_launch_params",
            format!("launch params are invalid: {err}"),
        )
    })
}

struct EffectiveLaunch {
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    stdin: Option<Vec<u8>>,
    _prompt: Option<String>,
}

enum PolicyLaunch {
    Accepted(EffectiveLaunch),
    Rejected(String),
}

fn launch_argv(params: &LaunchParams, request_id: &str) -> Result<PolicyLaunch, ProviderFailure> {
    let policy_params = policy_params_for_launch(params, request_id)?;
    let result = policy::evaluate(policy_params, request_id)?;
    if result.get("accepted").and_then(Value::as_bool) != Some(true) {
        return Ok(PolicyLaunch::Rejected(policy_rejection_reason(&result)));
    }
    Ok(PolicyLaunch::Accepted(effective_launch(
        result, request_id,
    )?))
}

fn effective_launch(result: Value, request_id: &str) -> Result<EffectiveLaunch, ProviderFailure> {
    let argv = result["argv"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if argv.is_empty() {
        return Err(ProviderFailure::invalid_request(
            request_id,
            "empty_policy_argv",
            "policy.evaluate returned no launch argv",
        ));
    }
    Ok(EffectiveLaunch {
        argv,
        env: effective_env_from_policy(&result, request_id)?,
        stdin: result
            .get("stdin")
            .and_then(Value::as_str)
            .map(|stdin| stdin.as_bytes().to_vec()),
        _prompt: result
            .get("prompt")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn effective_env_from_policy(
    result: &Value,
    request_id: &str,
) -> Result<BTreeMap<String, String>, ProviderFailure> {
    let Some(env) = result.get("env").and_then(Value::as_object) else {
        return Ok(BTreeMap::new());
    };
    env.iter()
        .map(|(key, value)| {
            let value = value.as_str().ok_or_else(|| {
                ProviderFailure::invalid_request(
                    request_id,
                    "invalid_policy_env",
                    "policy.evaluate returned a non-string env value",
                )
            })?;
            Ok((key.clone(), value.to_string()))
        })
        .collect()
}

fn policy_rejection_reason(result: &Value) -> String {
    let diagnostics = result
        .get("diagnostics")
        .cloned()
        .unwrap_or_else(|| json!([]));
    format!("policy.evaluate rejected launch params; diagnostics={diagnostics}")
}

fn policy_params_for_launch(
    params: &LaunchParams,
    request_id: &str,
) -> Result<policy::PolicyEvaluateParams, ProviderFailure> {
    serde_json::from_value(json!({
        "settings_id": params.settings_id,
        "mode": params.mode,
        "model": params.model,
        "launch": {
            "argv": params.argv,
            "env": params.env,
            "stdin": policy_stdin_for_launch(params.stdin.as_ref(), request_id)?,
        }
    }))
    .map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_launch_policy_params",
            format!("launch params could not be evaluated by policy: {err}"),
        )
    })
}

fn policy_stdin_for_launch(
    input: Option<&BytePayload>,
    request_id: &str,
) -> Result<Option<String>, ProviderFailure> {
    let Some(input) = input else {
        return Ok(None);
    };
    let bytes = decode_byte_payload(input, request_id)?;
    String::from_utf8(bytes).map(Some).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_stdin_utf8",
            format!("launch stdin must be UTF-8 at the policy boundary: {err}"),
        )
    })
}

fn decode_byte_payload(
    payload: &BytePayload,
    request_id: &str,
) -> Result<Vec<u8>, ProviderFailure> {
    match payload.encoding.as_str() {
        "base64" => decode_base64(&payload.data).map_err(|err| {
            ProviderFailure::invalid_request(
                request_id,
                "invalid_stdin_base64",
                format!("launch stdin base64 is invalid: {err}"),
            )
        }),
        "utf8" => Ok(payload.data.as_bytes().to_vec()),
        other => Err(ProviderFailure::invalid_request(
            request_id,
            "invalid_stdin_encoding",
            format!("unsupported launch stdin encoding: {other}"),
        )),
    }
}

fn spawn_child(
    argv: &[String],
    working_directory: &str,
    env: &BTreeMap<String, String>,
    stdin: Option<&Vec<u8>>,
) -> std::io::Result<Child> {
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .current_dir(working_directory)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    command.env_clear();
    command.envs(std::env::vars().filter(|(key, _)| !policy::is_forbidden_env_key(key)));
    command.envs(
        env.iter()
            .filter(|(key, _)| !policy::is_forbidden_env_key(key))
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    configure_process_group(&mut command);
    let mut child = command.spawn()?;
    write_child_stdin(&mut child, stdin)?;
    Ok(child)
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
    writer: &mut W,
) -> Result<i32, ProviderFailure> {
    let mut state = LaunchState::new(request_id, host.deadline_unix_ms);
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
    mut reader: R,
    sender: mpsc::Sender<DrainMessage>,
    stdout: bool,
) {
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        while let Ok(count) = reader.read(&mut buffer) {
            if count == 0 {
                break;
            }
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
        let done = if stdout {
            DrainMessage::StdoutDone
        } else {
            DrainMessage::StderrDone
        };
        let _ = sender.send(done);
    });
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
    state.final_status = Some(ProcessStatus::ProlongedSilence {
        reason: "no output before host deadline".to_string(),
    });
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
    let mut state = LaunchState::new(request_id, None);
    state.final_status = Some(ProcessStatus::SpawnError {
        reason: err.to_string(),
    });
    state.mark_drains_done();
    state.finish(writer)
}

fn stream_policy_rejection<W: Write>(
    request_id: &str,
    writer: &mut W,
    reason: String,
) -> Result<i32, ProviderFailure> {
    let mut state = LaunchState::new(request_id, None);
    state.final_status = Some(ProcessStatus::SpawnError { reason });
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
    session_id: Option<String>,
    deadline_unix_ms: Option<u64>,
    next_heartbeat: Instant,
}

impl LaunchState {
    fn new(request_id: &str, deadline_unix_ms: Option<u64>) -> Self {
        Self {
            request_id: request_id.to_string(),
            seq: 1,
            stdout_done: false,
            stderr_done: false,
            final_status: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            parser: EventParser::default(),
            session_id: None,
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
            json!({
                "contract": CONTRACT,
                "request_id": self.request_id,
                "seq": self.seq,
                "time_unix_ms": now_unix_ms(),
                "kind": kind,
                "data_base64": encode_base64(bytes),
            }),
            writer,
        )
    }

    fn capture_session<W: Write>(
        &mut self,
        session: Option<String>,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        if self.session_id.is_some() {
            return Ok(());
        }
        if let Some(session_id) = session {
            self.session_id = Some(session_id.clone());
            self.marker(format!("opencode.sessionID.{session_id}"), writer)?;
        }
        Ok(())
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
        first_session_id(&self.parser.ingest(bytes))
    }

    fn capture_session_from_parser_tail<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        let session = self.session_from_parser_tail();
        self.capture_session(session, writer)
    }

    fn session_from_parser_tail(&mut self) -> Option<String> {
        first_session_id(&self.parser.finish())
    }

    fn marker<W: Write>(&mut self, name: String, writer: &mut W) -> Result<(), ProviderFailure> {
        self.write_event(
            json!({
                "contract": CONTRACT,
                "request_id": self.request_id,
                "seq": self.seq,
                "time_unix_ms": now_unix_ms(),
                "kind": "marker",
                "name": name,
                "value": true,
            }),
            writer,
        )
    }

    fn heartbeat<W: Write>(&mut self, writer: &mut W) -> Result<(), ProviderFailure> {
        if self.final_status.is_some() {
            return Ok(());
        }
        self.next_heartbeat = Instant::now() + HEARTBEAT_INTERVAL;
        self.write_event(
            json!({
                "contract": CONTRACT,
                "request_id": self.request_id,
                "seq": self.seq,
                "time_unix_ms": now_unix_ms(),
                "kind": "heartbeat",
                "detail": "child still running",
            }),
            writer,
        )
    }

    fn finish<W: Write>(&mut self, writer: &mut W) -> Result<i32, ProviderFailure> {
        self.capture_session_from_parser_tail(writer)?;
        let status = self.finished_status();
        let signal = self.terminal_signal_for(&status);
        let event = self.exit_event(&status, signal);
        self.write_event(event, writer)?;
        Ok(provider_exit_code(&status))
    }

    fn finished_status(&self) -> ProcessStatus {
        self.final_status.clone().unwrap_or(ProcessStatus::Unknown)
    }

    fn terminal_signal_for(&self, status: &ProcessStatus) -> Value {
        classify(&self.stdout, &self.stderr, status, now_unix_ms())
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
        event["seq"] = json!(self.seq);
        serde_json::to_writer(&mut *writer, &event).map_err(json_write_failure)?;
        writer.write_all(b"\n").map_err(write_failure)?;
        writer.flush().map_err(write_failure)?;
        self.seq += 1;
        Ok(())
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

fn process_status_from_exit(status: ExitStatus) -> ProcessStatus {
    if let Some(code) = status.code() {
        return ProcessStatus::Exited { code };
    }
    signal_status(status)
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
        command.pre_exec(|| {
            if setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_child(child: &mut Child) {
    let pgid = -(child.id() as i32);
    unsafe {
        let _ = kill(pgid, SIGTERM);
    }
    std::thread::sleep(TERMINATION_GRACE);
    if child.try_wait().ok().flatten().is_none() {
        unsafe {
            let _ = kill(pgid, SIGKILL);
        }
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
