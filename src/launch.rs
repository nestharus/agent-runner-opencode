//! Declared roles: orchestration, adapter, formatter, parser, mapper

use crate::account::profile_for_settings_id;
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
    let argv = launch_argv(&params, request_id)?;
    let stdin = launch_stdin(params.stdin.as_ref(), request_id)?;
    let mut child = match spawn_child(&argv, &params, stdin.as_ref()) {
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

fn launch_argv(params: &LaunchParams, request_id: &str) -> Result<Vec<String>, ProviderFailure> {
    let account = profile_for_settings_id(&params.settings_id).ok_or_else(|| {
        ProviderFailure::invalid_request(
            request_id,
            "unknown_settings_id",
            format!("unknown opencode settings_id: {}", params.settings_id),
        )
    })?;
    let policy_params = policy_params_for_launch(params);
    let result = policy::evaluate(policy_params, request_id)?;
    Ok(result["argv"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .chain(std::iter::empty::<String>())
        .collect::<Vec<_>>()
        .into_iter()
        .enumerate()
        .map(|(index, arg)| {
            if index == 0 {
                account.opencode_wrapper.to_string()
            } else {
                arg
            }
        })
        .collect())
}

fn policy_params_for_launch(params: &LaunchParams) -> policy::PolicyEvaluateParams {
    serde_json::from_value(json!({
        "settings_id": params.settings_id,
        "mode": params.mode,
        "model": params.model,
        "launch": {
            "argv": params.argv,
            "env": params.env,
            "stdin": null,
        }
    }))
    .expect("launch params already satisfy policy params")
}

fn launch_stdin(
    input: Option<&BytePayload>,
    request_id: &str,
) -> Result<Option<Vec<u8>>, ProviderFailure> {
    input
        .map(|payload| decode_byte_payload(payload, request_id))
        .transpose()
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
    params: &LaunchParams,
    stdin: Option<&Vec<u8>>,
) -> std::io::Result<Child> {
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .current_dir(&params.working_directory)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    if let Some(env) = params.env.as_ref() {
        command.envs(
            env.iter()
                .filter(|(key, _)| !key.starts_with("OPENAI_API_KEY")),
        );
    }
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
        enforce_deadline(child, state)?;
        capture_child_exit(child, state)?;
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
        self.stdout.extend_from_slice(bytes);
        self.stream_bytes("stdout", bytes, writer)?;
        let events = self.parser.ingest(bytes);
        self.capture_session(first_session_id(&events), writer)
    }

    fn stderr_bytes<W: Write>(
        &mut self,
        bytes: &[u8],
        writer: &mut W,
    ) -> Result<(), ProviderFailure> {
        self.stderr.extend_from_slice(bytes);
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
        let events = self.parser.finish();
        self.capture_session(first_session_id(&events), writer)?;
        let status = self.final_status.clone().unwrap_or(ProcessStatus::Unknown);
        let signal = classify(&self.stdout, &self.stderr, &status, now_unix_ms());
        let mut event = json!({
            "contract": CONTRACT,
            "request_id": self.request_id,
            "seq": self.seq,
            "time_unix_ms": now_unix_ms(),
            "kind": "exit",
            "status": process_status_json(&status),
            "terminal_signal": signal,
        });
        if let Some(session_id) = self.session_id.as_ref() {
            event["session"] = json!({
                "provider_session_id": session_id,
                "source": "opencode.run.format_json",
            });
        }
        self.write_event(event, writer)?;
        Ok(exit_code_for_status(&status))
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
