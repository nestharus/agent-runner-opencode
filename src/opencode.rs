//! Declared roles: orchestration, parser, accessor, filter, predicate, mapper, validator, formatter
//! adapter_declarations:
//!   - component: src/opencode.rs
//!     role: adapter
//!     Translates:
//!       - opencode run --format json event stream
//!       - opencode sessionID launch marker metadata
//!       - opencode event type/timestamp/part metadata
//!       - opencode export native session JSON
//!       - opencode auth list status plus observed credential-file effect

use crate::child_custody::ChildCustody;
use crate::native_runtime::NativeRuntimeContext;
use crate::shell::ShellOutput;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
#[cfg(unix)]
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

pub const MAX_EXPORT_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_NATIVE_COMMAND_OUTPUT_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub struct EventParser {
    pending: Vec<u8>,
    errors: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeEventMetadata {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(rename = "sessionID")]
    pub session_id: Option<String>,
    pub timestamp: u64,
    #[serde(default)]
    pub part: Value,
    #[serde(default)]
    pub error: Option<OpencodeEventError>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeEventError {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub data: OpencodeEventErrorData,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct OpencodeEventErrorData {
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OpencodeExport {
    pub info: OpencodeExportInfo,
    pub messages: Vec<OpencodeMessage>,
    native_json: Value,
}

#[derive(Deserialize)]
struct ParsedOpencodeExport {
    info: OpencodeExportInfo,
    #[serde(default)]
    messages: Vec<OpencodeMessage>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeExportInfo {
    pub id: String,
    pub title: Option<String>,
    pub directory: Option<String>,
    #[serde(default)]
    pub model: Option<OpencodeModelIdentity>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeMessage {
    pub info: OpencodeMessageInfo,
    #[serde(default)]
    pub parts: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeMessageInfo {
    pub id: String,
    pub role: String,
    #[serde(rename = "sessionID")]
    pub session_id: Option<String>,
    pub time: Option<OpencodeMessageTime>,
    #[serde(default)]
    pub model: Option<OpencodeModelIdentity>,
    #[serde(rename = "modelID")]
    pub model_id: Option<String>,
    #[serde(rename = "providerID")]
    pub provider_id: Option<String>,
    pub variant: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeModelIdentity {
    #[serde(alias = "modelID")]
    pub id: Option<String>,
    #[serde(rename = "providerID")]
    pub provider_id: Option<String>,
    pub variant: Option<String>,
}

impl OpencodeMessageInfo {
    pub fn model_identity(&self) -> (Option<&str>, Option<&str>, Option<&str>) {
        (
            self.provider_id.as_deref().or_else(|| {
                self.model
                    .as_ref()
                    .and_then(|model| model.provider_id.as_deref())
            }),
            self.model_id
                .as_deref()
                .or_else(|| self.model.as_ref().and_then(|model| model.id.as_deref())),
            self.variant.as_deref().or_else(|| {
                self.model
                    .as_ref()
                    .and_then(|model| model.variant.as_deref())
            }),
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpencodeMessageTime {
    pub created: Option<u64>,
    pub completed: Option<u64>,
}

impl OpencodeExport {
    pub fn native_json(&self) -> &Value {
        &self.native_json
    }
}

#[derive(Debug)]
pub enum OpencodeExportError {
    Spawn(String),
    Failed {
        status: Option<i32>,
        stderr: String,
    },
    InvalidJson(String),
    OutputTooLarge {
        stream: &'static str,
        maximum_bytes: usize,
    },
    TimedOut,
}

#[derive(Debug)]
pub enum OpencodeSessionListError {
    Spawn(String),
    Failed { status: Option<i32>, stderr: String },
    InvalidJson(String),
}

#[derive(Debug)]
pub enum OpencodeImportError {
    Spawn(String),
    Failed {
        status: Option<i32>,
        stderr: String,
    },
    MissingSessionId(String),
    OutputTooLarge {
        stream: &'static str,
        maximum_bytes: usize,
    },
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpencodeAuthEffect {
    CredentialsChanged,
    CredentialsUnchanged,
    CredentialStateUnobservable,
}

#[derive(Debug)]
pub struct OpencodeAuthObservation {
    pub output: ShellOutput,
    pub effect: OpencodeAuthEffect,
}

impl OpencodeAuthObservation {
    pub fn command_succeeded(&self) -> bool {
        self.output.status == 0
    }

    pub fn credentials_refreshed(&self) -> bool {
        self.command_succeeded() && self.effect == OpencodeAuthEffect::CredentialsChanged
    }
}

impl EventParser {
    pub fn ingest(&mut self, bytes: &[u8]) -> Vec<OpencodeEventMetadata> {
        self.pending.extend_from_slice(bytes);
        let lines = drain_complete_lines(&mut self.pending);
        self.parse_lines(&lines)
    }

    pub fn finish(&mut self) -> Vec<OpencodeEventMetadata> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let line = std::mem::take(&mut self.pending);
        self.parse_lines(&[line])
    }

    pub fn take_errors(&mut self) -> Vec<String> {
        std::mem::take(&mut self.errors)
    }

    fn parse_lines(&mut self, lines: &[Vec<u8>]) -> Vec<OpencodeEventMetadata> {
        lines
            .iter()
            .filter_map(
                |line| match serde_json::from_slice::<OpencodeEventMetadata>(line) {
                    Ok(event) => pinned_native_event(event),
                    Err(error) => {
                        if self.errors.len() < 4 {
                            self.errors.push(error.to_string());
                        }
                        None
                    }
                },
            )
            .collect()
    }
}

pub fn first_session_id(events: &[OpencodeEventMetadata]) -> Option<String> {
    events.iter().find_map(|event| event.session_id.clone())
}

pub fn is_structured_error_event(event: &OpencodeEventMetadata) -> bool {
    event.event_type.as_str() == "error" && event.error.is_some()
}

pub fn is_successful_terminal_event(event: &OpencodeEventMetadata) -> bool {
    event.event_type == "step_finish"
        && event.part.get("reason").and_then(Value::as_str) == Some("stop")
}

pub fn export(
    session_id: &str,
    runtime: &NativeRuntimeContext,
) -> Result<OpencodeExport, OpencodeExportError> {
    export_with_timeout(session_id, runtime, Duration::from_secs(20))
}

pub fn export_with_timeout(
    session_id: &str,
    runtime: &NativeRuntimeContext,
    timeout: Duration,
) -> Result<OpencodeExport, OpencodeExportError> {
    let mut command = runtime.command();
    command.arg("export").arg(session_id);
    run_export_command(command, timeout)
}

pub fn export_with_launch_context(
    session_id: &str,
    program: &str,
    working_directory: &str,
    env: &BTreeMap<String, String>,
    timeout: Duration,
) -> Result<OpencodeExport, OpencodeExportError> {
    let mut command = Command::new(program);
    command
        .arg("export")
        .arg(session_id)
        .current_dir(working_directory)
        .env_clear()
        .envs(env);
    run_export_command(command, timeout)
}

#[cfg(unix)]
fn run_export_command(
    mut command: Command,
    timeout: Duration,
) -> Result<OpencodeExport, OpencodeExportError> {
    let mut stdout = tempfile::tempfile().map_err(export_capture_error)?;
    let stdout_writer = stdout.try_clone().map_err(export_capture_error)?;
    command
        .stdout(Stdio::from(stdout_writer))
        .stderr(Stdio::piped());
    constrain_export_file_size(&mut command);
    let child = command.spawn().map_err(export_spawn_error)?;
    let output = ChildCustody::new(child)
        .wait_with_bounded_output_timeout(timeout, 0, MAX_NATIVE_COMMAND_OUTPUT_BYTES)
        .map_err(export_spawn_error)?
        .ok_or(OpencodeExportError::TimedOut)?;
    let stdout_bytes = stdout.metadata().map_err(export_capture_error)?.len() as usize;
    validate_bounded_export_output(&output, stdout_bytes)?;
    validate_export_status(&output)?;
    stdout
        .seek(SeekFrom::Start(0))
        .map_err(export_capture_error)?;
    let mut bytes = Vec::with_capacity(stdout_bytes);
    (&mut stdout)
        .take(MAX_EXPORT_OUTPUT_BYTES.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .map_err(export_capture_error)?;
    parse_export_stdout(&bytes)
}

#[cfg(not(unix))]
fn run_export_command(
    mut command: Command,
    timeout: Duration,
) -> Result<OpencodeExport, OpencodeExportError> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = command.spawn().map_err(export_spawn_error)?;
    let output = ChildCustody::new(child)
        .wait_with_bounded_output_timeout(
            timeout,
            MAX_EXPORT_OUTPUT_BYTES,
            MAX_NATIVE_COMMAND_OUTPUT_BYTES,
        )
        .map_err(export_spawn_error)?
        .ok_or(OpencodeExportError::TimedOut)?;
    validate_bounded_export_output(&output, output.stdout.len())?;
    validate_export_status(&output)?;
    parse_export_stdout(&output.stdout)
}

pub fn session_list(
    limit: Option<usize>,
    runtime: &NativeRuntimeContext,
) -> Result<Vec<Value>, OpencodeSessionListError> {
    let mut command = runtime.command();
    command
        .arg("session")
        .arg("list")
        .arg("--format")
        .arg("json");
    if let Some(limit) = limit {
        command.arg("--max-count").arg(limit.to_string());
    }
    let output = command.output().map_err(session_list_spawn_error)?;
    validate_session_list_status(&output)?;
    parse_session_list_stdout(&output.stdout)
}

pub fn import_session(
    path: &Path,
    runtime: &NativeRuntimeContext,
    working_directory: &Path,
    timeout: Duration,
) -> Result<String, OpencodeImportError> {
    let mut command = runtime.command();
    command
        .current_dir(working_directory)
        .arg("import")
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn().map_err(import_spawn_error)?;
    let output = ChildCustody::new(child)
        .wait_with_bounded_output_timeout(
            timeout,
            MAX_NATIVE_COMMAND_OUTPUT_BYTES,
            MAX_NATIVE_COMMAND_OUTPUT_BYTES,
        )
        .map_err(import_spawn_error)?
        .ok_or(OpencodeImportError::TimedOut)?;
    validate_bounded_import_output(&output)?;
    validate_import_status(&output)?;
    parse_import_stdout(&output.stdout)
}

pub fn observe_auth_list(
    runtime: &NativeRuntimeContext,
    auth_path: &Path,
    timeout: Duration,
) -> std::io::Result<OpencodeAuthObservation> {
    let before = credential_snapshot(auth_path);
    let mut command = runtime.command();
    command
        .arg("auth")
        .arg("list")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn()?;
    let output = ChildCustody::new(child)
        .wait_with_bounded_output_timeout(
            timeout,
            MAX_NATIVE_COMMAND_OUTPUT_BYTES,
            MAX_NATIVE_COMMAND_OUTPUT_BYTES,
        )?
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "opencode auth list timed out")
        })?;
    if output.stdout.len() > MAX_NATIVE_COMMAND_OUTPUT_BYTES
        || output.stderr.len() > MAX_NATIVE_COMMAND_OUTPUT_BYTES
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "opencode auth list output exceeds supported {}-byte per-stream bound",
                MAX_NATIVE_COMMAND_OUTPUT_BYTES
            ),
        ));
    }
    let after = credential_snapshot(auth_path);
    Ok(OpencodeAuthObservation {
        output: ShellOutput {
            stdout: output.stdout,
            stderr: output.stderr,
            status: output.status.code().unwrap_or(1),
        },
        effect: observed_auth_effect(before, after),
    })
}

fn credential_snapshot(path: &Path) -> Option<Vec<u8>> {
    fs::read(path).ok()
}

fn observed_auth_effect(before: Option<Vec<u8>>, after: Option<Vec<u8>>) -> OpencodeAuthEffect {
    match (before, after) {
        (Some(before), Some(after)) if before != after => OpencodeAuthEffect::CredentialsChanged,
        (Some(_), Some(_)) => OpencodeAuthEffect::CredentialsUnchanged,
        (None, Some(_)) => OpencodeAuthEffect::CredentialsChanged,
        _ => OpencodeAuthEffect::CredentialStateUnobservable,
    }
}

pub fn parse_export_stdout(stdout: &[u8]) -> Result<OpencodeExport, OpencodeExportError> {
    let start = export_json_start(stdout)?;
    parse_export_json(&stdout[start..])
}

pub fn parse_session_list_stdout(stdout: &[u8]) -> Result<Vec<Value>, OpencodeSessionListError> {
    let start = session_list_json_start(stdout)?;
    parse_session_list_json(&stdout[start..])
}

pub fn parse_import_stdout(stdout: &[u8]) -> Result<String, OpencodeImportError> {
    let text = String::from_utf8_lossy(stdout);
    text.lines()
        .find_map(|line| line.trim().strip_prefix("Imported session: "))
        .map(str::trim)
        .filter(|session_id| !session_id.is_empty())
        .map(str::to_string)
        .ok_or_else(|| OpencodeImportError::MissingSessionId(text.into_owned()))
}

fn drain_complete_lines(pending: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let split_at = match pending.iter().rposition(|byte| *byte == b'\n') {
        Some(index) => index + 1,
        None => return Vec::new(),
    };
    let drained = pending.drain(..split_at).collect::<Vec<_>>();
    non_empty_lines(&drained)
}

fn is_pinned_native_event(event: &OpencodeEventMetadata) -> bool {
    is_pinned_part_event(event) || is_structured_error_event(event)
}

fn is_pinned_part_event(event: &OpencodeEventMetadata) -> bool {
    matches!(
        event.event_type.as_str(),
        "step_start" | "text" | "step_finish"
    ) && event.part.is_object()
}

fn export_spawn_error(err: std::io::Error) -> OpencodeExportError {
    OpencodeExportError::Spawn(err.to_string())
}

#[cfg(unix)]
fn export_capture_error(err: std::io::Error) -> OpencodeExportError {
    OpencodeExportError::Spawn(format!("failed to capture opencode export: {err}"))
}

fn import_spawn_error(err: std::io::Error) -> OpencodeImportError {
    OpencodeImportError::Spawn(err.to_string())
}

fn validate_bounded_export_output(
    output: &std::process::Output,
    stdout_bytes: usize,
) -> Result<(), OpencodeExportError> {
    if stdout_bytes > MAX_EXPORT_OUTPUT_BYTES {
        return Err(OpencodeExportError::OutputTooLarge {
            stream: "stdout",
            maximum_bytes: MAX_EXPORT_OUTPUT_BYTES,
        });
    }
    if output.stderr.len() > MAX_NATIVE_COMMAND_OUTPUT_BYTES {
        return Err(OpencodeExportError::OutputTooLarge {
            stream: "stderr",
            maximum_bytes: MAX_NATIVE_COMMAND_OUTPUT_BYTES,
        });
    }
    Ok(())
}

#[cfg(unix)]
fn constrain_export_file_size(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    let maximum_with_sentinel = MAX_EXPORT_OUTPUT_BYTES.saturating_add(1) as libc::rlim_t;
    unsafe {
        command.pre_exec(move || {
            let limit = libc::rlimit {
                rlim_cur: maximum_with_sentinel,
                rlim_max: maximum_with_sentinel,
            };
            if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

fn validate_bounded_import_output(
    output: &std::process::Output,
) -> Result<(), OpencodeImportError> {
    for (stream, bytes) in [
        ("stdout", output.stdout.as_slice()),
        ("stderr", output.stderr.as_slice()),
    ] {
        if bytes.len() > MAX_NATIVE_COMMAND_OUTPUT_BYTES {
            return Err(OpencodeImportError::OutputTooLarge {
                stream,
                maximum_bytes: MAX_NATIVE_COMMAND_OUTPUT_BYTES,
            });
        }
    }
    Ok(())
}

fn session_list_spawn_error(err: std::io::Error) -> OpencodeSessionListError {
    OpencodeSessionListError::Spawn(err.to_string())
}

fn validate_export_status(output: &std::process::Output) -> Result<(), OpencodeExportError> {
    if output.status.success() {
        return Ok(());
    }
    Err(export_failed_error(output))
}

fn validate_session_list_status(
    output: &std::process::Output,
) -> Result<(), OpencodeSessionListError> {
    if output.status.success() {
        return Ok(());
    }
    Err(session_list_failed_error(output))
}

fn validate_import_status(output: &std::process::Output) -> Result<(), OpencodeImportError> {
    if output.status.success() {
        return Ok(());
    }
    Err(OpencodeImportError::Failed {
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    })
}

fn export_failed_error(output: &std::process::Output) -> OpencodeExportError {
    OpencodeExportError::Failed {
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

fn session_list_failed_error(output: &std::process::Output) -> OpencodeSessionListError {
    OpencodeSessionListError::Failed {
        status: output.status.code(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    }
}

fn export_json_start(stdout: &[u8]) -> Result<usize, OpencodeExportError> {
    stdout
        .iter()
        .position(|byte| *byte == b'{')
        .ok_or_else(missing_export_json_error)
}

fn session_list_json_start(stdout: &[u8]) -> Result<usize, OpencodeSessionListError> {
    stdout
        .iter()
        .position(|byte| *byte == b'[')
        .ok_or_else(missing_session_list_json_error)
}

fn parse_export_json(bytes: &[u8]) -> Result<OpencodeExport, OpencodeExportError> {
    let native_json: Value = serde_json::from_slice(bytes).map_err(invalid_export_json_error)?;
    let parsed: ParsedOpencodeExport =
        serde_json::from_value(native_json.clone()).map_err(invalid_export_json_error)?;
    Ok(OpencodeExport {
        info: parsed.info,
        messages: parsed.messages,
        native_json,
    })
}

fn parse_session_list_json(bytes: &[u8]) -> Result<Vec<Value>, OpencodeSessionListError> {
    serde_json::from_slice(bytes).map_err(invalid_session_list_json_error)
}

fn missing_export_json_error() -> OpencodeExportError {
    OpencodeExportError::InvalidJson("missing JSON object".to_string())
}

fn missing_session_list_json_error() -> OpencodeSessionListError {
    OpencodeSessionListError::InvalidJson("missing JSON array".to_string())
}

fn invalid_export_json_error(err: serde_json::Error) -> OpencodeExportError {
    OpencodeExportError::InvalidJson(err.to_string())
}

fn invalid_session_list_json_error(err: serde_json::Error) -> OpencodeSessionListError {
    OpencodeSessionListError::InvalidJson(err.to_string())
}

fn non_empty_lines(drained: &[u8]) -> Vec<Vec<u8>> {
    drained
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.trim_ascii().is_empty())
        .map(Vec::from)
        .collect()
}

fn pinned_native_event(event: OpencodeEventMetadata) -> Option<OpencodeEventMetadata> {
    is_pinned_native_event(&event).then_some(event)
}
