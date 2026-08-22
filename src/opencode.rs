//! Declared roles: orchestration, parser, accessor, filter, predicate, mapper, validator, formatter
//! adapter_declarations:
//!   - component: src/opencode.rs
//!     role: adapter
//!     Translates:
//!       - opencode run --format json event stream
//!       - opencode sessionID launch marker metadata
//!       - opencode event type/timestamp/part metadata
//!       - opencode session list rows to one typed provider observation
//!       - opencode export native session JSON
//!       - opencode auth list status plus observed credential-file effect

use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::native_process::{
    actor_for_child, terminate_process_group_child, ExecGate, GatedCommand, ProcessGroupActor,
};
use crate::native_runtime::NativeRuntimeContext;
use crate::shell::ShellOutput;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
#[cfg(unix)]
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

pub const MAX_EXPORT_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SESSION_LIST_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_NATIVE_COMMAND_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_NATIVE_EVENT_LINE_BYTES: usize = 1024 * 1024;
const MAX_NATIVE_EVENT_FAILURE_DETAILS_PER_BATCH: usize = 4;
const SESSION_LIST_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Default)]
pub struct EventParser {
    pending: Vec<u8>,
    discarding_oversized_line: bool,
    failures: EventParseFailureSummary,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventParseFailureSummary {
    pub representative_details: Vec<String>,
    pub omitted_count: u64,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpencodeSessionListRow {
    pub provider_session_id: String,
    pub title: Option<String>,
    pub directory: OpencodeSessionDirectory,
    pub created_unix_ms: Option<u64>,
    pub updated_unix_ms: Option<u64>,
    pub turn_count: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpencodeSessionDirectory {
    Missing,
    Absolute(String),
    Invalid(String),
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
    Failed {
        status: Option<i32>,
        stderr: String,
    },
    InvalidJson(String),
    InvalidRow {
        index: usize,
        message: String,
    },
    OutputTooLarge {
        stream: &'static str,
        maximum_bytes: usize,
    },
    TimedOut,
}

#[derive(Debug)]
pub enum OpencodeImportError {
    Spawn(String),
    Failed {
        status: Option<i32>,
        stderr: String,
    },
    InvalidUtf8(String),
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
    pub output_exceeded_bound: bool,
}

pub(crate) struct PreparedOpencodeAuthObservation {
    before: Option<Vec<u8>>,
    auth_path: PathBuf,
    custody: ChildCustody,
    gate: ExecGate,
    actor: ProcessGroupActor,
}

impl PreparedOpencodeAuthObservation {
    pub(crate) fn actor(&self) -> &ProcessGroupActor {
        &self.actor
    }

    pub(crate) fn observe(
        self,
        timeout: Duration,
    ) -> Result<OpencodeAuthObservation, OpencodeAuthFailure> {
        let Self {
            before,
            auth_path,
            custody,
            gate,
            actor: _,
        } = self;
        gate.release()
            .map_err(OpencodeAuthFailure::EffectUnsettled)?;
        let output = custody
            .wait_with_bounded_output_timeout(
                timeout,
                MAX_NATIVE_COMMAND_OUTPUT_BYTES,
                MAX_NATIVE_COMMAND_OUTPUT_BYTES,
            )
            .map_err(OpencodeAuthFailure::EffectUnsettled)?
            .ok_or_else(|| {
                OpencodeAuthFailure::EffectUnsettled(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "opencode auth list timed out",
                ))
            })?;
        let output_exceeded_bound = output.stdout.len() > MAX_NATIVE_COMMAND_OUTPUT_BYTES
            || output.stderr.len() > MAX_NATIVE_COMMAND_OUTPUT_BYTES;
        let after =
            credential_snapshot(&auth_path).map_err(OpencodeAuthFailure::EffectUnsettled)?;
        Ok(OpencodeAuthObservation {
            output: ShellOutput {
                stdout: output.stdout,
                stderr: output.stderr,
                status: output.status.code().unwrap_or(1),
            },
            effect: observed_auth_effect(before, after),
            output_exceeded_bound,
        })
    }
}

impl OpencodeAuthObservation {
    pub fn command_succeeded(&self) -> bool {
        self.output.status == 0
    }

    pub fn observation_succeeded(&self) -> bool {
        self.command_succeeded() && !self.output_exceeded_bound
    }

    pub fn credentials_refreshed(&self) -> bool {
        self.observation_succeeded() && self.effect == OpencodeAuthEffect::CredentialsChanged
    }
}

#[derive(Debug)]
pub enum OpencodeAuthFailure {
    BeforeEffect(std::io::Error),
    EffectUnsettled(std::io::Error),
}

impl OpencodeAuthFailure {
    pub fn effect_was_possible(&self) -> bool {
        matches!(self, Self::EffectUnsettled(_))
    }

    pub fn kind(&self) -> std::io::ErrorKind {
        match self {
            Self::BeforeEffect(error) | Self::EffectUnsettled(error) => error.kind(),
        }
    }
}

impl std::fmt::Display for OpencodeAuthFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BeforeEffect(error) => {
                write!(formatter, "before native effect capability: {error}")
            }
            Self::EffectUnsettled(error) => {
                write!(formatter, "after native effect capability: {error}")
            }
        }
    }
}

impl std::error::Error for OpencodeAuthFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BeforeEffect(error) | Self::EffectUnsettled(error) => Some(error),
        }
    }
}

impl EventParser {
    pub fn ingest(&mut self, bytes: &[u8]) -> Vec<OpencodeEventMetadata> {
        let mut lines = Vec::new();
        let mut remaining = bytes;
        while !remaining.is_empty() {
            if self.discarding_oversized_line {
                let Some(newline) = remaining.iter().position(|byte| *byte == b'\n') else {
                    break;
                };
                self.discarding_oversized_line = false;
                remaining = &remaining[newline + 1..];
                continue;
            }

            let newline = remaining.iter().position(|byte| *byte == b'\n');
            let content_bytes = newline.unwrap_or(remaining.len());
            if self.pending.len().saturating_add(content_bytes) > MAX_NATIVE_EVENT_LINE_BYTES {
                self.pending.clear();
                self.record_failure(format!(
                    "native event line exceeds the {MAX_NATIVE_EVENT_LINE_BYTES}-byte metadata parsing bound"
                ));
                match newline {
                    Some(index) => remaining = &remaining[index + 1..],
                    None => {
                        self.discarding_oversized_line = true;
                        break;
                    }
                }
                continue;
            }

            let consumed = content_bytes + usize::from(newline.is_some());
            self.pending.extend_from_slice(&remaining[..consumed]);
            remaining = &remaining[consumed..];
            if newline.is_some() {
                let line = std::mem::take(&mut self.pending);
                if !line.trim_ascii().is_empty() {
                    lines.push(line);
                }
            }
        }
        self.parse_lines(&lines)
    }

    pub fn finish(&mut self) -> Vec<OpencodeEventMetadata> {
        self.discarding_oversized_line = false;
        if self.pending.is_empty() {
            return Vec::new();
        }
        let line = std::mem::take(&mut self.pending);
        self.parse_lines(&[line])
    }

    pub fn take_failure_summary(&mut self) -> EventParseFailureSummary {
        std::mem::take(&mut self.failures)
    }

    fn parse_lines(&mut self, lines: &[Vec<u8>]) -> Vec<OpencodeEventMetadata> {
        let mut events = Vec::new();
        for line in lines {
            match serde_json::from_slice::<OpencodeEventMetadata>(line) {
                Ok(event) => events.extend(pinned_native_event(event)),
                Err(error) => self.record_failure(error.to_string()),
            }
        }
        events
    }

    fn record_failure(&mut self, detail: String) {
        if self.failures.representative_details.len() < MAX_NATIVE_EVENT_FAILURE_DETAILS_PER_BATCH {
            self.failures.representative_details.push(detail);
        } else {
            self.failures.omitted_count = self.failures.omitted_count.saturating_add(1);
        }
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
    fixed_args: &[String],
    working_directory: &str,
    env: &BTreeMap<String, String>,
    timeout: Duration,
) -> Result<OpencodeExport, OpencodeExportError> {
    let mut command = Command::new(program);
    command
        .args(fixed_args)
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
) -> Result<Vec<OpencodeSessionListRow>, OpencodeSessionListError> {
    let mut command = runtime.command();
    configure_session_list_command(&mut command, limit);
    run_session_list_command(command, SESSION_LIST_TIMEOUT)
}

pub fn session_list_with_timeout(
    limit: Option<usize>,
    runtime: &NativeRuntimeContext,
    timeout: Duration,
) -> Result<Vec<OpencodeSessionListRow>, OpencodeSessionListError> {
    let mut command = runtime.command();
    configure_session_list_command(&mut command, limit);
    run_session_list_command(command, timeout)
}

pub fn session_list_with_launch_context(
    program: &str,
    fixed_args: &[String],
    working_directory: &str,
    env: &BTreeMap<String, String>,
    limit: Option<usize>,
    timeout: Duration,
) -> Result<Vec<OpencodeSessionListRow>, OpencodeSessionListError> {
    let mut command = Command::new(program);
    command
        .args(fixed_args)
        .current_dir(working_directory)
        .env_clear()
        .envs(env);
    configure_session_list_command(&mut command, limit);
    run_session_list_command(command, timeout)
}

fn configure_session_list_command(command: &mut Command, limit: Option<usize>) {
    command
        .arg("session")
        .arg("list")
        .arg("--format")
        .arg("json");
    if let Some(limit) = limit {
        command.arg("--max-count").arg(limit.to_string());
    }
}

fn run_session_list_command(
    mut command: Command,
    timeout: Duration,
) -> Result<Vec<OpencodeSessionListRow>, OpencodeSessionListError> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = command.spawn().map_err(session_list_spawn_error)?;
    let output = ChildCustody::new(child)
        .wait_with_bounded_output_timeout(
            timeout,
            MAX_SESSION_LIST_OUTPUT_BYTES,
            MAX_NATIVE_COMMAND_OUTPUT_BYTES,
        )
        .map_err(session_list_spawn_error)?
        .ok_or(OpencodeSessionListError::TimedOut)?;
    validate_bounded_session_list_output(&output)?;
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

pub(crate) fn prepare_auth_list(
    runtime: &NativeRuntimeContext,
    auth_path: &Path,
) -> Result<PreparedOpencodeAuthObservation, OpencodeAuthFailure> {
    let before = credential_snapshot(auth_path).map_err(OpencodeAuthFailure::BeforeEffect)?;
    let mut command = GatedCommand::new(runtime.program(), runtime.fixed_args())
        .map_err(OpencodeAuthFailure::BeforeEffect)?;
    command
        .command_mut()
        .arg("auth")
        .arg("list")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_clear()
        .envs(runtime.execution_environment(&BTreeMap::new()));
    let (child, gate) = command.spawn().map_err(OpencodeAuthFailure::BeforeEffect)?;
    let custody = ChildCustody::with_cleanup(child, |child| {
        let _ = terminate_process_group_child(child);
    });
    let actor = actor_for_child(
        custody
            .child_ref()
            .expect("prepared auth-list child custody is active"),
    )
    .map_err(OpencodeAuthFailure::BeforeEffect)?;
    Ok(PreparedOpencodeAuthObservation {
        before,
        auth_path: auth_path.to_path_buf(),
        custody,
        gate,
        actor,
    })
}

fn credential_snapshot(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    match durable_fs::read_file_bounded(path, durable_fs::MAX_AUTH_FILE_BYTES) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
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

pub fn parse_session_list_stdout(
    stdout: &[u8],
) -> Result<Vec<OpencodeSessionListRow>, OpencodeSessionListError> {
    let start = session_list_json_start(stdout)?;
    parse_session_list_json(&stdout[start..])
}

pub fn parse_import_stdout(stdout: &[u8]) -> Result<String, OpencodeImportError> {
    let text = std::str::from_utf8(stdout)
        .map_err(|error| OpencodeImportError::InvalidUtf8(error.to_string()))?;
    text.lines()
        .find_map(|line| line.trim().strip_prefix("Imported session: "))
        .map(str::trim)
        .filter(|session_id| !session_id.is_empty())
        .map(str::to_string)
        .ok_or_else(|| OpencodeImportError::MissingSessionId(text.to_string()))
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

fn validate_bounded_session_list_output(
    output: &std::process::Output,
) -> Result<(), OpencodeSessionListError> {
    for (stream, bytes, maximum_bytes) in [
        (
            "stdout",
            output.stdout.as_slice(),
            MAX_SESSION_LIST_OUTPUT_BYTES,
        ),
        (
            "stderr",
            output.stderr.as_slice(),
            MAX_NATIVE_COMMAND_OUTPUT_BYTES,
        ),
    ] {
        if bytes.len() > maximum_bytes {
            return Err(OpencodeSessionListError::OutputTooLarge {
                stream,
                maximum_bytes,
            });
        }
    }
    Ok(())
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

fn parse_session_list_json(
    bytes: &[u8],
) -> Result<Vec<OpencodeSessionListRow>, OpencodeSessionListError> {
    let native: Value = serde_json::from_slice(bytes).map_err(invalid_session_list_json_error)?;
    let rows = native.as_array().ok_or_else(|| {
        OpencodeSessionListError::InvalidJson("session list is not a JSON array".to_string())
    })?;
    rows.iter()
        .enumerate()
        .map(|(index, row)| parse_session_list_row(index, row))
        .collect()
}

fn parse_session_list_row(
    index: usize,
    row: &Value,
) -> Result<OpencodeSessionListRow, OpencodeSessionListError> {
    if !row.is_object() {
        return Err(invalid_session_list_row(index, "row is not a JSON object"));
    }
    let provider_session_id = optional_session_list_string(
        index,
        row,
        &["id", "sessionID", "sessionId", "session_id"],
        "session identity",
    )?
    .filter(|value| !value.trim().is_empty())
    .ok_or_else(|| invalid_session_list_row(index, "row has no non-empty session identity"))?;
    let title = optional_session_list_string(index, row, &["title"], "title")?
        .filter(|value| !value.trim().is_empty());
    let directory = match optional_session_list_string(
        index,
        row,
        &["directory", "cwd", "working_directory"],
        "directory",
    )? {
        None => OpencodeSessionDirectory::Missing,
        Some(directory) if Path::new(&directory).is_absolute() => {
            OpencodeSessionDirectory::Absolute(directory)
        }
        Some(directory) => OpencodeSessionDirectory::Invalid(directory),
    };
    let nested_time = optional_session_list_object(index, row, "time")?;
    let created_unix_ms = merge_session_list_field(
        index,
        "created timestamp",
        optional_session_list_u64(
            index,
            row,
            &["created", "created_unix_ms", "createdUnixMs"],
            "created timestamp",
        )?,
        nested_time
            .map(|time| {
                optional_session_list_u64(
                    index,
                    time,
                    &["created", "created_unix_ms"],
                    "time.created timestamp",
                )
            })
            .transpose()?
            .flatten(),
    )?;
    let updated_unix_ms = merge_session_list_field(
        index,
        "updated timestamp",
        optional_session_list_u64(
            index,
            row,
            &["updated", "updated_unix_ms", "updatedUnixMs"],
            "updated timestamp",
        )?,
        nested_time
            .map(|time| {
                optional_session_list_u64(
                    index,
                    time,
                    &["updated", "updated_unix_ms"],
                    "time.updated timestamp",
                )
            })
            .transpose()?
            .flatten(),
    )?;
    let explicit_turn_count = optional_session_list_u64(
        index,
        row,
        &["turn_count", "turnCount", "message_count", "messageCount"],
        "turn count",
    )?;
    let turn_count = match (explicit_turn_count, row.get("messages")) {
        (Some(count), _) => Some(count),
        (None, None | Some(Value::Null)) => None,
        (None, Some(Value::Array(messages))) => Some(messages.len() as u64),
        (None, Some(_)) => {
            return Err(invalid_session_list_row(
                index,
                "messages is not a JSON array",
            ));
        }
    };
    Ok(OpencodeSessionListRow {
        provider_session_id,
        title,
        directory,
        created_unix_ms,
        updated_unix_ms,
        turn_count,
    })
}

fn optional_session_list_object<'a>(
    index: usize,
    row: &'a Value,
    key: &str,
) -> Result<Option<&'a Value>, OpencodeSessionListError> {
    match row.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value @ Value::Object(_)) => Ok(Some(value)),
        Some(_) => Err(invalid_session_list_row(
            index,
            format!("{key} is not a JSON object"),
        )),
    }
}

fn optional_session_list_string(
    index: usize,
    row: &Value,
    keys: &[&str],
    label: &str,
) -> Result<Option<String>, OpencodeSessionListError> {
    let mut observed = None;
    for key in keys {
        let Some(value) = row.get(*key) else {
            continue;
        };
        let candidate = match value {
            Value::Null => None,
            Value::String(value) => Some(value.clone()),
            _ => {
                return Err(invalid_session_list_row(
                    index,
                    format!("{label} field {key} is not a string"),
                ));
            }
        };
        observed = merge_session_list_field(index, label, observed, candidate)?;
    }
    Ok(observed)
}

fn optional_session_list_u64(
    index: usize,
    row: &Value,
    keys: &[&str],
    label: &str,
) -> Result<Option<u64>, OpencodeSessionListError> {
    let mut observed = None;
    for key in keys {
        let Some(value) = row.get(*key) else {
            continue;
        };
        let candidate = match value {
            Value::Null => None,
            Value::Number(number) => Some(number.as_u64().ok_or_else(|| {
                invalid_session_list_row(
                    index,
                    format!("{label} field {key} is not an unsigned integer"),
                )
            })?),
            Value::String(value) => Some(value.parse::<u64>().map_err(|_| {
                invalid_session_list_row(
                    index,
                    format!("{label} field {key} is not an unsigned integer"),
                )
            })?),
            _ => {
                return Err(invalid_session_list_row(
                    index,
                    format!("{label} field {key} is not an unsigned integer"),
                ));
            }
        };
        observed = merge_session_list_field(index, label, observed, candidate)?;
    }
    Ok(observed)
}

fn merge_session_list_field<T: Eq>(
    index: usize,
    label: &str,
    left: Option<T>,
    right: Option<T>,
) -> Result<Option<T>, OpencodeSessionListError> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => Err(invalid_session_list_row(
            index,
            format!("row has conflicting {label} aliases"),
        )),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn invalid_session_list_row(index: usize, message: impl Into<String>) -> OpencodeSessionListError {
    OpencodeSessionListError::InvalidRow {
        index,
        message: message.into(),
    }
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

fn pinned_native_event(event: OpencodeEventMetadata) -> Option<OpencodeEventMetadata> {
    is_pinned_native_event(&event).then_some(event)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_import_stdout, parse_session_list_stdout, EventParser, OpencodeImportError,
        OpencodeSessionDirectory, OpencodeSessionListError, OpencodeSessionListRow,
        MAX_NATIVE_EVENT_LINE_BYTES,
    };

    #[test]
    fn event_parser_bounds_partial_lines_and_recovers_at_the_next_frame() {
        let mut parser = EventParser::default();
        let oversized = vec![b'x'; MAX_NATIVE_EVENT_LINE_BYTES + 1];

        assert!(parser.ingest(&oversized).is_empty());
        assert!(parser.pending.is_empty());
        assert!(parser.discarding_oversized_line);
        let failures = parser.take_failure_summary();
        assert_eq!(failures.representative_details.len(), 1);
        assert_eq!(failures.omitted_count, 0);

        let valid = br#"{"type":"text","sessionID":"ses-after-oversized","timestamp":1,"part":{}}"#;
        let mut recovery = Vec::with_capacity(valid.len() + 2);
        recovery.push(b'\n');
        recovery.extend_from_slice(valid);
        recovery.push(b'\n');
        let events = parser.ingest(&recovery);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session_id.as_deref(), Some("ses-after-oversized"));
        assert!(!parser.discarding_oversized_line);
        assert!(parser.pending.is_empty());
    }

    #[test]
    fn event_parser_accepts_a_large_valid_line_in_fixed_drain_chunks() {
        let mut line = serde_json::to_vec(&serde_json::json!({
            "type": "text",
            "sessionID": "ses-large-event",
            "timestamp": 1,
            "part": { "text": "x".repeat(128 * 1024) }
        }))
        .expect("serialize large native event");
        line.push(b'\n');
        let mut parser = EventParser::default();
        let events = line
            .chunks(8 * 1024)
            .flat_map(|chunk| parser.ingest(chunk))
            .collect::<Vec<_>>();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session_id.as_deref(), Some("ses-large-event"));
        assert!(parser.pending.is_empty());
        assert!(parser
            .take_failure_summary()
            .representative_details
            .is_empty());
    }

    #[test]
    fn event_parser_reports_failure_multiplicity_beyond_local_details() {
        let mut parser = EventParser::default();
        let malformed = b"not-json\n".repeat(9);

        assert!(parser.ingest(&malformed).is_empty());
        let failures = parser.take_failure_summary();

        assert_eq!(failures.representative_details.len(), 4);
        assert_eq!(failures.omitted_count, 5);
    }

    #[test]
    fn import_edge_rejects_a_lossily_decodable_session_identity() {
        let error = parse_import_stdout(b"Imported session: ses-invalid-\xff\n")
            .expect_err("import identity must be strict UTF-8");

        assert!(matches!(error, OpencodeImportError::InvalidUtf8(_)));
    }

    #[test]
    fn session_list_edge_canonicalizes_identity_and_string_timestamps() {
        let rows = parse_session_list_stdout(
            br#"[{"sessionID":"ses-typed","cwd":"/tmp/typed","time":{"created":"41","updated":42},"messages":[{},{}]}]"#,
        )
        .expect("parse typed session-list observation");

        assert_eq!(
            rows,
            vec![OpencodeSessionListRow {
                provider_session_id: "ses-typed".to_string(),
                title: None,
                directory: OpencodeSessionDirectory::Absolute("/tmp/typed".to_string()),
                created_unix_ms: Some(41),
                updated_unix_ms: Some(42),
                turn_count: Some(2),
            }]
        );
    }

    #[test]
    fn session_list_edge_rejects_a_row_without_identity() {
        let error = parse_session_list_stdout(br#"[{"created":41}]"#)
            .expect_err("a row without identity must invalidate the whole observation");

        assert!(matches!(
            error,
            OpencodeSessionListError::InvalidRow { index: 0, .. }
        ));
    }

    #[test]
    fn session_list_edge_rejects_conflicting_timestamp_aliases() {
        let error = parse_session_list_stdout(
            br#"[{"id":"ses-conflict","created":41,"time":{"created":"42"}}]"#,
        )
        .expect_err("conflicting aliases must not be consumer-selected");

        assert!(matches!(
            error,
            OpencodeSessionListError::InvalidRow { index: 0, .. }
        ));
    }
}
