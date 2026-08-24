//! Declared roles: resolver, validator, accessor
//! intrinsic_surface_declarations:
//!   - component: src/native_runtime.rs
//!     role: intrinsic-surface
//!     Domain: durable OpenCode native runtime identity
//!     Owns:
//!       - one direct OpenCode executable and stable state-namespace binding per declared account
//!       - full executable admission at bind/rebind and constant-time metadata validation on reuse
//!       - exact launch-context admission against the durable account binding

use crate::account::AccountProfile;
use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use crate::native_implementation_manifest;
use crate::operation_bounds;
use crate::path_guard;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

const NATIVE_RUNTIME_STATE_DIR: &str = "provider-state/opencode/native-runtimes";
const NATIVE_RUNTIME_SCHEMA_VERSION: u32 = 6;
const PATH_BOUND_NATIVE_RUNTIME_SCHEMA_VERSION: u32 = 5;
const FULL_ENV_NATIVE_RUNTIME_SCHEMA_VERSION: u32 = 4;
const MANIFEST_NATIVE_RUNTIME_SCHEMA_VERSION: u32 = 3;
const DIRECT_NATIVE_RUNTIME_SCHEMA_VERSION: u32 = 2;
const WRAPPER_NATIVE_RUNTIME_SCHEMA_VERSION: u32 = 1;
const OPENCODE_NATIVE_PROGRAM: &str = "opencode";
pub(crate) const OPENCODE_NATIVE_CONTRACT_ID: &str =
    "agent-runner-opencode.opencode-native-state/v1";
pub(crate) const OPENCODE_NATIVE_FIXED_ARGS: &[&str] = &["--pure"];
const OPENCODE_BASH_DEFAULT_TIMEOUT_ENV: &str = "OPENCODE_EXPERIMENTAL_BASH_DEFAULT_TIMEOUT_MS";
// Agent Runner launches are allowed to run long-lived agent work. When the
// host does not make a different explicit choice, the provider owns this
// finite fallback so OpenCode's per-Bash default does not terminate otherwise
// healthy work before provider/host custody does. The value is deliberately
// below the signed 32-bit millisecond ceiling; an optional host deadline still
// remains the outer operation limit.
const PROVIDER_BASH_DEFAULT_TIMEOUT_MS: &str = "2000000000";
const NATIVE_RUNTIME_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const AUTO_UPDATE_VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const AUTO_UPDATE_VERSION_OUTPUT_BYTES: usize = 4 * 1024;
const MAX_NATIVE_RUNTIME_STATE_BYTES: usize = 1024 * 1024;
const NATIVE_RUNTIME_IDENTITY_ENV_KEYS: &[&str] = &[
    "HOME",
    "XDG_DATA_HOME",
    OPENCODE_BASH_DEFAULT_TIMEOUT_ENV,
    "OULIPOLY_OPENCODE_ACCOUNT",
];

#[derive(Clone)]
/// An admitted executable capability for one account's pinned native runtime.
///
/// The persisted JSON representation is private and cannot be deserialized
/// directly into this effectful type. Public callers obtain a context only
/// through the validating resolvers in this module.
pub struct NativeRuntimeContext {
    record: NativeRuntimeRecord,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
struct NativeRuntimeRecord {
    schema_version: u32,
    account_wrapper: String,
    program: String,
    program_sha256: String,
    execution_env: BTreeMap<String, String>,
    #[serde(default)]
    native_contract_id: String,
    #[serde(default)]
    fixed_args: Vec<String>,
    #[serde(default)]
    implementation_manifest_id: String,
    #[serde(default)]
    implementation_version: String,
    #[serde(default)]
    program_stamp: NativeProgramStamp,
    identity_sha256: String,
}

#[derive(Clone, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NativeProgramStamp {
    kind: String,
    byte_length: u64,
    device: u64,
    inode: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

impl NativeProgramStamp {
    pub(crate) fn is_complete(&self) -> bool {
        !self.kind.is_empty() && self.byte_length > 0
    }
}

pub fn resolve_for_account(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    let timeout = runtime_lock_timeout(host, request_id)?;
    resolve_for_account_with_timeout(host, account, timeout, request_id)
}

pub(crate) fn resolve_for_account_with_timeout(
    host: &HostContext,
    account: &AccountProfile,
    timeout: Duration,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    let _lock = acquire_runtime_lock(host, account, timeout, request_id)?;
    if let Some(record) = read_runtime_record(host, account, request_id)? {
        return activate_runtime_record(host, account, record, request_id);
    }
    let context = candidate_context(account, ambient_environment(), request_id)?;
    write_runtime_context(host, account, &context, request_id)?;
    Ok(context)
}

pub fn resolve_existing_for_account(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<NativeRuntimeContext>, ProviderFailure> {
    let timeout = runtime_lock_timeout(host, request_id)?;
    let _lock = acquire_runtime_lock(host, account, timeout, request_id)?;
    let Some(record) = read_runtime_record(host, account, request_id)? else {
        return Ok(None);
    };
    activate_runtime_record(host, account, record, request_id).map(Some)
}

pub(crate) fn resolve_existing_for_setup(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<NativeRuntimeContext>, ProviderFailure> {
    let _lock = acquire_runtime_lock(host, account, Duration::ZERO, request_id)?;
    let Some(record) = read_runtime_record(host, account, request_id)? else {
        return Ok(None);
    };
    preview_runtime_record_activation(account, record, request_id).map(Some)
}

pub(crate) fn persisted_identity_evidence(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<(String, String)>, ProviderFailure> {
    read_persisted_identity_evidence(host, account, request_id, false)
}

pub(crate) fn validated_persisted_identity_evidence(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<(String, String)>, ProviderFailure> {
    read_persisted_identity_evidence(host, account, request_id, true)
}

fn read_persisted_identity_evidence(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
    require_current_implementation: bool,
) -> Result<Option<(String, String)>, ProviderFailure> {
    let path = runtime_context_path(host, account, request_id)?;
    let bytes = match durable_fs::read_file_bounded(&path, MAX_NATIVE_RUNTIME_STATE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(native_runtime_failure(request_id, error)),
    };
    let record = serde_json::from_slice::<NativeRuntimeRecord>(&bytes)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    validate_runtime_record_identity(&record, account, request_id)?;
    if require_current_implementation {
        validate_runtime_record(&record, account, request_id)?;
    }
    Ok(Some((record.identity_sha256, sha256_hex(&bytes))))
}

pub fn resolve_for_launch(
    host: &HostContext,
    account: &AccountProfile,
    program: &str,
    declared_env: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    if program != account.opencode_wrapper {
        return Err(ProviderFailure::invalid_request(
            request_id,
            "native_runtime_program_mismatch",
            format!(
                "launch command for account {} must be the exact canonical wrapper {}",
                account.opencode_wrapper, account.opencode_wrapper
            ),
        ));
    }
    let launch_environment = launch_environment(declared_env);
    let requested_program = resolve_program(OPENCODE_NATIVE_PROGRAM, &launch_environment)
        .ok_or_else(|| {
            native_runtime_failure(
                request_id,
                format!(
                    "the direct {OPENCODE_NATIVE_PROGRAM} implementation for account {} was not found in the selected native PATH",
                    account.opencode_wrapper,
                ),
            )
        })?;
    let requested_environment = native_execution_environment(
        account,
        runtime_identity_environment(&launch_environment),
        request_id,
    )?;
    let timeout =
        operation_bounds::remaining_timeout(host.deadline_unix_ms, NATIVE_RUNTIME_LOCK_TIMEOUT)
            .unwrap_or(Duration::ZERO);
    let _lock = acquire_runtime_lock(host, account, timeout, request_id)?;
    if let Some(record) = read_runtime_record(host, account, request_id)? {
        let context = activate_runtime_record(host, account, record, request_id)?;
        let program_changed = Path::new(context.program()) != requested_program;
        let state_selector_conflicts = NATIVE_RUNTIME_IDENTITY_ENV_KEYS
            .iter()
            .filter(|key| {
                context.stable_execution_env().get(**key) != requested_environment.get(**key)
            })
            .map(|key| {
                json!({
                    "key": key,
                    "bound": context.stable_execution_env().get(*key),
                    "attempted": requested_environment.get(*key),
                })
            })
            .collect::<Vec<_>>();
        if program_changed || !state_selector_conflicts.is_empty() {
            let mut differences = Vec::new();
            if program_changed {
                differences.push(format!(
                    "native executable is bound to {:?}, but the current PATH resolves {:?}",
                    context.program(),
                    requested_program
                ));
            }
            if !state_selector_conflicts.is_empty() {
                differences.push(format!(
                    "state selectors differ: {}",
                    state_selector_conflicts
                        .iter()
                        .map(|conflict| {
                            let key = conflict["key"].as_str().unwrap_or("unknown");
                            let bound = conflict["bound"]
                                .as_str()
                                .map(|value| format!("{value:?}"))
                                .unwrap_or_else(|| "<unset>".to_string());
                            let attempted = conflict["attempted"]
                                .as_str()
                                .map(|value| format!("{value:?}"))
                                .unwrap_or_else(|| "<unset>".to_string());
                            format!("{key} (bound {bound}, attempted {attempted})")
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            return Err(ProviderFailure::conflict(
                request_id,
                "native_runtime_context_conflict",
                format!(
                    "account {} runtime context conflict: {}",
                    account.opencode_wrapper,
                    differences.join("; ")
                ),
                json!({
                    "account": account.opencode_wrapper,
                    "bound_program": context.program(),
                    "attempted_program": requested_program,
                    "conflicting_state_selectors": state_selector_conflicts,
                    "bound_runtime_identity_sha256": context.identity_sha256(),
                }),
            ));
        }
        return Ok(context);
    }
    let candidate = candidate_context(account, launch_environment, request_id)?;
    write_runtime_context(host, account, &candidate, request_id)?;
    Ok(candidate)
}

impl NativeRuntimeContext {
    pub fn command(&self) -> Command {
        let mut command = Command::new(&self.record.program);
        command
            .args(&self.record.fixed_args)
            .env_clear()
            .envs(self.execution_environment(&BTreeMap::new()));
        command
    }

    pub fn execution_environment(
        &self,
        declared_env: &BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        let mut environment = ambient_environment();
        environment.extend(declared_env.clone());
        // The persisted values select the admitted executable and account state
        // namespace. Keep them authoritative while forwarding every other
        // inherited or request-declared variable to the child unchanged.
        environment.extend(self.record.execution_env.clone());
        if self.record.account_wrapper == "opencode1"
            && !self.record.execution_env.contains_key("XDG_DATA_HOME")
        {
            environment.remove("XDG_DATA_HOME");
        }
        environment
    }

    pub fn program(&self) -> &str {
        &self.record.program
    }

    pub(crate) fn fixed_args(&self) -> &[String] {
        &self.record.fixed_args
    }

    pub(crate) fn program_sha256(&self) -> &str {
        &self.record.program_sha256
    }

    pub(crate) fn native_contract_id(&self) -> &str {
        &self.record.native_contract_id
    }

    pub(crate) fn implementation_manifest_id(&self) -> &str {
        &self.record.implementation_manifest_id
    }

    pub(crate) fn implementation_version(&self) -> &str {
        &self.record.implementation_version
    }

    pub(crate) fn program_stamp(&self) -> &NativeProgramStamp {
        &self.record.program_stamp
    }

    pub fn stable_execution_env(&self) -> &BTreeMap<String, String> {
        &self.record.execution_env
    }

    pub fn identity_sha256(&self) -> &str {
        &self.record.identity_sha256
    }

    pub fn expand_path(&self, path: &str) -> PathBuf {
        match (
            path.strip_prefix("~/"),
            self.record.execution_env.get("HOME"),
        ) {
            (Some(relative), Some(home)) => Path::new(home).join(relative),
            _ => PathBuf::from(path),
        }
    }
}

fn candidate_context(
    account: &AccountProfile,
    execution_env: BTreeMap<String, String>,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    let resolution_env = native_execution_environment(account, execution_env, request_id)?;
    let program = resolve_program(OPENCODE_NATIVE_PROGRAM, &resolution_env).ok_or_else(|| {
        native_runtime_failure(
            request_id,
            format!(
                "the direct {OPENCODE_NATIVE_PROGRAM} implementation for account {} was not found in the selected native PATH",
                account.opencode_wrapper,
            ),
        )
    })?;
    let execution_env = native_execution_environment(
        account,
        runtime_identity_environment(&resolution_env),
        request_id,
    )?;
    admit_candidate_program(account, program, execution_env, None, request_id)
}

fn admit_candidate_program(
    account: &AccountProfile,
    program: PathBuf,
    execution_env: BTreeMap<String, String>,
    auto_update_from: Option<&NativeRuntimeRecord>,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    if !durable_fs::is_executable_file(&program)
        .map_err(|error| native_runtime_failure(request_id, error))?
    {
        return Err(native_runtime_failure(
            request_id,
            format!(
                "the direct OpenCode implementation for account {} is not an executable regular file",
                account.opencode_wrapper,
            ),
        ));
    }
    let program_stamp = native_program_stamp(&program)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    let (program_sha256, program_bytes) =
        durable_fs::sha256_file_bounded(&program, durable_fs::MAX_BOUND_EXECUTABLE_BYTES)
            .map_err(|error| native_runtime_failure(request_id, error))?;
    let observed_stamp = native_program_stamp(&program)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    if observed_stamp != program_stamp || observed_stamp.byte_length != program_bytes as u64 {
        return Err(native_runtime_failure(
            request_id,
            "the direct OpenCode implementation changed during manifest admission",
        ));
    }
    let program = program.to_str().ok_or_else(|| {
        native_runtime_failure(
            request_id,
            "direct OpenCode implementation path is not valid UTF-8",
        )
    })?;
    let approved = native_implementation_manifest::approved_implementation(
        OPENCODE_NATIVE_PROGRAM,
        &program_sha256,
        program_bytes,
    )
    .map_err(|error| native_runtime_failure(request_id, error))?;
    let approved = match (approved, auto_update_from) {
        (Some(approved), _) => approved,
        (None, Some(prior)) => admit_forward_auto_update(
            account,
            Path::new(program),
            &execution_env,
            &program_sha256,
            program_bytes,
            prior,
            request_id,
        )?,
        (None, None) => {
            return Err(unapproved_native_implementation(
                account,
                program,
                &program_sha256,
                program_bytes,
                request_id,
            ));
        }
    };
    if approved.semantic_contract != OPENCODE_NATIVE_CONTRACT_ID {
        return Err(native_runtime_failure(
            request_id,
            "the reviewed OpenCode implementation has the wrong semantic contract",
        ));
    }
    let final_stamp = native_program_stamp(Path::new(program))
        .map_err(|error| native_runtime_failure(request_id, error))?;
    if final_stamp != program_stamp {
        return Err(native_runtime_failure(
            request_id,
            "the direct OpenCode implementation changed before admission completed",
        ));
    }
    let fixed_args = OPENCODE_NATIVE_FIXED_ARGS
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let identity_sha256 = runtime_identity_sha256(
        account.opencode_wrapper,
        program,
        &program_sha256,
        &execution_env,
        OPENCODE_NATIVE_CONTRACT_ID,
        &fixed_args,
        (&approved.id, &approved.version),
    );
    Ok(NativeRuntimeContext {
        record: NativeRuntimeRecord {
            schema_version: NATIVE_RUNTIME_SCHEMA_VERSION,
            account_wrapper: account.opencode_wrapper.to_string(),
            program: program.to_string(),
            program_sha256,
            execution_env,
            native_contract_id: OPENCODE_NATIVE_CONTRACT_ID.to_string(),
            fixed_args,
            implementation_manifest_id: approved.id,
            implementation_version: approved.version,
            program_stamp,
            identity_sha256,
        },
    })
}

fn admit_forward_auto_update(
    account: &AccountProfile,
    program: &Path,
    execution_env: &BTreeMap<String, String>,
    program_sha256: &str,
    program_bytes: usize,
    prior: &NativeRuntimeRecord,
    request_id: &str,
) -> Result<native_implementation_manifest::ApprovedImplementation, ProviderFailure> {
    let prior_version =
        native_implementation_manifest::parse_numeric_version(&prior.implementation_version)
            .ok_or_else(|| {
                native_runtime_failure(
                    request_id,
                    "the prior native implementation does not have a numeric auto-update version",
                )
            })?;
    let version = probe_opencode_version(program, execution_env, request_id)?;
    let observed_version = native_implementation_manifest::parse_numeric_version(&version)
        .filter(|observed| *observed > prior_version)
        .ok_or_else(|| {
            ProviderFailure::conflict(
                request_id,
                "native_runtime_auto_update_not_forward",
                format!(
                    "account {} observed OpenCode version {version}, which is not newer than its bound version {}",
                    account.opencode_wrapper, prior.implementation_version
                ),
                json!({
                    "account": account.opencode_wrapper,
                    "program": program,
                    "bound_version": prior.implementation_version,
                    "observed_version": version,
                }),
            )
        })?;
    let _ = observed_version;
    native_implementation_manifest::auto_update_implementation(
        &version,
        program_sha256,
        program_bytes,
    )
    .ok_or_else(|| {
        native_runtime_failure(
            request_id,
            "the forward OpenCode auto-update identity is not canonical",
        )
    })
}

fn probe_opencode_version(
    program: &Path,
    execution_env: &BTreeMap<String, String>,
    request_id: &str,
) -> Result<String, ProviderFailure> {
    let mut command = Command::new(program);
    command
        .arg("--version")
        .env_clear()
        .envs(execution_env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn().map_err(|error| {
        native_runtime_failure(
            request_id,
            format!("failed to start OpenCode auto-update version probe: {error}"),
        )
    })?;
    let output = ChildCustody::new(child)
        .wait_with_bounded_output_timeout(
            AUTO_UPDATE_VERSION_PROBE_TIMEOUT,
            AUTO_UPDATE_VERSION_OUTPUT_BYTES,
            AUTO_UPDATE_VERSION_OUTPUT_BYTES,
        )
        .map_err(|error| {
            native_runtime_failure(
                request_id,
                format!("OpenCode auto-update version probe failed: {error}"),
            )
        })?
        .ok_or_else(|| {
            native_runtime_failure(request_id, "OpenCode auto-update version probe timed out")
        })?;
    if output.stdout.len() > AUTO_UPDATE_VERSION_OUTPUT_BYTES
        || output.stderr.len() > AUTO_UPDATE_VERSION_OUTPUT_BYTES
    {
        return Err(native_runtime_failure(
            request_id,
            "OpenCode auto-update version probe exceeded its output bound",
        ));
    }
    if !output.status.success() {
        return Err(native_runtime_failure(
            request_id,
            format!(
                "OpenCode auto-update version probe exited with status {:?}: {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }
    let version = std::str::from_utf8(&output.stdout)
        .map(str::trim)
        .map_err(|error| {
            native_runtime_failure(
                request_id,
                format!("OpenCode auto-update version was not UTF-8: {error}"),
            )
        })?;
    native_implementation_manifest::parse_numeric_version(version)
        .map(|_| version.to_string())
        .ok_or_else(|| {
            native_runtime_failure(
                request_id,
                format!("OpenCode auto-update reported invalid version {version:?}"),
            )
        })
}

fn unapproved_native_implementation(
    account: &AccountProfile,
    program: &str,
    program_sha256: &str,
    program_bytes: usize,
    request_id: &str,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "native_runtime_implementation_unapproved",
        format!(
            "the initial OpenCode implementation for account {} is not in the reviewed native implementation manifest; automatic lineage admission is available only for a forward same-path update of an existing binding",
            account.opencode_wrapper
        ),
        json!({
            "account": account.opencode_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "program_bytes": program_bytes,
            "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
        }),
    )
}

fn native_execution_environment(
    account: &AccountProfile,
    mut execution_env: BTreeMap<String, String>,
    request_id: &str,
) -> Result<BTreeMap<String, String>, ProviderFailure> {
    execution_env.insert(
        "OULIPOLY_OPENCODE_ACCOUNT".to_string(),
        account.opencode_wrapper.to_string(),
    );
    if account.opencode_index > 1 && !execution_env.contains_key("XDG_DATA_HOME") {
        let home = execution_env
            .get("HOME")
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                native_runtime_failure(
                    request_id,
                    format!(
                        "account {} requires HOME to select its native OpenCode state namespace",
                        account.opencode_wrapper
                    ),
                )
            })?;
        execution_env.insert(
            "XDG_DATA_HOME".to_string(),
            Path::new(home)
                .join(format!(".opencode{}", account.opencode_index))
                .to_string_lossy()
                .into_owned(),
        );
    }
    execution_env
        .entry(OPENCODE_BASH_DEFAULT_TIMEOUT_ENV.to_string())
        .or_insert_with(|| PROVIDER_BASH_DEFAULT_TIMEOUT_MS.to_string());
    Ok(execution_env)
}

fn launch_environment(declared_env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut environment = ambient_environment();
    environment.extend(declared_env.clone());
    // A non-empty request environment is authoritative about an intentionally
    // absent XDG_DATA_HOME (Agent Runner uses that to select account one's
    // default namespace). PATH and HOME may still fall back to the provider
    // process for direct callers that send a partial environment. PATH is used
    // only to resolve the reviewed executable and is forwarded per invocation;
    // it is never part of the durable runtime identity.
    if !declared_env.is_empty() && !declared_env.contains_key("XDG_DATA_HOME") {
        environment.remove("XDG_DATA_HOME");
    }
    environment
}

fn ambient_environment() -> BTreeMap<String, String> {
    std::env::vars().collect()
}

fn runtime_identity_environment(
    environment: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    NATIVE_RUNTIME_IDENTITY_ENV_KEYS
        .iter()
        .filter_map(|key| {
            environment
                .get(*key)
                .map(|value| ((*key).to_string(), value.clone()))
        })
        .collect()
}

fn resolve_program(program: &str, env: &BTreeMap<String, String>) -> Option<PathBuf> {
    let path = Path::new(program);
    let candidate = if path.is_absolute() || path.components().count() > 1 {
        path.to_path_buf()
    } else {
        env.get("PATH")
            .map(std::ffi::OsStr::new)
            .into_iter()
            .flat_map(std::env::split_paths)
            .map(|directory| directory.join(program))
            .find(|candidate| candidate.is_file())?
    };
    let canonical = fs::canonicalize(candidate).ok()?;
    durable_fs::is_executable_file(&canonical)
        .ok()
        .is_some_and(|executable| executable)
        .then_some(canonical)
}

#[cfg(unix)]
fn native_program_stamp(path: &Path) -> std::io::Result<NativeProgramStamp> {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path)?;
    Ok(NativeProgramStamp {
        kind: "unix-metadata-v1".to_string(),
        byte_length: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

#[cfg(not(unix))]
fn native_program_stamp(path: &Path) -> std::io::Result<NativeProgramStamp> {
    use std::time::UNIX_EPOCH;

    let metadata = fs::metadata(path)?;
    let modified = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?;
    Ok(NativeProgramStamp {
        kind: "portable-metadata-v1".to_string(),
        byte_length: metadata.len(),
        device: 0,
        inode: 0,
        modified_seconds: modified.as_secs().min(i64::MAX as u64) as i64,
        modified_nanoseconds: i64::from(modified.subsec_nanos()),
        changed_seconds: modified.as_secs().min(i64::MAX as u64) as i64,
        changed_nanoseconds: i64::from(modified.subsec_nanos()),
    })
}

fn runtime_identity_sha256(
    account_wrapper: &str,
    program: &str,
    program_sha256: &str,
    execution_env: &BTreeMap<String, String>,
    native_contract_id: &str,
    fixed_args: &[String],
    implementation: (&str, &str),
) -> String {
    let (implementation_manifest_id, implementation_version) = implementation;
    sha256_hex(
        json!({
            "account_wrapper": account_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "execution_env": execution_env,
            "native_contract_id": native_contract_id,
            "fixed_args": fixed_args,
            "implementation_manifest_id": implementation_manifest_id,
            "implementation_version": implementation_version,
        })
        .to_string()
        .as_bytes(),
    )
}

fn validate_runtime_record(
    record: &NativeRuntimeRecord,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    validate_runtime_record_identity(record, account, request_id)?;
    if record.schema_version == NATIVE_RUNTIME_SCHEMA_VERSION {
        validate_current_runtime_implementation(record, account, request_id)
    } else {
        validate_predecessor_runtime_implementation(record, account, request_id).map(|_| ())
    }
}

fn validate_runtime_record_identity(
    record: &NativeRuntimeRecord,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let identity_sha256 = match record.schema_version {
        NATIVE_RUNTIME_SCHEMA_VERSION
        | PATH_BOUND_NATIVE_RUNTIME_SCHEMA_VERSION
        | FULL_ENV_NATIVE_RUNTIME_SCHEMA_VERSION => runtime_identity_sha256(
            &record.account_wrapper,
            &record.program,
            &record.program_sha256,
            &record.execution_env,
            &record.native_contract_id,
            &record.fixed_args,
            (
                &record.implementation_manifest_id,
                &record.implementation_version,
            ),
        ),
        MANIFEST_NATIVE_RUNTIME_SCHEMA_VERSION => manifest_runtime_identity_sha256(
            &record.account_wrapper,
            &record.program,
            &record.program_sha256,
            &record.execution_env,
            &record.native_contract_id,
            &record.fixed_args,
            (
                &record.implementation_manifest_id,
                &record.implementation_version,
            ),
        ),
        DIRECT_NATIVE_RUNTIME_SCHEMA_VERSION => direct_runtime_identity_sha256(
            &record.account_wrapper,
            &record.program,
            &record.program_sha256,
            &record.execution_env,
            &record.native_contract_id,
            &record.fixed_args,
        ),
        WRAPPER_NATIVE_RUNTIME_SCHEMA_VERSION => predecessor_runtime_identity_sha256(
            &record.account_wrapper,
            &record.program,
            &record.program_sha256,
            &record.execution_env,
        ),
        _ => String::new(),
    };
    if record.account_wrapper != account.opencode_wrapper
        || record.identity_sha256 != identity_sha256
        || (matches!(
            record.schema_version,
            NATIVE_RUNTIME_SCHEMA_VERSION
                | PATH_BOUND_NATIVE_RUNTIME_SCHEMA_VERSION
                | FULL_ENV_NATIVE_RUNTIME_SCHEMA_VERSION
        ) && (record.native_contract_id != OPENCODE_NATIVE_CONTRACT_ID
            || record.fixed_args
                != OPENCODE_NATIVE_FIXED_ARGS
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect::<Vec<_>>()
            || record.implementation_manifest_id.trim().is_empty()
            || record.implementation_version.trim().is_empty()
            || !record.program_stamp.is_complete()))
    {
        return Err(native_runtime_failure(
            request_id,
            "persisted native runtime identity is inconsistent",
        ));
    }
    Ok(())
}

fn manifest_runtime_identity_sha256(
    account_wrapper: &str,
    program: &str,
    program_sha256: &str,
    execution_env: &BTreeMap<String, String>,
    native_contract_id: &str,
    fixed_args: &[String],
    implementation: (&str, &str),
) -> String {
    let (implementation_manifest_id, implementation_version) = implementation;
    sha256_hex(
        json!({
            "account_wrapper": account_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "execution_env": execution_env,
            "native_contract_id": native_contract_id,
            "fixed_args": fixed_args,
            "implementation_manifest_id": implementation_manifest_id,
            "implementation_version": implementation_version,
        })
        .to_string()
        .as_bytes(),
    )
}

fn direct_runtime_identity_sha256(
    account_wrapper: &str,
    program: &str,
    program_sha256: &str,
    execution_env: &BTreeMap<String, String>,
    native_contract_id: &str,
    fixed_args: &[String],
) -> String {
    sha256_hex(
        json!({
            "account_wrapper": account_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "execution_env": execution_env,
            "native_contract_id": native_contract_id,
            "fixed_args": fixed_args,
        })
        .to_string()
        .as_bytes(),
    )
}

fn predecessor_runtime_identity_sha256(
    account_wrapper: &str,
    program: &str,
    program_sha256: &str,
    execution_env: &BTreeMap<String, String>,
) -> String {
    sha256_hex(
        json!({
            "account_wrapper": account_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "execution_env": execution_env,
        })
        .to_string()
        .as_bytes(),
    )
}

fn activate_runtime_record(
    host: &HostContext,
    account: &AccountProfile,
    record: NativeRuntimeRecord,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    let activated = preview_runtime_record_activation(account, record.clone(), request_id)?;
    if activated.record != record {
        write_runtime_context(host, account, &activated, request_id)?;
    }
    Ok(activated)
}

fn preview_runtime_record_activation(
    account: &AccountProfile,
    record: NativeRuntimeRecord,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    validate_runtime_record_identity(&record, account, request_id)?;
    if record.schema_version == NATIVE_RUNTIME_SCHEMA_VERSION {
        return match validate_current_runtime_implementation(&record, account, request_id) {
            Ok(()) => Ok(NativeRuntimeContext { record }),
            Err(_) => preview_approved_same_path_replacement(account, &record, request_id),
        };
    }
    if matches!(
        record.schema_version,
        PATH_BOUND_NATIVE_RUNTIME_SCHEMA_VERSION | FULL_ENV_NATIVE_RUNTIME_SCHEMA_VERSION
    ) {
        validate_current_runtime_implementation(&record, account, request_id)?;
        let execution_env = native_execution_environment(
            account,
            runtime_identity_environment(&record.execution_env),
            request_id,
        )?;
        let identity_sha256 = runtime_identity_sha256(
            &record.account_wrapper,
            &record.program,
            &record.program_sha256,
            &execution_env,
            &record.native_contract_id,
            &record.fixed_args,
            (
                &record.implementation_manifest_id,
                &record.implementation_version,
            ),
        );
        return Ok(NativeRuntimeContext {
            record: NativeRuntimeRecord {
                schema_version: NATIVE_RUNTIME_SCHEMA_VERSION,
                execution_env,
                identity_sha256,
                ..record
            },
        });
    }
    if record.schema_version == WRAPPER_NATIVE_RUNTIME_SCHEMA_VERSION {
        validate_predecessor_runtime_implementation(&record, account, request_id)?;
        return candidate_context(account, record.execution_env, request_id);
    }
    let admitted = validate_predecessor_runtime_implementation(&record, account, request_id)?;
    let selected_program = resolve_program(OPENCODE_NATIVE_PROGRAM, &record.execution_env)
        .ok_or_else(|| {
            native_runtime_failure(
                request_id,
                "the selected direct OpenCode implementation is unavailable during runtime identity upgrade",
            )
        })?;
    if selected_program != Path::new(&record.program) {
        return Err(ProviderFailure::conflict(
            request_id,
            "native_runtime_context_conflict",
            "the selected direct OpenCode implementation changed before runtime identity upgrade",
            json!({
                "account": account.opencode_wrapper,
                "recorded_program": record.program,
                "selected_program": selected_program,
            }),
        ));
    }
    let approved = admitted.approved.ok_or_else(|| {
        native_runtime_failure(
            request_id,
            "a direct predecessor runtime has no reviewed implementation identity",
        )
    })?;
    let fixed_args = OPENCODE_NATIVE_FIXED_ARGS
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    let execution_env = native_execution_environment(
        account,
        runtime_identity_environment(&record.execution_env),
        request_id,
    )?;
    let identity_sha256 = runtime_identity_sha256(
        account.opencode_wrapper,
        &record.program,
        &record.program_sha256,
        &execution_env,
        OPENCODE_NATIVE_CONTRACT_ID,
        &fixed_args,
        (&approved.id, &approved.version),
    );
    Ok(NativeRuntimeContext {
        record: NativeRuntimeRecord {
            schema_version: NATIVE_RUNTIME_SCHEMA_VERSION,
            account_wrapper: account.opencode_wrapper.to_string(),
            program: record.program,
            program_sha256: record.program_sha256,
            execution_env,
            native_contract_id: OPENCODE_NATIVE_CONTRACT_ID.to_string(),
            fixed_args,
            implementation_manifest_id: approved.id,
            implementation_version: approved.version,
            program_stamp: admitted.program_stamp,
            identity_sha256,
        },
    })
}

fn preview_approved_same_path_replacement(
    account: &AccountProfile,
    record: &NativeRuntimeRecord,
    request_id: &str,
) -> Result<NativeRuntimeContext, ProviderFailure> {
    let prior_bytes = usize::try_from(record.program_stamp.byte_length).map_err(|_| {
        native_runtime_failure(
            request_id,
            "persisted native runtime byte length cannot be represented on this platform",
        )
    })?;
    let prior = recorded_implementation(
        &record.program_sha256,
        prior_bytes,
        &record.implementation_manifest_id,
        &record.implementation_version,
        &record.native_contract_id,
    )
    .map_err(|error| native_runtime_failure(request_id, error))?
    .ok_or_else(|| {
        ProviderFailure::conflict(
            request_id,
            "native_runtime_prior_implementation_unapproved",
            format!(
                "account {} cannot advance from a persisted native implementation outside the reviewed manifest and admitted auto-update lineage",
                account.opencode_wrapper
            ),
            json!({
                "account": account.opencode_wrapper,
                "program": record.program,
                "persisted_program_sha256": record.program_sha256,
                "persisted_implementation_manifest_id": record.implementation_manifest_id,
                "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
            }),
        )
    })?;
    if prior.semantic_contract != OPENCODE_NATIVE_CONTRACT_ID {
        return Err(native_runtime_failure(
            request_id,
            "persisted native runtime has the wrong semantic contract",
        ));
    }
    let mut resolution_env = ambient_environment();
    resolution_env.extend(record.execution_env.clone());
    let execution_env = native_execution_environment(
        account,
        runtime_identity_environment(&resolution_env),
        request_id,
    )?;
    admit_candidate_program(
        account,
        PathBuf::from(&record.program),
        execution_env,
        Some(record),
        request_id,
    )
}

struct PredecessorImplementationAdmission {
    program_stamp: NativeProgramStamp,
    approved: Option<native_implementation_manifest::ApprovedImplementation>,
}

fn validate_predecessor_runtime_implementation(
    record: &NativeRuntimeRecord,
    account: &AccountProfile,
    request_id: &str,
) -> Result<PredecessorImplementationAdmission, ProviderFailure> {
    if !durable_fs::is_executable_file(Path::new(&record.program))
        .map_err(|error| native_runtime_failure(request_id, error))?
    {
        return Err(native_runtime_failure(
            request_id,
            format!(
                "bound native OpenCode implementation for account {} is no longer executable",
                account.opencode_wrapper
            ),
        ));
    }
    let program = Path::new(&record.program);
    let program_stamp =
        native_program_stamp(program).map_err(|error| native_runtime_failure(request_id, error))?;
    let (program_sha256, program_bytes) =
        durable_fs::sha256_file_bounded(program, durable_fs::MAX_BOUND_EXECUTABLE_BYTES)
            .map_err(|error| native_runtime_failure(request_id, error))?;
    let observed_stamp =
        native_program_stamp(program).map_err(|error| native_runtime_failure(request_id, error))?;
    if program_sha256 != record.program_sha256
        || observed_stamp != program_stamp
        || observed_stamp.byte_length != program_bytes as u64
    {
        return Err(ProviderFailure::conflict(
            request_id,
            "native_runtime_implementation_changed",
            format!(
                "bound native OpenCode implementation for account {} changed after admission",
                account.opencode_wrapper
            ),
            json!({
                "account": account.opencode_wrapper,
                "runtime_identity_sha256": record.identity_sha256,
                "program": record.program,
            }),
        ));
    }
    if record.schema_version == WRAPPER_NATIVE_RUNTIME_SCHEMA_VERSION {
        // The predecessor wrapper is retained only as authenticated transition
        // evidence. It is never executed by this provider; candidate_context
        // must independently admit the reviewed direct implementation before
        // activate_runtime_record publishes the schema-v6 successor.
        return Ok(PredecessorImplementationAdmission {
            program_stamp,
            approved: None,
        });
    }
    let approved = native_implementation_manifest::approved_implementation(
        OPENCODE_NATIVE_PROGRAM,
        &record.program_sha256,
        program_bytes,
    )
    .map_err(|error| native_runtime_failure(request_id, error))?
    .ok_or_else(|| {
        ProviderFailure::conflict(
            request_id,
            "native_runtime_implementation_unapproved",
            format!(
                "bound native OpenCode implementation for account {} is no longer approved",
                account.opencode_wrapper
            ),
            json!({
                "account": account.opencode_wrapper,
                "runtime_identity_sha256": record.identity_sha256,
                "program": record.program,
                "program_sha256": record.program_sha256,
                "program_bytes": program_bytes,
                "manifest_contract": native_implementation_manifest::MANIFEST_CONTRACT,
            }),
        )
    })?;
    if approved.semantic_contract != OPENCODE_NATIVE_CONTRACT_ID {
        return Err(native_runtime_failure(
            request_id,
            "the reviewed OpenCode implementation has the wrong semantic contract",
        ));
    }
    if matches!(
        record.schema_version,
        NATIVE_RUNTIME_SCHEMA_VERSION
            | PATH_BOUND_NATIVE_RUNTIME_SCHEMA_VERSION
            | FULL_ENV_NATIVE_RUNTIME_SCHEMA_VERSION
    ) && (record.implementation_manifest_id != approved.id
        || record.implementation_version != approved.version)
    {
        return Err(native_runtime_failure(
            request_id,
            "persisted native runtime manifest identity is inconsistent",
        ));
    }
    Ok(PredecessorImplementationAdmission {
        program_stamp,
        approved: Some(approved),
    })
}

fn validate_current_runtime_implementation(
    record: &NativeRuntimeRecord,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    validate_pinned_program(
        Path::new(&record.program),
        &record.program_sha256,
        &record.implementation_manifest_id,
        &record.implementation_version,
        &record.native_contract_id,
        &record.program_stamp,
    )
    .map_err(|error| {
        ProviderFailure::conflict(
            request_id,
            "native_runtime_implementation_changed",
            format!(
                "bound native OpenCode implementation for account {} is unavailable: {error}",
                account.opencode_wrapper
            ),
            json!({
                "account": account.opencode_wrapper,
                "runtime_identity_sha256": record.identity_sha256,
                "program": record.program,
            }),
        )
    })
}

fn recorded_implementation(
    program_sha256: &str,
    program_bytes: usize,
    implementation_id: &str,
    implementation_version: &str,
    native_contract_id: &str,
) -> Result<Option<native_implementation_manifest::ApprovedImplementation>, String> {
    let implementation = native_implementation_manifest::approved_implementation(
        OPENCODE_NATIVE_PROGRAM,
        program_sha256,
        program_bytes,
    )?
    .or_else(|| {
        native_implementation_manifest::auto_update_implementation(
            implementation_version,
            program_sha256,
            program_bytes,
        )
    });
    Ok(implementation.filter(|implementation| {
        implementation.id == implementation_id
            && implementation.version == implementation_version
            && implementation.semantic_contract == native_contract_id
            && native_contract_id == OPENCODE_NATIVE_CONTRACT_ID
    }))
}

pub(crate) fn validate_pinned_program(
    program: &Path,
    program_sha256: &str,
    implementation_manifest_id: &str,
    implementation_version: &str,
    native_contract_id: &str,
    expected_stamp: &NativeProgramStamp,
) -> Result<(), String> {
    if !durable_fs::is_executable_file(program).map_err(|error| error.to_string())? {
        return Err("the implementation is not an executable regular file".to_string());
    }
    let observed_stamp = native_program_stamp(program).map_err(|error| error.to_string())?;
    if &observed_stamp != expected_stamp {
        return Err("the implementation metadata stamp changed after admission".to_string());
    }
    let approved = recorded_implementation(
        program_sha256,
        observed_stamp.byte_length as usize,
        implementation_manifest_id,
        implementation_version,
        native_contract_id,
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "the implementation identity is not an admitted runtime lineage".to_string())?;
    let _ = approved;
    Ok(())
}

pub(crate) fn validate_predecessor_pinned_program(
    program: &Path,
    program_sha256: &str,
    implementation_manifest_id: &str,
    implementation_version: &str,
    native_contract_id: &str,
) -> Result<(), String> {
    if !durable_fs::is_executable_file(program).map_err(|error| error.to_string())? {
        return Err("the implementation is not an executable regular file".to_string());
    }
    let stamp = native_program_stamp(program).map_err(|error| error.to_string())?;
    let (observed_sha256, observed_bytes) =
        durable_fs::sha256_file_bounded(program, durable_fs::MAX_BOUND_EXECUTABLE_BYTES)
            .map_err(|error| error.to_string())?;
    let observed_stamp = native_program_stamp(program).map_err(|error| error.to_string())?;
    if observed_sha256 != program_sha256
        || observed_stamp != stamp
        || observed_stamp.byte_length != observed_bytes as u64
    {
        return Err("the implementation content changed after launch admission".to_string());
    }
    let approved = recorded_implementation(
        program_sha256,
        observed_bytes,
        implementation_manifest_id,
        implementation_version,
        native_contract_id,
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "the implementation identity is not an admitted runtime lineage".to_string())?;
    let _ = approved;
    Ok(())
}

fn acquire_runtime_lock(
    host: &HostContext,
    account: &AccountProfile,
    timeout: Duration,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let root = runtime_state_root(host, request_id)?;
    durable_fs::create_private_directories(&root)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    let lock_path = confined_runtime_target(
        host,
        &root.join(format!("{}.lock", account.opencode_wrapper)),
        request_id,
    )?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = options
        .open(lock_path)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    if !operation_bounds::lock_exclusive_for(&lock, timeout)
        .map_err(|error| native_runtime_failure(request_id, error))?
    {
        return Err(native_runtime_lock_timeout(request_id));
    }
    Ok(lock)
}

fn runtime_lock_timeout(host: &HostContext, request_id: &str) -> Result<Duration, ProviderFailure> {
    operation_bounds::remaining_timeout(host.deadline_unix_ms, NATIVE_RUNTIME_LOCK_TIMEOUT)
        .ok_or_else(|| native_runtime_lock_timeout(request_id))
}

fn read_runtime_record(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<NativeRuntimeRecord>, ProviderFailure> {
    let path = runtime_context_path(host, account, request_id)?;
    let bytes = match durable_fs::read_file_bounded(&path, MAX_NATIVE_RUNTIME_STATE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(native_runtime_failure(request_id, error)),
    };
    serde_json::from_slice::<NativeRuntimeRecord>(&bytes)
        .map(Some)
        .map_err(|error| native_runtime_failure(request_id, error))
}

fn write_runtime_context(
    host: &HostContext,
    account: &AccountProfile,
    context: &NativeRuntimeContext,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let path = runtime_context_path(host, account, request_id)?;
    let parent = path
        .parent()
        .expect("native runtime context path always has a parent");
    durable_fs::create_private_directories(parent)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    let bytes = serde_json::to_vec_pretty(&context.record)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    if bytes.len() > MAX_NATIVE_RUNTIME_STATE_BYTES {
        return Err(native_runtime_state_capacity(request_id, bytes.len()));
    }
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    temporary
        .write_all(&bytes)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(|error| native_runtime_failure(request_id, error))?;
    temporary
        .persist(&path)
        .map_err(|error| native_runtime_failure(request_id, error.error))?;
    durable_fs::sync_directory(parent).map_err(|error| native_runtime_failure(request_id, error))
}

fn runtime_context_path(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let root = runtime_state_root(host, request_id)?;
    confined_runtime_target(
        host,
        &root.join(format!("{}.json", account.opencode_wrapper)),
        request_id,
    )
}

fn runtime_state_root(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let data_root = host
        .data_root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "native_runtime_data_root_missing",
                "native OpenCode operations require host.data_root for durable runtime identity",
            )
        })?;
    confined_runtime_target(host, &data_root.join(NATIVE_RUNTIME_STATE_DIR), request_id)
}

fn confined_runtime_target(
    host: &HostContext,
    target: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = host.data_root.as_deref().map(Path::new).ok_or_else(|| {
        native_runtime_failure(request_id, "native runtime data root is unavailable")
    })?;
    path_guard::confined_target(data_root, target)
        .map_err(|error| native_runtime_failure(request_id, error))
}

fn native_runtime_failure(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "native_runtime_state_failed",
        format!("native runtime identity failed: {error}"),
    )
}

fn native_runtime_lock_timeout(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "native_runtime_lock_timeout",
        "native runtime identity lock could not be acquired before the operation deadline",
    )
}

fn native_runtime_state_capacity(request_id: &str, observed_bytes: usize) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "native_runtime_state_capacity_exceeded",
        format!(
            "native runtime identity state is {observed_bytes} bytes; the supported maximum is {MAX_NATIVE_RUNTIME_STATE_BYTES} bytes"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        admit_forward_auto_update, native_execution_environment, runtime_identity_environment,
        NativeProgramStamp, NativeRuntimeRecord, OPENCODE_BASH_DEFAULT_TIMEOUT_ENV,
        OPENCODE_NATIVE_CONTRACT_ID, PROVIDER_BASH_DEFAULT_TIMEOUT_MS,
    };
    use crate::account::ACCOUNTS;
    use std::collections::BTreeMap;

    #[cfg(unix)]
    #[test]
    fn forward_auto_update_uses_bounded_version_probe_and_exact_digest_identity() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("create auto-update probe directory");
        let program = directory.path().join("opencode");
        std::fs::write(&program, "#!/bin/sh\nprintf '1.18.23\\n'\n")
            .expect("write auto-update version probe");
        let mut permissions = std::fs::metadata(&program)
            .expect("auto-update probe metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&program, permissions).expect("chmod auto-update probe");
        let prior = NativeRuntimeRecord {
            schema_version: 6,
            account_wrapper: "opencode1".to_string(),
            program: program.to_string_lossy().into_owned(),
            program_sha256: "cd".repeat(32),
            execution_env: BTreeMap::new(),
            native_contract_id: OPENCODE_NATIVE_CONTRACT_ID.to_string(),
            fixed_args: vec!["--pure".to_string()],
            implementation_manifest_id: "opencode-1.18.22-test".to_string(),
            implementation_version: "1.18.22".to_string(),
            program_stamp: NativeProgramStamp::default(),
            identity_sha256: "prior".to_string(),
        };
        let digest = "ab".repeat(32);

        let admitted = admit_forward_auto_update(
            &ACCOUNTS[0],
            &program,
            &BTreeMap::new(),
            &digest,
            42,
            &prior,
            "req-forward-auto-update",
        )
        .expect("admit forward auto-update");

        assert_eq!(admitted.version, "1.18.23");
        assert_eq!(admitted.id, "opencode-auto-update-1.18.23-abababab");
    }

    #[test]
    fn provider_owns_a_finite_long_running_bash_timeout_fallback() {
        let environment = native_execution_environment(
            &ACCOUNTS[0],
            BTreeMap::new(),
            "req-provider-bash-timeout-fallback",
        )
        .expect("construct account-one native environment");

        assert_eq!(
            environment
                .get(OPENCODE_BASH_DEFAULT_TIMEOUT_ENV)
                .map(String::as_str),
            Some(PROVIDER_BASH_DEFAULT_TIMEOUT_MS)
        );
    }

    #[test]
    fn explicit_host_bash_timeout_overrides_the_provider_fallback() {
        let explicit_timeout = "45000";
        let environment = native_execution_environment(
            &ACCOUNTS[0],
            BTreeMap::from([(
                OPENCODE_BASH_DEFAULT_TIMEOUT_ENV.to_string(),
                explicit_timeout.to_string(),
            )]),
            "req-explicit-bash-timeout",
        )
        .expect("construct account-one native environment");

        assert_eq!(
            environment
                .get(OPENCODE_BASH_DEFAULT_TIMEOUT_ENV)
                .map(String::as_str),
            Some(explicit_timeout)
        );
    }

    #[test]
    fn runtime_identity_excludes_per_invocation_environment_and_path() {
        let environment = BTreeMap::from([
            ("HOME".to_string(), "/tmp/home".to_string()),
            ("PATH".to_string(), "/tmp/bin".to_string()),
            (
                "PER_INVOCATION_ENV".to_string(),
                "forward-without-binding".to_string(),
            ),
        ]);

        let identity_environment = runtime_identity_environment(&environment);

        assert_eq!(identity_environment.get("HOME"), environment.get("HOME"));
        assert!(!identity_environment.contains_key("PATH"));
        assert!(!identity_environment.contains_key("PER_INVOCATION_ENV"));
    }
}
