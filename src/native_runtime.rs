//! Declared roles: resolver, validator, accessor
//! intrinsic_surface_declarations:
//!   - component: src/native_runtime.rs
//!     role: intrinsic-surface
//!     Domain: durable OpenCode native runtime identity
//!     Owns:
//!       - one executable and stable execution-environment binding per declared account
//!       - executable content validation before every native operation
//!       - exact launch-context admission against the durable account binding

use crate::account::AccountProfile;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use crate::operation_bounds;
use crate::path_guard;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

const NATIVE_RUNTIME_STATE_DIR: &str = "provider-state/opencode/native-runtimes";
const NATIVE_RUNTIME_SCHEMA_VERSION: u32 = 1;
const NATIVE_RUNTIME_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_NATIVE_RUNTIME_STATE_BYTES: usize = 1024 * 1024;
const STABLE_AMBIENT_ENV_KEYS: &[&str] = &[
    "PATH",
    "HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "XDG_CACHE_HOME",
    "OPENCODE_CONFIG",
    "OPENCODE_CONFIG_DIR",
    "OULIPOLY_DATA_DIR",
    "AGENT_BASH_AGENT_RUNNER_BIN",
];
const TRANSIENT_ENV_KEYS: &[&str] = &[
    "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
    "AGENT_RUNNER_OPENCODE_WRAPPER_LOG",
    "OULIPOLY_PARENT_INVOCATION",
];

#[derive(Clone, Deserialize, Serialize)]
pub struct NativeRuntimeContext {
    schema_version: u32,
    account_wrapper: String,
    program: String,
    program_sha256: String,
    execution_env: BTreeMap<String, String>,
    identity_sha256: String,
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
    if let Some(context) = read_runtime_context(host, account, request_id)? {
        validate_runtime_context(&context, account, request_id)?;
        return Ok(context);
    }
    let context = candidate_context(account, ambient_stable_environment(), request_id)?;
    write_runtime_context(host, account, &context, request_id)?;
    Ok(context)
}

pub fn resolve_existing_for_account(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<NativeRuntimeContext>, ProviderFailure> {
    let Some(context) = read_runtime_context(host, account, request_id)? else {
        return Ok(None);
    };
    validate_runtime_context(&context, account, request_id)?;
    Ok(Some(context))
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
    let context = serde_json::from_slice(&bytes)
        .map_err(|error| native_runtime_failure(request_id, error))?;
    validate_runtime_record_identity(&context, account, request_id)?;
    if require_current_implementation {
        validate_runtime_implementation(&context, account, request_id)?;
    }
    Ok(Some((context.identity_sha256, sha256_hex(&bytes))))
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
    let candidate =
        candidate_context(account, stable_launch_environment(declared_env), request_id)?;
    let timeout =
        operation_bounds::remaining_timeout(host.deadline_unix_ms, NATIVE_RUNTIME_LOCK_TIMEOUT)
            .unwrap_or(Duration::ZERO);
    let _lock = acquire_runtime_lock(host, account, timeout, request_id)?;
    if let Some(context) = read_runtime_context(host, account, request_id)? {
        validate_runtime_context(&context, account, request_id)?;
        if context.identity_sha256 != candidate.identity_sha256 {
            return Err(ProviderFailure::conflict(
                request_id,
                "native_runtime_context_conflict",
                format!(
                    "account {} is already bound to a different native executable or state environment",
                    account.opencode_wrapper
                ),
                json!({
                    "account": account.opencode_wrapper,
                    "attempted_runtime_identity_sha256": candidate.identity_sha256,
                    "bound_runtime_identity_sha256": context.identity_sha256,
                }),
            ));
        }
        return Ok(context);
    }
    write_runtime_context(host, account, &candidate, request_id)?;
    Ok(candidate)
}

impl NativeRuntimeContext {
    pub fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command
            .env_clear()
            .envs(self.execution_environment(&BTreeMap::new()));
        command
    }

    pub fn execution_environment(
        &self,
        declared_env: &BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        let mut environment = self.execution_env.clone();
        for key in TRANSIENT_ENV_KEYS {
            if let Some(value) = declared_env
                .get(*key)
                .cloned()
                .or_else(|| std::env::var(key).ok())
            {
                environment.insert((*key).to_string(), value);
            }
        }
        environment
    }

    pub fn program(&self) -> &str {
        &self.program
    }

    pub fn stable_execution_env(&self) -> &BTreeMap<String, String> {
        &self.execution_env
    }

    pub fn identity_sha256(&self) -> &str {
        &self.identity_sha256
    }

    pub fn expand_path(&self, path: &str) -> PathBuf {
        match (path.strip_prefix("~/"), self.execution_env.get("HOME")) {
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
    let program = resolve_program(account.opencode_wrapper, &execution_env).ok_or_else(|| {
        native_runtime_failure(
            request_id,
            format!(
                "canonical wrapper {} was not found in the selected native PATH",
                account.opencode_wrapper
            ),
        )
    })?;
    if !durable_fs::is_executable_file(&program)
        .map_err(|error| native_runtime_failure(request_id, error))?
    {
        return Err(native_runtime_failure(
            request_id,
            format!(
                "canonical wrapper {} is not an executable regular file",
                account.opencode_wrapper
            ),
        ));
    }
    let program_bytes =
        durable_fs::read_file_bounded(&program, durable_fs::MAX_BOUND_EXECUTABLE_BYTES)
            .map_err(|error| native_runtime_failure(request_id, error))?;
    let program = program.to_str().ok_or_else(|| {
        native_runtime_failure(request_id, "native wrapper path is not valid UTF-8")
    })?;
    let program_sha256 = sha256_hex(&program_bytes);
    let identity_sha256 = runtime_identity_sha256(
        account.opencode_wrapper,
        program,
        &program_sha256,
        &execution_env,
    );
    Ok(NativeRuntimeContext {
        schema_version: NATIVE_RUNTIME_SCHEMA_VERSION,
        account_wrapper: account.opencode_wrapper.to_string(),
        program: program.to_string(),
        program_sha256,
        execution_env,
        identity_sha256,
    })
}

pub(crate) fn ambient_stable_environment() -> BTreeMap<String, String> {
    STABLE_AMBIENT_ENV_KEYS
        .iter()
        .filter_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| ((*key).to_string(), value))
        })
        .collect()
}

pub(crate) fn ambient_launch_environment() -> BTreeMap<String, String> {
    let mut environment = ambient_stable_environment();
    for key in TRANSIENT_ENV_KEYS {
        if let Ok(value) = std::env::var(key) {
            environment.insert((*key).to_string(), value);
        }
    }
    environment
}

fn stable_launch_environment(declared_env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut environment = ambient_stable_environment();
    environment.extend(
        declared_env
            .iter()
            .filter(|(key, _)| !TRANSIENT_ENV_KEYS.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    environment
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

fn runtime_identity_sha256(
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

fn validate_runtime_context(
    context: &NativeRuntimeContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    validate_runtime_record_identity(context, account, request_id)?;
    validate_runtime_implementation(context, account, request_id)
}

fn validate_runtime_record_identity(
    context: &NativeRuntimeContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let identity_sha256 = runtime_identity_sha256(
        &context.account_wrapper,
        &context.program,
        &context.program_sha256,
        &context.execution_env,
    );
    if context.schema_version != NATIVE_RUNTIME_SCHEMA_VERSION
        || context.account_wrapper != account.opencode_wrapper
        || context.identity_sha256 != identity_sha256
    {
        return Err(native_runtime_failure(
            request_id,
            "persisted native runtime identity is inconsistent",
        ));
    }
    Ok(())
}

fn validate_runtime_implementation(
    context: &NativeRuntimeContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if !durable_fs::is_executable_file(Path::new(&context.program))
        .map_err(|error| native_runtime_failure(request_id, error))?
    {
        return Err(native_runtime_failure(
            request_id,
            format!(
                "bound native wrapper for account {} is no longer executable",
                account.opencode_wrapper
            ),
        ));
    }
    let bytes = durable_fs::read_file_bounded(
        Path::new(&context.program),
        durable_fs::MAX_BOUND_EXECUTABLE_BYTES,
    )
    .map_err(|error| native_runtime_failure(request_id, error))?;
    if sha256_hex(&bytes) != context.program_sha256 {
        return Err(ProviderFailure::conflict(
            request_id,
            "native_runtime_implementation_changed",
            format!(
                "bound native wrapper for account {} changed after admission",
                account.opencode_wrapper
            ),
            json!({
                "account": account.opencode_wrapper,
                "runtime_identity_sha256": context.identity_sha256,
                "program": context.program,
            }),
        ));
    }
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

fn read_runtime_context(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<NativeRuntimeContext>, ProviderFailure> {
    let path = runtime_context_path(host, account, request_id)?;
    let bytes = match durable_fs::read_file_bounded(&path, MAX_NATIVE_RUNTIME_STATE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(native_runtime_failure(request_id, error)),
    };
    serde_json::from_slice(&bytes)
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
    let bytes = serde_json::to_vec_pretty(context)
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
