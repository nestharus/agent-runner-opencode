//! Declared roles: resolver, validator, accessor
//! intrinsic_surface_declarations:
//!   - component: src/quota_observer.rs
//!     role: intrinsic-surface
//!     Domain: durable ChatGPT quota observer identity
//!     Owns:
//!       - one content-addressed curl implementation per declared account
//!       - the stable environment and fixed WHAM invocation contract
//!       - durable reuse and implementation-change rejection across probes

use crate::account::AccountProfile;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
use crate::path_guard;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

const QUOTA_OBSERVER_STATE_DIR: &str = "provider-state/opencode/quota-observers";
const QUOTA_OBSERVER_SCHEMA_VERSION: u32 = 1;
const QUOTA_OBSERVER_CONTRACT: &str = "chatgpt_wham_curl/v1";
const OBSERVER_ENV_KEYS: &[&str] = &["PATH", "HOME"];

#[derive(Clone, Deserialize, Serialize)]
pub struct QuotaObserverContext {
    schema_version: u32,
    observer_contract: String,
    account_wrapper: String,
    program: String,
    program_sha256: String,
    execution_env: BTreeMap<String, String>,
    identity_sha256: String,
}

pub fn resolve(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<QuotaObserverContext, ProviderFailure> {
    let _lock = acquire_observer_lock(host, account, request_id)?;
    if let Some(context) = read_observer_context(host, account, request_id)? {
        validate_observer_context(&context, account, request_id)?;
        return Ok(context);
    }
    let context = candidate_context(account, request_id)?;
    write_observer_context(host, account, &context, request_id)?;
    Ok(context)
}

impl QuotaObserverContext {
    pub fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.env_clear().envs(&self.execution_env);
        command
    }

    pub fn identity_sha256(&self) -> &str {
        &self.identity_sha256
    }
}

fn candidate_context(
    account: &AccountProfile,
    request_id: &str,
) -> Result<QuotaObserverContext, ProviderFailure> {
    let execution_env = OBSERVER_ENV_KEYS
        .iter()
        .filter_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| ((*key).to_string(), value))
        })
        .collect::<BTreeMap<_, _>>();
    let program = resolve_program("curl", &execution_env).ok_or_else(|| {
        quota_observer_failure(
            request_id,
            "curl was not found in the selected quota-observer PATH",
        )
    })?;
    let program_bytes =
        fs::read(&program).map_err(|error| quota_observer_failure(request_id, error))?;
    let program = program.to_str().ok_or_else(|| {
        quota_observer_failure(request_id, "quota observer path is not valid UTF-8")
    })?;
    let program_sha256 = sha256_hex(&program_bytes);
    let identity_sha256 = observer_identity_sha256(
        account.opencode_wrapper,
        program,
        &program_sha256,
        &execution_env,
    );
    Ok(QuotaObserverContext {
        schema_version: QUOTA_OBSERVER_SCHEMA_VERSION,
        observer_contract: QUOTA_OBSERVER_CONTRACT.to_string(),
        account_wrapper: account.opencode_wrapper.to_string(),
        program: program.to_string(),
        program_sha256,
        execution_env,
        identity_sha256,
    })
}

fn resolve_program(program: &str, environment: &BTreeMap<String, String>) -> Option<PathBuf> {
    environment
        .get("PATH")
        .map(std::ffi::OsStr::new)
        .into_iter()
        .flat_map(std::env::split_paths)
        .map(|directory| directory.join(program))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| fs::canonicalize(candidate).ok())
}

fn observer_identity_sha256(
    account_wrapper: &str,
    program: &str,
    program_sha256: &str,
    execution_env: &BTreeMap<String, String>,
) -> String {
    sha256_hex(
        json!({
            "observer_contract": QUOTA_OBSERVER_CONTRACT,
            "account_wrapper": account_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "execution_env": execution_env,
        })
        .to_string()
        .as_bytes(),
    )
}

fn validate_observer_context(
    context: &QuotaObserverContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let identity_sha256 = observer_identity_sha256(
        &context.account_wrapper,
        &context.program,
        &context.program_sha256,
        &context.execution_env,
    );
    if context.schema_version != QUOTA_OBSERVER_SCHEMA_VERSION
        || context.observer_contract != QUOTA_OBSERVER_CONTRACT
        || context.account_wrapper != account.opencode_wrapper
        || context.identity_sha256 != identity_sha256
    {
        return Err(quota_observer_failure(
            request_id,
            "persisted quota observer identity is inconsistent",
        ));
    }
    let bytes =
        fs::read(&context.program).map_err(|error| quota_observer_failure(request_id, error))?;
    if sha256_hex(&bytes) != context.program_sha256 {
        return Err(ProviderFailure::conflict(
            request_id,
            "quota_observer_implementation_changed",
            format!(
                "bound quota observer for account {} changed after admission",
                account.opencode_wrapper
            ),
            json!({
                "account": account.opencode_wrapper,
                "quota_observer_identity_sha256": context.identity_sha256,
                "program": context.program,
            }),
        ));
    }
    Ok(())
}

fn acquire_observer_lock(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let root = observer_state_root(host, request_id)?;
    durable_fs::create_private_directories(&root)
        .map_err(|error| quota_observer_failure(request_id, error))?;
    let lock_path = confined_observer_target(
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
        .map_err(|error| quota_observer_failure(request_id, error))?;
    lock.lock_exclusive()
        .map_err(|error| quota_observer_failure(request_id, error))?;
    Ok(lock)
}

fn read_observer_context(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<QuotaObserverContext>, ProviderFailure> {
    let path = observer_context_path(host, account, request_id)?;
    let bytes = match durable_fs::read_file(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(quota_observer_failure(request_id, error)),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| quota_observer_failure(request_id, error))
}

fn write_observer_context(
    host: &HostContext,
    account: &AccountProfile,
    context: &QuotaObserverContext,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let path = observer_context_path(host, account, request_id)?;
    let parent = path
        .parent()
        .expect("quota observer context path always has a parent");
    durable_fs::create_private_directories(parent)
        .map_err(|error| quota_observer_failure(request_id, error))?;
    let bytes = serde_json::to_vec_pretty(context)
        .map_err(|error| quota_observer_failure(request_id, error))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| quota_observer_failure(request_id, error))?;
    temporary
        .write_all(&bytes)
        .map_err(|error| quota_observer_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(|error| quota_observer_failure(request_id, error))?;
    temporary
        .persist(&path)
        .map_err(|error| quota_observer_failure(request_id, error.error))?;
    durable_fs::sync_directory(parent).map_err(|error| quota_observer_failure(request_id, error))
}

fn observer_context_path(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let root = observer_state_root(host, request_id)?;
    confined_observer_target(
        host,
        &root.join(format!("{}.json", account.opencode_wrapper)),
        request_id,
    )
}

fn observer_state_root(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let data_root = host
        .data_root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "quota_observer_data_root_missing",
                "quota observation requires host.data_root for durable implementation identity",
            )
        })?;
    confined_observer_target(host, &data_root.join(QUOTA_OBSERVER_STATE_DIR), request_id)
}

fn confined_observer_target(
    host: &HostContext,
    target: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = host.data_root.as_deref().map(Path::new).ok_or_else(|| {
        quota_observer_failure(request_id, "quota observer data root is unavailable")
    })?;
    path_guard::confined_target(data_root, target)
        .map_err(|error| quota_observer_failure(request_id, error))
}

fn quota_observer_failure(request_id: &str, error: impl std::fmt::Display) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "quota_observer_state_failed",
        format!("quota observer identity failed: {error}"),
    )
}
