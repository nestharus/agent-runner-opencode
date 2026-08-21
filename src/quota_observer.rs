//! Declared roles: resolver, validator, accessor
//! intrinsic_surface_declarations:
//!   - component: src/quota_observer.rs
//!     role: intrinsic-surface
//!     Domain: durable ChatGPT quota observer identity
//!     Owns:
//!       - one source-included in-process WHAM transport identity per declared account
//!       - bounded predecessor external-transport upgrade before a production probe
//!       - durable reuse and provider-build identity validation across probes

use crate::account::AccountProfile;
use crate::durable_fs;
use crate::encoding::sha256_hex;
use crate::envelope::{HostContext, ProviderFailure};
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
use crate::native_implementation_manifest;
use crate::operation_bounds;
use crate::path_guard;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
use std::process::Command;
use std::time::Duration;

const QUOTA_OBSERVER_STATE_DIR: &str = "provider-state/opencode/quota-observers";
const QUOTA_OBSERVER_SCHEMA_VERSION: u32 = 3;
const MANIFEST_QUOTA_OBSERVER_SCHEMA_VERSION: u32 = 2;
const PREDECESSOR_QUOTA_OBSERVER_SCHEMA_VERSION: u32 = 1;
const PREDECESSOR_QUOTA_OBSERVER_CONTRACT: &str = "chatgpt_wham_curl/v1";
pub(crate) const QUOTA_OBSERVER_CONTRACT: &str = "agent-runner-opencode.chatgpt-wham-http/v1";
const QUOTA_OBSERVER_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_QUOTA_OBSERVER_STATE_BYTES: usize = 1024 * 1024;
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
const OBSERVER_ENV_KEYS: &[&str] = &["PATH", "HOME"];
const IN_PROCESS_TRANSPORT_KIND: &str = "in_process_reqwest_rustls_webpki";
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
const CONTRACT_FIXTURE_TRANSPORT_KIND: &str = "contract_test_external_curl";
#[cfg(all(
    target_os = "linux",
    not(all(feature = "contract-test-fixtures", debug_assertions))
))]
const IN_PROCESS_TRANSPORT_SOURCE: &str = include_str!("quota_adapter.rs");
#[cfg(all(
    target_os = "linux",
    not(all(feature = "contract-test-fixtures", debug_assertions))
))]
const IN_PROCESS_DEPENDENCY_LOCK: &str = include_str!("../Cargo.lock");

#[derive(Clone, Deserialize, Serialize)]
pub struct QuotaObserverContext {
    schema_version: u32,
    observer_contract: String,
    #[serde(default)]
    transport_kind: String,
    account_wrapper: String,
    program: String,
    program_sha256: String,
    execution_env: BTreeMap<String, String>,
    #[serde(default)]
    implementation_manifest_id: String,
    #[serde(default)]
    implementation_version: String,
    identity_sha256: String,
}

pub fn resolve(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<QuotaObserverContext, ProviderFailure> {
    let timeout =
        operation_bounds::remaining_timeout(host.deadline_unix_ms, QUOTA_OBSERVER_LOCK_TIMEOUT)
            .ok_or_else(|| quota_observer_lock_timeout(request_id))?;
    let _lock = acquire_observer_lock(host, account, timeout, request_id)?;
    if let Some(context) = read_observer_context(host, account, request_id)? {
        return activate_observer_context(host, account, context, request_id);
    }
    let context = candidate_context(account, ambient_observer_environment(), request_id)?;
    write_observer_context(host, account, &context, request_id)?;
    Ok(context)
}

#[cfg(all(
    target_os = "linux",
    not(all(feature = "contract-test-fixtures", debug_assertions))
))]
pub(crate) fn setup_transport_evidence() -> serde_json::Value {
    let source_sha256 = sha256_hex(IN_PROCESS_TRANSPORT_SOURCE.as_bytes());
    let dependency_lock_sha256 = sha256_hex(IN_PROCESS_DEPENDENCY_LOCK.as_bytes());
    json!({
        "program": "in-process-wham-http",
        "present": true,
        "path": null,
        "version": {
            "present": true,
            "ready": true,
            "status": 0,
        },
        "implementation": {
            "ready": true,
            "transport_kind": IN_PROCESS_TRANSPORT_KIND,
            "semantic_contract": QUOTA_OBSERVER_CONTRACT,
            "source_sha256": source_sha256,
            "dependency_lock_sha256": dependency_lock_sha256,
            "provider_version": env!("CARGO_PKG_VERSION"),
        },
    })
}

#[cfg(all(
    not(target_os = "linux"),
    not(all(feature = "contract-test-fixtures", debug_assertions))
))]
pub(crate) fn setup_transport_evidence() -> serde_json::Value {
    json!({
        "program": "in-process-wham-http",
        "present": true,
        "path": null,
        "version": {
            "present": true,
            "ready": false,
            "error": "the in-process WHAM transport is currently reviewed only for Linux targets",
        },
        "implementation": {
            "ready": false,
            "semantic_contract": QUOTA_OBSERVER_CONTRACT,
            "target_os": std::env::consts::OS,
        },
    })
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
    let path = observer_context_path(host, account, request_id)?;
    let bytes = match durable_fs::read_file_bounded(&path, MAX_QUOTA_OBSERVER_STATE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(quota_observer_failure(request_id, error)),
    };
    let context = serde_json::from_slice(&bytes)
        .map_err(|error| quota_observer_failure(request_id, error))?;
    validate_observer_record_identity(&context, account, request_id)?;
    if require_current_implementation {
        validate_observer_implementation(&context, account, request_id)?;
    }
    Ok(Some((context.identity_sha256, sha256_hex(&bytes))))
}

impl QuotaObserverContext {
    #[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
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
    execution_env: BTreeMap<String, String>,
    request_id: &str,
) -> Result<QuotaObserverContext, ProviderFailure> {
    #[cfg(all(
        target_os = "linux",
        not(all(feature = "contract-test-fixtures", debug_assertions))
    ))]
    {
        let _ = (execution_env, request_id);
        let source_sha256 = sha256_hex(IN_PROCESS_TRANSPORT_SOURCE.as_bytes());
        let dependency_lock_sha256 = sha256_hex(IN_PROCESS_DEPENDENCY_LOCK.as_bytes());
        let program_sha256 = sha256_hex(
            json!({
                "transport_kind": IN_PROCESS_TRANSPORT_KIND,
                "source_sha256": source_sha256,
                "dependency_lock_sha256": dependency_lock_sha256,
            })
            .to_string()
            .as_bytes(),
        );
        let implementation_manifest_id = format!(
            "in-process-wham:{}:{}",
            &source_sha256[..12],
            &dependency_lock_sha256[..12]
        );
        let implementation_version = env!("CARGO_PKG_VERSION").to_string();
        let execution_env = BTreeMap::new();
        let program = "in-process:reqwest-blocking-rustls-webpki-roots";
        let identity_sha256 = observer_identity_sha256(
            account.opencode_wrapper,
            IN_PROCESS_TRANSPORT_KIND,
            program,
            &program_sha256,
            &execution_env,
            &implementation_manifest_id,
            &implementation_version,
        );
        Ok(QuotaObserverContext {
            schema_version: QUOTA_OBSERVER_SCHEMA_VERSION,
            observer_contract: QUOTA_OBSERVER_CONTRACT.to_string(),
            transport_kind: IN_PROCESS_TRANSPORT_KIND.to_string(),
            account_wrapper: account.opencode_wrapper.to_string(),
            program: program.to_string(),
            program_sha256,
            execution_env,
            implementation_manifest_id,
            implementation_version,
            identity_sha256,
        })
    }

    #[cfg(all(
        not(target_os = "linux"),
        not(all(feature = "contract-test-fixtures", debug_assertions))
    ))]
    {
        let _ = (account, execution_env);
        Err(quota_observer_failure(
            request_id,
            "the in-process WHAM transport is currently reviewed only for Linux targets",
        ))
    }

    #[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
    {
        candidate_external_fixture_context(account, execution_env, request_id)
    }
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn candidate_external_fixture_context(
    account: &AccountProfile,
    execution_env: BTreeMap<String, String>,
    request_id: &str,
) -> Result<QuotaObserverContext, ProviderFailure> {
    let program = resolve_program("curl", &execution_env).ok_or_else(|| {
        quota_observer_failure(
            request_id,
            "contract-test curl was not found in the selected quota-observer PATH",
        )
    })?;
    if !durable_fs::is_executable_file(&program)
        .map_err(|error| quota_observer_failure(request_id, error))?
    {
        return Err(quota_observer_failure(
            request_id,
            "contract-test curl is not an executable regular file",
        ));
    }
    let (program_sha256, program_bytes) =
        durable_fs::sha256_file_bounded(&program, durable_fs::MAX_BOUND_EXECUTABLE_BYTES)
            .map_err(|error| quota_observer_failure(request_id, error))?;
    let program = program.to_str().ok_or_else(|| {
        quota_observer_failure(request_id, "quota observer path is not valid UTF-8")
    })?;
    let approved = native_implementation_manifest::approved_implementation(
        "curl",
        &program_sha256,
        program_bytes,
    )
    .map_err(|error| quota_observer_failure(request_id, error))?
    .ok_or_else(|| quota_observer_failure(request_id, "contract-test curl was not admitted"))?;
    let identity_sha256 = observer_identity_sha256(
        account.opencode_wrapper,
        CONTRACT_FIXTURE_TRANSPORT_KIND,
        program,
        &program_sha256,
        &execution_env,
        &approved.id,
        &approved.version,
    );
    Ok(QuotaObserverContext {
        schema_version: QUOTA_OBSERVER_SCHEMA_VERSION,
        observer_contract: QUOTA_OBSERVER_CONTRACT.to_string(),
        transport_kind: CONTRACT_FIXTURE_TRANSPORT_KIND.to_string(),
        account_wrapper: account.opencode_wrapper.to_string(),
        program: program.to_string(),
        program_sha256,
        execution_env,
        implementation_manifest_id: approved.id,
        implementation_version: approved.version,
        identity_sha256,
    })
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn ambient_observer_environment() -> BTreeMap<String, String> {
    OBSERVER_ENV_KEYS
        .iter()
        .filter_map(|key| {
            std::env::var(key)
                .ok()
                .map(|value| ((*key).to_string(), value))
        })
        .collect()
}

#[cfg(not(all(feature = "contract-test-fixtures", debug_assertions)))]
fn ambient_observer_environment() -> BTreeMap<String, String> {
    BTreeMap::new()
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn resolve_program(program: &str, environment: &BTreeMap<String, String>) -> Option<PathBuf> {
    environment
        .get("PATH")
        .map(std::ffi::OsStr::new)
        .into_iter()
        .flat_map(std::env::split_paths)
        .map(|directory| directory.join(program))
        .find(|candidate| {
            durable_fs::is_executable_file(candidate)
                .ok()
                .is_some_and(|executable| executable)
        })
        .and_then(|candidate| fs::canonicalize(candidate).ok())
}

fn observer_identity_sha256(
    account_wrapper: &str,
    transport_kind: &str,
    program: &str,
    program_sha256: &str,
    execution_env: &BTreeMap<String, String>,
    implementation_manifest_id: &str,
    implementation_version: &str,
) -> String {
    sha256_hex(
        json!({
            "observer_contract": QUOTA_OBSERVER_CONTRACT,
            "transport_kind": transport_kind,
            "account_wrapper": account_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "execution_env": execution_env,
            "implementation_manifest_id": implementation_manifest_id,
            "implementation_version": implementation_version,
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
    validate_observer_record_identity(context, account, request_id)?;
    validate_observer_implementation(context, account, request_id)
}

fn validate_observer_record_identity(
    context: &QuotaObserverContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let identity_sha256 = match context.schema_version {
        QUOTA_OBSERVER_SCHEMA_VERSION => observer_identity_sha256(
            &context.account_wrapper,
            &context.transport_kind,
            &context.program,
            &context.program_sha256,
            &context.execution_env,
            &context.implementation_manifest_id,
            &context.implementation_version,
        ),
        MANIFEST_QUOTA_OBSERVER_SCHEMA_VERSION => manifest_observer_identity_sha256(
            &context.account_wrapper,
            &context.program,
            &context.program_sha256,
            &context.execution_env,
            &context.implementation_manifest_id,
            &context.implementation_version,
        ),
        PREDECESSOR_QUOTA_OBSERVER_SCHEMA_VERSION => predecessor_observer_identity_sha256(
            &context.account_wrapper,
            &context.program,
            &context.program_sha256,
            &context.execution_env,
        ),
        _ => String::new(),
    };
    if context.account_wrapper != account.opencode_wrapper
        || context.identity_sha256 != identity_sha256
        || (context.schema_version == QUOTA_OBSERVER_SCHEMA_VERSION
            && (context.observer_contract != QUOTA_OBSERVER_CONTRACT
                || context.transport_kind.trim().is_empty()
                || context.implementation_manifest_id.trim().is_empty()
                || context.implementation_version.trim().is_empty()))
        || (context.schema_version == MANIFEST_QUOTA_OBSERVER_SCHEMA_VERSION
            && context.observer_contract != "agent-runner-opencode.chatgpt-wham-curl/v1")
        || (context.schema_version == PREDECESSOR_QUOTA_OBSERVER_SCHEMA_VERSION
            && context.observer_contract != PREDECESSOR_QUOTA_OBSERVER_CONTRACT)
    {
        return Err(quota_observer_failure(
            request_id,
            "persisted quota observer identity is inconsistent",
        ));
    }
    Ok(())
}

fn manifest_observer_identity_sha256(
    account_wrapper: &str,
    program: &str,
    program_sha256: &str,
    execution_env: &BTreeMap<String, String>,
    implementation_manifest_id: &str,
    implementation_version: &str,
) -> String {
    sha256_hex(
        json!({
            "observer_contract": "agent-runner-opencode.chatgpt-wham-curl/v1",
            "account_wrapper": account_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "execution_env": execution_env,
            "implementation_manifest_id": implementation_manifest_id,
            "implementation_version": implementation_version,
        })
        .to_string()
        .as_bytes(),
    )
}

fn predecessor_observer_identity_sha256(
    account_wrapper: &str,
    program: &str,
    program_sha256: &str,
    execution_env: &BTreeMap<String, String>,
) -> String {
    sha256_hex(
        json!({
            "observer_contract": PREDECESSOR_QUOTA_OBSERVER_CONTRACT,
            "account_wrapper": account_wrapper,
            "program": program,
            "program_sha256": program_sha256,
            "execution_env": execution_env,
        })
        .to_string()
        .as_bytes(),
    )
}

fn activate_observer_context(
    host: &HostContext,
    account: &AccountProfile,
    context: QuotaObserverContext,
    request_id: &str,
) -> Result<QuotaObserverContext, ProviderFailure> {
    validate_observer_context(&context, account, request_id)?;
    if context.schema_version == QUOTA_OBSERVER_SCHEMA_VERSION {
        return Ok(context);
    }
    let upgraded = candidate_context(account, BTreeMap::new(), request_id)?;
    write_observer_context(host, account, &upgraded, request_id)?;
    Ok(upgraded)
}

fn validate_observer_implementation(
    context: &QuotaObserverContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if context.schema_version < QUOTA_OBSERVER_SCHEMA_VERSION {
        return Ok(());
    }
    if context.transport_kind == IN_PROCESS_TRANSPORT_KIND {
        let candidate = candidate_context(account, BTreeMap::new(), request_id)?;
        if candidate.identity_sha256 == context.identity_sha256 {
            return Ok(());
        }
        return Err(quota_observer_failure(
            request_id,
            "persisted in-process quota transport identity is inconsistent with this provider build",
        ));
    }
    #[cfg(not(all(feature = "contract-test-fixtures", debug_assertions)))]
    {
        Err(quota_observer_failure(
            request_id,
            "external quota transport identities are not admitted by production builds",
        ))
    }
    #[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
    {
        validate_external_fixture_implementation(context, account, request_id)
    }
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn validate_external_fixture_implementation(
    context: &QuotaObserverContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if context.transport_kind != CONTRACT_FIXTURE_TRANSPORT_KIND
        || !durable_fs::is_executable_file(Path::new(&context.program))
            .map_err(|error| quota_observer_failure(request_id, error))?
    {
        return Err(quota_observer_failure(
            request_id,
            "bound contract-test quota observer is no longer executable",
        ));
    }
    let (program_sha256, program_bytes) = durable_fs::sha256_file_bounded(
        Path::new(&context.program),
        durable_fs::MAX_BOUND_EXECUTABLE_BYTES,
    )
    .map_err(|error| quota_observer_failure(request_id, error))?;
    if program_sha256 != context.program_sha256 {
        return Err(ProviderFailure::conflict(
            request_id,
            "quota_observer_implementation_changed",
            format!(
                "bound contract-test quota observer for account {} changed after admission",
                account.opencode_wrapper
            ),
            json!({
                "account": account.opencode_wrapper,
                "quota_observer_identity_sha256": context.identity_sha256,
                "program": context.program,
            }),
        ));
    }
    let approved = native_implementation_manifest::approved_implementation(
        "curl",
        &context.program_sha256,
        program_bytes,
    )
    .map_err(|error| quota_observer_failure(request_id, error))?
    .ok_or_else(|| {
        quota_observer_failure(request_id, "contract-test curl is no longer admitted")
    })?;
    if context.implementation_manifest_id != approved.id
        || context.implementation_version != approved.version
    {
        return Err(quota_observer_failure(
            request_id,
            "persisted contract-test quota observer identity is inconsistent",
        ));
    }
    Ok(())
}

fn acquire_observer_lock(
    host: &HostContext,
    account: &AccountProfile,
    timeout: Duration,
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
    if !operation_bounds::lock_exclusive_for(&lock, timeout)
        .map_err(|error| quota_observer_failure(request_id, error))?
    {
        return Err(quota_observer_lock_timeout(request_id));
    }
    Ok(lock)
}

fn read_observer_context(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<Option<QuotaObserverContext>, ProviderFailure> {
    let path = observer_context_path(host, account, request_id)?;
    let bytes = match durable_fs::read_file_bounded(&path, MAX_QUOTA_OBSERVER_STATE_BYTES) {
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
    if bytes.len() > MAX_QUOTA_OBSERVER_STATE_BYTES {
        return Err(quota_observer_state_capacity(request_id, bytes.len()));
    }
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

fn quota_observer_lock_timeout(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "quota_observer_lock_timeout",
        "quota observer identity lock could not be acquired before the operation deadline",
    )
}

fn quota_observer_state_capacity(request_id: &str, observed_bytes: usize) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "quota_observer_state_capacity_exceeded",
        format!(
            "quota observer identity state is {observed_bytes} bytes; the supported maximum is {MAX_QUOTA_OBSERVER_STATE_BYTES} bytes"
        ),
    )
}

#[cfg(all(
    test,
    target_os = "linux",
    not(all(feature = "contract-test-fixtures", debug_assertions))
))]
mod tests {
    use super::*;
    use crate::account::ACCOUNTS;

    #[test]
    fn production_observer_identity_is_source_included_and_has_no_external_program() {
        let context = candidate_context(&ACCOUNTS[0], BTreeMap::new(), "observer-unit")
            .expect("construct in-process observer identity");
        assert_eq!(context.schema_version, QUOTA_OBSERVER_SCHEMA_VERSION);
        assert_eq!(context.transport_kind, IN_PROCESS_TRANSPORT_KIND);
        assert!(context.program.starts_with("in-process:"));
        assert!(context.execution_env.is_empty());
        assert!(!context.program_sha256.is_empty());
        assert!(!context.implementation_manifest_id.is_empty());
        validate_observer_context(&context, &ACCOUNTS[0], "observer-unit")
            .expect("validate source-included observer identity");
    }
}
