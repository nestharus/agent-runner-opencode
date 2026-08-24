//! Declared roles: orchestration, mapper, parser, validator, accessor, predicate, formatter
//! adapter_declarations:
//!   - component: src/quota.rs
//!     role: adapter
//!     Translates:
//!       - opencode auth source profile to QuotaSourceResult
//!       - provider-owned QuotaObservation to QuotaProbeWindow
//!       - quota refresh requirement projection at the observation boundary

use crate::account::AccountProfile;
use crate::activity::ActivityTargets;
use crate::encoding::now_unix_ms;
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope};
use crate::native_runtime::{self, NativeRuntimeContext};
use crate::quota_adapter::{self, QuotaObservation, QuotaObservationFailure, QuotaWindow};
use crate::quota_observer::{self, QuotaObserverContext};
use crate::runtime_selection::{append_resolved_activity_targets, resolve_runtime_selection};
use chrono::DateTime;
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Deserialize)]
struct QuotaBaseParams {
    settings_id: String,
}

#[derive(Clone, Copy)]
pub(crate) enum Command {
    Source,
    Probe,
    RefreshAuth,
}

pub(crate) fn handle(command: Command, request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let RequestEnvelope {
        host,
        params,
        request_id,
        provider_instance_id,
        ..
    } = request;
    match command {
        Command::Source => source_params(&host, params, &request_id),
        Command::Probe => probe_params(&host, params, &request_id),
        Command::RefreshAuth => crate::quota_auth_refresh::refresh_auth_params(
            &host,
            params,
            &request_id,
            provider_instance_id.as_deref(),
        ),
    }
}

pub(crate) fn activity_targets(
    host: &HostContext,
    params: &Value,
    result: Option<&Value>,
    request_id: &str,
) -> ActivityTargets {
    let mut targets = ActivityTargets::default();
    let settings_id = params
        .get("settings_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if let Some(settings_id) = settings_id {
        targets.attempted("settings_record", settings_id, "params.settings_id");
        if result.is_some() {
            append_resolved_activity_targets(
                &mut targets,
                host,
                settings_id,
                request_id,
                "runtime_selection.settings_record",
            );
        }
    }
    targets
}

pub fn source_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params = parse_base_params(params, request_id)?;
    let account = account_from_settings_record(host, &params.settings_id, request_id)?;
    let auth_path = observed_auth_path(host, account, request_id)?;
    Ok(source_result(account, &auth_path))
}

pub fn probe_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params = parse_base_params(params, request_id)?;
    let account = account_from_settings_record(host, &params.settings_id, request_id)?;
    let auth_path = observed_auth_path(host, account, request_id)?;
    let observer = quota_observer::resolve(host, account, request_id)?;
    Ok(probe_auth_path(&auth_path, &observer))
}

fn source_result(account: &AccountProfile, auth_path: &Path) -> Value {
    let has_source = auth_has_source(auth_path);
    let source_id = readable_source_id(has_source, account, auth_path);
    source_result_json(has_source, source_id)
}

fn source_result_json(has_source: bool, source_id: Option<String>) -> Value {
    let mut result = serde_json::Map::new();
    result.insert("has_source".to_string(), json!(has_source));
    result.insert("freshness".to_string(), json!(source_freshness(has_source)));
    if let Some(source_id) = source_id {
        result.insert("source_id".to_string(), json!(source_id));
    }
    Value::Object(result)
}

fn readable_source_id(
    has_source: bool,
    account: &AccountProfile,
    auth_path: &Path,
) -> Option<String> {
    has_source.then(|| source_id(account, auth_path))
}

fn probe_auth_path(auth_path: &Path, observer: &QuotaObserverContext) -> Value {
    if !auth_has_source(auth_path) {
        return unreadable_auth_probe_result();
    }
    probe_observation_result(run_probe(auth_path, observer))
}

fn unreadable_auth_probe_result() -> Value {
    unavailable_result("native opencode auth source is missing or unreadable".to_string())
}

pub(crate) fn run_probe(
    auth_path: &Path,
    observer: &QuotaObserverContext,
) -> Result<QuotaObservation, QuotaObservationFailure> {
    quota_adapter::observe_quota(auth_path, observer)
}

fn probe_observation_result(
    observation: Result<QuotaObservation, QuotaObservationFailure>,
) -> Value {
    match observation {
        Ok(observation) => available_probe_result(&observation.windows),
        Err(failure) => unavailable_result(quota_observation_failure_detail(failure)),
    }
}

fn quota_observation_failure_detail(failure: QuotaObservationFailure) -> String {
    if failure.needs_auth_refresh() {
        return format!(
            "{} (authentication refresh required; invoke quota.refresh_auth with a new request_id before probing again)",
            failure.detail
        );
    }
    failure.detail
}

fn unavailable_result(detail: String) -> Value {
    json!({
        "available": false,
        "checked_at_unix_ms": now_unix_ms(),
        "windows": [],
        "detail": detail,
    })
}

fn quota_windows(windows: &[QuotaWindow]) -> Vec<Value> {
    windows.iter().map(quota_window).collect()
}

fn quota_window(window: &QuotaWindow) -> Value {
    let mut result = serde_json::Map::new();
    if let Some(name) = &window.name {
        result.insert("name".to_string(), json!(name));
    }
    result.insert(
        "remaining_ratio".to_string(),
        json!(((100.0 - window.used_percent) / 100.0).clamp(0.0, 1.0)),
    );
    result.insert(
        "resets_at_unix_ms".to_string(),
        json!(epoch_ms(&window.resets_at)),
    );
    Value::Object(result)
}

fn parse_base_params(params: Value, request_id: &str) -> Result<QuotaBaseParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_quota_params_failure(request_id, err))
}

fn account_from_settings_record(
    host: &HostContext,
    settings_id: &str,
    request_id: &str,
) -> Result<&'static AccountProfile, ProviderFailure> {
    resolve_runtime_selection(host, settings_id, request_id).map(|selection| selection.account)
}

pub(crate) fn resolved_auth_path(
    account: &AccountProfile,
    runtime: &NativeRuntimeContext,
) -> PathBuf {
    runtime.expand_path(account.quota_auth_path())
}

fn ambient_auth_path(account: &AccountProfile) -> PathBuf {
    match (
        account.quota_auth_path().strip_prefix("~/"),
        std::env::var_os("HOME"),
    ) {
        (Some(relative), Some(home)) => PathBuf::from(home).join(relative),
        _ => PathBuf::from(account.quota_auth_path()),
    }
}

fn observed_auth_path(
    host: &HostContext,
    account: &AccountProfile,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    native_runtime::resolve_existing_for_account(host, account, request_id).map(|runtime| {
        runtime
            .as_ref()
            .map(|runtime| resolved_auth_path(account, runtime))
            .unwrap_or_else(|| ambient_auth_path(account))
    })
}

fn auth_is_readable(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() || !has_read_permission(&metadata) {
        return false;
    }
    fs::File::open(path).is_ok()
}

fn auth_has_source(path: &Path) -> bool {
    auth_is_readable(path)
}

#[cfg(unix)]
fn has_read_permission(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o444 != 0
}

#[cfg(not(unix))]
fn has_read_permission(_metadata: &fs::Metadata) -> bool {
    true
}

fn source_id(account: &AccountProfile, auth_path: &Path) -> String {
    format!(
        "{}:{}:{}:{}",
        account.quota_source_kind(),
        account.opencode_wrapper,
        account.opencode_index,
        auth_path.to_string_lossy()
    )
}

fn source_freshness(has_source: bool) -> &'static str {
    if has_source {
        "auth_readable"
    } else {
        "auth_missing_or_unreadable"
    }
}

fn available_probe_result(windows: &[QuotaWindow]) -> Value {
    json!({
        "available": true,
        "checked_at_unix_ms": now_unix_ms(),
        "windows": quota_windows(windows),
    })
}

fn invalid_quota_params_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_quota_params",
        format!("quota params are invalid: {err}"),
    )
}

fn epoch_ms(rfc3339: &str) -> i64 {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("quota observation resets_at was validated before projection")
        .timestamp_millis()
}
