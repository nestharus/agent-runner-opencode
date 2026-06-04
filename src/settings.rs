//! Declared roles: accessor, validator, mapper
//! intrinsic_surface_declarations:
//!   - component: src/settings.rs
//!     role: intrinsic-surface
//!     Domain: provider-owned opencode settings store
//!     Owns:
//!       - profile record persistence rooted at host.config_root
//!       - opaque settings version tokens and stale-write conflict detection
//!       - opencode.settings/v1 semantic validation and legacy mapping

use crate::account::ACCOUNTS;
use crate::encoding::{now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure, CATEGORY_CONFLICT};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const STORE_DIR: &str = "agent-runner-opencode";
const STORE_FILE: &str = "settings-store.json";

#[derive(Deserialize)]
struct SettingsCreateParams {
    display_name: Option<String>,
    values: Value,
}

#[derive(Deserialize)]
struct SettingsGetParams {
    id: String,
}

#[derive(Deserialize)]
struct SettingsUpdateParams {
    id: String,
    version: String,
    values: Value,
}

#[derive(Deserialize)]
struct SettingsDeleteParams {
    id: String,
    version: String,
}

#[derive(Deserialize)]
struct SettingsValidateParams {
    values: Value,
}

#[derive(Deserialize)]
struct SettingsMigrateParams {
    dry_run: bool,
    legacy: Value,
}

#[derive(Serialize, Deserialize, Default)]
struct SettingsStore {
    records: Vec<SettingsRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
struct SettingsRecord {
    id: String,
    display_name: String,
    version: String,
    values: Value,
}

pub fn list_params(host: &HostContext, request_id: &str) -> Result<Value, ProviderFailure> {
    let store = read_store(host, request_id)?;
    Ok(json!({
        "records": store.records.iter().map(record_summary).collect::<Vec<_>>(),
    }))
}

pub fn get_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params: SettingsGetParams =
        parse_params(params, request_id, "invalid_settings_get_params")?;
    let store = read_store(host, request_id)?;
    let record = find_record(&store, &params.id, request_id)?;
    Ok(json!({ "record": record_json(record) }))
}

pub fn create_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params: SettingsCreateParams =
        parse_params(params, request_id, "invalid_settings_create_params")?;
    let values = sanitize_value(&params.values);
    let diagnostics = validate_values(&values);
    let mut store = read_store(host, request_id)?;
    let record = new_record(params.display_name, values);
    store.records.push(record.clone());
    write_store(host, &store, request_id)?;
    Ok(json!({ "record": record_json(&record), "diagnostics": diagnostics }))
}

pub fn update_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params: SettingsUpdateParams =
        parse_params(params, request_id, "invalid_settings_update_params")?;
    let mut store = read_store(host, request_id)?;
    let index = record_index(&store, &params.id, request_id)?;
    ensure_version(&store.records[index], &params.version, request_id)?;
    let values = sanitize_value(&params.values);
    let diagnostics = validate_values(&values);
    store.records[index].version = version_token(&params.id, &values);
    store.records[index].values = values;
    let record = store.records[index].clone();
    write_store(host, &store, request_id)?;
    Ok(json!({ "record": record_json(&record), "diagnostics": diagnostics }))
}

pub fn delete_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params: SettingsDeleteParams =
        parse_params(params, request_id, "invalid_settings_delete_params")?;
    let mut store = read_store(host, request_id)?;
    let index = record_index(&store, &params.id, request_id)?;
    ensure_version(&store.records[index], &params.version, request_id)?;
    store.records.remove(index);
    write_store(host, &store, request_id)?;
    Ok(json!({ "deleted": true, "id": params.id }))
}

pub fn validate_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params: SettingsValidateParams =
        parse_params(params, request_id, "invalid_settings_validate_params")?;
    let values = sanitize_value(&params.values);
    let diagnostics = validate_values(&values);
    Ok(json!({ "valid": diagnostics.is_empty(), "diagnostics": diagnostics }))
}

pub fn migrate_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params: SettingsMigrateParams =
        parse_params(params, request_id, "invalid_settings_migrate_params")?;
    let actions = legacy_actions(&params.legacy);
    let warnings = legacy_warnings(&params.legacy);
    let diagnostics = legacy_diagnostics(&params.legacy);
    if !params.dry_run {
        write_migrated_settings(host, &params.legacy, request_id)?;
    }
    Ok(json!({
        "actions": actions,
        "warnings": warnings,
        "requires_user_input": diagnostics.iter().any(is_error_diagnostic),
        "diagnostics": diagnostics,
    }))
}

fn parse_params<T: for<'de> Deserialize<'de>>(
    params: Value,
    request_id: &str,
    code: &'static str,
) -> Result<T, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            code,
            format!("settings params are invalid: {err}"),
        )
    })
}

fn read_store(host: &HostContext, request_id: &str) -> Result<SettingsStore, ProviderFailure> {
    let path = store_path(host, request_id)?;
    if !path.exists() {
        return Ok(SettingsStore::default());
    }
    let bytes = fs::read(&path)
        .map_err(|err| store_io_failure(request_id, "settings_store_read_failed", err))?;
    serde_json::from_slice(&bytes).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "settings_store_parse_failed",
            format!("provider settings store is invalid JSON: {err}"),
        )
    })
}

fn write_store(
    host: &HostContext,
    store: &SettingsStore,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let config_root = config_root(host, request_id)?;
    let path = store_path_from_root(&config_root);
    let parent = path.parent().expect("settings store always has parent");
    ensure_store_path_contained(parent, &config_root, request_id)?;
    fs::create_dir_all(parent)
        .map_err(|err| store_io_failure(request_id, "settings_store_create_dir_failed", err))?;
    let tmp = parent.join(format!(".{STORE_FILE}.{}.tmp", std::process::id()));
    ensure_store_path_contained(&tmp, &config_root, request_id)?;
    ensure_store_path_contained(&path, &config_root, request_id)?;
    write_store_temp(&tmp, store, request_id)?;
    fs::rename(&tmp, &path)
        .map_err(|err| store_io_failure(request_id, "settings_store_rename_failed", err))
}

fn write_store_temp(
    path: &Path,
    store: &SettingsStore,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let bytes = serde_json::to_vec(store).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "settings_store_serialize_failed",
            format!("failed to serialize provider settings store: {err}"),
        )
    })?;
    let mut file = fs::File::create(path)
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_create_failed", err))?;
    file.write_all(&bytes)
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_write_failed", err))?;
    file.sync_all()
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_sync_failed", err))
}

fn store_path(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    Ok(store_path_from_root(&config_root(host, request_id)?))
}

fn config_root(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let Some(root) = host
        .config_root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
    else {
        return Err(ProviderFailure::invalid_request(
            request_id,
            "missing_config_root",
            "settings store requires host.config_root",
        ));
    };
    Ok(PathBuf::from(root))
}

fn store_path_from_root(config_root: &Path) -> PathBuf {
    config_root.join(STORE_DIR).join(STORE_FILE)
}

fn ensure_store_path_contained(
    path: &Path,
    config_root: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let canonical_root = canonical_store_path(config_root, request_id)?;
    let canonical_target = canonical_store_create_path(path, request_id)?;
    if canonical_target.starts_with(canonical_root) {
        return Ok(());
    }
    Err(ProviderFailure::invalid_request(
        request_id,
        "settings_store_outside_provider_root",
        "settings store path must stay under host.config_root",
    ))
}

fn canonical_store_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    fs::canonicalize(path).map_err(|err| {
        ProviderFailure::internal(
            request_id,
            "settings_store_canonicalize_failed",
            format!("failed to canonicalize provider settings path: {err}"),
        )
    })
}

fn canonical_store_create_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let existing = path
        .ancestors()
        .find(|ancestor| ancestor.exists())
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "settings_store_outside_provider_root",
                "settings store path must have an existing host.config_root ancestor",
            )
        })?;
    let mut canonical = canonical_store_path(existing, request_id)?;
    let suffix = path.strip_prefix(existing).map_err(|_| {
        ProviderFailure::invalid_request(
            request_id,
            "settings_store_outside_provider_root",
            "settings store path must stay under host.config_root",
        )
    })?;
    for component in suffix.components() {
        match component {
            std::path::Component::Normal(part) => canonical.push(part),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::Prefix(_)
            | std::path::Component::RootDir => {
                return Err(ProviderFailure::invalid_request(
                    request_id,
                    "settings_store_outside_provider_root",
                    "settings store path must stay under host.config_root",
                ));
            }
        }
    }
    Ok(canonical)
}

fn store_io_failure(request_id: &str, code: &'static str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        code,
        format!("provider settings store I/O failed: {err}"),
    )
}

fn find_record<'a>(
    store: &'a SettingsStore,
    id: &str,
    request_id: &str,
) -> Result<&'a SettingsRecord, ProviderFailure> {
    store
        .records
        .iter()
        .find(|record| record.id == id)
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "settings_not_found",
                "settings record was not found",
            )
        })
}

fn record_index(
    store: &SettingsStore,
    id: &str,
    request_id: &str,
) -> Result<usize, ProviderFailure> {
    store
        .records
        .iter()
        .position(|record| record.id == id)
        .ok_or_else(|| {
            ProviderFailure::invalid_request(
                request_id,
                "settings_not_found",
                "settings record was not found",
            )
        })
}

fn ensure_version(
    record: &SettingsRecord,
    version: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if record.version == version {
        return Ok(());
    }
    Err(ProviderFailure {
        request_id: request_id.to_string(),
        category: CATEGORY_CONFLICT,
        code: "stale_settings_version",
        message: "settings version is stale".to_string(),
        details: json!({}),
        retryable: false,
        exit_code: 4,
    })
}

fn new_record(display_name: Option<String>, values: Value) -> SettingsRecord {
    let id = settings_id(&values);
    SettingsRecord {
        display_name: display_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| default_display_name(&values)),
        version: version_token(&id, &values),
        id,
        values,
    }
}

fn settings_id(values: &Value) -> String {
    let wrapper = values
        .get("wrapper")
        .and_then(Value::as_str)
        .or_else(|| values.get("profile").and_then(Value::as_str))
        .unwrap_or("opencode");
    let digest = sha256_hex(format!("{}:{}", wrapper, now_unix_ms()).as_bytes());
    format!("{wrapper}-{}", &digest[..12])
}

fn version_token(id: &str, values: &Value) -> String {
    let digest = sha256_hex(format!("{}:{}:{}", id, now_unix_ms(), values).as_bytes());
    format!("v{}", &digest[..24])
}

fn default_display_name(values: &Value) -> String {
    values
        .get("profile")
        .and_then(Value::as_str)
        .or_else(|| values.get("wrapper").and_then(Value::as_str))
        .unwrap_or("opencode profile")
        .to_string()
}

fn record_summary(record: &SettingsRecord) -> Value {
    json!({
        "id": record.id,
        "display_name": record.display_name,
        "version": record.version,
        "summary": summary_values(&record.values),
    })
}

fn record_json(record: &SettingsRecord) -> Value {
    json!({
        "id": record.id,
        "display_name": record.display_name,
        "version": record.version,
        "values": record.values,
    })
}

fn summary_values(values: &Value) -> Value {
    json!({
        "provider": values.get("provider").cloned().unwrap_or(Value::Null),
        "wrapper": values.get("wrapper").cloned().unwrap_or(Value::Null),
        "model": values.pointer("/model/name").cloned().unwrap_or(Value::Null),
    })
}

fn validate_values(values: &Value) -> Vec<Value> {
    let mut diagnostics = Vec::new();
    require_string(values, "provider", "opencode", &mut diagnostics);
    require_known_wrapper(values, &mut diagnostics);
    require_model(values, &mut diagnostics);
    require_quota(values, &mut diagnostics);
    diagnostics
}

fn require_string(values: &Value, key: &str, expected: &str, diagnostics: &mut Vec<Value>) {
    if values.get(key).and_then(Value::as_str) == Some(expected) {
        return;
    }
    diagnostics.push(diagnostic(
        "error",
        format!("values.{key}"),
        format!("{key} must be {expected}"),
        "invalid_settings_value",
    ));
}

fn require_known_wrapper(values: &Value, diagnostics: &mut Vec<Value>) {
    let wrapper = values
        .get("wrapper")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if ACCOUNTS
        .iter()
        .any(|account| account.opencode_wrapper == wrapper)
    {
        return;
    }
    diagnostics.push(diagnostic(
        "error",
        "values.wrapper",
        "wrapper must be one of opencode1 through opencode5",
        "invalid_wrapper",
    ));
}

fn require_model(values: &Value, diagnostics: &mut Vec<Value>) {
    let provider_model = values
        .pointer("/model/provider_model")
        .and_then(Value::as_str);
    if provider_model != Some("openai/gpt-5.5") {
        diagnostics.push(diagnostic(
            "error",
            "values.model.provider_model",
            "provider_model must be openai/gpt-5.5",
            "invalid_provider_model",
        ));
    }
    let variant = values.pointer("/model/variant").and_then(Value::as_str);
    if !matches!(variant, Some("none" | "low" | "medium" | "high" | "xhigh")) {
        diagnostics.push(diagnostic(
            "error",
            "values.model.variant",
            "variant must be none, low, medium, high, or xhigh",
            "invalid_model_variant",
        ));
    }
}

fn require_quota(values: &Value, diagnostics: &mut Vec<Value>) {
    let auth_path = values
        .pointer("/quota/auth_path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !auth_path.trim().is_empty() {
        return;
    }
    diagnostics.push(diagnostic(
        "error",
        "values.quota.auth_path",
        "quota.auth_path must be non-empty",
        "invalid_quota_auth_path",
    ));
}

fn diagnostic(
    severity: &str,
    path: impl Into<String>,
    message: impl Into<String>,
    code: &str,
) -> Value {
    json!({
        "severity": severity,
        "path": path.into(),
        "message": message.into(),
        "code": code,
    })
}

fn sanitize_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => sanitize_object(object),
        Value::Array(values) => Value::Array(values.iter().map(sanitize_value).collect()),
        _ => value.clone(),
    }
}

fn sanitize_object(object: &Map<String, Value>) -> Value {
    let mut sanitized = Map::new();
    for (key, value) in object {
        if is_secret_key(key) {
            continue;
        }
        sanitized.insert(key.clone(), sanitize_value(value));
    }
    Value::Object(sanitized)
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("token")
        || key.contains("secret")
        || key.contains("password")
        || key.contains("api_key")
}

fn legacy_actions(legacy: &Value) -> Vec<Value> {
    let mut actions = Vec::new();
    let providers = legacy_provider_names(legacy);
    for provider in providers {
        actions.push(json!({
            "kind": "settings_profile",
            "provider": provider,
            "operation": "create_or_update_provider_owned_profile",
        }));
    }
    if actions.is_empty() {
        actions.push(json!({
            "kind": "settings_profile",
            "operation": "inspect_legacy_opencode_tables",
        }));
    }
    actions
}

fn legacy_warnings(legacy: &Value) -> Vec<Value> {
    let mut warnings = vec![json!(
        "legacy live provider/model TOML is design input only; no live route cutover is performed"
    )];
    if legacy_models(legacy).is_empty() {
        warnings.push(json!("legacy input did not include model TOML entries"));
    }
    warnings
}

fn legacy_diagnostics(legacy: &Value) -> Vec<Value> {
    let mut diagnostics = Vec::new();
    if legacy_provider_names(legacy).is_empty() {
        diagnostics.push(diagnostic(
            "error",
            "legacy.providers_toml",
            "no opencode provider tables were found in legacy providers_toml",
            "legacy_providers_missing",
        ));
    }
    diagnostics
}

fn is_error_diagnostic(diagnostic: &Value) -> bool {
    diagnostic.get("severity").and_then(Value::as_str) == Some("error")
}

fn legacy_provider_names(legacy: &Value) -> Vec<String> {
    let Some(providers_toml) = legacy.get("providers_toml").and_then(Value::as_str) else {
        return Vec::new();
    };
    let Ok(parsed) = providers_toml.parse::<toml::Value>() else {
        return Vec::new();
    };
    let Some(table) = parsed.as_table() else {
        return Vec::new();
    };
    table
        .iter()
        .filter(|(name, value)| legacy_opencode_provider(name, value))
        .map(|(name, _)| name.clone())
        .collect()
}

fn legacy_opencode_provider(name: &str, value: &toml::Value) -> bool {
    name.starts_with("opencode")
        && value
            .get("command")
            .and_then(toml::Value::as_str)
            .is_some_and(|command| command.starts_with("opencode"))
}

fn legacy_models(legacy: &Value) -> Vec<String> {
    legacy
        .get("models")
        .and_then(Value::as_object)
        .map(|models| models.keys().cloned().collect())
        .unwrap_or_default()
}

fn write_migrated_settings(
    host: &HostContext,
    legacy: &Value,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let mut store = read_store(host, request_id)?;
    for provider in legacy_provider_names(legacy) {
        let values = migrated_values(&provider);
        store.records.push(new_record(Some(provider), values));
    }
    write_store(host, &store, request_id)
}

fn migrated_values(provider: &str) -> Value {
    let account = ACCOUNTS
        .iter()
        .find(|account| account.opencode_wrapper == provider)
        .unwrap_or(&ACCOUNTS[0]);
    json!({
        "provider": "opencode",
        "profile": account.opencode_wrapper,
        "wrapper": account.opencode_wrapper,
        "model": {
            "name": "gpt-high",
            "provider_model": "openai/gpt-5.5",
            "variant": "high"
        },
        "quota": {
            "source": "codex",
            "auth_path": account.codex_auth_path,
            "usage_command": "chatgpt-usage"
        },
        "launch": {
            "format": "json",
            "dangerously_skip_permissions": true
        }
    })
}
