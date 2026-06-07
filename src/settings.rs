//! Declared roles: accessor, validator, mapper, parser, predicate, filter, orchestration, formatter
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
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope, CATEGORY_CONFLICT};
use crate::models::{default_model_effort, effort_values, DEFAULT_MODEL_ALIAS, PROVIDER_MODEL};
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

pub fn handle(subcommand: &str, request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let RequestEnvelope {
        host,
        params,
        request_id,
        ..
    } = request;
    match subcommand {
        "settings.list" => list_params(&host, &request_id),
        "settings.get" => get_params(&host, params, &request_id),
        "settings.create" => create_params(&host, params, &request_id),
        "settings.update" => update_params(&host, params, &request_id),
        "settings.delete" => delete_params(&host, params, &request_id),
        "settings.validate" => validate_params(params, &request_id),
        "settings.migrate" => migrate_params(&host, params, &request_id),
        unknown => Err(unknown_settings_subcommand_failure(request_id, unknown)),
    }
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
    Ok(settings_list_result(&store.records))
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
    Ok(settings_get_result(record))
}

pub fn create_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params: SettingsCreateParams =
        parse_params(params, request_id, "invalid_settings_create_params")?;
    let values = normalize_settings_value(sanitize_value(&params.values));
    let diagnostics = validate_values(&values);
    let mut store = read_store(host, request_id)?;
    let record = new_record(params.display_name, values);
    insert_record(&mut store, record.clone());
    write_store(host, &store, request_id)?;
    Ok(settings_record_result(&record, diagnostics))
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
    ensure_version(indexed_record(&store, index), &params.version, request_id)?;
    let values = normalize_settings_value(sanitize_value(&params.values));
    let diagnostics = validate_values(&values);
    let record = update_record(&mut store, index, &params.id, values);
    write_store(host, &store, request_id)?;
    Ok(settings_record_result(&record, diagnostics))
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
    ensure_version(indexed_record(&store, index), &params.version, request_id)?;
    remove_record(&mut store, index);
    write_store(host, &store, request_id)?;
    Ok(settings_delete_result(params.id))
}

pub fn validate_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params: SettingsValidateParams =
        parse_params(params, request_id, "invalid_settings_validate_params")?;
    let values = normalize_settings_value(sanitize_value(&params.values));
    let diagnostics = validate_values(&values);
    let valid = settings_valid(&diagnostics);
    Ok(settings_validate_result(valid, diagnostics))
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
    let requires_user_input = settings_requires_user_input(&diagnostics);
    Ok(settings_migrate_result(
        actions,
        warnings,
        requires_user_input,
        diagnostics,
    ))
}

fn settings_list_result(records: &[SettingsRecord]) -> Value {
    json!({
        "records": records.iter().map(record_summary).collect::<Vec<_>>(),
    })
}

fn settings_get_result(record: &SettingsRecord) -> Value {
    json!({ "record": record_json(record) })
}

fn settings_record_result(record: &SettingsRecord, diagnostics: Vec<Value>) -> Value {
    json!({ "record": record_json(record), "diagnostics": diagnostics })
}

fn settings_delete_result(id: String) -> Value {
    json!({ "deleted": true, "id": id })
}

fn settings_valid(diagnostics: &[Value]) -> bool {
    diagnostics.is_empty()
}

fn settings_validate_result(valid: bool, diagnostics: Vec<Value>) -> Value {
    json!({ "valid": valid, "diagnostics": diagnostics })
}

fn settings_migrate_result(
    actions: Vec<Value>,
    warnings: Vec<Value>,
    requires_user_input: bool,
    diagnostics: Vec<Value>,
) -> Value {
    json!({
        "actions": actions,
        "warnings": warnings,
        "requires_user_input": requires_user_input,
        "diagnostics": diagnostics,
    })
}

fn settings_requires_user_input(diagnostics: &[Value]) -> bool {
    diagnostics.iter().any(is_error_diagnostic)
}

fn parse_params<T: for<'de> Deserialize<'de>>(
    params: Value,
    request_id: &str,
    code: &'static str,
) -> Result<T, ProviderFailure> {
    serde_json::from_value(params)
        .map_err(|err| invalid_settings_params_failure(request_id, code, err))
}

fn read_store(host: &HostContext, request_id: &str) -> Result<SettingsStore, ProviderFailure> {
    let path = store_path(host, request_id)?;
    read_store_path(&path, request_id)
}

fn read_store_path(path: &Path, request_id: &str) -> Result<SettingsStore, ProviderFailure> {
    if !store_path_exists(path) {
        return Ok(SettingsStore::default());
    }
    let bytes = read_store_bytes(path, request_id)?;
    parse_store_bytes(&bytes, request_id)
}

fn store_path_exists(path: &Path) -> bool {
    path.exists()
}

fn read_store_bytes(path: &Path, request_id: &str) -> Result<Vec<u8>, ProviderFailure> {
    fs::read(path).map_err(|err| store_io_failure(request_id, "settings_store_read_failed", err))
}

fn parse_store_bytes(bytes: &[u8], request_id: &str) -> Result<SettingsStore, ProviderFailure> {
    serde_json::from_slice(bytes).map_err(|err| settings_store_parse_failure(request_id, err))
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
    let tmp = store_temp_path(parent);
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
    let bytes = serialize_store(store, request_id)?;
    write_store_temp_bytes(path, &bytes, request_id)
}

fn serialize_store(store: &SettingsStore, request_id: &str) -> Result<Vec<u8>, ProviderFailure> {
    serde_json::to_vec(store).map_err(|err| settings_store_serialize_failure(request_id, err))
}

fn write_store_temp_bytes(
    path: &Path,
    bytes: &[u8],
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let mut file = fs::File::create(path)
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_create_failed", err))?;
    file.write_all(bytes)
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_write_failed", err))?;
    file.sync_all()
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_sync_failed", err))
}

fn store_path(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    Ok(store_path_from_root(&config_root(host, request_id)?))
}

fn config_root(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let Some(root) = host_config_root(host) else {
        return Err(missing_config_root_failure(request_id));
    };
    Ok(PathBuf::from(root))
}

fn host_config_root(host: &HostContext) -> Option<&str> {
    non_empty_config_root(raw_host_config_root(host))
}

fn raw_host_config_root(host: &HostContext) -> Option<&str> {
    host.config_root.as_deref()
}

fn non_empty_config_root(root: Option<&str>) -> Option<&str> {
    root.filter(|root| non_empty_text(root))
}

fn non_empty_text(value: &str) -> bool {
    !value.trim().is_empty()
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
    validate_store_path_contained(&canonical_target, &canonical_root, request_id)
}

fn validate_store_path_contained(
    canonical_target: &Path,
    canonical_root: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if canonical_target.starts_with(canonical_root) {
        return Ok(());
    }
    Err(settings_store_outside_provider_root_failure(request_id))
}

fn canonical_store_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    fs::canonicalize(path).map_err(|err| settings_store_canonicalize_failure(request_id, err))
}

fn canonical_store_create_path(path: &Path, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let existing = existing_store_ancestor_path(path, request_id)?;
    let mut canonical = canonical_store_path(existing, request_id)?;
    append_store_suffix(
        &mut canonical,
        store_path_suffix(path, existing, request_id)?,
        request_id,
    )?;
    Ok(canonical)
}

fn existing_store_ancestor_path<'a>(
    path: &'a Path,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    existing_store_ancestor(path).ok_or_else(|| settings_store_missing_ancestor_failure(request_id))
}

fn store_path_suffix<'a>(
    path: &'a Path,
    existing: &'a Path,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    path.strip_prefix(existing)
        .map_err(|_| settings_store_outside_provider_root_failure(request_id))
}

fn append_store_suffix(
    canonical: &mut PathBuf,
    suffix: &Path,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    for component in suffix.components() {
        push_store_component(canonical, component, request_id)?;
    }
    Ok(())
}

fn push_store_component(
    canonical: &mut PathBuf,
    component: std::path::Component<'_>,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    ensure_store_component_pushable(component, request_id)?;
    push_valid_store_component(canonical, component);
    Ok(())
}

fn ensure_store_component_pushable(
    component: std::path::Component<'_>,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if is_store_component_pushable(component) {
        return Ok(());
    }
    Err(settings_store_outside_provider_root_failure(request_id))
}

fn is_store_component_pushable(component: std::path::Component<'_>) -> bool {
    matches!(
        component,
        std::path::Component::Normal(_) | std::path::Component::CurDir
    )
}

fn push_valid_store_component(canonical: &mut PathBuf, component: std::path::Component<'_>) {
    if let std::path::Component::Normal(part) = component {
        canonical.push(part);
    }
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
    selected_record(store, id).ok_or_else(|| settings_not_found_failure(request_id))
}

fn selected_record<'a>(store: &'a SettingsStore, id: &str) -> Option<&'a SettingsRecord> {
    store.records.iter().find(|record| record.id == id)
}

fn record_index(
    store: &SettingsStore,
    id: &str,
    request_id: &str,
) -> Result<usize, ProviderFailure> {
    selected_record_index(store, id).ok_or_else(|| settings_not_found_failure(request_id))
}

fn selected_record_index(store: &SettingsStore, id: &str) -> Option<usize> {
    store.records.iter().position(|record| record.id == id)
}

fn indexed_record(store: &SettingsStore, index: usize) -> &SettingsRecord {
    &store.records[index]
}

fn indexed_record_mut(store: &mut SettingsStore, index: usize) -> &mut SettingsRecord {
    &mut store.records[index]
}

fn insert_record(store: &mut SettingsStore, record: SettingsRecord) {
    store.records.push(record);
}

fn update_record(
    store: &mut SettingsStore,
    index: usize,
    id: &str,
    values: Value,
) -> SettingsRecord {
    let version = version_token(id, &values);
    replace_record(indexed_record_mut(store, index), version, values);
    indexed_record(store, index).clone()
}

fn replace_record(record: &mut SettingsRecord, version: String, values: Value) {
    record.version = version;
    record.values = values;
}

fn remove_record(store: &mut SettingsStore, index: usize) {
    store.records.remove(index);
}

fn ensure_version(
    record: &SettingsRecord,
    version: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if record.version == version {
        return Ok(());
    }
    Err(stale_settings_version_failure(request_id))
}

fn new_record(display_name: Option<String>, values: Value) -> SettingsRecord {
    let id = settings_id(&values);
    SettingsRecord {
        display_name: record_display_name(display_name, &values),
        version: version_token(&id, &values),
        id,
        values,
    }
}

fn record_display_name(display_name: Option<String>, values: &Value) -> String {
    non_empty_display_name(display_name).unwrap_or_else(|| default_display_name(values))
}

fn non_empty_display_name(display_name: Option<String>) -> Option<String> {
    display_name.filter(|name| non_empty_text(name))
}

fn settings_id(values: &Value) -> String {
    settings_id_for_base(settings_id_base(values))
}

fn settings_id_base(values: &Value) -> &str {
    values
        .get("wrapper")
        .and_then(Value::as_str)
        .or_else(|| values.get("profile").and_then(Value::as_str))
        .unwrap_or("opencode")
}

fn settings_id_for_base(wrapper: &str) -> String {
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
    if has_expected_string(values, key, expected) {
        return;
    }
    diagnostics.push(required_string_diagnostic(key, expected));
}

fn has_expected_string(values: &Value, key: &str, expected: &str) -> bool {
    values.get(key).and_then(Value::as_str) == Some(expected)
}

fn required_string_diagnostic(key: &str, expected: &str) -> Value {
    diagnostic(
        "error",
        format!("values.{key}"),
        format!("{key} must be {expected}"),
        "invalid_settings_value",
    )
}

fn require_known_wrapper(values: &Value, diagnostics: &mut Vec<Value>) {
    if known_wrapper(wrapper_value(values)) {
        return;
    }
    diagnostics.push(invalid_wrapper_diagnostic());
}

fn wrapper_value(values: &Value) -> &str {
    values
        .get("wrapper")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn known_wrapper(wrapper: &str) -> bool {
    ACCOUNTS
        .iter()
        .any(|account| account.opencode_wrapper == wrapper)
}

fn invalid_wrapper_diagnostic() -> Value {
    diagnostic(
        "error",
        "values.wrapper",
        "wrapper must be one of opencode1 through opencode5",
        "invalid_wrapper",
    )
}

fn require_model(values: &Value, diagnostics: &mut Vec<Value>) {
    require_provider_model(values, diagnostics);
    require_model_variant(values, diagnostics);
}

fn require_provider_model(values: &Value, diagnostics: &mut Vec<Value>) {
    if provider_model_value(values) == Some(PROVIDER_MODEL) {
        return;
    }
    diagnostics.push(invalid_provider_model_diagnostic());
}

fn provider_model_value(values: &Value) -> Option<&str> {
    values
        .pointer("/model/provider_model")
        .and_then(Value::as_str)
}

fn invalid_provider_model_diagnostic() -> Value {
    diagnostic(
        "error",
        "values.model.provider_model",
        "provider_model must be openai/gpt-5.5",
        "invalid_provider_model",
    )
}

fn require_model_variant(values: &Value, diagnostics: &mut Vec<Value>) {
    if known_model_variant(model_variant_value(values)) {
        return;
    }
    diagnostics.push(invalid_model_variant_diagnostic());
}

fn model_variant_value(values: &Value) -> Option<&str> {
    values.pointer("/model/variant").and_then(Value::as_str)
}

fn known_model_variant(variant: Option<&str>) -> bool {
    let valid_efforts = effort_values();
    variant.is_some_and(|variant| valid_efforts.contains(&variant))
}

fn invalid_model_variant_diagnostic() -> Value {
    diagnostic(
        "error",
        "values.model.variant",
        "variant must be none, low, medium, high, or xhigh",
        "invalid_model_variant",
    )
}

fn require_quota(values: &Value, diagnostics: &mut Vec<Value>) {
    if has_quota_auth_path(values) {
        return;
    }
    diagnostics.push(invalid_quota_auth_path_diagnostic());
}

fn has_quota_auth_path(values: &Value) -> bool {
    quota_auth_path(values).is_some_and(non_empty_text)
}

fn quota_auth_path(values: &Value) -> Option<&str> {
    values.pointer("/quota/auth_path").and_then(Value::as_str)
}

fn invalid_quota_auth_path_diagnostic() -> Value {
    diagnostic(
        "error",
        "values.quota.auth_path",
        "quota.auth_path must be non-empty",
        "invalid_quota_auth_path",
    )
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
    let entries = non_secret_entries(object);
    Value::Object(sanitized_entries(entries))
}

fn non_secret_entries(object: &Map<String, Value>) -> Vec<(&String, &Value)> {
    object
        .iter()
        .filter(|(key, _)| !is_secret_key(key))
        .collect()
}

fn sanitized_entries(entries: Vec<(&String, &Value)>) -> Map<String, Value> {
    let mut sanitized = Map::new();
    for (key, value) in entries {
        sanitized.insert(key.clone(), sanitize_value(value));
    }
    sanitized
}

fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("token")
        || key.contains("secret")
        || key.contains("password")
        || key.contains("api_key")
}

fn normalize_settings_value(value: Value) -> Value {
    let Some(account) = settings_value_account(&value) else {
        return value;
    };
    normalize_account_settings_value(value, account)
}

fn settings_value_account(value: &Value) -> Option<&'static crate::account::AccountProfile> {
    settings_account_reference(value).and_then(account_for_settings_reference)
}

fn settings_account_reference(value: &Value) -> Option<&str> {
    value
        .get("wrapper")
        .and_then(Value::as_str)
        .or_else(|| value.get("profile").and_then(Value::as_str))
}

fn account_for_settings_reference(
    reference: &str,
) -> Option<&'static crate::account::AccountProfile> {
    let basename = settings_reference_basename(reference);
    ACCOUNTS
        .iter()
        .find(|account| account.opencode_wrapper == basename)
        .or_else(|| account_one_for_plain_opencode(basename))
}

fn account_one_for_plain_opencode(
    basename: &str,
) -> Option<&'static crate::account::AccountProfile> {
    (basename == "opencode").then_some(&ACCOUNTS[0])
}

fn settings_reference_basename(reference: &str) -> &str {
    Path::new(reference)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(reference)
}

fn normalize_account_settings_value(
    mut value: Value,
    account: &crate::account::AccountProfile,
) -> Value {
    if let Value::Object(object) = &mut value {
        object.insert("provider".to_string(), json!("opencode"));
        object.insert("profile".to_string(), json!(account.opencode_wrapper));
        object.insert("wrapper".to_string(), json!(account.opencode_wrapper));
        normalize_quota_value(object, account);
        normalize_launch_value(object);
    }
    value
}

fn normalize_quota_value(
    object: &mut Map<String, Value>,
    account: &crate::account::AccountProfile,
) {
    let quota = child_object(object, "quota");
    quota.insert("source".to_string(), json!("codex"));
    quota.insert("auth_path".to_string(), json!(account.codex_auth_path));
    quota.insert("usage_command".to_string(), json!("chatgpt-usage"));
}

fn normalize_launch_value(object: &mut Map<String, Value>) {
    let launch = child_object(object, "launch");
    launch.insert("format".to_string(), json!("json"));
    launch.insert("dangerously_skip_permissions".to_string(), json!(true));
}

fn child_object<'a>(object: &'a mut Map<String, Value>, key: &str) -> &'a mut Map<String, Value> {
    let value = object.entry(key.to_string()).or_insert_with(|| json!({}));
    if !value.is_object() {
        *value = json!({});
    }
    value
        .as_object_mut()
        .expect("child value normalized to object")
}

fn legacy_actions(legacy: &Value) -> Vec<Value> {
    let providers = legacy_provider_names(legacy);
    legacy_actions_for_providers(&providers)
}

fn legacy_actions_for_providers(providers: &[String]) -> Vec<Value> {
    let mut actions = legacy_provider_actions(providers);
    if legacy_providers_empty(providers) {
        actions.push(legacy_inspect_tables_action());
    }
    actions
}

fn legacy_provider_actions(providers: &[String]) -> Vec<Value> {
    providers
        .iter()
        .map(|provider| legacy_provider_action(provider))
        .collect()
}

fn legacy_provider_action(provider: &str) -> Value {
    json!({
        "kind": "settings_profile",
        "provider": provider,
        "operation": "create_or_update_provider_owned_profile",
    })
}

fn legacy_providers_empty(providers: &[String]) -> bool {
    providers.is_empty()
}

fn legacy_inspect_tables_action() -> Value {
    json!({
        "kind": "settings_profile",
        "operation": "inspect_legacy_opencode_tables",
    })
}

fn legacy_warnings(legacy: &Value) -> Vec<Value> {
    let models = legacy_models(legacy);
    legacy_warnings_for_model_state(legacy_models_empty(&models))
}

fn legacy_warnings_for_model_state(models_empty: bool) -> Vec<Value> {
    let mut warnings = vec![json!(
        "legacy live provider/model TOML is design input only; no live route cutover is performed"
    )];
    if models_empty {
        warnings.push(json!("legacy input did not include model TOML entries"));
    }
    warnings
}

fn legacy_models_empty(models: &[String]) -> bool {
    models.is_empty()
}

fn legacy_diagnostics(legacy: &Value) -> Vec<Value> {
    legacy_diagnostics_for_providers(&legacy_provider_names(legacy))
}

fn legacy_diagnostics_for_providers(providers: &[String]) -> Vec<Value> {
    let mut diagnostics = Vec::new();
    if providers.is_empty() {
        diagnostics.push(legacy_providers_missing_diagnostic());
    }
    diagnostics
}

fn legacy_providers_missing_diagnostic() -> Value {
    diagnostic(
        "error",
        "legacy.providers_toml",
        "no opencode provider tables were found in legacy providers_toml",
        "legacy_providers_missing",
    )
}

fn is_error_diagnostic(diagnostic: &Value) -> bool {
    diagnostic.get("severity").and_then(Value::as_str) == Some("error")
}

fn legacy_provider_names(legacy: &Value) -> Vec<String> {
    let Some(parsed) = legacy_providers_toml(legacy) else {
        return Vec::new();
    };
    legacy_provider_names_from_toml(&parsed)
}

fn legacy_provider_names_from_toml(parsed: &toml::Value) -> Vec<String> {
    legacy_provider_names_from_table(legacy_provider_table(parsed))
}

fn legacy_provider_table(parsed: &toml::Value) -> Option<&toml::Table> {
    parsed.as_table()
}

fn legacy_provider_names_from_table(table: Option<&toml::Table>) -> Vec<String> {
    let Some(table) = table else {
        return Vec::new();
    };
    legacy_opencode_provider_names(table.iter())
}

fn legacy_opencode_provider_names<'a>(
    providers: impl Iterator<Item = (&'a String, &'a toml::Value)>,
) -> Vec<String> {
    legacy_opencode_provider_name_entries(providers)
        .into_iter()
        .map(legacy_provider_name)
        .collect()
}

fn legacy_opencode_provider_name_entries<'a>(
    providers: impl Iterator<Item = (&'a String, &'a toml::Value)>,
) -> Vec<(&'a String, &'a toml::Value)> {
    providers
        .filter(|(name, value)| legacy_opencode_provider(name, value))
        .collect()
}

fn legacy_provider_name(entry: (&String, &toml::Value)) -> String {
    entry.0.clone()
}

fn legacy_opencode_provider(name: &str, value: &toml::Value) -> bool {
    name.starts_with("opencode")
        && value
            .get("command")
            .and_then(toml::Value::as_str)
            .is_some_and(|command| command.starts_with("opencode"))
}

fn legacy_models(legacy: &Value) -> Vec<String> {
    legacy_models_object(legacy)
        .map(legacy_model_names)
        .unwrap_or_default()
}

fn legacy_models_object(legacy: &Value) -> Option<&Map<String, Value>> {
    legacy.get("models").and_then(Value::as_object)
}

fn legacy_model_names(models: &Map<String, Value>) -> Vec<String> {
    models.keys().cloned().collect()
}

fn write_migrated_settings(
    host: &HostContext,
    legacy: &Value,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let mut store = read_store(host, request_id)?;
    append_records(&mut store, migrated_records(legacy_provider_names(legacy)));
    write_store(host, &store, request_id)
}

fn migrated_records(providers: Vec<String>) -> Vec<SettingsRecord> {
    providers.into_iter().map(migrated_record).collect()
}

fn migrated_record(provider: String) -> SettingsRecord {
    let values = migrated_values(&provider);
    new_record(Some(provider), values)
}

fn append_records(store: &mut SettingsStore, records: Vec<SettingsRecord>) {
    store.records.extend(records);
}

fn migrated_values(provider: &str) -> Value {
    migrated_values_for_account(migration_account(provider))
}

fn migration_account(provider: &str) -> &'static crate::account::AccountProfile {
    ACCOUNTS
        .iter()
        .find(|account| account.opencode_wrapper == provider)
        .unwrap_or(&ACCOUNTS[0])
}

fn migrated_values_for_account(account: &crate::account::AccountProfile) -> Value {
    json!({
        "provider": "opencode",
        "profile": account.opencode_wrapper,
        "wrapper": account.opencode_wrapper,
        "model": {
            "name": DEFAULT_MODEL_ALIAS,
            "provider_model": PROVIDER_MODEL,
            "variant": default_model_effort()
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

fn unknown_settings_subcommand_failure(request_id: String, unknown: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "unknown_settings_subcommand",
        format!("unknown settings subcommand: {unknown}"),
    )
}

fn invalid_settings_params_failure(
    request_id: &str,
    code: &'static str,
    err: serde_json::Error,
) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        code,
        format!("settings params are invalid: {err}"),
    )
}

fn settings_store_parse_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "settings_store_parse_failed",
        format!("provider settings store is invalid JSON: {err}"),
    )
}

fn settings_store_serialize_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "settings_store_serialize_failed",
        format!("failed to serialize provider settings store: {err}"),
    )
}

fn store_temp_path(parent: &Path) -> PathBuf {
    parent.join(format!(".{STORE_FILE}.{}.tmp", std::process::id()))
}

fn missing_config_root_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "missing_config_root",
        "settings store requires host.config_root",
    )
}

fn settings_store_outside_provider_root_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "settings_store_outside_provider_root",
        "settings store path must stay under host.config_root",
    )
}

fn settings_store_canonicalize_failure(request_id: &str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "settings_store_canonicalize_failed",
        format!("failed to canonicalize provider settings path: {err}"),
    )
}

fn existing_store_ancestor(path: &Path) -> Option<&Path> {
    path.ancestors().find(|ancestor| ancestor.exists())
}

fn settings_store_missing_ancestor_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "settings_store_outside_provider_root",
        "settings store path must have an existing host.config_root ancestor",
    )
}

fn settings_not_found_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "settings_not_found",
        "settings record was not found",
    )
}

fn stale_settings_version_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure {
        request_id: request_id.to_string(),
        category: CATEGORY_CONFLICT,
        code: "stale_settings_version",
        message: "settings version is stale".to_string(),
        details: json!({}),
        retryable: false,
        exit_code: 4,
    }
}

fn legacy_providers_toml(legacy: &Value) -> Option<toml::Value> {
    let providers_toml = legacy_providers_toml_text(legacy)?;
    parse_legacy_providers_toml(providers_toml)
}

fn legacy_providers_toml_text(legacy: &Value) -> Option<&str> {
    legacy.get("providers_toml").and_then(Value::as_str)
}

fn parse_legacy_providers_toml(providers_toml: &str) -> Option<toml::Value> {
    providers_toml.parse::<toml::Value>().ok()
}
