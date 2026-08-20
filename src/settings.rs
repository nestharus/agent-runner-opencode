//! Declared roles: accessor, validator, mapper, parser, predicate, filter, orchestration, formatter
//! intrinsic_surface_declarations:
//!   - component: src/settings.rs
//!     role: intrinsic-surface
//!     Domain: provider-owned opencode settings store
//!     Owns:
//!       - profile record persistence rooted at host.config_root
//!       - opaque settings version tokens and stale-write conflict detection
//!       - record normalization, sanitization, and legacy-store mapping

use crate::account::{profile_for_wrapper_reference, AccountProfile};
use crate::activity::ActivityTargets;
use crate::encoding::{now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure, RequestEnvelope, CATEGORY_CONFLICT};
use crate::models::{default_model, model_alias, DEFAULT_MODEL_ALIAS};
use crate::path_guard;
use crate::settings_definition::{model_name_value, validate_values, wrapper_value};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const STORE_DIR: &str = "agent-runner-opencode";
const STORE_FILE: &str = "settings-store.json";
const STORE_LOCK_FILE: &str = ".settings-store.lock";
const CURRENT_STORE_SCHEMA_VERSION: u32 = 3;

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

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Command {
    List,
    Get,
    Create,
    Update,
    Delete,
    Validate,
    Migrate,
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
        Command::List => list_params(&host, &request_id),
        Command::Get => get_params(&host, params, &request_id),
        Command::Create => {
            create_params(&host, params, &request_id, provider_instance_id.as_deref())
        }
        Command::Update => {
            update_params(&host, params, &request_id, provider_instance_id.as_deref())
        }
        Command::Delete => {
            delete_params(&host, params, &request_id, provider_instance_id.as_deref())
        }
        Command::Validate => validate_params(params, &request_id),
        Command::Migrate => {
            migrate_params(&host, params, &request_id, provider_instance_id.as_deref())
        }
    }
}

pub(crate) fn activity_targets(
    command: Command,
    params: &Value,
    result: Option<&Value>,
) -> ActivityTargets {
    let mut targets = ActivityTargets::default();
    if matches!(command, Command::Get | Command::Update | Command::Delete) {
        if let Some(id) = non_empty_value(params.get("id")) {
            targets.attempted("settings_record", id, "params.id");
        }
    }
    if let Some(values) = params.get("values") {
        append_settings_value_activity_targets(&mut targets, values, "params.values", false);
    }
    let Some(result) = result else {
        return targets;
    };
    if let Some(record) = result.get("record") {
        append_settings_record_activity_targets(&mut targets, record, command == Command::Create);
    }
    if let Some(records) = result.get("records").and_then(Value::as_array) {
        for record in records {
            append_settings_record_activity_targets(&mut targets, record, false);
        }
    }
    if command == Command::Delete {
        if let Some(id) = non_empty_value(result.get("id")) {
            targets.resolved("settings_record", id, "result.id");
        }
    }
    targets
}

fn append_settings_record_activity_targets(
    targets: &mut ActivityTargets,
    record: &Value,
    generated: bool,
) {
    if let Some(id) = non_empty_value(record.get("id")) {
        if generated {
            targets.generated("settings_record", id, "result.record.id");
        } else {
            targets.resolved("settings_record", id, "result.record.id");
        }
    }
    if let Some(values) = record.get("values") {
        append_settings_value_activity_targets(targets, values, "result.record.values", true);
    }
}

fn append_settings_value_activity_targets(
    targets: &mut ActivityTargets,
    values: &Value,
    provenance: &'static str,
    resolved: bool,
) {
    let wrapper = wrapper_value(values);
    if !wrapper.trim().is_empty() {
        let canonical = profile_for_wrapper_reference(wrapper)
            .map(|profile| profile.opencode_wrapper)
            .unwrap_or(wrapper);
        if resolved {
            targets.resolved("account", canonical, format!("{provenance}.wrapper"));
        } else {
            targets.attempted("account", wrapper, format!("{provenance}.wrapper"));
            if canonical != wrapper {
                targets.resolved(
                    "account",
                    canonical,
                    format!("{provenance}.wrapper.catalog"),
                );
            }
        }
    }
    if let Some(model_name) = model_name_value(values) {
        if resolved {
            targets.resolved(
                "model_alias",
                model_name,
                format!("{provenance}.model.name"),
            );
        } else {
            targets.attempted(
                "model_alias",
                model_name,
                format!("{provenance}.model.name"),
            );
        }
    }
}

fn non_empty_value(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

#[derive(Serialize, Deserialize)]
struct SettingsStore {
    #[serde(default)]
    schema_version: u32,
    records: Vec<SettingsRecord>,
    #[serde(default)]
    history: Vec<Value>,
    #[serde(default)]
    mutation_receipts: BTreeMap<String, SettingsMutationReceipt>,
}

impl Default for SettingsStore {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_STORE_SCHEMA_VERSION,
            records: Vec::new(),
            history: Vec::new(),
            mutation_receipts: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct SettingsMutationReceipt {
    operation: String,
    binding_sha256: String,
    result: Value,
    recorded_at_unix_ms: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct SettingsRecord {
    id: String,
    display_name: String,
    version: String,
    values: Value,
}

pub(crate) struct PersistedRuntimeRecord {
    pub record_id: String,
    pub record_version: String,
    pub account: &'static AccountProfile,
    pub model: Option<&'static crate::models::ModelAlias>,
}

pub(crate) fn resolve_persisted_runtime_record(
    host: &HostContext,
    reference: &str,
    request_id: &str,
) -> Result<PersistedRuntimeRecord, ProviderFailure> {
    let store = read_store(host, request_id)?;
    let record = find_record(&store, reference, request_id)?;
    let diagnostics = validate_values(&record.values);
    ensure_valid_settings(request_id, &diagnostics)?;
    let wrapper = wrapper_value(&record.values);
    let account = account_for_settings_reference(wrapper)
        .ok_or_else(|| settings_not_found_failure(request_id))?;
    let model = model_name_value(&record.values).and_then(model_alias);
    Ok(PersistedRuntimeRecord {
        record_id: record.id.clone(),
        record_version: record.version.clone(),
        account,
        model,
    })
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
    provider_instance_id: Option<&str>,
) -> Result<Value, ProviderFailure> {
    let binding_sha256 = settings_mutation_binding_sha256(
        "settings.create",
        &params,
        provider_instance_id,
        &host.app,
    );
    mutate_store(
        host,
        request_id,
        provider_instance_id,
        "settings.create",
        &binding_sha256,
        |store| {
            let params: SettingsCreateParams =
                parse_params(params, request_id, "invalid_settings_create_params")?;
            let values = normalize_settings_value(sanitize_value(&params.values));
            let diagnostics = validate_values(&values);
            ensure_valid_settings(request_id, &diagnostics)?;
            let record = new_record(store, params.display_name, values, request_id);
            store.records.push(record.clone());
            Ok(settings_record_result(&record, diagnostics))
        },
    )
}

pub fn update_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
    provider_instance_id: Option<&str>,
) -> Result<Value, ProviderFailure> {
    let binding_sha256 = settings_mutation_binding_sha256(
        "settings.update",
        &params,
        provider_instance_id,
        &host.app,
    );
    mutate_store(
        host,
        request_id,
        provider_instance_id,
        "settings.update",
        &binding_sha256,
        |store| {
            let params: SettingsUpdateParams =
                parse_params(params, request_id, "invalid_settings_update_params")?;
            let values = normalize_settings_value(sanitize_value(&params.values));
            let diagnostics = validate_values(&values);
            ensure_valid_settings(request_id, &diagnostics)?;
            let index = record_index(store, &params.id, request_id)?;
            ensure_version(&store.records[index], &params.version, request_id)?;
            let record = update_record(store, index, &params.id, values, request_id);
            Ok(settings_record_result(&record, diagnostics))
        },
    )
}

pub fn delete_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
    provider_instance_id: Option<&str>,
) -> Result<Value, ProviderFailure> {
    let binding_sha256 = settings_mutation_binding_sha256(
        "settings.delete",
        &params,
        provider_instance_id,
        &host.app,
    );
    mutate_store(
        host,
        request_id,
        provider_instance_id,
        "settings.delete",
        &binding_sha256,
        |store| {
            let params: SettingsDeleteParams =
                parse_params(params, request_id, "invalid_settings_delete_params")?;
            let index = record_index(store, &params.id, request_id)?;
            ensure_version(&store.records[index], &params.version, request_id)?;
            store.records.remove(index);
            Ok(settings_delete_result(params.id))
        },
    )
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
    provider_instance_id: Option<&str>,
) -> Result<Value, ProviderFailure> {
    let parsed: SettingsMigrateParams = parse_params(
        params.clone(),
        request_id,
        "invalid_settings_migrate_params",
    )?;
    if parsed.dry_run {
        let actions = legacy_actions(&parsed.legacy);
        let warnings = legacy_warnings(&parsed.legacy);
        let diagnostics = legacy_diagnostics(&parsed.legacy);
        let requires_user_input = settings_requires_user_input(&diagnostics);
        return Ok(settings_migrate_result(
            actions,
            warnings,
            requires_user_input,
            diagnostics,
        ));
    }
    let binding_sha256 = settings_mutation_binding_sha256(
        "settings.migrate",
        &params,
        provider_instance_id,
        &host.app,
    );
    mutate_store(
        host,
        request_id,
        provider_instance_id,
        "settings.migrate",
        &binding_sha256,
        |store| {
            let params: SettingsMigrateParams =
                parse_params(params, request_id, "invalid_settings_migrate_params")?;
            let actions = legacy_actions(&params.legacy);
            let warnings = legacy_warnings(&params.legacy);
            let diagnostics = legacy_diagnostics(&params.legacy);
            ensure_valid_settings(request_id, &diagnostics)?;
            for provider in legacy_provider_names(&params.legacy) {
                upsert_migrated_record(store, &provider, request_id)?;
            }
            let requires_user_input = settings_requires_user_input(&diagnostics);
            Ok(settings_migrate_result(
                actions,
                warnings,
                requires_user_input,
                diagnostics,
            ))
        },
    )
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

fn ensure_valid_settings(request_id: &str, diagnostics: &[Value]) -> Result<(), ProviderFailure> {
    if settings_valid(diagnostics) {
        return Ok(());
    }
    Err(ProviderFailure::invalid_settings(
        request_id,
        "settings_validation_failed",
        "provider settings contain error diagnostics",
        json!({ "diagnostics": diagnostics }),
    ))
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

fn mutate_store(
    host: &HostContext,
    request_id: &str,
    provider_instance_id: Option<&str>,
    operation: &str,
    binding_sha256: &str,
    mutation: impl FnOnce(&mut SettingsStore) -> Result<Value, ProviderFailure>,
) -> Result<Value, ProviderFailure> {
    let config_root = config_root(host, request_id)?;
    let path = store_path_from_root(&config_root);
    let parent = path.parent().expect("settings store always has parent");
    ensure_store_path_contained(parent, &config_root, request_id)?;
    fs::create_dir_all(parent)
        .map_err(|err| store_io_failure(request_id, "settings_store_create_dir_failed", err))?;
    let lock_path = parent.join(STORE_LOCK_FILE);
    ensure_store_path_contained(&lock_path, &config_root, request_id)?;
    ensure_store_path_contained(&path, &config_root, request_id)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|err| store_io_failure(request_id, "settings_store_lock_open_failed", err))?;
    lock.lock_exclusive()
        .map_err(|err| store_io_failure(request_id, "settings_store_lock_failed", err))?;
    let mut store = read_store_path(&path, request_id)?;
    if let Some(receipt) = store.mutation_receipts.get(request_id) {
        if receipt.operation == operation && receipt.binding_sha256 == binding_sha256 {
            return Ok(receipt.result.clone());
        }
        return Err(settings_mutation_request_conflict(
            request_id,
            operation,
            binding_sha256,
            receipt,
        ));
    }
    let before = store.records.clone();
    let result = mutation(&mut store)?;
    append_settings_history(
        &mut store,
        &before,
        operation,
        request_id,
        provider_instance_id,
        &host.app,
    );
    store.mutation_receipts.insert(
        request_id.to_string(),
        SettingsMutationReceipt {
            operation: operation.to_string(),
            binding_sha256: binding_sha256.to_string(),
            result: result.clone(),
            recorded_at_unix_ms: now_unix_ms(),
        },
    );
    write_store_path(&path, &config_root, &store, request_id)?;
    Ok(result)
}

fn settings_mutation_binding_sha256(
    operation: &str,
    params: &Value,
    provider_instance_id: Option<&str>,
    host_app: &str,
) -> String {
    sha256_hex(
        json!({
            "operation": operation,
            "params": params,
            "provider_instance_id": provider_instance_id,
            "host_app": host_app,
        })
        .to_string()
        .as_bytes(),
    )
}

fn append_settings_history(
    store: &mut SettingsStore,
    before: &[SettingsRecord],
    operation: &str,
    request_id: &str,
    provider_instance_id: Option<&str>,
    host_app: &str,
) {
    let previous_event_sha256 = store
        .history
        .last()
        .and_then(|entry| entry.get("event_sha256"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut event = json!({
        "sequence": store.history.len() + 1,
        "operation": operation,
        "request_id": request_id,
        "provider_instance_id": provider_instance_id,
        "host_app": host_app,
        "recorded_at_unix_ms": now_unix_ms(),
        "prior_records_sha256": records_digest(before),
        "result_records_sha256": records_digest(&store.records),
        "changes": settings_record_changes(before, &store.records),
        "previous_event_sha256": previous_event_sha256,
    });
    let event_sha256 = sha256_hex(event.to_string().as_bytes());
    event["event_sha256"] = json!(event_sha256);
    store.history.push(event);
}

fn records_digest(records: &[SettingsRecord]) -> String {
    sha256_hex(
        serde_json::to_string(records)
            .unwrap_or_default()
            .as_bytes(),
    )
}

fn settings_record_changes(before: &[SettingsRecord], after: &[SettingsRecord]) -> Vec<Value> {
    let before = records_by_id(before);
    let after = records_by_id(after);
    before
        .keys()
        .chain(after.keys())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter_map(|id| {
            let prior = before.get(*id).copied();
            let result = after.get(*id).copied();
            (record_identity(prior) != record_identity(result)).then(|| {
                json!({
                    "settings_id": id,
                    "prior_version": prior.map(|record| record.version.as_str()),
                    "result_version": result.map(|record| record.version.as_str()),
                    "prior_values_sha256": prior.map(|record| sha256_hex(record.values.to_string().as_bytes())),
                    "result_values_sha256": result.map(|record| sha256_hex(record.values.to_string().as_bytes())),
                    "tombstone": prior.is_some() && result.is_none(),
                })
            })
        })
        .collect()
}

fn records_by_id(records: &[SettingsRecord]) -> BTreeMap<&str, &SettingsRecord> {
    records
        .iter()
        .map(|record| (record.id.as_str(), record))
        .collect()
}

fn record_identity(record: Option<&SettingsRecord>) -> Option<(&str, String)> {
    record.map(|record| {
        (
            record.version.as_str(),
            sha256_hex(record.values.to_string().as_bytes()),
        )
    })
}

fn store_path_exists(path: &Path) -> bool {
    path.exists()
}

fn read_store_bytes(path: &Path, request_id: &str) -> Result<Vec<u8>, ProviderFailure> {
    fs::read(path).map_err(|err| store_io_failure(request_id, "settings_store_read_failed", err))
}

fn parse_store_bytes(bytes: &[u8], request_id: &str) -> Result<SettingsStore, ProviderFailure> {
    let store = serde_json::from_slice(bytes)
        .map_err(|err| settings_store_parse_failure(request_id, err))?;
    upgrade_persisted_store(store, request_id)
}

fn upgrade_persisted_store(
    mut store: SettingsStore,
    request_id: &str,
) -> Result<SettingsStore, ProviderFailure> {
    if store.schema_version > CURRENT_STORE_SCHEMA_VERSION {
        return Err(settings_store_upgrade_failure(
            request_id,
            format!(
                "settings store schema {} is newer than supported schema {}",
                store.schema_version, CURRENT_STORE_SCHEMA_VERSION
            ),
        ));
    }
    if store.schema_version == CURRENT_STORE_SCHEMA_VERSION {
        return Ok(store);
    }
    if store.schema_version < 2 {
        for record in &mut store.records {
            record.values = upgrade_persisted_values(&record.values).ok_or_else(|| {
                settings_store_upgrade_failure(
                    request_id,
                    format!(
                        "settings record {} cannot be upgraded automatically; back up and recreate that record",
                        record.id
                    ),
                )
            })?;
        }
    }
    store.schema_version = CURRENT_STORE_SCHEMA_VERSION;
    Ok(store)
}

fn upgrade_persisted_values(values: &Value) -> Option<Value> {
    let account = persisted_settings_account(values)?;
    let model = persisted_model_alias(values)?;
    let mut upgraded = json!({
        "provider": "opencode",
        "profile": account.opencode_wrapper,
        "wrapper": account.opencode_wrapper,
        "model": {
            "name": model.name,
            "provider_model": model.provider_model,
            "variant": model.effort,
        },
        "quota": {
            "source": account.quota_source_kind(),
            "auth_path": account.quota_auth_path(),
            "probe": account.quota_probe_kind(),
        },
        "launch": {
            "format": "json",
            "dangerously_skip_permissions": true,
        },
    });
    copy_persisted_optional_field(values, &mut upgraded, "extra_env");
    copy_persisted_optional_field(values, &mut upgraded, "working_directory");
    copy_persisted_optional_field(values, &mut upgraded, "mode");
    if let Some(preserve) = values
        .pointer("/launch/preserve_pure_wrapper")
        .and_then(Value::as_bool)
    {
        upgraded["launch"]["preserve_pure_wrapper"] = json!(preserve);
    }
    settings_valid(&validate_values(&upgraded)).then_some(upgraded)
}

fn persisted_settings_account(values: &Value) -> Option<&'static AccountProfile> {
    ["wrapper", "profile", "opencode_wrapper", "account"]
        .into_iter()
        .find_map(|key| values.get(key).and_then(Value::as_str))
        .and_then(account_for_settings_reference)
}

fn persisted_model_alias(values: &Value) -> Option<&'static crate::models::ModelAlias> {
    values
        .pointer("/model/name")
        .and_then(Value::as_str)
        .or_else(|| values.get("model").and_then(Value::as_str))
        .and_then(model_alias)
}

fn copy_persisted_optional_field(source: &Value, target: &mut Value, field: &str) {
    if let Some(value) = source.get(field) {
        target[field] = sanitize_value(value);
    }
}

fn write_store_path(
    path: &Path,
    config_root: &Path,
    store: &SettingsStore,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let parent = path.parent().expect("settings store always has parent");
    ensure_store_path_contained(parent, config_root, request_id)?;
    fs::create_dir_all(parent)
        .map_err(|err| store_io_failure(request_id, "settings_store_create_dir_failed", err))?;
    ensure_store_path_contained(path, config_root, request_id)?;
    let bytes = serialize_store(store, request_id)?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_create_failed", err))?;
    ensure_store_path_contained(tmp.path(), config_root, request_id)?;
    tmp.write_all(&bytes)
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_write_failed", err))?;
    tmp.as_file()
        .sync_all()
        .map_err(|err| store_io_failure(request_id, "settings_store_temp_sync_failed", err))?;
    tmp.persist(path)
        .map_err(|err| store_io_failure(request_id, "settings_store_rename_failed", err.error))?;
    sync_store_parent(parent, request_id)
}

fn serialize_store(store: &SettingsStore, request_id: &str) -> Result<Vec<u8>, ProviderFailure> {
    serde_json::to_vec(store).map_err(|err| settings_store_serialize_failure(request_id, err))
}

fn sync_store_parent(parent: &Path, request_id: &str) -> Result<(), ProviderFailure> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| store_io_failure(request_id, "settings_store_parent_sync_failed", err))
}

fn store_path(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let config_root = config_root(host, request_id)?;
    let path = store_path_from_root(&config_root);
    ensure_store_path_contained(&path, &config_root, request_id)?;
    Ok(path)
}

fn config_root(host: &HostContext, request_id: &str) -> Result<PathBuf, ProviderFailure> {
    let Some(root) = host
        .config_root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
    else {
        return Err(missing_config_root_failure(request_id));
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
    path_guard::confined_target(config_root, path)
        .map(|_| ())
        .map_err(|err| settings_store_confinement_failure(request_id, err))
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
        .ok_or_else(|| settings_not_found_failure(request_id))
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
        .ok_or_else(|| settings_not_found_failure(request_id))
}

fn update_record(
    store: &mut SettingsStore,
    index: usize,
    id: &str,
    values: Value,
    request_id: &str,
) -> SettingsRecord {
    let version = version_token(id, &values, request_id);
    let record = &mut store.records[index];
    record.version = version;
    record.values = values;
    record.clone()
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

fn new_record(
    store: &SettingsStore,
    display_name: Option<String>,
    values: Value,
    request_id: &str,
) -> SettingsRecord {
    let id = unique_settings_id(store, &values, request_id);
    SettingsRecord {
        display_name: record_display_name(display_name, &values),
        version: version_token(&id, &values, request_id),
        id,
        values,
    }
}

fn record_display_name(display_name: Option<String>, values: &Value) -> String {
    display_name
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| default_display_name(values))
}

fn unique_settings_id(store: &SettingsStore, values: &Value, request_id: &str) -> String {
    let base = settings_id_base(values);
    (0_u64..)
        .map(|attempt| settings_id_for_base(base, request_id, attempt))
        .find(|candidate| !store.records.iter().any(|record| record.id == *candidate))
        .expect("unbounded settings identifier search must find a free identity")
}

fn settings_id_base(values: &Value) -> &str {
    values
        .get("wrapper")
        .and_then(Value::as_str)
        .or_else(|| values.get("profile").and_then(Value::as_str))
        .unwrap_or("opencode")
}

fn settings_id_for_base(wrapper: &str, request_id: &str, attempt: u64) -> String {
    let digest =
        sha256_hex(format!("{wrapper}:{request_id}:{}:{attempt}", now_unix_ms()).as_bytes());
    format!("{wrapper}-{}", &digest[..24])
}

fn version_token(id: &str, values: &Value, request_id: &str) -> String {
    let digest = sha256_hex(format!("{id}:{request_id}:{}:{values}", now_unix_ms()).as_bytes());
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

fn sanitize_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .filter(|(key, _)| !is_secret_key(key))
                .map(|(key, value)| (key.clone(), sanitize_value(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(sanitize_value).collect()),
        _ => value.clone(),
    }
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
    profile_for_wrapper_reference(reference)
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
    quota.insert("source".to_string(), json!(account.quota_source_kind()));
    quota.insert("auth_path".to_string(), json!(account.quota_auth_path()));
    quota.insert("probe".to_string(), json!(account.quota_probe_kind()));
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
    let mut actions = providers
        .iter()
        .map(|provider| {
            json!({
                "kind": "settings_profile",
                "provider": provider,
                "operation": "create_or_update_provider_owned_profile",
            })
        })
        .collect::<Vec<_>>();
    if providers.is_empty() {
        actions.push(legacy_inspect_tables_action());
    }
    actions
}

fn legacy_inspect_tables_action() -> Value {
    json!({
        "kind": "settings_profile",
        "operation": "inspect_legacy_opencode_tables",
    })
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
    let providers = legacy_provider_names(legacy);
    let mut diagnostics = Vec::new();
    if providers.is_empty() {
        diagnostics.push(legacy_providers_missing_diagnostic());
    }
    diagnostics.extend(
        legacy_unrecognized_provider_names(legacy)
            .into_iter()
            .map(legacy_unrecognized_provider_diagnostic),
    );
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

fn legacy_provider_names(legacy: &Value) -> Vec<String> {
    let Some(parsed) = legacy_providers_toml(legacy) else {
        return Vec::new();
    };
    let Some(table) = parsed.as_table() else {
        return Vec::new();
    };
    table
        .iter()
        .filter_map(|(_, value)| legacy_provider_account(value))
        .map(|account| account.opencode_wrapper.to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn legacy_provider_account(value: &toml::Value) -> Option<&'static AccountProfile> {
    value
        .get("command")
        .and_then(toml::Value::as_str)
        .and_then(profile_for_wrapper_reference)
}

fn legacy_unrecognized_provider_names(legacy: &Value) -> Vec<String> {
    let Some(parsed) = legacy_providers_toml(legacy) else {
        return Vec::new();
    };
    let Some(table) = parsed.as_table() else {
        return Vec::new();
    };
    table
        .iter()
        .filter(|(_, value)| {
            legacy_opencode_shaped_command(value) && legacy_provider_account(value).is_none()
        })
        .map(|(name, _)| name.clone())
        .collect()
}

fn legacy_opencode_shaped_command(value: &toml::Value) -> bool {
    value
        .get("command")
        .and_then(toml::Value::as_str)
        .and_then(|command| Path::new(command).file_name())
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("opencode"))
}

fn legacy_unrecognized_provider_diagnostic(provider: String) -> Value {
    diagnostic(
        "error",
        format!("legacy.providers_toml.{provider}"),
        "legacy OpenCode command does not resolve to a declared account wrapper",
        "legacy_provider_unknown",
    )
}

fn legacy_models(legacy: &Value) -> Vec<String> {
    legacy
        .get("models")
        .and_then(Value::as_object)
        .map(|models| models.keys().cloned().collect())
        .unwrap_or_default()
}

fn upsert_migrated_record(
    store: &mut SettingsStore,
    provider: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let account = profile_for_wrapper_reference(provider)
        .ok_or_else(|| legacy_account_resolution_failure(request_id, provider))?;
    let values = migrated_values_for_account(account);
    if let Some(index) = store.records.iter().position(|record| {
        record.values.get("profile").and_then(Value::as_str) == Some(account.opencode_wrapper)
    }) {
        let id = store.records[index].id.clone();
        update_record(store, index, &id, values, request_id);
        return Ok(());
    }
    let record = new_record(
        store,
        Some(account.opencode_wrapper.to_string()),
        values,
        request_id,
    );
    store.records.push(record);
    Ok(())
}

fn legacy_account_resolution_failure(request_id: &str, provider: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "legacy_provider_unknown",
        format!("legacy provider {provider} does not resolve to a declared OpenCode account"),
    )
}

fn migrated_values_for_account(account: &crate::account::AccountProfile) -> Value {
    let model = default_model();
    json!({
        "provider": "opencode",
        "profile": account.opencode_wrapper,
        "wrapper": account.opencode_wrapper,
        "model": {
            "name": DEFAULT_MODEL_ALIAS,
            "provider_model": model.provider_model,
            "variant": model.effort
        },
        "quota": {
            "source": account.quota_source_kind(),
            "auth_path": account.quota_auth_path(),
            "probe": account.quota_probe_kind()
        },
        "launch": {
            "format": "json",
            "dangerously_skip_permissions": true
        }
    })
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

fn settings_store_upgrade_failure(request_id: &str, message: impl Into<String>) -> ProviderFailure {
    ProviderFailure::invalid_request(request_id, "settings_store_upgrade_required", message)
}

fn settings_store_serialize_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "settings_store_serialize_failed",
        format!("failed to serialize provider settings store: {err}"),
    )
}

fn missing_config_root_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "missing_config_root",
        "settings store requires host.config_root",
    )
}

fn settings_store_confinement_failure(request_id: &str, err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "settings_store_confinement_failed",
        format!("failed to confine provider settings path: {err}"),
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

fn settings_mutation_request_conflict(
    request_id: &str,
    attempted_operation: &str,
    attempted_binding_sha256: &str,
    receipt: &SettingsMutationReceipt,
) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "settings_mutation_request_conflict",
        "settings mutation request_id was already committed with a different binding",
        json!({
            "attempted_operation": attempted_operation,
            "attempted_binding_sha256": attempted_binding_sha256,
            "committed_operation": receipt.operation,
            "committed_binding_sha256": receipt.binding_sha256,
        }),
    )
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
