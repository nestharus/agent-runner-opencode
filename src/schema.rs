//! Declared roles: formatter, parser, validator, accessor, mapper, predicate

use crate::account::{AccountProfile, ACCOUNTS};
use crate::envelope::{ProviderFailure, CONTRACT};
use crate::models::{ModelAlias, DEFAULT_MODEL_ALIAS, MODEL_ALIASES};
use serde::Deserialize;
use serde_json::{json, Value};

pub const SETTINGS_SCHEMA_ID: &str = "opencode.settings/v1";
const SETTINGS_SCHEMA_URI: &str = "https://schemas.oulipoly.dev/opencode.settings/v1.json";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaParams {
    pub schema_id: String,
}

pub fn schema_result_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_schema_params(params, request_id)?;
    validate_schema_id(request_id, &params.schema_id)?;
    Ok(schema_result())
}

pub fn validate_schema_id(request_id: &str, schema_id: &str) -> Result<(), ProviderFailure> {
    if is_supported_schema_id(schema_id) {
        return Ok(());
    }
    Err(unknown_schema_failure(request_id, schema_id))
}

pub fn describe_result() -> Value {
    json!({
        "provider_id": "opencode",
        "display_name": "OpenCode",
        "contract_versions": [CONTRACT],
        "preferred_contract": CONTRACT,
        "capabilities": {
            "launch": true,
            "policy": true,
            "quota": true,
            "session": true,
            "session_enumerate": true,
            "terminal": true,
            "rotation": true,
            "discovery": true,
            "settings": true,
            "setup_brain": false,
            "setup": true,
            "migration": true,
        },
        "settings_schema_id": SETTINGS_SCHEMA_ID,
        "concurrency": {
            "safe_for_parallel_invocation": true,
            "state_locking": "interprocess_locked_atomic_file_transactions",
            "settings_version_tokens": true,
            "stdout_protocol_only": true,
            "notes": "This provider is one-shot and daemonless; each account's native OpenCode auth path owns quota probing and refresh attribution.",
        },
    })
}

pub fn opencode_settings_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SETTINGS_SCHEMA_URI,
        "title": "OpenCode Provider Settings",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "provider": {
                "const": "opencode",
                "default": "opencode"
            },
            "profile": {
                "type": "string",
                "enum": account_values(account_wrapper),
                "default": default_account_value(account_wrapper)
            },
            "wrapper": {
                "type": "string",
                "enum": account_values(account_wrapper),
                "default": default_account_value(account_wrapper),
                "description": "Canonical account wrapper consumed by policy, launch, session, rotation, and quota."
            },
            "model": {
                "type": "object",
                "oneOf": model_schema_variants(),
                "default": default_model_schema_value(),
                "description": "One exact provider-owned alias, OpenCode model, and effort tuple. Every advertised tuple is eligible for every declared OpenCode account profile."
            },
            "launch": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "dangerously_skip_permissions": { "type": "boolean", "default": true },
                    "format": { "type": "string", "enum": ["json"], "default": "json" },
                    "preserve_pure_wrapper": { "type": "boolean", "default": true }
                },
                "required": ["dangerously_skip_permissions", "format"]
            },
            "quota": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "source": { "const": "opencode_auth", "default": "opencode_auth" },
                    "auth_path": { "type": "string", "enum": account_values(quota_auth_path) },
                    "probe": { "const": "native_chatgpt_usage", "default": "native_chatgpt_usage" }
                },
                "required": ["source", "auth_path", "probe"]
            },
            "extra_env": {
                "type": "object",
                "additionalProperties": { "type": "string" },
                "default": {}
            },
            "working_directory": { "type": "string", "minLength": 1 },
            "mode": { "type": "string", "enum": ["interactive", "non_interactive"], "default": "non_interactive" }
        },
        "required": ["provider", "wrapper", "model", "quota", "launch"]
    })
}

fn account_values(field: fn(&AccountProfile) -> &'static str) -> Vec<&'static str> {
    ACCOUNTS.iter().map(field).collect()
}

fn account_wrapper(account: &AccountProfile) -> &'static str {
    account.opencode_wrapper
}

fn quota_auth_path(account: &AccountProfile) -> &'static str {
    account.quota_auth_path()
}

fn model_schema_variants() -> Vec<Value> {
    MODEL_ALIASES.iter().map(model_schema_variant).collect()
}

fn model_schema_variant(model: &ModelAlias) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "name": { "const": model.name },
            "provider_model": { "const": model.provider_model },
            "variant": { "const": model.effort }
        },
        "required": ["name", "provider_model", "variant"]
    })
}

fn default_model_schema_value() -> Value {
    let model = MODEL_ALIASES
        .iter()
        .find(|model| model.name == DEFAULT_MODEL_ALIAS)
        .expect("default model alias must exist");
    json!({
        "name": model.name,
        "provider_model": model.provider_model,
        "variant": model.effort,
    })
}

fn default_account_value(field: fn(&AccountProfile) -> &'static str) -> &'static str {
    field(&ACCOUNTS[0])
}

fn parse_schema_params(params: Value, request_id: &str) -> Result<SchemaParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_schema_params_failure(request_id, err))
}

fn schema_result() -> Value {
    json!({
        "schema_id": SETTINGS_SCHEMA_ID,
        "schema": opencode_settings_schema(),
        "ui": settings_schema_ui(),
    })
}

fn settings_schema_ui() -> Value {
    json!({
        "sections": [
            {
                "id": "launch",
                "title": "Launch",
                "fields": ["wrapper", "model", "working_directory"]
            },
            {
                "id": "metadata",
                "title": "Metadata",
                "fields": ["profile", "quota", "extra_env"]
            }
        ]
    })
}

fn is_supported_schema_id(schema_id: &str) -> bool {
    schema_id == SETTINGS_SCHEMA_ID
}

fn unknown_schema_failure(request_id: &str, schema_id: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "unknown_schema",
        format!("unknown provider schema id: {schema_id}"),
    )
}

fn invalid_schema_params_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_schema_params",
        format!("schema params must contain schema_id only: {err}"),
    )
}
