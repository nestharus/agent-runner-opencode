//! Declared roles: formatter, parser, validator, accessor, mapper, predicate

use crate::envelope::{ProviderFailure, CONTRACT};
use crate::settings_definition;
use serde::Deserialize;
use serde_json::{json, Value};

pub const SETTINGS_SCHEMA_ID: &str = "opencode.settings/v1";
pub const NATIVE_IDENTITY_REBIND_SCHEMA_ID: &str = "opencode.native-identity-rebind/v1";
pub const ROTATION_DECISION_SCHEMA_ID: &str = "opencode.rotation-decision/v1";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaParams {
    pub schema_id: String,
}

pub fn schema_result_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_schema_params(params, request_id)?;
    validate_schema_id(request_id, &params.schema_id)?;
    Ok(schema_result(&params.schema_id))
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
    settings_definition::opencode_settings_schema()
}

fn parse_schema_params(params: Value, request_id: &str) -> Result<SchemaParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_schema_params_failure(request_id, err))
}

fn schema_result(schema_id: &str) -> Value {
    match schema_id {
        SETTINGS_SCHEMA_ID => json!({
            "schema_id": SETTINGS_SCHEMA_ID,
            "schema": opencode_settings_schema(),
            "ui": settings_schema_ui(),
        }),
        NATIVE_IDENTITY_REBIND_SCHEMA_ID => json!({
            "schema_id": NATIVE_IDENTITY_REBIND_SCHEMA_ID,
            "schema": serde_json::from_str::<Value>(include_str!(
                "../protocol/v1/native-identity-rebind.schema.json"
            ))
            .expect("native identity rebind schema must be valid JSON"),
        }),
        ROTATION_DECISION_SCHEMA_ID => json!({
            "schema_id": ROTATION_DECISION_SCHEMA_ID,
            "schema": serde_json::from_str::<Value>(include_str!(
                "../protocol/v1/rotation-decision.schema.json"
            ))
            .expect("rotation decision schema must be valid JSON"),
        }),
        _ => unreachable!("schema id was validated before projection"),
    }
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
    matches!(
        schema_id,
        SETTINGS_SCHEMA_ID | NATIVE_IDENTITY_REBIND_SCHEMA_ID | ROTATION_DECISION_SCHEMA_ID
    )
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
