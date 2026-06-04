//! Declared roles: formatter, parser, validator

use crate::envelope::{success_response, ProviderFailure, RequestEnvelope, CONTRACT};
use serde::Deserialize;
use serde_json::{json, Value};

pub const SETTINGS_SCHEMA_ID: &str = "opencode.settings/v1";
const SETTINGS_SCHEMA_URI: &str = "https://schemas.oulipoly.dev/opencode.settings/v1.json";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaParams {
    pub schema_id: String,
}

pub fn schema_response(request: RequestEnvelope) -> Result<Value, ProviderFailure> {
    let params = parse_schema_params(request.params, &request.request_id)?;
    validate_schema_id(&request.request_id, &params.schema_id)?;
    Ok(success_response(&request.request_id, schema_result()))
}

pub fn validate_schema_id(request_id: &str, schema_id: &str) -> Result<(), ProviderFailure> {
    if schema_id == SETTINGS_SCHEMA_ID {
        return Ok(());
    }
    Err(ProviderFailure::unsupported(
        request_id,
        "unknown_schema",
        format!("unknown provider schema id: {schema_id}"),
    ))
}

pub fn describe_result() -> Value {
    json!({
        "provider_id": "opencode",
        "display_name": "OpenCode Codex Hybrid",
        "contract_versions": [CONTRACT],
        "preferred_contract": CONTRACT,
        "capabilities": {
            "launch": true,
            "policy": true,
            "quota": true,
            "session": true,
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
            "state_locking": "atomic_file_writes_and_provider_cli_owned_state",
            "settings_version_tokens": true,
            "stdout_protocol_only": true,
            "notes": "This provider is one-shot and daemonless; auth and quota attribution are owned by paired codex auth paths.",
        },
    })
}

pub fn opencode_settings_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SETTINGS_SCHEMA_URI,
        "title": "OpenCode Hybrid Provider Settings",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "id": {
                "type": "string",
                "minLength": 1,
                "description": "Stable provider settings identifier."
            },
            "display_name": {
                "type": "string",
                "minLength": 1
            },
            "account": {
                "type": "string",
                "enum": ["opencode1", "opencode2", "opencode3", "opencode4", "opencode5"],
                "default": "opencode1",
                "description": "Pinned OpenCode wrapper profile; quota and auth are attributed through the paired codex auth path."
            },
            "opencode_wrapper": {
                "type": "string",
                "enum": ["opencode1", "opencode2", "opencode3", "opencode4", "opencode5"],
                "description": "Resolved wrapper command for the selected account."
            },
            "opencode_index": {
                "type": "integer",
                "minimum": 1,
                "maximum": 5,
                "description": "Resolved one-based wrapper index for the selected account."
            },
            "codex_auth_path": {
                "type": "string",
                "enum": ["~/.codex/auth.json", "~/.codex5/auth.json", "~/.codex2/auth.json", "~/.codex3/auth.json", "~/.codex4/auth.json"],
                "description": "Paired codex auth path used for quota attribution."
            },
            "codex_account_tag": {
                "type": "string",
                "enum": ["codex1", "codex5", "codex2", "codex3", "codex4"],
                "description": "Human-readable tag for the paired codex account."
            },
            "codex_account_hash": {
                "type": "string",
                "enum": ["781db66f", "27f8ea6e", "60238f0b", "9d764739", "835bbc4d"],
                "description": "Stable short fingerprint for the paired codex account."
            },
            "model": {
                "type": "string",
                "enum": ["gpt-none", "gpt-low", "gpt-medium", "gpt-high", "gpt-xhigh"],
                "default": "gpt-high",
                "description": "Provider model alias mapped to openai/gpt-5.5 with the matching effort variant."
            },
            "working_directory": {
                "type": "string",
                "minLength": 1,
                "description": "Launch working directory."
            },
            "mode": {
                "type": "string",
                "enum": ["interactive", "non_interactive"],
                "default": "non_interactive"
            },
            "launch": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "dangerously_skip_permissions": { "type": "boolean", "default": true },
                    "format": { "type": "string", "enum": ["json"], "default": "json" },
                    "preserve_pure_wrapper": { "type": "boolean", "default": true }
                }
            },
            "quota": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "source": { "type": "string", "enum": ["codex_auth"], "default": "codex_auth" },
                    "auth_path": { "type": "string", "enum": ["~/.codex/auth.json", "~/.codex5/auth.json", "~/.codex2/auth.json", "~/.codex3/auth.json", "~/.codex4/auth.json"] }
                }
            },
            "extra_env": {
                "type": "object",
                "additionalProperties": { "type": "string" },
                "default": {}
            }
        }
    })
}

fn parse_schema_params(params: Value, request_id: &str) -> Result<SchemaParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_schema_params",
            format!("schema params must contain schema_id only: {err}"),
        )
    })
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
                "fields": ["account", "model", "working_directory"]
            },
            {
                "id": "metadata",
                "title": "Metadata",
                "fields": ["id", "display_name", "extra_env"]
            }
        ]
    })
}
