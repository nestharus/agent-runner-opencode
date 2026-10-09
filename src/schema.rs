//! Declared roles: formatter, parser, validator, accessor, mapper, predicate

use crate::envelope::{HostContext, ProviderFailure, CONTRACT};
use crate::settings_definition;
use agent_provider_contract::generated::{DescribeCapabilities, DescribeResult, JsonObject};
use agent_provider_contract::host_extensions::{launch_output, session_turn_pages};
use serde::Deserialize;
use serde_json::{json, Value};

pub const SETTINGS_SCHEMA_ID: &str = "opencode.settings/v1";
pub const NATIVE_IDENTITY_REBIND_SCHEMA_ID: &str = "opencode.native-identity-rebind/v1";
pub const ROTATION_DECISION_SCHEMA_ID: &str = "opencode.rotation-decision/v1";

/// Base contract versions this provider serves. Only versions a host can
/// currently select are listed; the SDK defines the version vocabulary.
pub const CONTRACT_VERSIONS: &[&str] = &[CONTRACT];
/// `oulipoly.launch_output/vN` versions this provider implements.
pub const LAUNCH_OUTPUT_VERSIONS: &[u32] = &[1];
/// `oulipoly.session_turn_pages/vN` versions this provider implements.
pub const SESSION_TURN_PAGES_VERSIONS: &[u32] = &[1];

/// Whether this request's own `host.env` selected a launch-output version this
/// provider implements. The ambient process environment never selects.
pub fn launch_output_selected(host: &HostContext) -> bool {
    !launch_output::FAMILY
        .advertised(LAUNCH_OUTPUT_VERSIONS, Some(&host.env))
        .is_empty()
}

/// Whether this request's own `host.env` selected a session-turn-pages version
/// this provider implements.
pub fn session_turn_pages_selected(host: &HostContext) -> bool {
    !session_turn_pages::FAMILY
        .advertised(SESSION_TURN_PAGES_VERSIONS, Some(&host.env))
        .is_empty()
}

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

/// Describes this provider. Optional host-selected extensions are advertised
/// only for versions the request offered and this provider implements;
/// prompt acceptance, resident sessions, tool mediation and exploration are
/// not implemented here and are therefore never advertised.
pub fn describe_result(host: &HostContext) -> Value {
    let describe = DescribeResult {
        provider_id: "opencode".to_string(),
        display_name: "OpenCode".to_string(),
        contract_versions: CONTRACT_VERSIONS.iter().map(|v| (*v).to_string()).collect(),
        preferred_contract: CONTRACT.to_string(),
        capabilities: DescribeCapabilities {
            launch: true,
            policy: true,
            quota: true,
            session: true,
            session_enumerate: true,
            terminal: true,
            rotation: true,
            discovery: true,
            settings: true,
            setup_brain: false,
            setup: true,
            migration: true,
            prompt_acceptance_v1: None,
            launch_output_v1: None,
            session_turn_pages_v1: None,
            resident_session_v1: None,
            additional: JsonObject::new(),
        },
        settings_schema_id: Some(SETTINGS_SCHEMA_ID.to_string()),
        concurrency: Some(concurrency()),
    };
    let mut result = serde_json::to_value(describe).expect("SDK describe DTO serializes");
    let capabilities = result["capabilities"]
        .as_object_mut()
        .expect("describe capabilities serialize as an object");
    launch_output::FAMILY.advertise_into(capabilities, LAUNCH_OUTPUT_VERSIONS, Some(&host.env));
    session_turn_pages::FAMILY.advertise_into(
        capabilities,
        SESSION_TURN_PAGES_VERSIONS,
        Some(&host.env),
    );
    result
}

fn concurrency() -> JsonObject {
    serde_json::from_value(json!({
        "safe_for_parallel_invocation": true,
        "state_locking": "interprocess_locked_atomic_file_transactions",
        "settings_version_tokens": true,
        "stdout_protocol_only": true,
        "notes": "This provider is one-shot and daemonless; each account's native OpenCode auth path owns quota probing and refresh attribution.",
    }))
    .expect("concurrency notes are a JSON object")
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
