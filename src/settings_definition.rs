//! Declared roles: validator, formatter, accessor
//! intrinsic_surface_declarations:
//!   - component: src/settings_definition.rs
//!     role: intrinsic-surface
//!     Domain: opencode.settings/v1 definition
//!     Owns:
//!       - the syntactic JSON schema projection
//!       - the executable domain-invariant validation
//!       - settings field, account, model-route, launch, and quota shape identity

use crate::account::{AccountProfile, ACCOUNTS};
use crate::models::{
    model_alias, model_alias_matches, ModelAlias, DEFAULT_MODEL_ALIAS, MODEL_ALIASES,
};
use serde_json::{json, Value};

const SETTINGS_SCHEMA_URI: &str = "https://schemas.oulipoly.dev/opencode.settings/v1.json";
const SETTINGS_FIELDS: &[&str] = &[
    "provider",
    "profile",
    "wrapper",
    "model",
    "quota",
    "launch",
    "extra_env",
    "working_directory",
    "mode",
];
const MODEL_FIELDS: &[&str] = &["name", "provider_model", "variant"];
const QUOTA_FIELDS: &[&str] = &["source", "auth_path", "probe"];
const LAUNCH_FIELDS: &[&str] = &[
    "dangerously_skip_permissions",
    "format",
    "preserve_pure_wrapper",
];

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

pub fn validate_values(values: &Value) -> Vec<Value> {
    let mut diagnostics = Vec::new();
    require_object_shape(values, "values", SETTINGS_FIELDS, &mut diagnostics);
    require_string(values, "provider", "opencode", &mut diagnostics);
    require_known_wrapper(values, &mut diagnostics);
    require_matching_profile(values, &mut diagnostics);
    require_model(values, &mut diagnostics);
    require_quota(values, &mut diagnostics);
    require_launch(values, &mut diagnostics);
    require_optional_fields(values, &mut diagnostics);
    diagnostics
}

pub fn wrapper_value(values: &Value) -> &str {
    values
        .get("wrapper")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

pub fn model_name_value(values: &Value) -> Option<&str> {
    values.pointer("/model/name").and_then(Value::as_str)
}

fn require_object_shape(value: &Value, path: &str, allowed: &[&str], diagnostics: &mut Vec<Value>) {
    let Some(object) = value.as_object() else {
        diagnostics.push(shape_diagnostic(path, "must be an object"));
        return;
    };
    for key in object.keys().filter(|key| !allowed.contains(&key.as_str())) {
        diagnostics.push(shape_diagnostic(
            format!("{path}.{key}"),
            "field is not part of opencode.settings/v1",
        ));
    }
}

fn shape_diagnostic(path: impl Into<String>, message: impl Into<String>) -> Value {
    diagnostic("error", path, message, "invalid_settings_schema_shape")
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
    if account_for_wrapper(wrapper_value(values)).is_some() {
        return;
    }
    diagnostics.push(diagnostic(
        "error",
        "values.wrapper",
        "wrapper must be one of opencode1 through opencode5",
        "invalid_wrapper",
    ));
}

fn require_matching_profile(values: &Value, diagnostics: &mut Vec<Value>) {
    if values.get("profile").is_none()
        || values.get("profile").and_then(Value::as_str) == Some(wrapper_value(values))
    {
        return;
    }
    diagnostics.push(shape_diagnostic(
        "values.profile",
        "profile must identify the same OpenCode account as wrapper",
    ));
}

fn require_model(values: &Value, diagnostics: &mut Vec<Value>) {
    require_object_shape(
        values.get("model").unwrap_or(&Value::Null),
        "values.model",
        MODEL_FIELDS,
        diagnostics,
    );
    let Some(name) = model_name_value(values) else {
        diagnostics.push(invalid_model_alias_diagnostic(""));
        return;
    };
    let Some(model) = model_alias(name) else {
        diagnostics.push(invalid_model_alias_diagnostic(name));
        return;
    };
    if model_alias_matches(
        name,
        provider_model_value(values),
        model_variant_value(values),
    ) {
        return;
    }
    if provider_model_value(values) != Some(model.provider_model) {
        diagnostics.push(diagnostic(
            "error",
            "values.model.provider_model",
            format!("provider_model for {name} must be {}", model.provider_model),
            "invalid_provider_model",
        ));
    }
    if model_variant_value(values) != Some(model.effort) {
        diagnostics.push(diagnostic(
            "error",
            "values.model.variant",
            format!("variant for {name} must be {}", model.effort),
            "invalid_model_variant",
        ));
    }
}

fn provider_model_value(values: &Value) -> Option<&str> {
    values
        .pointer("/model/provider_model")
        .and_then(Value::as_str)
}

fn model_variant_value(values: &Value) -> Option<&str> {
    values.pointer("/model/variant").and_then(Value::as_str)
}

fn invalid_model_alias_diagnostic(name: &str) -> Value {
    diagnostic(
        "error",
        "values.model.name",
        format!("unknown provider model alias: {name}"),
        "invalid_model_alias",
    )
}

fn require_quota(values: &Value, diagnostics: &mut Vec<Value>) {
    require_object_shape(
        values.get("quota").unwrap_or(&Value::Null),
        "values.quota",
        QUOTA_FIELDS,
        diagnostics,
    );
    let Some(account) = account_for_wrapper(wrapper_value(values)) else {
        diagnostics.push(invalid_quota_auth_path_diagnostic());
        return;
    };
    if quota_value(values, "source") == Some(account.quota_source_kind())
        && quota_value(values, "auth_path") == Some(account.quota_auth_path())
        && quota_value(values, "probe") == Some(account.quota_probe_kind())
    {
        return;
    }
    diagnostics.push(invalid_quota_auth_path_diagnostic());
}

fn require_launch(values: &Value, diagnostics: &mut Vec<Value>) {
    let launch = values.get("launch").unwrap_or(&Value::Null);
    require_object_shape(launch, "values.launch", LAUNCH_FIELDS, diagnostics);
    if launch.get("format").and_then(Value::as_str) != Some("json") {
        diagnostics.push(shape_diagnostic(
            "values.launch.format",
            "format must be json",
        ));
    }
    if launch
        .get("dangerously_skip_permissions")
        .and_then(Value::as_bool)
        != Some(true)
    {
        diagnostics.push(shape_diagnostic(
            "values.launch.dangerously_skip_permissions",
            "dangerously_skip_permissions must be true",
        ));
    }
    if launch.get("preserve_pure_wrapper").is_some()
        && launch
            .get("preserve_pure_wrapper")
            .and_then(Value::as_bool)
            .is_none()
    {
        diagnostics.push(shape_diagnostic(
            "values.launch.preserve_pure_wrapper",
            "preserve_pure_wrapper must be a boolean",
        ));
    }
}

fn require_optional_fields(values: &Value, diagnostics: &mut Vec<Value>) {
    if let Some(extra_env) = values.get("extra_env") {
        match extra_env.as_object() {
            Some(entries) if entries.values().all(Value::is_string) => {}
            _ => diagnostics.push(shape_diagnostic(
                "values.extra_env",
                "extra_env must map names to string values",
            )),
        }
    }
    if values.get("working_directory").is_some()
        && values
            .get("working_directory")
            .and_then(Value::as_str)
            .is_none_or(|directory| directory.is_empty())
    {
        diagnostics.push(shape_diagnostic(
            "values.working_directory",
            "working_directory must be a non-empty string",
        ));
    }
    if values.get("mode").is_some()
        && !matches!(
            values.get("mode").and_then(Value::as_str),
            Some("interactive" | "non_interactive")
        )
    {
        diagnostics.push(shape_diagnostic(
            "values.mode",
            "mode must be interactive or non_interactive",
        ));
    }
}

fn quota_value<'a>(values: &'a Value, key: &str) -> Option<&'a str> {
    values
        .get("quota")
        .and_then(|quota| quota.get(key))
        .and_then(Value::as_str)
}

fn invalid_quota_auth_path_diagnostic() -> Value {
    diagnostic(
        "error",
        "values.quota.auth_path",
        "quota source, auth_path, and probe must match the selected OpenCode account",
        "invalid_quota_auth_path",
    )
}

fn account_for_wrapper(wrapper: &str) -> Option<&'static AccountProfile> {
    ACCOUNTS
        .iter()
        .find(|account| account.opencode_wrapper == wrapper)
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
        "required": MODEL_FIELDS
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
