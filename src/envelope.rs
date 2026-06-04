//! Declared roles: formatter, validator

use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const CONTRACT: &str = "oulipoly.provider/v1";

pub const CATEGORY_UNSUPPORTED: &str = "unsupported";
pub const CATEGORY_INVALID_REQUEST: &str = "invalid_request";
pub const CATEGORY_NOT_FOUND: &str = "not_found";
pub const CATEGORY_CONFLICT: &str = "conflict";
pub const CATEGORY_INTERNAL: &str = "internal";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestEnvelope {
    pub contract: String,
    pub request_id: String,
    pub provider_instance_id: Option<String>,
    pub host: HostContext,
    pub params: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostContext {
    pub app: String,
    pub app_version: Option<String>,
    pub platform: Option<String>,
    pub working_directory: Option<String>,
    pub config_root: Option<String>,
    pub data_root: Option<String>,
    pub env: Option<BTreeMap<String, String>>,
    pub deadline_unix_ms: Option<u64>,
}

#[derive(Debug)]
pub struct ProviderFailure {
    pub request_id: String,
    pub category: &'static str,
    pub code: &'static str,
    pub message: String,
    pub details: Value,
    pub retryable: bool,
    pub exit_code: i32,
}

impl ProviderFailure {
    pub fn invalid_request(
        request_id: impl Into<String>,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        provider_failure(
            request_id,
            CATEGORY_INVALID_REQUEST,
            code,
            message,
            json!({}),
            false,
            2,
        )
    }

    pub fn unsupported(
        request_id: impl Into<String>,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        provider_failure(
            request_id,
            CATEGORY_UNSUPPORTED,
            code,
            message,
            json!({}),
            false,
            3,
        )
    }

    pub fn internal(
        request_id: impl Into<String>,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        provider_failure(
            request_id,
            CATEGORY_INTERNAL,
            code,
            message,
            json!({}),
            false,
            1,
        )
    }
}

pub fn success_response(request_id: &str, result: Value) -> Value {
    json!({
        "contract": CONTRACT,
        "request_id": request_id,
        "ok": true,
        "result": result,
    })
}

pub fn error_response(
    request_id: &str,
    category: &str,
    code: &str,
    message: &str,
    details: Value,
    retryable: bool,
) -> Value {
    json!({
        "contract": CONTRACT,
        "request_id": request_id,
        "ok": false,
        "error": {
            "category": category,
            "code": code,
            "message": message,
            "details": object_details(details),
            "retryable": retryable,
        },
    })
}

pub fn failure_response(failure: &ProviderFailure) -> Value {
    error_response(
        &failure.request_id,
        failure.category,
        failure.code,
        &failure.message,
        failure.details.clone(),
        failure.retryable,
    )
}

fn provider_failure(
    request_id: impl Into<String>,
    category: &'static str,
    code: &'static str,
    message: impl Into<String>,
    details: Value,
    retryable: bool,
    exit_code: i32,
) -> ProviderFailure {
    ProviderFailure {
        request_id: request_id.into(),
        category,
        code,
        message: message.into(),
        details,
        retryable,
        exit_code,
    }
}

fn object_details(details: Value) -> Value {
    if details.is_object() {
        return details;
    }
    json!({})
}
