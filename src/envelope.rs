//! Declared roles: formatter, validator, mapper

use agent_provider_contract::generated::{
    self as contract, ErrorCategory, ErrorObject, ErrorResponseEnvelope, FalseBool, JsonObject,
};
use serde_json::{json, Value};

/// The shared SDK host context; request admission is operation-bound in `dispatch`.
pub use agent_provider_contract::generated::HostContext;
/// The shared SDK request envelope with operation params kept as admitted JSON.
pub type RequestEnvelope = contract::RequestEnvelope<Value>;

/// The only base contract version this provider serves, as defined by the SDK.
pub const CONTRACT: &str = agent_provider_contract::CONTRACT_VERSION;
pub const MAX_REQUEST_ENVELOPE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_REQUEST_ID_BYTES: usize = 256;
pub const MAX_PROVIDER_INSTANCE_ID_BYTES: usize = 256;
pub const MAX_HOST_LABEL_BYTES: usize = 256;
pub const MAX_HOST_PATH_BYTES: usize = 64 * 1024;
pub const MAX_HOST_ENV_ENTRIES: usize = 128;
pub const MAX_HOST_ENV_KEY_BYTES: usize = 256;
pub const MAX_HOST_ENV_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_HOST_ENV_BYTES: usize = 256 * 1024;

pub const CATEGORY_UNSUPPORTED: &str = "unsupported";
pub const CATEGORY_INVALID_REQUEST: &str = "invalid_request";
pub const CATEGORY_INVALID_SETTINGS: &str = "invalid_settings";
pub const CATEGORY_CONFLICT: &str = "conflict";
pub const CATEGORY_FAILED: &str = "failed";

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

    pub fn invalid_settings(
        request_id: impl Into<String>,
        code: &'static str,
        message: impl Into<String>,
        details: Value,
    ) -> Self {
        provider_failure(
            request_id,
            CATEGORY_INVALID_SETTINGS,
            code,
            message,
            details,
            false,
            2,
        )
    }

    pub fn conflict(
        request_id: impl Into<String>,
        code: &'static str,
        message: impl Into<String>,
        details: Value,
    ) -> Self {
        provider_failure(
            request_id,
            CATEGORY_CONFLICT,
            code,
            message,
            details,
            false,
            2,
        )
    }

    pub fn retryable_conflict(
        request_id: impl Into<String>,
        code: &'static str,
        message: impl Into<String>,
        details: Value,
    ) -> Self {
        provider_failure(
            request_id,
            CATEGORY_CONFLICT,
            code,
            message,
            details,
            true,
            2,
        )
    }

    pub fn internal(
        request_id: impl Into<String>,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        provider_failure(
            request_id,
            CATEGORY_FAILED,
            code,
            message,
            json!({}),
            false,
            1,
        )
    }
}

pub fn success_response(request_id: &str, result: Value) -> Value {
    serde_json::to_value(contract::SuccessResponseEnvelope {
        contract: CONTRACT.to_string(),
        request_id: request_id.to_string(),
        ok: contract::TrueBool,
        result,
    })
    .expect("SDK success envelope DTO serializes")
}

pub fn error_response(
    request_id: &str,
    category: &str,
    code: &str,
    message: &str,
    details: Value,
    retryable: bool,
) -> Value {
    serde_json::to_value(error_envelope(
        request_id, category, code, message, details, retryable,
    ))
    .expect("SDK error envelope DTO serializes")
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

fn error_envelope(
    request_id: &str,
    category: &str,
    code: &str,
    message: &str,
    details: Value,
    retryable: bool,
) -> ErrorResponseEnvelope {
    ErrorResponseEnvelope {
        contract: CONTRACT.to_string(),
        request_id: request_id.to_string(),
        ok: FalseBool,
        error: ErrorObject {
            code: code.to_string(),
            category: error_category(category),
            message: message.to_string(),
            retryable,
            details: Some(object_details(details)),
            diagnostics: Vec::new(),
        },
        process_status: None,
    }
}

fn error_category(category: &str) -> ErrorCategory {
    match category {
        CATEGORY_UNSUPPORTED => ErrorCategory::Unsupported,
        CATEGORY_INVALID_REQUEST => ErrorCategory::InvalidRequest,
        CATEGORY_INVALID_SETTINGS => ErrorCategory::InvalidSettings,
        CATEGORY_CONFLICT => ErrorCategory::Conflict,
        _ => ErrorCategory::Failed,
    }
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

fn object_details(details: Value) -> JsonObject {
    match details {
        Value::Object(details) => details.into_iter().collect(),
        _ => JsonObject::new(),
    }
}
