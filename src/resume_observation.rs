//! Declared roles: observer, validator, mapper
//! intrinsic_surface_declarations:
//!   - component: src/resume_observation.rs
//!     role: intrinsic-surface
//!     Domain: evidence-backed OpenCode resume delivery and completion observation
//!     Owns:
//!       - the route, session, payload, and observation-window request identity
//!       - native transcript traversal and submitted-turn matching
//!       - explicit observed-versus-unconfirmed completion results

use crate::account::profile_for_wrapper_reference;
use crate::encoding::{now_unix_ms, sha256_hex};
use crate::opencode::{self, OpencodeExport, OpencodeMessage};
use serde_json::{json, Value};
use std::time::Duration;

const RESUME_MESSAGE_CLOCK_TOLERANCE_MS: u64 = 5_000;
const RESUME_EXPORT_TIMEOUT: Duration = Duration::from_millis(750);
const SUBMITTED_USER_TURN_SOURCE: &str = "opencode.export";
const DELIVERY_NONCE_PREFIX: &str = "[OULIPOLY-DELIVERY ";
const DELIVERY_NONCE_SUFFIX: char = ']';

#[derive(Clone)]
pub struct ResumeObservationRequest {
    account_wrapper: String,
    session_id: String,
    payload: String,
    delivery_nonce: Option<String>,
    started_at_unix_ms: u64,
    deadline_unix_ms: Option<u64>,
    route: RouteIdentity,
}

#[derive(Clone)]
struct RouteIdentity {
    provider_id: String,
    model_id: String,
    variant: String,
}

pub struct ResumeObservation {
    pub submitted_user_turn: Option<Value>,
    pub completion: ResumeCompletion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResumeCompletion {
    Observed,
    Unconfirmed,
}

impl ResumeObservationRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_wrapper: String,
        session_id: String,
        payload: String,
        started_at_unix_ms: u64,
        deadline_unix_ms: Option<u64>,
        provider_id: String,
        model_id: String,
        variant: String,
    ) -> Self {
        let delivery_nonce = delivery_nonce_from_payload(&payload);
        Self {
            account_wrapper,
            session_id,
            payload,
            delivery_nonce,
            started_at_unix_ms,
            deadline_unix_ms,
            route: RouteIdentity {
                provider_id,
                model_id,
                variant,
            },
        }
    }
}

impl ResumeObservation {
    pub fn completion_observed(&self) -> bool {
        self.completion == ResumeCompletion::Observed
    }
}

pub fn observe(request: &ResumeObservationRequest) -> ResumeObservation {
    let Some(native) = export_for_observation(request) else {
        return unconfirmed_observation();
    };
    if !export_session_matches_request(&native, request) {
        return unconfirmed_observation();
    }
    ResumeObservation {
        submitted_user_turn: submitted_user_turn_marker_value(&native, request),
        completion: completion_observation(&native, request),
    }
}

fn unconfirmed_observation() -> ResumeObservation {
    ResumeObservation {
        submitted_user_turn: None,
        completion: ResumeCompletion::Unconfirmed,
    }
}

fn submitted_user_turn_marker_value(
    native: &OpencodeExport,
    request: &ResumeObservationRequest,
) -> Option<Value> {
    let message = submitted_user_turn_message(&native.messages, request)?;
    Some(submitted_user_turn_marker(
        request,
        Some(message.info.id.as_str()),
    ))
}

fn completion_observation(
    native: &OpencodeExport,
    request: &ResumeObservationRequest,
) -> ResumeCompletion {
    let Some(submitted_index) = native
        .messages
        .iter()
        .rposition(|message| current_launch_user_message(message, request))
    else {
        return ResumeCompletion::Unconfirmed;
    };
    if native.messages[submitted_index + 1..]
        .iter()
        .any(|message| completed_assistant_message(message, request))
    {
        ResumeCompletion::Observed
    } else {
        ResumeCompletion::Unconfirmed
    }
}

fn current_launch_user_message(
    message: &OpencodeMessage,
    request: &ResumeObservationRequest,
) -> bool {
    submitted_user_message_matches(message, request)
        && message
            .info
            .time
            .as_ref()
            .and_then(|time| time.created)
            .is_some_and(|created| {
                created
                    >= request
                        .started_at_unix_ms
                        .saturating_sub(RESUME_MESSAGE_CLOCK_TOLERANCE_MS)
            })
}

fn completed_assistant_message(
    message: &OpencodeMessage,
    request: &ResumeObservationRequest,
) -> bool {
    message.info.role == "assistant"
        && message.info.session_id.as_deref() == Some(request.session_id.as_str())
        && message_model_matches_request(message, request)
        && message
            .info
            .time
            .as_ref()
            .is_some_and(|time| time.completed.is_some())
}

fn export_for_observation(request: &ResumeObservationRequest) -> Option<OpencodeExport> {
    let account = profile_for_wrapper_reference(&request.account_wrapper)?;
    let timeout = remaining_export_timeout(request)?;
    opencode::export_with_timeout(&request.session_id, account, timeout).ok()
}

fn remaining_export_timeout(request: &ResumeObservationRequest) -> Option<Duration> {
    let Some(deadline) = request.deadline_unix_ms else {
        return Some(RESUME_EXPORT_TIMEOUT);
    };
    let remaining_ms = deadline.saturating_sub(now_unix_ms());
    (remaining_ms > 0)
        .then(|| Duration::from_millis(remaining_ms.min(RESUME_EXPORT_TIMEOUT.as_millis() as u64)))
}

fn message_model_matches_request(
    message: &OpencodeMessage,
    request: &ResumeObservationRequest,
) -> bool {
    let (provider_id, model_id, variant) = message.info.model_identity();
    provider_id == Some(request.route.provider_id.as_str())
        && model_id == Some(request.route.model_id.as_str())
        && variant == Some(request.route.variant.as_str())
}

fn export_session_matches_request(
    native: &OpencodeExport,
    request: &ResumeObservationRequest,
) -> bool {
    native.info.id.as_str() == request.session_id.as_str()
}

fn submitted_user_turn_message<'a>(
    messages: &'a [OpencodeMessage],
    request: &ResumeObservationRequest,
) -> Option<&'a OpencodeMessage> {
    messages
        .iter()
        .find(|message| submitted_user_message_matches(message, request))
}

fn submitted_user_turn_marker(
    request: &ResumeObservationRequest,
    message_id: Option<&str>,
) -> Value {
    let mut marker = json!({
        "provider_session_id": request.session_id.as_str(),
        "prompt_sha256": sha256_hex(request.payload.as_bytes()),
        "source": SUBMITTED_USER_TURN_SOURCE,
    });
    if let Some(message_id) = message_id {
        marker["message_id"] = json!(message_id);
    }
    if let Some(delivery_nonce) = request.delivery_nonce.as_deref() {
        marker["delivery_nonce"] = json!(delivery_nonce);
    }
    marker
}

fn submitted_user_message_matches(
    message: &OpencodeMessage,
    request: &ResumeObservationRequest,
) -> bool {
    message.info.role.as_str() == "user"
        && message.info.session_id.as_deref() == Some(request.session_id.as_str())
        && message_model_matches_request(message, request)
        && message_confirms_resume_payload(message, request)
}

fn message_confirms_resume_payload(
    message: &OpencodeMessage,
    request: &ResumeObservationRequest,
) -> bool {
    if let Some(delivery_nonce) = request.delivery_nonce.as_deref() {
        return message_contains_delivery_nonce(message, delivery_nonce);
    }
    message_has_exact_text_part(message, &request.payload)
}

fn message_has_exact_text_part(message: &OpencodeMessage, payload: &str) -> bool {
    message.parts.iter().any(|part| {
        part.get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| text_matches_payload(text, payload))
    })
}

fn text_matches_payload(text: &str, payload: &str) -> bool {
    text == payload
        || text
            .strip_prefix('"')
            .and_then(|quoted| quoted.strip_suffix('"'))
            .is_some_and(|unquoted| unquoted == payload)
        || serde_json::from_str::<String>(text).is_ok_and(|decoded| decoded.as_str() == payload)
}

fn message_contains_delivery_nonce(message: &OpencodeMessage, delivery_nonce: &str) -> bool {
    let marker = delivery_marker(delivery_nonce);
    let fields = message_string_fields(message);
    fields.iter().any(|field| field.contains(&marker)) || fields.concat().contains(&marker)
}

fn message_string_fields(message: &OpencodeMessage) -> Vec<&str> {
    message.parts.iter().flat_map(value_string_fields).collect()
}

fn value_string_fields(value: &Value) -> Vec<&str> {
    if let Some(text) = value.as_str() {
        return vec![text];
    }
    if let Some(values) = value.as_array() {
        return values.iter().flat_map(value_string_fields).collect();
    }
    if let Some(values) = value.as_object() {
        return values.values().flat_map(value_string_fields).collect();
    }
    Vec::new()
}

fn delivery_nonce_from_payload(payload: &str) -> Option<String> {
    let start = payload.find(DELIVERY_NONCE_PREFIX)? + DELIVERY_NONCE_PREFIX.len();
    let tail = &payload[start..];
    let end = tail.find(DELIVERY_NONCE_SUFFIX)?;
    let nonce = tail[..end].trim();
    (!nonce.is_empty()).then(|| nonce.to_string())
}

fn delivery_marker(delivery_nonce: &str) -> String {
    format!("{DELIVERY_NONCE_PREFIX}{delivery_nonce}{DELIVERY_NONCE_SUFFIX}")
}

#[cfg(test)]
mod tests {
    use super::text_matches_payload;

    #[test]
    fn quoted_native_text_matches_payload_with_literal_newline() {
        let payload = "Print exactly LIVE_ROTATION_OK and nothing else.\n";
        let native = format!("\"{payload}\"");

        assert!(text_matches_payload(&native, payload));
    }
}
