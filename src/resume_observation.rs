//! Declared roles: observer, validator, mapper
//! intrinsic_surface_declarations:
//!   - component: src/resume_observation.rs
//!     role: intrinsic-surface
//!     Domain: evidence-backed OpenCode resume delivery and completion observation
//!     Owns:
//!       - the route, session, payload, and observation-window request identity
//!       - native transcript traversal and submitted-turn matching
//!       - explicit observed-versus-unconfirmed completion results

use crate::encoding::{now_unix_ms, sha256_hex};
use crate::opencode::{self, OpencodeExport, OpencodeMessage};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
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
    program: String,
    working_directory: String,
    env: BTreeMap<String, String>,
}

#[derive(Clone)]
struct RouteIdentity {
    provider_id: String,
    model_id: String,
    variant: String,
}

enum ResumePayloadEvidence<'a> {
    Plaintext(&'a str),
    Sha256(&'a str),
}

struct ResumeMatchIdentity<'a> {
    session_id: &'a str,
    prompt_sha256: String,
    payload: ResumePayloadEvidence<'a>,
    delivery_nonce: Option<&'a str>,
    started_at_unix_ms: u64,
    provider_id: &'a str,
    model_id: &'a str,
    variant: &'a str,
}

pub struct ResumeObservation {
    pub available: bool,
    pub submitted_user_turn: Option<Value>,
    pub completion: ResumeCompletion,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DurableResumeObservationRequest {
    pub account_wrapper: String,
    pub session_id: String,
    pub payload_sha256: String,
    pub delivery_nonce: Option<String>,
    pub started_at_unix_ms: u64,
    pub provider_id: String,
    pub model_id: String,
    pub variant: String,
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
        program: String,
        working_directory: String,
        env: BTreeMap<String, String>,
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
            program,
            working_directory,
            env,
        }
    }

    pub fn durable_identity(&self) -> DurableResumeObservationRequest {
        DurableResumeObservationRequest {
            account_wrapper: self.account_wrapper.clone(),
            session_id: self.session_id.clone(),
            payload_sha256: sha256_hex(self.payload.as_bytes()),
            delivery_nonce: self.delivery_nonce.clone(),
            started_at_unix_ms: self.started_at_unix_ms,
            provider_id: self.route.provider_id.clone(),
            model_id: self.route.model_id.clone(),
            variant: self.route.variant.clone(),
        }
    }

    fn match_identity(&self) -> ResumeMatchIdentity<'_> {
        ResumeMatchIdentity {
            session_id: &self.session_id,
            prompt_sha256: sha256_hex(self.payload.as_bytes()),
            payload: ResumePayloadEvidence::Plaintext(&self.payload),
            delivery_nonce: self.delivery_nonce.as_deref(),
            started_at_unix_ms: self.started_at_unix_ms,
            provider_id: &self.route.provider_id,
            model_id: &self.route.model_id,
            variant: &self.route.variant,
        }
    }
}

impl DurableResumeObservationRequest {
    fn match_identity(&self) -> ResumeMatchIdentity<'_> {
        ResumeMatchIdentity {
            session_id: &self.session_id,
            prompt_sha256: self.payload_sha256.clone(),
            payload: ResumePayloadEvidence::Sha256(&self.payload_sha256),
            delivery_nonce: self.delivery_nonce.as_deref(),
            started_at_unix_ms: self.started_at_unix_ms,
            provider_id: &self.provider_id,
            model_id: &self.model_id,
            variant: &self.variant,
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
    observation_from_export(&native, &request.match_identity())
}

fn unconfirmed_observation() -> ResumeObservation {
    ResumeObservation {
        available: false,
        submitted_user_turn: None,
        completion: ResumeCompletion::Unconfirmed,
    }
}

pub fn observe_durable(
    request: &DurableResumeObservationRequest,
    program: &str,
    working_directory: &str,
    env: &BTreeMap<String, String>,
) -> ResumeObservation {
    let Ok(native) = opencode::export_with_launch_context(
        &request.session_id,
        program,
        working_directory,
        env,
        RESUME_EXPORT_TIMEOUT,
    ) else {
        return unconfirmed_observation();
    };
    if native.info.id != request.session_id {
        return unconfirmed_observation();
    }
    observation_from_export(&native, &request.match_identity())
}

fn observation_from_export(
    native: &OpencodeExport,
    identity: &ResumeMatchIdentity<'_>,
) -> ResumeObservation {
    let submitted_index = native
        .messages
        .iter()
        .rposition(|message| submitted_user_message(message, identity));
    let submitted_user_turn = submitted_index.map(|index| {
        let message = &native.messages[index];
        let mut marker = json!({
            "provider_session_id": identity.session_id,
            "prompt_sha256": identity.prompt_sha256,
            "source": SUBMITTED_USER_TURN_SOURCE,
            "message_id": message.info.id,
        });
        if let Some(delivery_nonce) = identity.delivery_nonce {
            marker["delivery_nonce"] = json!(delivery_nonce);
        }
        marker
    });
    let completion = if submitted_index.is_some_and(|index| {
        native.messages[index + 1..]
            .iter()
            .any(|message| completed_assistant_message(message, identity))
    }) {
        ResumeCompletion::Observed
    } else {
        ResumeCompletion::Unconfirmed
    };
    ResumeObservation {
        available: true,
        submitted_user_turn,
        completion,
    }
}

fn submitted_user_message(message: &OpencodeMessage, identity: &ResumeMatchIdentity<'_>) -> bool {
    message.info.role == "user"
        && message.info.session_id.as_deref() == Some(identity.session_id)
        && message_model_matches(message, identity)
        && message
            .info
            .time
            .as_ref()
            .and_then(|time| time.created)
            .is_some_and(|created| {
                created
                    >= identity
                        .started_at_unix_ms
                        .saturating_sub(RESUME_MESSAGE_CLOCK_TOLERANCE_MS)
            })
        && if let Some(delivery_nonce) = identity.delivery_nonce {
            message_contains_delivery_nonce(message, delivery_nonce)
        } else {
            message.parts.iter().any(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| payload_matches(text, &identity.payload))
            })
        }
}

fn completed_assistant_message(
    message: &OpencodeMessage,
    identity: &ResumeMatchIdentity<'_>,
) -> bool {
    message.info.role == "assistant"
        && message.info.session_id.as_deref() == Some(identity.session_id)
        && message_model_matches(message, identity)
        && message
            .info
            .time
            .as_ref()
            .is_some_and(|time| time.completed.is_some())
}

fn message_model_matches(message: &OpencodeMessage, identity: &ResumeMatchIdentity<'_>) -> bool {
    let (provider_id, model_id, variant) = message.info.model_identity();
    provider_id == Some(identity.provider_id)
        && model_id == Some(identity.model_id)
        && variant == Some(identity.variant)
}

fn payload_matches(text: &str, payload: &ResumePayloadEvidence<'_>) -> bool {
    match payload {
        ResumePayloadEvidence::Plaintext(payload) => text_matches_payload(text, payload),
        ResumePayloadEvidence::Sha256(expected) => text_sha256_matches(text, expected),
    }
}

fn text_sha256_matches(text: &str, expected: &str) -> bool {
    sha256_hex(text.as_bytes()) == expected
        || text
            .strip_prefix('"')
            .and_then(|quoted| quoted.strip_suffix('"'))
            .is_some_and(|unquoted| sha256_hex(unquoted.as_bytes()) == expected)
        || serde_json::from_str::<String>(text)
            .is_ok_and(|decoded| sha256_hex(decoded.as_bytes()) == expected)
}

fn export_for_observation(request: &ResumeObservationRequest) -> Option<OpencodeExport> {
    let timeout = remaining_export_timeout(request)?;
    opencode::export_with_launch_context(
        &request.session_id,
        &request.program,
        &request.working_directory,
        &request.env,
        timeout,
    )
    .ok()
}

fn remaining_export_timeout(request: &ResumeObservationRequest) -> Option<Duration> {
    let Some(deadline) = request.deadline_unix_ms else {
        return Some(RESUME_EXPORT_TIMEOUT);
    };
    let remaining_ms = deadline.saturating_sub(now_unix_ms());
    (remaining_ms > 0)
        .then(|| Duration::from_millis(remaining_ms.min(RESUME_EXPORT_TIMEOUT.as_millis() as u64)))
}

fn export_session_matches_request(
    native: &OpencodeExport,
    request: &ResumeObservationRequest,
) -> bool {
    native.info.id.as_str() == request.session_id.as_str()
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
    use super::{text_matches_payload, text_sha256_matches};
    use crate::encoding::sha256_hex;

    #[test]
    fn quoted_native_text_matches_payload_with_literal_newline() {
        let payload = "Print exactly LIVE_ROTATION_OK and nothing else.\n";
        let native = format!("\"{payload}\"");

        assert!(text_matches_payload(&native, payload));
    }

    #[test]
    fn durable_digest_matches_json_encoded_native_text() {
        let payload = "Notifications delivered:\n- durable resume\n";
        let native = serde_json::to_string(payload).expect("encode native text");

        assert!(text_sha256_matches(
            &native,
            &sha256_hex(payload.as_bytes())
        ));
    }
}
