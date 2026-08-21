//! Declared roles: observer, validator, mapper
//! intrinsic_surface_declarations:
//!   - component: src/resume_observation.rs
//!     role: intrinsic-surface
//!     Domain: evidence-backed OpenCode resume delivery and completion observation
//!     Owns:
//!       - the route, session, payload, and observation-window request identity
//!       - native transcript traversal and submitted-turn matching
//!       - explicit observed-versus-unconfirmed completion results

use crate::encoding::sha256_hex;
use crate::opencode::{self, OpencodeEventMetadata, OpencodeExport, OpencodeMessage};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

const RESUME_MESSAGE_CLOCK_TOLERANCE_MS: u64 = 5_000;
const RESUME_EXPORT_TIMEOUT: Duration = Duration::from_millis(750);
const SUBMITTED_USER_TURN_SOURCE: &str = "opencode.export";
const STREAM_SUBMITTED_USER_TURN_SOURCE: &str = "opencode.run.format_json";
const DELIVERY_NONCE_PREFIX: &str = "[OULIPOLY-DELIVERY ";
const DELIVERY_NONCE_SUFFIX: char = ']';

#[derive(Clone)]
pub struct ResumeObservationRequest {
    account_wrapper: String,
    session_id: String,
    payload: String,
    delivery_nonce: Option<String>,
    started_at_unix_ms: u64,
    route: RouteIdentity,
}

#[derive(Clone)]
pub(crate) struct RouteIdentity {
    pub provider_id: String,
    pub model_id: String,
    pub variant: String,
}

struct ResumeMatchIdentity<'a> {
    session_id: &'a str,
    prompt_sha256: String,
    delivery_nonce: Option<&'a str>,
    started_at_unix_ms: u64,
    provider_id: &'a str,
    model_id: &'a str,
    variant: &'a str,
}

#[derive(Clone)]
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
    pub(crate) fn new(
        account_wrapper: String,
        session_id: String,
        payload: String,
        delivery_nonce: String,
        started_at_unix_ms: u64,
        route: RouteIdentity,
    ) -> Self {
        Self {
            account_wrapper,
            session_id,
            payload,
            delivery_nonce: Some(delivery_nonce),
            started_at_unix_ms,
            route,
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
}

impl DurableResumeObservationRequest {
    fn match_identity(&self) -> ResumeMatchIdentity<'_> {
        ResumeMatchIdentity {
            session_id: &self.session_id,
            prompt_sha256: self.payload_sha256.clone(),
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

pub fn observe_stream_event(
    request: &ResumeObservationRequest,
    event: &OpencodeEventMetadata,
) -> Option<ResumeObservation> {
    let completion_observed = opencode::is_successful_terminal_event(event);
    if event.session_id.as_deref() != Some(request.session_id.as_str())
        || (event.event_type != "step_start" && !completion_observed)
    {
        return None;
    }
    let mut marker = json!({
        "provider_session_id": request.session_id,
        "prompt_sha256": sha256_hex(request.payload.as_bytes()),
        "source": STREAM_SUBMITTED_USER_TURN_SOURCE,
        "event_timestamp_unix_ms": event.timestamp,
    });
    if let Some(delivery_nonce) = request.delivery_nonce.as_deref() {
        marker["delivery_nonce"] = json!(delivery_nonce);
    }
    Some(ResumeObservation {
        available: true,
        submitted_user_turn: Some(marker),
        completion: if completion_observed {
            ResumeCompletion::Observed
        } else {
            ResumeCompletion::Unconfirmed
        },
    })
}

pub fn unconfirmed_observation() -> ResumeObservation {
    ResumeObservation {
        available: false,
        submitted_user_turn: None,
        completion: ResumeCompletion::Unconfirmed,
    }
}

pub fn observe_durable(
    request: &DurableResumeObservationRequest,
    program: &str,
    fixed_args: &[String],
    working_directory: &str,
    env: &BTreeMap<String, String>,
) -> ResumeObservation {
    // Durable records written before provider-authored delivery identities
    // cannot distinguish identical sibling turns. They remain unresolved
    // rather than using payload/time proximity as request-local evidence.
    if request.delivery_nonce.is_none() {
        return unconfirmed_observation();
    }
    let Ok(native) = opencode::export_with_launch_context(
        &request.session_id,
        program,
        fixed_args,
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
            .take_while(|message| message.info.role != "user")
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
        && identity
            .delivery_nonce
            .is_some_and(|delivery_nonce| message_contains_delivery_nonce(message, delivery_nonce))
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

#[cfg(test)]
fn text_sha256_matches(text: &str, expected: &str) -> bool {
    sha256_hex(text.as_bytes()) == expected
        || text
            .strip_prefix('"')
            .and_then(|quoted| quoted.strip_suffix('"'))
            .is_some_and(|unquoted| sha256_hex(unquoted.as_bytes()) == expected)
        || serde_json::from_str::<String>(text)
            .is_ok_and(|decoded| sha256_hex(decoded.as_bytes()) == expected)
}

fn message_contains_delivery_nonce(message: &OpencodeMessage, delivery_nonce: &str) -> bool {
    let fields = message_string_fields(message);
    let joined = fields.join(" ");
    let Some(start) = joined.rfind(DELIVERY_NONCE_PREFIX) else {
        return false;
    };
    let tail = &joined[start + DELIVERY_NONCE_PREFIX.len()..];
    let Some(end) = tail.find(DELIVERY_NONCE_SUFFIX) else {
        return false;
    };
    tail[..end].trim() == delivery_nonce
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

pub(crate) fn delivery_marker(delivery_nonce: &str) -> String {
    format!("{DELIVERY_NONCE_PREFIX}{delivery_nonce}{DELIVERY_NONCE_SUFFIX}")
}

pub(crate) fn message_has_delivery_nonce(message: &OpencodeMessage, delivery_nonce: &str) -> bool {
    message_contains_delivery_nonce(message, delivery_nonce)
}

#[cfg(test)]
mod tests {
    use super::{
        observation_from_export, text_sha256_matches, ResumeCompletion, ResumeMatchIdentity,
    };
    use crate::encoding::sha256_hex;
    use crate::opencode;
    use serde_json::json;

    #[test]
    fn quoted_native_text_matches_payload_with_literal_newline() {
        let payload = "Print exactly LIVE_ROTATION_OK and nothing else.\n";
        let native = format!("\"{payload}\"");

        assert!(text_sha256_matches(
            &native,
            &sha256_hex(payload.as_bytes())
        ));
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

    #[test]
    fn request_delivery_nonce_does_not_consume_identical_sibling_completion() {
        let native = opencode::parse_export_stdout(
            serde_json::to_vec(&json!({
                "info": {"id": "session-1", "title": "sibling delivery"},
                "messages": [
                    {
                        "info": {
                            "id": "message-a",
                            "role": "user",
                            "sessionID": "session-1",
                            "model": {
                                "providerID": "openai",
                                "modelID": "gpt-5.6-luna",
                                "variant": "low"
                            },
                            "time": {"created": 10}
                        },
                        "parts": [{"type": "text", "text": "same payload\n\n[OULIPOLY-DELIVERY request-a]"}]
                    },
                    {
                        "info": {
                            "id": "message-b",
                            "role": "user",
                            "sessionID": "session-1",
                            "model": {
                                "providerID": "openai",
                                "modelID": "gpt-5.6-luna",
                                "variant": "low"
                            },
                            "time": {"created": 11}
                        },
                        "parts": [{"type": "text", "text": "same payload\n\n[OULIPOLY-DELIVERY request-a]\n\n[OULIPOLY-DELIVERY request-b]"}]
                    },
                    {
                        "info": {
                            "id": "assistant-b",
                            "role": "assistant",
                            "sessionID": "session-1",
                            "providerID": "openai",
                            "modelID": "gpt-5.6-luna",
                            "variant": "low",
                            "time": {"created": 12, "completed": 13}
                        },
                        "parts": [{"type": "text", "text": "done"}]
                    }
                ]
            }))
            .expect("serialize sibling export")
            .as_slice(),
        )
        .expect("parse sibling export");
        let payload_sha256 = sha256_hex(b"same payload");
        let identity = ResumeMatchIdentity {
            session_id: "session-1",
            prompt_sha256: payload_sha256.clone(),
            delivery_nonce: Some("request-a"),
            started_at_unix_ms: 10,
            provider_id: "openai",
            model_id: "gpt-5.6-luna",
            variant: "low",
        };

        let observation = observation_from_export(&native, &identity);

        assert_eq!(observation.completion, ResumeCompletion::Unconfirmed);
        assert_eq!(
            observation
                .submitted_user_turn
                .as_ref()
                .and_then(|marker| marker["message_id"].as_str()),
            Some("message-a")
        );
    }
}
