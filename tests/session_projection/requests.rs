// declared_role: formatter, mapper
#![allow(unused_imports)]

use super::*;

pub const OBSERVATION_DELIVERY_NONCE: &str =
    "5169694dde0f40d1890c6e28e55bab275169694dde0f40d1890c6e28e55bab27";

pub fn session_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "session_id": session_id
    })
}

pub fn session_params_for_settings(settings_id: &str, session_id: &str) -> Value {
    json!({
        "settings_id": settings_id,
        "session_id": session_id
    })
}

pub fn session_turn_page_beginning_params(
    session_id: &str,
    projection: &str,
    after_token: Option<&str>,
) -> Value {
    let mut params = json!({
        "settings_id": "opencode1",
        "session_id": session_id,
        "read_protocol": "oulipoly.session_turn_pages/v1",
        "turn_projection": projection,
        "start_mode": "beginning",
        "after_token": after_token,
        "snapshot_id": null,
        "page_token": null,
        "max_turns": 1,
        "max_response_bytes": 8192,
        "max_source_bytes": 131072,
        "max_inline_body_bytes": 512
    });
    if projection == "user_observation" {
        params["expected_delivery_nonce"] = json!(OBSERVATION_DELIVERY_NONCE);
    }
    params
}

pub fn session_turn_page_tail_params(session_id: &str) -> Value {
    let mut params = session_turn_page_beginning_params(session_id, "user_observation", None);
    params["start_mode"] = json!("tail");
    params
}

pub fn session_turn_page_continuation_params(
    prior: &Value,
    snapshot_id: &str,
    page_token: &str,
) -> Value {
    let mut params = prior.clone();
    params["start_mode"] = json!("continuation");
    params["after_token"] = Value::Null;
    params["snapshot_id"] = json!(snapshot_id);
    params["page_token"] = json!(page_token);
    params
}

pub fn session_enumerate_params() -> Value {
    json!({
        "settings_id": "opencode1"
    })
}

pub fn session_enumerate_limit_params(limit: u64) -> Value {
    json!({
        "settings_id": "opencode1",
        "limit": limit,
        "cursor": null,
        "include_cwd": true,
        "include_turn_count": true,
        "since_unix_ms": null
    })
}

pub fn session_enumerate_cursor_params(limit: u64, cursor: &str) -> Value {
    let mut params = session_enumerate_limit_params(limit);
    params["cursor"] = json!(cursor);
    params
}

pub fn launch_capture_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "session_id": session_id,
        "launch": {
            "session": {
                "provider_session_id": session_id,
                "source": "opencode.run.format_json"
            }
        }
    })
}

pub fn conflicting_launch_capture_params(session_id: &str) -> Value {
    let mut params = launch_capture_params(session_id);
    params["session_id"] = json!("conflicting-fallback-session-id");
    params
}

pub fn bare_capture_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "session_id": session_id,
    })
}

pub fn lifecycle_capture_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "model_name": "gpt-high",
        "provider_name": "opencode",
        "invocation_uuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "invocation_row_id": 42,
        "effective_cwd": "/tmp/project",
        "start_bound_provider_session_id": session_id,
    })
}

pub fn live_capture_params(session_id: &str, invocation_uuid: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "model_name": "gpt-high",
        "provider_name": "opencode1",
        "invocation_uuid": invocation_uuid,
        "live_report": {
            "provider_session_id": session_id,
            "invocation_uuid": invocation_uuid,
        }
    })
}

pub fn pinned_lifecycle_capture_params(pinned_session_id: &str, start_bound_id: &str) -> Value {
    let mut params = lifecycle_capture_params(start_bound_id);
    params["pinned_target"] = Value::String(pinned_session_id.to_string());
    params
}

pub fn removed_evidence_capture_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "evidence": {
            "provider_session_id": session_id
        }
    })
}

pub fn session_replace_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "session_id": session_id,
        "canonical_format": CANONICAL_FORMAT,
        "data_base64": encode_base64(replacement_record_bytes()),
        "sha256": sha256_hex(replacement_record_bytes()),
        "turn_count": 1
    })
}
