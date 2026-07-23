// declared_role: formatter, mapper
#![allow(unused_imports)]

use super::*;

pub fn session_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "session_id": session_id
    })
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

pub fn launch_capture_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "session_id": "fallback-session-id",
        "launch": {
            "session": {
                "provider_session_id": session_id,
                "source": "opencode.run.format_json"
            }
        }
    })
}

pub fn bare_capture_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "session_id": session_id,
    })
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
