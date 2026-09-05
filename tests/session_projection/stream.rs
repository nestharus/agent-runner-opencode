// declared_role: parser, filter, mapper, accessor, validator, orchestration
#![allow(unused_imports)]

use super::*;

pub const CANONICAL_FORMAT: &str = "oulipoly.canonical_transcript/v1";

pub fn read_turns_result(params: Value, path: &str) -> Value {
    success_result(
        invoke_with_host_and_env(
            "session.read_turns",
            params,
            json!({
                "env": {
                    "TERM": "xterm-256color",
                    "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
                }
            }),
            &[("PATH", path)],
        ),
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    )
}

pub fn enumerate_result(params: Value, path: &str) -> Value {
    success_result(
        invoke_with_env("session.enumerate", params, &[("PATH", path)]),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    )
}

pub fn continue_turn_page(prior_params: &Value, prior_result: &Value, path: &str) -> Value {
    read_turns_result(
        session_turn_page_continuation_params(
            prior_params,
            prior_result["snapshot_id"]
                .as_str()
                .expect("snapshot id string"),
            prior_result["next_page_token"]
                .as_str()
                .expect("next page token string"),
        ),
        path,
    )
}
