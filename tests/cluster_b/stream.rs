// declared_role: parser, filter, mapper, accessor, validator, orchestration
#![allow(unused_imports)]

use super::*;

pub const CANONICAL_FORMAT: &str = "oulipoly.canonical_transcript/v1";

pub fn read_turns_result(params: Value, path: &str) -> Value {
    success_result(
        invoke_with_env("session.read_turns", params, &[("PATH", path)]),
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    )
}

pub fn missing_read_turns_output(path: &str) -> std::process::Output {
    invoke_with_env(
        "session.read_turns",
        session_params("ses_missing_contract_cluster_b"),
        &[("PATH", path)],
    )
}
