// declared_role: validator, accessor, predicate, orchestration
#![allow(unused_imports)]

use super::*;

pub fn assert_native_export_fixture(export: &Value) {
    let info = native_export_info(export);
    let session_id = native_export_info_id(info);
    assert_native_export_session_id(session_id);
    assert_native_export_title(native_export_info_title(info));
    let messages = native_export_messages(export);
    assert_native_export_messages_present(messages);
    assert_native_messages(messages, session_id);
    assert_native_export_not_contract_envelope(export);
}

pub fn native_export_info(export: &Value) -> &serde_json::Map<String, Value> {
    export["info"].as_object().expect("export.info object")
}

pub fn native_export_info_id(info: &serde_json::Map<String, Value>) -> &str {
    info["id"].as_str().expect("info.id string")
}

pub fn assert_native_export_session_id(session_id: &str) {
    assert!(
        session_id.starts_with("ses_"),
        "unexpected session id {session_id}"
    );
}

pub fn native_export_info_title(info: &serde_json::Map<String, Value>) -> Option<&str> {
    info["title"].as_str()
}

pub fn assert_native_export_title(title: Option<&str>) {
    assert!(
        title.is_some_and(non_empty_string),
        "info.title should be a non-empty native opencode title"
    );
}

pub fn non_empty_string(value: &str) -> bool {
    !value.is_empty()
}

pub fn native_export_messages(export: &Value) -> &[Value] {
    export["messages"].as_array().expect("messages array")
}

pub fn assert_native_export_messages_present(messages: &[Value]) {
    assert!(
        !messages.is_empty(),
        "native export should include messages"
    );
}

pub fn assert_native_export_not_contract_envelope(export: &Value) {
    assert!(
        export.get("contract").is_none(),
        "native opencode export is source material, not a provider contract envelope"
    );
}

pub fn assert_native_messages(messages: &[Value], session_id: &str) {
    let mut part_types = BTreeSet::new();
    for message in messages {
        assert_native_message(message, session_id, &mut part_types);
    }
    assert_expected_part_types(&part_types);
}

pub fn assert_native_message(message: &Value, session_id: &str, part_types: &mut BTreeSet<String>) {
    assert_native_message_role(native_message_role(message));
    assert_native_message_session_id(native_message_session_id(message), session_id);
    let parts = native_message_parts(message);
    assert_native_message_parts_present(parts);
    assert_native_message_parts(parts, session_id, part_types);
}

pub fn native_message_role(message: &Value) -> &str {
    message["info"]["role"]
        .as_str()
        .expect("message.info.role string")
}

pub fn assert_native_message_role(role: &str) {
    assert!(
        matches!(role, "user" | "assistant"),
        "unexpected native message role {role}"
    );
}

pub fn native_message_session_id(message: &Value) -> Option<&str> {
    message["info"]["sessionID"].as_str()
}

pub fn assert_native_message_session_id(actual: Option<&str>, session_id: &str) {
    assert_eq!(
        actual,
        Some(session_id),
        "message sessionID should match export info.id"
    );
}

pub fn native_message_parts(message: &Value) -> &[Value] {
    message["parts"].as_array().expect("message.parts array")
}

pub fn assert_native_message_parts_present(parts: &[Value]) {
    assert!(!parts.is_empty(), "native message should include parts");
}

pub fn assert_native_message_parts(
    parts: &[Value],
    session_id: &str,
    part_types: &mut BTreeSet<String>,
) {
    for part in parts {
        assert_native_part(part, session_id, part_types);
    }
}

pub fn assert_native_part(part: &Value, session_id: &str, part_types: &mut BTreeSet<String>) {
    record_part_type(part_types, native_part_type(part));
    assert_native_part_session(part, session_id);
}

pub fn assert_native_part_session(part: &Value, session_id: &str) {
    assert_eq!(
        part["sessionID"].as_str(),
        Some(session_id),
        "part sessionID should match export info.id"
    );
}

pub fn assert_expected_part_types(part_types: &BTreeSet<String>) {
    for expected in ["step-start", "text", "step-finish"] {
        assert!(
            part_types.contains(expected),
            "native export should include a {expected} part; saw {part_types:?}"
        );
    }
}

pub fn assert_read_turns_result(result: &Value) {
    assert_eq!(
        result["turn_count"].as_u64(),
        Some(fixture_message_count() as u64),
        "turn_count should match the native opencode export message count"
    );
    assert!(result["complete"].is_boolean(), "complete should be a bool");
    assert_eq!(
        result["turns"].as_array().expect("turns array").len(),
        fixture_message_count(),
        "turns length should match turn_count and native message count"
    );
}

pub fn assert_first_read_turns_result(result: &Value) -> Vec<String> {
    assert_read_turns_result(result);
    let first_ids = turn_ids(result);
    assert_turn_id_count_matches_fixture(&first_ids);
    first_ids
}

pub fn assert_turn_id_count_matches_fixture(first_ids: &[String]) {
    assert_eq!(first_ids.len(), fixture_message_count());
}

pub fn assert_missing_read_turns_error(path: &str) {
    assert_error_envelope(missing_read_turns_output(path));
}

pub fn assert_not_located_result(result: &Value) {
    assert_not_located_result_state(result);
    assert_not_located_result_metadata(result);
}

pub fn assert_not_located_result_state(result: &Value) {
    assert_not_located_value(located_value(result));
    assert_locate_path_absent(result_path(result));
}

pub fn located_value(result: &Value) -> &Value {
    &result["located"]
}

pub fn assert_not_located_value(located: &Value) {
    assert_eq!(*located, false);
}

pub fn result_path(result: &Value) -> Option<&Value> {
    result.get("path")
}

pub fn assert_locate_path_absent(path: Option<&Value>) {
    assert!(
        path.is_none(),
        "opencode has no transcript file, so locate_transcript must omit path"
    );
}

pub fn assert_not_located_result_metadata(result: &Value) {
    assert_non_empty_result_string(
        result,
        "format_id",
        "not-located response should still identify the transcript/export format",
    );
    assert_non_empty_result_string(
        result,
        "source_id",
        "not-located response should still identify the opencode source",
    );
}

pub fn assert_non_empty_result_string(result: &Value, key: &str, message: &str) {
    assert!(
        result[key].as_str().is_some_and(|value| !value.is_empty()),
        "{message}"
    );
}

pub fn assert_stable_turn_ids(second: &Value, first_ids: &[String]) {
    assert_eq!(
        turn_ids(second),
        first_ids,
        "turn ids must be stable across repeated reads of the same opencode export"
    );
}

pub fn assert_canonical_export_result(result: &Value, sha_message: &str) {
    assert_canonical_export_format(result);
    let decoded = canonical_result_decoded_bytes(result);
    assert_canonical_export_sha(result, &decoded, sha_message);
    assert_canonical_export_turn_count(result, &decoded);
}

pub fn assert_canonical_export_format(result: &Value) {
    assert_eq!(result["canonical_format"], CANONICAL_FORMAT);
}

pub fn assert_canonical_export_sha(result: &Value, decoded: &[u8], sha_message: &str) {
    assert_eq!(
        canonical_bytes_sha(decoded),
        canonical_result_sha(result),
        "{sha_message}"
    );
}

pub fn assert_canonical_export_turn_count(result: &Value, decoded: &[u8]) {
    assert_eq!(
        canonical_record_count(decoded),
        canonical_result_turn_count(result),
        "canonical record count must match turn_count"
    );
}

pub fn assert_deterministic_export(first: &Value, second: &Value) {
    assert_eq!(
        second["data_base64"], first["data_base64"],
        "canonical export bytes must be deterministic for the same native export"
    );
    assert_eq!(
        second["sha256"], first["sha256"],
        "canonical export sha256 must be deterministic for the same native export"
    );
}

pub fn assert_empty_enumerate_result(result: &Value) {
    assert_eq!(enumerate_sessions(result).len(), 0);
    assert_eq!(result["complete"], true);
    assert!(result["next_cursor"].is_null());
    assert_eq!(enumerate_warnings(result).len(), 0);
}

pub fn assert_multiple_enumerate_result(result: &Value) {
    let sessions = enumerate_sessions(result);
    assert_eq!(sessions.len(), 2);
    assert_enumerate_entry(
        &sessions[0],
        "ses_list_one",
        Some("First session"),
        Some("/tmp/project-one"),
        Some(111),
        Some(222),
        Some(3),
    );
    assert_enumerate_entry(
        &sessions[1],
        "ses_list_two",
        None,
        Some("/var/tmp/project-two"),
        Some(333),
        Some(444),
        Some(0),
    );
    assert_eq!(enumerate_warnings(result).len(), 0);
}

pub fn assert_bad_cwd_enumerate_result(result: &Value) {
    let sessions = enumerate_sessions(result);
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0]["provider_session_id"], "ses_relative_cwd");
    assert!(sessions[0]["cwd"].is_null());
    assert_eq!(sessions[1]["provider_session_id"], "ses_missing_cwd");
    assert!(sessions[1]["cwd"].is_null());
    let warnings = enumerate_warning_text(result);
    assert!(
        warnings.contains("non-absolute"),
        "relative cwd warning missing: {warnings}"
    );
    assert!(
        warnings.contains("no directory/cwd"),
        "missing cwd warning missing: {warnings}"
    );
}

pub fn assert_limited_enumerate_result(result: &Value, limit: usize) {
    let sessions = enumerate_sessions(result);
    assert_eq!(sessions.len(), limit);
    assert_eq!(sessions[0]["provider_session_id"], "ses_limit_one");
    assert_eq!(sessions[1]["provider_session_id"], "ses_limit_two");
}

pub fn assert_session_list_limit_forwarded(log_path: &Path, limit: u64) {
    let log = fs::read_to_string(log_path).expect("read fake session list wrapper log");
    assert!(
        log.contains("arg=session"),
        "session list wrapper should receive session subcommand: {log}"
    );
    assert!(
        log.contains("arg=list"),
        "session list wrapper should receive list subcommand: {log}"
    );
    assert!(
        log.contains("arg=--format") && log.contains("arg=json"),
        "session list wrapper should receive JSON format args: {log}"
    );
    assert!(
        log.contains("arg=--max-count") && log.contains(&format!("arg={limit}")),
        "session list wrapper should receive max-count limit {limit}: {log}"
    );
}

pub fn assert_enumerate_error_code(response: &Value, code: &str) {
    assert_eq!(response["error"]["code"], code);
}

pub fn assert_error_message_contains(response: &Value, needle: &str) {
    let message = response["error"]["message"]
        .as_str()
        .expect("error message string");
    assert!(
        message.contains(needle),
        "error message should contain {needle:?}: {message}"
    );
}

pub fn enumerate_sessions(result: &Value) -> &[Value] {
    result["sessions"].as_array().expect("sessions array")
}

pub fn enumerate_warnings(result: &Value) -> &[Value] {
    result["warnings"].as_array().expect("warnings array")
}

pub fn enumerate_warning_text(result: &Value) -> String {
    enumerate_warnings(result)
        .iter()
        .map(|warning| warning.as_str().expect("warning string"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn assert_enumerate_entry(
    entry: &Value,
    provider_session_id: &str,
    title: Option<&str>,
    cwd: Option<&str>,
    created_unix_ms: Option<u64>,
    updated_unix_ms: Option<u64>,
    turn_count: Option<u64>,
) {
    assert_eq!(entry["provider_session_id"], provider_session_id);
    assert_optional_string(&entry["title"], title, "title");
    assert_optional_string(&entry["cwd"], cwd, "cwd");
    assert_optional_u64(
        &entry["created_unix_ms"],
        created_unix_ms,
        "created_unix_ms",
    );
    assert_optional_u64(
        &entry["updated_unix_ms"],
        updated_unix_ms,
        "updated_unix_ms",
    );
    assert_optional_u64(&entry["turn_count"], turn_count, "turn_count");
    assert_eq!(entry["source"]["kind"], "opencode.session_list");
    assert!(entry["source"]["detail"].as_str().is_some());
}

pub fn assert_optional_string(value: &Value, expected: Option<&str>, label: &str) {
    match expected {
        Some(expected) => assert_eq!(value.as_str(), Some(expected), "{label}"),
        None => assert!(value.is_null(), "{label} should be null: {value}"),
    }
}

pub fn assert_optional_u64(value: &Value, expected: Option<u64>, label: &str) {
    match expected {
        Some(expected) => assert_eq!(value.as_u64(), Some(expected), "{label}"),
        None => assert!(value.is_null(), "{label} should be null: {value}"),
    }
}

pub fn assert_launch_capture_result(result: &Value, session_id: &str) {
    assert_launch_capture_state(result, session_id);
    assert_capture_artifacts(&result["artifacts"]);
}

pub fn assert_launch_capture_state(result: &Value, session_id: &str) {
    assert_launch_capture_provider_session(result, session_id);
    assert_launch_capture_state_source(result);
}

pub fn assert_launch_capture_provider_session(result: &Value, session_id: &str) {
    assert_eq!(
        result["provider_session_id"].as_str(),
        Some(session_id),
        "capture should preserve the launch-derived opencode sessionID"
    );
}

pub fn assert_launch_capture_state_source(result: &Value) {
    assert_eq!(
        result["state"]["source"].as_str(),
        Some("launch.session.provider_session_id"),
        "launch.session.provider_session_id should be the canonical launch evidence key"
    );
}

pub fn assert_capture_artifacts(artifacts: &Value) {
    let artifacts = capture_artifacts_array(artifacts);
    assert_capture_artifact_collection(artifacts);
    assert_capture_artifact_entries(artifacts);
}

pub fn capture_artifacts_array(artifacts: &Value) -> &[Value] {
    artifacts.as_array().expect("artifacts array")
}

pub fn assert_capture_artifact_collection(artifacts: &[Value]) {
    assert!(
        !artifacts.is_empty(),
        "capture should return source artifacts"
    );
}

pub fn assert_capture_artifact_entries(artifacts: &[Value]) {
    for artifact in artifacts {
        assert_not_private_db_artifact(artifact);
    }
}

pub fn assert_not_private_db_artifact(artifact: &Value) {
    if let Some(path) = artifact.get("path").and_then(Value::as_str) {
        assert!(
            !path.contains("opencode.db") && !path.contains(".opencode"),
            "capture artifacts should avoid private DB path assumptions: {artifact}"
        );
    }
}

pub fn assert_bare_capture_result(result: &Value, session_id: &str) {
    assert_eq!(
        result["provider_session_id"].as_str(),
        Some(session_id),
        "capture should preserve the declared bare session_id fallback"
    );
    assert_eq!(
        result["state"]["source"].as_str(),
        Some("session_id"),
        "session_id should be the canonical bare evidence key"
    );
}

pub fn assert_removed_evidence_capture_error(response: &Value) {
    assert_eq!(
        removed_evidence_error_code(response),
        "invalid_session_capture_params",
        "removed evidence.provider_session_id shape must not be accepted"
    );
}

pub fn removed_evidence_error_code(response: &Value) -> &Value {
    &response["error"]["code"]
}

pub fn assert_replace_response(output: &std::process::Output) {
    let response = json_stdout(output);
    assert_replace_response_envelope(output, &response);
}

pub fn assert_replace_response_envelope(output: &std::process::Output, response: &Value) {
    if replace_response_unsupported(response) {
        assert_unsupported_replace_response(response);
    } else {
        assert_successful_replace_response(output, response);
    }
}

pub fn replace_response_unsupported(response: &Value) -> bool {
    response["ok"] == false
}

pub fn assert_unsupported_replace_response(response: &Value) {
    assert_valid(response, "common.schema.json#/$defs/ErrorResponseEnvelope");
    assert_eq!(
        response["error"]["category"], "unsupported",
        "session.replace should be honestly unsupported rather than mutating opencode storage"
    );
}

pub fn assert_successful_replace_response(output: &std::process::Output, response: &Value) {
    assert_successful_replace_process(output);
    assert_successful_replace_schemas(response);
    assert_successful_replace_result_fields(&response["result"]);
}

pub fn assert_successful_replace_process(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "successful session.replace envelope should exit zero; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn assert_successful_replace_schemas(response: &Value) {
    assert_valid(
        response,
        "session.schema.json#/$defs/SessionReplaceResponse",
    );
    assert_valid(
        &response["result"],
        "session.schema.json#/$defs/SessionReplaceResult",
    );
}

pub fn assert_successful_replace_result_fields(result: &Value) {
    assert_replace_changed_false(result);
    assert_replace_artifacts_empty(replace_artifacts(result));
}

pub fn assert_replace_changed_false(result: &Value) {
    assert_eq!(result["changed"], false);
}

pub fn replace_artifacts(result: &Value) -> &[Value] {
    result["artifacts"].as_array().expect("artifacts array")
}

pub fn assert_replace_artifacts_empty(artifacts: &[Value]) {
    assert_eq!(
        artifacts.len(),
        0,
        "changed=false replace fallback should not report storage artifacts"
    );
}

pub fn assert_file_unchanged(path: &Path, before: &str, message: &str) {
    assert_eq!(file_sha256(path), before, "{message}");
}
