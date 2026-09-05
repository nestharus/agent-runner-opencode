// declared_role: validator, orchestration
#![allow(unused_imports)]

use super::*;

pub fn assert_native_export_fixture(export: &Value) {
    let info = export["info"].as_object().expect("export.info object");
    let session_id = info["id"].as_str().expect("info.id string");
    assert!(
        session_id.starts_with("ses_"),
        "unexpected session id {session_id}"
    );
    assert!(
        info["title"]
            .as_str()
            .is_some_and(|title| !title.is_empty()),
        "info.title should be a non-empty native opencode title"
    );

    let messages = export["messages"].as_array().expect("messages array");
    assert!(
        !messages.is_empty(),
        "native export should include messages"
    );
    let mut part_types = BTreeSet::new();
    for message in messages {
        let role = message["info"]["role"]
            .as_str()
            .expect("message.info.role string");
        assert!(
            matches!(role, "user" | "assistant"),
            "unexpected native message role {role}"
        );
        assert_eq!(
            message["info"]["sessionID"].as_str(),
            Some(session_id),
            "message sessionID should match export info.id"
        );
        let parts = message["parts"].as_array().expect("message.parts array");
        assert!(!parts.is_empty(), "native message should include parts");
        for part in parts {
            part_types.insert(part["type"].as_str().expect("native part type").to_string());
            assert_eq!(
                part["sessionID"].as_str(),
                Some(session_id),
                "part sessionID should match export info.id"
            );
        }
    }
    for expected in ["step-start", "text", "step-finish"] {
        assert!(
            part_types.contains(expected),
            "native export should include a {expected} part; saw {part_types:?}"
        );
    }
    assert!(
        export.get("contract").is_none(),
        "native opencode export is source material, not a provider contract envelope"
    );
}

pub fn assert_not_located_result(result: &Value) {
    assert_eq!(result["located"], false);
    assert!(
        result.get("path").is_none(),
        "opencode has no transcript file, so locate_transcript must omit path"
    );
    for (key, message) in [
        (
            "format_id",
            "not-located response should still identify the transcript/export format",
        ),
        (
            "source_id",
            "not-located response should still identify the opencode source",
        ),
    ] {
        assert!(
            result[key].as_str().is_some_and(|value| !value.is_empty()),
            "{message}"
        );
    }
}

pub fn assert_canonical_export_result(result: &Value, sha_message: &str) {
    assert_eq!(result["canonical_format"], CANONICAL_FORMAT);
    let decoded = canonical_result_decoded_bytes(result);
    assert_eq!(
        canonical_bytes_sha(&decoded),
        canonical_result_sha(result),
        "{sha_message}"
    );
    assert_eq!(
        canonical_record_count(&decoded),
        canonical_result_turn_count(result),
        "canonical record count must match turn_count"
    );

    let text = std::str::from_utf8(&decoded).expect("canonical export UTF-8");
    let records = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).expect("canonical record"))
        .collect::<Vec<_>>();
    assert!(!records.is_empty());
    for record in records {
        assert_eq!(record["metadata"]["provider_id"], "openai");
        assert_eq!(record["metadata"]["model_id"], "gpt-5.5");
        assert_eq!(record["metadata"]["variant"], "low");
    }
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
    assert_eq!(
        result["sessions"].as_array().expect("sessions array").len(),
        0
    );
    assert_eq!(result["complete"], true);
    assert!(result["next_cursor"].is_null());
    assert_eq!(
        result["warnings"].as_array().expect("warnings array").len(),
        0
    );
}

pub fn assert_multiple_enumerate_result(result: &Value) {
    let sessions = result["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 2);
    for (entry, provider_session_id, title, cwd, created, updated, turn_count) in [
        (
            &sessions[0],
            "ses_list_one",
            Some("First session"),
            Some("/tmp/project-one"),
            Some(111),
            Some(222),
            Some(3),
        ),
        (
            &sessions[1],
            "ses_list_two",
            None,
            Some("/var/tmp/project-two"),
            Some(333),
            Some(444),
            Some(0),
        ),
    ] {
        assert_eq!(entry["provider_session_id"], provider_session_id);
        match title {
            Some(value) => assert_eq!(entry["title"].as_str(), Some(value)),
            None => assert!(entry["title"].is_null()),
        }
        match cwd {
            Some(value) => assert_eq!(entry["cwd"].as_str(), Some(value)),
            None => assert!(entry["cwd"].is_null()),
        }
        for (field, expected) in [
            ("created_unix_ms", created),
            ("updated_unix_ms", updated),
            ("turn_count", turn_count),
        ] {
            match expected {
                Some(value) => assert_eq!(entry[field].as_u64(), Some(value), "{field}"),
                None => assert!(entry[field].is_null(), "{field} should be null: {entry}"),
            }
        }
        assert_eq!(entry["source"]["kind"], "opencode.session_list");
        assert!(entry["source"]["detail"].as_str().is_some());
    }
    assert_eq!(
        result["warnings"].as_array().expect("warnings array").len(),
        0
    );
}

pub fn assert_bad_cwd_enumerate_result(result: &Value) {
    let sessions = result["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0]["provider_session_id"], "ses_relative_cwd");
    assert!(sessions[0]["cwd"].is_null());
    assert_eq!(sessions[1]["provider_session_id"], "ses_missing_cwd");
    assert!(sessions[1]["cwd"].is_null());
    let warnings = result["warnings"]
        .as_array()
        .expect("warnings array")
        .iter()
        .map(|warning| warning.as_str().expect("warning string"))
        .collect::<Vec<_>>()
        .join("\n");
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
    let sessions = result["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), limit);
    assert_eq!(sessions[0]["provider_session_id"], "ses_limit_one");
    assert_eq!(sessions[1]["provider_session_id"], "ses_limit_two");
    assert_eq!(result["complete"], false);
    assert!(
        result["next_cursor"]
            .as_str()
            .is_some_and(|cursor| cursor.starts_with("v3:")),
        "truncated enumeration must return an opaque continuation cursor: {result}"
    );
}

pub fn assert_session_list_uses_bounded_snapshot(log_path: &Path) {
    let log = fs::read_to_string(log_path).expect("read fake session list wrapper log");
    assert!(
        log.contains("arg=session"),
        "missing session subcommand: {log}"
    );
    assert!(log.contains("arg=list"), "missing list subcommand: {log}");
    assert!(
        log.contains("arg=--format") && log.contains("arg=json"),
        "missing JSON format args: {log}"
    );
    assert!(
        log.contains("arg=--max-count"),
        "missing native row bound: {log}"
    );
    assert!(
        log.contains("arg=257"),
        "provider pagination must request one sentinel row above its 256-session snapshot bound: {log}"
    );
}

pub fn assert_second_enumerate_page(result: &Value) {
    let sessions = result["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["provider_session_id"], "ses_limit_three");
    assert_eq!(result["complete"], true);
    assert!(result["next_cursor"].is_null());
}

fn assert_capture_artifacts(artifacts: &Value) {
    let artifacts = artifacts.as_array().expect("artifacts array");
    assert!(
        !artifacts.is_empty(),
        "capture should return source artifacts"
    );
    for artifact in artifacts {
        if let Some(path) = artifact.get("path").and_then(Value::as_str) {
            assert!(
                !path.contains("opencode.db") && !path.contains(".opencode"),
                "capture artifacts should avoid private DB path assumptions: {artifact}"
            );
        }
    }
}

pub fn assert_launch_capture_result(result: &Value, session_id: &str) {
    assert_eq!(
        result["provider_session_id"].as_str(),
        Some(session_id),
        "capture should preserve the launch-derived opencode sessionID"
    );
    assert_eq!(
        result["state"]["source"].as_str(),
        Some("launch.session.provider_session_id"),
        "launch.session.provider_session_id should be the canonical launch evidence key"
    );
    assert_capture_artifacts(&result["artifacts"]);
}

pub fn assert_live_capture_result(result: &Value, session_id: &str) {
    assert_eq!(result["provider_session_id"].as_str(), Some(session_id));
    assert_eq!(
        result["state"]["source"].as_str(),
        Some("live_report.provider_session_id")
    );
    assert_capture_artifacts(&result["artifacts"]);
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

pub fn assert_lifecycle_capture_result(result: &Value, session_id: &str) {
    assert_eq!(
        result["provider_session_id"].as_str(),
        Some(session_id),
        "capture should preserve the lifecycle-bound provider session"
    );
    assert_eq!(
        result["state"]["source"].as_str(),
        Some("start_bound_provider_session_id"),
        "capture should identify lifecycle-bound session evidence"
    );
}

pub fn assert_pinned_capture_result(result: &Value, session_id: &str) {
    assert_eq!(result["provider_session_id"].as_str(), Some(session_id));
    assert_eq!(result["state"]["source"].as_str(), Some("pinned_target"));
}

pub fn assert_removed_evidence_capture_error(response: &Value) {
    assert_eq!(
        response["error"]["code"], "invalid_session_capture_params",
        "removed evidence.provider_session_id shape must not be accepted"
    );
}

pub fn assert_replace_response(output: &std::process::Output) {
    let response = json_stdout(output);
    if response["ok"] == false {
        assert_valid(&response, "common.schema.json#/$defs/ErrorResponseEnvelope");
        assert_eq!(
            response["error"]["category"], "unsupported",
            "session.replace should be honestly unsupported rather than mutating opencode storage"
        );
        return;
    }

    assert!(
        output.status.success(),
        "successful session.replace envelope should exit zero; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_valid(
        &response,
        "session.schema.json#/$defs/SessionReplaceResponse",
    );
    assert_valid(
        &response["result"],
        "session.schema.json#/$defs/SessionReplaceResult",
    );
    assert_eq!(response["result"]["changed"], false);
    assert_eq!(
        response["result"]["artifacts"]
            .as_array()
            .expect("artifacts array")
            .len(),
        0,
        "changed=false replace fallback should not report storage artifacts"
    );
}
