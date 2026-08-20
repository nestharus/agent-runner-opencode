//! Declared roles: orchestration

mod cluster_b;
#[allow(dead_code)]
mod support;

use cluster_b::*;
use serde_json::{json, Value};
use support::{invoke, invoke_validated, invoke_with_env, invoke_with_host_and_env};

#[test]
fn characterization_opencode_session_export_json() {
    let export = native_export_fixture();
    assert_native_export_fixture(&export);
}

#[test]
fn contract_session_read_turns() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let params = session_params(session_id);

    let result = read_turns_result(params.clone(), &path);
    let first_ids = assert_first_read_turns_result(&result);

    let second = read_turns_result(params, &path);
    assert_stable_turn_ids(&second, &first_ids);

    assert_missing_read_turns_error(&path);
}

#[test]
fn contract_session_read_turns_uses_regular_file_for_export_stdout() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::pipe_truncating(session_id);
    let path = prepend_path(fake_opencode.dir());

    let result = read_turns_result(session_params(session_id), &path);

    assert_first_read_turns_result(&result);
}

#[test]
fn contract_session_read_turns_projects_bounded_user_observation() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let result = read_turns_result(
        json!({
            "settings_id": "opencode1",
            "session_id": session_id,
            "turn_projection": "user_observation",
            "body_tail_limit": 4
        }),
        &path,
    );

    assert_eq!(result["turn_count"], 1);
    let turns = result["turns"].as_array().expect("projected turns array");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0]["role"], "user");
    assert_eq!(turns[0]["session_id"], session_id);
    assert!(turns[0].get("native").is_none());
    assert_eq!(
        turns[0]["body"][0]["text"],
        "\"reply with the single word: ok\""
    );
}

#[test]
fn contract_session_export_canonical() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let params = session_params(session_id);

    let first = success_result(
        invoke_with_env("session.export", params.clone(), &[("PATH", path.as_str())]),
        "session.schema.json#/$defs/SessionExportResponse",
        "session.schema.json#/$defs/SessionExportResult",
    );
    assert_canonical_export_result(
        &first,
        "sha256 must be computed over decoded data_base64 bytes",
    );

    let second = success_result(
        invoke_with_env("session.export", params, &[("PATH", path.as_str())]),
        "session.schema.json#/$defs/SessionExportResponse",
        "session.schema.json#/$defs/SessionExportResult",
    );
    assert_deterministic_export(&first, &second);
}

#[test]
fn contract_session_enumerate_empty_list() {
    let fake_opencode = FakeOpencodeSessionList::with_output("[]", "", 0);
    let path = prepend_path(fake_opencode.dir());

    let result = enumerate_result(session_enumerate_params(), &path);

    assert_empty_enumerate_result(&result);
}

#[test]
fn contract_session_enumerate_maps_multiple_sessions() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_multiple_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());

    let result = enumerate_result(session_enumerate_params(), &path);

    assert_multiple_enumerate_result(&result);
}

#[test]
fn contract_session_enumerate_returns_warning_for_bad_cwd_rows() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_bad_cwd_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());

    let result = enumerate_result(session_enumerate_params(), &path);

    assert_bad_cwd_enumerate_result(&result);
}

#[test]
fn contract_session_enumerate_honors_limit() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());

    let result = enumerate_result(session_enumerate_limit_params(2), &path);

    assert_limited_enumerate_result(&result, 2);
    assert_session_list_is_exhaustive(fake_opencode.log_path());

    let cursor = result["next_cursor"]
        .as_str()
        .expect("truncated page cursor");
    let second = enumerate_result(session_enumerate_cursor_params(2, cursor), &path);
    assert_second_enumerate_page(&second);
}

#[test]
fn contract_session_enumerate_invalid_json_is_provider_error() {
    let fake_opencode = FakeOpencodeSessionList::with_output("not json", "", 0);
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_env(
        "session.enumerate",
        session_enumerate_params(),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_opencode_session_list");
}

#[test]
fn contract_session_enumerate_nonzero_wrapper_exit_is_provider_error() {
    let fake_opencode = FakeOpencodeSessionList::with_output("[]", "list failed", 9);
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_env(
        "session.enumerate",
        session_enumerate_params(),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "session_list_failed");
    assert!(response["error"]["message"]
        .as_str()
        .expect("error message string")
        .contains("list failed"));
}

#[test]
#[ignore = "live opencode auth/network session export proof; run explicitly when external dependencies are available"]
fn integration_session_export_live() {
    let session_id = live_opencode_session_id();
    let path = std::env::var("PATH").expect("live PATH");
    let home = std::env::var("HOME").expect("live HOME");
    let result = success_result(
        invoke_with_env(
            "session.export",
            session_params_for_settings("opencode5", &session_id),
            &[("PATH", path.as_str()), ("HOME", home.as_str())],
        ),
        "session.schema.json#/$defs/SessionExportResponse",
        "session.schema.json#/$defs/SessionExportResult",
    );

    assert_canonical_export_result(
        &result,
        "live session.export sha256 must be computed over decoded data_base64 bytes",
    );
}

#[test]
fn contract_session_capture() {
    let session_id = fixture_session_id();
    let result = success_result(
        invoke_validated(
            "session.capture",
            launch_capture_params(session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_launch_capture_result(&result, session_id);

    let bare_session_result = success_result(
        invoke_validated(
            "session.capture",
            bare_capture_params(session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_bare_capture_result(&bare_session_result, session_id);

    let lifecycle_result = success_result(
        invoke_validated(
            "session.capture",
            lifecycle_capture_params(session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_lifecycle_capture_result(&lifecycle_result, session_id);

    let pinned_session_id = "ses_pinned_lifecycle";
    let pinned_result = success_result(
        invoke_validated(
            "session.capture",
            pinned_lifecycle_capture_params(pinned_session_id, pinned_session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_pinned_capture_result(&pinned_result, pinned_session_id);
}

#[test]
fn contract_session_capture_rejects_conflicting_identity_carriers() {
    let session_id = fixture_session_id();
    for params in [
        conflicting_launch_capture_params(session_id),
        pinned_lifecycle_capture_params("ses_pinned_conflict", session_id),
    ] {
        let response = assert_error_envelope(invoke_validated(
            "session.capture",
            params,
            "session.schema.json#/$defs/SessionCaptureRequest",
        ));
        assert_eq!(response["error"]["code"], "invalid_session_capture_params");
        assert!(response["error"]["message"]
            .as_str()
            .expect("error message string")
            .contains("conflicting session evidence"));
    }
}

#[test]
fn contract_session_capture_validates_exact_live_report() {
    let session_id = fixture_session_id();
    let invocation_uuid = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let working_directory = native_export_fixture()["info"]["directory"]
        .as_str()
        .unwrap()
        .to_string();

    let result = success_result(
        invoke_with_host_and_env(
            "session.capture",
            live_capture_params(session_id, invocation_uuid),
            json!({"working_directory": working_directory}),
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );

    assert_live_capture_result(&result, session_id);
}

#[test]
fn contract_session_capture_rejects_live_report_workspace_mismatch() {
    let session_id = fixture_session_id();
    let invocation_uuid = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_host_and_env(
        "session.capture",
        live_capture_params(session_id, invocation_uuid),
        json!({"working_directory": "/tmp/not-the-exported-workspace"}),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_session_capture_params");
}

#[test]
fn contract_session_capture_rejects_live_report_invocation_mismatch() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let mut params = live_capture_params(session_id, "cccccccc-cccc-4ccc-8ccc-cccccccccccc");
    params["live_report"]["invocation_uuid"] =
        Value::String("dddddddd-dddd-4ddd-8ddd-dddddddddddd".to_string());

    let response = assert_error_envelope(invoke_with_env(
        "session.capture",
        params,
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_session_capture_params");
}

#[test]
fn contract_session_capture_rejects_removed_evidence_shape() {
    let session_id = fixture_session_id();
    let response = assert_error_envelope(invoke(
        "session.capture",
        removed_evidence_capture_params(session_id),
    ));
    assert_removed_evidence_capture_error(&response);
}

#[test]
fn contract_session_locate_not_located() {
    let session_id = fixture_session_id();
    let result = success_result(
        invoke_with_env("session.locate_transcript", session_params(session_id), &[]),
        "session.schema.json#/$defs/SessionLocateTranscriptResponse",
        "session.schema.json#/$defs/SessionLocateTranscriptResult",
    );
    assert_not_located_result(&result);
}

#[test]
fn contract_session_replace_unsupported() {
    let session_id = fixture_session_id();
    let fixture = SessionReplaceFixture::new();

    let output = invoke_with_host_and_env(
        "session.replace",
        session_replace_params(session_id),
        fixture.host_override(),
        &[],
    );

    assert_replace_response(&output);
    fixture.assert_unchanged();
}
