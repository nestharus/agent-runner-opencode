//! Declared roles: orchestration

mod cluster_b;
#[allow(dead_code)]
mod support;

use cluster_b::*;
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
    assert_session_list_limit_forwarded(fake_opencode.log_path(), 2);
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

    assert_enumerate_error_code(&response, "invalid_opencode_session_list");
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

    assert_enumerate_error_code(&response, "session_list_failed");
    assert_error_message_contains(&response, "list failed");
}

#[test]
#[ignore = "live opencode auth/network session export proof; run explicitly when external dependencies are available"]
fn integration_session_export_live() {
    let session_id = live_opencode_session_id();
    let result = success_result(
        invoke("session.export", session_params(&session_id)),
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
            pinned_lifecycle_capture_params(pinned_session_id, session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_pinned_capture_result(&pinned_result, pinned_session_id);
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
