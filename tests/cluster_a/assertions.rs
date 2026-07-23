// declared_role: validator, accessor, predicate, orchestration
#![allow(unused_imports)]

use super::*;

pub fn assert_opencode_launch_fixture(fixture: &str) {
    let events = parse_opencode_fixture_events(fixture);
    assert_opencode_fixture_events(&events);
}

pub fn assert_opencode_fixture_events(events: &[NumberedFixtureEvent]) {
    let mut coverage = FixtureCoverage::default();
    for numbered in events {
        assert_opencode_fixture_event(numbered);
        coverage.record(fixture_event_type(numbered));
    }
    assert_fixture_coverage(&coverage);
}

pub fn assert_opencode_fixture_event(numbered: &NumberedFixtureEvent) {
    let event_type = fixture_event_type(numbered);
    let session_id = fixture_event_session_id(numbered);
    let part = fixture_event_part(numbered);
    let part_type = fixture_part_type(part, numbered.line_number);
    assert_fixture_event_type(event_type, numbered.line_number);
    assert_fixture_timestamp(numbered);
    assert_fixture_session_id(session_id);
    assert_fixture_part_type(part_type, numbered.line_number);
    assert_fixture_part_session(part, session_id);
    assert_native_fixture_event(numbered);
    assert_fixture_event_payload(event_type, part);
}

pub fn assert_fixture_event_type(event_type: &str, line_number: usize) {
    assert!(
        matches!(event_type, "step_start" | "text" | "step_finish"),
        "unexpected opencode event type {event_type} on fixture line {line_number}"
    );
}

pub fn assert_fixture_timestamp(numbered: &NumberedFixtureEvent) {
    assert!(
        numbered.event["timestamp"].as_u64().is_some(),
        "fixture line {} missing millisecond timestamp",
        numbered.line_number
    );
}

pub fn assert_fixture_session_id(session_id: &str) {
    assert!(
        session_id.starts_with("ses_"),
        "unexpected sessionID {session_id}"
    );
}

pub fn assert_fixture_part_type(part_type: &str, line_number: usize) {
    assert!(
        matches!(part_type, "step-start" | "text" | "step-finish"),
        "unexpected part.type {part_type} on fixture line {line_number}"
    );
}

pub fn assert_fixture_part_session(part: &serde_json::Map<String, Value>, session_id: &str) {
    assert_eq!(
        part.get("sessionID").and_then(Value::as_str),
        Some(session_id),
        "nested part sessionID should match top-level sessionID"
    );
}

pub fn assert_native_fixture_event(numbered: &NumberedFixtureEvent) {
    for key in ["contract", "request_id", "seq", "kind"] {
        assert!(
            numbered.event.get(key).is_none(),
            "native opencode event is not a contract event"
        );
    }
}

pub fn assert_fixture_event_payload(event_type: &str, part: &serde_json::Map<String, Value>) {
    match event_type {
        "text" => assert_fixture_text_part(part),
        "step_finish" => assert_fixture_step_finish_part(part),
        _ => {}
    }
}

pub fn assert_fixture_text_part(part: &serde_json::Map<String, Value>) {
    assert_eq!(part.get("text").and_then(Value::as_str), Some("ok"));
    assert!(
        part.get("time").is_some(),
        "text part should carry timing metadata"
    );
}

pub fn assert_fixture_step_finish_part(part: &serde_json::Map<String, Value>) {
    let tokens = part
        .get("tokens")
        .and_then(Value::as_object)
        .expect("step_finish part should carry token metadata");
    for token_field in ["total", "input", "output", "reasoning"] {
        assert!(
            tokens.get(token_field).and_then(Value::as_u64).is_some(),
            "tokens.{token_field} should be present"
        );
    }
    assert!(
        part.get("cost").and_then(Value::as_f64).is_some(),
        "step_finish part should carry numeric cost metadata"
    );
}

pub fn assert_fixture_coverage(coverage: &FixtureCoverage) {
    assert!(coverage.saw_step_start, "fixture should include step_start");
    assert!(coverage.saw_text, "fixture should include text");
    assert!(
        coverage.saw_step_finish,
        "fixture should include step_finish"
    );
}

pub fn assert_contract_launch_stream_output(
    output: &std::process::Output,
    wrapper_log_path: &Path,
    fixture_session_id: &str,
) {
    assert_stderr_diagnostics_only(output);
    let events = launch_events_from_output(output, "launch stdout");
    assert_contract_launch_events(&events, fixture_session_id);
    assert_output_status_code(
        output,
        Some(7),
        "provider process should preserve nonzero child exit-code parity",
    );
    assert_wrapper_log(wrapper_log_path);
}

pub fn assert_contract_launch_events(events: &[Value], fixture_session_id: &str) {
    assert_monotonic_launch_events(events);
    assert_launch_stream_bytes(events);
    assert_session_marker(events, fixture_session_id);
    assert_exit_event(
        events,
        json!({ "kind": "exited", "code": 7 }),
        fixture_session_id,
    );
}

pub fn assert_output_status_code(
    output: &std::process::Output,
    expected: Option<i32>,
    message: &str,
) {
    assert_eq!(output.status.code(), expected, "{message}");
}

pub fn assert_launch_events_not_empty(events: &[Value], label: &str) {
    assert!(!events.is_empty(), "{label} must contain NDJSON events");
}

pub fn assert_launch_stream_bytes(events: &[Value]) {
    assert_launch_stdout_bytes(&collect_stream_bytes(events, "stdout"));
    assert_launch_stderr_bytes(&collect_stream_bytes(events, "stderr"));
}

pub fn assert_launch_stdout_bytes(stdout_bytes: &[u8]) {
    assert_eq!(
        stdout_bytes, FAKE_LAUNCH_STDOUT,
        "stdout events must byte-preserve the selected opencodeN wrapper output"
    );
}

pub fn assert_launch_stderr_bytes(stderr_bytes: &[u8]) {
    assert_eq!(
        stderr_bytes, FAKE_LAUNCH_STDERR,
        "stderr events must byte-preserve the selected opencodeN wrapper output"
    );
}

pub fn assert_session_marker(events: &[Value], fixture_session_id: &str) {
    let session_marker = expected_session_marker(events, fixture_session_id);
    assert_session_marker_truthy(session_marker);
}

pub fn assert_session_marker_truthy(session_marker: &Value) {
    assert_eq!(
        session_marker["value"], true,
        "session marker should use a truthy marker value"
    );
}

pub fn assert_exit_event(events: &[Value], expected_status: Value, fixture_session_id: &str) {
    let final_event = final_launch_event(events);
    assert_exit_event_kind(final_event);
    assert_exit_event_status_present(final_event);
    assert_exit_event_terminal_signal_present(final_event);
    assert_process_status_kind(&final_event["status"]);
    assert_exit_event_status(final_event, expected_status);
    assert_status_derived_terminal_signal(final_event);
    assert_exit_event_session_present(final_event);
    assert_exit_event_session_contains(final_event, fixture_session_id);
}

pub fn assert_exit_event_kind(final_event: &Value) {
    assert_eq!(
        final_event["kind"], "exit",
        "final launch line must be exit"
    );
}

pub fn assert_exit_event_status_present(final_event: &Value) {
    assert!(
        final_event.get("status").is_some(),
        "exit event must carry status"
    );
}

pub fn assert_exit_event_terminal_signal_present(final_event: &Value) {
    assert!(
        final_event.get("terminal_signal").is_some(),
        "exit event must carry terminal_signal"
    );
}

pub fn assert_exit_event_status(final_event: &Value, expected_status: Value) {
    assert_eq!(
        final_event["status"], expected_status,
        "final status should truthfully report the controlled wrapper exit status"
    );
}

pub fn assert_exit_event_session_present(final_event: &Value) {
    assert!(
        final_event.get("session").is_some(),
        "exit event must carry captured session evidence"
    );
}

pub fn assert_exit_event_session_contains(final_event: &Value, fixture_session_id: &str) {
    assert!(
        json_contains_string(&final_event["session"], fixture_session_id),
        "exit.session must carry the same opencode sessionID evidence as the marker; session={}",
        final_event["session"]
    );
}

pub fn assert_status_derived_terminal_signal(final_event: &Value) {
    assert_eq!(
        final_event["terminal_signal"]["kind"],
        expected_signal_kind_for_status(&final_event["status"]),
        "terminal_signal should be status-derived"
    );
}

pub fn assert_wrapper_log(wrapper_log_path: &Path) {
    let wrapper_log = wrapper_log_text(wrapper_log_path);
    assert_selected_wrapper_invoked(&wrapper_log);
    assert_wrapper_run_arg(&wrapper_log);
}

pub fn assert_selected_wrapper_invoked(wrapper_log: &str) {
    assert!(
        wrapper_log_has_selected_wrapper(wrapper_log),
        "launch should cross the selected opencode1 wrapper boundary; log={wrapper_log:?}"
    );
}

pub fn assert_wrapper_run_arg(wrapper_log: &str) {
    assert!(
        wrapper_log_has_run_arg(wrapper_log),
        "wrapper should receive opencode run argv; log={wrapper_log:?}"
    );
}

pub fn assert_declared_env_boundary(output: &std::process::Output, wrapper_log_path: &Path) {
    assert_stderr_diagnostics_only(output);
    let events = parse_launch_events(&output.stdout);
    let final_event = final_launch_event(&events);
    assert_declared_env_exit_event(final_event);
    assert_declared_env_log(wrapper_log_path);
}

pub fn assert_declared_env_exit_event(final_event: &Value) {
    assert_eq!(final_event["kind"], "exit");
    assert_eq!(
        final_event["status"],
        json!({ "kind": "exited", "code": 0 })
    );
}

pub fn assert_declared_env_log(wrapper_log_path: &Path) {
    let wrapper_log = declared_env_log_text(wrapper_log_path);
    assert_declared_child_env_logged(&wrapper_log);
    assert_declared_xdg_data_home_logged(&wrapper_log);
    assert_oulipoly_linkage_logged(&wrapper_log);
    assert_undeclared_child_env_unset(&wrapper_log);
    assert_ambient_secret_absent(&wrapper_log);
    assert_openai_api_key_unset(&wrapper_log);
}

pub fn assert_declared_child_env_logged(wrapper_log: &str) {
    assert!(
        wrapper_log.contains("declared=declared-child-value"),
        "declared params.env value must reach child; log={wrapper_log:?}"
    );
}

pub fn assert_declared_xdg_data_home_logged(wrapper_log: &str) {
    assert!(
        wrapper_log.contains("xdg=/tmp/declared-opencode-data-home"),
        "declared XDG_DATA_HOME must reach child; log={wrapper_log:?}"
    );
}

pub fn assert_oulipoly_linkage_logged(wrapper_log: &str) {
    assert!(
        wrapper_log.contains("oulipoly_data=/tmp/real-oulipoly-data"),
        "OULIPOLY_DATA_DIR must reach env-cleared launch child; log={wrapper_log:?}"
    );
    assert!(
        wrapper_log.contains("oulipoly_parent=parent-invocation-token"),
        "OULIPOLY_PARENT_INVOCATION must reach env-cleared launch child; log={wrapper_log:?}"
    );
    assert!(
        wrapper_log.contains("agent_runner_bin=/tmp/target-release/oulipoly-agent-runner"),
        "AGENT_BASH_AGENT_RUNNER_BIN must reach env-cleared launch child; log={wrapper_log:?}"
    );
}

pub fn assert_undeclared_child_env_unset(wrapper_log: &str) {
    assert!(
        wrapper_log.contains("undeclared=<unset>"),
        "undeclared parent env must not reach child; log={wrapper_log:?}"
    );
}

pub fn assert_ambient_secret_absent(wrapper_log: &str) {
    assert!(
        !wrapper_log.contains("ambient-secret-do-not-leak"),
        "undeclared parent env value leaked into child log; log={wrapper_log:?}"
    );
}

pub fn assert_openai_api_key_unset(wrapper_log: &str) {
    assert!(
        wrapper_log.contains("openai=<unset>"),
        "ambient OPENAI_API_KEY must not reach child; log={wrapper_log:?}"
    );
    assert!(
        !wrapper_log.contains("ambient-openai-secret-do-not-leak"),
        "ambient OPENAI_API_KEY value leaked into child log; log={wrapper_log:?}"
    );
}

pub fn assert_heartbeat_launch_output(output: &std::process::Output) {
    assert_stderr_diagnostics_only(output);
    let events = launch_events_from_output(output, "launch stdout");
    assert_monotonic_launch_events(&events);
    assert_heartbeat_event_present(&events);
    let final_event = final_launch_event(&events);
    assert_final_launch_exit_kind(final_event);
    assert_process_status_kind(&final_event["status"]);
    assert_status_derived_terminal_signal(final_event);
}

pub fn assert_heartbeat_event_present(events: &[Value]) {
    assert!(
        has_heartbeat_event(events),
        "slow launch should deterministically emit at least one heartbeat before exit; events={events:?}"
    );
}

pub fn assert_final_launch_exit_kind(final_event: &Value) {
    assert_eq!(
        final_event["kind"], "exit",
        "final launch line must be exit"
    );
}

pub fn assert_deadline_launch_output(deadline_output: &std::process::Output) {
    assert_stderr_diagnostics_only(deadline_output);
    let deadline_events = launch_events_from_output(deadline_output, "deadline launch stdout");
    assert_monotonic_launch_events(&deadline_events);
    let deadline_final_event = final_launch_event(&deadline_events);
    assert_deadline_final_event(deadline_final_event);
    assert_deadline_provider_exit_code(deadline_output);
}

pub fn assert_deadline_final_event(deadline_final_event: &Value) {
    assert_eq!(
        deadline_final_event["kind"], "exit",
        "final deadline launch line must be exit"
    );
    assert_eq!(
        deadline_final_event["status"]["kind"], "prolonged_silence",
        "deadline-enforced silence should be represented as prolonged_silence"
    );
    assert_eq!(
        deadline_final_event["terminal_signal"]["kind"], "prolonged_silence",
        "prolonged_silence status should derive a prolonged_silence terminal signal"
    );
}

pub fn assert_deadline_provider_exit_code(deadline_output: &std::process::Output) {
    assert_eq!(
        deadline_output.status.code(),
        Some(124),
        "provider exit code should preserve prolonged_silence host parity"
    );
}

pub fn assert_final_opencode_error_launch_output(output: &std::process::Output) {
    assert_output_success(output, "launch final opencode error event");
    let events = launch_events_from_output(output, "launch final opencode error stdout");
    assert_monotonic_launch_events(&events);
    let final_event = final_launch_event(&events);
    assert_final_opencode_error_launch_event(final_event);
}

pub fn assert_final_opencode_error_launch_event(final_event: &Value) {
    assert_eq!(final_event["kind"], "exit");
    assert_eq!(
        final_event["status"],
        json!({ "kind": "exited", "code": 0 })
    );
    assert_eq!(final_event["terminal_signal"]["kind"], "unknown");
    assert!(
        final_event["terminal_signal"]["evidence"]
            .as_str()
            .is_some_and(|evidence| evidence.contains(INCIDENT_ERROR_EVENT_MESSAGE)),
        "terminal_signal.evidence should include structured opencode error message; event={final_event}"
    );
    assert_eq!(
        final_event["terminal_signal"]["observed_at_unix_ms"].as_u64(),
        Some(INCIDENT_ERROR_EVENT_TIMESTAMP)
    );
}

pub fn assert_recovered_opencode_error_launch_output(output: &std::process::Output) {
    assert_output_success(output, "launch recovered opencode error event");
    let events = launch_events_from_output(output, "launch recovered opencode error stdout");
    assert_monotonic_launch_events(&events);
    let final_event = final_launch_event(&events);
    assert_recovered_opencode_error_launch_event(final_event);
}

pub fn assert_recovered_opencode_error_launch_event(final_event: &Value) {
    assert_eq!(final_event["kind"], "exit");
    assert_eq!(
        final_event["status"],
        json!({ "kind": "exited", "code": 0 })
    );
    assert_eq!(final_event["terminal_signal"]["kind"], "clean_exit");
}

pub fn assert_live_launch_output(output: &std::process::Output) {
    assert_stderr_diagnostics_only(output);
    let events = launch_events_from_output(output, "launch stdout");
    assert_monotonic_launch_events(&events);
    let final_event = final_launch_event(&events);
    assert_final_launch_exit_kind(final_event);
    assert_status_derived_terminal_signal(final_event);
    let diagnostics = output_stderr_stdout_diagnostics(output);
    assert_live_provider_exit_code(output, final_event, &diagnostics);
}

pub fn assert_live_provider_exit_code(
    output: &std::process::Output,
    final_event: &Value,
    diagnostics: &str,
) {
    assert_eq!(
        output.status.code(),
        expected_provider_exit_code(final_event),
        "provider process exit should preserve host parity for the final launch status; {diagnostics}"
    );
}

pub fn assert_output_success(output: &std::process::Output, label: &str) {
    let diagnostics = output_stderr_stdout_diagnostics(output);
    assert_output_success_with_diagnostics(output, label, &diagnostics);
}

pub fn assert_output_success_with_diagnostics(
    output: &std::process::Output,
    label: &str,
    diagnostics: &str,
) {
    assert!(
        output.status.success(),
        "{label} exited {:?}; {diagnostics}",
        output.status.code(),
    );
}

pub fn output_stderr_stdout_diagnostics(output: &std::process::Output) -> String {
    format!(
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    )
}

pub fn assert_resume_arg_payload_wrapper_log(wrapper_log_path: &Path) {
    let wrapper_log = wrapper_log_text(wrapper_log_path);
    assert_wrapper_log_arg_value(&wrapper_log, OPENCODE_SESSION_FLAG_FOR_TEST);
    assert_wrapper_log_arg_value(&wrapper_log, resume_session_id());
    assert_wrapper_log_arg_value(&wrapper_log, resume_payload());
}

pub fn assert_resume_stdin_payload_wrapper_log(wrapper_log_path: &Path) {
    let wrapper_log = wrapper_log_text(wrapper_log_path);
    assert_wrapper_log_arg_value(&wrapper_log, OPENCODE_SESSION_FLAG_FOR_TEST);
    assert_wrapper_log_arg_value(&wrapper_log, resume_session_id());
    assert_wrapper_log_stdin_value(&wrapper_log, resume_payload());
}

pub fn assert_wrapper_log_arg_value(wrapper_log: &str, value: &str) {
    let expected = wrapper_arg_log_line(value);
    assert!(wrapper_log.contains(&expected), "{wrapper_log}");
}

pub fn assert_wrapper_log_stdin_value(wrapper_log: &str, value: &str) {
    let expected = wrapper_stdin_log_line(value);
    assert!(wrapper_log.contains(&expected), "{wrapper_log}");
}

pub fn assert_session_before_notification_payload(wrapper_log_path: &Path) {
    let wrapper_log = wrapper_log_text(wrapper_log_path);
    let argv = wrapper_log_args(&wrapper_log);
    assert_argv_session_before_notification_payload(&argv);
}

pub fn assert_argv_session_before_notification_payload(argv: &[&str]) {
    let session_flag = argv_arg_index(argv, OPENCODE_SESSION_FLAG_FOR_TEST);
    let payload = argv_arg_index_containing(argv, NOTIFICATION_PAYLOAD_NEEDLE_FOR_TEST);
    assert!(
        argv_index_before(session_flag, payload),
        "--session must be before notification payload; argv={argv:?}"
    );
}

pub fn assert_submitted_user_turn_marker(events: &[Value]) {
    let marker = expected_submitted_user_turn_marker(events);
    assert_submitted_user_turn_marker_value(marker);
}

pub fn assert_submitted_user_turn_marker_without_message_id(events: &[Value]) {
    let marker = expected_submitted_user_turn_marker(events);
    assert_submitted_user_turn_provider_session(marker);
    assert_submitted_user_turn_prompt_hash(marker);
    assert_submitted_user_turn_source(marker);
    assert_submitted_user_turn_no_message_id(marker);
    assert_submitted_user_turn_delivery_nonce(marker);
}

pub fn assert_submitted_user_turn_marker_value(marker: &Value) {
    assert_submitted_user_turn_provider_session(marker);
    assert_submitted_user_turn_prompt_hash(marker);
    assert_submitted_user_turn_source(marker);
    assert_submitted_user_turn_message_id(marker);
    assert_submitted_user_turn_delivery_nonce(marker);
}

pub fn assert_submitted_user_turn_provider_session(marker: &Value) {
    assert_eq!(
        marker["value"]["provider_session_id"].as_str(),
        Some(resume_session_id())
    );
}

pub fn assert_submitted_user_turn_prompt_hash(marker: &Value) {
    let expected = resume_payload_sha256();
    assert_eq!(
        marker["value"]["prompt_sha256"].as_str(),
        Some(expected.as_str())
    );
}

pub fn assert_submitted_user_turn_source(marker: &Value) {
    assert_eq!(marker["value"]["source"].as_str(), Some("opencode.export"));
}

pub fn assert_submitted_user_turn_message_id(marker: &Value) {
    assert_eq!(marker["value"]["message_id"].as_str(), Some("msg-user"));
}

pub fn assert_submitted_user_turn_no_message_id(marker: &Value) {
    assert!(
        marker["value"].get("message_id").is_none(),
        "submitted user turn marker must omit message_id when export lacks payload; marker={marker:?}"
    );
}

pub fn assert_submitted_user_turn_delivery_nonce(marker: &Value) {
    assert_eq!(
        marker["value"]["delivery_nonce"].as_str(),
        Some("5169694d-de0f-40d1-890c-6e28e55bab27")
    );
}

pub fn assert_empty_resume_payload_rejected(
    output: &std::process::Output,
    wrapper_log_path: &Path,
) {
    assert_empty_resume_payload_status(output);
    assert_empty_resume_payload_did_not_spawn(wrapper_log_path);
    assert_empty_resume_payload_response(&json_stdout(output));
}

pub fn assert_empty_resume_payload_status(output: &std::process::Output) {
    assert_ne!(output.status.code(), Some(0), "{output:?}");
}

pub fn assert_empty_resume_payload_did_not_spawn(wrapper_log_path: &Path) {
    assert!(
        !wrapper_log_path.exists(),
        "empty resume payload must fail before spawning opencode"
    );
}

pub fn assert_empty_resume_payload_response(response: &Value) {
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "empty_resume_payload");
}

pub fn assert_policy_accepts(response: &Value) {
    assert_policy_response_shape(response);
    assert_policy_response_secret_absent(response);
    let result = policy_result(response);
    assert_policy_accepted(result);
    assert_policy_argv(&policy_result_argv(result));
    assert_policy_env(policy_result_env(result));
}

pub fn assert_policy_accepts_for_wrapper(response: &Value, wrapper: &str) {
    assert_policy_response_shape(response);
    assert_policy_response_secret_absent(response);
    let result = policy_result(response);
    assert_policy_accepted(result);
    assert_policy_argv_for_wrapper(&policy_result_argv(result), wrapper);
    assert_policy_env(policy_result_env(result));
}

pub fn assert_policy_accepted(result: &Value) {
    assert_eq!(result["accepted"], true);
}

pub fn assert_policy_response_secret_absent(response: &Value) {
    let response_json = value_json_text(response);
    assert!(
        !text_contains(&response_json, "SENTINEL_DO_NOT_LEAK"),
        "policy response JSON must not leak process OPENAI_API_KEY value: {response_json}"
    );
}

pub fn assert_policy_response_shape(response: &Value) {
    assert_valid(response, "policy.schema.json#/$defs/PolicyEvaluateResponse");
    assert_valid(
        &response["result"],
        "policy.schema.json#/$defs/PolicyEvaluateResult",
    );
}

pub fn assert_policy_argv(argv: &[String]) {
    assert_policy_argv_for_wrapper(argv, "opencode1");
}

pub fn assert_policy_argv_for_wrapper(argv: &[String], wrapper: &str) {
    assert_eq!(argv.first().map(String::as_str), Some(wrapper));
    assert_contains_subsequence(argv, expected_policy_argv_subsequence());
    assert!(
        !argv_contains_plain_opencode(argv),
        "policy should preserve wrapper semantics instead of bypassing opencodeN"
    );
    assert!(
        pure_semantics_preserved(argv),
        "policy must preserve --pure semantics either by retaining --pure or by invoking an opencodeN wrapper; argv={argv:?}"
    );
}

pub fn assert_policy_env(env: &Value) {
    let env = env.as_object().expect("result.env should be present");
    assert!(
        !env.contains_key("OPENAI_API_KEY"),
        "policy result env must not leak OPENAI_API_KEY"
    );
    assert!(
        !env.values().any(|value| value == "SENTINEL_DO_NOT_LEAK"),
        "policy result env must not leak process OPENAI_API_KEY value"
    );
}

pub fn assert_policy_rejects_forbidden(
    response: &Value,
    forbidden_flag: &str,
    forbidden_env_key: &str,
) {
    assert_policy_response_shape(response);
    let result = policy_result(response);
    assert_policy_rejected(
        result,
        "forbidden launch inputs must be rejected by policy.evaluate",
    );
    let diagnostics = policy_diagnostics(result);
    assert_policy_diagnostic(diagnostics, "forbidden_flag", forbidden_flag);
    assert_policy_diagnostic(diagnostics, "forbidden_env", forbidden_env_key);
    assert_forbidden_env_removed(&result["env"], forbidden_env_key);
}

pub fn assert_policy_rejects_forbidden_arg(response: &Value, forbidden_flag: &str) {
    assert_policy_response_shape(response);
    let result = policy_result(response);
    assert_policy_rejected(
        result,
        "forbidden launch flag must be rejected by policy.evaluate",
    );
    let diagnostics = policy_diagnostics(result);
    assert_policy_diagnostic(diagnostics, "forbidden_flag", forbidden_flag);
}

pub fn assert_policy_rejected(result: &Value, message: &str) {
    assert_eq!(result["accepted"], false, "{message}");
}

pub fn policy_diagnostics(result: &Value) -> &[Value] {
    result["diagnostics"].as_array().expect("diagnostics array")
}

pub fn assert_policy_diagnostic(diagnostics: &[Value], code: &str, needle: &str) {
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| policy_diagnostic_matches(diagnostic, code, needle)),
        "policy diagnostics must name {needle} for {code}; diagnostics={diagnostics:?}"
    );
}

pub fn assert_forbidden_env_removed(env: &Value, forbidden_env_key: &str) {
    let env = env.as_object().expect("result.env object");
    assert!(
        !env.contains_key(forbidden_env_key),
        "forbidden env key must be omitted from the effective env"
    );
    assert_eq!(
        env.get("CONTRACT_ALLOWED_ENV").and_then(Value::as_str),
        Some("allowed"),
        "allowed env keys should remain visible in the effective env"
    );
}

pub fn assert_quota_text_does_not_change_terminal_status() {
    let quota_stdout = "usage limit reached; quota exhausted";
    let quota_stderr = "rate limit: try again later";
    assert_terminal_classification(
        json!({ "kind": "exited", "code": 0 }),
        quota_stdout,
        quota_stderr,
        "clean_exit",
    );
    assert_terminal_classification(
        json!({ "kind": "exited", "code": 2 }),
        quota_stdout,
        quota_stderr,
        "nonzero_exit",
    );
}

pub fn assert_valid_launch_event(line_number: usize, event: &Value) {
    assert_valid(event, launch_event_schema_id(line_number, event));
}

pub fn assert_monotonic_launch_events(events: &[Value]) {
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["contract"], CONTRACT);
        assert_eq!(event["request_id"], "req-launch");
        assert!(
            event["time_unix_ms"].as_u64().is_some(),
            "launch event line {} must carry time_unix_ms",
            index + 1
        );
        let seq = event["seq"]
            .as_u64()
            .unwrap_or_else(|| panic!("launch event line {} missing seq", index + 1));
        assert_eq!(
            seq,
            (index + 1) as u64,
            "seq must be strictly monotonic from 1 with no gaps"
        );
    }
}

pub fn assert_base64_round_trip(kind: &str, decoded: &[u8]) {
    assert_eq!(
        decode_base64(&encode_base64(decoded)),
        decoded,
        "{kind} event data_base64 should round-trip to bytes"
    );
}

pub fn assert_process_status_kind(status: &Value) {
    let kind = status["kind"].as_str().expect("status.kind");
    assert!(
        matches!(
            kind,
            "exited"
                | "signal_terminated"
                | "spawn_error"
                | "prolonged_silence"
                | "cancelled"
                | "unknown"
        ),
        "status.kind must be one of the contract ProcessStatus kinds; status={status}"
    );
}

pub fn assert_terminal_classification(
    status: Value,
    stdout: &str,
    stderr: &str,
    expected_kind: &str,
) {
    let output = invoke(
        "terminal.classify",
        terminal_classify_params(status, stdout, stderr),
    );
    assert_terminal_classify_output(&output, expected_kind);
}

pub fn assert_terminal_classify_output(output: &std::process::Output, expected_kind: &str) {
    assert_output_success(output, "terminal.classify");
    let response = json_stdout(output);
    assert_terminal_classify_response_shape(&response);
    assert_terminal_classify_kind(&response, expected_kind);
}

pub fn assert_terminal_classify_response_shape(response: &Value) {
    assert_valid(
        response,
        "terminal.schema.json#/$defs/TerminalClassifyResponse",
    );
    assert_valid(
        &response["result"],
        "terminal.schema.json#/$defs/TerminalClassifyResult",
    );
}

pub fn assert_terminal_classify_kind(response: &Value, expected_kind: &str) {
    assert_eq!(response["result"]["terminal_signal"]["kind"], expected_kind);
}

pub fn assert_contains_subsequence(argv: &[String], expected: &[&str]) {
    assert!(
        expected.len() <= argv.len(),
        "argv too short to contain expected subsequence; argv={argv:?} expected={expected:?}"
    );
    assert!(
        contains_subsequence(argv, expected),
        "argv must contain expected subsequence; argv={argv:?} expected={expected:?}"
    );
}
