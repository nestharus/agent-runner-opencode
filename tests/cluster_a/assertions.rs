// declared_role: validator, accessor, predicate, orchestration
#![allow(unused_imports)]

use super::*;
use agent_runner_opencode::{encoding::sha256_hex, launch::OPENCODE_PROMPT_ARG_BYTE_CEILING};

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
    assert_monotonic_launch_events(&events);
    assert_eq!(
        collect_stream_bytes(&events, "stdout"),
        FAKE_LAUNCH_STDOUT,
        "stdout events must byte-preserve the selected opencodeN wrapper output"
    );
    assert_eq!(
        collect_stream_bytes(&events, "stderr"),
        FAKE_LAUNCH_STDERR,
        "stderr events must byte-preserve the selected opencodeN wrapper output"
    );
    assert_eq!(
        expected_session_marker(&events, fixture_session_id)["value"],
        true,
        "session marker should use a truthy marker value"
    );
    assert_provider_session_marker(&events, fixture_session_id);
    let final_event = final_launch_event(&events);
    assert_eq!(
        final_event["kind"], "exit",
        "final launch line must be exit"
    );
    assert!(
        final_event.get("status").is_some(),
        "exit event must carry status"
    );
    assert!(
        final_event.get("terminal_signal").is_some(),
        "exit event must carry terminal_signal"
    );
    assert_process_status_kind(&final_event["status"]);
    assert_eq!(
        final_event["status"],
        json!({ "kind": "exited", "code": 7 }),
        "final status should truthfully report the controlled wrapper exit status"
    );
    assert_status_derived_terminal_signal(final_event);
    assert!(
        final_event.get("session").is_some(),
        "exit event must carry captured session evidence"
    );
    assert!(
        json_contains_string(&final_event["session"], fixture_session_id),
        "exit.session must carry the same opencode sessionID evidence as the marker; session={}",
        final_event["session"]
    );
    assert_output_status_code(
        output,
        Some(7),
        "provider process should preserve nonzero child exit-code parity",
    );
    let wrapper_log = wrapper_log_text(wrapper_log_path);
    assert!(
        wrapper_log
            .lines()
            .any(|line| { line == "argv0=opencode1" || line.ends_with("/opencode1") }),
        "launch should cross the selected opencode1 wrapper boundary; log={wrapper_log:?}"
    );
    assert!(
        wrapper_log.lines().any(|line| line == "arg=run"),
        "wrapper should receive opencode run argv; log={wrapper_log:?}"
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

pub fn assert_provider_session_marker(events: &[Value], fixture_session_id: &str) {
    let marker = events
        .iter()
        .find(|event| event["kind"] == "marker" && event["name"] == "oulipoly.provider_session")
        .unwrap_or_else(|| panic!("missing fixed provider session marker; events={events:?}"));
    assert_eq!(
        marker["value"]["provider_session_id"].as_str(),
        Some(fixture_session_id)
    );
}

pub fn assert_status_derived_terminal_signal(final_event: &Value) {
    assert_eq!(
        final_event["terminal_signal"]["kind"],
        expected_signal_kind_for_status(&final_event["status"]),
        "terminal_signal should be status-derived"
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
    assert_output_success_with_diagnostics(output, "live launch", &diagnostics);
    assert_eq!(
        final_event["status"],
        json!({ "kind": "exited", "code": 0 })
    );
    assert_eq!(final_event["terminal_signal"]["kind"], "clean_exit");
    assert!(events.iter().any(|event| {
        event["kind"] == "marker"
            && event["name"] == "oulipoly.launch_route"
            && event["value"].to_string().contains("openai/gpt-5.6-luna")
            && event["value"].to_string().contains("low")
    }));
    assert!(events.iter().any(|event| {
        event["kind"] == "marker" && event["name"] == "oulipoly.provider_session"
    }));
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

pub fn assert_oversized_prompt_segments(wrapper_log_path: &Path, prompt: &str) {
    assert!(
        prompt.len() > 128 * 1024,
        "fixture must exceed Linux's per-string argv cap"
    );
    let argv = wrapper_nul_log_args(wrapper_log_path);
    assert!(
        argv.iter()
            .all(|arg| arg.len() < OPENCODE_PROMPT_ARG_BYTE_CEILING),
        "every final child argv element must remain below the ceiling"
    );
    let boundary = argv_arg_index_owned(&argv, "--");
    let segments = &argv[boundary + 1..];
    assert!(segments.len() > 1, "oversized prompt must be tokenized");
    assert!(
        segments.iter().all(|segment| !segment.contains(' ')),
        "generated message tokens must not contain ASCII spaces"
    );
    for option_text in ["--share", "--attach", "--session", "-m"] {
        assert!(
            segments.iter().any(|segment| segment == option_text),
            "{option_text} must remain positional message text"
        );
    }
    assert_eq!(opencode_1_18_9_message(segments), prompt);
}

pub fn assert_short_prompt_argv_unchanged(wrapper_log_path: &Path) {
    let argv = wrapper_nul_log_args(wrapper_log_path);
    let expected = policy_effective_argv("low")[1..]
        .iter()
        .map(|arg| (*arg).to_string())
        .collect::<Vec<_>>();
    assert_eq!(argv, expected);
}

pub fn assert_oversized_resume_prompt(
    output: &std::process::Output,
    wrapper_log_path: &Path,
    prompt: &str,
) {
    assert_output_success(output, "launch oversized resume prompt");
    let argv = wrapper_nul_log_args(wrapper_log_path);
    let session = argv_arg_index_owned(&argv, OPENCODE_SESSION_FLAG_FOR_TEST);
    let boundary = argv_arg_index_owned(&argv, "--");
    assert_eq!(
        argv.get(session + 1).map(String::as_str),
        Some(resume_session_id())
    );
    assert!(
        session < boundary,
        "--session <id> must precede --: {argv:?}"
    );
    assert_oversized_prompt_segments(wrapper_log_path, prompt);
    let events = launch_events_from_output(output, "launch oversized resume prompt stdout");
    let marker = expected_submitted_user_turn_marker(&events);
    assert_eq!(
        marker["value"]["prompt_sha256"],
        sha256_hex(prompt.as_bytes())
    );
    assert_eq!(marker["value"]["message_id"], "msg-user");
}

pub fn assert_oversized_prompt_rejected(output: &std::process::Output, wrapper_log_path: &Path) {
    assert_ne!(output.status.code(), Some(0), "{output:?}");
    assert!(
        !wrapper_log_path.exists(),
        "oversized unbroken prompt must fail before spawning opencode"
    );
    let response = json_stdout(output);
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "oversized_prompt_token");
    assert!(response["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("ASCII space")));
}

pub fn opencode_1_18_9_message(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if arg.contains(' ') {
                format!("\"{}\"", arg.replace('"', "\\\""))
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn argv_arg_index_owned(argv: &[String], needle: &str) -> usize {
    argv.iter()
        .position(|arg| arg == needle)
        .unwrap_or_else(|| panic!("argv missing {needle:?}: {argv:?}"))
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
        session_flag < payload,
        "--session must be before notification payload; argv={argv:?}"
    );
}

pub fn assert_submitted_user_turn_marker(events: &[Value]) {
    let marker = expected_submitted_user_turn_marker(events);
    assert_submitted_user_turn_marker_value(marker);
}

pub fn assert_no_submitted_user_turn_marker(events: &[Value]) {
    assert!(
        submitted_user_turn_marker(events).is_none(),
        "unconfirmed resume must not emit a submitted user turn marker; events={events:?}"
    );
}

pub fn assert_produced_assistant_response_marker(events: &[Value]) {
    let markers = events
        .iter()
        .filter(|event| {
            event["kind"] == "marker"
                && event["name"] == PRODUCED_ASSISTANT_RESPONSE_MARKER_FOR_TEST
        })
        .collect::<Vec<_>>();
    assert_eq!(
        markers.len(),
        1,
        "completed resume must emit exactly one assistant response marker; events={events:?}"
    );
    assert_eq!(markers[0]["value"], true);
}

pub fn assert_no_produced_assistant_response_marker(events: &[Value]) {
    assert!(
        events.iter().all(|event| {
            event["kind"] != "marker"
                || event["name"] != PRODUCED_ASSISTANT_RESPONSE_MARKER_FOR_TEST
        }),
        "resume without completed assistant response must not emit a productivity marker; events={events:?}"
    );
}

pub fn assert_unresolved_resume_completion(output: &std::process::Output, events: &[Value]) {
    assert_output_status_code(
        output,
        Some(1),
        "a submitted resume without a completed response must be non-clean",
    );
    let markers = events
        .iter()
        .filter(|event| {
            event["kind"] == "marker" && event["name"] == "oulipoly.resume_completion_unresolved"
        })
        .collect::<Vec<_>>();
    assert_eq!(
        markers.len(),
        1,
        "unresolved resume completion must have one explicit handoff marker; events={events:?}"
    );
    assert_eq!(
        markers[0]["value"]["state"],
        "submitted_user_turn_without_completed_assistant_response"
    );
    assert_eq!(
        markers[0]["value"]["provider_session_id"],
        resume_session_id()
    );
    assert!(markers[0]["value"]["required_action"]
        .as_str()
        .is_some_and(|action| action.contains("reconcile")));
    let final_event = final_launch_event(events);
    assert_eq!(final_event["status"]["kind"], "unknown");
    assert_eq!(final_event["terminal_signal"]["kind"], "unknown");
    assert!(final_event["terminal_signal"]["evidence"]
        .as_str()
        .is_some_and(|evidence| evidence.contains("completion remains unconfirmed")));
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

pub fn assert_policy_accepts_model(response: &Value, provider_model: &str, effort: &str) {
    assert_policy_response_shape(response);
    assert_policy_response_secret_absent(response);
    let result = policy_result(response);
    assert_policy_accepted(result);
    let argv = policy_result_argv(result);
    assert_eq!(argv.first().map(String::as_str), Some("opencode1"));
    assert!(
        pure_semantics_preserved(&argv),
        "policy must preserve wrapper-owned pure semantics; argv={argv:?}"
    );
    assert_contains_subsequence(
        &argv,
        &[
            "run",
            "--format",
            "json",
            "--dangerously-skip-permissions",
            "-m",
            provider_model,
            "--variant",
            effort,
        ],
    );
    assert_policy_env(policy_result_env(result));
}

pub fn assert_policy_rejects_invalid_model(response: &Value) {
    assert_policy_response_shape(response);
    let result = policy_result(response);
    assert_policy_rejected(result, "inconsistent model identity must be rejected");
    assert_policy_diagnostic(policy_diagnostics(result), "invalid_model", "provider args");
}

pub fn assert_policy_rejected_with_code(response: &Value, code: &str) {
    assert_policy_response_shape(response);
    let result = policy_result(response);
    assert_policy_rejected(result, "policy request must be rejected");
    assert_policy_diagnostic(policy_diagnostics(result), code, "account");
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
        pure_semantics_preserved(argv),
        "policy must preserve --pure semantics from the configured command; argv={argv:?}"
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
    configured_env_key: &str,
) {
    assert_policy_response_shape(response);
    let result = policy_result(response);
    assert_policy_rejected(
        result,
        "forbidden launch inputs must be rejected by policy.evaluate",
    );
    let diagnostics = policy_diagnostics(result);
    assert_policy_diagnostic(diagnostics, "forbidden_flag", forbidden_flag);
    assert_eq!(
        result["env"][configured_env_key], "SENTINEL_POLICY_CONFIGURED_ENV",
        "provider policy must not rewrite host-configured environment values"
    );
    assert_eq!(result["env"]["CONTRACT_ALLOWED_ENV"], "allowed");
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

pub fn assert_policy_preserves_configured_env(response: &Value, configured_env_key: &str) {
    assert_policy_response_shape(response);
    let result = policy_result(response);
    assert_policy_accepted(result);
    assert!(policy_diagnostics(result).is_empty());
    let env = result["env"].as_object().expect("result.env object");
    assert_eq!(
        env.get("XDG_DATA_HOME").and_then(Value::as_str),
        Some("/tmp/configured-opencode-data-home")
    );
    assert_eq!(
        env.get("CONTRACT_ALLOWED_ENV").and_then(Value::as_str),
        Some("allowed")
    );
    assert_eq!(
        env.get(configured_env_key).and_then(Value::as_str),
        Some("configured-provider-value"),
        "provider policy must preserve host-configured values without key-specific logic"
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
    let request_id = events
        .first()
        .and_then(|event| event["request_id"].as_str())
        .expect("launch stream must contain a request_id");
    assert!(
        request_id.starts_with("req-launch-"),
        "launch request_id should use the test sequence: {request_id}"
    );
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["contract"], CONTRACT);
        assert_eq!(event["request_id"], request_id);
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
