//! Declared roles: orchestration

mod cluster_a;
mod support;

use agent_runner_opencode::encoding::sha256_hex;
use cluster_a::*;
use serde_json::json;
use support::{invoke, invoke_with_env, invoke_with_host_and_env, json_stdout};

#[test]
fn characterization_opencode_launch_json_events() {
    let fixture = include_str!("fixtures/opencode_launch_events.jsonl");
    assert_opencode_launch_fixture(fixture);
}

#[test]
fn contract_launch_stream() {
    let fake_wrapper = FakeOpencodeWrapper::new();
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let fixture_session_id = fixture_session_id();

    let output = invoke_with_env(
        "launch",
        launch_params_with_env(
            "low",
            &[
                ("PATH", path.as_str()),
                ("AGENT_RUNNER_OPENCODE_WRAPPER_LOG", log_path),
            ],
        ),
        &[("PATH", path.as_str())],
    );
    assert_contract_launch_stream_output(&output, fake_wrapper.log_path(), fixture_session_id);
}

#[test]
fn contract_launch_stream_accepts_policy_effective_argv() {
    let fake_wrapper = FakeOpencodeWrapper::new();
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let fixture_session_id = fixture_session_id();
    let mut params = launch_params_with_policy_effective_argv("low");
    params["env"] = json!({
        "PATH": path,
        "AGENT_RUNNER_OPENCODE_WRAPPER_LOG": log_path
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_contract_launch_stream_output(&output, fake_wrapper.log_path(), fixture_session_id);
}

#[test]
fn contract_launch_resume_forwards_session_and_arg_payload() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let mut params = resume_launch_params_with_arg_payload();
    params["env"] = json!({
        "PATH": path,
        "AGENT_RUNNER_OPENCODE_WRAPPER_LOG": log_path
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch resume arg payload");
    let wrapper_log = wrapper_log_text(fake_wrapper.log_path());
    assert!(wrapper_log.contains("arg=--session"), "{wrapper_log}");
    assert!(
        wrapper_log.contains(&format!("arg={}", resume_session_id())),
        "{wrapper_log}"
    );
    assert!(
        wrapper_log.contains(&format!("arg={}", resume_payload())),
        "{wrapper_log}"
    );
}

#[test]
fn contract_launch_resume_places_session_before_notification_arg_when_prompt_metadata_differs() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let mut params = resume_launch_params_with_arg_payload();
    params["model"]["inputs"]["prompt"] = json!("metadata prompt differs from argv payload");
    params["env"] = json!({
        "PATH": path,
        "AGENT_RUNNER_OPENCODE_WRAPPER_LOG": log_path
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(
        &output,
        "launch resume arg payload with mismatched prompt metadata",
    );
    let wrapper_log = wrapper_log_text(fake_wrapper.log_path());
    let argv = wrapper_log_args(&wrapper_log);
    let session_flag = argv_arg_index(&argv, OPENCODE_SESSION_FLAG_FOR_TEST);
    let payload = argv_arg_index_containing(&argv, "[OULIPOLY NOTIFICATIONS]");
    assert!(
        session_flag < payload,
        "--session must be before notification payload; argv={argv:?}"
    );
}

const OPENCODE_SESSION_FLAG_FOR_TEST: &str = "--session";

fn wrapper_log_args(wrapper_log: &str) -> Vec<&str> {
    wrapper_log
        .lines()
        .filter_map(|line| line.strip_prefix("arg="))
        .collect()
}

fn argv_arg_index(argv: &[&str], needle: &str) -> usize {
    argv.iter()
        .position(|arg| *arg == needle)
        .unwrap_or_else(|| panic!("argv missing {needle:?}: {argv:?}"))
}

fn argv_arg_index_containing(argv: &[&str], needle: &str) -> usize {
    argv.iter()
        .position(|arg| arg.contains(needle))
        .unwrap_or_else(|| panic!("argv missing arg containing {needle:?}: {argv:?}"))
}

#[test]
fn contract_launch_resume_forwards_session_and_stdin_payload() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let mut params = resume_launch_params_with_stdin_payload();
    params["env"] = json!({
        "PATH": path,
        "AGENT_RUNNER_OPENCODE_WRAPPER_LOG": log_path
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch resume stdin payload");
    let wrapper_log = wrapper_log_text(fake_wrapper.log_path());
    assert!(wrapper_log.contains("arg=--session"), "{wrapper_log}");
    assert!(
        wrapper_log.contains(&format!("arg={}", resume_session_id())),
        "{wrapper_log}"
    );
    assert!(
        wrapper_log.contains(&format!("stdin={}", resume_payload())),
        "{wrapper_log}"
    );
}

#[test]
fn contract_launch_resume_emits_submitted_user_turn_marker_after_export_confirms_payload() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_wrapper_resume_confirming_export_script().to_string(),
    );
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let mut params = resume_launch_params_with_arg_payload();
    params["env"] = json!({
        "PATH": path,
        "AGENT_RUNNER_OPENCODE_WRAPPER_LOG": log_path
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch resume confirmed payload");
    let events = launch_events_from_output(&output, "launch resume confirmed payload stdout");
    assert_monotonic_launch_events(&events);
    let marker = events
        .iter()
        .find(|event| event["kind"] == "marker" && event["name"] == "oulipoly.submitted_user_turn")
        .unwrap_or_else(|| panic!("missing submitted user turn marker; events={events:?}"));
    assert_eq!(
        marker["value"]["provider_session_id"].as_str(),
        Some(resume_session_id())
    );
    assert_eq!(
        marker["value"]["prompt_sha256"].as_str(),
        Some(sha256_hex(resume_payload().as_bytes()).as_str())
    );
    assert_eq!(marker["value"]["source"].as_str(), Some("opencode.export"));
    assert_eq!(marker["value"]["message_id"].as_str(), Some("msg-user"));
    assert_eq!(
        marker["value"]["delivery_nonce"].as_str(),
        Some("5169694d-de0f-40d1-890c-6e28e55bab27")
    );
}

#[test]
fn contract_launch_resume_does_not_emit_submitted_user_turn_marker_when_export_lacks_payload() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_wrapper_resume_unconfirmed_export_script().to_string(),
    );
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let mut params = resume_launch_params_with_arg_payload();
    params["env"] = json!({
        "PATH": path,
        "AGENT_RUNNER_OPENCODE_WRAPPER_LOG": log_path
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch resume unconfirmed payload");
    let events = launch_events_from_output(&output, "launch resume unconfirmed payload stdout");
    assert!(
        !events.iter().any(|event| {
            event["kind"] == "marker" && event["name"] == "oulipoly.submitted_user_turn"
        }),
        "unconfirmed export must not emit submitted user turn marker; events={events:?}"
    );
}

#[test]
fn contract_launch_resume_rejects_empty_payload_without_spawning_child() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let mut params = resume_launch_params_without_payload();
    params["env"] = json!({
        "PATH": path,
        "AGENT_RUNNER_OPENCODE_WRAPPER_LOG": log_path
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_ne!(output.status.code(), Some(0), "{output:?}");
    assert!(
        !fake_wrapper.log_path().exists(),
        "empty resume payload must fail before spawning opencode"
    );
    let response = json_stdout(&output);
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "empty_resume_payload");
}

#[test]
fn contract_launch_env_uses_declared_boundary() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(env_probe_opencode_script());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();

    let output = invoke_with_env(
        "launch",
        launch_params_with_env(
            "low",
            &[
                ("PATH", path.as_str()),
                ("AGENT_RUNNER_OPENCODE_WRAPPER_LOG", log_path),
                ("DECLARED_CHILD_ENV", "declared-child-value"),
                ("XDG_DATA_HOME", "/tmp/declared-opencode-data-home"),
            ],
        ),
        &[
            ("PATH", path.as_str()),
            ("OULIPOLY_DATA_DIR", "/tmp/real-oulipoly-data"),
            ("OULIPOLY_PARENT_INVOCATION", "parent-invocation-token"),
            (
                "AGENT_BASH_AGENT_RUNNER_BIN",
                "/tmp/target-release/oulipoly-agent-runner",
            ),
            ("UNDECLARED_PARENT_ENV", "ambient-secret-do-not-leak"),
            ("OPENAI_API_KEY", "ambient-openai-secret-do-not-leak"),
        ],
    );
    assert_declared_env_boundary(&output, fake_wrapper.log_path());
}

#[test]
fn contract_launch_stream_heartbeat_policy() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(slow_opencode_script(0));
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();

    let output = invoke_with_env(
        "launch",
        launch_params_with_env(
            "low",
            &[
                ("PATH", path.as_str()),
                ("AGENT_RUNNER_OPENCODE_WRAPPER_LOG", log_path),
            ],
        ),
        &[("PATH", path.as_str())],
    );
    assert_heartbeat_launch_output(&output);

    let deadline_wrapper = FakeOpencodeWrapper::with_script(slow_opencode_script(0));
    let deadline_path = prepend_path(deadline_wrapper.dir());
    let deadline_log_path = deadline_wrapper.log_path_str();
    let deadline_unix_ms = short_deadline_unix_ms();
    let deadline_output = invoke_with_host_and_env(
        "launch",
        launch_params_with_env(
            "low",
            &[
                ("PATH", deadline_path.as_str()),
                ("AGENT_RUNNER_OPENCODE_WRAPPER_LOG", deadline_log_path),
            ],
        ),
        deadline_host(deadline_unix_ms),
        &[("PATH", deadline_path.as_str())],
    );
    assert_deadline_launch_output(&deadline_output);
}

#[test]
#[ignore = "live opencode auth/network smoke; run explicitly when external dependencies are available"]
fn integration_launch_live_smoke() {
    let output = invoke("launch", launch_params("low"));
    assert_live_launch_output(&output);
}

#[test]
fn contract_policy_evaluate() {
    let output = invoke_with_env(
        "policy.evaluate",
        policy_evaluate_params(),
        &[("OPENAI_API_KEY", "SENTINEL_DO_NOT_LEAK")],
    );
    assert_output_success(&output, "policy.evaluate");
    let response = json_stdout(&output);
    assert_policy_accepts(&response);
}

#[test]
fn contract_policy_evaluate_accepts_host_candidate_argv() {
    let output = invoke_with_env(
        "policy.evaluate",
        policy_evaluate_params_with_host_candidate_argv(),
        &[],
    );

    assert_output_success(&output, "policy.evaluate host candidate argv");
    let response = json_stdout(&output);
    assert_policy_accepts(&response);
}

#[test]
fn contract_policy_evaluate_accepts_account_one_provider_name_settings_id() {
    let mut params = policy_evaluate_params_with_host_candidate_argv();
    params["settings_id"] = json!("opencode");

    let output = invoke_with_env("policy.evaluate", params, &[]);

    assert_output_success(&output, "policy.evaluate account-one settings id");
    let response = json_stdout(&output);
    assert_policy_accepts(&response);
}

#[test]
fn contract_policy_evaluate_accepts_account_one_plain_host_command() {
    let mut params = policy_evaluate_params_with_host_candidate_command("opencode");
    params["settings_id"] = json!("opencode");

    let output = invoke_with_env("policy.evaluate", params, &[]);

    assert_output_success(&output, "policy.evaluate account-one plain host command");
    let response = json_stdout(&output);
    assert_policy_accepts(&response);
}

#[test]
fn contract_policy_evaluate_rejects_forbidden() {
    let forbidden_env_key = "OPENAI_API_KEY_CONTRACT_FORBIDDEN";
    let forbidden_flag = "--variant";

    let output = invoke_with_env(
        "policy.evaluate",
        forbidden_policy_evaluate_params(forbidden_flag, forbidden_env_key),
        &[],
    );
    assert_output_success(&output, "policy.evaluate rejection");
    let response = json_stdout(&output);
    assert_policy_rejects_forbidden(&response, forbidden_flag, forbidden_env_key);
}

#[test]
fn contract_terminal_classify_status_only() {
    for (status, expected) in terminal_status_cases() {
        assert_terminal_classification(status, "", "", expected);
    }

    assert_quota_text_does_not_change_terminal_status();
}
