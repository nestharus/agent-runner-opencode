//! Declared roles: orchestration

mod cluster_a;
mod support;

use cluster_a::*;
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
    let params = launch_params_with_policy_effective_argv_env("low", path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_contract_launch_stream_output(&output, fake_wrapper.log_path(), fixture_session_id);
}

#[test]
fn contract_launch_resume_forwards_session_and_arg_payload() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch resume arg payload");
    assert_resume_arg_payload_wrapper_log(fake_wrapper.log_path());
}

#[test]
fn contract_launch_resume_places_session_before_notification_arg_when_prompt_metadata_differs() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_prompt_env(
        "metadata prompt differs from argv payload",
        path.as_str(),
        log_path,
    );

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(
        &output,
        "launch resume arg payload with mismatched prompt metadata",
    );
    assert_session_before_notification_payload(fake_wrapper.log_path());
}

#[test]
fn contract_launch_resume_forwards_session_and_stdin_payload() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_stdin_payload_env(path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch resume stdin payload");
    assert_resume_stdin_payload_wrapper_log(fake_wrapper.log_path());
}

#[test]
fn contract_launch_resume_emits_submitted_user_turn_marker_after_export_confirms_payload() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_wrapper_resume_confirming_export_script().to_string(),
    );
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch resume confirmed payload");
    let events = launch_events_from_output(&output, "launch resume confirmed payload stdout");
    assert_monotonic_launch_events(&events);
    assert_submitted_user_turn_marker(&events);
}

#[test]
fn contract_launch_resume_does_not_emit_submitted_user_turn_marker_when_export_lacks_payload() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_wrapper_resume_unconfirmed_export_script().to_string(),
    );
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch resume unconfirmed payload");
    let events = launch_events_from_output(&output, "launch resume unconfirmed payload stdout");
    assert_no_submitted_user_turn_marker(&events);
}

#[test]
fn contract_launch_resume_rejects_empty_payload_without_spawning_child() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_without_payload_env(path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_empty_resume_payload_rejected(&output, fake_wrapper.log_path());
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
fn contract_policy_evaluate_accepts_host_candidate_argv_for_every_account_id() {
    for (settings_id, command) in account_host_command_cases() {
        let output = invoke_with_env(
            "policy.evaluate",
            policy_evaluate_params_for_alias_host_candidate(settings_id, command.as_str()),
            &[],
        );

        assert_output_success(
            &output,
            &format!("policy.evaluate host candidate argv for {settings_id}"),
        );
        let response = json_stdout(&output);
        assert_policy_accepts_for_wrapper(&response, settings_id);
    }
}

#[test]
fn contract_policy_evaluate_accepts_host_candidate_argv_for_account_aliases() {
    for (settings_id, command, expected_wrapper) in [
        ("opencode", "opencode", "opencode1"),
        ("opencode", "/tmp/host-bin/opencode", "opencode1"),
    ] {
        let output = invoke_with_env(
            "policy.evaluate",
            policy_evaluate_params_for_alias_host_candidate(settings_id, command),
            &[],
        );

        assert_output_success(
            &output,
            &format!("policy.evaluate host candidate argv for alias {settings_id}"),
        );
        let response = json_stdout(&output);
        assert_policy_accepts_for_wrapper(&response, expected_wrapper);
    }
}

#[test]
fn contract_policy_evaluate_rejects_user_injected_managed_flag_after_host_prefix() {
    let forbidden_flag = "--variant";
    let output = invoke_with_env(
        "policy.evaluate",
        forbidden_policy_evaluate_params_for_account_host_candidate("opencode2", forbidden_flag),
        &[],
    );

    assert_output_success(&output, "policy.evaluate injected host suffix rejection");
    let response = json_stdout(&output);
    assert_policy_rejects_forbidden_arg(&response, forbidden_flag);
}

fn account_host_command_cases() -> Vec<(&'static str, String)> {
    [
        "opencode1",
        "opencode2",
        "opencode3",
        "opencode4",
        "opencode5",
    ]
    .into_iter()
    .flat_map(|settings_id| {
        [
            settings_id.to_string(),
            format!("/tmp/host-bin/{settings_id}"),
            "opencode".to_string(),
            "/tmp/host-bin/opencode".to_string(),
            "opencode5".to_string(),
            "/tmp/host-bin/opencode5".to_string(),
        ]
        .into_iter()
        .map(move |command| (settings_id, command))
    })
    .collect()
}

#[test]
fn contract_policy_evaluate_accepts_account_one_provider_name_settings_id() {
    let output = invoke_with_env(
        "policy.evaluate",
        policy_evaluate_account_one_provider_name_settings_id_params(),
        &[],
    );

    assert_output_success(&output, "policy.evaluate account-one settings id");
    let response = json_stdout(&output);
    assert_policy_accepts(&response);
}

#[test]
fn contract_policy_evaluate_accepts_account_one_plain_host_command() {
    let output = invoke_with_env(
        "policy.evaluate",
        policy_evaluate_account_one_plain_host_command_params(),
        &[],
    );

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
