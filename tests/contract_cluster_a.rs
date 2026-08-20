//! Declared roles: orchestration

mod cluster_a;
mod support;

use cluster_a::*;
use support::{invoke_validated, invoke_with_env, invoke_with_host_and_env, json_stdout};

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
fn contract_launch_splits_oversized_prompt_at_ascii_spaces() {
    let prompt = oversized_prompt();
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_prompt_env(&prompt, path.as_str(), fake_wrapper.log_path_str());

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch oversized prompt");
    assert_oversized_prompt_segments(fake_wrapper.log_path(), &prompt);
}

#[test]
fn contract_launch_transports_oversized_argv_without_prompt_metadata() {
    let prompt = oversized_prompt();
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_argv_and_prompt_env(
        vec![prompt.clone()],
        None,
        path.as_str(),
        fake_wrapper.log_path_str(),
    );

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch oversized argv without prompt metadata");
    assert_oversized_prompt_segments(fake_wrapper.log_path(), &prompt);
}

#[test]
fn contract_launch_transports_oversized_argv_with_mismatched_prompt_metadata() {
    let prompt = oversized_prompt();
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_argv_and_prompt_env(
        vec!["--share".to_string(), "--".to_string(), prompt.clone()],
        Some("short metadata that differs from the positional payload"),
        path.as_str(),
        fake_wrapper.log_path_str(),
    );

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch oversized argv with mismatched metadata");
    assert_oversized_prompt_segments(fake_wrapper.log_path(), &prompt);
    let argv = wrapper_nul_log_args(fake_wrapper.log_path());
    let share = argv_arg_index_owned(&argv, "--share");
    let boundary = argv_arg_index_owned(&argv, "--");
    assert!(
        share < boundary,
        "existing options must remain before --: {argv:?}"
    );
    assert_eq!(
        argv.iter().filter(|arg| arg.as_str() == "--").count(),
        1,
        "an existing message boundary must be reused"
    );
}

#[test]
fn contract_launch_transforms_duplicate_oversized_positional_values() {
    let prompt = oversized_prompt();
    let expected_message = format!("{prompt} {prompt}");
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_argv_and_prompt_env(
        vec![prompt.clone(), prompt],
        Some("different metadata"),
        path.as_str(),
        fake_wrapper.log_path_str(),
    );

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch duplicate oversized positional values");
    assert_oversized_prompt_segments(fake_wrapper.log_path(), &expected_message);
}

#[test]
fn contract_launch_preserves_short_prompt_argv() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_prompt_env(
        "reply with the single word: ok",
        path.as_str(),
        fake_wrapper.log_path_str(),
    );

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch short prompt");
    assert_short_prompt_argv_unchanged(fake_wrapper.log_path());
}

#[test]
fn contract_launch_luna_max_reaches_exact_native_route() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_luna_max_params_with_env(path.as_str(), fake_wrapper.log_path_str());
    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);
    assert_output_success(&output, "launch Luna max");
    let argv = wrapper_nul_log_args(fake_wrapper.log_path());
    assert_contains_subsequence(&argv, &["-m", "openai/gpt-5.6-luna", "--variant", "max"]);
    let events = launch_events_from_output(&output, "Luna launch stdout");
    assert!(events.iter().any(|event| {
        event["kind"] == "marker"
            && event["name"] == "oulipoly.launch_route"
            && event["value"].to_string().contains("openai/gpt-5.6-luna")
            && event["value"].to_string().contains("max")
    }));
}

#[test]
fn contract_launch_luna_low_reaches_exact_native_route() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_luna_low_params_with_env(path.as_str(), fake_wrapper.log_path_str());
    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);
    assert_output_success(&output, "launch Luna low");
    let argv = wrapper_nul_log_args(fake_wrapper.log_path());
    assert_contains_subsequence(&argv, &["-m", "openai/gpt-5.6-luna", "--variant", "low"]);
}

#[test]
fn contract_launch_rejects_caller_selected_create_session_before_spawn() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_create_session_params_with_env(path.as_str(), fake_wrapper.log_path_str());
    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);
    assert_ne!(output.status.code(), Some(0));
    assert!(!fake_wrapper.log_path().exists());
    let response = json_stdout(&output);
    assert_eq!(
        response["error"]["code"],
        "launch_session_create_unsupported"
    );
}

#[test]
fn contract_launch_malformed_native_event_prevents_clean_terminal_claim() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_opencode_script_with_output_and_status("not-json\n", "", 0),
    );
    let path = prepend_path(fake_wrapper.dir());
    let output = invoke_with_env(
        "launch",
        launch_params_with_env(
            "low",
            &[
                ("PATH", path.as_str()),
                (
                    "AGENT_RUNNER_OPENCODE_WRAPPER_LOG",
                    fake_wrapper.log_path_str(),
                ),
            ],
        ),
        &[("PATH", path.as_str())],
    );
    assert_ne!(output.status.code(), Some(0));
    let events = launch_events_from_output(&output, "malformed native event launch stdout");
    let final_event = final_launch_event(&events);
    assert_eq!(final_event["status"]["kind"], "unknown");
    assert!(events.iter().any(|event| {
        event["kind"] == "marker"
            && event["name"] == "oulipoly.launch_evidence_loss"
            && event["value"]
                .to_string()
                .contains("native event parse failed")
    }));
}

#[test]
fn contract_launch_rejects_oversized_unbroken_prompt_before_spawn() {
    let prompt = oversized_unbroken_prompt();
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_prompt_env(&prompt, path.as_str(), fake_wrapper.log_path_str());

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_oversized_prompt_rejected(&output, fake_wrapper.log_path());
}

#[test]
fn contract_launch_final_opencode_error_event_exit_zero_reports_unknown_signal() {
    let stdout = incident_error_event_stdout();
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_opencode_script_with_output_and_status(&stdout, "", 0),
    );
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

    assert_final_opencode_error_launch_output(&output);
}

#[test]
fn contract_launch_error_event_followed_by_later_opencode_event_exit_zero_stays_clean() {
    let stdout = recovered_after_incident_error_event_stdout();
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_opencode_script_with_output_and_status(&stdout, "", 0),
    );
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

    assert_recovered_opencode_error_launch_output(&output);
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
fn contract_launch_resume_splits_prompt_and_preserves_confirmation() {
    let prompt = oversized_prompt();
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_wrapper_nul_log_resume_confirming_export_script(&prompt),
    );
    let path = prepend_path(fake_wrapper.dir());
    let params =
        resume_launch_params_with_prompt_env(&prompt, path.as_str(), fake_wrapper.log_path_str());

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_oversized_resume_prompt(&output, fake_wrapper.log_path(), &prompt);
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
fn contract_launch_resume_returns_non_clean_when_submitted_turn_has_no_completed_response() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_wrapper_resume_confirming_export_script().to_string(),
    );
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    let events = launch_events_from_output(&output, "launch resume confirmed payload stdout");
    assert_monotonic_launch_events(&events);
    assert_submitted_user_turn_marker(&events);
    assert_no_produced_assistant_response_marker(&events);
    assert_unresolved_resume_completion(&output, &events);
}

#[test]
fn contract_launch_resume_omits_submitted_user_turn_marker_when_export_lacks_payload() {
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
fn contract_launch_completed_resume_does_not_wait_for_lingering_native_process() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_completed_resume_then_hang_script());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);
    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);
    let events = launch_events_from_output(&output, "completed lingering resume stdout");
    assert_produced_assistant_response_marker(&events);
    let final_event = final_launch_event(&events);
    assert_eq!(final_event["status"]["kind"], "signal_terminated");
    assert_live_provider_exit_code(&output, final_event, "completed lingering resume");
}

#[test]
fn contract_launch_completed_resume_preserves_completion_across_non_terminal_parser_tail() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_wrapper_completed_resume_with_non_terminal_tail_script().to_string(),
    );
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_output_success(&output, "launch completed resume with parser tail");
    let events = launch_events_from_output(&output, "completed resume parser tail stdout");
    assert_produced_assistant_response_marker(&events);
}

#[test]
fn contract_launch_completed_export_does_not_wait_for_buffered_native_events() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_completed_export_then_hang_script());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);
    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);
    let events = launch_events_from_output(&output, "completed buffered resume stdout");
    assert_produced_assistant_response_marker(&events);
    let final_event = final_launch_event(&events);
    assert_eq!(final_event["status"]["kind"], "signal_terminated");
    assert_live_provider_exit_code(&output, final_event, "completed buffered resume");
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
    let path = std::env::var("PATH").expect("live PATH");
    let home = std::env::var("HOME").expect("live HOME");
    let output = invoke_with_env(
        "launch",
        live_luna_low_launch_params(&path, &home),
        &[("PATH", path.as_str()), ("HOME", home.as_str())],
    );
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
fn contract_policy_evaluate_accepts_luna_max() {
    let output = invoke_validated(
        "policy.evaluate",
        policy_evaluate_luna_max_params(),
        "policy.schema.json#/$defs/PolicyEvaluateRequest",
    );
    assert_output_success(&output, "policy.evaluate Luna max");
    assert_policy_accepts_model(&json_stdout(&output), "openai/gpt-5.6-luna", "max");
}

#[test]
fn contract_policy_evaluate_accepts_luna_low() {
    let output = invoke_validated(
        "policy.evaluate",
        policy_evaluate_luna_low_params(),
        "policy.schema.json#/$defs/PolicyEvaluateRequest",
    );
    assert_output_success(&output, "policy.evaluate Luna low");
    assert_policy_accepts_model(&json_stdout(&output), "openai/gpt-5.6-luna", "low");
}

#[test]
fn contract_policy_evaluate_accepts_luna_for_every_declared_account() {
    for account in [
        "opencode1",
        "opencode2",
        "opencode3",
        "opencode4",
        "opencode5",
    ] {
        let output = invoke_validated(
            "policy.evaluate",
            policy_evaluate_params_for_account_model(
                account,
                "gpt-luna-low",
                "openai/gpt-5.6-luna",
                "low",
            ),
            "policy.schema.json#/$defs/PolicyEvaluateRequest",
        );
        assert_output_success(&output, "policy.evaluate account-eligible Luna low");
        let response = json_stdout(&output);
        assert_eq!(response["result"]["accepted"], true, "{response}");
        assert!(
            response["result"]["diagnostics"]
                .as_array()
                .is_some_and(Vec::is_empty),
            "{response}"
        );
        let argv = response["result"]["argv"]
            .as_array()
            .expect("policy argv")
            .iter()
            .map(|arg| arg.as_str().expect("policy argv text").to_string())
            .collect::<Vec<_>>();
        assert_eq!(argv.first().map(String::as_str), Some(account));
        assert_contains_subsequence(&argv, &["-m", "openai/gpt-5.6-luna", "--variant", "low"]);
        assert!(response["result"]["markers"]
            .as_array()
            .expect("policy markers")
            .iter()
            .any(|marker| { marker["name"] == "opencode.account" && marker["value"] == account }));
        assert!(response["result"]["markers"]
            .as_array()
            .expect("policy markers")
            .iter()
            .any(|marker| {
                marker["name"] == "opencode.settings_record_identity"
                    && marker["value"] == format!("settings record {account} at version fixture-v1")
            }));
    }
}

#[test]
fn contract_policy_evaluate_rejects_model_identity_mismatch() {
    let output = invoke_validated(
        "policy.evaluate",
        policy_evaluate_model_mismatch_params(),
        "policy.schema.json#/$defs/PolicyEvaluateRequest",
    );
    assert_output_success(&output, "policy.evaluate model mismatch");
    assert_policy_rejects_invalid_model(&json_stdout(&output));
}

#[test]
fn contract_policy_evaluate_rejects_extra_model_provider_arg() {
    let output = invoke_validated(
        "policy.evaluate",
        policy_evaluate_extra_model_arg_params(),
        "policy.schema.json#/$defs/PolicyEvaluateRequest",
    );
    assert_output_success(&output, "policy.evaluate extra model arg");
    assert_policy_rejects_invalid_model(&json_stdout(&output));
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
        assert_policy_accepts_for_wrapper(&response, command.as_str());
    }
}

#[test]
fn contract_policy_evaluate_accepts_account_one_wrapper_command_aliases() {
    for (settings_id, command) in [
        ("opencode1", "opencode"),
        ("opencode1", "/tmp/host-bin/opencode"),
    ] {
        let output = invoke_with_env(
            "policy.evaluate",
            policy_evaluate_params_for_alias_host_candidate(settings_id, command),
            &[],
        );

        assert_output_success(
            &output,
            &format!("policy.evaluate wrapper command alias for {settings_id}"),
        );
        let response = json_stdout(&output);
        assert_policy_accepts_for_wrapper(&response, command);
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
    account_host_settings_ids()
        .into_iter()
        .flat_map(account_host_command_cases_for)
        .collect()
}

fn account_host_settings_ids() -> [&'static str; 5] {
    [
        "opencode1",
        "opencode2",
        "opencode3",
        "opencode4",
        "opencode5",
    ]
}

fn account_host_command_cases_for(settings_id: &'static str) -> Vec<(&'static str, String)> {
    account_host_commands(settings_id)
        .into_iter()
        .map(move |command| account_host_command_case(settings_id, command))
        .collect()
}

fn account_host_commands(settings_id: &str) -> [String; 2] {
    [settings_id.to_string(), host_bin_command(settings_id)]
}

fn account_host_command_case(settings_id: &'static str, command: String) -> (&'static str, String) {
    (settings_id, command)
}

fn host_bin_command(settings_id: &str) -> String {
    format!("/tmp/host-bin/{settings_id}")
}

#[test]
fn contract_policy_evaluate_rejects_command_from_another_account() {
    let output = invoke_with_env(
        "policy.evaluate",
        policy_evaluate_params_for_alias_host_candidate("opencode1", "opencode5"),
        &[],
    );

    assert_output_success(&output, "policy.evaluate cross-account command");
    let response = json_stdout(&output);
    assert_policy_rejected_with_code(&response, "settings_command_mismatch");
}

#[test]
fn contract_policy_evaluate_accepts_account_one_persisted_settings_id() {
    let output = invoke_with_env(
        "policy.evaluate",
        policy_evaluate_account_one_persisted_settings_id_params(),
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
    assert_policy_accepts_for_wrapper(&response, "opencode");
}

#[test]
fn contract_policy_evaluate_rejects_forbidden_arg_without_rewriting_env() {
    let configured_env_key = "OPENAI_API_KEY_CONFIGURED";
    let forbidden_flag = "--variant";

    let output = invoke_with_env(
        "policy.evaluate",
        forbidden_policy_evaluate_params(forbidden_flag, configured_env_key),
        &[],
    );
    assert_output_success(&output, "policy.evaluate rejection");
    let response = json_stdout(&output);
    assert_policy_rejects_forbidden(&response, forbidden_flag, configured_env_key);
}

#[test]
fn contract_policy_evaluate_preserves_host_configured_environment() {
    let configured_env_key = "OPENAI_API_KEY_CONFIGURED";
    let output = invoke_with_env(
        "policy.evaluate",
        policy_evaluate_params_with_env(
            "opencode2",
            &[
                ("XDG_DATA_HOME", "/tmp/configured-opencode-data-home"),
                (configured_env_key, "configured-provider-value"),
                ("CONTRACT_ALLOWED_ENV", "allowed"),
            ],
        ),
        &[],
    );

    assert_output_success(&output, "policy.evaluate account env transform");
    assert_policy_preserves_configured_env(&json_stdout(&output), configured_env_key);
}

#[test]
fn contract_terminal_classify_status_only() {
    for (status, expected) in terminal_status_cases() {
        assert_terminal_classification(status, "", "", expected);
    }

    assert_quota_text_does_not_change_terminal_status();
}
