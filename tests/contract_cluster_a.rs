//! Declared roles: orchestration

mod cluster_a;
mod support;

use cluster_a::*;
use serde_json::{json, Value};
use std::fs;
#[cfg(unix)]
use std::{
    os::fd::AsRawFd,
    os::unix::{net::UnixStream, process::CommandExt},
};
use support::{invoke_validated, invoke_with_env, invoke_with_host_and_env, json_stdout};

#[cfg(unix)]
extern "C" {
    fn setpgid(pid: i32, pgid: i32) -> i32;
}

struct FailAfterFirstLaunchEvent {
    completed_events: usize,
}

struct RejectLaunchWrites;

struct IsolatedLaunchSettings {
    _root: tempfile::TempDir,
    host_overrides: serde_json::Value,
}

impl IsolatedLaunchSettings {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create isolated launch host root");
        let config_root = root.path().join("config");
        let data_root = root.path().join("data");
        let working_directory = root.path().join("workspace");
        let store_root = config_root.join("agent-runner-opencode");
        fs::create_dir_all(&store_root).expect("create isolated settings root");
        fs::create_dir_all(&data_root).expect("create isolated launch data root");
        fs::create_dir_all(&working_directory).expect("create isolated host workspace");
        let store = serde_json::json!({
            "schema_version": 3,
            "records": [{
                "id": "opencode1",
                "display_name": "isolated launch account",
                "version": "fixture-v1",
                "values": {
                    "provider": "opencode",
                    "profile": "opencode1",
                    "wrapper": "opencode1",
                    "model": { "selection": "requested" },
                    "quota": {
                        "source": "opencode_auth",
                        "auth_path": "~/.local/share/opencode/auth.json",
                        "probe": "native_chatgpt_usage"
                    },
                    "launch": {
                        "dangerously_skip_permissions": true,
                        "format": "json",
                        "preserve_pure_wrapper": true
                    },
                    "extra_env": {},
                    "mode": "non_interactive"
                }
            }],
            "history": [],
            "mutation_receipts": {}
        });
        fs::write(
            store_root.join("settings-store.json"),
            serde_json::to_vec_pretty(&store).expect("serialize isolated settings store"),
        )
        .expect("write isolated settings store");
        let host_overrides = serde_json::json!({
            "config_root": config_root.to_string_lossy(),
            "data_root": data_root.to_string_lossy(),
            "working_directory": working_directory.to_string_lossy(),
        });
        Self {
            _root: root,
            host_overrides,
        }
    }

    fn host_overrides(&self) -> serde_json::Value {
        self.host_overrides.clone()
    }

    fn delete_settings_record(&self) {
        let output = support::invoke_validated_with_host(
            "settings.delete",
            serde_json::json!({ "id": "opencode1", "version": "fixture-v1" }),
            self.host_overrides(),
            "settings.schema.json#/$defs/SettingsDeleteRequest",
        );
        let response = json_stdout(&output);
        support::assert_valid(
            &response,
            "settings.schema.json#/$defs/SettingsDeleteResponse",
        );
        assert_eq!(
            response["ok"], true,
            "settings deletion response={response}"
        );
    }
}

impl std::io::Write for RejectLaunchWrites {
    fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "simulated route handoff failure",
        ))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl std::io::Write for FailAfterFirstLaunchEvent {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.completed_events >= 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "simulated launch event handoff failure",
            ));
        }
        self.completed_events += buffer.iter().filter(|byte| **byte == b'\n').count();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
#[cfg(unix)]
fn contract_launch_exec_gate_prevents_unpublished_native_effect() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_script().to_string());
    let (gate_writer, inherited_gate) = UnixStream::pair().expect("create launch exec gate");
    let inherited_gate_fd = inherited_gate.as_raw_fd();
    let inherited_gate_guard = inherited_gate
        .try_clone()
        .expect("clone inherited launch gate");
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-opencode"));
    command
        .arg("__launch_exec_gate")
        .arg(fake_wrapper_path(fake_wrapper.dir()))
        .arg("run")
        .env_clear()
        .env(
            "AGENT_RUNNER_OPENCODE_LAUNCH_GATE_FD",
            inherited_gate_fd.to_string(),
        )
        .env("AGENT_RUNNER_OPENCODE_WRAPPER_LOG", fake_wrapper.log_path())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    unsafe {
        command.pre_exec(move || {
            let _keep_gate_open = &inherited_gate_guard;
            let flags = libc::fcntl(inherited_gate_fd, libc::F_GETFD);
            if flags == -1
                || libc::fcntl(inherited_gate_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) == -1
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().expect("spawn launch exec gate");
    drop(inherited_gate);
    drop(gate_writer);

    let status = child.wait().expect("wait for closed launch exec gate");
    assert_eq!(status.code(), Some(126));
    assert!(
        !fake_wrapper.log_path().exists(),
        "closing an unpublished actor gate must prevent the native command effect"
    );
}

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
fn contract_launch_replay_reconciles_durable_generated_session_after_output_loss() {
    let runtime = IsolatedLaunchSettings::new();
    let provider_session_id = "ses_durable_launch_response_loss";
    let fake_wrapper = FakeOpencodeWrapper::with_counted_new_session(provider_session_id);
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_env(
        "low",
        &[
            ("PATH", path.as_str()),
            (
                "AGENT_RUNNER_OPENCODE_WRAPPER_LOG",
                fake_wrapper.log_path_str(),
            ),
            ("XDG_DATA_HOME", "/tmp/agent-runner-opencode-recovery-xdg"),
        ],
    );
    let mut request = support::validated_request_envelope(
        "launch",
        params,
        runtime.host_overrides(),
        "launch.schema.json#/$defs/LaunchRequest",
    );
    request["request_id"] = serde_json::json!("req-launch-generated-session-response-loss");
    support::assert_valid_request_envelope(&request, "launch.schema.json#/$defs/LaunchRequest");
    support::ensure_default_runtime_settings(&request);

    let args = vec!["agent-runner-opencode".to_string(), "launch".to_string()];
    let mut writer = FailAfterFirstLaunchEvent {
        completed_events: 0,
    };
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&request).expect("serialize launch request"),
            &mut writer,
        ),
        1,
        "losing the stdout event should fail the first invocation"
    );
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read launch count"),
        "1\n",
        "the first invocation should create exactly one provider session"
    );
    runtime.delete_settings_record();

    let replay = json_stdout(&support::invoke_with_request("launch", request));
    assert_eq!(
        replay["error"]["code"],
        "launch_session_reconciliation_required"
    );
    assert_eq!(
        replay["error"]["details"]["provider_session_id"],
        provider_session_id
    );
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read replay launch count"),
        "1\n",
        "an exact replay must not create a second provider session"
    );
}

#[test]
fn contract_launch_resume_replay_does_not_resubmit_after_output_loss() {
    let runtime = IsolatedLaunchSettings::new();
    let fake_wrapper = FakeOpencodeWrapper::with_counted_resume();
    let path = prepend_path(fake_wrapper.dir());
    let params =
        resume_launch_params_with_arg_payload_env(path.as_str(), fake_wrapper.log_path_str());
    let mut request = support::validated_request_envelope(
        "launch",
        params,
        runtime.host_overrides(),
        "launch.schema.json#/$defs/LaunchRequest",
    );
    request["request_id"] = serde_json::json!("req-launch-resume-submission-response-loss");
    support::assert_valid_request_envelope(&request, "launch.schema.json#/$defs/LaunchRequest");
    support::ensure_default_runtime_settings(&request);

    let args = vec!["agent-runner-opencode".to_string(), "launch".to_string()];
    let mut writer = FailAfterFirstLaunchEvent {
        completed_events: 0,
    };
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&request).expect("serialize resumed launch request"),
            &mut writer,
        ),
        1,
        "losing a deferred post-spawn event should fail the first invocation"
    );
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read resumed launch count"),
        "1\n",
        "the first invocation should submit exactly one resumed turn"
    );
    runtime.delete_settings_record();

    let replay = json_stdout(&support::invoke_with_request("launch", request));
    assert_eq!(
        replay["error"]["code"],
        "launch_resume_reconciliation_required"
    );
    assert_eq!(
        replay["error"]["details"]["provider_session_id"],
        resume_session_id()
    );
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read replay resume count"),
        "1\n",
        "an exact replay must not submit the resumed turn twice"
    );
}

#[test]
fn contract_launch_resume_reconciliation_observes_late_completion_without_resubmission() {
    let runtime = IsolatedLaunchSettings::new();
    let (fake_wrapper, completion_marker) =
        FakeOpencodeWrapper::with_counted_resume_late_completion();
    let path = prepend_path(fake_wrapper.dir());
    let params =
        resume_launch_params_with_arg_payload_env(path.as_str(), fake_wrapper.log_path_str());
    let mut request = support::validated_request_envelope(
        "launch",
        params,
        runtime.host_overrides(),
        "launch.schema.json#/$defs/LaunchRequest",
    );
    request["request_id"] = serde_json::json!("req-launch-resume-late-completion-reconciliation");
    support::assert_valid_request_envelope(&request, "launch.schema.json#/$defs/LaunchRequest");
    support::ensure_default_runtime_settings(&request);

    let args = vec!["agent-runner-opencode".to_string(), "launch".to_string()];
    let mut writer = FailAfterFirstLaunchEvent {
        completed_events: 0,
    };
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&request).expect("serialize resumed launch request"),
            &mut writer,
        ),
        1
    );
    fs::write(&completion_marker, b"ready").expect("publish later assistant completion");

    let replay = json_stdout(&support::invoke_with_request("launch", request.clone()));
    assert_eq!(
        replay["error"]["code"],
        "launch_resume_reconciliation_required"
    );
    assert_eq!(replay["error"]["details"]["phase"], "completion_observed");
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read resumed launch count"),
        "1\n",
        "late completion reconciliation must not submit another turn"
    );
    let data_root = request["host"]["data_root"]
        .as_str()
        .expect("launch data root");
    let request_id = request["request_id"].as_str().expect("launch request id");
    let state_path = std::path::Path::new(data_root)
        .join("provider-state/opencode/launch/requests")
        .join(format!(
            "{}.json",
            agent_runner_opencode::encoding::sha256_hex(request_id.as_bytes())
        ));
    let state: Value = serde_json::from_slice(
        &fs::read(state_path).expect("read reconciled resumed launch state"),
    )
    .expect("parse reconciled resumed launch state");
    assert_eq!(state["phase"], "completion_observed");
}

#[test]
fn contract_launch_resume_replay_preserves_multi_part_positional_identity() {
    let runtime = IsolatedLaunchSettings::new();
    let submitted_message = "hello world";
    let fake_wrapper = FakeOpencodeWrapper::with_counted_resume_payload(submitted_message);
    let path = prepend_path(fake_wrapper.dir());
    let mut params = launch_params_with_argv_and_prompt_env(
        vec!["--".to_string(), "hello".to_string(), "world".to_string()],
        None,
        path.as_str(),
        fake_wrapper.log_path_str(),
    );
    params["session"] = serde_json::json!({
        "known_provider_session_id": resume_session_id(),
        "start_mode": "resume"
    });
    let mut request = support::validated_request_envelope(
        "launch",
        params,
        runtime.host_overrides(),
        "launch.schema.json#/$defs/LaunchRequest",
    );
    request["request_id"] =
        serde_json::json!("req-launch-resume-multi-part-submission-response-loss");
    support::assert_valid_request_envelope(&request, "launch.schema.json#/$defs/LaunchRequest");
    support::ensure_default_runtime_settings(&request);

    let args = vec!["agent-runner-opencode".to_string(), "launch".to_string()];
    let mut writer = FailAfterFirstLaunchEvent {
        completed_events: 0,
    };
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&request).expect("serialize resumed launch request"),
            &mut writer,
        ),
        1,
        "losing a deferred post-spawn event should fail the first invocation"
    );
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read resumed launch count"),
        "1\n",
        "the first invocation should submit the multi-part resumed turn exactly once"
    );
    runtime.delete_settings_record();

    let replay = json_stdout(&support::invoke_with_request("launch", request));
    assert_eq!(
        replay["error"]["code"],
        "launch_resume_reconciliation_required"
    );
    assert_eq!(
        replay["error"]["details"]["provider_session_id"],
        resume_session_id()
    );
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read replay resume count"),
        "1\n",
        "an exact replay must not duplicate the reconstructed multi-part turn"
    );
}

#[test]
fn contract_launch_route_handoff_failure_releases_request_before_spawn() {
    let runtime = IsolatedLaunchSettings::new();
    let provider_session_id = "ses_route_handoff_retry";
    let fake_wrapper = FakeOpencodeWrapper::with_counted_new_session(provider_session_id);
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_env(
        "low",
        &[
            ("PATH", path.as_str()),
            (
                "AGENT_RUNNER_OPENCODE_WRAPPER_LOG",
                fake_wrapper.log_path_str(),
            ),
        ],
    );
    let mut request = support::validated_request_envelope(
        "launch",
        params,
        runtime.host_overrides(),
        "launch.schema.json#/$defs/LaunchRequest",
    );
    request["request_id"] = serde_json::json!("req-launch-route-response-loss");
    support::assert_valid_request_envelope(&request, "launch.schema.json#/$defs/LaunchRequest");
    support::ensure_default_runtime_settings(&request);
    let args = vec!["agent-runner-opencode".to_string(), "launch".to_string()];

    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&request).expect("serialize launch request"),
            &mut RejectLaunchWrites,
        ),
        1
    );
    assert!(
        !fake_wrapper.log_path().exists(),
        "route handoff must complete before the child can spawn"
    );

    let home = std::env::var("HOME").expect("test HOME");
    let replay = support::invoke_with_request_and_env("launch", request, &[("HOME", &home)]);
    assert_output_success(&replay, "launch after pre-spawn route handoff failure");
    let events = launch_events_from_output(&replay, "route handoff retry stdout");
    assert!(events.iter().any(|event| {
        event["kind"] == "marker"
            && event["name"] == "oulipoly.provider_session"
            && event["value"]["provider_session_id"] == provider_session_id
    }));
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read route retry launch count"),
        "1\n"
    );
}

#[test]
#[cfg(unix)]
fn contract_launch_prepared_recovery_waits_for_prior_actor_before_readmission() {
    let runtime = IsolatedLaunchSettings::new();
    let provider_session_id = "ses_prepared_recovery_readmission";
    let fake_wrapper = FakeOpencodeWrapper::with_counted_new_session(provider_session_id);
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_env(
        "low",
        &[
            ("PATH", path.as_str()),
            (
                "AGENT_RUNNER_OPENCODE_WRAPPER_LOG",
                fake_wrapper.log_path_str(),
            ),
            ("XDG_DATA_HOME", "/tmp/agent-runner-opencode-recovery-xdg"),
        ],
    );
    let mut request = support::validated_request_envelope(
        "launch",
        params,
        runtime.host_overrides(),
        "launch.schema.json#/$defs/LaunchRequest",
    );
    let request_id = "req-launch-prepared-recovery-readmission";
    request["request_id"] = serde_json::json!(request_id);
    support::assert_valid_request_envelope(&request, "launch.schema.json#/$defs/LaunchRequest");
    support::ensure_default_runtime_settings(&request);

    let policy = json_stdout(&support::invoke(
        "policy.evaluate",
        policy_evaluate_params(),
    ));
    let route = policy["result"]["markers"].clone();
    let binding_sha256 = agent_runner_opencode::encoding::sha256_hex(
        serde_json::json!({
            "host_app": request["host"]["app"],
            "params": request["params"],
            "route": route,
        })
        .to_string()
        .as_bytes(),
    );
    let request_identity_sha256 = agent_runner_opencode::encoding::sha256_hex(
        serde_json::json!({
            "host_app": request["host"]["app"],
            "params": request["params"],
        })
        .to_string()
        .as_bytes(),
    );
    let prompt_sha256 = agent_runner_opencode::encoding::sha256_hex(
        request["params"]["model"]["inputs"]["prompt"]
            .as_str()
            .expect("launch prompt")
            .as_bytes(),
    );
    let declared_env_sha256 = agent_runner_opencode::encoding::sha256_hex(
        serde_json::to_vec(&request["params"]["env"])
            .expect("serialize declared launch environment")
            .as_slice(),
    );
    let data_root = std::path::PathBuf::from(
        request["host"]["data_root"]
            .as_str()
            .expect("launch data root"),
    );
    let state_root = data_root.join("provider-state/opencode/launch/requests");
    fs::create_dir_all(&state_root).expect("create prepared launch state root");
    let state_path = state_root.join(format!(
        "{}.json",
        agent_runner_opencode::encoding::sha256_hex(request_id.as_bytes())
    ));
    let prepared_at_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("current time")
        .as_millis() as u64;
    let mut prepared_state = serde_json::json!({
        "schema_version": 5,
        "operation_kind": "new_session",
        "request_id": request_id,
        "request_identity_sha256": request_identity_sha256,
        "binding_sha256": binding_sha256,
        "prompt_sha256": prompt_sha256,
        "recovery": {
            "program": fake_wrapper.dir().join("opencode1").to_string_lossy(),
            "passthrough_env": {},
            "declared_env_sha256": declared_env_sha256,
            "working_directory": env!("CARGO_MANIFEST_DIR"),
            "provider_id": "openai",
            "model_id": "gpt-5.6-sol",
            "effort": "low"
        },
        "phase": "prepared",
        "actor_process_group_id": null,
        "provider_session_id": null,
        "terminal_status": null,
        "prepared_at_unix_ms": prepared_at_unix_ms,
        "observed_at_unix_ms": null
    });
    let mut prior_actor_command = std::process::Command::new("/bin/sleep");
    prior_actor_command.arg("30");
    unsafe {
        prior_actor_command.pre_exec(|| {
            if setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut prior_actor = prior_actor_command
        .spawn()
        .expect("spawn prior launch actor");
    prepared_state["actor_process_group_id"] = serde_json::json!(prior_actor.id());
    fs::write(
        &state_path,
        serde_json::to_vec(&prepared_state).expect("serialize actor-bound launch state"),
    )
    .expect("write actor-bound launch state");

    let held = json_stdout(&support::invoke_with_request("launch", request.clone()));
    assert_eq!(
        held["error"]["code"],
        "launch_actor_reconciliation_required"
    );
    assert_eq!(
        held["error"]["details"]["process_group_id"],
        prior_actor.id()
    );
    assert!(
        !fake_wrapper.log_path().exists(),
        "a live prior actor must prevent a second native spawn"
    );

    prior_actor.kill().expect("terminate prior launch actor");
    prior_actor.wait().expect("reap prior launch actor");

    prepared_state["actor_process_group_id"] = serde_json::Value::Null;
    fs::write(
        &state_path,
        serde_json::to_vec(&prepared_state).expect("serialize unpublished-actor launch state"),
    )
    .expect("write unpublished-actor launch state");

    let replay = support::invoke_with_request("launch", request);
    assert_output_success(&replay, "recovered prepared launch");
    assert_eq!(
        fs::read_to_string(fake_wrapper.log_path()).expect("read recovered launch count"),
        "1\n",
        "the exec-gated unpublished actor and exhaustive empty session list should readmit exactly one launch"
    );
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
fn contract_native_runtime_identity_is_shared_across_capabilities() {
    let runtime = IsolatedLaunchSettings::new();
    let admitted_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_runtime_identity_script());
    let conflicting_wrapper = FakeOpencodeWrapper::with_script("#!/bin/sh\nexit 17\n".to_string());
    let admitted_path = prepend_path(admitted_wrapper.dir());
    let conflicting_path = prepend_path(conflicting_wrapper.dir());
    let admitted_home = tempfile::tempdir().expect("create admitted native HOME");
    let conflicting_home = tempfile::tempdir().expect("create conflicting native HOME");
    let admitted_home_path = admitted_home.path().to_string_lossy().into_owned();
    let conflicting_home_path = conflicting_home.path().to_string_lossy().into_owned();
    let admitted_auth_path = admitted_home.path().join(".local/share/opencode/auth.json");
    fs::create_dir_all(admitted_auth_path.parent().expect("auth parent"))
        .expect("create admitted auth parent");
    fs::write(&admitted_auth_path, "{}\n").expect("write admitted auth source");
    let launch = invoke_with_host_and_env(
        "launch",
        launch_params_with_env(
            "low",
            &[
                ("PATH", admitted_path.as_str()),
                ("HOME", admitted_home_path.as_str()),
                ("CONTEXT_SELECTOR", "runtime-a"),
            ],
        ),
        runtime.host_overrides(),
        &[
            ("PATH", admitted_path.as_str()),
            ("HOME", admitted_home_path.as_str()),
        ],
    );
    assert_output_success(&launch, "launch that admits native runtime identity");

    let session_id = "ses_native_runtime_identity";
    let exported = support::invoke_validated_with_host_and_env(
        "session.export",
        serde_json::json!({
            "settings_id": "opencode1",
            "session_id": session_id,
        }),
        runtime.host_overrides(),
        "session.schema.json#/$defs/SessionExportRequest",
        &[
            ("PATH", conflicting_path.as_str()),
            ("HOME", conflicting_home_path.as_str()),
        ],
    );
    let export_response = json_stdout(&exported);
    support::assert_valid(
        &export_response,
        "session.schema.json#/$defs/SessionExportResponse",
    );
    assert_eq!(export_response["ok"], true, "response={export_response}");
    assert_eq!(export_response["result"]["turn_count"], 0);

    let quota_source = support::invoke_validated_with_host_and_env(
        "quota.source",
        serde_json::json!({ "settings_id": "opencode1" }),
        runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaSourceRequest",
        &[
            ("PATH", conflicting_path.as_str()),
            ("HOME", conflicting_home_path.as_str()),
        ],
    );
    let quota_response = json_stdout(&quota_source);
    support::assert_valid(
        &quota_response,
        "quota.schema.json#/$defs/QuotaSourceResponse",
    );
    assert_eq!(quota_response["ok"], true, "response={quota_response}");
    assert_eq!(quota_response["result"]["has_source"], true);
    assert!(quota_response["result"]["source_id"]
        .as_str()
        .is_some_and(|source| source.contains(admitted_home_path.as_str())));

    let conflicting_launch = invoke_with_host_and_env(
        "launch",
        launch_params_with_env(
            "low",
            &[
                ("PATH", conflicting_path.as_str()),
                ("HOME", conflicting_home_path.as_str()),
            ],
        ),
        runtime.host_overrides(),
        &[
            ("PATH", conflicting_path.as_str()),
            ("HOME", conflicting_home_path.as_str()),
        ],
    );
    let conflict_response = json_stdout(&conflicting_launch);
    assert_eq!(
        conflict_response["error"]["code"],
        "native_runtime_context_conflict"
    );
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
fn contract_launch_rejects_untyped_native_session_selector_before_spawn() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(fake_wrapper_log_only_script());
    let path = prepend_path(fake_wrapper.dir());
    let params = launch_params_with_argv_and_prompt_env(
        vec![
            "--session".to_string(),
            resume_session_id().to_string(),
            "hello".to_string(),
        ],
        Some("hello"),
        path.as_str(),
        fake_wrapper.log_path_str(),
    );

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_ne!(output.status.code(), Some(0));
    assert!(
        !fake_wrapper.log_path().exists(),
        "an untyped native session selector must fail before spawn"
    );
    let response = json_stdout(&output);
    assert_eq!(
        response["error"]["code"],
        "native_session_selector_forbidden"
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

    assert_eq!(output.status.code(), Some(1));
    assert_resume_arg_payload_wrapper_log(fake_wrapper.log_path());
}

#[test]
fn contract_launch_resume_preserves_session_shaped_positional_message() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let mut params = launch_params_with_argv_and_prompt_env(
        vec![
            "--".to_string(),
            "--session".to_string(),
            "literal-session-text".to_string(),
        ],
        None,
        path.as_str(),
        fake_wrapper.log_path_str(),
    );
    params["session"] = serde_json::json!({
        "known_provider_session_id": resume_session_id(),
        "start_mode": "resume"
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_eq!(output.status.code(), Some(1));
    let wrapper_log = wrapper_log_text(fake_wrapper.log_path());
    let argv = wrapper_log_args(&wrapper_log);
    let boundary = argv_arg_index(&argv, "--");
    assert_eq!(
        &argv[boundary + 1..boundary + 3],
        &["--session", "literal-session-text"],
        "the managed session option must not rewrite positional message text"
    );
    assert!(argv
        .get(boundary + 3)
        .is_some_and(|arg| { arg.starts_with("[OULIPOLY-DELIVERY ") && arg.ends_with(']') }));
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

    assert_eq!(output.status.code(), Some(1));
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

    assert_eq!(output.status.code(), Some(1));
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
fn contract_launch_resume_uses_run_event_instead_of_export_payload_for_submission() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(
        fake_wrapper_resume_unconfirmed_export_script().to_string(),
    );
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    let events = launch_events_from_output(&output, "launch resume unconfirmed payload stdout");
    assert_submitted_user_turn_marker(&events);
    assert_unresolved_resume_completion(&output, &events);
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
fn contract_launch_resume_does_not_poll_full_export_for_completion() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_completed_export_then_hang_script());
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_str();
    let params = resume_launch_params_with_arg_payload_env(path.as_str(), log_path);
    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);
    let events = launch_events_from_output(&output, "unobserved buffered resume stdout");
    assert_no_produced_assistant_response_marker(&events);
    assert!(
        !fake_wrapper.log_path().exists(),
        "normal resume supervision must use bounded run events rather than full transcript export"
    );
    let final_event = final_launch_event(&events);
    assert_eq!(final_event["status"]["kind"], "exited");
    assert_live_provider_exit_code(&output, final_event, "unobserved buffered resume");
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
fn contract_launch_resume_rejects_unbounded_option_shaped_payload_before_spawn() {
    let fake_wrapper =
        FakeOpencodeWrapper::with_script(fake_wrapper_log_stdin_script().to_string());
    let path = prepend_path(fake_wrapper.dir());
    let mut params = launch_params_with_argv_and_prompt_env(
        vec!["--share".to_string(), "hello".to_string()],
        None,
        path.as_str(),
        fake_wrapper.log_path_str(),
    );
    params["session"] = serde_json::json!({
        "known_provider_session_id": resume_session_id(),
        "start_mode": "resume"
    });

    let output = invoke_with_env("launch", params, &[("PATH", path.as_str())]);

    assert_ne!(output.status.code(), Some(0), "{output:?}");
    assert!(
        !fake_wrapper.log_path().exists(),
        "ambiguous resume payload must fail before spawning opencode"
    );
    let response = json_stdout(&output);
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "ambiguous_resume_payload");
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
                    && marker["value"].as_str().is_some_and(|value| {
                        value.starts_with(&format!("settings record {account} at version v"))
                    })
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
fn contract_policy_evaluate_rejects_unsupported_tool_restrictions() {
    let output = invoke_validated(
        "policy.evaluate",
        policy_evaluate_params_with_tool_restrictions(),
        "policy.schema.json#/$defs/PolicyEvaluateRequest",
    );

    assert_output_success(&output, "policy.evaluate tool restriction rejection");
    let response = json_stdout(&output);
    assert_policy_response_shape(&response);
    let result = policy_result(&response);
    assert_policy_rejected(
        result,
        "unsupported OpenCode tool restrictions must fail closed",
    );
    assert_policy_diagnostic(
        policy_diagnostics(result),
        "unsupported_tool_restrictions",
        "cannot faithfully enforce",
    );
}

#[test]
fn contract_policy_evaluate_rejects_unsupported_system_prompt_override() {
    let output = invoke_validated(
        "policy.evaluate",
        policy_evaluate_params_with_system_prompt_override(),
        "policy.schema.json#/$defs/PolicyEvaluateRequest",
    );

    assert_output_success(&output, "policy.evaluate system prompt override rejection");
    let response = json_stdout(&output);
    assert_policy_response_shape(&response);
    let result = policy_result(&response);
    assert_policy_rejected(
        result,
        "unsupported OpenCode system prompt override must fail closed",
    );
    assert_policy_diagnostic(
        policy_diagnostics(result),
        "unsupported_system_prompt_override",
        "cannot faithfully enforce",
    );
}

#[test]
fn contract_policy_evaluate_accepts_only_canonical_command_for_every_account_id() {
    for settings_id in account_host_settings_ids() {
        let output = invoke_with_env(
            "policy.evaluate",
            policy_evaluate_params_for_alias_host_candidate(settings_id, settings_id),
            &[],
        );

        assert_output_success(
            &output,
            &format!("policy.evaluate canonical host candidate argv for {settings_id}"),
        );
        let response = json_stdout(&output);
        assert_policy_accepts_for_wrapper(&response, settings_id);

        let path_command = host_bin_command(settings_id);
        let path_output = invoke_with_env(
            "policy.evaluate",
            policy_evaluate_params_for_alias_host_candidate(settings_id, &path_command),
            &[],
        );
        assert_output_success(
            &path_output,
            &format!("policy.evaluate path-shaped host candidate argv for {settings_id}"),
        );
        assert_policy_rejected_with_code(&json_stdout(&path_output), "invalid_command");
    }
}

#[test]
fn contract_policy_evaluate_rejects_account_one_wrapper_command_aliases() {
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
        assert_policy_rejected_with_code(&response, "invalid_command");
    }
}

#[test]
fn contract_policy_evaluate_rejects_user_injected_managed_flag_after_host_prefix() {
    for forbidden_flag in [
        "--variant",
        "--session",
        "--session=ses_caller_selected",
        "-s",
        "-sses_caller_selected",
        "--continue",
        "--continue=true",
        "-c",
        "--fork",
        "--fork=true",
    ] {
        let output = invoke_with_env(
            "policy.evaluate",
            forbidden_policy_evaluate_params_for_account_host_candidate(
                "opencode2",
                forbidden_flag,
            ),
            &[],
        );

        assert_output_success(&output, "policy.evaluate injected host suffix rejection");
        let response = json_stdout(&output);
        assert_policy_rejects_forbidden_arg(&response, forbidden_flag);
    }
}

#[test]
fn contract_policy_evaluate_preserves_session_control_text_after_message_boundary() {
    let mut params = policy_evaluate_params_for_account_host_candidate("opencode2");
    let argv = params["launch"]["argv"]
        .as_array_mut()
        .expect("host candidate argv");
    argv.pop().expect("host candidate prompt");
    argv.extend([
        json!("--"),
        json!("--session"),
        json!("literal-session"),
        json!("-s"),
        json!("--continue"),
        json!("-c"),
        json!("--fork"),
    ]);

    let output = invoke_with_env("policy.evaluate", params, &[]);

    assert_output_success(&output, "policy.evaluate literal session control text");
    let response = json_stdout(&output);
    assert_policy_response_shape(&response);
    let result = policy_result(&response);
    assert_policy_accepted(result);
    let effective_argv = result["argv"].as_array().expect("effective argv");
    let boundary = effective_argv
        .iter()
        .position(|arg| arg == "--")
        .expect("message boundary");
    assert_eq!(
        &effective_argv[boundary + 1..],
        &[
            json!("--session"),
            json!("literal-session"),
            json!("-s"),
            json!("--continue"),
            json!("-c"),
            json!("--fork"),
        ]
    );
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
fn contract_policy_evaluate_rejects_account_one_plain_host_command() {
    let output = invoke_with_env(
        "policy.evaluate",
        policy_evaluate_account_one_plain_host_command_params(),
        &[],
    );

    assert_output_success(&output, "policy.evaluate account-one plain host command");
    let response = json_stdout(&output);
    assert_policy_rejected_with_code(&response, "invalid_command");
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
