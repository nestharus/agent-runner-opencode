// declared_role: formatter, mapper
#![allow(unused_imports)]

use agent_runner_opencode::encoding::sha256_hex;

use super::*;

pub fn launch_params(effort: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "mode": "agent",
        "model": model_request(effort),
        "argv": ["reply with the single word: ok"],
        "working_directory": env!("CARGO_MANIFEST_DIR")
    })
}

pub fn resume_launch_params_with_arg_payload() -> Value {
    let mut params = launch_params("low");
    params["session"] = json!({ "known_provider_session_id": resume_session_id() });
    params["argv"] = json!([resume_payload()]);
    params["model"]["inputs"]["prompt"] = json!(resume_payload());
    params
}

pub fn resume_launch_params_with_arg_payload_env(path: &str, log_path: &str) -> Value {
    launch_params_with_wrapper_env(resume_launch_params_with_arg_payload(), path, log_path)
}

pub fn resume_launch_params_with_arg_payload_prompt_env(
    prompt: &str,
    path: &str,
    log_path: &str,
) -> Value {
    let mut params = resume_launch_params_with_arg_payload();
    params["model"]["inputs"]["prompt"] = json!(prompt);
    launch_params_with_wrapper_env(params, path, log_path)
}

pub fn resume_launch_params_with_stdin_payload() -> Value {
    let mut params = launch_params("low");
    params["session"] = json!({ "known_provider_session_id": resume_session_id() });
    params["argv"] = json!([]);
    params["stdin"] = json!({
        "encoding": "utf8",
        "data": resume_payload(),
    });
    params["model"]["inputs"]["prompt"] = json!(resume_payload());
    params
}

pub fn resume_launch_params_with_stdin_payload_env(path: &str, log_path: &str) -> Value {
    launch_params_with_wrapper_env(resume_launch_params_with_stdin_payload(), path, log_path)
}

pub fn resume_launch_params_without_payload() -> Value {
    let mut params = launch_params("low");
    params["session"] = json!({ "known_provider_session_id": resume_session_id() });
    params["argv"] = json!([]);
    params["model"]["inputs"]["prompt"] = json!("");
    params
}

pub fn resume_launch_params_without_payload_env(path: &str, log_path: &str) -> Value {
    launch_params_with_wrapper_env(resume_launch_params_without_payload(), path, log_path)
}

pub fn launch_params_with_policy_effective_argv(effort: &str) -> Value {
    let mut params = launch_params(effort);
    params["argv"] = json!(policy_effective_argv(effort));
    params
}

pub fn launch_params_with_policy_effective_argv_env(
    effort: &str,
    path: &str,
    log_path: &str,
) -> Value {
    launch_params_with_wrapper_env(
        launch_params_with_policy_effective_argv(effort),
        path,
        log_path,
    )
}

pub fn policy_evaluate_params() -> Value {
    json!({
        "settings_id": "opencode1",
        "mode": "agent",
        "model": model_request("low"),
        "launch": {
            "argv": ["reply with the single word: ok"],
            "working_directory": env!("CARGO_MANIFEST_DIR")
        }
    })
}

pub fn policy_evaluate_params_with_host_candidate_argv() -> Value {
    let mut params = policy_evaluate_params();
    params["launch"]["argv"] = json!(host_candidate_argv("low"));
    params
}

pub fn policy_evaluate_params_with_host_candidate_command(command: &str) -> Value {
    let mut params = policy_evaluate_params();
    params["launch"]["argv"] = json!(host_candidate_argv_for_command(command, "low"));
    params
}

pub fn policy_evaluate_params_for_account_host_candidate(settings_id: &str) -> Value {
    policy_evaluate_params_for_alias_host_candidate(settings_id, settings_id)
}

pub fn policy_evaluate_params_for_alias_host_candidate(settings_id: &str, command: &str) -> Value {
    let mut params = policy_evaluate_params();
    params["settings_id"] = json!(settings_id);
    params["launch"]["argv"] = json!(host_candidate_argv_for_command(command, "low"));
    params
}

pub fn forbidden_policy_evaluate_params_for_account_host_candidate(
    settings_id: &str,
    forbidden_flag: &str,
) -> Value {
    let mut params = policy_evaluate_params_for_account_host_candidate(settings_id);
    params["launch"]["argv"]
        .as_array_mut()
        .expect("host candidate argv")
        .extend([json!(forbidden_flag), json!("high")]);
    params
}

pub fn policy_evaluate_account_one_provider_name_settings_id_params() -> Value {
    policy_evaluate_params_with_settings_id(policy_evaluate_params_with_host_candidate_argv())
}

pub fn policy_evaluate_account_one_plain_host_command_params() -> Value {
    policy_evaluate_params_with_settings_id(policy_evaluate_params_with_host_candidate_command(
        "opencode",
    ))
}

pub fn policy_evaluate_params_with_settings_id(mut params: Value) -> Value {
    params["settings_id"] = json!("opencode");
    params
}

pub fn forbidden_policy_evaluate_params(forbidden_flag: &str, forbidden_env_key: &str) -> Value {
    let mut env = serde_json::Map::new();
    env.insert(
        forbidden_env_key.to_string(),
        json!("SENTINEL_POLICY_FORBIDDEN_ENV_DO_NOT_LEAK"),
    );
    env.insert("CONTRACT_ALLOWED_ENV".to_string(), json!("allowed"));
    json!({
        "settings_id": "opencode1",
        "mode": "agent",
        "model": model_request("low"),
        "launch": {
            "argv": [forbidden_flag, "high", "reply with the single word: ok"],
            "env": env,
            "working_directory": env!("CARGO_MANIFEST_DIR")
        }
    })
}

pub fn policy_evaluate_params_with_env(settings_id: &str, env: &[(&str, &str)]) -> Value {
    let mut params = policy_evaluate_params();
    params["settings_id"] = json!(settings_id);
    params["launch"]["env"] = env
        .iter()
        .map(|(key, value)| (key.to_string(), json!(value)))
        .collect::<serde_json::Map<String, Value>>()
        .into();
    params
}

pub fn terminal_status_cases() -> Vec<(Value, &'static str)> {
    vec![
        (json!({ "kind": "exited", "code": 0 }), "clean_exit"),
        (json!({ "kind": "exited", "code": 17 }), "nonzero_exit"),
        (
            json!({ "kind": "signal_terminated", "signal": 15 }),
            "signal_exit",
        ),
        (
            json!({ "kind": "spawn_error", "reason": "ENOENT" }),
            "spawn_error",
        ),
        (
            json!({ "kind": "prolonged_silence", "reason": "no output before deadline" }),
            "prolonged_silence",
        ),
        (json!({ "kind": "cancelled" }), "cancelled"),
        (json!({ "kind": "unknown" }), "unknown"),
    ]
}

pub fn launch_params_with_env(effort: &str, env: &[(&str, &str)]) -> Value {
    let mut params = launch_params(effort);
    params["env"] = Value::Object(
        env.iter()
            .map(|(key, value)| ((*key).to_string(), json!(*value)))
            .collect(),
    );
    params
}

pub fn launch_params_with_wrapper_env(mut params: Value, path: &str, log_path: &str) -> Value {
    params["env"] = wrapper_env(path, log_path);
    params
}

pub fn wrapper_env(path: &str, log_path: &str) -> Value {
    json!({
        "PATH": path,
        "AGENT_RUNNER_OPENCODE_WRAPPER_LOG": log_path
    })
}

pub fn model_request(effort: &str) -> Value {
    json!({
        "name": format!("gpt-{effort}"),
        "provider_args": ["-m", "openai/gpt-5.5", "--variant", effort],
        "inputs": {
            "prompt": "reply with the single word: ok",
            "named": {}
        }
    })
}

pub fn resume_session_id() -> &'static str {
    "ses_resume_contract"
}

pub fn resume_payload() -> &'static str {
    "[OULIPOLY NOTIFICATIONS]\nkind: agent_bash_complete\nhandle: h-s11-external\n[OULIPOLY-DELIVERY 5169694d-de0f-40d1-890c-6e28e55bab27]\n[END OULIPOLY NOTIFICATIONS]\n"
}

pub fn resume_payload_sha256() -> String {
    sha256_hex(resume_payload().as_bytes())
}

pub fn host_candidate_argv(effort: &str) -> Vec<&str> {
    host_candidate_argv_for_command("opencode1", effort)
}

pub fn host_candidate_argv_for_command<'a>(command: &'a str, effort: &'a str) -> Vec<&'a str> {
    vec![
        command,
        "run",
        "--dangerously-skip-permissions",
        "-m",
        "openai/gpt-5.5",
        "--variant",
        effort,
        "reply with the single word: ok",
    ]
}

pub fn policy_effective_argv(effort: &str) -> Vec<&str> {
    vec![
        "opencode1",
        "run",
        "--format",
        "json",
        "--dangerously-skip-permissions",
        "-m",
        "openai/gpt-5.5",
        "--variant",
        effort,
        "reply with the single word: ok",
    ]
}

pub fn terminal_classify_params(status: Value, stdout: &str, stderr: &str) -> Value {
    json!({
        "stdout_base64": encode_base64(stdout.as_bytes()),
        "stderr_base64": encode_base64(stderr.as_bytes()),
        "status": status,
        "observed_at_unix_ms": OBSERVED_AT_UNIX_MS
    })
}

pub fn deadline_host(deadline_unix_ms: u64) -> Value {
    json!({ "deadline_unix_ms": deadline_unix_ms })
}
