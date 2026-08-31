// declared_role: formatter, mapper
#![allow(unused_imports)]

use agent_runner_opencode::encoding::sha256_hex;

use super::*;

pub fn launch_params(effort: &str) -> Value {
    let alias = format!("gpt-{effort}");
    launch_params_for_model(
        "opencode1",
        "opencode1",
        &alias,
        "openai/gpt-5.6-sol",
        effort,
    )
}

pub fn launch_params_for_model(
    settings_id: &str,
    command: &str,
    alias: &str,
    provider_model: &str,
    effort: &str,
) -> Value {
    json!({
        "settings_id": settings_id,
        "mode": "agent",
        "model": model_request_for(alias, provider_model, effort),
        "argv": host_candidate_argv_for_model(command, provider_model, effort),
        "working_directory": env!("CARGO_MANIFEST_DIR"),
        "output_delivery": {
            "protocol": "oulipoly.launch_output/v1"
        }
    })
}

pub fn launch_params_with_prompt(prompt: &str) -> Value {
    let mut params = launch_params("low");
    *params["argv"]
        .as_array_mut()
        .expect("launch argv")
        .last_mut()
        .expect("prompt arg") = json!(prompt);
    params["model"]["inputs"]["prompt"] = json!(prompt);
    params
}

pub fn launch_params_with_prompt_env(prompt: &str, path: &str, log_path: &str) -> Value {
    launch_params_with_wrapper_env(launch_params_with_prompt(prompt), path, log_path)
}

pub fn launch_params_with_argv_and_prompt_env(
    argv: Vec<String>,
    prompt: Option<&str>,
    path: &str,
    log_path: &str,
) -> Value {
    let mut params = launch_params("low");
    let launch_argv = params["argv"].as_array_mut().expect("launch argv");
    launch_argv.pop();
    launch_argv.extend(argv.into_iter().map(Value::String));
    match prompt {
        Some(prompt) => params["model"]["inputs"]["prompt"] = json!(prompt),
        None => {
            params["model"]["inputs"]
                .as_object_mut()
                .expect("model inputs")
                .remove("prompt");
        }
    }
    launch_params_with_wrapper_env(params, path, log_path)
}

pub fn resume_launch_params_with_arg_payload() -> Value {
    let mut params = launch_params("low");
    params["session"] = json!({
        "known_provider_session_id": resume_session_id(),
        "start_mode": "resume"
    });
    *params["argv"]
        .as_array_mut()
        .expect("launch argv")
        .last_mut()
        .expect("prompt arg") = json!(resume_payload());
    params["model"]["inputs"]["prompt"] = json!(resume_payload());
    params
}

pub fn resume_launch_params_with_arg_payload_env(path: &str, log_path: &str) -> Value {
    launch_params_with_wrapper_env(resume_launch_params_with_arg_payload(), path, log_path)
}

pub fn resume_launch_params_with_prompt_env(prompt: &str, path: &str, log_path: &str) -> Value {
    let mut params = launch_params_with_prompt(prompt);
    params["session"] = json!({
        "known_provider_session_id": resume_session_id(),
        "start_mode": "resume"
    });
    launch_params_with_wrapper_env(params, path, log_path)
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
    params["session"] = json!({
        "known_provider_session_id": resume_session_id(),
        "start_mode": "resume"
    });
    params["argv"].as_array_mut().expect("launch argv").pop();
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
    params["session"] = json!({
        "known_provider_session_id": resume_session_id(),
        "start_mode": "resume"
    });
    params["argv"].as_array_mut().expect("launch argv").pop();
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
    policy_evaluate_params_for_model("gpt-low", "openai/gpt-5.6-sol", "low")
}

pub fn policy_evaluate_params_for_model(alias: &str, provider_model: &str, effort: &str) -> Value {
    policy_evaluate_params_for_account_model("opencode1", alias, provider_model, effort)
}

pub fn policy_evaluate_params_for_account_model(
    account: &str,
    alias: &str,
    provider_model: &str,
    effort: &str,
) -> Value {
    json!({
        "settings_id": account,
        "mode": "agent",
        "model": model_request_for(alias, provider_model, effort),
        "launch": {
            "argv": host_candidate_argv_for_model(account, provider_model, effort),
            "working_directory": env!("CARGO_MANIFEST_DIR")
        }
    })
}

pub fn policy_evaluate_luna_max_params() -> Value {
    policy_evaluate_params_for_model("gpt-luna-max", "openai/gpt-5.6-luna", "max")
}

pub fn policy_evaluate_luna_low_params() -> Value {
    policy_evaluate_params_for_model("gpt-luna-low", "openai/gpt-5.6-luna", "low")
}

pub fn policy_evaluate_model_mismatch_params() -> Value {
    let mut params = policy_evaluate_luna_max_params();
    params["model"]["provider_args"] = json!(["-m", "openai/gpt-5.6-sol", "--variant", "max"]);
    params
}

pub fn policy_evaluate_extra_model_arg_params() -> Value {
    let mut params = policy_evaluate_params();
    params["model"]["provider_args"]
        .as_array_mut()
        .expect("model provider args")
        .push(json!("--share"));
    params
}

pub fn policy_evaluate_params_with_host_candidate_argv() -> Value {
    let mut params = policy_evaluate_params();
    params["launch"]["argv"] = json!(host_candidate_argv("low"));
    params
}

pub fn policy_evaluate_params_with_tool_restrictions() -> Value {
    let mut params = policy_evaluate_params_with_host_candidate_argv();
    params["launch"]["tool_restrictions"] = json!({
        "kind": "codex",
        "codex": {
            "disabled_features": ["web_search"]
        }
    });
    params
}

pub fn policy_evaluate_params_with_system_prompt_override() -> Value {
    let mut params = policy_evaluate_params_with_host_candidate_argv();
    params["launch"]["system_prompt_override"] =
        json!("Do not use a built-in child invocation mechanism.");
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

pub fn policy_evaluate_account_one_persisted_settings_id_params() -> Value {
    policy_evaluate_params_with_settings_id(policy_evaluate_params_with_host_candidate_argv())
}

pub fn policy_evaluate_account_one_plain_host_command_params() -> Value {
    policy_evaluate_params_with_settings_id(policy_evaluate_params_with_host_candidate_command(
        "opencode",
    ))
}

pub fn policy_evaluate_params_with_settings_id(mut params: Value) -> Value {
    params["settings_id"] = json!("opencode1");
    params
}

pub fn forbidden_policy_evaluate_params(forbidden_flag: &str, forbidden_env_key: &str) -> Value {
    let mut params = policy_evaluate_params();
    let argv = params["launch"]["argv"]
        .as_array_mut()
        .expect("host candidate argv");
    let prompt = argv.pop().expect("prompt arg");
    argv.extend([json!(forbidden_flag), json!("high"), prompt]);
    params["launch"]["env"] = Value::Object(
        [
            (
                forbidden_env_key.to_string(),
                json!("SENTINEL_POLICY_CONFIGURED_ENV"),
            ),
            ("CONTRACT_ALLOWED_ENV".to_string(), json!("allowed")),
        ]
        .into_iter()
        .collect(),
    );
    params
}

pub fn policy_evaluate_params_with_env(settings_id: &str, env: &[(&str, &str)]) -> Value {
    let mut params = policy_evaluate_params_for_account_host_candidate(settings_id);
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

pub fn launch_luna_max_params_with_env(path: &str, log_path: &str) -> Value {
    let params = launch_params_for_model(
        "opencode1",
        "opencode1",
        "gpt-luna-max",
        "openai/gpt-5.6-luna",
        "max",
    );
    launch_params_with_wrapper_env(params, path, log_path)
}

pub fn launch_luna_low_params_with_env(path: &str, log_path: &str) -> Value {
    let params = launch_params_for_model(
        "opencode1",
        "opencode1",
        "gpt-luna-low",
        "openai/gpt-5.6-luna",
        "low",
    );
    launch_params_with_wrapper_env(params, path, log_path)
}

pub fn live_luna_low_launch_params(path: &str, home: &str) -> Value {
    let mut params = launch_params_for_model(
        "opencode5",
        "opencode5",
        "gpt-luna-low",
        "openai/gpt-5.6-luna",
        "low",
    );
    params["env"] = json!({ "PATH": path, "HOME": home });
    params
}

pub fn launch_create_session_params_with_env(path: &str, log_path: &str) -> Value {
    let mut params = launch_params_with_wrapper_env(launch_params("low"), path, log_path);
    params["session"] = json!({
        "known_provider_session_id": "ses_caller_selected_create",
        "start_mode": "create"
    });
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

pub fn model_request_for(alias: &str, provider_model: &str, effort: &str) -> Value {
    json!({
        "name": alias,
        "provider_args": ["-m", provider_model, "--variant", effort],
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

pub fn oversized_prompt() -> String {
    format!(
        "  {}\n\"quoted\"  --share --attach --session -m trailing  ",
        vec!["prompt\u{1f642}chunk"; 12_000].join(" ")
    )
}

pub fn oversized_unbroken_prompt() -> String {
    "x".repeat(64 * 1024)
}

pub fn host_candidate_argv(effort: &str) -> Vec<&str> {
    host_candidate_argv_for_command("opencode1", effort)
}

pub fn host_candidate_argv_for_command<'a>(command: &'a str, effort: &'a str) -> Vec<&'a str> {
    host_candidate_argv_for_model(command, "openai/gpt-5.6-sol", effort)
}

pub fn host_candidate_argv_for_model<'a>(
    command: &'a str,
    provider_model: &'a str,
    effort: &'a str,
) -> Vec<&'a str> {
    let mut argv = vec![command];
    if command.rsplit('/').next() == Some("opencode") {
        argv.push("--pure");
    }
    argv.extend([
        "run",
        "--dangerously-skip-permissions",
        "-m",
        provider_model,
        "--variant",
        effort,
        "reply with the single word: ok",
    ]);
    argv
}

pub fn policy_effective_argv(effort: &str) -> Vec<&str> {
    vec![
        "opencode1",
        "run",
        "--format",
        "json",
        "--dangerously-skip-permissions",
        "-m",
        "openai/gpt-5.6-sol",
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
