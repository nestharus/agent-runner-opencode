// declared_role: formatter, mapper
#![allow(unused_imports)]

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

pub fn launch_params_with_policy_effective_argv(effort: &str) -> Value {
    let mut params = launch_params(effort);
    params["argv"] = json!(policy_effective_argv(effort));
    params
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
