//! Declared roles: validator, mapper, formatter, parser

use crate::account::profile_for_settings_id;
use crate::envelope::ProviderFailure;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Deserialize)]
pub struct PolicyEvaluateParams {
    settings_id: String,
    mode: String,
    model: ProviderModelRequest,
    launch: PolicyLaunchParams,
}

#[derive(Deserialize)]
struct ProviderModelRequest {
    name: String,
    provider_args: Vec<String>,
    inputs: ModelInputs,
}

#[derive(Deserialize)]
struct ModelInputs {
    prompt: Option<String>,
    #[serde(rename = "named")]
    _named: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
struct PolicyLaunchParams {
    argv: Option<Vec<String>>,
    env: Option<BTreeMap<String, String>>,
    stdin: Option<String>,
}

pub fn evaluate_params(params: Value, request_id: &str) -> Result<Value, ProviderFailure> {
    let params = parse_policy_params(params, request_id)?;
    evaluate(params, request_id)
}

pub fn evaluate(params: PolicyEvaluateParams, request_id: &str) -> Result<Value, ProviderFailure> {
    let account = profile_for_settings_id(&params.settings_id).ok_or_else(|| {
        ProviderFailure::invalid_request(
            request_id,
            "unknown_settings_id",
            format!("unknown opencode settings_id: {}", params.settings_id),
        )
    })?;
    let diagnostics = diagnostics_for_policy(&params);
    let accepted = !diagnostics.iter().any(is_error_diagnostic);
    Ok(json!({
        "accepted": accepted,
        "argv": effective_argv(account.opencode_wrapper, &params),
        "env": effective_env(params.launch.env.as_ref()),
        "stdin": params.launch.stdin,
        "prompt": params.model.inputs.prompt,
        "diagnostics": diagnostics,
        "markers": policy_markers(account.opencode_wrapper, &params),
    }))
}

fn parse_policy_params(
    params: Value,
    request_id: &str,
) -> Result<PolicyEvaluateParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_policy_params",
            format!("policy.evaluate params are invalid: {err}"),
        )
    })
}

fn effective_argv(wrapper: &str, params: &PolicyEvaluateParams) -> Vec<String> {
    let mut argv = vec![
        wrapper.to_string(),
        "run".to_string(),
        "--format".to_string(),
        "json".to_string(),
        "--dangerously-skip-permissions".to_string(),
        "-m".to_string(),
        "openai/gpt-5.5".to_string(),
        "--variant".to_string(),
        model_effort(params).to_string(),
    ];
    if let Some(prompt_args) = params.launch.argv.as_ref() {
        argv.extend(prompt_args.iter().cloned());
    }
    argv
}

fn model_effort(params: &PolicyEvaluateParams) -> &str {
    provider_arg_after(&params.model.provider_args, "--variant")
        .or_else(|| effort_from_model_name(&params.model.name))
        .unwrap_or("medium")
}

fn provider_arg_after<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].as_str())
}

fn effort_from_model_name(name: &str) -> Option<&str> {
    name.strip_prefix("gpt-")
        .filter(|effort| !effort.is_empty())
}

fn effective_env(input: Option<&BTreeMap<String, String>>) -> BTreeMap<String, String> {
    input
        .into_iter()
        .flat_map(BTreeMap::iter)
        .filter(|(key, _)| !is_forbidden_env_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn diagnostics_for_policy(params: &PolicyEvaluateParams) -> Vec<Value> {
    let mut diagnostics = forbidden_env_diagnostics(params.launch.env.as_ref());
    diagnostics.extend(forbidden_argv_diagnostics(params.launch.argv.as_deref()));
    diagnostics
}

fn forbidden_env_diagnostics(input: Option<&BTreeMap<String, String>>) -> Vec<Value> {
    input
        .into_iter()
        .flat_map(BTreeMap::keys)
        .filter(|key| is_forbidden_env_key(key))
        .map(|key| {
            diagnostic(
                "error",
                "forbidden_env",
                format!("forbidden env key omitted: {key}"),
            )
        })
        .collect()
}

fn forbidden_argv_diagnostics(input: Option<&[String]>) -> Vec<Value> {
    input
        .into_iter()
        .flatten()
        .filter(|arg| is_forbidden_launch_arg(arg))
        .map(|arg| {
            diagnostic(
                "error",
                "forbidden_flag",
                format!("forbidden launch arg: {arg}"),
            )
        })
        .collect()
}

pub(crate) fn is_forbidden_env_key(key: &str) -> bool {
    key.starts_with("OPENAI_API_KEY") || key.starts_with("OPENAI_BASE_URL")
}

fn is_forbidden_launch_arg(arg: &str) -> bool {
    matches!(
        arg,
        "opencode"
            | "opencode1"
            | "opencode2"
            | "opencode3"
            | "opencode4"
            | "opencode5"
            | "--format"
            | "--variant"
            | "-m"
    )
}

fn diagnostic(severity: &str, code: &str, message: String) -> Value {
    json!({
        "severity": severity,
        "code": code,
        "message": message,
    })
}

fn is_error_diagnostic(diagnostic: &Value) -> bool {
    diagnostic.get("severity").and_then(Value::as_str) == Some("error")
}

fn policy_markers(wrapper: &str, params: &PolicyEvaluateParams) -> Vec<Value> {
    vec![
        json!({ "name": "opencode.wrapper", "value": wrapper }),
        json!({ "name": "opencode.mode", "value": params.mode }),
    ]
}
