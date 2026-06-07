//! Declared roles: validator, mapper, formatter, parser, filter, predicate

use crate::account::profile_for_settings_id;
use crate::envelope::ProviderFailure;
use crate::models::PROVIDER_MODEL;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;

const HOST_LAUNCH_COMMAND_BASENAMES: &[&str] = &[
    "opencode",
    "opencode1",
    "opencode2",
    "opencode3",
    "opencode4",
    "opencode5",
];

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
    let account = policy_account(&params.settings_id, request_id)?;
    let diagnostics = diagnostics_for_policy(&params);
    Ok(policy_result(
        account.opencode_wrapper,
        &params,
        diagnostics,
    ))
}

fn policy_result(wrapper: &str, params: &PolicyEvaluateParams, diagnostics: Vec<Value>) -> Value {
    json!({
        "accepted": policy_accepted(&diagnostics),
        "argv": effective_argv(wrapper, params),
        "env": effective_env(params.launch.env.as_ref()),
        "stdin": params.launch.stdin.clone(),
        "prompt": params.model.inputs.prompt.clone(),
        "diagnostics": diagnostics,
        "markers": policy_markers(wrapper, params),
    })
}

fn policy_account(
    settings_id: &str,
    request_id: &str,
) -> Result<&'static crate::account::AccountProfile, ProviderFailure> {
    profile_for_settings_id(settings_id)
        .ok_or_else(|| unknown_settings_id_failure(request_id, settings_id))
}

fn unknown_settings_id_failure(request_id: &str, settings_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "unknown_settings_id",
        format!("unknown opencode settings_id: {settings_id}"),
    )
}

fn policy_accepted(diagnostics: &[Value]) -> bool {
    !diagnostics.iter().any(is_error_diagnostic)
}

fn parse_policy_params(
    params: Value,
    request_id: &str,
) -> Result<PolicyEvaluateParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_policy_params_failure(request_id, err))
}

fn effective_argv(wrapper: &str, params: &PolicyEvaluateParams) -> Vec<String> {
    let mut argv = vec![
        wrapper.to_string(),
        "run".to_string(),
        "--format".to_string(),
        "json".to_string(),
        "--dangerously-skip-permissions".to_string(),
        "-m".to_string(),
        PROVIDER_MODEL.to_string(),
        "--variant".to_string(),
        model_effort(params).to_string(),
    ];
    argv.extend(policy_launch_args(params));
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
    allowed_env_entries(input)
        .into_iter()
        .map(env_entry)
        .collect()
}

fn diagnostics_for_policy(params: &PolicyEvaluateParams) -> Vec<Value> {
    let mut diagnostics = forbidden_env_diagnostics(params.launch.env.as_ref());
    diagnostics.extend(forbidden_argv_diagnostics(&policy_launch_args(params)));
    diagnostics
}

fn forbidden_env_diagnostics(input: Option<&BTreeMap<String, String>>) -> Vec<Value> {
    forbidden_env_keys(input)
        .into_iter()
        .map(forbidden_env_diagnostic)
        .collect()
}

fn forbidden_argv_diagnostics(input: &[String]) -> Vec<Value> {
    forbidden_launch_args(input)
        .into_iter()
        .map(forbidden_arg_diagnostic)
        .collect()
}

fn policy_launch_args(params: &PolicyEvaluateParams) -> Vec<String> {
    let argv = params.launch.argv.as_deref().unwrap_or_default();
    stripped_policy_launch_args(params, argv)
        .unwrap_or(argv)
        .to_vec()
}

fn stripped_policy_launch_args<'a>(
    params: &PolicyEvaluateParams,
    argv: &'a [String],
) -> Option<&'a [String]> {
    let effort = model_effort(params);
    strip_host_candidate_prefix(argv, effort)
        .or_else(|| strip_policy_effective_prefix(argv, effort))
}

fn strip_host_candidate_prefix<'a>(argv: &'a [String], effort: &str) -> Option<&'a [String]> {
    strip_intrinsic_launch_prefix(argv, &host_candidate_args(effort))
}

fn strip_policy_effective_prefix<'a>(argv: &'a [String], effort: &str) -> Option<&'a [String]> {
    strip_intrinsic_launch_prefix(argv, &policy_effective_args(effort))
}

fn strip_intrinsic_launch_prefix<'a>(
    argv: &'a [String],
    args_after_command: &[String],
) -> Option<&'a [String]> {
    let (command, args) = argv.split_first()?;
    if !intrinsic_host_launch_command(command) || !args.starts_with(args_after_command) {
        return None;
    }
    Some(&args[args_after_command.len()..])
}

fn host_candidate_args(effort: &str) -> Vec<String> {
    vec![
        "run".to_string(),
        "--dangerously-skip-permissions".to_string(),
        "-m".to_string(),
        PROVIDER_MODEL.to_string(),
        "--variant".to_string(),
        effort.to_string(),
    ]
}

fn policy_effective_args(effort: &str) -> Vec<String> {
    vec![
        "run".to_string(),
        "--format".to_string(),
        "json".to_string(),
        "--dangerously-skip-permissions".to_string(),
        "-m".to_string(),
        PROVIDER_MODEL.to_string(),
        "--variant".to_string(),
        effort.to_string(),
    ]
}

pub(crate) fn is_forbidden_env_key(key: &str) -> bool {
    key.starts_with("OPENAI_API_KEY") || key.starts_with("OPENAI_BASE_URL")
}

fn is_forbidden_launch_arg(arg: &str) -> bool {
    intrinsic_host_launch_command(arg) || matches!(arg, "--format" | "--variant" | "-m")
}

fn intrinsic_host_launch_command(command: &str) -> bool {
    HOST_LAUNCH_COMMAND_BASENAMES.contains(&command_basename(command))
}

fn command_basename(command: &str) -> &str {
    Path::new(command)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(command)
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

fn invalid_policy_params_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_policy_params",
        format!("policy.evaluate params are invalid: {err}"),
    )
}

fn allowed_env_entries(input: Option<&BTreeMap<String, String>>) -> Vec<(&String, &String)> {
    input
        .into_iter()
        .flat_map(BTreeMap::iter)
        .filter(|(key, _)| !is_forbidden_env_key(key))
        .collect()
}

fn env_entry((key, value): (&String, &String)) -> (String, String) {
    (key.clone(), value.clone())
}

fn forbidden_env_keys(input: Option<&BTreeMap<String, String>>) -> Vec<&String> {
    input
        .into_iter()
        .flat_map(BTreeMap::keys)
        .filter(|key| is_forbidden_env_key(key))
        .collect()
}

fn forbidden_launch_args(input: &[String]) -> Vec<&String> {
    input
        .iter()
        .filter(|arg| is_forbidden_launch_arg(arg))
        .collect()
}

fn forbidden_env_diagnostic(key: &String) -> Value {
    diagnostic(
        "error",
        "forbidden_env",
        format!("forbidden env key omitted: {key}"),
    )
}

fn forbidden_arg_diagnostic(arg: &String) -> Value {
    diagnostic(
        "error",
        "forbidden_flag",
        format!("forbidden launch arg: {arg}"),
    )
}
