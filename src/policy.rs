//! Declared roles: validator, mapper, formatter, parser, filter, predicate

use crate::account::profile_for_wrapper_reference;
use crate::envelope::{HostContext, ProviderFailure};
use crate::models::{model_alias, provider_args_match, ModelAlias};
use crate::runtime_selection::{resolve_runtime_selection, RuntimeSelection};
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

pub fn evaluate_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let params = parse_policy_params(params, request_id)?;
    evaluate(host, params, request_id)
}

pub fn evaluate(
    host: &HostContext,
    params: PolicyEvaluateParams,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    let selection = resolve_runtime_selection(host, &params.settings_id, request_id)?;
    let model = resolved_model(&params);
    let diagnostics = diagnostics_for_policy(&params, &selection, model);
    Ok(policy_result(&params, &selection, model, diagnostics))
}

fn policy_result(
    params: &PolicyEvaluateParams,
    selection: &RuntimeSelection,
    model: Option<&ModelAlias>,
    diagnostics: Vec<Value>,
) -> Value {
    let argv = effective_argv(params, model);
    json!({
        "accepted": policy_accepted(&diagnostics),
        "argv": argv,
        "env": effective_env(params.launch.env.as_ref()),
        "stdin": params.launch.stdin.clone(),
        "prompt": params.model.inputs.prompt.clone(),
        "diagnostics": diagnostics,
        "markers": policy_markers(configured_launch_command(params), params, selection, model),
    })
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

fn effective_argv(params: &PolicyEvaluateParams, model: Option<&ModelAlias>) -> Vec<String> {
    let Some(prefix) = configured_launch_prefix(params) else {
        return params.launch.argv.clone().unwrap_or_default();
    };
    let Some(model) = model else {
        return params.launch.argv.clone().unwrap_or_default();
    };
    let mut argv = prefix.to_vec();
    argv.extend(model.policy_effective_args());
    argv.extend(policy_launch_args(params, Some(model)));
    argv
}

fn configured_launch_command(params: &PolicyEvaluateParams) -> Option<&str> {
    configured_launch_prefix(params)
        .and_then(|prefix| prefix.first())
        .map(String::as_str)
}

fn configured_launch_prefix(params: &PolicyEvaluateParams) -> Option<&[String]> {
    let argv = params.launch.argv.as_deref()?;
    let run_index = argv.iter().position(|arg| arg == "run")?;
    let prefix = &argv[..run_index];
    valid_launch_command_prefix(prefix).then_some(prefix)
}

fn valid_launch_command_prefix(prefix: &[String]) -> bool {
    let Some((command, options)) = prefix.split_first() else {
        return false;
    };
    intrinsic_host_launch_command(command) && options.iter().all(|arg| arg == "--pure")
}

fn resolved_model(params: &PolicyEvaluateParams) -> Option<&'static ModelAlias> {
    model_alias(&params.model.name)
        .filter(|model| provider_args_match(model, &params.model.provider_args))
}

fn effective_env(input: Option<&BTreeMap<String, String>>) -> BTreeMap<String, String> {
    input.cloned().unwrap_or_default()
}

fn diagnostics_for_policy(
    params: &PolicyEvaluateParams,
    selection: &RuntimeSelection,
    model: Option<&ModelAlias>,
) -> Vec<Value> {
    let mut diagnostics = launch_command_diagnostics(params, selection);
    if model.is_none() {
        diagnostics.push(invalid_model_diagnostic(params));
    } else if selection.exact_model().is_some() && selection.exact_model() != model {
        diagnostics.push(settings_model_mismatch_diagnostic(selection, model));
    }
    if model.is_some_and(|model| !model.supports_account(selection.account)) {
        diagnostics.push(model_account_ineligible_diagnostic(selection, model));
    }
    diagnostics.extend(forbidden_argv_diagnostics(&policy_launch_args(
        params, model,
    )));
    diagnostics
}

fn model_account_ineligible_diagnostic(
    selection: &RuntimeSelection,
    requested: Option<&ModelAlias>,
) -> Value {
    diagnostic(
        "error",
        "model_account_ineligible",
        format!(
            "model {} is not eligible for account {} selected by settings record {} ({})",
            requested.map(|model| model.name).unwrap_or("<invalid>"),
            selection.account.opencode_wrapper,
            selection.settings_id,
            selection.evidence_label(),
        ),
    )
}

fn invalid_model_diagnostic(params: &PolicyEvaluateParams) -> Value {
    diagnostic(
        "error",
        "invalid_model",
        format!(
            "model {} must use the exact provider args advertised by discovery.models",
            params.model.name
        ),
    )
}

fn launch_command_diagnostics(
    params: &PolicyEvaluateParams,
    selection: &RuntimeSelection,
) -> Vec<Value> {
    let Some(command) = configured_launch_command(params) else {
        return vec![diagnostic(
            "error",
            "invalid_command",
            "launch argv must begin with a configured OpenCode command".to_string(),
        )];
    };
    if profile_for_wrapper_reference(command)
        .is_some_and(|account| account.opencode_wrapper == selection.account.opencode_wrapper)
    {
        return Vec::new();
    }
    vec![diagnostic(
        "error",
        "settings_command_mismatch",
        format!(
            "launch command must resolve to account {} selected by settings record {} ({})",
            selection.account.opencode_wrapper,
            selection.settings_id,
            selection.evidence_label(),
        ),
    )]
}

fn settings_model_mismatch_diagnostic(
    selection: &RuntimeSelection,
    requested: Option<&ModelAlias>,
) -> Value {
    diagnostic(
        "error",
        "settings_model_mismatch",
        format!(
            "model {} does not match the route stored by settings record {} ({})",
            requested.map(|model| model.name).unwrap_or("<invalid>"),
            selection.settings_id,
            selection.evidence_label(),
        ),
    )
}

fn forbidden_argv_diagnostics(input: &[String]) -> Vec<Value> {
    forbidden_launch_args(input)
        .into_iter()
        .map(forbidden_arg_diagnostic)
        .collect()
}

fn policy_launch_args(params: &PolicyEvaluateParams, model: Option<&ModelAlias>) -> Vec<String> {
    let argv = params.launch.argv.as_deref().unwrap_or_default();
    stripped_policy_launch_args(argv, model)
        .unwrap_or(argv)
        .to_vec()
}

fn stripped_policy_launch_args<'a>(
    argv: &'a [String],
    model: Option<&ModelAlias>,
) -> Option<&'a [String]> {
    let model = model?;
    strip_host_candidate_prefix(argv, model).or_else(|| strip_policy_effective_prefix(argv, model))
}

fn strip_host_candidate_prefix<'a>(argv: &'a [String], model: &ModelAlias) -> Option<&'a [String]> {
    strip_intrinsic_launch_prefix(argv, &host_candidate_args(model))
}

fn strip_policy_effective_prefix<'a>(
    argv: &'a [String],
    model: &ModelAlias,
) -> Option<&'a [String]> {
    strip_intrinsic_launch_prefix(argv, &policy_effective_args(model))
}

fn strip_intrinsic_launch_prefix<'a>(
    argv: &'a [String],
    args_after_command: &[String],
) -> Option<&'a [String]> {
    let run_index = argv.iter().position(|arg| arg == "run")?;
    if !valid_launch_command_prefix(&argv[..run_index])
        || !argv[run_index..].starts_with(args_after_command)
    {
        return None;
    }
    Some(&argv[run_index + args_after_command.len()..])
}

fn host_candidate_args(model: &ModelAlias) -> Vec<String> {
    model.host_candidate_args()
}

fn policy_effective_args(model: &ModelAlias) -> Vec<String> {
    model.policy_effective_args()
}

fn is_forbidden_launch_arg(arg: &str) -> bool {
    intrinsic_host_launch_command(arg) || matches!(arg, "--format" | "--variant" | "-m")
}

fn intrinsic_host_launch_command(command: &str) -> bool {
    profile_for_wrapper_reference(command).is_some()
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

fn policy_markers(
    command: Option<&str>,
    params: &PolicyEvaluateParams,
    selection: &RuntimeSelection,
    model: Option<&ModelAlias>,
) -> Vec<Value> {
    vec![
        json!({ "name": "opencode.command", "value": command.unwrap_or("") }),
        json!({ "name": "opencode.mode", "value": params.mode }),
        json!({ "name": "opencode.settings_record_id", "value": selection.settings_id }),
        json!({ "name": "opencode.settings_record_identity", "value": selection.evidence_label() }),
        json!({ "name": "opencode.account", "value": selection.account.opencode_wrapper }),
        json!({ "name": "opencode.account_hash", "value": selection.account.account_hash }),
        json!({ "name": "opencode.model_alias", "value": model.map(|model| model.name).unwrap_or("") }),
        json!({ "name": "opencode.provider_model", "value": model.map(|model| model.provider_model).unwrap_or("") }),
        json!({ "name": "opencode.effort", "value": model.map(|model| model.effort).unwrap_or("") }),
        json!({ "name": "opencode.attempted_provider_args", "value": params.model.provider_args }),
    ]
}

fn invalid_policy_params_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_policy_params",
        format!("policy.evaluate params are invalid: {err}"),
    )
}

fn forbidden_launch_args(input: &[String]) -> Vec<&String> {
    input
        .iter()
        .filter(|arg| is_forbidden_launch_arg(arg))
        .collect()
}

fn forbidden_arg_diagnostic(arg: &String) -> Value {
    diagnostic(
        "error",
        "forbidden_flag",
        format!("forbidden launch arg: {arg}"),
    )
}
