//! Declared roles: validator, mapper, formatter, parser, filter, predicate

use crate::account::profile_for_wrapper_reference;
use crate::activity::ActivityTargets;
use crate::envelope::{HostContext, ProviderFailure};
use crate::models::{model_alias, provider_args_match, ModelAlias};
use crate::runtime_selection::{resolve_runtime_selection, RuntimeSelection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct PolicyEvaluateWireParams {
    settings_id: String,
    mode: String,
    model: PolicyModelRequest,
    launch: PolicyLaunchInput,
}

#[derive(Clone, Deserialize)]
pub(crate) struct PolicyModelRequest {
    name: String,
    provider_args: Vec<String>,
    inputs: ModelInputs,
}

#[derive(Clone, Deserialize)]
struct ModelInputs {
    prompt: Option<String>,
    #[serde(rename = "named")]
    _named: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
struct PolicyLaunchInput {
    argv: Option<Vec<String>>,
    env: Option<BTreeMap<String, String>>,
    stdin: Option<String>,
    tool_restrictions: Option<Value>,
}

struct PolicyInput {
    settings_id: String,
    mode: String,
    model: PolicyModelRequest,
    launch: PolicyLaunchInput,
}

pub(crate) struct PolicyLaunchRequest {
    pub settings_id: String,
    pub mode: String,
    pub model: PolicyModelRequest,
    pub argv: Vec<String>,
    pub env: Option<BTreeMap<String, String>>,
    pub stdin: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct PolicyDiagnostic {
    severity: &'static str,
    code: &'static str,
    message: String,
}

#[derive(Serialize)]
pub(crate) struct PolicyMarker {
    name: &'static str,
    value: PolicyMarkerValue,
}

#[derive(Serialize)]
#[serde(untagged)]
enum PolicyMarkerValue {
    Text(String),
    Strings(Vec<String>),
}

pub(crate) struct PolicyLaunchPlan {
    pub argv: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub stdin: Option<String>,
    pub prompt: Option<String>,
    pub diagnostics: Vec<PolicyDiagnostic>,
    pub markers: Vec<PolicyMarker>,
    pub route: PolicyRouteIdentity,
}

pub(crate) struct PolicyRejection {
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    stdin: Option<String>,
    prompt: Option<String>,
    diagnostics: Vec<PolicyDiagnostic>,
    markers: Vec<PolicyMarker>,
    selection: PolicySelectionIdentity,
}

#[derive(Clone)]
pub(crate) struct PolicyRouteIdentity {
    pub settings_record_id: String,
    pub account_wrapper: String,
    pub model_alias: String,
    pub provider_id: String,
    pub model_id: String,
    pub effort: String,
}

struct PolicySelectionIdentity {
    settings_record_id: String,
    account_wrapper: String,
}

pub(crate) enum PolicyDecision {
    Accepted(PolicyLaunchPlan),
    Rejected(PolicyRejection),
}

struct PolicyPlanCandidate {
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    stdin: Option<String>,
    prompt: Option<String>,
    diagnostics: Vec<PolicyDiagnostic>,
    markers: Vec<PolicyMarker>,
}

#[derive(Serialize)]
struct PolicyEvaluateWireResult {
    accepted: bool,
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    stdin: Option<String>,
    prompt: Option<String>,
    diagnostics: Vec<PolicyDiagnostic>,
    markers: Vec<PolicyMarker>,
}

pub fn evaluate_params(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<Value, ProviderFailure> {
    evaluate_params_with_activity(host, params, request_id).map(|(result, _)| result)
}

pub(crate) fn evaluate_params_with_activity(
    host: &HostContext,
    params: Value,
    request_id: &str,
) -> Result<(Value, ActivityTargets), ProviderFailure> {
    let wire = parse_policy_params(params, request_id)?;
    let decision = evaluate(host, wire.into(), request_id)?;
    let targets = decision.activity_targets();
    Ok((project_policy_result(decision), targets))
}

pub(crate) fn attempted_activity_targets(params: &Value) -> ActivityTargets {
    let mut targets = ActivityTargets::default();
    let settings_id = params
        .get("settings_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if let Some(settings_id) = settings_id {
        targets.attempted("settings_record", settings_id, "params.settings_id");
    }
    if let Some(name) = params
        .pointer("/model/name")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        targets.attempted("model_alias", name, "params.model.name");
    }
    if let Some(provider_args) = params.pointer("/model/provider_args") {
        targets.provider_args(provider_args);
    }
    targets
}

pub(crate) fn evaluate_launch(
    host: &HostContext,
    request: PolicyLaunchRequest,
    request_id: &str,
) -> Result<PolicyDecision, ProviderFailure> {
    evaluate(
        host,
        PolicyInput {
            settings_id: request.settings_id,
            mode: request.mode,
            model: request.model,
            launch: PolicyLaunchInput {
                argv: Some(request.argv),
                env: request.env,
                stdin: request.stdin,
                tool_restrictions: None,
            },
        },
        request_id,
    )
}

fn evaluate(
    host: &HostContext,
    params: PolicyInput,
    request_id: &str,
) -> Result<PolicyDecision, ProviderFailure> {
    let selection = resolve_runtime_selection(host, &params.settings_id, request_id)?;
    let model = resolved_model(&params);
    let diagnostics = diagnostics_for_policy(&params, &selection, model);
    let plan = policy_plan_candidate(&params, &selection, model, diagnostics);
    match model {
        Some(model) if policy_accepted(&plan.diagnostics) => Ok(PolicyDecision::Accepted(
            plan.into_accepted(&selection, model),
        )),
        _ => Ok(PolicyDecision::Rejected(plan.into_rejection(&selection))),
    }
}

fn policy_plan_candidate(
    params: &PolicyInput,
    selection: &RuntimeSelection,
    model: Option<&ModelAlias>,
    diagnostics: Vec<PolicyDiagnostic>,
) -> PolicyPlanCandidate {
    PolicyPlanCandidate {
        argv: effective_argv(params, model),
        env: effective_env(params.launch.env.as_ref()),
        stdin: params.launch.stdin.clone(),
        prompt: params.model.inputs.prompt.clone(),
        diagnostics,
        markers: policy_markers(configured_launch_command(params), params, selection, model),
    }
}

fn project_policy_result(decision: PolicyDecision) -> Value {
    json!(PolicyEvaluateWireResult::from(decision))
}

fn policy_accepted(diagnostics: &[PolicyDiagnostic]) -> bool {
    !diagnostics.iter().any(PolicyDiagnostic::is_error)
}

fn parse_policy_params(
    params: Value,
    request_id: &str,
) -> Result<PolicyEvaluateWireParams, ProviderFailure> {
    serde_json::from_value(params).map_err(|err| invalid_policy_params_failure(request_id, err))
}

impl From<PolicyEvaluateWireParams> for PolicyInput {
    fn from(wire: PolicyEvaluateWireParams) -> Self {
        Self {
            settings_id: wire.settings_id,
            mode: wire.mode,
            model: wire.model,
            launch: wire.launch,
        }
    }
}

impl PolicyDiagnostic {
    fn is_error(&self) -> bool {
        self.severity == "error"
    }
}

impl PolicyPlanCandidate {
    fn into_accepted(self, selection: &RuntimeSelection, model: &ModelAlias) -> PolicyLaunchPlan {
        let route = resolved_route_identity(selection, model);
        PolicyLaunchPlan {
            argv: self.argv,
            env: self.env,
            stdin: self.stdin,
            prompt: self.prompt,
            diagnostics: self.diagnostics,
            markers: self.markers,
            route,
        }
    }

    fn into_rejection(self, selection: &RuntimeSelection) -> PolicyRejection {
        PolicyRejection {
            argv: self.argv,
            env: self.env,
            stdin: self.stdin,
            prompt: self.prompt,
            diagnostics: self.diagnostics,
            markers: self.markers,
            selection: PolicySelectionIdentity {
                settings_record_id: selection.settings_id.clone(),
                account_wrapper: selection.account.opencode_wrapper.to_string(),
            },
        }
    }
}

impl PolicyDecision {
    fn activity_targets(&self) -> ActivityTargets {
        let mut targets = ActivityTargets::default();
        match self {
            Self::Accepted(plan) => append_route_activity_targets(&mut targets, &plan.route),
            Self::Rejected(plan) => {
                targets.resolved(
                    "settings_record",
                    plan.selection.settings_record_id.clone(),
                    "policy.selection.settings_record",
                );
                targets.resolved(
                    "account",
                    plan.selection.account_wrapper.clone(),
                    "policy.selection.account",
                );
            }
        }
        targets
    }
}

pub(crate) fn append_route_activity_targets(
    targets: &mut ActivityTargets,
    route: &PolicyRouteIdentity,
) {
    targets.resolved(
        "settings_record",
        route.settings_record_id.clone(),
        "policy.route.settings_record",
    );
    targets.resolved(
        "account",
        route.account_wrapper.clone(),
        "policy.route.account",
    );
    targets.resolved(
        "provider_model",
        format!("{}/{}", route.provider_id, route.model_id),
        "policy.route.provider_model",
    );
    targets.resolved(
        "model_alias",
        route.model_alias.clone(),
        "policy.route.model_alias",
    );
    targets.resolved("effort", route.effort.clone(), "policy.route.effort");
}

impl PolicyRejection {
    pub(crate) fn diagnostics_json(&self) -> Value {
        json!(self.diagnostics)
    }
}

impl From<PolicyDecision> for PolicyEvaluateWireResult {
    fn from(decision: PolicyDecision) -> Self {
        match decision {
            PolicyDecision::Accepted(plan) => Self {
                accepted: true,
                argv: plan.argv,
                env: plan.env,
                stdin: plan.stdin,
                prompt: plan.prompt,
                diagnostics: plan.diagnostics,
                markers: plan.markers,
            },
            PolicyDecision::Rejected(plan) => Self {
                accepted: false,
                argv: plan.argv,
                env: plan.env,
                stdin: plan.stdin,
                prompt: plan.prompt,
                diagnostics: plan.diagnostics,
                markers: plan.markers,
            },
        }
    }
}

fn resolved_route_identity(
    selection: &RuntimeSelection,
    model: &ModelAlias,
) -> PolicyRouteIdentity {
    let (provider_id, model_id) = model
        .provider_model
        .split_once('/')
        .expect("catalog provider model includes provider and model ids");
    PolicyRouteIdentity {
        settings_record_id: selection.settings_id.clone(),
        account_wrapper: selection.account.opencode_wrapper.to_string(),
        model_alias: model.name.to_string(),
        provider_id: provider_id.to_string(),
        model_id: model_id.to_string(),
        effort: model.effort.to_string(),
    }
}

fn effective_argv(params: &PolicyInput, model: Option<&ModelAlias>) -> Vec<String> {
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

fn configured_launch_command(params: &PolicyInput) -> Option<&str> {
    configured_launch_prefix(params)
        .and_then(|prefix| prefix.first())
        .map(String::as_str)
}

fn configured_launch_prefix(params: &PolicyInput) -> Option<&[String]> {
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

fn resolved_model(params: &PolicyInput) -> Option<&'static ModelAlias> {
    model_alias(&params.model.name)
        .filter(|model| provider_args_match(model, &params.model.provider_args))
}

fn effective_env(input: Option<&BTreeMap<String, String>>) -> BTreeMap<String, String> {
    input.cloned().unwrap_or_default()
}

fn diagnostics_for_policy(
    params: &PolicyInput,
    selection: &RuntimeSelection,
    model: Option<&ModelAlias>,
) -> Vec<PolicyDiagnostic> {
    let mut diagnostics = launch_command_diagnostics(params, selection);
    if model.is_none() {
        diagnostics.push(invalid_model_diagnostic(params));
    } else if selection.exact_model().is_some() && selection.exact_model() != model {
        diagnostics.push(settings_model_mismatch_diagnostic(selection, model));
    }
    if model.is_some_and(|model| !model.supports_account(selection.account)) {
        diagnostics.push(model_account_ineligible_diagnostic(selection, model));
    }
    if params.launch.tool_restrictions.is_some() {
        diagnostics.push(unsupported_tool_restrictions_diagnostic());
    }
    diagnostics.extend(forbidden_argv_diagnostics(&policy_launch_args(
        params, model,
    )));
    diagnostics
}

fn unsupported_tool_restrictions_diagnostic() -> PolicyDiagnostic {
    diagnostic(
        "error",
        "unsupported_tool_restrictions",
        "OpenCode cannot faithfully enforce the configured tool_restrictions; refusing unrestricted launch"
            .to_string(),
    )
}

fn model_account_ineligible_diagnostic(
    selection: &RuntimeSelection,
    requested: Option<&ModelAlias>,
) -> PolicyDiagnostic {
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

fn invalid_model_diagnostic(params: &PolicyInput) -> PolicyDiagnostic {
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
    params: &PolicyInput,
    selection: &RuntimeSelection,
) -> Vec<PolicyDiagnostic> {
    let Some(command) = configured_launch_command(params) else {
        return vec![diagnostic(
            "error",
            "invalid_command",
            format!(
                "launch argv for account {} must begin with its exact canonical OpenCode wrapper",
                selection.account.opencode_wrapper
            ),
        )];
    };
    if command == selection.account.opencode_wrapper {
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
) -> PolicyDiagnostic {
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

fn forbidden_argv_diagnostics(input: &[String]) -> Vec<PolicyDiagnostic> {
    forbidden_launch_args(input)
        .into_iter()
        .map(forbidden_arg_diagnostic)
        .collect()
}

fn policy_launch_args(params: &PolicyInput, model: Option<&ModelAlias>) -> Vec<String> {
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
    profile_for_wrapper_reference(command)
        .is_some_and(|account| command == account.opencode_wrapper)
}

fn diagnostic(severity: &'static str, code: &'static str, message: String) -> PolicyDiagnostic {
    PolicyDiagnostic {
        severity,
        code,
        message,
    }
}

fn policy_markers(
    command: Option<&str>,
    params: &PolicyInput,
    selection: &RuntimeSelection,
    model: Option<&ModelAlias>,
) -> Vec<PolicyMarker> {
    vec![
        marker("opencode.command", command.unwrap_or("")),
        marker("opencode.mode", &params.mode),
        marker("opencode.settings_record_id", &selection.settings_id),
        marker(
            "opencode.settings_record_identity",
            &selection.evidence_label(),
        ),
        marker("opencode.account", selection.account.opencode_wrapper),
        marker("opencode.account_hash", selection.account.account_hash),
        marker(
            "opencode.model_alias",
            model.map(|model| model.name).unwrap_or(""),
        ),
        marker(
            "opencode.provider_model",
            model.map(|model| model.provider_model).unwrap_or(""),
        ),
        marker(
            "opencode.effort",
            model.map(|model| model.effort).unwrap_or(""),
        ),
        string_list_marker(
            "opencode.attempted_provider_args",
            params.model.provider_args.clone(),
        ),
    ]
}

fn marker(name: &'static str, value: &str) -> PolicyMarker {
    PolicyMarker {
        name,
        value: PolicyMarkerValue::Text(value.to_string()),
    }
}

fn string_list_marker(name: &'static str, value: Vec<String>) -> PolicyMarker {
    PolicyMarker {
        name,
        value: PolicyMarkerValue::Strings(value),
    }
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

fn forbidden_arg_diagnostic(arg: &String) -> PolicyDiagnostic {
    diagnostic(
        "error",
        "forbidden_flag",
        format!("forbidden launch arg: {arg}"),
    )
}
