//! Declared roles: formatter, orchestration, parser, predicate, validator
//! intrinsic_surface_declarations:
//!   - component: src/dispatch.rs
//!     role: intrinsic-surface
//!     Domain: provider subcommand routing
//!     Owns:
//!       - provider subcommand router and dispatch table
//!       - per-capability handler invocation
//!       - request/response envelope decode-encode

use crate::activity::{ActivityContext, ActivityTargets};
use crate::discovery;
use crate::encoding::canonical_json_bytes;
use crate::envelope::{
    failure_response, success_response, ProviderFailure, RequestEnvelope, CONTRACT,
};
use crate::schema::{describe_result, schema_result_params};
use crate::{launch, migration, policy, quota, rotation, session, settings, setup, terminal};
use serde_json::Value;
use std::io::Write;

#[derive(Clone, Copy)]
struct Route<'a> {
    external_name: &'a str,
    operation: Operation,
}

#[derive(Clone, Copy)]
enum Operation {
    Describe,
    Schema,
    DiscoveryModels,
    DiscoveryAccounts,
    Launch,
    PolicyEvaluate,
    TerminalClassify,
    Session(session::Command),
    Quota(quota::Command),
    Settings(settings::Command),
    Setup(setup::Command),
    RotationAssess,
    RotationMaterialize,
    MigrationPlan,
    MigrationApply,
    Unknown,
}

struct EnvelopedOutcome {
    result: Value,
    activity_targets: ActivityTargets,
}

impl EnvelopedOutcome {
    fn new(result: Value) -> Self {
        Self {
            result,
            activity_targets: ActivityTargets::default(),
        }
    }

    fn with_activity(result: Value, activity_targets: ActivityTargets) -> Self {
        Self {
            result,
            activity_targets,
        }
    }
}

impl<'a> Route<'a> {
    fn resolve(external_name: &'a str) -> Self {
        let operation = match external_name {
            "describe" => Operation::Describe,
            "schema" => Operation::Schema,
            "discovery.models" => Operation::DiscoveryModels,
            "discovery.accounts" => Operation::DiscoveryAccounts,
            "launch" => Operation::Launch,
            "policy.evaluate" => Operation::PolicyEvaluate,
            "terminal.classify" => Operation::TerminalClassify,
            "session.locate_transcript" => Operation::Session(session::Command::LocateTranscript),
            "session.read_turns" => Operation::Session(session::Command::ReadTurns),
            "session.capture" => Operation::Session(session::Command::Capture),
            "session.enumerate" => Operation::Session(session::Command::Enumerate),
            "session.export" => Operation::Session(session::Command::Export),
            "session.replace" => Operation::Session(session::Command::Replace),
            "quota.source" => Operation::Quota(quota::Command::Source),
            "quota.probe" => Operation::Quota(quota::Command::Probe),
            "quota.refresh_auth" => Operation::Quota(quota::Command::RefreshAuth),
            "settings.list" => Operation::Settings(settings::Command::List),
            "settings.get" => Operation::Settings(settings::Command::Get),
            "settings.create" => Operation::Settings(settings::Command::Create),
            "settings.update" => Operation::Settings(settings::Command::Update),
            "settings.delete" => Operation::Settings(settings::Command::Delete),
            "settings.validate" => Operation::Settings(settings::Command::Validate),
            "settings.migrate" => Operation::Settings(settings::Command::Migrate),
            "setup.detect" => Operation::Setup(setup::Command::Detect),
            "setup.install_plan" => Operation::Setup(setup::Command::InstallPlan),
            "setup.sync_plan" => Operation::Setup(setup::Command::SyncPlan),
            "setup_brain.turn" => Operation::Setup(setup::Command::BrainTurn),
            "rotation.assess" => Operation::RotationAssess,
            "rotation.materialize" => Operation::RotationMaterialize,
            "migration.plan" => Operation::MigrationPlan,
            "migration.apply" => Operation::MigrationApply,
            _ => Operation::Unknown,
        };
        Self {
            external_name,
            operation,
        }
    }

    fn write<W: Write>(
        self,
        request: RequestEnvelope,
        writer: &mut W,
    ) -> Result<i32, ProviderFailure> {
        match self.operation {
            Operation::Describe => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                no_activity_targets,
                |request| {
                    validate_empty_params(
                        &request.params,
                        &request.request_id,
                        "invalid_describe_params",
                    )?;
                    Ok(EnvelopedOutcome::new(describe_result()))
                },
            ),
            Operation::Schema => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                no_activity_targets,
                |request| {
                    schema_result_params(request.params, &request.request_id)
                        .map(EnvelopedOutcome::new)
                },
            ),
            Operation::DiscoveryModels => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                no_activity_targets,
                |_| Ok(EnvelopedOutcome::new(discovery::models())),
            ),
            Operation::DiscoveryAccounts => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                no_activity_targets,
                |_| Ok(EnvelopedOutcome::new(discovery::accounts())),
            ),
            Operation::Launch => write_streaming_launch(self.external_name, request, writer),
            Operation::PolicyEvaluate => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                |request, _| policy::attempted_activity_targets(&request.params),
                |request| {
                    policy::evaluate_params_with_activity(
                        &request.host,
                        request.params,
                        &request.request_id,
                    )
                    .map(|(result, targets)| EnvelopedOutcome::with_activity(result, targets))
                },
            ),
            Operation::TerminalClassify => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                no_activity_targets,
                |request| {
                    terminal::classify_params(request.params, &request.request_id)
                        .map(EnvelopedOutcome::new)
                },
            ),
            Operation::Session(command) => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                move |request, result| {
                    session::activity_targets(
                        command,
                        &request.host,
                        &request.params,
                        result,
                        &request.request_id,
                    )
                },
                move |request| session::handle(command, request).map(EnvelopedOutcome::new),
            ),
            Operation::Quota(command) => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                |request, result| {
                    quota::activity_targets(
                        &request.host,
                        &request.params,
                        result,
                        &request.request_id,
                    )
                },
                move |request| quota::handle(command, request).map(EnvelopedOutcome::new),
            ),
            Operation::Settings(command) => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                move |request, result| settings::activity_targets(command, &request.params, result),
                move |request| settings::handle(command, request).map(EnvelopedOutcome::new),
            ),
            Operation::Setup(command) => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                no_activity_targets,
                move |request| setup::handle(command, request).map(EnvelopedOutcome::new),
            ),
            Operation::RotationAssess => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                |request, result| rotation::activity_targets(&request.params, result),
                |request| {
                    rotation::assess_params(
                        &request.host,
                        request.params,
                        &request.request_id,
                        request.provider_instance_id.as_deref().unwrap_or(""),
                    )
                    .map(EnvelopedOutcome::new)
                },
            ),
            Operation::RotationMaterialize => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                |request, result| rotation::activity_targets(&request.params, result),
                |request| {
                    rotation::materialize_params(
                        &request.host,
                        request.params,
                        &request.request_id,
                        request.provider_instance_id.as_deref().unwrap_or(""),
                    )
                    .map(EnvelopedOutcome::new)
                },
            ),
            Operation::MigrationPlan => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                |request, result| migration::activity_targets(&request.params, result),
                |request| {
                    migration::plan_params(request.params, &request.request_id)
                        .map(EnvelopedOutcome::new)
                },
            ),
            Operation::MigrationApply => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                |request, result| migration::activity_targets(&request.params, result),
                |request| {
                    migration::apply_params(&request.host, request.params, &request.request_id)
                        .map(EnvelopedOutcome::new)
                },
            ),
            Operation::Unknown => write_enveloped_operation(
                self.external_name,
                request,
                writer,
                no_activity_targets,
                |request| {
                    Err(unknown_subcommand_failure(
                        request.request_id,
                        self.external_name,
                    ))
                },
            ),
        }
    }
}

pub fn handle_invocation(args: &[String], stdin: &[u8]) -> (Vec<u8>, i32) {
    let mut stdout = Vec::new();
    let exit_code = write_invocation(args, stdin, &mut stdout);
    (stdout, exit_code)
}

pub fn write_invocation<W: Write>(args: &[String], stdin: &[u8], writer: &mut W) -> i32 {
    match write_invocation_result(args, stdin, writer) {
        Ok(exit_code) => exit_code,
        Err(failure) => write_failure_output(writer, failure),
    }
}

pub fn subcommand_from_args<'a>(
    args: &'a [String],
    request_id: &str,
) -> Result<&'a str, ProviderFailure> {
    match args {
        [_, subcommand] => Ok(subcommand.as_str()),
        [_] => Err(missing_subcommand_failure(request_id)),
        _ => Err(invalid_argv_failure(request_id)),
    }
}

pub fn decode_request(stdin: &[u8]) -> Result<RequestEnvelope, ProviderFailure> {
    let raw = parse_raw_request(stdin).map_err(invalid_json_failure)?;
    let request_id = fallback_request_id(request_id_from_raw(&raw));
    validate_params_present(&raw, &request_id)?;
    let request = parse_request_envelope(raw, &request_id)?;
    validate_request_envelope(request)
}

fn write_enveloped_operation<W: Write, P, H>(
    external_name: &str,
    request: RequestEnvelope,
    writer: &mut W,
    project_activity: P,
    handle: H,
) -> Result<i32, ProviderFailure>
where
    P: Fn(&RequestEnvelope, Option<&Value>) -> ActivityTargets,
    H: FnOnce(RequestEnvelope) -> Result<EnvelopedOutcome, ProviderFailure>,
{
    let activity = ActivityContext::from_request(&request, external_name);
    let attempted_targets = project_activity(&request, None);
    if let Err(error) = activity.started(&attempted_targets) {
        eprintln!("provider activity start evidence warning: {error:?}");
    }
    let request_id = request.request_id.clone();
    let request_snapshot = request.clone();
    let outcome = match handle(request) {
        Ok(outcome) => {
            let mut completed_targets = project_activity(&request_snapshot, Some(&outcome.result));
            completed_targets.extend(outcome.activity_targets.clone());
            if let Err(error) = activity.succeeded(0, &completed_targets) {
                eprintln!("provider activity completion evidence warning: {error:?}");
            }
            outcome
        }
        Err(failure) => {
            if let Err(error) = activity.failed(&failure, &attempted_targets) {
                eprintln!("provider activity completion evidence warning: {error:?}");
            }
            return Err(failure);
        }
    };
    let response = success_response(&request_id, outcome.result);
    writer
        .write_all(&canonical_json_bytes(&response))
        .map_err(stdout_write_failure)?;
    Ok(0)
}

fn write_streaming_launch<W: Write>(
    external_name: &str,
    request: RequestEnvelope,
    writer: &mut W,
) -> Result<i32, ProviderFailure> {
    let activity = ActivityContext::from_request(&request, external_name);
    let attempted_targets = launch::attempted_activity_targets(&request.params);
    if let Err(error) = activity.started(&attempted_targets) {
        eprintln!("provider activity start evidence warning: {error:?}");
    }
    let result = launch::stream(&request.request_id, &request.host, request.params, writer);
    match &result {
        Ok(outcome) => {
            let mut completed_targets = attempted_targets.clone();
            completed_targets.extend(outcome.activity_targets.clone());
            if let Err(error) = activity.succeeded(outcome.exit_code, &completed_targets) {
                eprintln!("provider activity completion evidence warning: {error:?}");
            }
        }
        Err(failure) => {
            if let Err(error) = activity.failed(failure, &attempted_targets) {
                eprintln!("provider activity completion evidence warning: {error:?}");
            }
        }
    }
    result.map(|outcome| outcome.exit_code)
}

fn no_activity_targets(_: &RequestEnvelope, _: Option<&Value>) -> ActivityTargets {
    ActivityTargets::default()
}

fn write_invocation_result<W: Write>(
    args: &[String],
    stdin: &[u8],
    writer: &mut W,
) -> Result<i32, ProviderFailure> {
    let request = decode_request(stdin)?;
    let external_name = subcommand_from_args(args, &request.request_id)?;
    Route::resolve(external_name).write(request, writer)
}

fn parse_raw_request(stdin: &[u8]) -> Result<Value, serde_json::Error> {
    serde_json::from_slice(stdin)
}

fn invalid_json_failure(err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        "unknown",
        "invalid_json",
        format!("stdin must be one UTF-8 JSON object: {err}"),
    )
}

fn request_id_from_raw(raw: &Value) -> Option<&str> {
    raw.get("request_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn fallback_request_id(candidate: Option<&str>) -> String {
    candidate.unwrap_or("unknown").to_string()
}

fn validate_params_present(raw: &Value, request_id: &str) -> Result<(), ProviderFailure> {
    if raw.get("params").is_some() {
        return Ok(());
    }
    Err(missing_params_failure(request_id))
}

fn validate_empty_params(
    params: &Value,
    request_id: &str,
    code: &'static str,
) -> Result<(), ProviderFailure> {
    if params.as_object().is_some_and(serde_json::Map::is_empty) {
        return Ok(());
    }
    Err(empty_params_failure(request_id, code))
}

fn parse_request_envelope(
    raw: Value,
    request_id: &str,
) -> Result<RequestEnvelope, ProviderFailure> {
    serde_json::from_value(raw).map_err(|err| invalid_envelope_failure(request_id, err))
}

fn validate_request_envelope(request: RequestEnvelope) -> Result<RequestEnvelope, ProviderFailure> {
    if request.contract != CONTRACT {
        return Err(unsupported_contract_failure(
            request.request_id,
            &request.contract,
        ));
    }
    if request.request_id.trim().is_empty() {
        return Err(invalid_request_id_failure());
    }
    if request.host.app.trim().is_empty() {
        return Err(invalid_host_failure(request.request_id));
    }
    Ok(request)
}

fn unknown_subcommand_failure(request_id: String, subcommand: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "unknown_subcommand",
        format!("unknown provider subcommand: {subcommand}"),
    )
}

fn failure_output(failure: ProviderFailure) -> (Vec<u8>, i32) {
    let exit_code = failure.exit_code;
    let response = failure_response(&failure);
    (canonical_json_bytes(&response), exit_code)
}

fn write_failure_output<W: Write>(writer: &mut W, failure: ProviderFailure) -> i32 {
    let (stdout, exit_code) = failure_output(failure);
    if let Err(err) = writer.write_all(&stdout) {
        report_stdout_write_failure(err);
        return 1;
    }
    exit_code
}

fn missing_subcommand_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "missing_subcommand",
        "provider invocation requires exactly one subcommand argument",
    )
}

fn invalid_argv_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_argv",
        "provider invocation accepts exactly one subcommand argument",
    )
}

fn stdout_write_failure(err: std::io::Error) -> ProviderFailure {
    ProviderFailure::internal("unknown", "stdout_write_failed", err.to_string())
}

fn missing_params_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "missing_params",
        "request envelope must include params",
    )
}

fn empty_params_failure(request_id: &str, code: &'static str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        code,
        "params must be an empty object for this subcommand",
    )
}

fn invalid_envelope_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_envelope",
        format!("request envelope does not match the provider contract: {err}"),
    )
}

fn unsupported_contract_failure(request_id: String, contract: &str) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "unsupported_contract",
        format!("unsupported contract version: {contract}"),
    )
}

fn invalid_request_id_failure() -> ProviderFailure {
    ProviderFailure::invalid_request(
        "unknown",
        "invalid_request_id",
        "request_id must be a non-empty string",
    )
}

fn invalid_host_failure(request_id: String) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_host",
        "host.app must be a non-empty string",
    )
}

fn report_stdout_write_failure(err: std::io::Error) {
    eprintln!("failed to write stdout: {err}");
}
