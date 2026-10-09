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
    MAX_HOST_ENV_BYTES, MAX_HOST_ENV_ENTRIES, MAX_HOST_ENV_KEY_BYTES, MAX_HOST_ENV_VALUE_BYTES,
    MAX_HOST_LABEL_BYTES, MAX_HOST_PATH_BYTES, MAX_PROVIDER_INSTANCE_ID_BYTES,
    MAX_REQUEST_ENVELOPE_BYTES, MAX_REQUEST_ID_BYTES,
};
use crate::schema::{describe_result, schema_result_params, CONTRACT_VERSIONS};
use crate::{launch, migration, policy, quota, rotation, session, settings, setup, terminal};
use agent_provider_contract::operations::{self as op, RequestOperation, ResponseOperation};
use agent_provider_contract::resident_session;
use agent_provider_contract::schemas::{
    ContractAdmissionError, SchemaRegistry, SchemaValidationError,
};
use serde_json::{json, Value};
use std::io::Write;

/// Schema errors echoed in an admission refusal; the remainder is counted.
const MAX_REPORTED_SCHEMA_ERRORS: usize = 8;
const MAX_REPORTED_SCHEMA_ERROR_BYTES: usize = 512;

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
    ResidentPrepare,
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

    fn from_session(outcome: session::SessionOutcome) -> Self {
        Self {
            result: outcome.result,
            activity_targets: ActivityTargets::default(),
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
            resident_session::PREPARE_SUBCOMMAND => Operation::ResidentPrepare,
            _ => Operation::Unknown,
        };
        Self {
            external_name,
            operation,
        }
    }

    fn write<W: Write>(
        self,
        admission: &Admission<'_>,
        writer: &mut W,
    ) -> Result<i32, ProviderFailure> {
        let name = self.external_name;
        match self.operation {
            Operation::Describe => write_enveloped_operation::<op::Describe, _, _, _>(
                name,
                admission,
                writer,
                no_activity_targets,
                |request| Ok(EnvelopedOutcome::new(describe_result(&request.host))),
            ),
            Operation::Schema => write_enveloped_operation::<op::Schema, _, _, _>(
                name,
                admission,
                writer,
                no_activity_targets,
                |request| {
                    schema_result_params(request.params, &request.request_id)
                        .map(EnvelopedOutcome::new)
                },
            ),
            Operation::DiscoveryModels => {
                write_enveloped_operation::<op::DiscoveryModels, _, _, _>(
                    name,
                    admission,
                    writer,
                    no_activity_targets,
                    |_| Ok(EnvelopedOutcome::new(discovery::models())),
                )
            }
            Operation::DiscoveryAccounts => {
                write_enveloped_operation::<op::DiscoveryAccounts, _, _, _>(
                    name,
                    admission,
                    writer,
                    no_activity_targets,
                    |_| Ok(EnvelopedOutcome::new(discovery::accounts())),
                )
            }
            Operation::Launch => {
                let request = admission.admit::<op::Launch>()?;
                write_streaming_launch(name, request, writer)
            }
            Operation::PolicyEvaluate => write_enveloped_operation::<op::PolicyEvaluate, _, _, _>(
                name,
                admission,
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
            Operation::TerminalClassify => {
                write_enveloped_operation::<op::TerminalClassify, _, _, _>(
                    name,
                    admission,
                    writer,
                    no_activity_targets,
                    |request| {
                        terminal::classify_params(request.params, &request.request_id)
                            .map(EnvelopedOutcome::new)
                    },
                )
            }
            Operation::Session(command) => {
                let project = move |request: &RequestEnvelope, result: Option<&Value>| {
                    session::activity_targets(
                        command,
                        &request.host,
                        &request.params,
                        result,
                        &request.request_id,
                    )
                };
                let handle = move |request| {
                    session::handle(command, request).map(EnvelopedOutcome::from_session)
                };
                match command {
                    session::Command::LocateTranscript => {
                        write_enveloped_operation::<op::SessionLocateTranscript, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    session::Command::ReadTurns => {
                        write_enveloped_operation::<op::SessionReadTurns, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    session::Command::Capture => {
                        write_enveloped_operation::<op::SessionCapture, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    session::Command::Enumerate => {
                        write_enveloped_operation::<op::SessionEnumerate, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    session::Command::Export => {
                        write_enveloped_operation::<op::SessionExport, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    session::Command::Replace => {
                        write_enveloped_operation::<op::SessionReplace, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                }
            }
            Operation::Quota(command) => {
                let project = |request: &RequestEnvelope, result: Option<&Value>| {
                    quota::activity_targets(
                        &request.host,
                        &request.params,
                        result,
                        &request.request_id,
                    )
                };
                let handle =
                    move |request| quota::handle(command, request).map(EnvelopedOutcome::new);
                match command {
                    quota::Command::Source => {
                        write_enveloped_operation::<op::QuotaSource, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    quota::Command::Probe => write_enveloped_operation::<op::QuotaProbe, _, _, _>(
                        name, admission, writer, project, handle,
                    ),
                    quota::Command::RefreshAuth => {
                        write_enveloped_operation::<op::QuotaRefreshAuth, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                }
            }
            Operation::Settings(command) => {
                let project = move |request: &RequestEnvelope, result: Option<&Value>| {
                    settings::activity_targets(command, &request.params, result)
                };
                let handle =
                    move |request| settings::handle(command, request).map(EnvelopedOutcome::new);
                match command {
                    settings::Command::List => {
                        write_enveloped_operation::<op::SettingsList, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    settings::Command::Get => {
                        write_enveloped_operation::<op::SettingsGet, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    settings::Command::Create => {
                        write_enveloped_operation::<op::SettingsCreate, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    settings::Command::Update => {
                        write_enveloped_operation::<op::SettingsUpdate, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    settings::Command::Delete => {
                        write_enveloped_operation::<op::SettingsDelete, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    settings::Command::Validate => {
                        write_enveloped_operation::<op::SettingsValidate, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                    settings::Command::Migrate => {
                        write_enveloped_operation::<op::SettingsMigrate, _, _, _>(
                            name, admission, writer, project, handle,
                        )
                    }
                }
            }
            Operation::Setup(command) => {
                let handle =
                    move |request| setup::handle(command, request).map(EnvelopedOutcome::new);
                match command {
                    setup::Command::Detect => {
                        write_enveloped_operation::<op::SetupDetect, _, _, _>(
                            name,
                            admission,
                            writer,
                            no_activity_targets,
                            handle,
                        )
                    }
                    setup::Command::InstallPlan => {
                        write_enveloped_operation::<op::SetupInstallPlan, _, _, _>(
                            name,
                            admission,
                            writer,
                            no_activity_targets,
                            handle,
                        )
                    }
                    setup::Command::SyncPlan => {
                        write_enveloped_operation::<op::SetupSyncPlan, _, _, _>(
                            name,
                            admission,
                            writer,
                            no_activity_targets,
                            handle,
                        )
                    }
                    setup::Command::BrainTurn => {
                        write_enveloped_operation::<op::SetupBrainTurn, _, _, _>(
                            name,
                            admission,
                            writer,
                            no_activity_targets,
                            handle,
                        )
                    }
                }
            }
            Operation::RotationAssess => write_enveloped_operation::<op::RotationAssess, _, _, _>(
                name,
                admission,
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
            Operation::RotationMaterialize => {
                write_enveloped_operation::<op::RotationMaterialize, _, _, _>(
                    name,
                    admission,
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
                )
            }
            Operation::MigrationPlan => write_enveloped_operation::<op::MigrationPlan, _, _, _>(
                name,
                admission,
                writer,
                |request, result| migration::activity_targets(&request.params, result),
                |request| {
                    migration::plan_params(request.params, &request.request_id)
                        .map(EnvelopedOutcome::new)
                },
            ),
            Operation::MigrationApply => write_enveloped_operation::<op::MigrationApply, _, _, _>(
                name,
                admission,
                writer,
                |request, result| migration::activity_targets(&request.params, result),
                |request| {
                    migration::apply_params(&request.host, request.params, &request.request_id)
                        .map(EnvelopedOutcome::new)
                },
            ),
            Operation::ResidentPrepare => {
                let request = admission.admit_unrouted()?;
                Err(resident_session_unsupported_failure(request.request_id))
            }
            Operation::Unknown => {
                let request = admission.admit_unrouted()?;
                Err(unknown_subcommand_failure(request.request_id, name))
            }
        }
    }
}

/// One request read from stdin: its bytes, its parsed JSON and the request id
/// used for refusals before the envelope is admitted.
struct Admission<'a> {
    registry: SchemaRegistry,
    stdin: &'a [u8],
    raw: Value,
    request_id: String,
}

impl Admission<'_> {
    /// Operation-bound wire admission: the SDK schema registry validates the
    /// whole envelope (including params) against this operation's request
    /// definition and the generated request DTO. Provider-native envelope
    /// bounds are applied afterwards; handlers keep native parameter meaning.
    fn admit<O: RequestOperation>(&self) -> Result<RequestEnvelope, ProviderFailure> {
        self.registry
            .decode_request::<O>(self.stdin)
            .map_err(|error| contract_admission_failure(&self.request_id, error))?;
        self.admit_unrouted()
    }

    /// Envelope admission for subcommands without an SDK operation schema.
    fn admit_unrouted(&self) -> Result<RequestEnvelope, ProviderFailure> {
        let request = serde_json::from_value(self.raw.clone())
            .map_err(|err| invalid_envelope_failure(&self.request_id, err))?;
        validate_request_envelope(request)
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
        Err(failure) => write_failure_output(writer, routed_subcommand(args), failure),
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

fn routed_subcommand(args: &[String]) -> Option<&str> {
    match args {
        [_, subcommand] => Some(subcommand.as_str()),
        _ => None,
    }
}

/// Reads the request JSON and applies the checks that precede operation
/// admission: the process bound, JSON syntax, `params` presence and the base
/// contract version this provider serves.
fn read_request(stdin: &[u8]) -> Result<(Value, String), ProviderFailure> {
    if stdin.len() > MAX_REQUEST_ENVELOPE_BYTES {
        return Err(request_envelope_capacity_exceeded());
    }
    let raw = parse_raw_request(stdin).map_err(invalid_json_failure)?;
    let request_id = fallback_request_id(request_id_from_raw(&raw));
    validate_params_present(&raw, &request_id)?;
    validate_contract_selection(&raw, &request_id)?;
    Ok((raw, request_id))
}

/// Refuses a request whose `contract` names a base version this provider does
/// not serve, before schema admission, so the refusal names the version.
fn validate_contract_selection(raw: &Value, request_id: &str) -> Result<(), ProviderFailure> {
    match raw.get("contract").and_then(Value::as_str) {
        Some(contract) if !CONTRACT_VERSIONS.contains(&contract) => Err(
            unsupported_contract_failure(request_id.to_string(), contract),
        ),
        _ => Ok(()),
    }
}

fn write_enveloped_operation<O, W, P, H>(
    external_name: &str,
    admission: &Admission<'_>,
    writer: &mut W,
    project_activity: P,
    handle: H,
) -> Result<i32, ProviderFailure>
where
    O: ResponseOperation,
    W: Write,
    P: Fn(&RequestEnvelope, Option<&Value>) -> ActivityTargets,
    H: FnOnce(RequestEnvelope) -> Result<EnvelopedOutcome, ProviderFailure>,
{
    let request = admission.admit::<O>()?;
    let activity = ActivityContext::from_request(&request, external_name);
    let attempted_targets = project_activity(&request, None);
    if let Err(error) = activity.started(&attempted_targets) {
        eprintln!("provider activity start evidence warning: {error:?}");
    }
    let request_id = request.request_id.clone();
    let request_snapshot = request.clone();
    let encoded = handle(request).and_then(|outcome| {
        encode_success::<O>(&admission.registry, &request_id, &outcome.result)
            .map(|bytes| (outcome, bytes))
    });
    let bytes = match encoded {
        Ok((outcome, bytes)) => {
            let mut completed_targets = project_activity(&request_snapshot, Some(&outcome.result));
            completed_targets.extend(outcome.activity_targets.clone());
            if let Err(error) = activity.succeeded(0, &completed_targets) {
                eprintln!("provider activity completion evidence warning: {error:?}");
            }
            bytes
        }
        Err(failure) => {
            if let Err(error) = activity.failed(&failure, &attempted_targets) {
                eprintln!("provider activity completion evidence warning: {error:?}");
            }
            return Err(failure);
        }
    };
    writer.write_all(&bytes).map_err(stdout_write_failure)?;
    writer.flush().map_err(stdout_write_failure)?;
    Ok(0)
}

/// Operation-bound encoding: the success envelope is emitted only after the
/// SDK registry admits it as this operation's response (schema and DTO).
fn encode_success<O: ResponseOperation>(
    registry: &SchemaRegistry,
    request_id: &str,
    result: &Value,
) -> Result<Vec<u8>, ProviderFailure> {
    let bytes = canonical_json_bytes(&success_response(request_id, result.clone()));
    registry
        .decode_response::<O>(&bytes)
        .map_err(|error| response_contract_failure(request_id, O::SUBCOMMAND, error))?;
    Ok(bytes)
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
    let (raw, request_id) = read_request(stdin)?;
    let external_name = subcommand_from_args(args, &request_id)?;
    let admission = Admission {
        registry: SchemaRegistry::new(),
        stdin,
        raw,
        request_id,
    };
    Route::resolve(external_name).write(&admission, writer)
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

/// Provider-native bounds on an admitted envelope. The shared schema fixes
/// shape; these bound the sizes this one-shot process accepts.
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
    if request.request_id.len() > MAX_REQUEST_ID_BYTES
        || request
            .provider_instance_id
            .as_ref()
            .is_some_and(|value| value.len() > MAX_PROVIDER_INSTANCE_ID_BYTES)
    {
        return Err(request_envelope_capacity_exceeded());
    }
    if request.host.app.trim().is_empty()
        || request.host.app.len() > MAX_HOST_LABEL_BYTES
        || request
            .host
            .app_version
            .as_ref()
            .is_some_and(|value| value.len() > MAX_HOST_LABEL_BYTES)
        || request
            .host
            .platform
            .as_ref()
            .is_some_and(|value| value.len() > MAX_HOST_LABEL_BYTES)
        || [
            request.host.working_directory.as_ref(),
            request.host.config_root.as_ref(),
            request.host.data_root.as_ref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| value.len() > MAX_HOST_PATH_BYTES)
        || !host_env_within_bounds(&request.host.env)
    {
        return Err(invalid_host_failure(request.request_id));
    }
    Ok(request)
}

fn host_env_within_bounds(env: &std::collections::BTreeMap<String, String>) -> bool {
    env.len() <= MAX_HOST_ENV_ENTRIES
        && env.iter().all(|(key, value)| {
            key.len() <= MAX_HOST_ENV_KEY_BYTES && value.len() <= MAX_HOST_ENV_VALUE_BYTES
        })
        && env.iter().fold(0_usize, |total, (key, value)| {
            total.saturating_add(key.len()).saturating_add(value.len())
        }) <= MAX_HOST_ENV_BYTES
}

fn unknown_subcommand_failure(request_id: String, subcommand: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "unknown_subcommand",
        format!("unknown provider subcommand: {subcommand}"),
    )
}

fn resident_session_unsupported_failure(request_id: String) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "resident_session_unsupported",
        format!(
            "OpenCode does not implement {}; {} is not served and resident_session_v1 is never advertised",
            resident_session::PROTOCOL,
            resident_session::PREPARE_SUBCOMMAND
        ),
    )
}

/// Encodes a refusal from the SDK error envelope DTO and, when the subcommand
/// has an SDK error-response definition, admits it against that definition.
fn failure_output(subcommand: Option<&str>, failure: ProviderFailure) -> (Vec<u8>, i32) {
    let exit_code = failure.exit_code;
    let response = failure_response(&failure);
    let admitted = subcommand.map_or(Ok(()), |subcommand| {
        match SchemaRegistry::new().validate_error_response(subcommand, &response) {
            Err(
                SchemaValidationError::UnknownSubcommand(_)
                | SchemaValidationError::MissingResponseEnvelope(_),
            ) => Ok(()),
            other => other,
        }
    });
    match admitted {
        Ok(()) => (canonical_json_bytes(&response), exit_code),
        Err(error) => {
            eprintln!("provider error envelope failed contract admission: {error}");
            let fallback = failure_response(&ProviderFailure::internal(
                failure.request_id,
                "response_contract_violation",
                "provider error response failed shared contract admission",
            ));
            (canonical_json_bytes(&fallback), 1)
        }
    }
}

fn write_failure_output<W: Write>(
    writer: &mut W,
    subcommand: Option<&str>,
    failure: ProviderFailure,
) -> i32 {
    let (stdout, exit_code) = failure_output(subcommand, failure);
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

fn contract_admission_failure(request_id: &str, error: ContractAdmissionError) -> ProviderFailure {
    let (code, message, details) = match error {
        ContractAdmissionError::Schema(SchemaValidationError::Validation {
            schema_file,
            definition,
            errors,
        }) => (
            "contract_schema_violation",
            format!("request does not satisfy {schema_file}#/$defs/{definition}"),
            json!({
                "schema_file": schema_file,
                "definition": definition,
                "errors": bounded_schema_errors(&errors),
                "error_count": errors.len(),
            }),
        ),
        other => (
            "invalid_envelope",
            format!("request envelope does not match the provider contract: {other}"),
            json!({}),
        ),
    };
    let mut failure = ProviderFailure::invalid_request(request_id, code, message);
    failure.details = details;
    failure
}

fn response_contract_failure(
    request_id: &str,
    subcommand: &str,
    error: ContractAdmissionError,
) -> ProviderFailure {
    eprintln!("provider {subcommand} response failed contract admission: {error}");
    ProviderFailure::internal(
        request_id,
        "response_contract_violation",
        format!("provider {subcommand} response failed shared contract admission"),
    )
}

fn bounded_schema_errors(errors: &[String]) -> Vec<String> {
    errors
        .iter()
        .take(MAX_REPORTED_SCHEMA_ERRORS)
        .map(|error| crate::encoding::bounded_text_bytes(error, MAX_REPORTED_SCHEMA_ERROR_BYTES))
        .collect()
}

fn invalid_envelope_failure(request_id: &str, err: serde_json::Error) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "invalid_envelope",
        format!("request envelope does not match the provider contract: {err}"),
    )
}

fn unsupported_contract_failure(request_id: String, contract: &str) -> ProviderFailure {
    let mut failure = ProviderFailure::invalid_request(
        request_id,
        "unsupported_contract",
        format!("unsupported contract version: {contract}"),
    );
    failure.details = json!({ "supported_contract_versions": CONTRACT_VERSIONS });
    failure
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
        "host identity, path, or environment exceeds the supported bounded envelope",
    )
}

fn request_envelope_capacity_exceeded() -> ProviderFailure {
    ProviderFailure::invalid_request(
        "unknown",
        "request_envelope_capacity_exceeded",
        format!("request envelope exceeds the supported {MAX_REQUEST_ENVELOPE_BYTES}-byte bound"),
    )
}

fn report_stdout_write_failure(err: std::io::Error) {
    eprintln!("failed to write stdout: {err}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn admission(bytes: &[u8]) -> Result<Admission<'_>, ProviderFailure> {
        let (raw, request_id) = read_request(bytes)?;
        Ok(Admission {
            registry: SchemaRegistry::new(),
            stdin: bytes,
            raw,
            request_id,
        })
    }

    fn request_bytes(contract: &str, params: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "contract": contract,
            "request_id": "request-admission",
            "provider_instance_id": "opencode-primary",
            "host": {"app": "agent-runner"},
            "params": params,
        }))
        .expect("serialize request fixture")
    }

    #[test]
    fn request_decode_rejects_input_above_the_process_bound_before_parsing() {
        let bytes = vec![b' '; MAX_REQUEST_ENVELOPE_BYTES + 1];
        let failure = admission(&bytes)
            .err()
            .expect("oversized envelope must fail");
        assert_eq!(failure.code, "request_envelope_capacity_exceeded");
    }

    #[test]
    fn request_decode_rejects_host_environment_above_the_entry_bound() {
        let env = (0..=MAX_HOST_ENV_ENTRIES)
            .map(|index| (format!("KEY_{index}"), json!("value")))
            .collect::<serde_json::Map<_, _>>();
        let bytes = serde_json::to_vec(&json!({
            "contract": CONTRACT,
            "request_id": "request-bounded-env",
            "provider_instance_id": "opencode-primary",
            "host": {"app": "agent-runner", "env": env},
            "params": {},
        }))
        .expect("serialize request fixture");
        let failure = admission(&bytes)
            .and_then(|admission| admission.admit::<op::Describe>())
            .expect_err("oversized host env must fail");
        assert_eq!(failure.code, "invalid_host");
    }

    #[test]
    fn unserved_contract_versions_are_refused_by_name_before_schema_admission() {
        for contract in ["oulipoly.provider/v2", "oulipoly.provider/v0"] {
            let bytes = request_bytes(contract, json!({}));
            let failure = admission(&bytes)
                .err()
                .expect("unserved contract version must be refused");
            assert_eq!(failure.code, "unsupported_contract");
            assert_eq!(
                failure.details["supported_contract_versions"],
                json!([CONTRACT])
            );
        }
    }

    #[test]
    fn operation_schema_admission_binds_params_to_the_routed_operation() {
        let bytes = request_bytes(CONTRACT, json!({"unexpected": true}));
        let admission = admission(&bytes).expect("read request");
        let failure = admission
            .admit::<op::Describe>()
            .expect_err("describe params admit no fields");
        assert_eq!(failure.code, "contract_schema_violation");
        assert_eq!(failure.details["definition"], "DescribeRequest");
        assert!(admission.admit::<op::SettingsList>().is_err());

        let bytes = request_bytes(CONTRACT, json!({"schema_id": "opencode.settings/v1"}));
        let admission = admission_ok(&bytes);
        assert!(admission.admit::<op::Schema>().is_ok());
        assert_eq!(
            admission
                .admit::<op::Describe>()
                .expect_err("schema params are not describe params")
                .code,
            "contract_schema_violation"
        );
    }

    fn admission_ok(bytes: &[u8]) -> Admission<'_> {
        admission(bytes).expect("read request")
    }

    #[test]
    fn resident_prepare_is_refused_as_an_unimplemented_extension() {
        let bytes = request_bytes(CONTRACT, json!({}));
        let mut stdout = Vec::new();
        let args = vec![
            "agent-runner-opencode".to_string(),
            resident_session::PREPARE_SUBCOMMAND.to_string(),
        ];
        let exit_code = write_invocation(&args, &bytes, &mut stdout);
        assert_eq!(exit_code, 3);
        let response: Value = serde_json::from_slice(&stdout).expect("error envelope");
        assert_eq!(response["ok"], false);
        assert_eq!(response["error"]["category"], "unsupported");
        assert_eq!(response["error"]["code"], "resident_session_unsupported");
    }

    #[test]
    fn success_encoding_refuses_a_result_outside_the_operation_schema() {
        let registry = SchemaRegistry::new();
        let failure = encode_success::<op::Describe>(&registry, "request-encode", &json!({}))
            .expect_err("an empty describe result is not admitted");
        assert_eq!(failure.code, "response_contract_violation");
        assert!(encode_success::<op::Describe>(
            &registry,
            "request-encode",
            &describe_result(&crate::envelope::HostContext {
                app: "agent-runner".to_string(),
                app_version: None,
                platform: None,
                working_directory: None,
                config_root: None,
                data_root: None,
                env: Default::default(),
                deadline_unix_ms: None,
            }),
        )
        .is_ok());
    }
}
