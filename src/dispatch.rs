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
                move |request| {
                    session::handle(command, request).map(EnvelopedOutcome::from_session)
                },
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
    if stdin.len() > MAX_REQUEST_ENVELOPE_BYTES {
        return Err(request_envelope_capacity_exceeded());
    }
    let raw = parse_raw_request(stdin).map_err(invalid_json_failure)?;
    let request_id = fallback_request_id(request_id_from_raw(&raw));
    validate_params_present(&raw, &request_id)?;
    let request = parse_request_envelope(raw.clone(), &request_id)?;
    let request = validate_request_envelope(request)?;
    agent_provider_contract::SchemaRegistry::new()
        .validate_request_envelope(&raw)
        .map_err(|err| invalid_envelope_failure(&request_id, err))?;
    Ok(request)
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
    writer.flush().map_err(stdout_write_failure)?;
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
    let raw = serde_json::from_slice::<&serde_json::value::RawValue>(stdin)?;
    parse_json_value(raw, 128)
}

// Arbitrary-precision Serde uses private map transports for numbers/raw values.
// Select containers by their actual JSON token so an ordinary caller object
// cannot be mistaken for either transport. Retain serde_json's default nesting
// limit and last-key-wins object mapping; no numeric magnitude limit is added.
fn parse_json_value(
    raw: &serde_json::value::RawValue,
    remaining_depth: usize,
) -> Result<Value, serde_json::Error> {
    match raw.get().as_bytes()[0] {
        b'{' => parse_json_object(raw, next_json_depth(remaining_depth)?),
        b'[' => parse_json_array(raw, next_json_depth(remaining_depth)?),
        _ => serde_json::from_str(raw.get()),
    }
}

fn next_json_depth(remaining_depth: usize) -> Result<usize, serde_json::Error> {
    remaining_depth
        .checked_sub(1)
        .filter(|depth| *depth > 0)
        .ok_or_else(|| <serde_json::Error as serde::de::Error>::custom("recursion limit exceeded"))
}

fn parse_json_object(
    raw: &serde_json::value::RawValue,
    remaining_depth: usize,
) -> Result<Value, serde_json::Error> {
    serde::Deserializer::deserialize_map(
        &mut serde_json::Deserializer::from_str(raw.get()),
        JsonObjectVisitor { remaining_depth },
    )
}

struct JsonObjectVisitor {
    remaining_depth: usize,
}

impl<'de> serde::de::Visitor<'de> for JsonObjectVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut fields = serde_json::Map::new();
        // Parse even shadowed duplicate values before last-key-wins insertion,
        // preserving the original parser's depth admission for the whole input.
        while let Some((key, raw)) = map.next_entry::<String, &serde_json::value::RawValue>()? {
            let value =
                parse_json_value(raw, self.remaining_depth).map_err(serde::de::Error::custom)?;
            fields.insert(key, value);
        }
        Ok(Value::Object(fields))
    }
}

fn parse_json_array(
    raw: &serde_json::value::RawValue,
    remaining_depth: usize,
) -> Result<Value, serde_json::Error> {
    let items: Vec<&serde_json::value::RawValue> = serde_json::from_str(raw.get())?;
    items
        .into_iter()
        .map(|value| parse_json_value(value, remaining_depth))
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
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
    let mut fields = raw;
    // Params are already parsed, and common admission permits every JSON value.
    // Move them unchanged around the typed Serde transport rather than decoding
    // Value a second time (which could reinterpret private-looking object keys).
    let params = fields
        .as_object_mut()
        .and_then(|fields| fields.remove("params"));
    fields["params"] = Value::Null;
    let mut request: RequestEnvelope =
        serde_json::from_value(fields).map_err(|err| invalid_envelope_failure(request_id, err))?;
    request.params = params.expect("params presence was checked before typed mapping");
    Ok(request)
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
        || !host_env_within_bounds(request.host.env.as_ref())
    {
        return Err(invalid_host_failure(request.request_id));
    }
    Ok(request)
}

fn host_env_within_bounds(env: Option<&std::collections::BTreeMap<String, String>>) -> bool {
    let Some(env) = env else {
        return true;
    };
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

fn invalid_envelope_failure(request_id: &str, err: impl std::fmt::Display) -> ProviderFailure {
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

    #[test]
    fn request_decode_rejects_input_above_the_process_bound_before_parsing() {
        let bytes = vec![b' '; MAX_REQUEST_ENVELOPE_BYTES + 1];
        let failure = decode_request(&bytes).expect_err("oversized envelope must fail");
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
        let failure = decode_request(&bytes).expect_err("oversized host env must fail");
        assert_eq!(failure.code, "invalid_host");
    }
    fn wire_request() -> Value {
        json!({"contract": CONTRACT, "request_id": "req-wire", "host": {"app": "fixture"}, "params": {}})
    }

    fn decode_value(value: &Value) -> Result<RequestEnvelope, ProviderFailure> {
        decode_request(&serde_json::to_vec(value).unwrap())
    }

    fn assert_failure(value: &Value, code: &str, id: &str) {
        let failure = decode_value(value).unwrap_err();
        println!(
            "failure code={} id={} category={} exit={}",
            failure.code, failure.request_id, failure.category, failure.exit_code
        );
        assert_eq!(
            (
                failure.code,
                failure.request_id.as_str(),
                failure.category,
                failure.exit_code
            ),
            (code, id, "invalid_request", 2)
        );
    }

    #[test]
    fn common_envelope_presence_and_numeric_admission() {
        for token in [
            "80",
            "80.0",
            "8e1",
            "800e-1",
            "0",
            "-0",
            "-0.0",
            "-0e-999999999999999999999",
            "18446744073709551616",
            "18446744073709551617",
            "1e1000001",
            "1e999999999999999999999",
        ] {
            let mut value = wire_request();
            value["host"]["deadline_unix_ms"] = serde_json::from_str(token).unwrap();
            let actual = decode_value(&value).unwrap();
            let number = actual.host.deadline_unix_ms.unwrap();
            println!(
                "deadline input={token} decoded={} serialized={}",
                number.as_number(),
                serde_json::to_string(&number).unwrap()
            );
            assert_eq!(
                number.as_number(),
                value["host"]["deadline_unix_ms"].as_number().unwrap()
            );
        }
        for token in [
            "-1",
            "-1e-1000001",
            "80.5",
            "80.00000000000000001",
            "1e-1000001",
            "null",
            "true",
            "\"80\"",
            "[]",
            "{}",
        ] {
            let mut value = wire_request();
            value["host"]["deadline_unix_ms"] = serde_json::from_str(token).unwrap();
            println!("reject deadline token={token}");
            assert_failure(&value, "invalid_envelope", "req-wire");
        }
        for field in [
            "app_version",
            "platform",
            "working_directory",
            "config_root",
            "data_root",
        ] {
            assert_optional_host_field(field);
        }
        for value in [
            json!(""),
            Value::Null,
            json!(1),
            json!(true),
            json!({}),
            json!([]),
        ] {
            let mut request = wire_request();
            request["provider_instance_id"] = value;
            assert_failure(&request, "invalid_envelope", "req-wire");
        }
        for value in [" ", "account-a", "account-b"] {
            let mut request = wire_request();
            request["provider_instance_id"] = json!(value);
            assert_eq!(
                decode_value(&request)
                    .unwrap()
                    .provider_instance_id
                    .as_deref(),
                Some(value)
            );
        }
        assert!(decode_value(&wire_request())
            .unwrap()
            .provider_instance_id
            .is_none());
        assert!(decode_value(&wire_request()).unwrap().host.env.is_none());
        for env in [json!({}), json!({"SENTINEL": "not-a-secret"})] {
            let mut request = wire_request();
            request["host"]["env"] = env.clone();
            let decoded = decode_value(&request).unwrap();
            println!("env input={env} decoded={:?}", decoded.host.env);
            assert_eq!(
                serde_json::to_value(decoded.host.env.as_ref().unwrap()).unwrap(),
                env
            );
        }
        for env in [Value::Null, json!(false), json!([]), json!({"SENTINEL": 1})] {
            let mut request = wire_request();
            request["host"]["env"] = env;
            assert_failure(&request, "invalid_envelope", "req-wire");
        }
        for params in [
            Value::Null,
            json!(true),
            json!([]),
            serde_json::from_str(
                r#"{"nested":[18446744073709551617,80.00000000000000001,1e999999999999999999999]}"#,
            )
            .unwrap(),
        ] {
            let mut request = wire_request();
            request["params"] = params.clone();
            assert_eq!(decode_value(&request).unwrap().params, params);
        }
    }

    fn assert_optional_host_field(field: &str) {
        for value in [None, Some(""), Some("sentinel/second-context")] {
            let mut request = wire_request();
            if let Some(value) = value {
                request["host"][field] = json!(value);
            }
            let host = decode_value(&request).unwrap().host;
            let actual = match field {
                "app_version" => host.app_version,
                "platform" => host.platform,
                "working_directory" => host.working_directory,
                "config_root" => host.config_root,
                "data_root" => host.data_root,
                _ => unreachable!(),
            };
            println!("host field={field} decoded={actual:?}");
            assert_eq!(actual.as_deref(), value);
        }
        for invalid in [Value::Null, json!(1), json!(true), json!([]), json!({})] {
            let mut request = wire_request();
            request["host"][field] = invalid;
            assert_failure(&request, "invalid_envelope", "req-wire");
        }
    }

    #[test]
    fn common_envelope_failure_precedence() {
        let mut missing = wire_request();
        missing.as_object_mut().unwrap().remove("params");
        missing["contract"] = json!("wrong");
        missing.as_object_mut().unwrap().remove("host");
        assert_failure(&missing, "missing_params", "req-wire");
        let mut wrong = wire_request();
        wrong["contract"] = json!("wrong");
        assert_failure(&wrong, "unsupported_contract", "req-wire");
        for token in ["80.0", "18446744073709551617", "null"] {
            let mut request = wrong.clone();
            request["host"]["deadline_unix_ms"] = serde_json::from_str(token).unwrap();
            assert_failure(&request, "unsupported_contract", "req-wire");
        }
        for token in ["-1", "80.00000000000000001", "true"] {
            let mut request = wrong.clone();
            request["host"]["deadline_unix_ms"] = serde_json::from_str(token).unwrap();
            assert_failure(&request, "invalid_envelope", "req-wire");
        }
        wrong["host"]["unexpected"] = json!(true);
        assert_failure(&wrong, "invalid_envelope", "req-wire");
        for (field, value, code, id) in [
            ("request_id", json!(""), "invalid_request_id", "unknown"),
            ("request_id", json!("  "), "invalid_request_id", "unknown"),
            ("request_id", json!(1), "invalid_envelope", "unknown"),
            ("contract", json!(1), "invalid_envelope", "req-wire"),
        ] {
            let mut request = wire_request();
            request[field] = value;
            assert_failure(&request, code, id);
        }
        for field in ["request_id", "contract", "host"] {
            let mut request = wire_request();
            request.as_object_mut().unwrap().remove(field);
            assert_failure(
                &request,
                "invalid_envelope",
                if field == "request_id" {
                    "unknown"
                } else {
                    "req-wire"
                },
            );
        }
        for app in [json!(""), json!(" ")] {
            let mut request = wire_request();
            request["host"]["app"] = app;
            request["provider_instance_id"] = Value::Null;
            assert_failure(&request, "invalid_host", "req-wire");
        }
        let mut request = wire_request();
        request["host"].as_object_mut().unwrap().remove("app");
        assert_failure(&request, "invalid_envelope", "req-wire");
        request["host"]["app"] = json!(1);
        assert_failure(&request, "invalid_envelope", "req-wire");
        let args = vec!["provider".into(), "describe".into(), "bad-argv".into()];
        assert_wire_error(
            &args,
            &vec![b'!'; MAX_REQUEST_ENVELOPE_BYTES + 1],
            "request_envelope_capacity_exceeded",
            "unknown",
        );
        for bytes in [b"{bad".as_slice(), b"{} {}".as_slice()] {
            assert_wire_error(&args, bytes, "invalid_json", "unknown");
        }
        assert_wire_error(
            &args,
            &serde_json::to_vec(&wire_request()).unwrap(),
            "invalid_argv",
            "req-wire",
        );
    }

    fn assert_wire_error(args: &[String], bytes: &[u8], code: &str, id: &str) {
        let (stdout, exit) = handle_invocation(args, bytes);
        println!("output={} exit={exit}", String::from_utf8_lossy(&stdout));
        let actual: Value = serde_json::from_slice(&stdout).unwrap();
        assert_eq!(exit, 2);
        assert_eq!(actual["error"]["code"], code);
        assert_eq!(actual["error"]["category"], "invalid_request");
        assert_eq!(actual["request_id"], id);
    }

    #[test]
    fn common_envelope_authored_byte_boundaries() {
        for (pointer, bound, code, id) in [
            (
                "/request_id",
                MAX_REQUEST_ID_BYTES,
                "request_envelope_capacity_exceeded",
                "unknown",
            ),
            (
                "/provider_instance_id",
                MAX_PROVIDER_INSTANCE_ID_BYTES,
                "request_envelope_capacity_exceeded",
                "unknown",
            ),
            (
                "/host/app",
                MAX_HOST_LABEL_BYTES,
                "invalid_host",
                "req-wire",
            ),
            (
                "/host/app_version",
                MAX_HOST_LABEL_BYTES,
                "invalid_host",
                "req-wire",
            ),
            (
                "/host/platform",
                MAX_HOST_LABEL_BYTES,
                "invalid_host",
                "req-wire",
            ),
            (
                "/host/working_directory",
                MAX_HOST_PATH_BYTES,
                "invalid_host",
                "req-wire",
            ),
            (
                "/host/config_root",
                MAX_HOST_PATH_BYTES,
                "invalid_host",
                "req-wire",
            ),
            (
                "/host/data_root",
                MAX_HOST_PATH_BYTES,
                "invalid_host",
                "req-wire",
            ),
        ] {
            assert_string_boundary(pointer, bound, code, id);
        }
        for count in [MAX_HOST_ENV_ENTRIES, MAX_HOST_ENV_ENTRIES + 1] {
            let env = (0..count)
                .map(|i| (i.to_string(), json!("v")))
                .collect::<serde_json::Map<_, _>>();
            assert_env_boundary(json!(env), count == MAX_HOST_ENV_ENTRIES);
        }
        assert_env_boundary(json!({"é".repeat(MAX_HOST_ENV_KEY_BYTES/2): "v"}), true);
        assert_env_boundary(
            json!({format!("{}x", "é".repeat(MAX_HOST_ENV_KEY_BYTES/2)): "v"}),
            false,
        );
        assert_env_boundary(json!({"k": "é".repeat(MAX_HOST_ENV_VALUE_BYTES/2)}), true);
        assert_env_boundary(
            json!({"k": format!("{}x", "é".repeat(MAX_HOST_ENV_VALUE_BYTES/2))}),
            false,
        );
        let mut env = (0..4)
            .map(|i| {
                (
                    i.to_string(),
                    json!("x".repeat(MAX_HOST_ENV_VALUE_BYTES - 1)),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        assert_env_boundary(json!(env), true);
        env.insert("0".into(), json!("x".repeat(MAX_HOST_ENV_VALUE_BYTES)));
        assert_env_boundary(json!(env), false);
        let mut request = wire_request();
        request["params"] = json!("");
        let base = serde_json::to_vec(&request).unwrap().len();
        request["params"] = json!("x".repeat(MAX_REQUEST_ENVELOPE_BYTES - base));
        let mut bytes = serde_json::to_vec(&request).unwrap();
        assert_eq!(bytes.len(), MAX_REQUEST_ENVELOPE_BYTES);
        assert!(decode_request(&bytes).is_ok());
        println!("wire bytes={} admitted=true", bytes.len());
        bytes.push(b' ');
        let failure = decode_request(&bytes).unwrap_err();
        println!("wire bytes={} error={}", bytes.len(), failure.code);
        assert_eq!(
            (failure.code, failure.request_id.as_str()),
            ("request_envelope_capacity_exceeded", "unknown")
        );
    }

    fn assert_string_boundary(pointer: &str, bound: usize, code: &str, id: &str) {
        let mut request = wire_request();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        let object = if parent.is_empty() {
            request.as_object_mut().unwrap()
        } else {
            request
                .pointer_mut(parent)
                .unwrap()
                .as_object_mut()
                .unwrap()
        };
        object.insert(key.into(), json!("é".repeat(bound / 2)));
        assert!(decode_value(&request).is_ok(), "{pointer} at {bound}");
        *request.pointer_mut(pointer).unwrap() = json!(format!("{}x", "é".repeat(bound / 2)));
        println!(
            "byte boundary pointer={pointer} admitted={bound} rejected={}",
            bound + 1
        );
        assert_failure(&request, code, id);
    }

    fn assert_env_boundary(env: Value, admitted: bool) {
        let mut request = wire_request();
        request["host"]["env"] = env;
        println!(
            "env entries={} bytes={} admitted={admitted}",
            request["host"]["env"].as_object().unwrap().len(),
            request["host"]["env"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| k.len() + v.as_str().unwrap().len())
                .sum::<usize>()
        );
        if admitted {
            assert!(decode_value(&request).is_ok());
            return;
        }
        assert_failure(&request, "invalid_host", "req-wire");
    }

    #[test]
    fn common_envelope_retains_json_token_shapes() {
        for key in [
            "$serde_json::private::Number",
            "$serde_json::private::RawValue",
        ] {
            let mut request = wire_request();
            request["host"]["deadline_unix_ms"] = json!({key: "80"});
            assert_failure(&request, "invalid_envelope", "req-wire");
            request["host"]
                .as_object_mut()
                .unwrap()
                .remove("deadline_unix_ms");
            request["params"] = json!({"nested": [{key: "80"}]});
            let decoded = decode_value(&request).unwrap();
            println!(
                "literal-object params input={} actual={}",
                request["params"], decoded.params
            );
            assert_eq!(decoded.params, request["params"]);
        }
        let mut request = wire_request();
        request["params"] = json!(0);
        let input = serde_json::to_string(&request)
            .unwrap()
            .replace("\"params\":0", "\"params\":1,\"params\":2");
        assert_eq!(decode_request(input.as_bytes()).unwrap().params, json!(2));
        for depth in [127_usize, 128] {
            let raw = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            let expected = serde_json::from_str::<Value>(&raw).is_ok();
            assert_eq!(
                parse_raw_request(raw.as_bytes()).is_ok(),
                expected,
                "container depth={depth}"
            );
        }
    }
    #[test]
    fn common_envelope_checks_shadowed_json_values() {
        let nested = format!("{}0{}", "[".repeat(128), "]".repeat(128));
        let raw = format!(r#"{{"params":{nested},"params":0}}"#);
        assert!(serde_json::from_str::<Value>(&raw).is_err());
        let failure = decode_request(raw.as_bytes()).unwrap_err();
        println!(
            "shadowed overdepth code={} id={}",
            failure.code, failure.request_id
        );
        assert_eq!(
            (failure.code, failure.request_id.as_str()),
            ("invalid_json", "unknown")
        );
    }
}
