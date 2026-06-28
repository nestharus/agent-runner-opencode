//! Declared roles: formatter, orchestration, parser, predicate, validator
//! intrinsic_surface_declarations:
//!   - component: src/dispatch.rs
//!     role: intrinsic-surface
//!     Domain: provider subcommand routing
//!     Owns:
//!       - provider subcommand router and dispatch table
//!       - per-capability handler invocation
//!       - request/response envelope decode-encode

use crate::discovery;
use crate::encoding::canonical_json_bytes;
use crate::envelope::{
    failure_response, success_response, ProviderFailure, RequestEnvelope, CONTRACT,
};
use crate::schema::{describe_result, schema_result_params};
use crate::{launch, migration, policy, quota, rotation, session, settings, setup, terminal};
use serde_json::Value;
use std::io::Write;

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

pub fn handle_decoded_invocation(
    request: RequestEnvelope,
    subcommand: &str,
) -> Result<Value, ProviderFailure> {
    match subcommand {
        "describe" => {
            validate_empty_params(
                &request.params,
                &request.request_id,
                "invalid_describe_params",
            )?;
            Ok(success_response(&request.request_id, describe_result()))
        }
        "schema" => Ok(success_response(
            &request.request_id,
            schema_result_params(request.params, &request.request_id)?,
        )),
        "discovery.models" => Ok(success_response(&request.request_id, discovery::models())),
        "discovery.accounts" => Ok(success_response(&request.request_id, discovery::accounts())),
        "launch" => Err(launch_requires_streaming_writer_failure(request.request_id)),
        "policy.evaluate" => Ok(success_response(
            &request.request_id,
            policy::evaluate_params(request.params, &request.request_id)?,
        )),
        "terminal.classify" => Ok(success_response(
            &request.request_id,
            terminal::classify_params(request.params, &request.request_id)?,
        )),
        "session.locate_transcript"
        | "session.read_turns"
        | "session.capture"
        | "session.enumerate"
        | "session.export"
        | "session.replace" => handle_capability(subcommand, request, session::handle),
        "quota.source" | "quota.probe" | "quota.refresh_auth" => {
            handle_capability(subcommand, request, quota::handle)
        }
        "settings.list" | "settings.get" | "settings.create" | "settings.update"
        | "settings.delete" | "settings.validate" | "settings.migrate" => {
            handle_capability(subcommand, request, settings::handle)
        }
        "setup.detect" | "setup.install_plan" | "setup.sync_plan" | "setup_brain.turn" => {
            handle_capability(subcommand, request, setup::handle)
        }
        "rotation.assess" => Ok(success_response(
            &request.request_id,
            rotation::assess_params(request.params, &request.request_id)?,
        )),
        "rotation.materialize" => Ok(success_response(
            &request.request_id,
            rotation::materialize_params(request.params, &request.request_id)?,
        )),
        "migration.plan" => Ok(success_response(
            &request.request_id,
            migration::plan_params(request.params, &request.request_id)?,
        )),
        "migration.apply" => Ok(success_response(
            &request.request_id,
            migration::apply_params(&request.host, request.params, &request.request_id)?,
        )),
        unknown => Err(unknown_subcommand_failure(request.request_id, unknown)),
    }
}

fn write_invocation_result<W: Write>(
    args: &[String],
    stdin: &[u8],
    writer: &mut W,
) -> Result<i32, ProviderFailure> {
    let request = decode_request(stdin)?;
    let subcommand = subcommand_from_args(args, &request.request_id)?;
    if subcommand == "launch" {
        return launch::stream(&request.request_id, &request.host, request.params, writer);
    }
    let response = handle_decoded_invocation(request, subcommand)?;
    writer
        .write_all(&canonical_json_bytes(&response))
        .map_err(stdout_write_failure)?;
    Ok(0)
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

fn handle_capability(
    subcommand: &str,
    request: RequestEnvelope,
    handle: fn(&str, RequestEnvelope) -> Result<Value, ProviderFailure>,
) -> Result<Value, ProviderFailure> {
    let request_id = request.request_id.clone();
    Ok(success_response(&request_id, handle(subcommand, request)?))
}

fn unknown_subcommand_failure(request_id: String, subcommand: &str) -> ProviderFailure {
    ProviderFailure::unsupported(
        request_id,
        "unknown_subcommand",
        format!("unknown provider subcommand: {subcommand}"),
    )
}

fn launch_requires_streaming_writer_failure(request_id: String) -> ProviderFailure {
    ProviderFailure::invalid_request(
        request_id,
        "launch_requires_streaming_writer",
        "launch must be invoked through the streaming dispatch branch",
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
