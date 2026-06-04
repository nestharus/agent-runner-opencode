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
use crate::schema::{describe_result, schema_response};
use crate::{launch, policy, terminal};
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
        [_] => Err(ProviderFailure::unsupported(
            request_id,
            "missing_subcommand",
            "provider invocation requires exactly one subcommand argument",
        )),
        _ => Err(ProviderFailure::invalid_request(
            request_id,
            "invalid_argv",
            "provider invocation accepts exactly one subcommand argument",
        )),
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
        "describe" => Ok(success_response(&request.request_id, describe_result())),
        "schema" => schema_response(request),
        "discovery.models" => Ok(success_response(&request.request_id, discovery::models())),
        "discovery.accounts" => Ok(success_response(&request.request_id, discovery::accounts())),
        "launch" => Err(ProviderFailure::invalid_request(
            request.request_id,
            "launch_requires_streaming_writer",
            "launch must be invoked through the streaming dispatch branch",
        )),
        "policy.evaluate" => Ok(success_response(
            &request.request_id,
            policy::evaluate_params(request.params, &request.request_id)?,
        )),
        "terminal.classify" => Ok(success_response(
            &request.request_id,
            terminal::classify_params(request.params, &request.request_id)?,
        )),
        // PHASE6-TODO(cluster-B): replace with real session.locate_transcript handler
        "session.locate_transcript" => Err(not_implemented_in_this_build(
            request.request_id,
            "session.locate_transcript",
        )),
        // PHASE6-TODO(cluster-B): replace with real session.read_turns handler
        "session.read_turns" => Err(not_implemented_in_this_build(
            request.request_id,
            "session.read_turns",
        )),
        // PHASE6-TODO(cluster-B): replace with real session.capture handler
        "session.capture" => Err(not_implemented_in_this_build(
            request.request_id,
            "session.capture",
        )),
        // PHASE6-TODO(cluster-B): replace with real session.export handler
        "session.export" => Err(not_implemented_in_this_build(
            request.request_id,
            "session.export",
        )),
        // PHASE6-TODO(cluster-B): replace with real session.replace handler
        "session.replace" => Err(not_implemented_in_this_build(
            request.request_id,
            "session.replace",
        )),
        // PHASE6-TODO(cluster-C): replace with real quota.source handler
        "quota.source" => Err(not_implemented_in_this_build(
            request.request_id,
            "quota.source",
        )),
        // PHASE6-TODO(cluster-C): replace with real quota.probe handler
        "quota.probe" => Err(not_implemented_in_this_build(
            request.request_id,
            "quota.probe",
        )),
        // PHASE6-TODO(cluster-C): replace with real quota.refresh_auth handler
        "quota.refresh_auth" => Err(not_implemented_in_this_build(
            request.request_id,
            "quota.refresh_auth",
        )),
        // PHASE6-TODO(cluster-D): replace with real settings.list handler
        "settings.list" => Err(not_implemented_in_this_build(
            request.request_id,
            "settings.list",
        )),
        // PHASE6-TODO(cluster-D): replace with real settings.get handler
        "settings.get" => Err(not_implemented_in_this_build(
            request.request_id,
            "settings.get",
        )),
        // PHASE6-TODO(cluster-D): replace with real settings.create handler
        "settings.create" => Err(not_implemented_in_this_build(
            request.request_id,
            "settings.create",
        )),
        // PHASE6-TODO(cluster-D): replace with real settings.update handler
        "settings.update" => Err(not_implemented_in_this_build(
            request.request_id,
            "settings.update",
        )),
        // PHASE6-TODO(cluster-D): replace with real settings.delete handler
        "settings.delete" => Err(not_implemented_in_this_build(
            request.request_id,
            "settings.delete",
        )),
        // PHASE6-TODO(cluster-D): replace with real settings.validate handler
        "settings.validate" => Err(not_implemented_in_this_build(
            request.request_id,
            "settings.validate",
        )),
        // PHASE6-TODO(cluster-D): replace with real settings.migrate handler
        "settings.migrate" => Err(not_implemented_in_this_build(
            request.request_id,
            "settings.migrate",
        )),
        // PHASE6-TODO(cluster-D): replace with real setup.detect handler
        "setup.detect" => Err(not_implemented_in_this_build(
            request.request_id,
            "setup.detect",
        )),
        // PHASE6-TODO(cluster-D): replace with real setup.install_plan handler
        "setup.install_plan" => Err(not_implemented_in_this_build(
            request.request_id,
            "setup.install_plan",
        )),
        // PHASE6-TODO(cluster-D): replace with real setup.sync_plan handler
        "setup.sync_plan" => Err(not_implemented_in_this_build(
            request.request_id,
            "setup.sync_plan",
        )),
        // PHASE6-TODO(cluster-D): replace with real setup_brain.turn handler
        "setup_brain.turn" => Err(not_implemented_in_this_build(
            request.request_id,
            "setup_brain.turn",
        )),
        // PHASE6-TODO(cluster-D): replace with real rotation.assess handler
        "rotation.assess" => Err(not_implemented_in_this_build(
            request.request_id,
            "rotation.assess",
        )),
        // PHASE6-TODO(cluster-D): replace with real rotation.materialize handler
        "rotation.materialize" => Err(not_implemented_in_this_build(
            request.request_id,
            "rotation.materialize",
        )),
        // PHASE6-TODO(cluster-D): replace with real migration.plan handler
        "migration.plan" => Err(not_implemented_in_this_build(
            request.request_id,
            "migration.plan",
        )),
        // PHASE6-TODO(cluster-D): replace with real migration.apply handler
        "migration.apply" => Err(not_implemented_in_this_build(
            request.request_id,
            "migration.apply",
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
        .map_err(|err| {
            ProviderFailure::internal("unknown", "stdout_write_failed", err.to_string())
        })?;
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
    Err(ProviderFailure::invalid_request(
        request_id,
        "missing_params",
        "request envelope must include params",
    ))
}

fn parse_request_envelope(
    raw: Value,
    request_id: &str,
) -> Result<RequestEnvelope, ProviderFailure> {
    serde_json::from_value(raw).map_err(|err| {
        ProviderFailure::invalid_request(
            request_id,
            "invalid_envelope",
            format!("request envelope does not match the provider contract: {err}"),
        )
    })
}

fn validate_request_envelope(request: RequestEnvelope) -> Result<RequestEnvelope, ProviderFailure> {
    if request.contract != CONTRACT {
        return Err(ProviderFailure::invalid_request(
            request.request_id,
            "unsupported_contract",
            format!("unsupported contract version: {}", request.contract),
        ));
    }
    if request.request_id.trim().is_empty() {
        return Err(ProviderFailure::invalid_request(
            "unknown",
            "invalid_request_id",
            "request_id must be a non-empty string",
        ));
    }
    if request.host.app.trim().is_empty() {
        return Err(ProviderFailure::invalid_request(
            request.request_id,
            "invalid_host",
            "host.app must be a non-empty string",
        ));
    }
    Ok(request)
}

fn not_implemented_in_this_build(request_id: String, subcommand: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "not_implemented",
        format!("{subcommand} is not implemented in this foundation build"),
    )
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
        eprintln!("failed to write stdout: {err}");
        return 1;
    }
    exit_code
}
