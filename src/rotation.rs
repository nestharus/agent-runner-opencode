//! Declared roles: mapper, validator, predicate, filter, formatter

use crate::encoding::sha256_hex;
use crate::envelope::ProviderFailure;
use serde_json::{json, Value};

pub fn assess_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let requirements = requirements(&params);
    let met = requirements_met(&requirements);
    let facts_allow = facts_allow_rotation(&params);
    let allowed = met && facts_allow;
    Ok(assess_result(allowed, &requirements, met, facts_allow))
}

pub fn materialize_params(params: Value, _request_id: &str) -> Result<Value, ProviderFailure> {
    let host_state_plan = host_state_plan(&params);
    Ok(json!({
        "changed": false,
        "target_provider_session_id": string_field(&params, "target_session_id", "pending-target-session"),
        "artifacts": [],
        "host_state_plan": host_state_plan,
    }))
}

fn requirements(params: &Value) -> Vec<Value> {
    params
        .get("requirements")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn requirements_met(requirements: &[Value]) -> bool {
    !requirements.is_empty()
        && requirements.iter().all(|requirement| {
            requirement
                .get("met")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
}

fn facts_allow_rotation(params: &Value) -> bool {
    let quota = params
        .pointer("/facts/quota/available")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let exportable = params
        .pointer("/facts/session/exportable")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let target = params
        .pointer("/facts/settings/target_profile_present")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    quota && exportable && target
}

fn score(requirements: &[Value], facts_allow: bool) -> u64 {
    if requirements.is_empty() {
        return u64::from(facts_allow) * 100;
    }
    let met = met_requirement_count(requirements);
    (met * 100) / requirements.len() as u64
}

fn assess_reason(allowed: bool, requirements_met: bool, facts_allow: bool) -> &'static str {
    match (allowed, requirements_met, facts_allow) {
        (true, _, _) => {
            "rotation requirements are satisfied; provider can return a host-applied plan"
        }
        (false, true, true) => "rotation was denied by provider policy",
        (false, false, _) => "one or more rotation requirements are not met",
        (false, _, false) => "provider facts do not permit safe rotation materialization",
    }
}

fn host_state_plan(params: &Value) -> Value {
    let chain_id = string_field(params, "chain_id", "chain-opencode");
    let source_provider = string_field(params, "source_provider", "opencode1");
    let target_provider = string_field(params, "target_provider", "opencode2");
    let source_session_id = string_field(params, "source_session_id", "source-session");
    let target_session_id = string_field(params, "target_session_id", "target-session");
    let transition_reason = transition_reason(params);
    let artifact_path = host_state_artifact_path(&chain_id);
    let artifact_sha = host_state_artifact_sha(
        &chain_id,
        &source_provider,
        &target_provider,
        &source_session_id,
        &target_session_id,
    );
    json!({
        "schema_version": 1,
        "operation": "rotation.materialize",
        "chain_id": chain_id,
        "source_provider": source_provider,
        "target_provider": target_provider,
        "source_session_id": source_session_id,
        "target_session_id": target_session_id,
        "transition_reason": transition_reason,
        "segments": [
            {"provider": source_provider, "session_id": source_session_id},
            {"provider": target_provider, "session_id": target_session_id}
        ],
        "artifacts": [{"kind": "file", "path": artifact_path, "sha256": artifact_sha}]
    })
}

fn transition_reason(params: &Value) -> &'static str {
    match params.get("transition_reason").and_then(Value::as_str) {
        Some("quota_threshold") => "quota_threshold",
        Some("exhausted") => "exhausted",
        _ => "manual",
    }
}

fn string_field(params: &Value, key: &str, fallback: &str) -> String {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(fallback)
        .to_string()
}

fn assess_result(
    allowed: bool,
    requirements: &[Value],
    requirements_met: bool,
    facts_allow: bool,
) -> Value {
    json!({
        "allowed": allowed,
        "score": score(requirements, facts_allow),
        "reason": assess_reason(allowed, requirements_met, facts_allow),
        "requirements": requirements,
    })
}

fn met_requirement_count(requirements: &[Value]) -> u64 {
    requirements
        .iter()
        .filter(|requirement| requirement_met(requirement))
        .count() as u64
}

fn requirement_met(requirement: &Value) -> bool {
    requirement.get("met").and_then(Value::as_bool) == Some(true)
}

fn host_state_artifact_path(chain_id: &str) -> String {
    format!("provider-owned://rotation/{chain_id}/host-state-plan.json")
}

fn host_state_artifact_sha(
    chain_id: &str,
    source_provider: &str,
    target_provider: &str,
    source_session_id: &str,
    target_session_id: &str,
) -> String {
    sha256_hex(
        format!("{chain_id}:{source_provider}:{target_provider}:{source_session_id}:{target_session_id}")
            .as_bytes(),
    )
}
