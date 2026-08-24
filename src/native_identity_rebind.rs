//! Declared roles: orchestration, parser, validator, accessor, predicate, formatter
//! intrinsic_surface_declarations:
//!   - component: src/native_identity_rebind.rs
//!     role: intrinsic-surface
//!     Domain: durable native-identity rebind protocol
//!     Owns:
//!       - typed plan, seal, observe, and release transitions
//!       - component identity evidence and host handoff validation
//!       - cycle persistence, retention, locking, replay, and errors

use crate::account::{profile_for_wrapper_reference, ACCOUNTS};
use crate::durable_fs;
use crate::encoding::{now_unix_ms, sha256_hex};
use crate::envelope::{HostContext, ProviderFailure};
use crate::native_runtime;
use crate::operation_bounds;
use crate::path_guard;
use crate::quota_observer;
use crate::schema::NATIVE_IDENTITY_REBIND_SCHEMA_ID;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const NATIVE_IDENTITY_REBIND_PROTOCOL: &str = "opencode.native-identity-rebind/v1";
const NATIVE_IDENTITY_REBIND_DRAIN_MS: u64 = 20_000;
const NATIVE_IDENTITY_REBIND_STATE_DIR: &str = "provider-state/opencode/native-identity-rebind";
const NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION: u32 = 2;
const NATIVE_IDENTITY_REBIND_STATE_BYTES: usize = 16 * 1024;
const NATIVE_IDENTITY_REBIND_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_NATIVE_IDENTITY_REBIND_CYCLES_PER_COMPONENT: usize = 64;
const NATIVE_IDENTITY_REBIND_REPLAY_WINDOW_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum NativeIdentityRebindRequest {
    Plan {
        protocol: String,
        targets: Vec<NativeIdentityRebindTarget>,
    },
    Seal {
        protocol: String,
        cycle_id: String,
        operation_id: String,
        profile: String,
        component: NativeIdentityRebindComponent,
        prior_evidence: NativeIdentityRebindEvidence,
        host_handoff: NativeIdentityRebindSealHandoff,
    },
    Observe {
        protocol: String,
        cycle_id: String,
        operation_id: String,
        profile: String,
        component: NativeIdentityRebindComponent,
        prior_evidence: NativeIdentityRebindEvidence,
        disposition: NativeIdentityRebindDisposition,
        host_handoff: NativeIdentityRebindObservationHandoff,
    },
    Release {
        protocol: String,
        cycle_id: String,
        operation_id: String,
        observation_id: String,
        profile: String,
        component: NativeIdentityRebindComponent,
        prior_evidence: NativeIdentityRebindEvidence,
        observed_evidence: NativeIdentityRebindEvidence,
        disposition: NativeIdentityRebindDisposition,
        host_handoff: NativeIdentityRebindReleaseHandoff,
    },
}

#[derive(Clone, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindTarget {
    profile: String,
    component: NativeIdentityRebindComponent,
}

#[derive(Clone, Copy, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum NativeIdentityRebindComponent {
    NativeRuntime,
    QuotaObserver,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindEvidence {
    component_identity_sha256: Option<String>,
    state_record_sha256: Option<String>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum NativeIdentityRebindDisposition {
    Committed,
    RolledBack,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindSealHandoff {
    ordinary_admission_blocked: bool,
    obligations_reconciled: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindObservationHandoff {
    ordinary_admission_blocked: bool,
    validation_capability_completed: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindReleaseHandoff {
    ordinary_admission_blocked: bool,
}

struct NativeIdentityRebindOperationView<'a> {
    cycle_id: &'a str,
    component: NativeIdentityRebindComponent,
    prior_evidence: &'a NativeIdentityRebindEvidence,
    observed_evidence: &'a NativeIdentityRebindEvidence,
    phase: &'a str,
    diagnostic: Option<&'a str>,
    disposition: Option<NativeIdentityRebindDisposition>,
    next_action: Option<&'a str>,
}

struct NativeIdentityRebindSealBinding<'a> {
    cycle_id: &'a str,
    profile: &'a str,
    component: NativeIdentityRebindComponent,
    prior_evidence: &'a NativeIdentityRebindEvidence,
}

struct NativeIdentityRebindObservationBinding<'a> {
    cycle_id: &'a str,
    profile: &'a str,
    component: NativeIdentityRebindComponent,
    prior_evidence: &'a NativeIdentityRebindEvidence,
    observed_evidence: &'a NativeIdentityRebindEvidence,
    disposition: NativeIdentityRebindDisposition,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeIdentityRebindCycleRecord {
    schema_version: u32,
    cycle_id: String,
    operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observation_id: Option<String>,
    profile: String,
    component: NativeIdentityRebindComponent,
    prior_evidence: NativeIdentityRebindEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observed_evidence: Option<NativeIdentityRebindEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    disposition: Option<NativeIdentityRebindDisposition>,
    phase: NativeIdentityRebindCyclePhase,
    updated_at_unix_ms: u64,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum NativeIdentityRebindCyclePhase {
    AwaitingHostDrain,
    AwaitingCutover,
    AwaitingHostRelease,
    Completed,
    RolledBack,
}

impl NativeIdentityRebindCyclePhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingHostDrain => "awaiting_host_drain",
            Self::AwaitingCutover => "awaiting_cutover",
            Self::AwaitingHostRelease => "awaiting_host_release",
            Self::Completed => "completed",
            Self::RolledBack => "rolled_back",
        }
    }
}

impl NativeIdentityRebindDisposition {
    fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
        }
    }
}

impl NativeIdentityRebindComponent {
    fn as_str(self) -> &'static str {
        match self {
            Self::NativeRuntime => "native_runtime",
            Self::QuotaObserver => "quota_observer",
        }
    }

    fn validation_capability(self) -> &'static str {
        match self {
            Self::NativeRuntime => "launch",
            Self::QuotaObserver => "quota_probe",
        }
    }

    fn provider_state_record(self, profile: &str) -> String {
        match self {
            Self::NativeRuntime => format!("native-runtimes/{profile}.json"),
            Self::QuotaObserver => format!("quota-observers/{profile}.json"),
        }
    }
}

fn parse_native_identity_rebind(
    params: &Value,
    request_id: &str,
) -> Result<Option<NativeIdentityRebindRequest>, ProviderFailure> {
    params
        .get("native_identity_rebind")
        .cloned()
        .map(|value| {
            serde_json::from_value(value).map_err(|error| {
                invalid_native_identity_rebind(
                    request_id,
                    format!("maintenance request does not match its protocol: {error}"),
                )
            })
        })
        .transpose()
}

pub(crate) fn operations(
    host: &HostContext,
    params: &Value,
    request_id: &str,
) -> Result<Option<Vec<Value>>, ProviderFailure> {
    parse_native_identity_rebind(params, request_id)?
        .map(|request| native_identity_rebind_operations(host, request, request_id))
        .transpose()
}

fn native_identity_rebind_operations(
    host: &HostContext,
    request: NativeIdentityRebindRequest,
    request_id: &str,
) -> Result<Vec<Value>, ProviderFailure> {
    match request {
        NativeIdentityRebindRequest::Plan { protocol, targets } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            if targets.is_empty() || targets.len() > ACCOUNTS.len() * 2 {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "plan targets must contain between one and ten component-scoped identities",
                ));
            }
            if targets.iter().collect::<BTreeSet<_>>().len() != targets.len() {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "plan targets must not contain duplicate profile/component identities",
                ));
            }
            targets
                .into_iter()
                .map(|target| {
                    let account = canonical_rebind_profile(&target.profile, request_id)?;
                    let cycle_id = native_identity_rebind_cycle_id(
                        request_id,
                        account.opencode_wrapper,
                        target.component,
                    );
                    let planned = persist_native_identity_rebind_plan(
                        host,
                        &cycle_id,
                        account,
                        target.component,
                        request_id,
                    )?;
                    Ok(native_identity_rebind_operation(
                        account.opencode_wrapper,
                        NativeIdentityRebindOperationView {
                            cycle_id: &cycle_id,
                            component: target.component,
                            prior_evidence: &planned.prior_evidence,
                            observed_evidence: &planned.prior_evidence,
                            phase: "awaiting_host_drain",
                            diagnostic: None,
                            disposition: None,
                            next_action: Some("seal"),
                        },
                    ))
                })
                .collect()
        }
        NativeIdentityRebindRequest::Seal {
            protocol,
            cycle_id,
            operation_id,
            profile,
            component,
            prior_evidence,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_rebind_cycle_id(&cycle_id, request_id)?;
            validate_native_identity_evidence(&prior_evidence, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id = native_identity_rebind_operation_id(
                &cycle_id,
                account.opencode_wrapper,
                component,
                &prior_evidence,
            );
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile, component, and prior identity evidence",
                ));
            }
            if !host_handoff.ordinary_admission_blocked || !host_handoff.obligations_reconciled {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "cutover sealing requires blocked ordinary admission and reconciled provider obligations",
                ));
            }
            let sealed_evidence =
                native_identity_evidence(host, account, component, request_id, false)?;
            if sealed_evidence != prior_evidence {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "selected provider identity state changed during the host drain; request a new component-scoped plan while admission remains blocked",
                ));
            }
            persist_native_identity_rebind_seal(
                host,
                NativeIdentityRebindSealBinding {
                    cycle_id: &cycle_id,
                    profile: account.opencode_wrapper,
                    component,
                    prior_evidence: &prior_evidence,
                },
                request_id,
            )?;
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                NativeIdentityRebindOperationView {
                    cycle_id: &cycle_id,
                    component,
                    prior_evidence: &prior_evidence,
                    observed_evidence: &sealed_evidence,
                    phase: "awaiting_cutover",
                    diagnostic: None,
                    disposition: None,
                    next_action: Some("observe"),
                },
            )])
        }
        NativeIdentityRebindRequest::Observe {
            protocol,
            cycle_id,
            operation_id,
            profile,
            component,
            prior_evidence,
            disposition,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_rebind_cycle_id(&cycle_id, request_id)?;
            validate_native_identity_evidence(&prior_evidence, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id = native_identity_rebind_operation_id(
                &cycle_id,
                account.opencode_wrapper,
                component,
                &prior_evidence,
            );
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile, component, and prior identity evidence",
                ));
            }
            let observed_evidence =
                native_identity_evidence(host, account, component, request_id, true)?;
            let validation_window_complete = host_handoff.ordinary_admission_blocked
                && host_handoff.validation_capability_completed;
            let (phase, diagnostic) = match disposition {
                _ if validation_window_complete
                    && native_identity_rebind_disposition_matches(
                        disposition,
                        &prior_evidence,
                        &observed_evidence,
                    ) =>
                {
                    let admitted_phase = persist_native_identity_rebind_observation(
                        host,
                        NativeIdentityRebindObservationBinding {
                            cycle_id: &cycle_id,
                            profile: account.opencode_wrapper,
                            component,
                            prior_evidence: &prior_evidence,
                            observed_evidence: &observed_evidence,
                            disposition,
                        },
                        request_id,
                    )?;
                    (admitted_phase.as_str(), None)
                }
                NativeIdentityRebindDisposition::Committed => (
                    "rejected",
                    Some("commit observation requires ordinary admission to remain blocked, completion of the selected component's validation capability, and a newly admitted identity record for that component"),
                ),
                NativeIdentityRebindDisposition::RolledBack => (
                    "rejected",
                    Some("rollback observation requires ordinary admission to remain blocked, completion of the selected component's validation capability, and exact restoration of that component's prior identity record"),
                ),
            };
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                NativeIdentityRebindOperationView {
                    cycle_id: &cycle_id,
                    component,
                    prior_evidence: &prior_evidence,
                    observed_evidence: &observed_evidence,
                    phase,
                    diagnostic,
                    disposition: Some(disposition),
                    next_action: (phase == "awaiting_host_release").then_some("release"),
                },
            )])
        }
        NativeIdentityRebindRequest::Release {
            protocol,
            cycle_id,
            operation_id,
            observation_id,
            profile,
            component,
            prior_evidence,
            observed_evidence,
            disposition,
            host_handoff,
        } => {
            require_native_identity_rebind_protocol(&protocol, request_id)?;
            validate_native_identity_rebind_cycle_id(&cycle_id, request_id)?;
            validate_native_identity_evidence(&prior_evidence, request_id)?;
            validate_native_identity_evidence(&observed_evidence, request_id)?;
            let account = canonical_rebind_profile(&profile, request_id)?;
            let expected_operation_id = native_identity_rebind_operation_id(
                &cycle_id,
                account.opencode_wrapper,
                component,
                &prior_evidence,
            );
            if operation_id != expected_operation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "operation_id does not bind the supplied profile, component, and prior identity evidence",
                ));
            }
            let expected_observation_id = native_identity_rebind_observation_id(
                &operation_id,
                &observed_evidence,
                disposition,
            );
            if observation_id != expected_observation_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "observation_id does not bind the supplied operation, disposition, and observed component identity",
                ));
            }
            let _observation_lock = acquire_native_identity_rebind_lock(
                host,
                account.opencode_wrapper,
                component,
                request_id,
            )?;
            let admitted_cycle = read_native_identity_rebind_cycle(
                host,
                &cycle_id,
                account.opencode_wrapper,
                component,
                request_id,
            )?
            .ok_or_else(|| {
                invalid_native_identity_rebind(
                    request_id,
                    "release requires a provider-admitted awaiting_host_release observation",
                )
            })?;
            if native_identity_rebind_cycle_expired(&admitted_cycle, now_unix_ms()) {
                let expired_path = native_identity_rebind_cycle_path(
                    host,
                    &cycle_id,
                    account.opencode_wrapper,
                    component,
                    request_id,
                )?;
                fs::remove_file(&expired_path)
                    .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
                durable_fs::sync_directory(
                    expired_path
                        .parent()
                        .expect("native identity rebind cycle always has a parent"),
                )
                .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "the rebind cycle replay window expired; begin a new plan request",
                ));
            }
            let expected_observation = NativeIdentityRebindCycleRecord {
                schema_version: NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION,
                cycle_id: cycle_id.clone(),
                operation_id: operation_id.clone(),
                observation_id: Some(observation_id.clone()),
                profile: account.opencode_wrapper.to_string(),
                component,
                prior_evidence: prior_evidence.clone(),
                observed_evidence: Some(observed_evidence.clone()),
                disposition: Some(disposition),
                phase: admitted_cycle.phase,
                updated_at_unix_ms: admitted_cycle.updated_at_unix_ms,
            };
            if !native_identity_rebind_cycle_matches(&admitted_cycle, &expected_observation) {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "release does not match the provider-admitted awaiting_host_release observation",
                ));
            }
            if !host_handoff.ordinary_admission_blocked {
                return Ok(vec![native_identity_rebind_operation(
                    account.opencode_wrapper,
                    NativeIdentityRebindOperationView {
                        cycle_id: &cycle_id,
                        component,
                        prior_evidence: &prior_evidence,
                        observed_evidence: &observed_evidence,
                        phase: "rejected",
                        diagnostic: Some(
                            "release settlement requires ordinary admission to remain blocked until the provider returns a terminal authorization",
                        ),
                        disposition: Some(disposition),
                        next_action: None,
                    },
                )]);
            }
            if admitted_cycle.phase != NativeIdentityRebindCyclePhase::AwaitingHostRelease {
                return Ok(vec![native_identity_rebind_operation(
                    account.opencode_wrapper,
                    NativeIdentityRebindOperationView {
                        cycle_id: &cycle_id,
                        component,
                        prior_evidence: &prior_evidence,
                        observed_evidence: &observed_evidence,
                        phase: admitted_cycle.phase.as_str(),
                        diagnostic: None,
                        disposition: Some(disposition),
                        next_action: None,
                    },
                )]);
            }
            let current_evidence =
                native_identity_evidence(host, account, component, request_id, true)?;
            let (phase, diagnostic) = if current_evidence != observed_evidence {
                (
                    "rejected",
                    Some("selected provider identity changed after observation and before provider release settlement"),
                )
            } else if !native_identity_rebind_disposition_matches(
                disposition,
                &prior_evidence,
                &current_evidence,
            ) {
                (
                    "rejected",
                    Some("selected provider identity no longer satisfies the admitted observation disposition"),
                )
            } else {
                let terminal_phase = match disposition {
                    NativeIdentityRebindDisposition::Committed => {
                        NativeIdentityRebindCyclePhase::Completed
                    }
                    NativeIdentityRebindDisposition::RolledBack => {
                        NativeIdentityRebindCyclePhase::RolledBack
                    }
                };
                let terminal_cycle = NativeIdentityRebindCycleRecord {
                    phase: terminal_phase,
                    updated_at_unix_ms: now_unix_ms(),
                    ..admitted_cycle
                };
                write_native_identity_rebind_cycle(host, &terminal_cycle, request_id)?;
                (terminal_phase.as_str(), None)
            };
            Ok(vec![native_identity_rebind_operation(
                account.opencode_wrapper,
                NativeIdentityRebindOperationView {
                    cycle_id: &cycle_id,
                    component,
                    prior_evidence: &prior_evidence,
                    observed_evidence: &current_evidence,
                    phase,
                    diagnostic,
                    disposition: Some(disposition),
                    next_action: None,
                },
            )])
        }
    }
}

fn native_identity_evidence(
    host: &HostContext,
    account: &crate::account::AccountProfile,
    component: NativeIdentityRebindComponent,
    request_id: &str,
    require_valid_identity: bool,
) -> Result<NativeIdentityRebindEvidence, ProviderFailure> {
    let evidence = match (component, require_valid_identity) {
        (NativeIdentityRebindComponent::NativeRuntime, true) => {
            native_runtime::validated_persisted_identity_evidence(host, account, request_id)?
        }
        (NativeIdentityRebindComponent::NativeRuntime, false) => {
            native_runtime::persisted_identity_evidence(host, account, request_id)?
        }
        (NativeIdentityRebindComponent::QuotaObserver, true) => {
            quota_observer::validated_persisted_identity_evidence(host, account, request_id)?
        }
        (NativeIdentityRebindComponent::QuotaObserver, false) => {
            quota_observer::persisted_identity_evidence(host, account, request_id)?
        }
    };
    let (component_identity_sha256, state_record_sha256) = evidence
        .map(|(component_identity, state_record)| (Some(component_identity), Some(state_record)))
        .unwrap_or((None, None));
    Ok(NativeIdentityRebindEvidence {
        component_identity_sha256,
        state_record_sha256,
    })
}

fn native_identity_rebind_operation(
    profile: &str,
    view: NativeIdentityRebindOperationView<'_>,
) -> Value {
    let NativeIdentityRebindOperationView {
        cycle_id,
        component,
        prior_evidence,
        observed_evidence,
        phase,
        diagnostic,
        disposition,
        next_action,
    } = view;
    let operation_id =
        native_identity_rebind_operation_id(cycle_id, profile, component, prior_evidence);
    let validation_capability = component.validation_capability();
    let mut operation = json!({
        "kind": "native_identity_rebind",
        "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
        "schema_id": NATIVE_IDENTITY_REBIND_SCHEMA_ID,
        "cycle_id": cycle_id,
        "operation_id": operation_id,
        "profile": profile,
        "component": component.as_str(),
        "phase": phase,
        "maximum_drain_ms": NATIVE_IDENTITY_REBIND_DRAIN_MS,
        "prior_evidence": prior_evidence,
        "observed_evidence": observed_evidence,
        "responsibilities": [
            {
                "actor": "host",
                "action": format!("block ordinary capability admission that consumes the selected {} identity, bound in-flight consumers to the drain interval, and keep ordinary admission blocked through provider release settlement", component.as_str()),
                "completion": "seal, observe, and release assert host_handoff.ordinary_admission_blocked=true"
            },
            {
                "actor": "operator",
                "action": format!("reconcile every nonterminal obligation that consumes the selected {} identity before cutover", component.as_str()),
                "completion": "the seal request asserts host_handoff.obligations_reconciled=true"
            },
            {
                "actor": "host",
                "action": format!("while ordinary admission remains blocked, authorize exactly one operation-bound {validation_capability}, then reopen ordinary admission only after a terminal provider release authorization"),
                "completion": "observe asserts the selected validation capability completed; completed or rolled_back returns release_authorization.ordinary_admission_may_reopen=true"
            },
            {
                "actor": "operator",
                "action": format!("stage the replacement {} dependency and preserve its prior provider identity record for rollback", component.as_str()),
                "completion": "the observed component identity differs from the plan-bound prior identity, or the prior identity is restored"
            },
            {
                "actor": "provider",
                "action": "bind the request to the component-scoped plan identity and observe that provider-owned identity record",
                "completion": "observation emits an observation-bound release request; release durably settles completed or rolled_back before authorizing host admission to reopen"
            }
        ],
        "implementation_evidence": {
            "provider_state_record": component.provider_state_record(profile)
        }
    });
    match next_action {
        Some("seal") => {
            operation["next_request"] = json!({
                "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
                "action": "seal",
                "cycle_id": cycle_id,
                "operation_id": operation_id,
                "profile": profile,
                "component": component.as_str(),
                "prior_evidence": prior_evidence,
                "host_handoff": {
                    "ordinary_admission_blocked": true,
                    "obligations_reconciled": true
                }
            });
        }
        Some("observe") => {
            operation["next_request"] = json!({
                "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
                "action": "observe",
                "cycle_id": cycle_id,
                "operation_id": operation_id,
                "profile": profile,
                "component": component.as_str(),
                "prior_evidence": prior_evidence,
                "disposition": "committed",
                "host_handoff": {
                    "ordinary_admission_blocked": true,
                    "validation_capability_completed": true
                }
            });
        }
        Some("release") => {
            let disposition = disposition.expect("release follows a typed observation");
            let observation_id = native_identity_rebind_observation_id(
                &operation_id,
                observed_evidence,
                disposition,
            );
            operation["observation_id"] = json!(observation_id);
            operation["disposition"] = json!(disposition.as_str());
            operation["next_request"] = json!({
                "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
                "action": "release",
                "cycle_id": cycle_id,
                "operation_id": operation_id,
                "observation_id": observation_id,
                "profile": profile,
                "component": component.as_str(),
                "prior_evidence": prior_evidence,
                "observed_evidence": observed_evidence,
                "disposition": disposition.as_str(),
                "host_handoff": {
                    "ordinary_admission_blocked": true
                }
            });
        }
        Some(_) => unreachable!("native rebind operation has a fixed next-action set"),
        None => {}
    }
    if let Some(disposition) = disposition {
        let observation_id =
            native_identity_rebind_observation_id(&operation_id, observed_evidence, disposition);
        operation["observation_id"] = json!(observation_id);
        operation["disposition"] = json!(disposition.as_str());
    }
    if phase == "completed" || phase == "rolled_back" {
        operation["release_authorization"] = json!({
            "ordinary_admission_may_reopen": true
        });
    }
    if let Some(diagnostic) = diagnostic {
        operation["diagnostic"] = json!(diagnostic);
    }
    operation
}

fn native_identity_rebind_observation_id(
    operation_id: &str,
    observed_evidence: &NativeIdentityRebindEvidence,
    disposition: NativeIdentityRebindDisposition,
) -> String {
    sha256_hex(
        json!({
            "operation_id": operation_id,
            "observed_evidence": observed_evidence,
            "disposition": disposition.as_str(),
        })
        .to_string()
        .as_bytes(),
    )
}

fn native_identity_rebind_disposition_matches(
    disposition: NativeIdentityRebindDisposition,
    prior_evidence: &NativeIdentityRebindEvidence,
    observed_evidence: &NativeIdentityRebindEvidence,
) -> bool {
    match disposition {
        NativeIdentityRebindDisposition::Committed => identity_component_rebound(
            prior_evidence.component_identity_sha256.as_deref(),
            observed_evidence.component_identity_sha256.as_deref(),
        ),
        NativeIdentityRebindDisposition::RolledBack => observed_evidence == prior_evidence,
    }
}

fn persist_native_identity_rebind_plan(
    host: &HostContext,
    cycle_id: &str,
    account: &crate::account::AccountProfile,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<NativeIdentityRebindCycleRecord, ProviderFailure> {
    let profile = account.opencode_wrapper;
    let _lock = acquire_native_identity_rebind_lock(host, profile, component, request_id)?;
    prepare_native_identity_rebind_cycle_slot(host, cycle_id, profile, component, request_id)?;
    if let Some(existing) =
        read_native_identity_rebind_cycle(host, cycle_id, profile, component, request_id)?
    {
        if existing.phase == NativeIdentityRebindCyclePhase::AwaitingHostDrain {
            return Ok(existing);
        }
        return Err(invalid_native_identity_rebind(
            request_id,
            "the rebind cycle already advanced beyond or conflicts with this plan request",
        ));
    }
    let prior_evidence = native_identity_evidence(host, account, component, request_id, false)?;
    let planned = NativeIdentityRebindCycleRecord {
        schema_version: NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION,
        cycle_id: cycle_id.to_string(),
        operation_id: native_identity_rebind_operation_id(
            cycle_id,
            profile,
            component,
            &prior_evidence,
        ),
        observation_id: None,
        profile: profile.to_string(),
        component,
        prior_evidence,
        observed_evidence: None,
        disposition: None,
        phase: NativeIdentityRebindCyclePhase::AwaitingHostDrain,
        updated_at_unix_ms: now_unix_ms(),
    };
    write_native_identity_rebind_cycle(host, &planned, request_id)?;
    Ok(planned)
}

fn persist_native_identity_rebind_seal(
    host: &HostContext,
    binding: NativeIdentityRebindSealBinding<'_>,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let NativeIdentityRebindSealBinding {
        cycle_id,
        profile,
        component,
        prior_evidence,
    } = binding;
    let _lock = acquire_native_identity_rebind_lock(host, profile, component, request_id)?;
    let sealed = NativeIdentityRebindCycleRecord {
        schema_version: NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION,
        cycle_id: cycle_id.to_string(),
        operation_id: native_identity_rebind_operation_id(
            cycle_id,
            profile,
            component,
            prior_evidence,
        ),
        observation_id: None,
        profile: profile.to_string(),
        component,
        prior_evidence: prior_evidence.clone(),
        observed_evidence: None,
        disposition: None,
        phase: NativeIdentityRebindCyclePhase::AwaitingCutover,
        updated_at_unix_ms: now_unix_ms(),
    };
    prepare_native_identity_rebind_cycle_slot(host, cycle_id, profile, component, request_id)?;
    let existing =
        read_native_identity_rebind_cycle(host, cycle_id, profile, component, request_id)?
            .ok_or_else(|| {
                invalid_native_identity_rebind(
                    request_id,
                    "seal requires a provider-admitted awaiting_host_drain predecessor for the exact rebind cycle",
                )
            })?;
    if existing.phase == NativeIdentityRebindCyclePhase::AwaitingHostDrain
        && native_identity_rebind_cycle_matches(&existing, &sealed)
    {
        return write_native_identity_rebind_cycle(host, &sealed, request_id);
    }
    if existing.phase == NativeIdentityRebindCyclePhase::AwaitingCutover
        && native_identity_rebind_cycle_matches(&existing, &sealed)
    {
        return Ok(());
    }
    Err(invalid_native_identity_rebind(
        request_id,
        "the rebind cycle already advanced beyond or conflicts with this seal request",
    ))
}

fn persist_native_identity_rebind_observation(
    host: &HostContext,
    binding: NativeIdentityRebindObservationBinding<'_>,
    request_id: &str,
) -> Result<NativeIdentityRebindCyclePhase, ProviderFailure> {
    let NativeIdentityRebindObservationBinding {
        cycle_id,
        profile,
        component,
        prior_evidence,
        observed_evidence,
        disposition,
    } = binding;
    let _lock = acquire_native_identity_rebind_lock(host, profile, component, request_id)?;
    let operation_id =
        native_identity_rebind_operation_id(cycle_id, profile, component, prior_evidence);
    let observation = NativeIdentityRebindCycleRecord {
        schema_version: NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION,
        cycle_id: cycle_id.to_string(),
        observation_id: Some(native_identity_rebind_observation_id(
            &operation_id,
            observed_evidence,
            disposition,
        )),
        operation_id,
        profile: profile.to_string(),
        component,
        prior_evidence: prior_evidence.clone(),
        observed_evidence: Some(observed_evidence.clone()),
        disposition: Some(disposition),
        phase: NativeIdentityRebindCyclePhase::AwaitingHostRelease,
        updated_at_unix_ms: now_unix_ms(),
    };
    prepare_native_identity_rebind_cycle_slot(host, cycle_id, profile, component, request_id)?;
    let existing = read_native_identity_rebind_cycle(
        host, cycle_id, profile, component, request_id,
    )?
    .ok_or_else(|| {
        invalid_native_identity_rebind(
            request_id,
            "observe requires a provider-sealed awaiting_cutover predecessor for the exact rebind cycle",
        )
    })?;
    if existing.phase == NativeIdentityRebindCyclePhase::AwaitingCutover {
        let sealed = NativeIdentityRebindCycleRecord {
            schema_version: NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION,
            cycle_id: cycle_id.to_string(),
            operation_id: observation.operation_id.clone(),
            observation_id: None,
            profile: profile.to_string(),
            component,
            prior_evidence: prior_evidence.clone(),
            observed_evidence: None,
            disposition: None,
            phase: NativeIdentityRebindCyclePhase::AwaitingCutover,
            updated_at_unix_ms: existing.updated_at_unix_ms,
        };
        if !native_identity_rebind_cycle_matches(&existing, &sealed) {
            return Err(invalid_native_identity_rebind(
                request_id,
                "observe does not match the provider-sealed awaiting_cutover predecessor",
            ));
        }
        write_native_identity_rebind_cycle(host, &observation, request_id)?;
        return Ok(observation.phase);
    }
    if existing.phase == NativeIdentityRebindCyclePhase::AwaitingHostDrain {
        return Err(invalid_native_identity_rebind(
            request_id,
            "observe requires a provider-sealed awaiting_cutover predecessor for the exact rebind cycle",
        ));
    }
    if native_identity_rebind_cycle_matches(&existing, &observation) {
        return Ok(existing.phase);
    }
    Err(invalid_native_identity_rebind(
        request_id,
        "the rebind cycle already owns a different admitted observation",
    ))
}

fn write_native_identity_rebind_cycle(
    host: &HostContext,
    cycle: &NativeIdentityRebindCycleRecord,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let path = native_identity_rebind_cycle_path(
        host,
        &cycle.cycle_id,
        &cycle.profile,
        cycle.component,
        request_id,
    )?;
    let parent = path
        .parent()
        .expect("native identity rebind cycle always has a parent");
    durable_fs::create_private_directories(parent)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    let bytes = serde_json::to_vec_pretty(&cycle)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    if bytes.len() > NATIVE_IDENTITY_REBIND_STATE_BYTES {
        return Err(native_identity_rebind_state_failure(
            request_id,
            format!(
                "cycle record exceeds supported {NATIVE_IDENTITY_REBIND_STATE_BYTES}-byte bound"
            ),
        ));
    }
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    temporary
        .write_all(&bytes)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    temporary
        .persist(&path)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error.error))?;
    durable_fs::sync_directory(parent)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))
}

fn native_identity_rebind_cycle_matches(
    left: &NativeIdentityRebindCycleRecord,
    right: &NativeIdentityRebindCycleRecord,
) -> bool {
    left.schema_version == right.schema_version
        && left.cycle_id == right.cycle_id
        && left.operation_id == right.operation_id
        && left.observation_id == right.observation_id
        && left.profile == right.profile
        && left.component == right.component
        && left.prior_evidence == right.prior_evidence
        && left.observed_evidence == right.observed_evidence
        && left.disposition == right.disposition
}

fn read_native_identity_rebind_cycle(
    host: &HostContext,
    cycle_id: &str,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<Option<NativeIdentityRebindCycleRecord>, ProviderFailure> {
    let path = native_identity_rebind_cycle_path(host, cycle_id, profile, component, request_id)?;
    let bytes = match durable_fs::read_file_bounded(&path, NATIVE_IDENTITY_REBIND_STATE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(native_identity_rebind_state_failure(request_id, error)),
    };
    let cycle: NativeIdentityRebindCycleRecord = serde_json::from_slice(&bytes)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    validate_native_identity_rebind_cycle_record(&cycle, cycle_id, profile, component, request_id)?;
    Ok(Some(cycle))
}

fn validate_native_identity_rebind_cycle_record(
    cycle: &NativeIdentityRebindCycleRecord,
    cycle_id: &str,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let phase_binding_valid = match (
        cycle.phase,
        &cycle.observation_id,
        &cycle.observed_evidence,
        cycle.disposition,
    ) {
        (
            NativeIdentityRebindCyclePhase::AwaitingHostDrain
            | NativeIdentityRebindCyclePhase::AwaitingCutover,
            None,
            None,
            None,
        ) => true,
        (
            NativeIdentityRebindCyclePhase::AwaitingHostRelease,
            Some(observation_id),
            Some(observed_evidence),
            Some(disposition),
        ) => {
            observation_id
                == &native_identity_rebind_observation_id(
                    &cycle.operation_id,
                    observed_evidence,
                    disposition,
                )
        }
        (
            NativeIdentityRebindCyclePhase::Completed,
            Some(observation_id),
            Some(observed_evidence),
            Some(NativeIdentityRebindDisposition::Committed),
        ) => {
            observation_id
                == &native_identity_rebind_observation_id(
                    &cycle.operation_id,
                    observed_evidence,
                    NativeIdentityRebindDisposition::Committed,
                )
        }
        (
            NativeIdentityRebindCyclePhase::RolledBack,
            Some(observation_id),
            Some(observed_evidence),
            Some(NativeIdentityRebindDisposition::RolledBack),
        ) => {
            observation_id
                == &native_identity_rebind_observation_id(
                    &cycle.operation_id,
                    observed_evidence,
                    NativeIdentityRebindDisposition::RolledBack,
                )
        }
        _ => false,
    };
    if cycle.schema_version != NATIVE_IDENTITY_REBIND_STATE_SCHEMA_VERSION
        || cycle.cycle_id != cycle_id
        || cycle.profile != profile
        || cycle.component != component
        || cycle.updated_at_unix_ms == 0
        || !phase_binding_valid
        || cycle.operation_id
            != native_identity_rebind_operation_id(
                cycle_id,
                profile,
                component,
                &cycle.prior_evidence,
            )
    {
        return Err(native_identity_rebind_state_failure(
            request_id,
            "persisted cycle record is inconsistent",
        ));
    }
    Ok(())
}

fn prepare_native_identity_rebind_cycle_slot(
    host: &HostContext,
    cycle_id: &str,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let root = native_identity_rebind_component_state_root(host, profile, component, request_id)?;
    durable_fs::create_private_directories(&root)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    let now = now_unix_ms();
    let mut retained = 0_usize;
    for entry in fs::read_dir(&root)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?
    {
        let entry =
            entry.map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
        if !entry
            .file_type()
            .map_err(|error| native_identity_rebind_state_failure(request_id, error))?
            .is_file()
            || entry.path().extension().and_then(|value| value.to_str()) != Some("json")
        {
            continue;
        }
        let retained_cycle_id = entry
            .path()
            .file_stem()
            .and_then(|value| value.to_str())
            .filter(|value| valid_sha256(value))
            .ok_or_else(|| {
                native_identity_rebind_state_failure(
                    request_id,
                    "persisted cycle path has an invalid cycle identity",
                )
            })?
            .to_string();
        let path = confined_native_identity_rebind_target(host, &entry.path(), request_id)?;
        let bytes = durable_fs::read_file_bounded(&path, NATIVE_IDENTITY_REBIND_STATE_BYTES)
            .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
        let cycle: NativeIdentityRebindCycleRecord = serde_json::from_slice(&bytes)
            .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
        validate_native_identity_rebind_cycle_record(
            &cycle,
            &retained_cycle_id,
            profile,
            component,
            request_id,
        )?;
        if native_identity_rebind_cycle_expired(&cycle, now) {
            fs::remove_file(&path)
                .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
            durable_fs::sync_directory(&root)
                .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
            if retained_cycle_id == cycle_id {
                return Err(invalid_native_identity_rebind(
                    request_id,
                    "the rebind cycle replay window expired; begin a new plan request",
                ));
            }
        } else {
            retained = retained.saturating_add(1);
        }
    }
    let target = native_identity_rebind_cycle_path(host, cycle_id, profile, component, request_id)?;
    if retained >= MAX_NATIVE_IDENTITY_REBIND_CYCLES_PER_COMPONENT && !target.exists() {
        return Err(native_identity_rebind_capacity_failure(request_id));
    }
    Ok(())
}

fn native_identity_rebind_cycle_expired(cycle: &NativeIdentityRebindCycleRecord, now: u64) -> bool {
    matches!(
        cycle.phase,
        NativeIdentityRebindCyclePhase::AwaitingHostDrain
            | NativeIdentityRebindCyclePhase::Completed
            | NativeIdentityRebindCyclePhase::RolledBack
    ) && cycle
        .updated_at_unix_ms
        .saturating_add(NATIVE_IDENTITY_REBIND_REPLAY_WINDOW_MS)
        < now
}

fn acquire_native_identity_rebind_lock(
    host: &HostContext,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<fs::File, ProviderFailure> {
    let root = native_identity_rebind_state_root(host, request_id)?;
    durable_fs::create_private_directories(&root)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    let lock_path = confined_native_identity_rebind_target(
        host,
        &root.join(format!("{profile}-{}.lock", component.as_str())),
        request_id,
    )?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = options
        .open(lock_path)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?;
    let timeout = operation_bounds::remaining_timeout(
        host.deadline_unix_ms,
        NATIVE_IDENTITY_REBIND_LOCK_TIMEOUT,
    )
    .ok_or_else(|| native_identity_rebind_lock_timeout(request_id))?;
    if !operation_bounds::lock_exclusive_for(&lock, timeout)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))?
    {
        return Err(native_identity_rebind_lock_timeout(request_id));
    }
    Ok(lock)
}

fn native_identity_rebind_cycle_path(
    host: &HostContext,
    cycle_id: &str,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let root = native_identity_rebind_component_state_root(host, profile, component, request_id)?;
    confined_native_identity_rebind_target(host, &root.join(format!("{cycle_id}.json")), request_id)
}

fn native_identity_rebind_component_state_root(
    host: &HostContext,
    profile: &str,
    component: NativeIdentityRebindComponent,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let root = native_identity_rebind_state_root(host, request_id)?;
    confined_native_identity_rebind_target(
        host,
        &root.join(format!("{profile}-{}", component.as_str())),
        request_id,
    )
}

fn native_identity_rebind_state_root(
    host: &HostContext,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = native_identity_rebind_data_root(host, request_id)?;
    confined_native_identity_rebind_target(
        host,
        &data_root.join(NATIVE_IDENTITY_REBIND_STATE_DIR),
        request_id,
    )
}

fn native_identity_rebind_data_root<'a>(
    host: &'a HostContext,
    request_id: &str,
) -> Result<&'a Path, ProviderFailure> {
    host.data_root
        .as_deref()
        .filter(|root| !root.trim().is_empty())
        .map(Path::new)
        .ok_or_else(|| {
            invalid_native_identity_rebind(
                request_id,
                "native identity rebind requires host.data_root for durable observation custody",
            )
        })
}

fn confined_native_identity_rebind_target(
    host: &HostContext,
    target: &Path,
    request_id: &str,
) -> Result<PathBuf, ProviderFailure> {
    let data_root = native_identity_rebind_data_root(host, request_id)?;
    path_guard::confined_target(data_root, target)
        .map_err(|error| native_identity_rebind_state_failure(request_id, error))
}

fn native_identity_rebind_state_failure(
    request_id: &str,
    error: impl std::fmt::Display,
) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "native_identity_rebind_state_failed",
        format!("native identity rebind observation custody failed: {error}"),
    )
}

fn native_identity_rebind_lock_timeout(request_id: &str) -> ProviderFailure {
    ProviderFailure::internal(
        request_id,
        "native_identity_rebind_lock_timeout",
        "native identity rebind observation lock could not be acquired before the operation deadline",
    )
}

fn native_identity_rebind_capacity_failure(request_id: &str) -> ProviderFailure {
    ProviderFailure::conflict(
        request_id,
        "native_identity_rebind_cycle_capacity",
        format!(
            "native identity rebind retains at most {MAX_NATIVE_IDENTITY_REBIND_CYCLES_PER_COMPONENT} active obligations and terminal replays per profile/component; terminal replays expire after the bounded replay window"
        ),
        json!({
            "maximum_cycles_per_component": MAX_NATIVE_IDENTITY_REBIND_CYCLES_PER_COMPONENT,
            "replay_window_ms": NATIVE_IDENTITY_REBIND_REPLAY_WINDOW_MS,
        }),
    )
}

fn native_identity_rebind_cycle_id(
    plan_request_id: &str,
    profile: &str,
    component: NativeIdentityRebindComponent,
) -> String {
    sha256_hex(
        json!({
            "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
            "plan_request_id": plan_request_id,
            "profile": profile,
            "component": component.as_str(),
        })
        .to_string()
        .as_bytes(),
    )
}

fn validate_native_identity_rebind_cycle_id(
    cycle_id: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if valid_sha256(cycle_id) {
        return Ok(());
    }
    Err(invalid_native_identity_rebind(
        request_id,
        "cycle_id must be a lowercase SHA-256 value emitted by a plan response",
    ))
}

fn native_identity_rebind_operation_id(
    cycle_id: &str,
    profile: &str,
    component: NativeIdentityRebindComponent,
    prior_evidence: &NativeIdentityRebindEvidence,
) -> String {
    sha256_hex(
        json!({
            "protocol": NATIVE_IDENTITY_REBIND_PROTOCOL,
            "cycle_id": cycle_id,
            "profile": profile,
            "component": component.as_str(),
            "prior_evidence": prior_evidence,
        })
        .to_string()
        .as_bytes(),
    )
}

fn identity_component_rebound(prior: Option<&str>, observed: Option<&str>) -> bool {
    observed.is_some() && observed != prior
}

fn canonical_rebind_profile(
    profile: &str,
    request_id: &str,
) -> Result<&'static crate::account::AccountProfile, ProviderFailure> {
    profile_for_wrapper_reference(profile)
        .filter(|account| account.opencode_wrapper == profile)
        .ok_or_else(|| {
            invalid_native_identity_rebind(
                request_id,
                format!("native identity rebind requires a canonical profile: {profile}"),
            )
        })
}

fn require_native_identity_rebind_protocol(
    protocol: &str,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    if protocol == NATIVE_IDENTITY_REBIND_PROTOCOL {
        return Ok(());
    }
    Err(invalid_native_identity_rebind(
        request_id,
        format!("unsupported native identity rebind protocol: {protocol}"),
    ))
}

fn validate_native_identity_evidence(
    evidence: &NativeIdentityRebindEvidence,
    request_id: &str,
) -> Result<(), ProviderFailure> {
    let component_valid = evidence
        .component_identity_sha256
        .as_deref()
        .is_none_or(valid_sha256);
    let record_valid = evidence
        .state_record_sha256
        .as_deref()
        .is_none_or(valid_sha256);
    let presence_matches =
        evidence.component_identity_sha256.is_some() == evidence.state_record_sha256.is_some();
    if component_valid && record_valid && presence_matches {
        return Ok(());
    }
    Err(invalid_native_identity_rebind(
        request_id,
        "component evidence must contain paired lowercase semantic-identity and state-record SHA-256 values, or two null values",
    ))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn invalid_native_identity_rebind(request_id: &str, message: impl Into<String>) -> ProviderFailure {
    ProviderFailure::invalid_request(request_id, "invalid_native_identity_rebind", message)
}
