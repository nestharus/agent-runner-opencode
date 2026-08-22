//! Declared roles: orchestration

mod cluster_d;
#[allow(dead_code)]
mod support;

use cluster_d::*;
use jsonschema::{Draft, JSONSchema};
use serde_json::{json, Value};
use std::fs;
use std::fs::OpenOptions;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use support::{
    invoke, invoke_validated, invoke_validated_with_host, invoke_validated_with_host_and_env,
};

static IO_INTENSIVE_CONTRACT_LOCK: Mutex<()> = Mutex::new(());

fn lock_io_intensive_contract() -> MutexGuard<'static, ()> {
    IO_INTENSIVE_CONTRACT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn contract_settings_crud() {
    let host = HostRoots::new("agent-runner-opencode-settings-crud");
    let create = success_result(
        invoke_validated_with_host(
            "settings.create",
            settings_create_params(Some(SECRET_TOKEN)),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsCreateRequest",
        ),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
    assert_settings_create_result(&create);
    let id = settings_create_id(&create);
    let created_version = settings_create_version(&create);

    let list = success_result(
        invoke_validated_with_host(
            "settings.list",
            empty_request_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsListRequest",
        ),
        "settings.schema.json#/$defs/SettingsListResponse",
        "settings.schema.json#/$defs/SettingsListResult",
    );
    assert_settings_list_result(&list, &id, &created_version);

    let get = success_result(
        invoke_validated_with_host(
            "settings.get",
            settings_get_params(&id),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsGetRequest",
        ),
        "settings.schema.json#/$defs/SettingsGetResponse",
        "settings.schema.json#/$defs/SettingsGetResult",
    );
    assert_settings_get_result(&get, &id, &created_version);

    let update_response = success_response(
        invoke_validated_with_host(
            "settings.update",
            settings_update_params(&id, &created_version, Some(UPDATE_SECRET_TOKEN)),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsUpdateRequest",
        ),
        "settings.schema.json#/$defs/SettingsUpdateResponse",
        "settings.schema.json#/$defs/SettingsUpdateResult",
    );
    assert_settings_update_response(&update_response, &created_version);
    let updated_version = settings_update_version(&update_response);

    let stale = invoke_validated_with_host(
        "settings.update",
        settings_update_params(&id, &created_version, None),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsUpdateRequest",
    );
    let stale_response = error_response(stale);
    assert_stale_settings_response(&stale_response);

    let delete = success_result(
        invoke_validated_with_host(
            "settings.delete",
            settings_delete_params(&id, &updated_version),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsDeleteRequest",
        ),
        "settings.schema.json#/$defs/SettingsDeleteResponse",
        "settings.schema.json#/$defs/SettingsDeleteResult",
    );
    assert_settings_delete_result(&delete, &id);
    assert_settings_history(&host, 3);
}

#[test]
fn contract_settings_mutations_replay_committed_results_after_response_loss() {
    struct RejectWrites;

    impl std::io::Write for RejectWrites {
        fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "contract output handoff failure",
            ))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let host = HostRoots::new("agent-runner-opencode-settings-mutation-replay");
    let mut create_request = support::validated_request_envelope(
        "settings.create",
        settings_create_params(None),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsCreateRequest",
    );
    create_request["request_id"] = json!("req-settings-create-response-loss");
    support::assert_valid_request_envelope(
        &create_request,
        "settings.schema.json#/$defs/SettingsCreateRequest",
    );
    let args = vec![
        "agent-runner-opencode".to_string(),
        "settings.create".to_string(),
    ];
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&create_request).expect("serialize create request"),
            &mut RejectWrites,
        ),
        1,
        "the first response handoff should fail after the mutation commits"
    );

    let created = success_result(
        support::invoke_with_request("settings.create", create_request.clone()),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
    let created_again = success_result(
        support::invoke_with_request("settings.create", create_request.clone()),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
    assert_eq!(created_again, created);
    let id = settings_create_id(&created);
    let created_version = settings_create_version(&created);

    let mut conflicting_create = create_request;
    conflicting_create["params"]["display_name"] = json!("different request binding");
    let conflict = error_response(support::invoke_with_request(
        "settings.create",
        conflicting_create,
    ));
    assert_eq!(
        conflict["error"]["code"],
        "settings_mutation_request_conflict"
    );

    let mut update_request = support::validated_request_envelope(
        "settings.update",
        settings_update_params(&id, &created_version, Some(UPDATE_SECRET_TOKEN)),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsUpdateRequest",
    );
    update_request["request_id"] = json!("req-settings-update-response-loss");
    let updated = success_result(
        support::invoke_with_request("settings.update", update_request.clone()),
        "settings.schema.json#/$defs/SettingsUpdateResponse",
        "settings.schema.json#/$defs/SettingsUpdateResult",
    );
    let updated_again = success_result(
        support::invoke_with_request("settings.update", update_request),
        "settings.schema.json#/$defs/SettingsUpdateResponse",
        "settings.schema.json#/$defs/SettingsUpdateResult",
    );
    assert_eq!(updated_again, updated);

    let mut delete_request = support::validated_request_envelope(
        "settings.delete",
        settings_delete_params(
            &id,
            updated["record"]["version"]
                .as_str()
                .expect("updated version"),
        ),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsDeleteRequest",
    );
    delete_request["request_id"] = json!("req-settings-delete-response-loss");
    let deleted = success_result(
        support::invoke_with_request("settings.delete", delete_request.clone()),
        "settings.schema.json#/$defs/SettingsDeleteResponse",
        "settings.schema.json#/$defs/SettingsDeleteResult",
    );
    let deleted_again = success_result(
        support::invoke_with_request("settings.delete", delete_request),
        "settings.schema.json#/$defs/SettingsDeleteResponse",
        "settings.schema.json#/$defs/SettingsDeleteResult",
    );
    assert_eq!(deleted_again, deleted);

    let mut migrate_request = support::validated_request_envelope(
        "settings.migrate",
        json!({ "dry_run": false, "legacy": legacy_fixture() }),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsMigrateRequest",
    );
    migrate_request["request_id"] = json!("req-settings-migrate-response-loss");
    let migrated = success_result(
        support::invoke_with_request("settings.migrate", migrate_request.clone()),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );
    let migrated_again = success_result(
        support::invoke_with_request("settings.migrate", migrate_request),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );
    assert_eq!(migrated_again, migrated);

    let store: serde_json::Value = serde_json::from_slice(
        &fs::read(
            host.config_root()
                .join("agent-runner-opencode/settings-store.json"),
        )
        .expect("settings store"),
    )
    .expect("settings store JSON");
    assert_eq!(
        store["records"].as_array().expect("settings records").len(),
        5,
        "migration replay must not repeat its five exact caller activations"
    );
    assert_eq!(
        store["history"].as_array().expect("settings history").len(),
        4
    );
    assert_eq!(
        store["mutation_receipts"]
            .as_object()
            .expect("settings mutation receipts")
            .len(),
        4
    );
}

#[test]
fn contract_settings_parallel_creates_do_not_lose_records() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-settings-parallel-create");
    let host_overrides = host.overrides();
    let workers = (0..8)
        .map(|worker_index| {
            let host_overrides = host_overrides.clone();
            thread::spawn(move || {
                let mut request = support::validated_request_envelope(
                    "settings.create",
                    settings_create_params(None),
                    host_overrides,
                    "settings.schema.json#/$defs/SettingsCreateRequest",
                );
                request["request_id"] =
                    json!(format!("req-settings-create-parallel-{worker_index}"));
                support::assert_valid_request_envelope(
                    &request,
                    "settings.schema.json#/$defs/SettingsCreateRequest",
                );
                let mut last_lock_timeout = None;
                for _ in 0..8 {
                    let output = support::invoke_with_request("settings.create", request.clone());
                    if output.status.success() {
                        return output;
                    }
                    let response = support::json_stdout(&output);
                    if response["error"]["code"] != "settings_store_lock_timeout" {
                        return output;
                    }
                    last_lock_timeout = Some(output);
                }
                last_lock_timeout.expect("at least one bounded settings lock attempt")
            })
        })
        .collect::<Vec<_>>();
    let mut ids = std::collections::BTreeSet::new();
    for worker in workers {
        let result = success_result(
            worker.join().expect("settings.create worker"),
            "settings.schema.json#/$defs/SettingsCreateResponse",
            "settings.schema.json#/$defs/SettingsCreateResult",
        );
        ids.insert(settings_create_id(&result));
    }
    assert_eq!(ids.len(), 8, "parallel creates need unique identities");
    let list = success_result(
        invoke_validated_with_host(
            "settings.list",
            empty_request_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsListRequest",
        ),
        "settings.schema.json#/$defs/SettingsListResponse",
        "settings.schema.json#/$defs/SettingsListResult",
    );
    assert_eq!(list["records"].as_array().expect("records").len(), 8);
    assert_settings_history(&host, 8);
}

#[test]
fn contract_settings_parallel_updates_preserve_optimistic_concurrency() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-settings-parallel-update");
    let created = success_result(
        invoke_validated_with_host(
            "settings.create",
            settings_create_params(None),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsCreateRequest",
        ),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
    let id = settings_create_id(&created);
    let version = settings_create_version(&created);
    let workers = (0..2)
        .map(|worker_index| {
            let host_overrides = host.overrides();
            let id = id.clone();
            let version = version.clone();
            thread::spawn(move || {
                let mut request = support::validated_request_envelope(
                    "settings.update",
                    settings_update_params(&id, &version, None),
                    host_overrides,
                    "settings.schema.json#/$defs/SettingsUpdateRequest",
                );
                request["request_id"] =
                    json!(format!("req-settings-update-parallel-{worker_index}"));
                support::assert_valid_request_envelope(
                    &request,
                    "settings.schema.json#/$defs/SettingsUpdateRequest",
                );
                let mut last_lock_timeout = None;
                for _ in 0..8 {
                    let output = support::invoke_with_request("settings.update", request.clone());
                    if output.status.success() {
                        return output;
                    }
                    let response = support::json_stdout(&output);
                    if response["error"]["code"] != "settings_store_lock_timeout" {
                        return output;
                    }
                    last_lock_timeout = Some(output);
                }
                last_lock_timeout.expect("at least one bounded settings lock attempt")
            })
        })
        .collect::<Vec<_>>();
    let outputs = workers
        .into_iter()
        .map(|worker| worker.join().expect("settings.update worker"))
        .collect::<Vec<_>>();
    assert_eq!(
        outputs
            .iter()
            .filter(|output| output.status.success())
            .count(),
        1,
        "exactly one update may consume a settings version"
    );
    for output in outputs.iter().filter(|output| !output.status.success()) {
        let response = support::json_stdout(output);
        assert_eq!(response["error"]["category"], "conflict", "{response}");
    }
    assert_settings_history(&host, 2);
}

#[test]
fn contract_generated_settings_id_is_usable_by_policy() {
    let host = HostRoots::new("agent-runner-opencode-settings-policy-identity");
    let created = success_result(
        invoke_validated_with_host(
            "settings.create",
            settings_create_params(None),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsCreateRequest",
        ),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
    let settings_id = settings_create_id(&created);
    let expected_settings_id = settings_id.clone();
    let install = success_result(
        invoke_validated_with_host(
            "setup.install_plan",
            json!({ "target": "local", "settings_id": settings_id.clone() }),
            host.overrides(),
            "setup.schema.json#/$defs/SetupInstallPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupInstallPlanResponse",
        "setup.schema.json#/$defs/SetupInstallPlanResult",
    );
    let activation = install["steps"]
        .as_array()
        .expect("install steps")
        .iter()
        .find(|step| step["kind"] == "verify_settings_transition")
        .expect("settings activation step");
    assert_eq!(activation["blocking"], false);
    assert_eq!(
        activation["settings_store"]["required_settings_ids"],
        json!([expected_settings_id.clone()])
    );
    assert!(activation["settings_store"]["missing_settings_ids"]
        .as_array()
        .expect("missing settings IDs")
        .is_empty());
    let policy = success_result(
        invoke_validated_with_host(
            "policy.evaluate",
            json!({
                "settings_id": settings_id,
                "mode": "agent",
                "model": {
                    "name": "gpt-high",
                    "provider_args": ["-m", "openai/gpt-5.6-sol", "--variant", "high"],
                    "inputs": {
                        "prompt": "ok",
                        "named": {}
                    }
                },
                "launch": {
                    "argv": [
                        "opencode1", "run", "--dangerously-skip-permissions",
                        "-m", "openai/gpt-5.6-sol", "--variant", "high", "ok"
                    ]
                }
            }),
            host.overrides(),
            "policy.schema.json#/$defs/PolicyEvaluateRequest",
        ),
        "policy.schema.json#/$defs/PolicyEvaluateResponse",
        "policy.schema.json#/$defs/PolicyEvaluateResult",
    );
    assert_eq!(policy["accepted"], true, "{policy}");
    assert!(
        policy["diagnostics"]
            .as_array()
            .expect("policy diagnostics")
            .is_empty(),
        "{policy}"
    );
    let identity = policy["markers"]
        .as_array()
        .expect("policy markers")
        .iter()
        .find(|marker| marker["name"] == "opencode.settings_record_identity")
        .expect("settings record identity marker")["value"]
        .as_str()
        .expect("settings record identity text");
    assert!(identity.starts_with("settings record "), "{identity}");
    assert!(identity.contains(" at version "), "{identity}");

    let events = read_activity_events(&host);
    let completed_policy = events
        .iter()
        .find(|event| event["subcommand"] == "policy.evaluate" && event["phase"] == "completed")
        .expect("completed policy activity event");
    let identities = activity_identities(completed_policy);
    for (kind, value, provenance) in [
        (
            "settings_record",
            expected_settings_id.as_str(),
            "policy.route.settings_record",
        ),
        ("account", "opencode1", "policy.route.account"),
        (
            "provider_model",
            "openai/gpt-5.6-sol",
            "policy.route.provider_model",
        ),
        ("model_alias", "gpt-high", "policy.route.model_alias"),
        ("effort", "high", "policy.route.effort"),
    ] {
        assert!(identities.iter().any(|identity| {
            identity["kind"] == kind
                && identity["value"] == value
                && identity["status"] == "resolved"
                && identity["provenance"] == provenance
        }));
    }
}

#[test]
fn contract_account_reference_is_not_an_implicit_settings_record() {
    let host = HostRoots::new("agent-runner-opencode-no-implicit-settings-record");
    let response = error_response(invoke_validated_with_host(
        "policy.evaluate",
        json!({
            "settings_id": "opencode1",
            "mode": "agent",
            "model": {
                "name": "gpt-low",
                "provider_args": ["-m", "openai/gpt-5.6-sol", "--variant", "low"],
                "inputs": { "prompt": "ok", "named": {} }
            },
            "launch": {
                "argv": [
                    "opencode1", "run", "--dangerously-skip-permissions",
                    "-m", "openai/gpt-5.6-sol", "--variant", "low", "ok"
                ]
            }
        }),
        host.overrides(),
        "policy.schema.json#/$defs/PolicyEvaluateRequest",
    ));
    assert_eq!(response["error"]["code"], "unknown_settings_id");
    assert!(response["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("persisted OpenCode settings record")));
}

#[test]
fn contract_activity_evidence_is_redacted_private_and_hash_chained() {
    let host = HostRoots::new("agent-runner-opencode-activity-ledger");
    let created = success_result(
        invoke_validated_with_host(
            "settings.create",
            settings_create_params(Some(SECRET_TOKEN)),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsCreateRequest",
        ),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
    let created_id = settings_create_id(&created);
    let ledger_path = host
        .data_root()
        .join("provider-state/opencode/activity/operations.jsonl");
    let ledger = fs::read_to_string(&ledger_path).expect("activity ledger");
    assert!(!ledger.contains(SECRET_TOKEN));
    assert!(!ledger.contains("auth_token"));
    let events = ledger
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("activity JSON"))
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert!(activity_identities(&events[1]).iter().any(|identity| {
        identity["kind"] == "settings_record"
            && identity["value"] == created_id
            && identity["status"] == "generated"
            && identity["provenance"] == "result.record.id"
    }));
    let mut previous = String::new();
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["sequence"], index + 1);
        assert_eq!(event["previous_event_sha256"], previous);
        assert!(event["authenticated_principal"].is_null());
        assert!(event["delegation"].is_null());
        let recorded = event["event_sha256"]
            .as_str()
            .expect("event digest")
            .to_string();
        let mut unhashed = event.clone();
        unhashed
            .as_object_mut()
            .expect("activity object")
            .remove("event_sha256");
        assert_eq!(recorded, sha256_hex(unhashed.to_string().as_bytes()));
        previous = recorded;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&ledger_path)
                .expect("activity metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn contract_activity_preserves_all_conflicting_session_identity_carriers() {
    let host = HostRoots::new("agent-runner-opencode-activity-session-conflict");
    let response = error_response(invoke_validated_with_host(
        "session.capture",
        json!({
            "settings_id": "opencode1",
            "session_id": "ses_bare_conflict",
            "launch": {
                "session": {
                    "provider_session_id": "ses_launch_conflict",
                    "source": "opencode.run.format_json"
                }
            },
            "pinned_target": "ses_pinned_conflict",
            "start_bound_provider_session_id": "ses_start_bound_conflict"
        }),
        host.overrides(),
        "session.schema.json#/$defs/SessionCaptureRequest",
    ));
    assert_eq!(response["error"]["code"], "invalid_session_capture_params");
    let events = read_activity_events(&host);
    assert_eq!(events.len(), 2);
    let identities = activity_identities(&events[1]);
    for (value, provenance) in [
        (
            "ses_launch_conflict",
            "params.launch.session.provider_session_id",
        ),
        ("ses_bare_conflict", "params.session_id"),
        ("ses_pinned_conflict", "params.pinned_target"),
        (
            "ses_start_bound_conflict",
            "params.start_bound_provider_session_id",
        ),
    ] {
        assert!(identities.iter().any(|identity| {
            identity["kind"] == "provider_session"
                && identity["value"] == value
                && identity["status"] == "attempted"
                && identity["provenance"] == provenance
        }));
    }
}

#[test]
fn contract_activity_keeps_rotation_alias_and_canonical_account_identity() {
    let host = HostRoots::new("agent-runner-opencode-activity-rotation-alias");
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(false),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let events = read_activity_events(&host);
    assert_eq!(events.len(), 2);
    let identities = activity_identities(&events[1]);
    assert!(identities.iter().any(|identity| {
        identity["kind"] == "account"
            && identity["value"] == "opencode"
            && identity["status"] == "attempted"
            && identity["provenance"] == "params.source_account"
    }));
    assert!(identities.iter().any(|identity| {
        identity["kind"] == "account"
            && identity["value"] == "opencode1"
            && identity["status"] == "resolved"
            && identity["provenance"] == "params.source_account.catalog"
    }));
}

#[test]
fn contract_corrupt_activity_evidence_warns_without_denial() {
    let host = HostRoots::new("agent-runner-opencode-activity-best-effort");
    let activity_root = host.data_root().join("provider-state/opencode/activity");
    fs::create_dir_all(&activity_root).expect("create corrupt activity root");
    let ledger_path = activity_root.join("operations.jsonl");
    fs::write(&ledger_path, b"not-json\n").expect("write corrupt activity ledger");

    let output = invoke_validated_with_host(
        "describe",
        json!({}),
        host.overrides(),
        "describe.schema.json#/$defs/DescribeRequest",
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("provider activity start evidence warning"),
        "activity failure must be observable: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let _ = success_result(
        output,
        "describe.schema.json#/$defs/DescribeResponse",
        "describe.schema.json#/$defs/DescribeResult",
    );
    assert_eq!(
        fs::read(&ledger_path).expect("read unchanged corrupt ledger"),
        b"not-json\n"
    );
}

#[test]
fn contract_busy_activity_evidence_never_blocks_capability_dispatch() {
    let host = HostRoots::new("agent-runner-opencode-activity-nonblocking");
    let activity_root = host.data_root().join("provider-state/opencode/activity");
    fs::create_dir_all(&activity_root).expect("create activity root");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(activity_root.join(".operations.lock"))
        .expect("open activity lock");
    fs2::FileExt::lock_exclusive(&lock).expect("hold activity lock");

    let started = std::time::Instant::now();
    let output = invoke_validated_with_host(
        "describe",
        json!({}),
        host.overrides(),
        "describe.schema.json#/$defs/DescribeRequest",
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(60),
        "busy best-effort activity evidence must not delay capability dispatch"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("activity start evidence warning"),
        "skipped evidence must remain observable"
    );
    let _ = success_result(
        output,
        "describe.schema.json#/$defs/DescribeResponse",
        "describe.schema.json#/$defs/DescribeResult",
    );
}

#[test]
fn contract_settings_capacity_rejection_preserves_existing_routes() {
    let host = HostRoots::new("agent-runner-opencode-settings-capacity");
    let store_root = host.config_root().join("agent-runner-opencode");
    fs::create_dir_all(&store_root).expect("create settings capacity root");
    let values = opencode_settings_values(None);
    let records = (0..256)
        .map(|index| {
            json!({
                "id": format!("bounded-{index}"),
                "display_name": format!("Bounded profile {index}"),
                "version": "fixture-v1",
                "values": values,
            })
        })
        .collect::<Vec<_>>();
    fs::write(
        store_root.join("settings-store.json"),
        serde_json::to_vec(&json!({
            "schema_version": 3,
            "records": records,
            "history": [],
            "mutation_receipts": {},
        }))
        .expect("serialize bounded settings store"),
    )
    .expect("write bounded settings store");

    let rejected = error_response(invoke_validated_with_host(
        "settings.create",
        settings_create_params(None),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsCreateRequest",
    ));
    assert_eq!(rejected["error"]["code"], "settings_capacity_exhausted");

    let existing = success_result(
        invoke_validated_with_host(
            "settings.get",
            settings_get_params("bounded-0"),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsGetRequest",
        ),
        "settings.schema.json#/$defs/SettingsGetResponse",
        "settings.schema.json#/$defs/SettingsGetResult",
    );
    assert_eq!(existing["record"]["id"], "bounded-0");
}

#[test]
fn contract_oversized_predecessor_store_stays_routable_during_in_band_recovery() {
    let host = HostRoots::new("agent-runner-opencode-settings-predecessor-capacity-recovery");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create predecessor settings root");
    let mut values = opencode_settings_values(None);
    values["extra_env"] = json!({
        "PREDECESSOR_PAYLOAD": "x".repeat(15 * 1024 * 1024)
    });
    let predecessor_bytes = serde_json::to_vec(&json!({
        "records": [{
            "id": "predecessor-0",
            "display_name": "Predecessor profile",
            "version": "predecessor-v1",
            "values": values,
        }]
    }))
    .expect("serialize predecessor store");
    assert!(predecessor_bytes.len() > 4 * 1024 * 1024);
    assert!(predecessor_bytes.len() < 16 * 1024 * 1024);
    assert!(predecessor_bytes.starts_with(br#"{"records":["#));
    fs::write(&store_path, &predecessor_bytes).expect("write predecessor settings store");

    let list = success_result(
        invoke_validated_with_host(
            "settings.list",
            empty_request_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsListRequest",
        ),
        "settings.schema.json#/$defs/SettingsListResponse",
        "settings.schema.json#/$defs/SettingsListResult",
    );
    assert_eq!(
        list["records"].as_array().expect("settings records").len(),
        1
    );

    let policy = success_result(
        invoke_validated_with_host(
            "policy.evaluate",
            json!({
                "settings_id": "predecessor-0",
                "mode": "agent",
                "model": {
                    "name": "gpt-high",
                    "provider_args": ["-m", "openai/gpt-5.6-sol", "--variant", "high"],
                    "inputs": { "prompt": "ok", "named": {} }
                },
                "launch": {
                    "argv": [
                        "opencode1", "run", "--dangerously-skip-permissions",
                        "-m", "openai/gpt-5.6-sol", "--variant", "high", "ok"
                    ]
                }
            }),
            host.overrides(),
            "policy.schema.json#/$defs/PolicyEvaluateRequest",
        ),
        "policy.schema.json#/$defs/PolicyEvaluateResponse",
        "policy.schema.json#/$defs/PolicyEvaluateResult",
    );
    assert_eq!(policy["accepted"], true, "{policy}");

    let _ = success_result(
        invoke_validated_with_host(
            "settings.update",
            settings_update_params("predecessor-0", "predecessor-v1", None),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsUpdateRequest",
        ),
        "settings.schema.json#/$defs/SettingsUpdateResponse",
        "settings.schema.json#/$defs/SettingsUpdateResult",
    );
    let recovered_bytes = fs::read(&store_path).expect("read recovered settings store");
    assert!(recovered_bytes.len() <= 4 * 1024 * 1024);
    let recovered: serde_json::Value =
        serde_json::from_slice(&recovered_bytes).expect("parse recovered settings store");
    assert_eq!(recovered["schema_version"], 3);

    let remaining = success_result(
        invoke_validated_with_host(
            "settings.get",
            settings_get_params("predecessor-0"),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsGetRequest",
        ),
        "settings.schema.json#/$defs/SettingsGetResponse",
        "settings.schema.json#/$defs/SettingsGetResult",
    );
    assert_eq!(remaining["record"]["id"], "predecessor-0");
}

#[test]
fn contract_predecessor_recovery_never_publishes_above_its_readable_envelope() {
    let host = HostRoots::new("agent-runner-opencode-settings-predecessor-publication-bound");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create predecessor settings root");
    let make_store = |padding: usize| {
        let mut large_values = opencode_settings_values(None);
        large_values["extra_env"] = json!({
            "PREDECESSOR_PAYLOAD": "x".repeat(padding)
        });
        let records = json!([
            {
                "id": "large-predecessor",
                "display_name": "Large predecessor profile",
                "version": "predecessor-v1",
                "values": large_values,
            },
            {
                "id": "small-predecessor",
                "display_name": "Small predecessor profile",
                "version": "predecessor-v1",
                "values": opencode_settings_values(None),
            }
        ]);
        format!(
            "{{\"schema_version\":0,\"records\":{},\"history\":[],\"mutation_receipts\":{{}}}}",
            serde_json::to_string(&records).expect("serialize predecessor records")
        )
        .into_bytes()
    };
    let maximum = 16 * 1024 * 1024;
    let base = make_store(0).len();
    let predecessor_bytes = make_store(maximum - base - 32);
    assert!(predecessor_bytes.starts_with(br#"{"schema_version":0,"records":["#));
    assert!(predecessor_bytes.len() <= maximum);
    assert!(predecessor_bytes.len() > maximum - 64);
    fs::write(&store_path, &predecessor_bytes).expect("write near-bound predecessor store");

    let rejected = error_response(invoke_validated_with_host(
        "settings.delete",
        settings_delete_params("small-predecessor", "predecessor-v1"),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsDeleteRequest",
    ));
    assert_eq!(rejected["error"]["code"], "settings_capacity_exhausted");
    assert_eq!(
        fs::read(&store_path).expect("read unchanged predecessor store"),
        predecessor_bytes,
        "a fully serialized result above the transition envelope must not replace the last readable store"
    );

    let _ = success_result(
        invoke_validated_with_host(
            "settings.delete",
            settings_delete_params("large-predecessor", "predecessor-v1"),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsDeleteRequest",
        ),
        "settings.schema.json#/$defs/SettingsDeleteResponse",
        "settings.schema.json#/$defs/SettingsDeleteResult",
    );
    let recovered_bytes = fs::read(&store_path).expect("read recovered settings store");
    assert!(recovered_bytes.len() <= 4 * 1024 * 1024);
    let remaining = success_result(
        invoke_validated_with_host(
            "settings.get",
            settings_get_params("small-predecessor"),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsGetRequest",
        ),
        "settings.schema.json#/$defs/SettingsGetResponse",
        "settings.schema.json#/$defs/SettingsGetResult",
    );
    assert_eq!(remaining["record"]["id"], "small-predecessor");
}

#[test]
fn contract_predecessor_store_above_transition_envelope_fails_explicitly() {
    let host = HostRoots::new("agent-runner-opencode-settings-predecessor-capacity-unsupported");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create predecessor settings root");
    let mut values = opencode_settings_values(None);
    values["extra_env"] = json!({
        "PREDECESSOR_PAYLOAD": "x".repeat(16 * 1024 * 1024)
    });
    let predecessor_bytes = serde_json::to_vec(&json!({
        "records": [{
            "id": "unsupported-predecessor-0",
            "display_name": "Unsupported predecessor profile",
            "version": "predecessor-v1",
            "values": values,
        }]
    }))
    .expect("serialize predecessor store above transition envelope");
    assert!(predecessor_bytes.len() > 16 * 1024 * 1024);
    fs::write(&store_path, predecessor_bytes).expect("write unsupported predecessor store");

    let rejected = error_response(invoke_validated_with_host(
        "settings.list",
        empty_request_params(),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsListRequest",
    ));
    assert_eq!(
        rejected["error"]["code"],
        "settings_store_capacity_unsupported"
    );
}

#[test]
fn contract_predecessor_store_above_transition_record_envelope_fails_explicitly() {
    let host = HostRoots::new("agent-runner-opencode-settings-predecessor-record-unsupported");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create predecessor settings root");
    let values = opencode_settings_values(None);
    let records = (0..4_097)
        .map(|index| {
            json!({
                "id": format!("unsupported-record-{index}"),
                "display_name": format!("Unsupported predecessor profile {index}"),
                "version": "predecessor-v1",
                "values": values,
            })
        })
        .collect::<Vec<_>>();
    let predecessor_bytes =
        serde_json::to_vec(&json!({ "records": records })).expect("serialize predecessor store");
    assert!(predecessor_bytes.len() < 16 * 1024 * 1024);
    fs::write(&store_path, predecessor_bytes).expect("write unsupported predecessor store");

    let rejected = error_response(invoke_validated_with_host(
        "settings.list",
        empty_request_params(),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsListRequest",
    ));
    assert_eq!(
        rejected["error"]["code"],
        "settings_store_capacity_unsupported"
    );
}

#[test]
fn contract_oversized_current_schema_cannot_use_predecessor_prefix_admission() {
    let host = HostRoots::new("agent-runner-opencode-settings-current-schema-prefix");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create settings root");
    let mut values = opencode_settings_values(None);
    values["extra_env"] = json!({
        "CURRENT_SCHEMA_PAYLOAD": "x".repeat(5 * 1024 * 1024)
    });
    let record = serde_json::to_string(&json!({
        "id": "current-schema-prefix-0",
        "display_name": "Current schema prefix",
        "version": "current-v1",
        "values": values,
    }))
    .expect("serialize current-schema record");
    let bytes = format!(
        "{{\"records\":[{record}],\"schema_version\":3,\"history\":[],\"mutation_receipts\":{{}}}}"
    )
    .into_bytes();
    assert!(bytes.starts_with(br#"{"records":["#));
    assert!(bytes.len() > 4 * 1024 * 1024);
    assert!(bytes.len() < 16 * 1024 * 1024);
    fs::write(&store_path, bytes).expect("write reordered current-schema store");

    let rejected = error_response(invoke_validated_with_host(
        "settings.list",
        empty_request_params(),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsListRequest",
    ));
    assert_eq!(
        rejected["error"]["code"],
        "settings_store_capacity_unsupported"
    );
}

#[test]
fn contract_record_heavy_predecessor_recovery_requires_one_step_into_current_capacity() {
    let host = HostRoots::new("agent-runner-opencode-settings-predecessor-record-recovery");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create predecessor settings root");
    let values = opencode_settings_values(None);
    let records = (0..258)
        .map(|index| {
            json!({
                "id": format!("record-heavy-{index}"),
                "display_name": format!("Record-heavy profile {index}"),
                "version": "predecessor-v1",
                "values": values,
            })
        })
        .collect::<Vec<_>>();
    let predecessor_bytes =
        serde_json::to_vec(&json!({ "records": records })).expect("serialize predecessor store");
    assert!(predecessor_bytes.len() < 4 * 1024 * 1024);
    fs::write(&store_path, &predecessor_bytes).expect("write predecessor settings store");

    let rejected = error_response(invoke_validated_with_host(
        "settings.delete",
        settings_delete_params("record-heavy-0", "predecessor-v1"),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsDeleteRequest",
    ));
    assert_eq!(rejected["error"]["code"], "settings_capacity_exhausted");
    assert_eq!(
        fs::read(&store_path).expect("read unchanged record-heavy predecessor store"),
        predecessor_bytes
    );
    let install = success_result(
        invoke_validated_with_host(
            "setup.install_plan",
            setup_install_plan_params(
                &path_string(host.data_root()),
                &path_string(host.config_root()),
            ),
            host.overrides(),
            "setup.schema.json#/$defs/SetupInstallPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupInstallPlanResponse",
        "setup.schema.json#/$defs/SetupInstallPlanResult",
    );
    let transition = install["steps"]
        .as_array()
        .expect("setup install steps")
        .iter()
        .find(|step| step["kind"] == "verify_settings_transition")
        .expect("settings transition step");
    assert_eq!(transition["blocking"], true);
    assert_eq!(
        transition["settings_store"]["state"],
        "predecessor_reduction_required"
    );
    assert_eq!(
        transition["settings_store"]["code"],
        "settings_predecessor_reduction_required"
    );

    let records = (0..257)
        .map(|index| {
            json!({
                "id": format!("record-heavy-{index}"),
                "display_name": format!("Record-heavy profile {index}"),
                "version": "predecessor-v1",
                "values": values,
            })
        })
        .collect::<Vec<_>>();
    fs::write(
        &store_path,
        serde_json::to_vec(&json!({ "records": records }))
            .expect("serialize one-step predecessor store"),
    )
    .expect("write one-step predecessor store");
    let _ = success_result(
        invoke_validated_with_host(
            "settings.delete",
            settings_delete_params("record-heavy-0", "predecessor-v1"),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsDeleteRequest",
        ),
        "settings.schema.json#/$defs/SettingsDeleteResponse",
        "settings.schema.json#/$defs/SettingsDeleteResult",
    );
    let persisted: serde_json::Value =
        serde_json::from_slice(&fs::read(&store_path).expect("read one-step recovered store"))
            .expect("parse one-step recovered store");
    assert_eq!(persisted["schema_version"], 3);

    let list = success_result(
        invoke_validated_with_host(
            "settings.list",
            empty_request_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsListRequest",
        ),
        "settings.schema.json#/$defs/SettingsListResponse",
        "settings.schema.json#/$defs/SettingsListResult",
    );
    assert_eq!(
        list["records"].as_array().expect("settings records").len(),
        256
    );
}

fn assert_settings_history(host: &HostRoots, expected_events: usize) {
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    let bytes = fs::read(&store_path).expect("settings store");
    let store: serde_json::Value = serde_json::from_slice(&bytes).expect("settings store JSON");
    let history = store["history"].as_array().expect("settings history");
    assert_eq!(history.len(), expected_events);
    let mut previous = "";
    for (index, event) in history.iter().enumerate() {
        assert_eq!(event["sequence"], index + 1);
        assert_eq!(event["previous_event_sha256"], previous);
        previous = event["event_sha256"]
            .as_str()
            .expect("history event digest");
        assert_eq!(previous.len(), 64);
    }
    let serialized = String::from_utf8(bytes).expect("settings store UTF-8");
    assert!(!serialized.contains(SECRET_TOKEN));
    assert!(!serialized.contains(UPDATE_SECRET_TOKEN));
}

fn read_activity_events(host: &HostRoots) -> Vec<serde_json::Value> {
    fs::read_to_string(
        host.data_root()
            .join("provider-state/opencode/activity/operations.jsonl"),
    )
    .expect("activity ledger")
    .lines()
    .map(|line| serde_json::from_str(line).expect("activity JSON"))
    .collect()
}

fn activity_identities(event: &serde_json::Value) -> &[serde_json::Value] {
    event["targets"]["identities"]
        .as_array()
        .expect("typed activity identities")
}

#[test]
fn contract_prior_settings_store_is_upgraded_without_losing_identity() {
    let host = HostRoots::new("agent-runner-opencode-settings-prior-store-upgrade");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create prior settings store parent");
    let mut prior_values = opencode_settings_values(None);
    prior_values["quota"] = json!({
        "source": "codex",
        "auth_path": "~/.codex/auth.json",
        "usage_command": "chatgpt-usage"
    });
    fs::write(
        &store_path,
        serde_json::to_vec(&json!({
            "records": [{
                "id": "prior-settings-id",
                "display_name": "Prior settings",
                "version": "prior-version",
                "values": prior_values
            }]
        }))
        .expect("serialize prior settings store"),
    )
    .expect("write prior settings store");

    let get = success_result(
        invoke_validated_with_host(
            "settings.get",
            settings_get_params("prior-settings-id"),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsGetRequest",
        ),
        "settings.schema.json#/$defs/SettingsGetResponse",
        "settings.schema.json#/$defs/SettingsGetResult",
    );
    assert_eq!(get["record"]["id"], "prior-settings-id");
    assert_eq!(get["record"]["version"], "prior-version");
    assert_normalized_account_settings_record(
        &get["record"],
        "opencode1",
        "~/.local/share/opencode/auth.json",
    );

    let policy = success_result(
        invoke_validated_with_host(
            "policy.evaluate",
            json!({
                "settings_id": "prior-settings-id",
                "mode": "agent",
                "model": {
                    "name": "gpt-high",
                    "provider_args": ["-m", "openai/gpt-5.6-sol", "--variant", "high"],
                    "inputs": { "prompt": "ok", "named": {} }
                },
                "launch": {
                    "argv": [
                        "opencode1", "run", "--dangerously-skip-permissions",
                        "-m", "openai/gpt-5.6-sol", "--variant", "high", "ok"
                    ]
                }
            }),
            host.overrides(),
            "policy.schema.json#/$defs/PolicyEvaluateRequest",
        ),
        "policy.schema.json#/$defs/PolicyEvaluateResponse",
        "policy.schema.json#/$defs/PolicyEvaluateResult",
    );
    assert_eq!(policy["accepted"], true, "{policy}");

    let updated = success_result(
        invoke_validated_with_host(
            "settings.update",
            settings_update_params("prior-settings-id", "prior-version", None),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsUpdateRequest",
        ),
        "settings.schema.json#/$defs/SettingsUpdateResponse",
        "settings.schema.json#/$defs/SettingsUpdateResult",
    );
    assert_eq!(updated["record"]["id"], "prior-settings-id");
    let persisted: serde_json::Value =
        serde_json::from_slice(&fs::read(&store_path).expect("read upgraded settings store"))
            .expect("parse upgraded settings store");
    assert_eq!(persisted["schema_version"], 3);
    assert_eq!(persisted["records"][0]["id"], "prior-settings-id");
    assert_eq!(
        persisted["records"][0]["values"]["quota"]["source"],
        "opencode_auth"
    );
    assert!(persisted["records"][0]["values"]["quota"]
        .get("usage_command")
        .is_none());
}

#[test]
fn contract_predecessor_model_tuple_and_unroutable_record_do_not_block_shared_store() {
    let host = HostRoots::new("agent-runner-opencode-settings-predecessor-quarantine");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create predecessor settings store parent");
    let mut tuple_values = opencode_settings_values(None);
    tuple_values["model"]
        .as_object_mut()
        .expect("model object")
        .remove("name");
    let unroutable_values = json!({
        "provider": "opencode",
        "wrapper": "retired-opencode-profile",
        "model": {
            "provider_model": "openai/gpt-5.6-sol",
            "variant": "high"
        },
        "quota": { "auth_path": "/retired/auth.json" }
    });
    fs::write(
        &store_path,
        serde_json::to_vec(&json!({
            "records": [
                {
                    "id": "tuple-settings-id",
                    "display_name": "Predecessor tuple settings",
                    "version": "tuple-version",
                    "values": tuple_values
                },
                {
                    "id": "repair-required-id",
                    "display_name": "Unroutable predecessor settings",
                    "version": "repair-version",
                    "values": unroutable_values
                }
            ]
        }))
        .expect("serialize predecessor mixed store"),
    )
    .expect("write predecessor mixed store");

    let list = success_result(
        invoke_validated_with_host(
            "settings.list",
            empty_request_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsListRequest",
        ),
        "settings.schema.json#/$defs/SettingsListResponse",
        "settings.schema.json#/$defs/SettingsListResult",
    );
    let records = list["records"].as_array().expect("settings summaries");
    assert_eq!(records.len(), 2);
    let quarantined = records
        .iter()
        .find(|record| record["id"] == "repair-required-id")
        .expect("repair-required predecessor summary");
    assert_eq!(
        quarantined["summary"]["migration"]["status"],
        "repair_required"
    );
    assert!(!quarantined["summary"]["migration"]["diagnostics"]
        .as_array()
        .expect("repair diagnostics")
        .is_empty());

    let tuple = success_result(
        invoke_validated_with_host(
            "settings.get",
            settings_get_params("tuple-settings-id"),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsGetRequest",
        ),
        "settings.schema.json#/$defs/SettingsGetResponse",
        "settings.schema.json#/$defs/SettingsGetResult",
    );
    assert_eq!(tuple["record"]["version"], "tuple-version");
    assert_eq!(tuple["record"]["values"]["model"]["name"], "gpt-high");

    let policy = success_result(
        invoke_validated_with_host(
            "policy.evaluate",
            json!({
                "settings_id": "tuple-settings-id",
                "mode": "agent",
                "model": {
                    "name": "gpt-high",
                    "provider_args": ["-m", "openai/gpt-5.6-sol", "--variant", "high"],
                    "inputs": { "prompt": "ok", "named": {} }
                },
                "launch": {
                    "argv": [
                        "opencode1", "run", "--dangerously-skip-permissions",
                        "-m", "openai/gpt-5.6-sol", "--variant", "high", "ok"
                    ]
                }
            }),
            host.overrides(),
            "policy.schema.json#/$defs/PolicyEvaluateRequest",
        ),
        "policy.schema.json#/$defs/PolicyEvaluateResponse",
        "policy.schema.json#/$defs/PolicyEvaluateResult",
    );
    assert_eq!(policy["accepted"], true, "{policy}");

    let deleted = success_result(
        invoke_validated_with_host(
            "settings.delete",
            settings_delete_params("repair-required-id", "repair-version"),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsDeleteRequest",
        ),
        "settings.schema.json#/$defs/SettingsDeleteResponse",
        "settings.schema.json#/$defs/SettingsDeleteResult",
    );
    assert_eq!(deleted["deleted"], true);

    let persisted: serde_json::Value =
        serde_json::from_slice(&fs::read(&store_path).expect("read repaired settings store"))
            .expect("parse repaired settings store");
    assert_eq!(persisted["schema_version"], 3);
    assert_eq!(persisted["records"].as_array().expect("records").len(), 1);
    assert_eq!(persisted["records"][0]["id"], "tuple-settings-id");
}

#[test]
fn contract_settings_create_rejects_path_shaped_account_references() {
    let host = HostRoots::new("agent-runner-opencode-settings-reject-path-accounts");
    for (wrapper, _) in normalized_account_cases() {
        let mut request = support::validated_request_envelope(
            "settings.create",
            settings_create_params_for_values(path_wrapped_opencode_settings_values(wrapper)),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsCreateRequest",
        );
        request["request_id"] = json!(format!("req-settings-create-reject-path-{wrapper}"));
        let response = error_response(support::invoke_with_request("settings.create", request));
        assert_eq!(response["error"]["code"], "settings_validation_failed");
        assert!(response["error"]["details"]["diagnostics"]
            .as_array()
            .expect("settings diagnostics")
            .iter()
            .any(|diagnostic| diagnostic["code"] == "invalid_wrapper"));
    }
}

#[test]
fn contract_settings_validate() {
    let host = HostRoots::new("agent-runner-opencode-settings-validate");
    let valid = success_result(
        invoke_validated_with_host(
            "settings.validate",
            valid_settings_validate_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsValidateRequest",
        ),
        "settings.schema.json#/$defs/SettingsValidateResponse",
        "settings.schema.json#/$defs/SettingsValidateResult",
    );
    assert_settings_valid_result(&valid);

    let invalid = success_result(
        invoke_validated_with_host(
            "settings.validate",
            invalid_settings_validate_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsValidateRequest",
        ),
        "settings.schema.json#/$defs/SettingsValidateResponse",
        "settings.schema.json#/$defs/SettingsValidateResult",
    );
    assert_settings_invalid_result(&invalid);
}

#[test]
fn contract_settings_validate_accepts_luna_max_and_rejects_mismatched_identity() {
    let host = HostRoots::new("agent-runner-opencode-settings-validate-luna");
    let valid = success_result(
        invoke_validated_with_host(
            "settings.validate",
            luna_settings_validate_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsValidateRequest",
        ),
        "settings.schema.json#/$defs/SettingsValidateResponse",
        "settings.schema.json#/$defs/SettingsValidateResult",
    );
    assert_settings_valid_result(&valid);

    let invalid = success_result(
        invoke_validated_with_host(
            "settings.validate",
            mismatched_luna_settings_validate_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsValidateRequest",
        ),
        "settings.schema.json#/$defs/SettingsValidateResponse",
        "settings.schema.json#/$defs/SettingsValidateResult",
    );
    assert_settings_invalid_model_result(&invalid, "invalid_provider_model");
}

fn normalized_account_cases() -> [(&'static str, &'static str); 5] {
    [
        ("opencode1", "~/.local/share/opencode/auth.json"),
        ("opencode2", "~/.opencode2/opencode/auth.json"),
        ("opencode3", "~/.opencode3/opencode/auth.json"),
        ("opencode4", "~/.opencode4/opencode/auth.json"),
        ("opencode5", "~/.opencode5/opencode/auth.json"),
    ]
}

#[test]
fn contract_settings_migrate() {
    let host = HostRoots::new("agent-runner-opencode-settings-migrate");
    let before = snapshot_tree(host.config_root());
    let dry_run = success_result(
        invoke_validated_with_host(
            "settings.migrate",
            settings_migrate_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsMigrateRequest",
        ),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );
    assert_settings_migrate_result(&dry_run, host.config_root(), &before);
    let expected_ids = [
        "opencode",
        "opencode2",
        "opencode3",
        "opencode4",
        "opencode5",
    ];
    assert_eq!(
        dry_run["actions"]
            .as_array()
            .expect("migration actions")
            .iter()
            .filter_map(|action| action["settings_id"].as_str())
            .collect::<Vec<_>>(),
        expected_ids,
        "dry-run actions must declare the exact caller IDs that apply will activate"
    );

    let applied = success_result(
        invoke_validated_with_host(
            "settings.migrate",
            json!({ "dry_run": false, "legacy": legacy_fixture() }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsMigrateRequest",
        ),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );
    assert_eq!(applied["actions"], dry_run["actions"]);
    let listed = success_result(
        invoke_validated_with_host(
            "settings.list",
            empty_request_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsListRequest",
        ),
        "settings.schema.json#/$defs/SettingsListResponse",
        "settings.schema.json#/$defs/SettingsListResult",
    );
    assert_eq!(
        listed["records"]
            .as_array()
            .expect("activated settings records")
            .iter()
            .filter_map(|record| record["id"].as_str())
            .collect::<Vec<_>>(),
        expected_ids,
        "apply must materialize the same exact record IDs consumed by legacy provider-name callers"
    );
}

#[test]
fn contract_setup_activation_population_is_not_bounded_by_account_count() {
    let host = HostRoots::new("agent-runner-opencode-setup-caller-population");
    let providers_toml = r#"
[opencode]
command = "opencode1"

[opencode1]
command = "opencode1"

[opencode2]
command = "opencode2"

[opencode3]
command = "opencode3"

[opencode4]
command = "opencode4"

[opencode5]
command = "opencode5"
"#;
    let expected_ids = [
        "opencode",
        "opencode1",
        "opencode2",
        "opencode3",
        "opencode4",
        "opencode5",
    ];
    success_result(
        invoke_validated_with_host(
            "settings.migrate",
            json!({
                "dry_run": false,
                "legacy": { "providers_toml": providers_toml }
            }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsMigrateRequest",
        ),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );

    let install = success_result(
        invoke_validated_with_host(
            "setup.install_plan",
            json!({ "target": "local", "settings_ids": expected_ids }),
            host.overrides(),
            "setup.schema.json#/$defs/SetupInstallPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupInstallPlanResponse",
        "setup.schema.json#/$defs/SetupInstallPlanResult",
    );
    let activation = install["steps"]
        .as_array()
        .expect("install steps")
        .iter()
        .find(|step| step["kind"] == "verify_settings_transition")
        .expect("settings activation step");
    assert_eq!(activation["blocking"], false);
    assert_eq!(
        activation["settings_store"]["required_settings_ids"],
        json!(expected_ids)
    );
    assert!(activation["settings_store"]["missing_settings_ids"]
        .as_array()
        .expect("missing settings IDs")
        .is_empty());
}

#[test]
fn contract_settings_migrate_rejects_path_shaped_legacy_accounts() {
    let host = HostRoots::new("agent-runner-opencode-settings-migrate-account-admission");
    let providers_toml = r#"
[legacy_alias]
command = "/tmp/bin/opencode"

[legacy_two]
command = "/tmp/bin/opencode2"

[legacy_unknown]
command = "/tmp/bin/opencode9"
"#;
    let legacy = json!({
        "providers_toml": providers_toml,
        "models": { "gpt-high.toml": MODEL_TOML }
    });
    let dry_run = success_result(
        invoke_validated_with_host(
            "settings.migrate",
            json!({ "dry_run": true, "legacy": legacy.clone() }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsMigrateRequest",
        ),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );
    let action_providers = dry_run["actions"]
        .as_array()
        .expect("migration actions")
        .iter()
        .filter_map(|action| action["provider"].as_str())
        .collect::<Vec<_>>();
    assert!(action_providers.is_empty());
    assert_eq!(dry_run["requires_user_input"], true);
    assert_eq!(
        dry_run["diagnostics"]
            .as_array()
            .expect("migration diagnostics")
            .iter()
            .filter(|diagnostic| diagnostic["code"] == "legacy_provider_unknown")
            .count(),
        3
    );

    let apply = invoke_validated_with_host(
        "settings.migrate",
        json!({ "dry_run": false, "legacy": legacy }),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsMigrateRequest",
    );
    let error = error_response(apply);
    assert_eq!(error["error"]["code"], "settings_validation_failed");
    assert!(
        !host
            .config_root()
            .join("agent-runner-opencode/settings-store.json")
            .exists(),
        "invalid legacy account must not be mapped or persisted"
    );
}

#[test]
fn contract_setup_detect_install_sync() {
    let host = HostRoots::new("agent-runner-opencode-setup");
    let toolchain = FakeToolchain::new();
    for wrapper in opencode_wrappers() {
        fs::remove_file(toolchain.dir().join(wrapper))
            .expect("remove logical wrapper executable from setup fixture");
    }
    let home = HomeFixture::new("agent-runner-opencode-setup-home");
    home.write_all_opencode_auths();
    let profile_root = host.data_root().join("provider-profile-root-contract");
    fs::create_dir_all(&profile_root).expect("create provider profile root fixture");
    fs::write(
        host.data_root().join("data-root-contract-present"),
        b"data root present\n",
    )
    .expect("write data root presence sentinel");
    fs::write(
        profile_root.join("profile-root-contract-present"),
        b"profile root present\n",
    )
    .expect("write profile root presence sentinel");
    let data_root = path_string(host.data_root());
    let profile_root = path_string(&profile_root);
    let path = prepend_path(toolchain.dir());

    let before_activation = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            setup_detect_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );
    assert_eq!(before_activation["installed"], false);
    assert!(before_activation["warnings"]
        .as_array()
        .expect("setup warnings")
        .iter()
        .any(|warning| warning
            .as_str()
            .is_some_and(|warning| warning.contains("settings activation is incomplete"))));

    success_result(
        invoke_validated_with_host(
            "settings.migrate",
            json!({ "dry_run": false, "legacy": legacy_fixture() }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsMigrateRequest",
        ),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );

    let detect = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            setup_detect_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );
    assert_setup_detect_installed(&detect, &data_root, &profile_root);

    let install = success_result(
        invoke_validated_with_host_and_env(
            "setup.install_plan",
            setup_install_plan_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupInstallPlanRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupInstallPlanResponse",
        "setup.schema.json#/$defs/SetupInstallPlanResult",
    );
    assert_setup_install_result(&install);

    let sync = success_result(
        invoke_validated_with_host_and_env(
            "setup.sync_plan",
            setup_sync_plan_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    assert_setup_sync_result(&sync);
}

#[test]
fn contract_setup_sync_accepts_only_declared_account_references() {
    let host = HostRoots::new("agent-runner-opencode-setup-account-admission");
    let result = success_result(
        invoke_validated_with_host(
            "setup.sync_plan",
            json!({
                "desired_profiles": ["/tmp/bin/opencode2", "opencode", "opencode9"],
                "settings_schema_id": "opencode.settings/v1"
            }),
            host.overrides(),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    let profiles = result["operations"]
        .as_array()
        .expect("setup operations")
        .iter()
        .map(|operation| operation["profile"].as_str().expect("canonical profile"))
        .collect::<Vec<_>>();
    assert_eq!(profiles, vec!["opencode1"]);
    assert_eq!(
        result["diagnostics"]
            .as_array()
            .expect("setup diagnostics")
            .iter()
            .filter(|diagnostic| diagnostic["code"] == "unknown_opencode_profile")
            .count(),
        2
    );
}

#[test]
fn contract_setup_sync_plans_bounded_identity_rebind() {
    let result = success_result(
        invoke_validated(
            "setup.sync_plan",
            setup_sync_rebind_params(),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    let rebinds = result["operations"]
        .as_array()
        .expect("sync operations")
        .iter()
        .filter(|operation| operation["kind"] == "native_identity_rebind")
        .collect::<Vec<_>>();
    assert_eq!(rebinds.len(), 2);
    assert_eq!(rebinds[0]["component"], "native_runtime");
    assert_eq!(rebinds[1]["component"], "quota_observer");
    assert_ne!(rebinds[0]["cycle_id"], rebinds[1]["cycle_id"]);
    assert_ne!(rebinds[0]["operation_id"], rebinds[1]["operation_id"]);
    assert_ne!(
        rebinds[0]["implementation_evidence"]["provider_state_record"],
        rebinds[1]["implementation_evidence"]["provider_state_record"]
    );
    let rebind = rebinds[0];
    assert_eq!(rebind["profile"], "opencode3");
    assert_eq!(rebind["component"], "native_runtime");
    assert_eq!(rebind["protocol"], "opencode.native-identity-rebind/v1");
    assert_eq!(rebind["schema_id"], "opencode.native-identity-rebind/v1");
    assert_eq!(
        rebind["cycle_id"]
            .as_str()
            .expect("rebind cycle identity")
            .len(),
        64
    );
    assert_eq!(
        rebind["operation_id"]
            .as_str()
            .expect("rebind operation identity")
            .len(),
        64
    );
    assert_eq!(rebind["phase"], "awaiting_host_drain");
    assert_eq!(rebind["maximum_drain_ms"], 20_000);
    assert!(rebind["responsibilities"]
        .as_array()
        .expect("typed actor responsibilities")
        .iter()
        .any(|responsibility| responsibility["actor"] == "host"));
    assert_eq!(rebind["next_request"]["action"], "seal");

    let protocol = success_result(
        invoke(
            "schema",
            json!({ "schema_id": "opencode.native-identity-rebind/v1" }),
        ),
        "schema.schema.json#/$defs/SchemaResponse",
        "schema.schema.json#/$defs/SchemaResult",
    )["schema"]
        .clone();
    assert_native_identity_rebind_schema(
        &protocol,
        "Request",
        &setup_sync_rebind_params()["native_identity_rebind"],
    );
    for operation in &rebinds {
        assert_native_identity_rebind_schema(&protocol, "Operation", operation);
    }
    assert_native_identity_rebind_schema(&protocol, "Operation", rebind);
    assert_native_identity_rebind_schema(&protocol, "Request", &rebind["next_request"]);

    let sealed = success_result(
        invoke_validated(
            "setup.sync_plan",
            json!({
                "settings_schema_id": "opencode.settings/v1",
                "desired_profiles": ["opencode3"],
                "native_identity_rebind": rebind["next_request"]
            }),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    let sealed = sealed["operations"]
        .as_array()
        .expect("sync operations")
        .iter()
        .find(|operation| operation["kind"] == "native_identity_rebind")
        .expect("sealed identity rebind operation");
    assert_eq!(sealed["phase"], "awaiting_cutover");
    assert_eq!(sealed["next_request"]["action"], "observe");
    assert_eq!(
        sealed["next_request"]["host_handoff"]["ordinary_admission_blocked"],
        true
    );
    assert_native_identity_rebind_schema(&protocol, "Operation", sealed);
    assert_native_identity_rebind_schema(&protocol, "Request", &sealed["next_request"]);

    let mut rollback_observation = sealed["next_request"].clone();
    rollback_observation["disposition"] = json!("rolled_back");
    let observation = success_result(
        invoke_validated(
            "setup.sync_plan",
            json!({
                "settings_schema_id": "opencode.settings/v1",
                "desired_profiles": ["opencode3"],
                "native_identity_rebind": rollback_observation
            }),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    let observed = observation["operations"]
        .as_array()
        .expect("sync operations")
        .iter()
        .find(|operation| operation["kind"] == "native_identity_rebind")
        .expect("identity rebind observation");
    assert_eq!(observed["operation_id"], rebind["operation_id"]);
    assert_eq!(observed["phase"], "awaiting_host_release");
    assert_eq!(observed["next_request"]["action"], "release");
    assert_eq!(
        observed["next_request"]["host_handoff"]["ordinary_admission_blocked"],
        true
    );
    assert_native_identity_rebind_schema(&protocol, "Operation", observed);
    assert_native_identity_rebind_schema(&protocol, "Request", &observed["next_request"]);

    let release_request = observed["next_request"].clone();
    let mut reopened_too_early = release_request.clone();
    reopened_too_early["host_handoff"]["ordinary_admission_blocked"] = json!(false);
    let rejected_release = success_result(
        invoke_validated(
            "setup.sync_plan",
            json!({
                "settings_schema_id": "opencode.settings/v1",
                "desired_profiles": ["opencode3"],
                "native_identity_rebind": reopened_too_early
            }),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    let rejected_release = native_identity_rebind_component(&rejected_release, "native_runtime");
    assert_eq!(rejected_release["phase"], "rejected");
    assert!(rejected_release.get("release_authorization").is_none());
    assert_native_identity_rebind_schema(&protocol, "Operation", rejected_release);

    let released = success_result(
        invoke_validated(
            "setup.sync_plan",
            json!({
                "settings_schema_id": "opencode.settings/v1",
                "desired_profiles": ["opencode3"],
                "native_identity_rebind": release_request
            }),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    let released = released["operations"]
        .as_array()
        .expect("sync operations")
        .iter()
        .find(|operation| operation["kind"] == "native_identity_rebind")
        .expect("released identity rebind operation");
    assert_eq!(released["operation_id"], rebind["operation_id"]);
    assert_eq!(released["observation_id"], observed["observation_id"]);
    assert_eq!(released["phase"], "rolled_back");
    assert_eq!(
        released["release_authorization"]["ordinary_admission_may_reopen"],
        true
    );
    assert_native_identity_rebind_schema(&protocol, "Operation", released);

    let replayed = success_result(
        invoke_validated(
            "setup.sync_plan",
            json!({
                "settings_schema_id": "opencode.settings/v1",
                "desired_profiles": ["opencode3"],
                "native_identity_rebind": release_request
            }),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    let replayed = replayed["operations"]
        .as_array()
        .expect("sync operations")
        .iter()
        .find(|operation| operation["kind"] == "native_identity_rebind")
        .expect("replayed released identity rebind operation");
    assert_eq!(replayed, released);
}

#[test]
fn contract_native_identity_rebind_rejects_unadmitted_release_authority() {
    let host = HostRoots::new("agent-runner-opencode-rebind-unadmitted-release");
    let planned = native_identity_rebind_step(&host, setup_sync_rebind_params());
    let planned = native_identity_rebind_component(&planned, "native_runtime");
    let sealed = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": planned["next_request"]
        }),
    );
    let sealed = native_identity_rebind_component(&sealed, "native_runtime");
    let rejected = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": sealed["next_request"]
        }),
    );
    let rejected = native_identity_rebind_component(&rejected, "native_runtime");
    assert_eq!(rejected["phase"], "rejected");
    assert_eq!(rejected["disposition"], "committed");

    let rejected_release = native_identity_rebind_release_request(rejected);
    let rejected_release_response = error_response(invoke_validated_with_host(
        "setup.sync_plan",
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": rejected_release
        }),
        host.overrides(),
        "setup.schema.json#/$defs/SetupSyncPlanRequest",
    ));
    assert_eq!(
        rejected_release_response["error"]["code"],
        "invalid_native_identity_rebind"
    );

    let skipped_host = HostRoots::new("agent-runner-opencode-rebind-skipped-observe");
    let skipped_release_response = error_response(invoke_validated_with_host(
        "setup.sync_plan",
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": rejected_release
        }),
        skipped_host.overrides(),
        "setup.schema.json#/$defs/SetupSyncPlanRequest",
    ));
    assert_eq!(
        skipped_release_response["error"]["code"],
        "invalid_native_identity_rebind"
    );
}

#[test]
fn contract_native_identity_rebind_rejects_false_rollback_and_changed_evidence() {
    let rejected_host = HostRoots::new("agent-runner-opencode-rebind-false-rollback");
    let planned = native_identity_rebind_step(&rejected_host, setup_sync_rebind_params());
    let planned = native_identity_rebind_component(&planned, "native_runtime");
    let sealed = native_identity_rebind_step(
        &rejected_host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": planned["next_request"]
        }),
    );
    let sealed = native_identity_rebind_component(&sealed, "native_runtime");
    let replacement = FakeToolchain::new();
    write_native_runtime_identity(rejected_host.data_root(), &replacement, "opencode3");
    let mut false_rollback = sealed["next_request"].clone();
    false_rollback["disposition"] = json!("rolled_back");
    let rejected = native_identity_rebind_step(
        &rejected_host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": false_rollback
        }),
    );
    let rejected = native_identity_rebind_component(&rejected, "native_runtime");
    assert_eq!(rejected["phase"], "rejected");
    assert_eq!(rejected["disposition"], "rolled_back");
    let false_rollback_release = native_identity_rebind_release_request(rejected);
    let release_response = error_response(invoke_validated_with_host(
        "setup.sync_plan",
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": false_rollback_release
        }),
        rejected_host.overrides(),
        "setup.schema.json#/$defs/SetupSyncPlanRequest",
    ));
    assert_eq!(
        release_response["error"]["code"],
        "invalid_native_identity_rebind"
    );

    let host = HostRoots::new("agent-runner-opencode-rebind-evidence-change");
    let prior = FakeToolchain::new();
    write_native_runtime_identity(host.data_root(), &prior, "opencode3");
    let planned = native_identity_rebind_step(&host, setup_sync_rebind_params());
    let planned = native_identity_rebind_component(&planned, "native_runtime");
    let sealed = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": planned["next_request"]
        }),
    );
    let sealed = native_identity_rebind_component(&sealed, "native_runtime");
    let mut rollback = sealed["next_request"].clone();
    rollback["disposition"] = json!("rolled_back");
    let observed = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": rollback
        }),
    );
    let observed = native_identity_rebind_component(&observed, "native_runtime");
    assert_eq!(observed["phase"], "awaiting_host_release");
    let release = observed["next_request"].clone();

    let changed = FakeToolchain::new();
    write_native_runtime_identity(host.data_root(), &changed, "opencode3");
    let rejected = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": release
        }),
    );
    assert_eq!(
        native_identity_rebind_component(&rejected, "native_runtime")["phase"],
        "rejected"
    );
    assert!(
        native_identity_rebind_component(&rejected, "native_runtime")
            .get("release_authorization")
            .is_none()
    );

    write_native_runtime_identity(host.data_root(), &prior, "opencode3");
    let released = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": release
        }),
    );
    let released = native_identity_rebind_component(&released, "native_runtime").clone();
    assert_eq!(released["phase"], "rolled_back");
    assert_eq!(
        released["release_authorization"]["ordinary_admission_may_reopen"],
        true
    );

    write_native_runtime_identity(host.data_root(), &changed, "opencode3");
    let replayed = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": release
        }),
    );
    assert_eq!(
        native_identity_rebind_component(&replayed, "native_runtime"),
        &released,
        "a durably terminal release must replay without consulting later component state"
    );
}

#[test]
fn contract_native_identity_rebind_distinguishes_identical_maintenance_cycles() {
    let host = HostRoots::new("agent-runner-opencode-rebind-distinct-cycles");
    let mut first_plan_request = support::validated_request_envelope(
        "setup.sync_plan",
        setup_sync_rebind_params(),
        host.overrides(),
        "setup.schema.json#/$defs/SetupSyncPlanRequest",
    );
    first_plan_request["request_id"] = json!("req-native-rebind-cycle-exact-replay");
    let first_plan = success_result(
        support::invoke_with_request("setup.sync_plan", first_plan_request.clone()),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    let replayed_plan = success_result(
        support::invoke_with_request("setup.sync_plan", first_plan_request),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    assert_eq!(replayed_plan, first_plan);
    let first_plan = native_identity_rebind_component(&first_plan, "native_runtime");
    let first_cycle_id = first_plan["cycle_id"].clone();
    let first_operation_id = first_plan["operation_id"].clone();
    let first_seal = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": first_plan["next_request"]
        }),
    );
    let first_seal = native_identity_rebind_component(&first_seal, "native_runtime");
    let mut first_rollback = first_seal["next_request"].clone();
    first_rollback["disposition"] = json!("rolled_back");
    let first_observation = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": first_rollback
        }),
    );
    let first_observation = native_identity_rebind_component(&first_observation, "native_runtime");
    assert_eq!(first_observation["phase"], "awaiting_host_release");
    let first_release = first_observation["next_request"].clone();
    let first_terminal = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": first_release
        }),
    );
    assert_eq!(
        native_identity_rebind_component(&first_terminal, "native_runtime")["phase"],
        "rolled_back"
    );

    let second_plan = native_identity_rebind_step(&host, setup_sync_rebind_params());
    let second_plan = native_identity_rebind_component(&second_plan, "native_runtime");
    assert_ne!(second_plan["cycle_id"], first_cycle_id);
    assert_ne!(second_plan["operation_id"], first_operation_id);
    assert_eq!(second_plan["prior_evidence"], first_plan["prior_evidence"]);
    let second_seal = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": second_plan["next_request"]
        }),
    );
    let second_seal = native_identity_rebind_component(&second_seal, "native_runtime");
    let mut second_rollback = second_seal["next_request"].clone();
    second_rollback["disposition"] = json!("rolled_back");
    let second_observation = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": second_rollback
        }),
    );
    let second_observation =
        native_identity_rebind_component(&second_observation, "native_runtime");
    assert_eq!(second_observation["phase"], "awaiting_host_release");
    assert_eq!(second_observation["cycle_id"], second_plan["cycle_id"]);
    assert_eq!(second_observation["next_request"]["action"], "release");
    let second_terminal = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": second_observation["next_request"]
        }),
    );
    assert_eq!(
        native_identity_rebind_component(&second_terminal, "native_runtime")["phase"],
        "rolled_back"
    );
}

#[test]
fn contract_native_identity_rebind_retains_nonterminal_release_custody_past_replay_window() {
    let host = HostRoots::new("agent-runner-opencode-rebind-active-retention");
    let first_observation = awaiting_native_identity_rollback(&host);
    let first_release = first_observation["next_request"].clone();
    let first_cycle_id = first_observation["cycle_id"]
        .as_str()
        .expect("first rebind cycle identity");
    let first_record = host
        .data_root()
        .join("provider-state/opencode/native-identity-rebind")
        .join("opencode3-native_runtime")
        .join(format!("{first_cycle_id}.json"));
    let mut stale: Value =
        serde_json::from_slice(&fs::read(&first_record).expect("read pending rebind observation"))
            .expect("pending rebind observation JSON");
    stale["updated_at_unix_ms"] = json!(1);
    fs::write(
        &first_record,
        serde_json::to_vec_pretty(&stale).expect("encode stale pending observation"),
    )
    .expect("age pending rebind observation past terminal replay window");

    let second_observation = awaiting_native_identity_rollback(&host);
    assert_ne!(
        second_observation["cycle_id"], first_observation["cycle_id"],
        "a later plan must retain a distinct cycle identity"
    );
    assert!(
        first_record.exists(),
        "admission of another cycle must not expire a nonterminal release obligation"
    );

    let released = native_identity_rebind_step(
        &host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": first_release
        }),
    );
    let released = native_identity_rebind_component(&released, "native_runtime");
    assert_eq!(released["phase"], "rolled_back");
    assert_eq!(
        released["release_authorization"]["ordinary_admission_may_reopen"],
        true
    );
}

fn awaiting_native_identity_rollback(host: &HostRoots) -> Value {
    let planned = native_identity_rebind_step(host, setup_sync_rebind_params());
    let planned = native_identity_rebind_component(&planned, "native_runtime");
    let sealed = native_identity_rebind_step(
        host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": planned["next_request"]
        }),
    );
    let sealed = native_identity_rebind_component(&sealed, "native_runtime");
    let mut rollback = sealed["next_request"].clone();
    rollback["disposition"] = json!("rolled_back");
    let observed = native_identity_rebind_step(
        host,
        json!({
            "settings_schema_id": "opencode.settings/v1",
            "desired_profiles": ["opencode3"],
            "native_identity_rebind": rollback
        }),
    );
    native_identity_rebind_component(&observed, "native_runtime").clone()
}

fn native_identity_rebind_step(host: &HostRoots, params: Value) -> Value {
    success_result(
        invoke_validated_with_host(
            "setup.sync_plan",
            params,
            host.overrides(),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    )
}

fn native_identity_rebind_component<'a>(result: &'a Value, component: &str) -> &'a Value {
    result["operations"]
        .as_array()
        .expect("sync operations")
        .iter()
        .find(|operation| {
            operation["kind"] == "native_identity_rebind" && operation["component"] == component
        })
        .expect("component-scoped native identity rebind operation")
}

fn native_identity_rebind_release_request(observation: &Value) -> Value {
    json!({
        "protocol": "opencode.native-identity-rebind/v1",
        "action": "release",
        "cycle_id": observation["cycle_id"],
        "operation_id": observation["operation_id"],
        "observation_id": observation["observation_id"],
        "profile": observation["profile"],
        "component": observation["component"],
        "prior_evidence": observation["prior_evidence"],
        "observed_evidence": observation["observed_evidence"],
        "disposition": observation["disposition"],
        "host_handoff": { "ordinary_admission_blocked": true }
    })
}

fn write_native_runtime_identity(
    data_root: &std::path::Path,
    tools: &FakeToolchain,
    profile: &str,
) {
    let program =
        std::fs::canonicalize(tools.dir().join(profile)).expect("canonical native runtime fixture");
    let program_sha256 = file_sha256(&program);
    let execution_env = json!({});
    let identity_sha256 = sha256_hex(
        json!({
            "account_wrapper": profile,
            "program": program.to_string_lossy(),
            "program_sha256": program_sha256,
            "execution_env": execution_env,
        })
        .to_string()
        .as_bytes(),
    );
    let record = json!({
        "schema_version": 1,
        "account_wrapper": profile,
        "program": program.to_string_lossy(),
        "program_sha256": program_sha256,
        "execution_env": execution_env,
        "identity_sha256": identity_sha256,
    });
    let root = data_root.join("provider-state/opencode/native-runtimes");
    std::fs::create_dir_all(&root).expect("create native runtime fixture root");
    std::fs::write(
        root.join(format!("{profile}.json")),
        serde_json::to_vec_pretty(&record).expect("serialize native runtime fixture"),
    )
    .expect("write native runtime fixture");
}

fn assert_native_identity_rebind_schema(schema: &Value, definition: &str, value: &Value) {
    let mut root = schema.clone();
    root["$ref"] = json!(format!("#/$defs/{definition}"));
    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&root)
        .expect("compile native identity rebind schema");
    if let Err(errors) = compiled.validate(value) {
        panic!(
            "native identity rebind {definition} failed its advertised schema: {}; value={value}",
            errors
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        );
    };
}

#[test]
fn contract_setup_detect_missing_dependency_diagnostics() {
    let host = HostRoots::new("agent-runner-opencode-setup-missing-dependency");
    let empty_path = unique_temp_dir("agent-runner-opencode-empty-path");
    fs::create_dir_all(&empty_path).expect("create empty PATH fixture");
    let home = HomeFixture::new("agent-runner-opencode-setup-missing-home");
    let path = path_string(&empty_path);
    let data_root = path_string(host.data_root());

    let detect = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            setup_detect_data_root_params(&data_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );

    assert_setup_missing_dependency_result(&detect);

    fs::remove_dir_all(&empty_path).expect("remove empty PATH fixture");
}

#[cfg(unix)]
#[test]
fn contract_setup_detect_rejects_present_but_unusable_dependencies() {
    let host = HostRoots::new("agent-runner-opencode-setup-unusable-dependencies");
    let tool_root = unique_temp_dir("agent-runner-opencode-unusable-setup-tools");
    fs::create_dir_all(&tool_root).expect("create unusable setup tool root");
    for program in ["opencode", "curl"] {
        fs::write(tool_root.join(program), b"regular but not executable\n")
            .expect("write non-executable setup dependency");
    }
    let home = HomeFixture::new("agent-runner-opencode-setup-unusable-home");
    home.write_all_opencode_auths();
    let path = path_string(&tool_root);
    let detect = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            setup_detect_data_root_params(&path_string(host.data_root())),
            host.overrides(),
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );

    assert_eq!(detect["installed"], false);
    assert_eq!(detect["binary"]["opencode"]["present"], true);
    assert_eq!(detect["binary"]["opencode"]["version"]["ready"], false);
    assert_eq!(detect["binary"]["curl"]["present"], true);
    assert_eq!(detect["binary"]["curl"]["version"]["ready"], false);
    let profiles = detect["profiles"].as_array().expect("profile evidence");
    assert!(profiles.iter().all(|profile| {
        profile["logical_account_present"] == true
            && profile["native_runtime_ready"] == false
            && profile["profile_ready"] == false
    }));
    assert!(!detect["warnings"].as_array().expect("warnings").is_empty());
    fs::remove_dir_all(&tool_root).expect("remove unusable setup tool root");
}

#[cfg(unix)]
#[test]
fn contract_setup_detect_bounds_hanging_version_probe() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-setup-probe-timeout");
    let tool_root = unique_temp_dir("agent-runner-opencode-hanging-setup-tools");
    fs::create_dir_all(&tool_root).expect("create hanging setup tool root");
    write_executable(
        &tool_root.join("opencode"),
        "#!/bin/sh\nwhile :; do :; done\n",
    );
    let home = HomeFixture::new("agent-runner-opencode-setup-probe-timeout-home");
    let path = path_string(&tool_root);
    let mut bounded_host = host.overrides();
    bounded_host["deadline_unix_ms"] = json!(agent_runner_opencode::encoding::now_unix_ms() + 500);
    let started = std::time::Instant::now();
    let detect = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            setup_detect_data_root_params(&path_string(host.data_root())),
            bounded_host,
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(30),
        "the 500 ms host deadline must still bound and reap the probe under scheduler load; elapsed={elapsed:?}"
    );
    assert_eq!(detect["installed"], false);
    assert_eq!(detect["binary"]["opencode"]["version"]["timed_out"], true);
    assert!(!detect["warnings"].as_array().expect("warnings").is_empty());
    fs::remove_dir_all(&tool_root).expect("remove hanging setup tool root");
}

#[test]
fn contract_setup_blocks_cutover_above_predecessor_transition_envelope() {
    let host = HostRoots::new("agent-runner-opencode-setup-settings-transition-block");
    let store_path = host
        .config_root()
        .join("agent-runner-opencode/settings-store.json");
    fs::create_dir_all(store_path.parent().expect("settings store parent"))
        .expect("create predecessor settings root");
    let mut values = opencode_settings_values(None);
    values["extra_env"] = json!({
        "PREDECESSOR_PAYLOAD": "x".repeat(16 * 1024 * 1024)
    });
    fs::write(
        &store_path,
        serde_json::to_vec(&json!({
            "records": [{
                "id": "setup-blocked-predecessor-0",
                "display_name": "Setup-blocked predecessor",
                "version": "predecessor-v1",
                "values": values,
            }]
        }))
        .expect("serialize above-envelope predecessor store"),
    )
    .expect("write above-envelope predecessor store");
    let toolchain = FakeToolchain::new();
    let home = HomeFixture::new("agent-runner-opencode-setup-settings-transition-home");
    home.write_all_opencode_auths();
    let path = prepend_path(toolchain.dir());
    let data_root = path_string(host.data_root());
    let profile_root = path_string(host.config_root());

    let detect = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            setup_detect_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );
    assert_eq!(detect["installed"], false);
    assert!(detect.get("settings_store").is_none());
    assert!(detect["warnings"]
        .as_array()
        .expect("setup warnings")
        .iter()
        .any(|warning| warning
            .as_str()
            .is_some_and(|warning| warning.contains("predecessor provider"))));

    let install = success_result(
        invoke_validated_with_host(
            "setup.install_plan",
            setup_install_plan_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupInstallPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupInstallPlanResponse",
        "setup.schema.json#/$defs/SetupInstallPlanResult",
    );
    let transition_step = install["steps"]
        .as_array()
        .expect("install steps")
        .iter()
        .find(|step| step["kind"] == "verify_settings_transition")
        .expect("settings transition install step");
    assert_eq!(transition_step["blocking"], true);
    assert_eq!(transition_step["settings_store"]["ready"], false);

    let sync = success_result(
        invoke_validated_with_host(
            "setup.sync_plan",
            setup_sync_plan_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    assert!(sync["diagnostics"]
        .as_array()
        .expect("sync diagnostics")
        .iter()
        .any(
            |diagnostic| diagnostic["code"] == "settings_transition_blocked"
                && diagnostic["severity"] == "error"
        ));
}

#[test]
fn contract_setup_install_sync_plan_missing_prerequisite() {
    let host = HostRoots::new("agent-runner-opencode-setup-plan-missing-prerequisite");
    let empty_path = unique_temp_dir("agent-runner-opencode-empty-path");
    fs::create_dir_all(&empty_path).expect("create empty PATH fixture");
    let home = HomeFixture::new("agent-runner-opencode-setup-plan-missing-home");
    let path = path_string(&empty_path);
    let data_root = path_string(host.data_root());
    let profile_root = host.config_root().join("missing-profile-root");
    let profile_root = path_string(&profile_root);

    let detect = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            setup_detect_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );
    assert_setup_plan_fixture_missing(&detect);

    let install = success_result(
        invoke_validated_with_host_and_env(
            "setup.install_plan",
            setup_install_plan_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupInstallPlanRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupInstallPlanResponse",
        "setup.schema.json#/$defs/SetupInstallPlanResult",
    );
    assert_missing_prereq_install_plan(&install);

    let sync = success_result(
        invoke_validated_with_host_and_env(
            "setup.sync_plan",
            setup_sync_plan_params(&data_root, &profile_root),
            host.overrides(),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    assert_missing_prereq_sync_plan(&sync);

    fs::remove_dir_all(&empty_path).expect("remove empty PATH fixture");
}

#[test]
fn contract_setup_brain_unsupported() {
    let describe = success_result(
        invoke("describe", empty_request_params()),
        "describe.schema.json#/$defs/DescribeResponse",
        "describe.schema.json#/$defs/DescribeResult",
    );
    assert_setup_brain_not_advertised(&describe);

    let response = error_response(invoke_validated(
        "setup_brain.turn",
        setup_brain_turn_params(),
        "setup.schema.json#/$defs/SetupBrainTurnRequest",
    ));
    assert_setup_brain_unsupported_response(&response);
}

fn rotation_settings_values(account: &str) -> Value {
    let mut values = opencode_settings_values(None);
    values["profile"] = json!(account);
    values["wrapper"] = json!(account);
    values["quota"]["auth_path"] = json!(match account {
        "opencode1" => "~/.local/share/opencode/auth.json",
        "opencode2" => "~/.opencode2/opencode/auth.json",
        "opencode3" => "~/.opencode3/opencode/auth.json",
        "opencode4" => "~/.opencode4/opencode/auth.json",
        "opencode5" => "~/.opencode5/opencode/auth.json",
        _ => panic!("unsupported rotation settings account"),
    });
    values
}

fn create_rotation_settings(host: &HostRoots, account: &str) -> (String, String) {
    let created = success_result(
        invoke_validated_with_host(
            "settings.create",
            settings_create_params_for_values(rotation_settings_values(account)),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsCreateRequest",
        ),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
    (
        settings_create_id(&created),
        settings_create_version(&created),
    )
}

fn update_rotation_settings(
    host: &HostRoots,
    settings_id: &str,
    version: &str,
    account: &str,
) -> String {
    let updated = success_result(
        invoke_validated_with_host(
            "settings.update",
            json!({
                "id": settings_id,
                "version": version,
                "values": rotation_settings_values(account),
            }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsUpdateRequest",
        ),
        "settings.schema.json#/$defs/SettingsUpdateResponse",
        "settings.schema.json#/$defs/SettingsUpdateResult",
    );
    updated["record"]["version"]
        .as_str()
        .expect("updated rotation settings version")
        .to_string()
}

fn assess_rotation_with_settings(host: &HostRoots, settings_id: &str) -> Value {
    let mut params = rotation_assess_alias_params(true);
    params["settings_id"] = json!(settings_id);
    success_result(
        invoke_validated_with_host(
            "rotation.assess",
            params,
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    )
}

fn rotation_authorized_settings_selection(assessment: &Value) -> Value {
    assessment["requirements"]
        .as_array()
        .expect("rotation requirements")
        .iter()
        .find(|requirement| requirement["kind"] == "provider_authorization")
        .expect("provider rotation authorization")["settings_selection"]
        .clone()
}

fn materialize_rotation_with_settings(selection: &Value) -> Value {
    let mut params = rotation_materialize_params();
    params["settings_id"] = selection["settings_id"].clone();
    params["settings_version"] = selection["settings_version"].clone();
    params["settings_account"] = selection["settings_account"].clone();
    params
}

fn assert_rotation_decision_protocol(materialized: &Value) -> Value {
    let decision_path = materialized["artifacts"][1]["path"]
        .as_str()
        .expect("rotation decision path");
    let decision: Value =
        serde_json::from_slice(&fs::read(decision_path).expect("read rotation decision artifact"))
            .expect("rotation decision JSON");
    let schema = success_result(
        invoke(
            "schema",
            json!({ "schema_id": "opencode.rotation-decision/v1" }),
        ),
        "schema.schema.json#/$defs/SchemaResponse",
        "schema.schema.json#/$defs/SchemaResult",
    )["schema"]
        .clone();
    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema)
        .expect("compile rotation decision schema");
    if let Err(errors) = compiled.validate(&decision) {
        panic!(
            "rotation decision failed its advertised schema: {}; decision={decision}",
            errors
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
    decision
}

#[test]
fn contract_rotation_assess_materialize() {
    let host = HostRoots::new("agent-runner-opencode-rotation");
    let opencode = RotationOpencodeFixture::new();
    let denied = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_params(false),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_denied(&denied);

    let canonical_same_account = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_same_account_alias_params(),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_denied(&canonical_same_account);

    let allowed = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_allowed(&allowed);

    let host_owned = RotationHostSentinels::new(host.data_root());

    let path = opencode.path_env();
    let materialized = success_result(
        invoke_validated_with_host_and_env(
            "rotation.materialize",
            rotation_materialize_params(),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_rotation_materialized(&materialized);
    assert_rotation_decision_protocol(&materialized);
    fs::remove_dir_all(host.working_directory())
        .expect("remove former working directory after committed materialization");
    let retried = success_result(
        invoke_validated_with_host_and_env(
            "rotation.materialize",
            rotation_materialize_params(),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_eq!(retried, materialized, "materialization must be retry-safe");
    let imported = opencode.imported_session();
    assert_eq!(opencode.imported_cwd(), host.working_directory());
    assert_eq!(opencode.import_count(), 1);
    assert_eq!(imported["info"]["id"], ROTATION_SOURCE_SESSION);
    assert_eq!(imported["info"]["projectID"], "project_rotation_native");
    assert_eq!(
        imported["messages"][0]["info"]["parentID"],
        "msg_rotation_parent"
    );
    assert_eq!(imported["messages"][0]["mode"], "build");
    assert_eq!(imported["nativeRoot"]["preserved"], true);
    host_owned.assert_unchanged();
}

#[test]
fn contract_rotation_rejects_changed_or_deleted_settings_before_import() {
    for mutation in ["update", "delete"] {
        let host = HostRoots::new(&format!(
            "agent-runner-opencode-rotation-settings-{mutation}-before-import"
        ));
        let opencode = RotationOpencodeFixture::new();
        let (settings_id, version) = create_rotation_settings(&host, "opencode2");
        let assessment = assess_rotation_with_settings(&host, &settings_id);
        assert_rotation_allowed(&assessment);
        let selection = rotation_authorized_settings_selection(&assessment);
        assert_eq!(selection["settings_id"], settings_id);
        assert_eq!(selection["settings_version"], version);
        assert_eq!(selection["settings_account"], "opencode2");

        match mutation {
            "update" => {
                update_rotation_settings(&host, &settings_id, &version, "opencode3");
            }
            "delete" => {
                let _ = success_result(
                    invoke_validated_with_host(
                        "settings.delete",
                        settings_delete_params(&settings_id, &version),
                        host.overrides(),
                        "settings.schema.json#/$defs/SettingsDeleteRequest",
                    ),
                    "settings.schema.json#/$defs/SettingsDeleteResponse",
                    "settings.schema.json#/$defs/SettingsDeleteResult",
                );
            }
            _ => unreachable!(),
        }

        let path = opencode.path_env();
        let response = error_response(invoke_validated_with_host_and_env(
            "rotation.materialize",
            materialize_rotation_with_settings(&selection),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
            &[("PATH", path.as_str())],
        ));
        assert_eq!(
            response["error"]["code"],
            "rotation_settings_selection_changed"
        );
        assert!(
            !opencode.import_was_attempted(),
            "a changed or deleted route must fail before native import"
        );
    }
}

#[cfg(unix)]
#[test]
fn contract_rotation_reconciles_settings_changed_after_import_without_reimport() {
    let host = HostRoots::new("agent-runner-opencode-rotation-settings-after-import");
    let opencode = RotationOpencodeFixture::with_post_import_finalization_fault();
    let (settings_id, version) = create_rotation_settings(&host, "opencode2");
    let assessment = assess_rotation_with_settings(&host, &settings_id);
    let selection = rotation_authorized_settings_selection(&assessment);
    let materialize = materialize_rotation_with_settings(&selection);
    let path = opencode.path_env();

    let first = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        materialize.clone(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(first["error"]["code"], "rotation_recovery_required");
    assert_eq!(opencode.import_count(), 1);
    opencode.restore_operation_state_writes(host.data_root());

    let operation_root = host
        .data_root()
        .join("provider-state/opencode/rotation/operations");
    let operation_path = fs::read_dir(&operation_root)
        .expect("rotation operation directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .expect("prepared rotation operation");
    let mut operation: Value = serde_json::from_slice(
        &fs::read(&operation_path).expect("read prepared rotation operation"),
    )
    .expect("prepared rotation operation JSON");
    operation["phase"] = json!("imported");
    operation["target_session_id"] = json!(ROTATION_SOURCE_SESSION);
    operation["imported_at_unix_ms"] = json!(agent_runner_opencode::encoding::now_unix_ms());
    fs::write(
        &operation_path,
        serde_json::to_vec_pretty(&operation).expect("encode imported rotation operation"),
    )
    .expect("publish imported rotation checkpoint");

    let changed_version = update_rotation_settings(&host, &settings_id, &version, "opencode3");
    let unresolved = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        materialize.clone(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(
        unresolved["error"]["code"],
        "rotation_settings_reconciliation_required"
    );
    assert_eq!(
        unresolved["error"]["details"]["imported_target_provider_session_id"],
        ROTATION_SOURCE_SESSION
    );
    assert_eq!(opencode.import_count(), 1);

    let reconciled_version =
        update_rotation_settings(&host, &settings_id, &changed_version, "opencode2");
    let settled_selection = json!({
        "settings_id": settings_id,
        "settings_version": reconciled_version,
        "settings_account": "opencode2",
    });
    let mut reconcile = materialize.clone();
    reconcile["settings_reconciliation"] = json!({
        "settings_id": settled_selection["settings_id"],
        "settings_version": settled_selection["settings_version"],
        "settings_account": "opencode2",
        "target_provider_session_id": ROTATION_SOURCE_SESSION,
    });
    let materialized = success_result(
        invoke_validated_with_host_and_env(
            "rotation.materialize",
            reconcile.clone(),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_rotation_materialized(&materialized);
    let decision = assert_rotation_decision_protocol(&materialized);
    assert_eq!(decision["schema_id"], "opencode.rotation-decision/v1");
    assert_eq!(decision["authorized_settings_selection"], selection);
    assert_eq!(decision["settled_settings_selection"], settled_selection);
    assert_eq!(opencode.import_count(), 1);

    let replayed = success_result(
        invoke_validated_with_host_and_env(
            "rotation.materialize",
            reconcile,
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_eq!(replayed, materialized);
    assert_eq!(opencode.import_count(), 1);
}

#[test]
fn contract_rotation_hanging_import_releases_global_capability_lock() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-rotation-import-timeout");
    let opencode = RotationOpencodeFixture::with_hanging_import();
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let request = support::validated_request_envelope(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
    );
    let (failed, bounded_elapsed) = support::invoke_with_request_and_env_fresh_deadline(
        "rotation.materialize",
        request,
        &[("PATH", path.as_str())],
        std::time::Duration::from_secs(20),
    );
    let failed = error_response(failed);
    assert_eq!(failed["error"]["code"], "rotation_recovery_required");
    assert!(
        bounded_elapsed < std::time::Duration::from_secs(60),
        "a stalled import must be terminated within the request bound"
    );
    assert!(
        !opencode.import_was_attempted(),
        "the timed-out import must be killed before the fixture records an effect"
    );

    let mut follow_up_host = host.overrides();
    follow_up_host["deadline_unix_ms"] =
        json!(agent_runner_opencode::encoding::now_unix_ms() + 30_000);
    let follow_up = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_params(false),
            follow_up_host,
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_denied(&follow_up);
}

#[test]
fn contract_rotation_independent_binding_progresses_during_native_import() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-rotation-independent-overlap");
    let opencode = RotationOpencodeFixture::with_hanging_import();
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );

    let first_params = rotation_materialize_params();
    let first_stripe = rotation_binding_test_stripe(&first_params);
    let mut independent_params = rotation_assess_params(false);
    let independent_chain = (0..256)
        .map(|index| format!("chain-independent-{index}"))
        .find(|chain| {
            independent_params["chain_id"] = json!(chain);
            rotation_binding_test_stripe(&independent_params) != first_stripe
        })
        .expect("one independent binding must map to another lock stripe");
    independent_params["chain_id"] = json!(independent_chain);

    let path = opencode.path_env();
    let first_path = path.clone();
    let first_request = support::validated_request_envelope(
        "rotation.materialize",
        first_params,
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
    );
    let worker = thread::spawn(move || {
        support::invoke_with_request_and_env_fresh_deadline(
            "rotation.materialize",
            first_request,
            &[("PATH", first_path.as_str())],
            std::time::Duration::from_secs(60),
        )
        .0
    });

    let operation_root = host
        .data_root()
        .join("provider-state/opencode/rotation/operations");
    let wait_started = std::time::Instant::now();
    while !fs::read_dir(&operation_root).is_ok_and(|entries| {
        entries
            .filter_map(Result::ok)
            .any(|entry| entry.path().is_file())
    }) {
        assert!(
            wait_started.elapsed() < std::time::Duration::from_secs(30) && !worker.is_finished(),
            "the first rotation must publish its prepared operation"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }

    let independent = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            independent_params,
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_denied(&independent);
    assert!(
        !worker.is_finished(),
        "independent binding work must not wait for another binding's native import"
    );
    opencode.release_hanging_import();

    let first = success_result(
        worker.join().expect("join released rotation worker"),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_rotation_materialized(&first);
}

#[test]
fn contract_rotation_validates_successful_import_before_terminal_settlement() {
    let host = HostRoots::new("agent-runner-opencode-rotation-import-validation");
    let opencode = RotationOpencodeFixture::with_inconsistent_successful_import();
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let request = support::validated_request_envelope(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
    );

    let first = error_response(support::invoke_with_request_and_env(
        "rotation.materialize",
        request.clone(),
        &[("PATH", path.as_str())],
    ));
    assert_eq!(first["error"]["code"], "rotation_recovery_required");
    assert_eq!(
        first["error"]["details"]["supplied_recovery_target_session_id"],
        ROTATION_SOURCE_SESSION
    );
    assert!(first["error"]["details"]["observation"]
        .as_str()
        .is_some_and(|observation| observation.contains("content does not match")));
    assert_eq!(opencode.import_count(), 1);

    let replay = error_response(support::invoke_with_request_and_env(
        "rotation.materialize",
        request,
        &[("PATH", path.as_str())],
    ));
    assert_eq!(replay["error"]["code"], "rotation_recovery_required");
    assert_eq!(
        opencode.import_count(),
        1,
        "an unverified successful import must remain prepared and never be repeated"
    );
}

#[test]
fn contract_rotation_preserves_rekeyed_import_candidate_until_target_is_observable() {
    let host = HostRoots::new("agent-runner-opencode-rotation-import-candidate-custody");
    let target_session_id = "ses_target_candidate_contract_d";
    let opencode =
        RotationOpencodeFixture::with_temporarily_unavailable_rekeyed_target(target_session_id);
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let request = support::validated_request_envelope(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
    );

    let unresolved = error_response(support::invoke_with_request_and_env(
        "rotation.materialize",
        request.clone(),
        &[("PATH", path.as_str())],
    ));
    assert_eq!(unresolved["error"]["code"], "rotation_recovery_required");
    assert_eq!(
        unresolved["error"]["details"]["supplied_recovery_target_session_id"],
        target_session_id
    );
    assert_eq!(opencode.import_count(), 1);

    opencode.restore_target_export();
    let recovered = success_result(
        support::invoke_with_request_and_env(
            "rotation.materialize",
            request,
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_eq!(recovered["target_provider_session_id"], target_session_id);
    assert_eq!(
        opencode.import_count(),
        1,
        "exact retry must use the durable native candidate without reimport"
    );
}

#[test]
fn contract_rotation_validates_an_explicit_alternate_to_an_unverified_import_report() {
    let host = HostRoots::new("agent-runner-opencode-rotation-alternate-import-candidate");
    let actual_target_session_id = "ses_actual_target_contract_d";
    let reported_target_session_id = "ses_incorrect_report_contract_d";
    let opencode = RotationOpencodeFixture::with_temporarily_unavailable_rekeyed_target(
        actual_target_session_id,
    );
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let mut request = support::validated_request_envelope(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
    );

    let first = error_response(support::invoke_with_request_and_env(
        "rotation.materialize",
        request.clone(),
        &[("PATH", path.as_str())],
    ));
    assert_eq!(first["error"]["code"], "rotation_recovery_required");
    assert_eq!(opencode.import_count(), 1);

    let operation_root = host
        .data_root()
        .join("provider-state/opencode/rotation/operations");
    let operation_path = fs::read_dir(operation_root)
        .expect("rotation operation directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().and_then(|value| value.to_str()) == Some("json"))
        .expect("prepared rotation operation");
    let mut operation: Value = serde_json::from_slice(
        &fs::read(&operation_path).expect("read prepared rotation operation"),
    )
    .expect("prepared rotation operation JSON");
    operation["import_candidate_session_id"] = json!(reported_target_session_id);
    fs::write(
        &operation_path,
        serde_json::to_vec_pretty(&operation).expect("encode incorrect import report"),
    )
    .expect("simulate a durable but incorrect import-reported identity");

    opencode.restore_target_export();
    request["params"]["recovery_target_session_id"] = json!(actual_target_session_id);
    let recovered = success_result(
        support::invoke_with_request_and_env(
            "rotation.materialize",
            request,
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_eq!(
        recovered["target_provider_session_id"],
        actual_target_session_id
    );
    assert_eq!(
        opencode.import_count(),
        1,
        "validating an explicit alternate must not repeat native import"
    );
}

fn rotation_binding_test_stripe(params: &Value) -> u8 {
    let source_account = match params["source_account"].as_str() {
        Some("opencode") => "opencode1",
        Some(value) => value,
        None => "",
    };
    let target_account = match params["target_account"].as_str() {
        Some("opencode-secondary") => "opencode2",
        Some(value) => value,
        None => "",
    };
    let binding = json!({
        "chain_id": params["chain_id"],
        "source_provider": params["source_provider"],
        "target_provider": params["target_provider"],
        "source_account": source_account,
        "target_account": target_account,
        "source_session_id": params["source_session_id"],
        "model_name": params["model_name"],
        "settings_selection": if params["settings_id"].as_str().is_some() {
            json!({
                "settings_id": params["settings_id"],
                "settings_version": params["settings_version"],
                "settings_account": params["settings_account"],
            })
        } else {
            Value::Null
        },
        "transition_reason": params["transition_reason"],
        "provider_instance_id": "opencode-primary",
        "host_app": "oulipoly-agent-runner",
    });
    let digest = sha256_hex(binding.to_string().as_bytes());
    u8::from_str_radix(&digest[..2], 16).expect("binding digest prefix") % 64
}

#[test]
fn contract_rotation_runtime_lock_contention_obeys_global_budget() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-rotation-runtime-lock-timeout");
    let opencode = RotationOpencodeFixture::new();
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let runtime_root = host
        .data_root()
        .join("provider-state/opencode/native-runtimes");
    fs::create_dir_all(&runtime_root).expect("create native runtime state root");
    let runtime_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(runtime_root.join("opencode1.lock"))
        .expect("open source runtime lock");
    fs2::FileExt::lock_exclusive(&runtime_lock).expect("hold source runtime lock");
    let path = opencode.path_env();
    let request = support::validated_request_envelope(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
    );
    let (failed, bounded_elapsed) = support::invoke_with_request_and_env_fresh_deadline(
        "rotation.materialize",
        request,
        &[("PATH", path.as_str())],
        std::time::Duration::from_secs(45),
    );
    let failed = error_response(failed);
    assert_eq!(failed["error"]["code"], "native_runtime_lock_timeout");
    assert!(
        bounded_elapsed < std::time::Duration::from_secs(300),
        "the 45-second host deadline must terminate runtime-lock contention within scheduler tolerance"
    );
    drop(runtime_lock);

    let follow_up = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_params(false),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_denied(&follow_up);
}

#[cfg(unix)]
#[test]
fn contract_rotation_prepared_recovery_runtime_lock_obeys_global_budget() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-rotation-recovery-runtime-lock-timeout");
    let opencode = RotationOpencodeFixture::with_post_import_finalization_fault();
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let first = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(first["error"]["code"], "rotation_recovery_required");
    assert_eq!(opencode.import_count(), 1);
    opencode.restore_operation_state_writes(host.data_root());

    let runtime_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(
            host.data_root()
                .join("provider-state/opencode/native-runtimes/opencode2.lock"),
        )
        .expect("open target runtime lock");
    fs2::FileExt::lock_exclusive(&runtime_lock).expect("hold target runtime lock");
    let request = support::validated_request_envelope(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
    );
    let (failed, bounded_elapsed) = support::invoke_with_request_and_env_fresh_deadline(
        "rotation.materialize",
        request,
        &[("PATH", path.as_str())],
        std::time::Duration::from_secs(10),
    );
    let failed = error_response(failed);
    assert_eq!(failed["error"]["code"], "native_runtime_lock_timeout");
    assert!(bounded_elapsed < std::time::Duration::from_secs(60));
    drop(runtime_lock);

    let follow_up = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_params(false),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_denied(&follow_up);
}

#[test]
fn contract_rotation_oversized_export_is_bounded_and_releases_global_capability_lock() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-rotation-export-capacity");
    let opencode = RotationOpencodeFixture::with_oversized_export();
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let started = std::time::Instant::now();
    let failed = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(failed["error"]["code"], "rotation_export_capacity_exceeded");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(90),
        "an oversized export must be drained and rejected within the 60-second provider bound plus scheduler tolerance"
    );
    assert!(!opencode.import_was_attempted());

    let follow_up = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_params(false),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_denied(&follow_up);
}

#[test]
fn contract_rotation_large_supported_artifact_completes_inside_global_lock_budget() {
    let _io_guard = lock_io_intensive_contract();
    let host = HostRoots::new("agent-runner-opencode-rotation-large-supported-artifact");
    let opencode = RotationOpencodeFixture::with_large_export();
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let started = std::time::Instant::now();
    let materialized = success_result(
        invoke_validated_with_host_and_env(
            "rotation.materialize",
            rotation_materialize_params(),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_rotation_materialized(&materialized);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(90),
        "a supported near-envelope artifact must complete within the 60-second provider bound plus scheduler tolerance"
    );
    let artifact_path = materialized["artifacts"][0]["path"]
        .as_str()
        .expect("rotation source artifact path");
    let artifact_bytes = fs::metadata(artifact_path)
        .expect("rotation source artifact metadata")
        .len();
    assert!(artifact_bytes > 15 * 1024 * 1024);
    assert!(artifact_bytes < 16 * 1024 * 1024);
    assert_eq!(opencode.import_count(), 1);
}

#[cfg(unix)]
#[test]
fn contract_rotation_recovers_post_import_pre_receipt_failure_without_reimport() {
    let host = HostRoots::new("agent-runner-opencode-rotation-post-import-recovery");
    let opencode = RotationOpencodeFixture::with_post_import_finalization_fault();
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let failed = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(failed["error"]["code"], "rotation_recovery_required");
    assert_eq!(opencode.import_count(), 1);

    opencode.restore_operation_state_writes(host.data_root());
    let recovered = success_result(
        invoke_validated_with_host_and_env(
            "rotation.materialize",
            rotation_materialize_params(),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_rotation_materialized(&recovered);
    assert_eq!(
        recovered["target_provider_session_id"],
        ROTATION_SOURCE_SESSION
    );
    assert_eq!(
        opencode.import_count(),
        1,
        "recovery must reconcile the durable prepared operation without repeating import"
    );
}

#[cfg(unix)]
#[test]
fn contract_rotation_requires_and_validates_changed_target_identity_during_recovery() {
    let host = HostRoots::new("agent-runner-opencode-rotation-changed-id-recovery");
    let target_session_id = "ses_target_rekeyed_contract_d";
    let opencode = RotationOpencodeFixture::with_post_import_finalization_fault_and_target_id(
        target_session_id,
    );
    let _ = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    let path = opencode.path_env();
    let _ = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(opencode.import_count(), 1);
    opencode.restore_operation_state_writes(host.data_root());

    let unresolved = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(unresolved["error"]["code"], "rotation_recovery_required");
    assert_eq!(opencode.import_count(), 1);

    let mut recovery = rotation_materialize_params();
    recovery["recovery_target_session_id"] = json!(target_session_id);
    let recovered = success_result(
        invoke_validated_with_host_and_env(
            "rotation.materialize",
            recovery,
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
            &[("PATH", path.as_str())],
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert_eq!(recovered["target_provider_session_id"], target_session_id);
    assert_eq!(opencode.import_count(), 1);
}

#[test]
fn contract_rotation_materialize_requires_matching_prior_assessment() {
    let host = HostRoots::new("agent-runner-opencode-rotation-no-assessment");
    let opencode = RotationOpencodeFixture::new();
    let path = opencode.path_env();
    let response = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(response["error"]["code"], "rotation_authorization_required");
    assert!(
        !opencode.import_was_attempted(),
        "authorization failure must precede native export/import side effects"
    );
}

#[test]
fn contract_rotation_denial_retires_prior_authorization_before_materialization() {
    let host = HostRoots::new("agent-runner-opencode-rotation-denial-retirement");
    let opencode = RotationOpencodeFixture::new();
    let allowed = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_allowed(&allowed);
    let denied = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_alias_params(false),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_rotation_denied(&denied);

    let path = opencode.path_env();
    let response = error_response(invoke_validated_with_host_and_env(
        "rotation.materialize",
        rotation_materialize_params(),
        host.overrides(),
        "rotation.schema.json#/$defs/RotationMaterializeRequest",
        &[("PATH", path.as_str())],
    ));
    assert_eq!(response["error"]["code"], "rotation_authorization_required");
    assert!(
        !opencode.import_was_attempted(),
        "durable denial must retire the prior grant before native export/import"
    );
}

#[test]
fn contract_migration_plan_apply() {
    let host = HostRoots::new("agent-runner-opencode-migration");
    let live = LiveConfigFixture::new(host.config_root());

    let plan = success_result(
        invoke_validated_with_host(
            "migration.plan",
            migration_plan_params(&live),
            host.overrides(),
            "migration.schema.json#/$defs/MigrationPlanRequest",
        ),
        "migration.schema.json#/$defs/MigrationPlanResponse",
        "migration.schema.json#/$defs/MigrationPlanResult",
    );
    assert_migration_plan_result(&plan);

    let snapshots = MigrationSnapshots::capture(&host, &live);
    let apply = success_result(
        invoke_validated_with_host(
            "migration.apply",
            migration_apply_params(&live),
            host.overrides(),
            "migration.schema.json#/$defs/MigrationApplyRequest",
        ),
        "migration.schema.json#/$defs/MigrationApplyResponse",
        "migration.schema.json#/$defs/MigrationApplyResult",
    );
    assert_migration_apply_result(&apply, live.provider_artifact_root());
    snapshots.assert_after_apply(&host, &live);
}

#[test]
fn contract_migration_apply_requires_true_confirmation_before_writes() {
    let host = HostRoots::new("agent-runner-opencode-migration-confirmation");
    let live = LiveConfigFixture::new(host.config_root());
    let before = snapshot_tree(host.config_root());
    for params in [
        migration_apply_params_without_confirmation(&live),
        migration_apply_params_with_false_confirmation(&live),
    ] {
        let response = error_response(invoke_validated_with_host(
            "migration.apply",
            params,
            host.overrides(),
            "migration.schema.json#/$defs/MigrationApplyRequest",
        ));
        assert_eq!(response["error"]["code"], "migration_confirmation_required");
    }
    assert_eq!(snapshot_tree(host.config_root()), before);
}

#[test]
fn contract_migration_apply_rejects_artifact_root_outside_provider_ownership() {
    let host = HostRoots::new("agent-runner-opencode-migration-root-boundary");
    let live = LiveConfigFixture::new(host.config_root());
    let forbidden_root = host.config_root().join("unowned-migration-output");
    let response = error_response(invoke_validated_with_host(
        "migration.apply",
        migration_apply_params_with_artifact_root(&live, &forbidden_root),
        host.overrides(),
        "migration.schema.json#/$defs/MigrationApplyRequest",
    ));
    assert_eq!(
        response["error"]["code"],
        "artifact_root_outside_provider_root"
    );
    assert!(!forbidden_root.exists());
}

#[test]
fn contract_migration_parallel_retries_converge_on_one_content_addressed_artifact() {
    let host = HostRoots::new("agent-runner-opencode-migration-parallel");
    let live = LiveConfigFixture::new(host.config_root());
    let params = migration_apply_params(&live);
    let workers = (0..8)
        .map(|_| {
            let params = params.clone();
            let host_overrides = host.overrides();
            thread::spawn(move || {
                invoke_validated_with_host(
                    "migration.apply",
                    params,
                    host_overrides,
                    "migration.schema.json#/$defs/MigrationApplyRequest",
                )
            })
        })
        .collect::<Vec<_>>();
    let mut artifact_paths = std::collections::BTreeSet::new();
    for worker in workers {
        let result = success_result(
            worker.join().expect("migration worker"),
            "migration.schema.json#/$defs/MigrationApplyResponse",
            "migration.schema.json#/$defs/MigrationApplyResult",
        );
        artifact_paths.insert(
            result["artifacts"][0]["path"]
                .as_str()
                .expect("artifact path")
                .to_string(),
        );
    }
    assert_eq!(artifact_paths.len(), 1);
    let files = fs::read_dir(live.provider_artifact_root())
        .expect("artifact directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_file())
                && entry.path().extension().and_then(|value| value.to_str()) == Some("json")
        })
        .count();
    assert_eq!(files, 1);
}
