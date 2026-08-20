//! Declared roles: orchestration

mod cluster_d;
#[allow(dead_code)]
mod support;

use cluster_d::*;
use serde_json::json;
use std::fs;
use std::thread;
use support::{
    invoke, invoke_validated, invoke_validated_with_host, invoke_validated_with_host_and_env,
};

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
fn contract_settings_parallel_creates_do_not_lose_records() {
    let host = HostRoots::new("agent-runner-opencode-settings-parallel-create");
    let host_overrides = host.overrides();
    let workers = (0..8)
        .map(|_| {
            let host_overrides = host_overrides.clone();
            thread::spawn(move || {
                invoke_validated_with_host(
                    "settings.create",
                    settings_create_params(None),
                    host_overrides,
                    "settings.schema.json#/$defs/SettingsCreateRequest",
                )
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
        .map(|_| {
            let host_overrides = host.overrides();
            let id = id.clone();
            let version = version.clone();
            thread::spawn(move || {
                invoke_validated_with_host(
                    "settings.update",
                    settings_update_params(&id, &version, None),
                    host_overrides,
                    "settings.schema.json#/$defs/SettingsUpdateRequest",
                )
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
    let _ = success_result(
        invoke_validated_with_host(
            "settings.create",
            settings_create_params(Some(SECRET_TOKEN)),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsCreateRequest",
        ),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
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
    assert_eq!(persisted["schema_version"], 2);
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
fn contract_settings_create_normalizes_all_account_records() {
    let host = HostRoots::new("agent-runner-opencode-settings-normalize-accounts");
    for (wrapper, auth_path) in normalized_account_cases() {
        let create = success_result(
            invoke_validated_with_host(
                "settings.create",
                settings_create_params_for_values(path_wrapped_opencode_settings_values(wrapper)),
                host.overrides(),
                "settings.schema.json#/$defs/SettingsCreateRequest",
            ),
            "settings.schema.json#/$defs/SettingsCreateResponse",
            "settings.schema.json#/$defs/SettingsCreateResult",
        );
        assert_normalized_account_settings_record(&create["record"], wrapper, auth_path);
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
    let result = success_result(
        invoke_validated_with_host(
            "settings.migrate",
            settings_migrate_params(),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsMigrateRequest",
        ),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );
    assert_settings_migrate_result(&result, host.config_root(), &before);
}

#[test]
fn contract_settings_migrate_canonicalizes_and_rejects_unknown_legacy_accounts() {
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
    assert_eq!(action_providers, vec!["opencode1", "opencode2"]);
    assert_eq!(dry_run["requires_user_input"], true);
    assert!(dry_run["diagnostics"]
        .as_array()
        .expect("migration diagnostics")
        .iter()
        .any(|diagnostic| diagnostic["code"] == "legacy_provider_unknown"));

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
fn contract_setup_sync_canonicalizes_and_rejects_unknown_account_references() {
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
    assert_eq!(profiles, vec!["opencode1", "opencode2"]);
    assert!(result["diagnostics"]
        .as_array()
        .expect("setup diagnostics")
        .iter()
        .any(|diagnostic| diagnostic["code"] == "unknown_opencode_profile"));
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
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .count();
    assert_eq!(files, 1);
}
