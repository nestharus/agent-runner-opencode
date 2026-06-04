#[allow(dead_code)]
mod support;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use support::{
    assert_valid, invoke, invoke_validated, invoke_validated_with_host,
    invoke_validated_with_host_and_env, json_stdout,
};

const SECRET_TOKEN: &str = "opencode_contract_secret_token_must_not_echo";
const UPDATE_SECRET_TOKEN: &str = "opencode_contract_update_secret_token_must_not_echo";
const SETUP_AUTH_SENTINEL: &str = "SETUP_AUTH_SENTINEL_DO_NOT_LEAK";
const OPENCODE_VERSION_SENTINEL: &str = "opencode 0.0.0-contract";
const CHATGPT_USAGE_READY_SENTINEL: &str = "contract_chatgpt_usage_ready";
const PROVIDERS_TOML: &str = r#"
[opencode]
command = "opencode1"
args = ["run", "--dangerously-skip-permissions"]
quota_script = "chatgpt-usage ~/.codex/auth.json"
refresh_auth_command = "/bin/false"

[opencode2]
command = "opencode2"
args = ["run", "--dangerously-skip-permissions"]
quota_script = "chatgpt-usage ~/.codex5/auth.json"
refresh_auth_command = "/bin/false"
"#;
const MODEL_TOML: &str = r#"
name = "gpt-high"
provider = "opencode"
model = "openai/gpt-5.5"
args = ["--variant", "high"]
"#;

#[test]
fn contract_settings_crud() {
    let host = HostRoots::new("agent-runner-opencode-settings-crud");
    let create = success_result(
        invoke_validated_with_host(
            "settings.create",
            json!({
                "display_name": "Contract opencode profile",
                "values": opencode_settings_values(Some(SECRET_TOKEN))
            }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsCreateRequest",
        ),
        "settings.schema.json#/$defs/SettingsCreateResponse",
        "settings.schema.json#/$defs/SettingsCreateResult",
    );
    assert_secret_absent(&create);
    assert_valid(
        &create["record"],
        "settings.schema.json#/$defs/SettingsRecord",
    );
    let id = create["record"]["id"]
        .as_str()
        .expect("created id")
        .to_owned();
    let created_version = create["record"]["version"]
        .as_str()
        .expect("created version")
        .to_owned();
    assert_eq!(
        create["record"]["display_name"],
        "Contract opencode profile"
    );
    assert_eq!(create["record"]["values"]["provider"], "opencode");
    assert_eq!(create["record"]["values"]["wrapper"], "opencode1");

    let list = success_result(
        invoke_validated_with_host(
            "settings.list",
            json!({}),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsListRequest",
        ),
        "settings.schema.json#/$defs/SettingsListResponse",
        "settings.schema.json#/$defs/SettingsListResult",
    );
    assert_secret_absent(&list);
    let list_record = find_record(list["records"].as_array().expect("settings records"), &id);
    assert_eq!(list_record["id"], id);
    assert_eq!(list_record["display_name"], "Contract opencode profile");
    assert_eq!(list_record["version"], created_version);
    assert!(
        list_record.get("values").is_none(),
        "settings.list summaries must not include full secret-bearing values"
    );

    let get = success_result(
        invoke_validated_with_host(
            "settings.get",
            json!({ "id": id }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsGetRequest",
        ),
        "settings.schema.json#/$defs/SettingsGetResponse",
        "settings.schema.json#/$defs/SettingsGetResult",
    );
    assert_secret_absent(&get);
    assert_eq!(get["record"]["id"], id);
    assert_eq!(get["record"]["display_name"], "Contract opencode profile");
    assert_eq!(get["record"]["version"], created_version);
    assert_eq!(get["record"]["values"]["wrapper"], "opencode1");

    let update_response = success_response(
        invoke_validated_with_host(
            "settings.update",
            json!({
                "id": id,
                "version": created_version,
                "values": opencode_settings_values(Some(UPDATE_SECRET_TOKEN))
            }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsUpdateRequest",
        ),
        "settings.schema.json#/$defs/SettingsUpdateResponse",
        "settings.schema.json#/$defs/SettingsUpdateResult",
    );
    assert_secret_absent(&update_response);
    assert_string_absent(
        &update_response,
        UPDATE_SECRET_TOKEN,
        "full SettingsUpdateResponse",
    );
    let update = &update_response["result"];
    assert_string_absent(&update["record"], UPDATE_SECRET_TOKEN, "updated record");
    assert_string_absent(
        &update["record"]["values"],
        UPDATE_SECRET_TOKEN,
        "updated record values",
    );
    let updated_version = update["record"]["version"]
        .as_str()
        .expect("updated version")
        .to_owned();
    assert_ne!(
        updated_version, created_version,
        "settings.update must advance the opaque version so stale writes can conflict"
    );

    let stale = invoke_validated_with_host(
        "settings.update",
        json!({
            "id": id,
            "version": created_version,
            "values": opencode_settings_values(None)
        }),
        host.overrides(),
        "settings.schema.json#/$defs/SettingsUpdateRequest",
    );
    let stale_response = error_response(stale);
    assert_eq!(stale_response["error"]["category"], "conflict");
    assert_secret_absent(&stale_response);

    let delete = success_result(
        invoke_validated_with_host(
            "settings.delete",
            json!({ "id": id, "version": updated_version }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsDeleteRequest",
        ),
        "settings.schema.json#/$defs/SettingsDeleteResponse",
        "settings.schema.json#/$defs/SettingsDeleteResult",
    );
    assert_eq!(delete["deleted"], true);
    assert_eq!(delete["id"], id);
}

#[test]
fn contract_settings_validate() {
    let host = HostRoots::new("agent-runner-opencode-settings-validate");
    let valid = success_result(
        invoke_validated_with_host(
            "settings.validate",
            json!({ "values": opencode_settings_values(None) }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsValidateRequest",
        ),
        "settings.schema.json#/$defs/SettingsValidateResponse",
        "settings.schema.json#/$defs/SettingsValidateResult",
    );
    assert_eq!(valid["valid"], true);
    assert!(valid["diagnostics"]
        .as_array()
        .expect("diagnostics")
        .is_empty());

    let invalid = success_result(
        invoke_validated_with_host(
            "settings.validate",
            json!({
                "values": {
                    "provider": "opencode",
                    "wrapper": "opencode99",
                    "model": { "provider_model": "", "variant": "impossible" },
                    "quota": { "auth_path": "" }
                }
            }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsValidateRequest",
        ),
        "settings.schema.json#/$defs/SettingsValidateResponse",
        "settings.schema.json#/$defs/SettingsValidateResult",
    );
    assert_eq!(invalid["valid"], false);
    assert!(
        !invalid["diagnostics"]
            .as_array()
            .expect("diagnostics")
            .is_empty(),
        "invalid settings draft must produce diagnostics"
    );
}

#[test]
fn contract_settings_migrate() {
    let host = HostRoots::new("agent-runner-opencode-settings-migrate");
    let before = snapshot_tree(host.config_root());
    let result = success_result(
        invoke_validated_with_host(
            "settings.migrate",
            json!({
                "dry_run": true,
                "legacy": legacy_fixture()
            }),
            host.overrides(),
            "settings.schema.json#/$defs/SettingsMigrateRequest",
        ),
        "settings.schema.json#/$defs/SettingsMigrateResponse",
        "settings.schema.json#/$defs/SettingsMigrateResult",
    );
    assert!(
        !result["actions"].as_array().expect("actions").is_empty(),
        "dry-run migration should describe provider-owned settings actions"
    );
    assert!(result["requires_user_input"].as_bool().is_some());
    assert!(result["warnings"].as_array().is_some());
    assert!(result["diagnostics"].as_array().is_some());
    assert_eq!(
        snapshot_tree(host.config_root()),
        before,
        "settings.migrate dry_run=true must not write provider settings files"
    );
}

#[test]
fn contract_setup_detect_install_sync() {
    let host = HostRoots::new("agent-runner-opencode-setup");
    let toolchain = FakeToolchain::new();
    let home = HomeFixture::new("agent-runner-opencode-setup-home");
    home.write_all_codex_auths();
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
    let data_root = host.data_root().to_string_lossy().into_owned();
    let profile_root = profile_root.to_string_lossy().into_owned();
    let path = prepend_path(toolchain.dir());

    let detect = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            json!({
                "data_root": data_root,
                "profile_root": profile_root
            }),
            host.overrides(),
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );
    assert_setup_auth_sentinel_absent(&detect);
    assert_eq!(detect["installed"], true);
    assert!(detect["warnings"].as_array().is_some());
    assert!(
        detect.get("binary").is_some(),
        "detect should report binary evidence"
    );
    assert!(
        detect.get("auth").is_some(),
        "detect should report auth readiness"
    );
    assert!(
        json_contains_string(&detect["binary"], OPENCODE_VERSION_SENTINEL),
        "detect binary evidence should include fake opencode --version sentinel {OPENCODE_VERSION_SENTINEL}; binary={}",
        detect["binary"]
    );
    assert!(
        json_contains_string(&detect["binary"], "chatgpt-usage")
            || json_contains_string(&detect["auth"], "chatgpt-usage")
            || json_contains_string(&detect["profiles"], "chatgpt-usage"),
        "detect provider-owned evidence should mention chatgpt-usage; detect={detect}"
    );
    assert!(
        json_contains_string(&detect["binary"], CHATGPT_USAGE_READY_SENTINEL)
            || json_contains_string(&detect["auth"], CHATGPT_USAGE_READY_SENTINEL)
            || json_contains_string(&detect["profiles"], CHATGPT_USAGE_READY_SENTINEL),
        "detect provider-owned evidence should include chatgpt-usage readiness sentinel {CHATGPT_USAGE_READY_SENTINEL}; detect={detect}"
    );
    assert!(
        json_contains_string(&detect["profiles"], &data_root)
            || json_contains_string(&detect["auth"], &data_root),
        "detect profile/auth evidence should include temp data-root evidence {data_root}; detect={detect}"
    );
    assert!(
        json_contains_string(&detect["profiles"], &profile_root)
            || json_contains_string(&detect["auth"], &profile_root),
        "detect profile/auth evidence should include temp profile-root evidence {profile_root}; detect={detect}"
    );
    let profiles = detect["profiles"].as_array().expect("profiles array");
    for wrapper in [
        "opencode1",
        "opencode2",
        "opencode3",
        "opencode4",
        "opencode5",
    ] {
        assert!(
            json_contains_string(&detect, wrapper),
            "detect should reflect {wrapper} wrapper presence; detect={detect}"
        );
    }
    assert!(
        profiles.len() >= 5,
        "detect should report the five opencode profiles"
    );

    let install = success_result(
        invoke_validated_with_host_and_env(
            "setup.install_plan",
            json!({
                "target": "local",
                "data_root": data_root,
                "profile_root": profile_root
            }),
            host.overrides(),
            "setup.schema.json#/$defs/SetupInstallPlanRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupInstallPlanResponse",
        "setup.schema.json#/$defs/SetupInstallPlanResult",
    );
    assert_setup_auth_sentinel_absent(&install);
    assert!(
        !install["steps"].as_array().expect("steps").is_empty(),
        "setup.install_plan should return actionable setup steps"
    );

    let sync = success_result(
        invoke_validated_with_host_and_env(
            "setup.sync_plan",
            json!({
                "desired_profiles": ["opencode1", "opencode2", "opencode3", "opencode4", "opencode5"],
                "settings_schema_id": "opencode.settings/v1",
                "data_root": data_root,
                "profile_root": profile_root
            }),
            host.overrides(),
            "setup.schema.json#/$defs/SetupSyncPlanRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupSyncPlanResponse",
        "setup.schema.json#/$defs/SetupSyncPlanResult",
    );
    assert_setup_auth_sentinel_absent(&sync);
    assert!(sync["operations"].as_array().is_some());
    assert!(sync["diagnostics"].as_array().is_some());
}

#[test]
fn contract_setup_detect_missing_dependency_diagnostics() {
    let host = HostRoots::new("agent-runner-opencode-setup-missing-dependency");
    let empty_path = unique_temp_dir("agent-runner-opencode-empty-path");
    fs::create_dir_all(&empty_path).expect("create empty PATH fixture");
    let home = HomeFixture::new("agent-runner-opencode-setup-missing-home");
    let path = empty_path.to_string_lossy().into_owned();
    let data_root = host.data_root().to_string_lossy().into_owned();

    let detect = success_result(
        invoke_validated_with_host_and_env(
            "setup.detect",
            json!({ "data_root": data_root }),
            host.overrides(),
            "setup.schema.json#/$defs/SetupDetectRequest",
            &[("PATH", path.as_str()), ("HOME", home.path_str())],
        ),
        "setup.schema.json#/$defs/SetupDetectResponse",
        "setup.schema.json#/$defs/SetupDetectResult",
    );

    assert_eq!(
        detect["installed"], false,
        "setup.detect must report not installed when required tools and wrappers are absent"
    );
    assert!(
        !detect["warnings"]
            .as_array()
            .expect("warnings array")
            .is_empty(),
        "missing setup prerequisites must produce warnings"
    );
    assert_eq!(detect["binary"]["opencode"]["present"], false);
    assert_eq!(detect["binary"]["chatgpt-usage"]["present"], false);
    assert!(
        detect["profiles"]
            .as_array()
            .expect("profiles array")
            .iter()
            .any(|profile| profile["wrapper_present"] == false),
        "missing wrappers should be reflected in profile readiness evidence; detect={detect}"
    );
    assert!(
        json_contains_string(&detect["binary"], "opencode"),
        "missing dependency diagnostics/evidence must name opencode; detect={detect}"
    );
    assert!(
        json_contains_string(&detect["binary"], "chatgpt-usage")
            || json_contains_string(&detect["profiles"], "chatgpt-usage")
            || json_contains_string(&detect["auth"], "chatgpt-usage"),
        "missing dependency diagnostics/evidence must name chatgpt-usage; detect={detect}"
    );

    fs::remove_dir_all(&empty_path).expect("remove empty PATH fixture");
}

#[test]
fn contract_setup_brain_unsupported() {
    let describe = success_result(
        invoke("describe", json!({})),
        "describe.schema.json#/$defs/DescribeResponse",
        "describe.schema.json#/$defs/DescribeResult",
    );
    assert_eq!(describe["capabilities"]["setup_brain"], false);

    let response = error_response(invoke_validated(
        "setup_brain.turn",
        json!({
            "conversation_id": "setup-brain-contract-test",
            "message": { "role": "user", "content": "configure opencode" }
        }),
        "setup.schema.json#/$defs/SetupBrainTurnRequest",
    ));
    assert_eq!(response["error"]["category"], "unsupported");
}

#[test]
fn contract_rotation_assess_materialize() {
    let host = HostRoots::new("agent-runner-opencode-rotation");
    let allowed = success_result(
        invoke_validated_with_host(
            "rotation.assess",
            rotation_assess_params(true),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationAssessRequest",
        ),
        "rotation.schema.json#/$defs/RotationAssessResponse",
        "rotation.schema.json#/$defs/RotationAssessResult",
    );
    assert_eq!(allowed["allowed"], true);
    assert!(
        allowed["requirements"]
            .as_array()
            .expect("requirements")
            .len()
            >= 3
    );
    assert!(allowed.get("score").is_some());
    assert!(allowed.get("reason").is_some());

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
    assert_eq!(denied["allowed"], false);
    assert!(
        denied["requirements"]
            .as_array()
            .expect("requirements")
            .len()
            >= 3
    );
    assert!(denied.get("reason").is_some());

    let host_db = host.data_root().join("oulipoly_state.sqlite");
    let host_journal = host.data_root().join("rotation-journal.jsonl");
    fs::write(&host_db, b"host db sentinel\n").expect("write host db sentinel");
    fs::write(&host_journal, b"host journal sentinel\n").expect("write host journal sentinel");
    let host_owned_before = file_hashes([host_db.as_path(), host_journal.as_path()]);

    let materialized = success_result(
        invoke_validated_with_host(
            "rotation.materialize",
            json!({
                "operation": "rotation.materialize",
                "chain_id": "chain-contract-d",
                "source_provider": "opencode1",
                "target_provider": "opencode2",
                "source_session_id": "ses_source_contract_d",
                "target_session_id": "ses_target_contract_d",
                "transition_reason": "quota_threshold",
                "requirements": rotation_requirements(true),
                "session_export": {
                    "canonical_format": "oulipoly.canonical_transcript/v1",
                    "turn_count": 1
                }
            }),
            host.overrides(),
            "rotation.schema.json#/$defs/RotationMaterializeRequest",
        ),
        "rotation.schema.json#/$defs/RotationMaterializeResponse",
        "rotation.schema.json#/$defs/RotationMaterializeResult",
    );
    assert!(materialized["changed"].as_bool().is_some());
    assert!(materialized["artifacts"].as_array().is_some());
    assert_valid(
        &materialized["host_state_plan"],
        "rotation.schema.json#/$defs/RotationHostStatePlan",
    );
    assert_eq!(
        materialized["host_state_plan"]["operation"],
        "rotation.materialize"
    );
    assert_eq!(
        materialized["host_state_plan"]["chain_id"],
        "chain-contract-d"
    );
    assert_eq!(
        file_hashes([host_db.as_path(), host_journal.as_path()]),
        host_owned_before,
        "rotation.materialize must return a host_state_plan, not mutate host DB or journal files"
    );
}

#[test]
fn contract_migration_plan_apply() {
    let host = HostRoots::new("agent-runner-opencode-migration");
    let live_config_root = host.config_root().join("live-config");
    let provider_artifact_root = host
        .config_root()
        .join("provider-owned-migration-artifacts");
    let live_providers = live_config_root.join("providers.toml");
    let live_model_dir = live_config_root.join("models");
    fs::create_dir_all(&live_model_dir).expect("create live model sentinel dir");
    let live_model = live_model_dir.join("gpt-high.toml");
    let live_model_medium = live_model_dir.join("gpt-medium.toml");
    let live_route_low = live_config_root.join("gpt-low.toml");
    let live_route_xhigh = live_config_root.join("gpt-xhigh.toml");
    fs::write(&live_providers, PROVIDERS_TOML).expect("write providers sentinel");
    fs::write(&live_model, MODEL_TOML).expect("write model sentinel");
    fs::write(&live_model_medium, MODEL_TOML).expect("write medium model sentinel");
    fs::write(&live_route_low, MODEL_TOML).expect("write low route sentinel");
    fs::write(&live_route_xhigh, MODEL_TOML).expect("write xhigh route sentinel");

    let plan = success_result(
        invoke_validated_with_host(
            "migration.plan",
            json!({
                "legacy": legacy_fixture(),
                "target_provider": "agent-runner-opencode",
                "scope": "provider_owned",
                "live_config_root": live_config_root.to_string_lossy(),
                "artifact_root": provider_artifact_root.to_string_lossy()
            }),
            host.overrides(),
            "migration.schema.json#/$defs/MigrationPlanRequest",
        ),
        "migration.schema.json#/$defs/MigrationPlanResponse",
        "migration.schema.json#/$defs/MigrationPlanResult",
    );
    assert!(
        !plan["actions"].as_array().expect("actions").is_empty(),
        "migration.plan should return planned provider-owned actions"
    );
    assert!(plan["warnings"].as_array().is_some());
    assert!(plan["requires_backup"].as_bool().is_some());

    let config_before = snapshot_tree(host.config_root());
    let data_before = snapshot_tree(host.data_root());
    let live_before = snapshot_tree(&live_config_root);
    let apply = success_result(
        invoke_validated_with_host(
            "migration.apply",
            json!({
                "legacy": legacy_fixture(),
                "target_provider": "agent-runner-opencode",
                "scope": "provider_owned",
                "live_config_root": live_config_root.to_string_lossy(),
                "artifact_root": provider_artifact_root.to_string_lossy(),
                "confirmation": { "approved": true, "source": "contract-test" }
            }),
            host.overrides(),
            "migration.schema.json#/$defs/MigrationApplyRequest",
        ),
        "migration.schema.json#/$defs/MigrationApplyResponse",
        "migration.schema.json#/$defs/MigrationApplyResult",
    );
    assert!(
        !apply["applied_actions"]
            .as_array()
            .expect("applied_actions")
            .is_empty(),
        "migration.apply should report applied provider-owned actions"
    );
    let artifacts = apply["artifacts"].as_array().expect("artifacts");
    assert!(
        !artifacts.is_empty(),
        "migration.apply should write provider-owned artifacts under the temp roots"
    );
    for artifact in artifacts {
        assert_valid(artifact, "common.schema.json#/$defs/Artifact");
        let path = artifact["path"].as_str().unwrap_or_else(|| {
            panic!("migration artifact should include a provider-owned path: {artifact}")
        });
        let path = Path::new(path);
        assert!(
            path.starts_with(&provider_artifact_root),
            "migration artifacts must stay under the explicit provider-owned artifact root {}; path={}",
            provider_artifact_root.display(),
            path.display()
        );
        assert!(
            !is_forbidden_live_route_path(path),
            "migration artifacts must not be live-route TOML files; path={}",
            path.display()
        );
    }
    assert!(apply["warnings"].as_array().is_some());
    assert!(apply["outcome"].as_object().is_some());
    assert_eq!(
        snapshot_tree(&live_config_root),
        live_before,
        "migration.apply must not mutate the live config route tree"
    );
    let config_after = snapshot_tree(host.config_root());
    let data_after = snapshot_tree(host.data_root());
    assert_only_tree_changes_under(
        &config_before,
        &config_after,
        host.config_root(),
        &provider_artifact_root,
        "host.config_root",
    );
    assert_only_tree_changes_under(
        &data_before,
        &data_after,
        host.data_root(),
        &provider_artifact_root,
        "host.data_root",
    );
    assert_forbidden_live_routes_unchanged(&config_before, &config_after);
}

fn opencode_settings_values(secret: Option<&str>) -> Value {
    let mut values = json!({
        "provider": "opencode",
        "profile": "opencode1",
        "wrapper": "opencode1",
        "model": {
            "name": "gpt-high",
            "provider_model": "openai/gpt-5.5",
            "variant": "high"
        },
        "quota": {
            "source": "codex",
            "auth_path": "~/.codex/auth.json",
            "usage_command": "chatgpt-usage"
        },
        "launch": {
            "format": "json",
            "dangerously_skip_permissions": true
        }
    });
    if let Some(secret) = secret {
        values["auth_token"] = json!(secret);
    }
    values
}

fn legacy_fixture() -> Value {
    json!({
        "providers_toml": PROVIDERS_TOML,
        "models": {
            "gpt-high.toml": MODEL_TOML
        }
    })
}

fn rotation_assess_params(allowed: bool) -> Value {
    json!({
        "operation": "rotation.assess",
        "settings_id": "opencode1",
        "model_name": "gpt-high",
        "source": { "provider": "opencode1", "session_id": "ses_source_contract_d" },
        "target": { "provider": "opencode2" },
        "requirements": rotation_requirements(allowed),
        "facts": {
            "quota": { "available": allowed, "remaining_ratio": if allowed { 0.72 } else { 0.01 } },
            "session": { "exportable": allowed, "replace_supported": false },
            "settings": { "source_profile_present": true, "target_profile_present": allowed }
        }
    })
}

fn rotation_requirements(allowed: bool) -> Value {
    json!([
        { "kind": "quota_available", "met": allowed, "detail": "target account has usable quota" },
        { "kind": "session_exportable", "met": allowed, "detail": "source session can be exported" },
        { "kind": "target_profile_present", "met": allowed, "detail": "target opencode profile exists" }
    ])
}

fn success_result(
    output: std::process::Output,
    response_schema: &str,
    result_schema: &str,
) -> Value {
    success_response(output, response_schema, result_schema)["result"].clone()
}

fn success_response(
    output: std::process::Output,
    response_schema: &str,
    result_schema: &str,
) -> Value {
    assert!(
        output.status.success(),
        "expected success for {response_schema}; exit {:?}; stderr: {}; stdout: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let response = json_stdout(&output);
    assert_valid(&response, response_schema);
    assert_valid(&response["result"], result_schema);
    response
}

fn error_response(output: std::process::Output) -> Value {
    assert!(
        !output.status.success(),
        "expected nonzero error envelope; stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let response = json_stdout(&output);
    assert_valid(&response, "common.schema.json#/$defs/ErrorResponseEnvelope");
    response
}

fn find_record<'a>(records: &'a [Value], id: &str) -> &'a Value {
    records
        .iter()
        .find(|record| record["id"].as_str() == Some(id))
        .unwrap_or_else(|| panic!("record {id} not found in settings list: {records:?}"))
}

fn assert_secret_absent(value: &Value) {
    assert_string_absent(value, SECRET_TOKEN, "contract response");
}

fn assert_string_absent(value: &Value, needle: &str, context: &str) {
    let json = value.to_string();
    assert!(
        !json.contains(needle),
        "{context} must not echo auth-token value {needle}: {json}"
    );
}

fn assert_setup_auth_sentinel_absent(value: &Value) {
    assert!(
        !json_contains_string(value, SETUP_AUTH_SENTINEL),
        "setup response must not echo auth token sentinel {SETUP_AUTH_SENTINEL}: {value}"
    );
}

fn assert_forbidden_live_routes_unchanged(
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
) {
    let forbidden_paths = before
        .keys()
        .chain(after.keys())
        .filter(|path| is_forbidden_live_route_path(path))
        .cloned()
        .collect::<BTreeSet<_>>();
    assert!(
        forbidden_paths.len() >= 5,
        "migration boundary fixture should include providers.toml, models/**, and gpt-*.toml files"
    );
    for path in forbidden_paths {
        assert_eq!(
            after.get(&path),
            before.get(&path),
            "migration.apply must not write, modify, or remove live-route file {}",
            path.display()
        );
    }
}

fn assert_only_tree_changes_under(
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
    root: &Path,
    allowed_root: &Path,
    context: &str,
) {
    for path in before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
    {
        if before.get(&path) == after.get(&path) {
            continue;
        }
        let absolute_path = root.join(&path);
        assert!(
            absolute_path.starts_with(allowed_root),
            "migration.apply must only add/change/remove files inside provider artifact root {}; changed {context} path {}",
            allowed_root.display(),
            absolute_path.display()
        );
    }
}

fn is_forbidden_live_route_path(path: &Path) -> bool {
    let file_name = path.file_name().and_then(|name| name.to_str());
    file_name == Some("providers.toml")
        || file_name.is_some_and(|name| name.starts_with("gpt-") && name.ends_with(".toml"))
        || path
            .components()
            .any(|component| component.as_os_str().to_str() == Some("models"))
}

struct HostRoots {
    root: PathBuf,
    config_root: PathBuf,
    data_root: PathBuf,
}

impl HostRoots {
    fn new(prefix: &str) -> Self {
        let root = unique_temp_dir(prefix);
        let config_root = root.join("config");
        let data_root = root.join("data");
        fs::create_dir_all(&config_root).expect("create temp config_root");
        fs::create_dir_all(&data_root).expect("create temp data_root");
        Self {
            root,
            config_root,
            data_root,
        }
    }

    fn overrides(&self) -> Value {
        json!({
            "config_root": self.config_root.to_string_lossy(),
            "data_root": self.data_root.to_string_lossy()
        })
    }

    fn config_root(&self) -> &Path {
        &self.config_root
    }

    fn data_root(&self) -> &Path {
        &self.data_root
    }
}

impl Drop for HostRoots {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct HomeFixture {
    path: PathBuf,
    path_string: String,
}

impl HomeFixture {
    fn new(prefix: &str) -> Self {
        let path = unique_temp_dir(prefix);
        fs::create_dir_all(&path).expect("create temp HOME");
        let path_string = path.to_string_lossy().into_owned();
        Self { path, path_string }
    }

    fn path_str(&self) -> &str {
        &self.path_string
    }

    fn write_all_codex_auths(&self) {
        for relative in [
            ".codex/auth.json",
            ".codex5/auth.json",
            ".codex2/auth.json",
            ".codex3/auth.json",
            ".codex4/auth.json",
        ] {
            let path = self.path.join(relative);
            fs::create_dir_all(path.parent().expect("auth parent")).expect("create auth parent");
            fs::write(
                path,
                format!(
                    "{{\"tokens\":{{\"access_token\":\"{SETUP_AUTH_SENTINEL}\",\"account_id\":\"acct\"}}}}\n"
                ),
            )
            .expect("write auth fixture");
        }
    }
}

impl Drop for HomeFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct FakeToolchain {
    dir: PathBuf,
}

impl FakeToolchain {
    fn new() -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-setup-tools");
        fs::create_dir_all(&dir).expect("create fake toolchain dir");
        write_executable(
            &dir.join("opencode"),
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'opencode 0.0.0-contract\\n'; exit 0; fi\nprintf 'fake opencode\\n'\nexit 0\n",
        );
        write_executable(
            &dir.join("chatgpt-usage"),
            "#!/bin/sh\nprintf '{\"contract_chatgpt_usage_ready\":true,\"windows\":[]}\\n'\nexit 0\n",
        );
        for wrapper in [
            "opencode1",
            "opencode2",
            "opencode3",
            "opencode4",
            "opencode5",
        ] {
            write_executable(
                &dir.join(wrapper),
                "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'wrapper contract\\n'; exit 0; fi\nexit 0\n",
            );
        }
        Self { dir }
    }

    fn dir(&self) -> &Path {
        &self.dir
    }
}

impl Drop for FakeToolchain {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn write_executable(path: &Path, script: &str) {
    fs::write(path, script).unwrap_or_else(|err| panic!("write {}: {err}", path.display()));
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(path)
            .unwrap_or_else(|err| panic!("metadata {}: {err}", path.display()))
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)
            .unwrap_or_else(|err| panic!("chmod {}: {err}", path.display()));
    }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

fn prepend_path(dir: &Path) -> String {
    let existing_path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(&existing_path));
    std::env::join_paths(paths)
        .expect("join PATH entries")
        .to_string_lossy()
        .into_owned()
}

fn snapshot_tree(root: &Path) -> BTreeMap<PathBuf, String> {
    let mut files = BTreeMap::new();
    collect_tree_hashes(root, root, &mut files);
    files
}

fn collect_tree_hashes(root: &Path, current: &Path, files: &mut BTreeMap<PathBuf, String>) {
    for entry in
        fs::read_dir(current).unwrap_or_else(|err| panic!("read_dir {}: {err}", current.display()))
    {
        let entry =
            entry.unwrap_or_else(|err| panic!("read_dir entry {}: {err}", current.display()));
        let path = entry.path();
        if path.is_dir() {
            collect_tree_hashes(root, &path, files);
        } else {
            let relative = path
                .strip_prefix(root)
                .unwrap_or_else(|err| panic!("strip prefix {}: {err}", path.display()))
                .to_path_buf();
            files.insert(relative, file_sha256(&path));
        }
    }
}

fn file_hashes<'a>(paths: impl IntoIterator<Item = &'a Path>) -> BTreeMap<PathBuf, String> {
    paths
        .into_iter()
        .map(|path| (path.to_path_buf(), file_sha256(path)))
        .collect()
}

fn file_sha256(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn json_contains_string(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(value) => value.contains(needle),
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, needle)),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains(needle) || json_contains_string(value, needle)),
        _ => false,
    }
}
