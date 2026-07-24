//! Declared roles: orchestration

mod cluster_d;
#[allow(dead_code)]
mod support;

use cluster_d::*;
use std::fs;
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

fn normalized_account_cases() -> [(&'static str, &'static str); 5] {
    [
        ("opencode1", "~/.codex/auth.json"),
        ("opencode2", "~/.codex5/auth.json"),
        ("opencode3", "~/.codex2/auth.json"),
        ("opencode4", "~/.codex3/auth.json"),
        ("opencode5", "~/.codex4/auth.json"),
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
    assert_rotation_allowed(&allowed);

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
    assert_eq!(opencode.import_count(), 2);
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
