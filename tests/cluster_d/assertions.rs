// declared_role: validator, accessor, predicate, mapper, orchestration
#![allow(unused_imports)]

use super::*;

pub fn assert_settings_create_result(create: &Value) {
    assert_secret_absent(create);
    assert_valid(
        &create["record"],
        "settings.schema.json#/$defs/SettingsRecord",
    );
    assert_eq!(
        create["record"]["display_name"],
        "Contract opencode profile"
    );
    assert_eq!(create["record"]["values"]["provider"], "opencode");
    assert_eq!(create["record"]["values"]["wrapper"], "opencode1");
}

pub fn assert_normalized_account_settings_record(record: &Value, wrapper: &str, auth_path: &str) {
    assert_secret_absent(record);
    assert_eq!(record["values"]["provider"], "opencode");
    assert_eq!(record["values"]["profile"], wrapper);
    assert_eq!(record["values"]["wrapper"], wrapper);
    assert_eq!(record["values"]["quota"]["source"], "codex");
    assert_eq!(record["values"]["quota"]["auth_path"], auth_path);
    assert_eq!(record["values"]["quota"]["usage_command"], "chatgpt-usage");
    assert_eq!(record["values"]["launch"]["format"], "json");
    assert_eq!(
        record["values"]["launch"]["dangerously_skip_permissions"].as_bool(),
        Some(true)
    );
}

pub fn assert_settings_list_result(list: &Value, id: &str, created_version: &str) {
    assert_secret_absent(list);
    let list_record = find_record(list["records"].as_array().expect("settings records"), id);
    assert_eq!(list_record["id"], id);
    assert_eq!(list_record["display_name"], "Contract opencode profile");
    assert_eq!(list_record["version"], created_version);
    assert!(
        list_record.get("values").is_none(),
        "settings.list summaries must not include full secret-bearing values"
    );
}

pub fn assert_settings_get_result(get: &Value, id: &str, created_version: &str) {
    assert_secret_absent(get);
    assert_eq!(get["record"]["id"], id);
    assert_eq!(get["record"]["display_name"], "Contract opencode profile");
    assert_eq!(get["record"]["version"], created_version);
    assert_eq!(get["record"]["values"]["wrapper"], "opencode1");
}

pub fn assert_settings_update_response(update_response: &Value, created_version: &str) {
    assert_secret_absent(update_response);
    assert_string_absent(
        update_response,
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
    assert_ne!(
        settings_update_version(update_response),
        created_version,
        "settings.update must advance the opaque version so stale writes can conflict"
    );
}

pub fn assert_stale_settings_response(stale_response: &Value) {
    assert_eq!(stale_response["error"]["category"], "conflict");
    assert_secret_absent(stale_response);
}

pub fn assert_settings_delete_result(delete: &Value, id: &str) {
    assert_eq!(delete["deleted"], true);
    assert_eq!(delete["id"], id);
}

pub fn assert_settings_valid_result(valid: &Value) {
    assert_eq!(valid["valid"], true);
    assert!(valid["diagnostics"]
        .as_array()
        .expect("diagnostics")
        .is_empty());
}

pub fn assert_settings_invalid_result(invalid: &Value) {
    assert_eq!(invalid["valid"], false);
    assert!(
        !invalid["diagnostics"]
            .as_array()
            .expect("diagnostics")
            .is_empty(),
        "invalid settings draft must produce diagnostics"
    );
}

pub fn assert_settings_migrate_result(
    result: &Value,
    config_root: &Path,
    before: &BTreeMap<PathBuf, String>,
) {
    assert!(
        !result["actions"].as_array().expect("actions").is_empty(),
        "dry-run migration should describe provider-owned settings actions"
    );
    assert!(result["requires_user_input"].as_bool().is_some());
    assert!(result["warnings"].as_array().is_some());
    assert!(result["diagnostics"].as_array().is_some());
    assert_eq!(
        snapshot_tree(config_root),
        *before,
        "settings.migrate dry_run=true must not write provider settings files"
    );
}

pub fn assert_setup_detect_installed(detect: &Value, data_root: &str, profile_root: &str) {
    assert_setup_auth_sentinel_absent(detect);
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
    assert_detect_contains_sentinels(detect);
    assert_detect_contains_roots(detect, data_root, profile_root);
    assert_detect_profiles(detect);
}

pub fn assert_detect_contains_sentinels(detect: &Value) {
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
}

pub fn assert_detect_contains_roots(detect: &Value, data_root: &str, profile_root: &str) {
    assert!(
        json_contains_string(&detect["profiles"], data_root)
            || json_contains_string(&detect["auth"], data_root),
        "detect profile/auth evidence should include temp data-root evidence {data_root}; detect={detect}"
    );
    assert!(
        json_contains_string(&detect["profiles"], profile_root)
            || json_contains_string(&detect["auth"], profile_root),
        "detect profile/auth evidence should include temp profile-root evidence {profile_root}; detect={detect}"
    );
}

pub fn assert_detect_profiles(detect: &Value) {
    let profiles = detect["profiles"].as_array().expect("profiles array");
    for wrapper in [
        "opencode1",
        "opencode2",
        "opencode3",
        "opencode4",
        "opencode5",
    ] {
        assert!(
            json_contains_string(detect, wrapper),
            "detect should reflect {wrapper} wrapper presence; detect={detect}"
        );
    }
    assert!(
        profiles.len() >= 5,
        "detect should report the five opencode profiles"
    );
}

pub fn assert_setup_install_result(install: &Value) {
    assert_setup_auth_sentinel_absent(install);
    assert!(
        !install["steps"].as_array().expect("steps").is_empty(),
        "setup.install_plan should return actionable setup steps"
    );
}

pub fn assert_setup_sync_result(sync: &Value) {
    assert_setup_auth_sentinel_absent(sync);
    assert!(sync["operations"].as_array().is_some());
    assert!(sync["diagnostics"].as_array().is_some());
}

pub fn assert_setup_missing_dependency_result(detect: &Value) {
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
}

pub fn assert_setup_plan_fixture_missing(detect: &Value) {
    assert_eq!(
        detect["installed"], false,
        "fixture must start with missing opencode/chatgpt-usage/wrapper prerequisites; detect={detect}"
    );
}

pub fn assert_missing_prereq_install_plan(install: &Value) {
    assert_setup_auth_sentinel_absent(install);
    assert!(
        !install["steps"]
            .as_array()
            .expect("install steps")
            .is_empty(),
        "missing prerequisites must produce install/repair steps; install={install}"
    );
    for needle in missing_prereq_install_needles() {
        assert!(
            json_contains_string(&install["steps"], needle),
            "setup.install_plan missing-prerequisite plan must include {needle}; install={install}"
        );
    }
}

pub fn assert_missing_prereq_sync_plan(sync: &Value) {
    assert_setup_auth_sentinel_absent(sync);
    assert!(
        !sync["operations"]
            .as_array()
            .expect("sync operations")
            .is_empty(),
        "missing prerequisites must produce sync repair operations; sync={sync}"
    );
    for needle in missing_prereq_sync_needles() {
        assert!(
            json_contains_string(&sync["operations"], needle),
            "setup.sync_plan missing-prerequisite plan must include {needle}; sync={sync}"
        );
    }
    assert!(sync["diagnostics"].as_array().is_some());
}

pub fn assert_setup_brain_not_advertised(describe: &Value) {
    assert_eq!(describe["capabilities"]["setup_brain"], false);
}

pub fn assert_setup_brain_unsupported_response(response: &Value) {
    assert_eq!(response["error"]["category"], "unsupported");
}

pub fn assert_rotation_allowed(allowed: &Value) {
    assert_eq!(allowed["allowed"], true);
    assert_rotation_requirements(allowed);
    assert!(allowed.get("score").is_some());
    assert!(allowed.get("reason").is_some());
}

pub fn assert_rotation_denied(denied: &Value) {
    assert_eq!(denied["allowed"], false);
    assert_rotation_requirements(denied);
    assert!(denied.get("reason").is_some());
}

pub fn assert_rotation_requirements(result: &Value) {
    assert!(
        result["requirements"]
            .as_array()
            .expect("requirements")
            .len()
            >= 3
    );
}

pub fn assert_rotation_materialized(materialized: &Value) {
    assert_eq!(materialized["changed"], true);
    assert_eq!(
        materialized["target_provider_session_id"],
        ROTATION_SOURCE_SESSION
    );
    let artifacts = materialized["artifacts"].as_array().expect("artifacts");
    assert_eq!(artifacts.len(), 1);
    let artifact_path = Path::new(artifacts[0]["path"].as_str().expect("artifact path"));
    let artifact_bytes = fs::read(artifact_path).expect("materialized native export artifact");
    let artifact_digest = sha256_hex(&artifact_bytes);
    assert_eq!(
        artifact_path.file_stem().and_then(|stem| stem.to_str()),
        Some(artifact_digest.as_str()),
        "rotation artifacts should be content-addressed"
    );
    assert_private_rotation_artifact(artifact_path);
    assert_eq!(
        artifacts[0]["sha256"],
        sha256_hex(&artifact_bytes),
        "artifact digest should cover the imported native export"
    );
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
    let segments = materialized["host_state_plan"]["segments"]
        .as_array()
        .expect("rotation segments");
    assert_eq!(segments[0]["ended_at"], "2026-07-01T00:00:00.000Z");
    assert_eq!(segments[1]["started_at"], segments[0]["ended_at"]);
}

#[cfg(unix)]
fn assert_private_rotation_artifact(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(path)
            .expect("artifact metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600,
        "native transcript artifacts must be owner-readable only"
    );
    assert_eq!(
        fs::metadata(path.parent().expect("artifact parent"))
            .expect("artifact parent metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700,
        "native transcript artifact directories must be owner-accessible only"
    );
}

#[cfg(not(unix))]
fn assert_private_rotation_artifact(_path: &Path) {}

pub fn assert_migration_plan_result(plan: &Value) {
    assert!(
        !plan["actions"].as_array().expect("actions").is_empty(),
        "migration.plan should return planned provider-owned actions"
    );
    assert!(plan["warnings"].as_array().is_some());
    assert!(plan["requires_backup"].as_bool().is_some());
}

pub fn assert_migration_apply_result(apply: &Value, provider_artifact_root: &Path) {
    assert!(
        !apply["applied_actions"]
            .as_array()
            .expect("applied_actions")
            .is_empty(),
        "migration.apply should report applied provider-owned actions"
    );
    assert_migration_artifacts(&apply["artifacts"], provider_artifact_root);
    assert!(apply["warnings"].as_array().is_some());
    assert!(apply["outcome"].as_object().is_some());
}

pub fn assert_migration_artifacts(artifacts: &Value, provider_artifact_root: &Path) {
    let artifacts = artifacts.as_array().expect("artifacts");
    assert!(
        !artifacts.is_empty(),
        "migration.apply should write provider-owned artifacts under the temp roots"
    );
    for artifact in artifacts {
        assert_migration_artifact(artifact, provider_artifact_root);
    }
}

pub fn assert_migration_artifact(artifact: &Value, provider_artifact_root: &Path) {
    assert_valid(artifact, "common.schema.json#/$defs/Artifact");
    let path = artifact["path"].as_str().unwrap_or_else(|| {
        panic!("migration artifact should include a provider-owned path: {artifact}")
    });
    let path = Path::new(path);
    assert!(
        path.starts_with(provider_artifact_root),
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

pub fn success_result(
    output: std::process::Output,
    response_schema: &str,
    result_schema: &str,
) -> Value {
    response_result(&success_response(output, response_schema, result_schema))
}

pub fn success_response(
    output: std::process::Output,
    response_schema: &str,
    result_schema: &str,
) -> Value {
    assert_success_output(&output, response_schema);
    validated_response(&output, response_schema, result_schema)
}

pub fn assert_success_output(output: &std::process::Output, response_schema: &str) {
    assert!(
        output.status.success(),
        "expected success for {response_schema}; exit {:?}; stderr: {}; stdout: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
}

pub fn validated_response(
    output: &std::process::Output,
    response_schema: &str,
    result_schema: &str,
) -> Value {
    let response = json_stdout(output);
    assert_valid(&response, response_schema);
    assert_valid(&response["result"], result_schema);
    response
}

pub fn response_result(response: &Value) -> Value {
    response["result"].clone()
}

pub fn error_response(output: std::process::Output) -> Value {
    assert_error_output(&output);
    let response = json_stdout(&output);
    assert_error_response_envelope(&response);
    response
}

pub fn assert_error_output(output: &std::process::Output) {
    assert!(
        !output.status.success(),
        "expected nonzero error envelope; stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

pub fn assert_error_response_envelope(response: &Value) {
    assert_valid(response, "common.schema.json#/$defs/ErrorResponseEnvelope");
}

pub fn find_record<'a>(records: &'a [Value], id: &str) -> &'a Value {
    expect_record(find_record_by_id(records, id), id, records)
}

pub fn expect_record<'a>(record: Option<&'a Value>, id: &str, records: &[Value]) -> &'a Value {
    record.unwrap_or_else(|| panic!("record {id} not found in settings list: {records:?}"))
}

pub fn find_record_by_id<'a>(records: &'a [Value], id: &str) -> Option<&'a Value> {
    records.iter().find(|record| record_id_is(record, id))
}

pub fn record_id_is(record: &Value, id: &str) -> bool {
    record["id"].as_str() == Some(id)
}

pub fn assert_secret_absent(value: &Value) {
    assert_string_absent(value, SECRET_TOKEN, "contract response");
}

pub fn assert_string_absent(value: &Value, needle: &str, context: &str) {
    let json = value.to_string();
    assert!(
        !json.contains(needle),
        "{context} must not echo auth-token value {needle}: {json}"
    );
}

pub fn assert_setup_auth_sentinel_absent(value: &Value) {
    assert!(
        !json_contains_string(value, SETUP_AUTH_SENTINEL),
        "setup response must not echo auth token sentinel {SETUP_AUTH_SENTINEL}: {value}"
    );
}

pub fn assert_forbidden_live_routes_unchanged(
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
) {
    let forbidden_paths = forbidden_live_route_paths(before, after);
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

pub fn assert_only_tree_changes_under(
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
    root: &Path,
    allowed_root: &Path,
    context: &str,
) {
    for path in changed_tree_paths(before, after) {
        assert_tree_change_under(&absolute_tree_path(root, &path), allowed_root, context);
    }
}

pub fn absolute_tree_path(root: &Path, path: &Path) -> PathBuf {
    root.join(path)
}

pub fn assert_tree_change_under(absolute_path: &Path, allowed_root: &Path, context: &str) {
    assert!(
        absolute_path.starts_with(allowed_root),
        "migration.apply must only add/change/remove files inside provider artifact root {}; changed {context} path {}",
        allowed_root.display(),
        absolute_path.display()
    );
}

pub struct RotationHostSentinels {
    pub host_db: PathBuf,
    pub host_journal: PathBuf,
    pub before: BTreeMap<PathBuf, String>,
}

impl RotationHostSentinels {
    pub fn new(data_root: &Path) -> Self {
        let host_db = data_root.join("oulipoly_state.sqlite");
        let host_journal = data_root.join("rotation-journal.jsonl");
        write_rotation_sentinel(&host_db, b"host db sentinel\n", "write host db sentinel");
        write_rotation_sentinel(
            &host_journal,
            b"host journal sentinel\n",
            "write host journal sentinel",
        );
        let before = file_hashes([host_db.as_path(), host_journal.as_path()]);
        Self {
            host_db,
            host_journal,
            before,
        }
    }

    pub fn assert_unchanged(&self) {
        assert_eq!(
            file_hashes([self.host_db.as_path(), self.host_journal.as_path()]),
            self.before,
            "rotation.materialize must return a host_state_plan, not mutate host DB or journal files"
        );
    }
}

pub fn write_rotation_sentinel(path: &Path, bytes: &[u8], message: &str) {
    fs::write(path, bytes).expect(message);
}

pub struct MigrationSnapshots {
    pub config_before: BTreeMap<PathBuf, String>,
    pub data_before: BTreeMap<PathBuf, String>,
    pub live_before: BTreeMap<PathBuf, String>,
}

impl MigrationSnapshots {
    pub fn capture(host: &HostRoots, live: &LiveConfigFixture) -> Self {
        Self {
            config_before: snapshot_tree(host.config_root()),
            data_before: snapshot_tree(host.data_root()),
            live_before: snapshot_tree(live.config_root()),
        }
    }

    pub fn assert_after_apply(&self, host: &HostRoots, live: &LiveConfigFixture) {
        assert_live_config_unchanged(live.config_root(), &self.live_before);
        let config_after = snapshot_tree(host.config_root());
        let data_after = snapshot_tree(host.data_root());
        assert_only_tree_changes_under(
            &self.config_before,
            &config_after,
            host.config_root(),
            live.provider_artifact_root(),
            "host.config_root",
        );
        assert_only_tree_changes_under(
            &self.data_before,
            &data_after,
            host.data_root(),
            live.provider_artifact_root(),
            "host.data_root",
        );
        assert_forbidden_live_routes_unchanged(&self.config_before, &config_after);
    }
}

pub fn assert_live_config_unchanged(
    live_config_root: &Path,
    live_before: &BTreeMap<PathBuf, String>,
) {
    assert_eq!(
        snapshot_tree(live_config_root),
        *live_before,
        "migration.apply must not mutate the live config route tree"
    );
}
