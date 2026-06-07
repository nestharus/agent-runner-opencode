// declared_role: formatter, mapper
#![allow(unused_imports)]

use super::*;

pub fn missing_prereq_install_needles() -> [&'static str; 7] {
    [
        "verify_tool",
        "opencode --version",
        "chatgpt-usage",
        "verify_wrappers",
        "opencode1",
        "prepare_provider_settings",
        "opencode.settings/v1",
    ]
}

pub fn missing_prereq_sync_needles() -> [&'static str; 4] {
    [
        "ensure_profile",
        "opencode1",
        "opencode5",
        "opencode.settings/v1",
    ]
}

pub fn empty_request_params() -> Value {
    json!({})
}

pub fn settings_create_params(secret: Option<&str>) -> Value {
    json!({
        "display_name": "Contract opencode profile",
        "values": opencode_settings_values(secret)
    })
}

pub fn settings_create_params_for_values(values: Value) -> Value {
    json!({
        "display_name": "Contract normalized opencode profile",
        "values": values
    })
}

pub fn settings_get_params(id: &str) -> Value {
    json!({ "id": id })
}

pub fn settings_update_params(id: &str, version: &str, secret: Option<&str>) -> Value {
    json!({
        "id": id,
        "version": version,
        "values": opencode_settings_values(secret)
    })
}

pub fn settings_delete_params(id: &str, version: &str) -> Value {
    json!({ "id": id, "version": version })
}

pub fn valid_settings_validate_params() -> Value {
    settings_validate_params(opencode_settings_values(None))
}

pub fn invalid_settings_validate_params() -> Value {
    settings_validate_params(invalid_opencode_settings_values())
}

pub fn settings_validate_params(values: Value) -> Value {
    json!({ "values": values })
}

pub fn invalid_opencode_settings_values() -> Value {
    json!({
        "provider": "opencode",
        "wrapper": "opencode99",
        "model": { "provider_model": "", "variant": "impossible" },
        "quota": { "auth_path": "" }
    })
}

pub fn settings_migrate_params() -> Value {
    json!({
        "dry_run": true,
        "legacy": legacy_fixture()
    })
}

pub fn setup_detect_params(data_root: &str, profile_root: &str) -> Value {
    json!({
        "data_root": data_root,
        "profile_root": profile_root
    })
}

pub fn setup_detect_data_root_params(data_root: &str) -> Value {
    json!({ "data_root": data_root })
}

pub fn setup_install_plan_params(data_root: &str, profile_root: &str) -> Value {
    json!({
        "target": "local",
        "data_root": data_root,
        "profile_root": profile_root
    })
}

pub fn setup_sync_plan_params(data_root: &str, profile_root: &str) -> Value {
    json!({
        "desired_profiles": ["opencode1", "opencode2", "opencode3", "opencode4", "opencode5"],
        "settings_schema_id": "opencode.settings/v1",
        "data_root": data_root,
        "profile_root": profile_root
    })
}

pub fn setup_brain_turn_params() -> Value {
    json!({
        "conversation_id": "setup-brain-contract-test",
        "message": { "role": "user", "content": "configure opencode" }
    })
}

pub fn opencode_settings_values(secret: Option<&str>) -> Value {
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

pub fn path_wrapped_opencode_settings_values(wrapper: &str) -> Value {
    let mut values = opencode_settings_values(None);
    values["profile"] = json!(format!("/tmp/host-bin/{wrapper}"));
    values["wrapper"] = json!(format!("/tmp/host-bin/{wrapper}"));
    values["quota"]["auth_path"] = json!("~/.codex/wrong-auth.json");
    values
}

pub fn rotation_assess_params(allowed: bool) -> Value {
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

pub fn rotation_requirements(allowed: bool) -> Value {
    json!([
        { "kind": "quota_available", "met": allowed, "detail": "target account has usable quota" },
        { "kind": "session_exportable", "met": allowed, "detail": "source session can be exported" },
        { "kind": "target_profile_present", "met": allowed, "detail": "target opencode profile exists" }
    ])
}

pub fn rotation_materialize_params() -> Value {
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
    })
}

pub fn migration_plan_params(live: &LiveConfigFixture) -> Value {
    json!({
        "legacy": legacy_fixture(),
        "target_provider": "agent-runner-opencode",
        "scope": "provider_owned",
        "live_config_root": live.config_root().to_string_lossy(),
        "artifact_root": live.provider_artifact_root().to_string_lossy()
    })
}

pub fn migration_apply_params(live: &LiveConfigFixture) -> Value {
    json!({
        "legacy": legacy_fixture(),
        "target_provider": "agent-runner-opencode",
        "scope": "provider_owned",
        "live_config_root": live.config_root().to_string_lossy(),
        "artifact_root": live.provider_artifact_root().to_string_lossy(),
        "confirmation": { "approved": true, "source": "contract-test" }
    })
}
