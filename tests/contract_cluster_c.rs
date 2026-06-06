//! Declared roles: orchestration

mod cluster_c;
#[allow(dead_code)]
mod support;

use cluster_c::*;
use serde_json::json;
use support::{invoke_with_env, invoke_with_host_and_env, json_stdout};

#[test]
fn characterization_codex_chatgpt_usage_windows() {
    let windows = parse_chatgpt_usage_windows(CHATGPT_USAGE_WINDOWS_RAW)
        .expect("captured chatgpt-usage fixture should parse");
    assert_usage_windows_fixture(&windows);
    assert_malformed_usage_inputs_rejected();
}

#[test]
fn contract_quota_source() {
    let home = HomeFixture::new("agent-runner-opencode-quota-source-home");
    home.write_paired_auth(
        b"{\"tokens\":{\"access_token\":\"sentinel\",\"account_id\":\"acct\"}}\n",
    );
    let fake_usage = FakeChatgptUsage::success(CHATGPT_USAGE_WINDOWS_RAW);
    let path = prepend_path(fake_usage.dir());

    let result = success_result(
        invoke_with_env(
            "quota.source",
            quota_base_params(),
            &[
                ("HOME", home.path_str()),
                ("PATH", path.as_str()),
                (
                    "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                    fake_usage.log_path_str(),
                ),
            ],
        ),
        "quota.schema.json#/$defs/QuotaSourceResponse",
        "quota.schema.json#/$defs/QuotaSourceResult",
    );

    assert_present_source_result(&result);
    assert_no_chatgpt_usage_invocation(fake_usage.log_path());

    let missing_home = HomeFixture::new("agent-runner-opencode-quota-source-missing-home");
    let missing_usage = FakeChatgptUsage::success(CHATGPT_USAGE_WINDOWS_RAW);
    let missing_path = prepend_path(missing_usage.dir());
    let missing = success_result(
        invoke_with_env(
            "quota.source",
            quota_base_params(),
            &[
                ("HOME", missing_home.path_str()),
                ("PATH", missing_path.as_str()),
                (
                    "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                    missing_usage.log_path_str(),
                ),
            ],
        ),
        "quota.schema.json#/$defs/QuotaSourceResponse",
        "quota.schema.json#/$defs/QuotaSourceResult",
    );
    assert_missing_source_result(
        &missing,
        "missing-auth source response must still report freshness",
    );
    assert_no_chatgpt_usage_invocation(missing_usage.log_path());

    let unreadable_home = HomeFixture::new("agent-runner-opencode-quota-source-unreadable-home");
    unreadable_home.write_unreadable_paired_auth(
        b"{\"tokens\":{\"access_token\":\"sentinel\",\"account_id\":\"acct\"}}\n",
    );
    let unreadable_usage = FakeChatgptUsage::success(CHATGPT_USAGE_WINDOWS_RAW);
    let unreadable_path = prepend_path(unreadable_usage.dir());
    let unreadable = success_result(
        invoke_with_env(
            "quota.source",
            quota_base_params(),
            &[
                ("HOME", unreadable_home.path_str()),
                ("PATH", unreadable_path.as_str()),
                (
                    "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                    unreadable_usage.log_path_str(),
                ),
            ],
        ),
        "quota.schema.json#/$defs/QuotaSourceResponse",
        "quota.schema.json#/$defs/QuotaSourceResult",
    );
    assert_missing_source_result(
        &unreadable,
        "unreadable-auth source response must still report freshness",
    );
    assert_no_chatgpt_usage_invocation(unreadable_usage.log_path());
}

#[test]
fn contract_quota_source_uses_all_f6_account_mappings() {
    for mapping in F6_ACCOUNT_MAPPINGS {
        let home = HomeFixture::new(&format!(
            "agent-runner-opencode-quota-source-{}-home",
            mapping.settings_id
        ));
        home.write_auth_at(
            mapping.codex_auth_relative,
            b"{\"tokens\":{\"access_token\":\"sentinel\",\"account_id\":\"acct\"}}\n",
        );
        let fake_usage = FakeChatgptUsage::success(CHATGPT_USAGE_WINDOWS_RAW);
        let path = prepend_path(fake_usage.dir());

        let result = success_result(
            invoke_with_env(
                "quota.source",
                quota_params(mapping.settings_id),
                &[
                    ("HOME", home.path_str()),
                    ("PATH", path.as_str()),
                    (
                        "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                        fake_usage.log_path_str(),
                    ),
                ],
            ),
            "quota.schema.json#/$defs/QuotaSourceResponse",
            "quota.schema.json#/$defs/QuotaSourceResult",
        );

        assert_f6_source_mapping(&result, mapping);
        assert_no_chatgpt_usage_invocation(fake_usage.log_path());
    }
}

#[test]
fn contract_quota_probe() {
    let raw_windows = parse_chatgpt_usage_windows(CHATGPT_USAGE_WINDOWS_RAW)
        .expect("captured chatgpt-usage fixture should parse");
    let home = HomeFixture::new("agent-runner-opencode-quota-probe-home");
    let auth_path = home.write_paired_auth(
        b"{\"tokens\":{\"access_token\":\"sentinel\",\"account_id\":\"acct\"}}\n",
    );
    let fake_usage = FakeChatgptUsage::success(CHATGPT_USAGE_WINDOWS_RAW);
    let path = prepend_path(fake_usage.dir());

    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[
                ("HOME", home.path_str()),
                ("PATH", path.as_str()),
                (
                    "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                    fake_usage.log_path_str(),
                ),
            ],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_available_probe_result(&result, &raw_windows);
    assert_probe_invocation(fake_usage.log_path(), &auth_path);

    let failing_usage = FakeChatgptUsage::failure(17, "forced quota probe failure");
    let failing_path = prepend_path(failing_usage.dir());
    let unavailable = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[
                ("HOME", home.path_str()),
                ("PATH", failing_path.as_str()),
                (
                    "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                    failing_usage.log_path_str(),
                ),
            ],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_unavailable_probe_result(&unavailable);

    for (case_name, malformed_stdout) in [
        ("invalid JSON", "{"),
        (
            "out-of-range used_percent",
            r#"{"windows":[{"used_percent":150,"resets_at":"2026-06-04T11:24:05Z"}]}"#,
        ),
        (
            "bad RFC3339 resets_at",
            r#"{"windows":[{"used_percent":25,"resets_at":"not-rfc3339"}]}"#,
        ),
    ] {
        let malformed_usage = FakeChatgptUsage::success(malformed_stdout);
        let malformed_path = prepend_path(malformed_usage.dir());
        assert_malformed_probe_rejected(
            invoke_with_env(
                "quota.probe",
                quota_base_params(),
                &[
                    ("HOME", home.path_str()),
                    ("PATH", malformed_path.as_str()),
                    (
                        "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                        malformed_usage.log_path_str(),
                    ),
                ],
            ),
            case_name,
        );
    }
}

#[test]
fn contract_quota_refresh_auth() {
    let fixture = RefreshAuthFixture::new();

    let result = success_result(
        invoke_with_env(
            "quota.refresh_auth",
            quota_refresh_auth_params(),
            &fixture.env(),
        ),
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );
    assert_refresh_auth_result(&result);
    fixture.assert_auth_unchanged();
}

#[test]
#[ignore]
fn integration_quota_probe_live() {
    let output = invoke_with_host_and_env("quota.probe", quota_base_params(), json!({}), &[]);
    let response = json_stdout(&output);
    assert_quota_probe_response(&response);
    assert_live_probe_result(&response["result"]);
}
