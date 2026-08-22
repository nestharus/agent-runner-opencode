//! Declared roles: orchestration

mod cluster_c;
#[allow(dead_code)]
mod support;

use cluster_c::*;
use serde_json::json;
use support::{invoke_with_env, invoke_with_host_and_env, json_stdout};

struct RejectWrites;

struct IsolatedQuotaSettings {
    _root: tempfile::TempDir,
    host_overrides: serde_json::Value,
}

impl IsolatedQuotaSettings {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create isolated quota host root");
        let config_root = root.path().join("config");
        let data_root = root.path().join("data");
        let store_root = config_root.join("agent-runner-opencode");
        std::fs::create_dir_all(&store_root).expect("create isolated quota settings root");
        std::fs::create_dir_all(&data_root).expect("create isolated quota data root");
        let store = json!({
            "schema_version": 3,
            "records": [{
                "id": "opencode3",
                "display_name": "isolated quota account",
                "version": "fixture-v1",
                "values": {
                    "provider": "opencode",
                    "profile": "opencode3",
                    "wrapper": "opencode3",
                    "model": { "selection": "requested" },
                    "quota": {
                        "source": "opencode_auth",
                        "auth_path": "~/.opencode3/opencode/auth.json",
                        "probe": "native_chatgpt_usage"
                    },
                    "launch": {
                        "dangerously_skip_permissions": true,
                        "format": "json",
                        "preserve_pure_wrapper": true
                    },
                    "extra_env": {},
                    "mode": "non_interactive"
                }
            }],
            "history": [],
            "mutation_receipts": {}
        });
        std::fs::write(
            store_root.join("settings-store.json"),
            serde_json::to_vec_pretty(&store).expect("serialize isolated quota settings store"),
        )
        .expect("write isolated quota settings store");
        let host_overrides = json!({
            "config_root": config_root.to_string_lossy(),
            "data_root": data_root.to_string_lossy(),
        });
        Self {
            _root: root,
            host_overrides,
        }
    }

    fn host_overrides(&self) -> serde_json::Value {
        self.host_overrides.clone()
    }

    fn delete_settings_record(&self) {
        let output = support::invoke_validated_with_host(
            "settings.delete",
            json!({ "id": "opencode3", "version": "fixture-v1" }),
            self.host_overrides(),
            "settings.schema.json#/$defs/SettingsDeleteRequest",
        );
        let response = json_stdout(&output);
        support::assert_valid(
            &response,
            "settings.schema.json#/$defs/SettingsDeleteResponse",
        );
        assert_eq!(
            response["ok"], true,
            "settings deletion response={response}"
        );
    }
}

impl std::io::Write for RejectWrites {
    fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "simulated quota response handoff failure",
        ))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn contract_quota_source() {
    let home = HomeFixture::new("agent-runner-opencode-quota-source-home");
    home.write_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());

    let result = success_result(
        invoke_with_env(
            "quota.source",
            quota_base_params(),
            &[("HOME", home.path_str())],
        ),
        "quota.schema.json#/$defs/QuotaSourceResponse",
        "quota.schema.json#/$defs/QuotaSourceResult",
    );

    assert_present_source_result(&result);

    let missing_home = HomeFixture::new("agent-runner-opencode-quota-source-missing-home");
    let missing = success_result(
        invoke_with_env(
            "quota.source",
            quota_base_params(),
            &[("HOME", missing_home.path_str())],
        ),
        "quota.schema.json#/$defs/QuotaSourceResponse",
        "quota.schema.json#/$defs/QuotaSourceResult",
    );
    assert_missing_source_result(
        &missing,
        "missing-auth source response must still report freshness",
    );

    let unreadable_home = HomeFixture::new("agent-runner-opencode-quota-source-unreadable-home");
    unreadable_home.write_unreadable_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());
    let unreadable = success_result(
        invoke_with_env(
            "quota.source",
            quota_base_params(),
            &[("HOME", unreadable_home.path_str())],
        ),
        "quota.schema.json#/$defs/QuotaSourceResponse",
        "quota.schema.json#/$defs/QuotaSourceResult",
    );
    assert_missing_source_result(
        &unreadable,
        "unreadable-auth source response must still report freshness",
    );
}

#[test]
fn contract_quota_source_uses_all_f6_account_mappings() {
    for mapping in F6_ACCOUNT_MAPPINGS {
        let home = HomeFixture::new(&format!(
            "agent-runner-opencode-quota-source-{}-home",
            mapping.settings_id
        ));
        home.write_auth_at(
            mapping.opencode_auth_relative,
            opencode_auth_json("sentinel", "acct").as_bytes(),
        );
        let result = success_result(
            invoke_with_env(
                "quota.source",
                quota_params(mapping.settings_id),
                &[("HOME", home.path_str())],
            ),
            "quota.schema.json#/$defs/QuotaSourceResponse",
            "quota.schema.json#/$defs/QuotaSourceResult",
        );

        assert_f6_source_mapping(&result, mapping);
    }
}

#[test]
fn contract_quota_probe() {
    let raw_windows = native_wham_expected_windows();
    let home = HomeFixture::new("agent-runner-opencode-quota-probe-home");
    home.write_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());
    let curl = FakeNativeCurl::new();
    let path = curl.path_env();

    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_available_probe_result(&result, &raw_windows);
    curl.assert_native_invocation();

    let failing_curl = FakeNativeCurl::transport_failure(17, "forced quota probe failure");
    let failing_path = failing_curl.path_env();
    let unavailable = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", failing_path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_unavailable_probe_result(&unavailable);

    for (case_name, malformed_body) in [
        ("invalid JSON", "{"),
        (
            "out-of-range used_percent",
            r#"{"rate_limit":{"primary_window":{"used_percent":150,"reset_at":1780572245}}}"#,
        ),
        (
            "non-numeric reset_at",
            r#"{"rate_limit":{"primary_window":{"used_percent":25,"reset_at":"not-unix-seconds"}}}"#,
        ),
    ] {
        let malformed_curl = FakeNativeCurl::with_response(200, malformed_body);
        let malformed_path = malformed_curl.path_env();
        assert_malformed_probe_rejected(
            invoke_with_env(
                "quota.probe",
                quota_base_params(),
                &[("HOME", home.path_str()), ("PATH", malformed_path.as_str())],
            ),
            case_name,
        );
    }
}

#[test]
fn contract_quota_probe_uses_native_opencode_auth_adapter_by_default() {
    let raw_windows = native_wham_expected_windows();
    let home = HomeFixture::new("agent-runner-opencode-native-quota-home");
    home.write_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());
    let curl = FakeNativeCurl::new();
    let path = curl.path_env();
    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_available_probe_result(&result, &raw_windows);
    curl.assert_native_invocation();
}

#[test]
fn contract_quota_observer_identity_is_reused_across_provider_processes() {
    let runtime = IsolatedQuotaSettings::new();
    let home = HomeFixture::new("agent-runner-opencode-bound-quota-observer-home");
    home.write_auth_at(
        ".opencode3/opencode/auth.json",
        opencode_auth_json("sentinel", "acct").as_bytes(),
    );
    let admitted_curl = FakeNativeCurl::new();
    let admitted_path = admitted_curl.path_env();
    let first = success_result(
        support::invoke_validated_with_host_and_env(
            "quota.probe",
            quota_base_params(),
            runtime.host_overrides(),
            "quota.schema.json#/$defs/QuotaProbeRequest",
            &[("HOME", home.path_str()), ("PATH", admitted_path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_available_probe_result(&first, &native_wham_expected_windows());
    let observer_state_path = std::path::Path::new(
        runtime.host_overrides()["data_root"]
            .as_str()
            .expect("isolated quota data root"),
    )
    .join("provider-state/opencode/quota-observers/opencode3.json");
    let observer_state: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&observer_state_path).expect("read quota observer state"),
    )
    .expect("parse quota observer state");
    assert_eq!(observer_state["schema_version"], 3);
    assert_eq!(
        observer_state["observer_contract"],
        "agent-runner-opencode.chatgpt-wham-http/v1"
    );
    assert_eq!(
        observer_state["transport_kind"],
        "contract_test_external_curl"
    );
    assert_eq!(
        observer_state["implementation_manifest_id"],
        format!(
            "contract-test-fixture:curl:{}",
            observer_state["program_sha256"]
                .as_str()
                .expect("observer program digest")
        )
    );
    assert_eq!(
        observer_state["implementation_version"],
        "contract-test-fixture"
    );

    let ambient_curl = FakeNativeCurl::http_failure(503, r#"{"detail":"wrong observer"}"#);
    let ambient_path = ambient_curl.path_env();
    let second = success_result(
        support::invoke_validated_with_host_and_env(
            "quota.probe",
            quota_base_params(),
            runtime.host_overrides(),
            "quota.schema.json#/$defs/QuotaProbeRequest",
            &[("HOME", home.path_str()), ("PATH", ambient_path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_available_probe_result(&second, &native_wham_expected_windows());
    assert!(
        !ambient_curl.invocation_path.exists(),
        "later ambient PATH must not replace the bound quota observer"
    );

    std::fs::write(admitted_curl.dir.join("curl"), "#!/bin/sh\nexit 0\n")
        .expect("mutate admitted quota observer fixture");
    let changed = support::invoke_validated_with_host_and_env(
        "quota.probe",
        quota_base_params(),
        runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaProbeRequest",
        &[("HOME", home.path_str()), ("PATH", ambient_path.as_str())],
    );
    assert!(!changed.status.success());
    let changed_response = json_stdout(&changed);
    support::assert_valid(
        &changed_response,
        "quota.schema.json#/$defs/QuotaProbeErrorResponse",
    );
    assert_eq!(
        changed_response["error"]["code"],
        "quota_observer_implementation_changed"
    );
}

#[test]
fn contract_quota_observer_identity_lock_obeys_host_deadline() {
    let runtime = IsolatedQuotaSettings::new();
    let home = HomeFixture::new("agent-runner-opencode-quota-observer-lock-home");
    home.write_auth_at(
        ".opencode3/opencode/auth.json",
        opencode_auth_json("sentinel", "acct").as_bytes(),
    );
    let fake_curl = FakeNativeCurl::new();
    let path = fake_curl.path_env();
    let host = runtime.host_overrides();
    let observer_root = std::path::Path::new(
        host["data_root"]
            .as_str()
            .expect("isolated quota data root"),
    )
    .join("provider-state/opencode/quota-observers");
    std::fs::create_dir_all(&observer_root).expect("create quota observer state root");
    let observer_lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(observer_root.join("opencode3.lock"))
        .expect("open quota observer lock");
    fs2::FileExt::lock_exclusive(&observer_lock).expect("hold quota observer lock");
    let request = support::validated_request_envelope(
        "quota.probe",
        quota_base_params(),
        host,
        "quota.schema.json#/$defs/QuotaProbeRequest",
    );
    let (failed, bounded_elapsed) = support::invoke_with_request_and_env_fresh_deadline(
        "quota.probe",
        request,
        &[("HOME", home.path_str()), ("PATH", path.as_str())],
        std::time::Duration::from_secs(10),
    );
    assert!(bounded_elapsed < std::time::Duration::from_secs(60));
    assert!(!failed.status.success());
    let response = json_stdout(&failed);
    support::assert_valid(
        &response,
        "quota.schema.json#/$defs/QuotaProbeErrorResponse",
    );
    assert_eq!(response["error"]["code"], "quota_observer_lock_timeout");
    assert!(
        !fake_curl.invocation_path.exists(),
        "quota transport must not start before observer identity admission"
    );
}

#[test]
fn contract_quota_observer_rejects_curl_config_injection_before_spawn() {
    let home = HomeFixture::new("agent-runner-opencode-quota-config-injection-home");
    let auth = serde_json::to_vec(&json!({
        "openai": {
            "access": "sentinel\nurl = \"https://attacker.invalid\"",
            "accountId": "acct",
        }
    }))
    .expect("serialize hostile auth fixture");
    home.write_paired_auth(&auth);
    let curl = FakeNativeCurl::new();
    let path = curl.path_env();
    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_eq!(result["available"], false);
    assert!(result["detail"]
        .as_str()
        .is_some_and(|detail| detail.contains("control character")));
    assert!(!curl.invocation_path.exists(), "unsafe auth reached curl");
}

#[test]
fn contract_quota_observer_rejects_oversized_auth_before_spawn() {
    let home = HomeFixture::new("agent-runner-opencode-quota-oversized-auth-home");
    home.write_paired_auth(&vec![b'x'; 1024 * 1024 + 1]);
    let curl = FakeNativeCurl::new();
    let path = curl.path_env();
    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_eq!(result["available"], false);
    assert!(result["detail"]
        .as_str()
        .is_some_and(|detail| detail.contains("1048576-byte bound")));
    assert!(
        !curl.invocation_path.exists(),
        "oversized auth must fail before quota transport"
    );
}

#[test]
fn contract_quota_observer_bounds_oversized_wham_output() {
    let home = HomeFixture::new("agent-runner-opencode-quota-oversized-wham-home");
    home.write_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());
    let body = "x".repeat(512 * 1024 + 1);
    let curl = FakeNativeCurl::with_response(200, &body);
    let path = curl.path_env();
    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert_eq!(result["available"], false);
    assert!(result["detail"]
        .as_str()
        .is_some_and(|detail| detail.contains("bounded response")));
    curl.assert_native_invocation();
}

#[test]
fn contract_native_quota_failure_names_wham_boundary() {
    let home = HomeFixture::new("agent-runner-opencode-native-quota-failure-home");
    home.write_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());
    let curl = FakeNativeCurl::http_failure(503, r#"{"detail":"maintenance"}"#);
    let path = curl.path_env();
    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );

    assert_eq!(result["available"], false);
    let detail = result["detail"].as_str().expect("native failure detail");
    assert!(detail.contains("ChatGPT WHAM API returned HTTP 503"));
    assert!(detail.contains("maintenance"));
}

#[test]
fn contract_native_wham_401_carries_typed_refresh_advice() {
    let home = HomeFixture::new("agent-runner-opencode-native-quota-401-home");
    let auth_path = home.write_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());
    let marker = home.path.join("native-wham-refresh-ran");
    let curl = FakeNativeCurl::http_failure(
        401,
        r#"{"detail":"Provided authentication token is expired."}"#,
    );
    let fake_auth =
        FakeOpencodeAuth::touches_marker_and_rewrites_auth("opencode3", &marker, &auth_path);
    let path = prepend_paths(&[fake_auth.dir(), &curl.dir]);
    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );

    assert!(
        !marker.exists(),
        "quota.probe must project refresh advice without mutating auth; result={result}"
    );
    assert_eq!(result["available"], false);
    let detail = result["detail"].as_str().expect("native 401 detail");
    assert!(detail.contains("ChatGPT WHAM API returned HTTP 401"));
    assert!(detail.contains("invoke quota.refresh_auth"));
}

#[test]
fn contract_quota_probe_reports_refresh_requirement_without_mutating_auth() {
    let home = HomeFixture::new("agent-runner-opencode-quota-probe-refresh-home");
    let auth_path = home.write_paired_auth(opencode_auth_json("expired", "acct").as_bytes());
    let auth_before = file_sha256(&auth_path);
    let marker = home.path.join("refresh-ran");
    let curl = FakeNativeCurl::http_failure(
        401,
        r#"{"detail":"Provided authentication token is expired."}"#,
    );
    let fake_auth =
        FakeOpencodeAuth::touches_marker_and_rewrites_auth("opencode3", &marker, &auth_path);
    let path = prepend_paths(&[fake_auth.dir(), &curl.dir]);

    let result = success_result(
        invoke_with_env(
            "quota.probe",
            quota_base_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaProbeResponse",
        "quota.schema.json#/$defs/QuotaProbeResult",
    );

    assert_eq!(result["available"], false);
    assert!(result["detail"]
        .as_str()
        .is_some_and(|detail| detail.contains("invoke quota.refresh_auth")));
    curl.assert_native_invocation();
    assert!(
        !marker.exists(),
        "quota.probe must not run the auth wrapper"
    );
    assert_eq!(
        file_sha256(&auth_path),
        auth_before,
        "quota.probe must leave the selected credential source unchanged"
    );
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
    fixture.assert_auth_changed();
    fixture.assert_auth_command_invoked();
}

#[test]
fn contract_quota_refresh_changed_credentials_with_nonzero_exit_require_reconciliation() {
    assert_changed_auth_requires_reconciliation(
        "nonzero-exit",
        fake_opencode_auth_rewrite_then_fail_script,
        "credential_changed_with_command_failure",
        true,
    );
}

#[test]
fn contract_quota_refresh_unicode_failure_replays_valid_reconciliation() {
    assert_changed_auth_requires_reconciliation(
        "unicode-nonzero-exit",
        fake_opencode_auth_rewrite_then_unicode_fail_script,
        "credential_changed_with_command_failure",
        false,
    );
}

#[test]
fn contract_quota_refresh_changed_credentials_with_oversized_output_require_reconciliation() {
    assert_changed_auth_requires_reconciliation(
        "oversized-output",
        fake_opencode_auth_rewrite_then_oversize_script,
        "credential_changed_with_oversized_output",
        false,
    );
}

fn assert_changed_auth_requires_reconciliation(
    case: &str,
    script: fn(&std::path::Path) -> String,
    expected_reason: &str,
    reconcile: bool,
) {
    let runtime = IsolatedQuotaSettings::new();
    let home = HomeFixture::new(&format!("agent-runner-opencode-quota-refresh-{case}-home"));
    let auth_path =
        home.write_paired_auth(opencode_auth_json("refresh-sentinel", "acct").as_bytes());
    let before = file_sha256(&auth_path);
    let usage_log = home.path.join("quota-operation.log");
    let fake_curl = FakeNativeCurl::transport_failure(17, "probe must not settle refresh");
    let fake_auth = FakeOpencodeAuth::with_script("opencode3", script(&auth_path));
    let path = prepend_paths(&[fake_auth.dir(), &fake_curl.dir]);
    let env = [
        ("HOME", home.path_str()),
        ("PATH", path.as_str()),
        (
            "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
            usage_log.to_str().expect("quota log path UTF-8"),
        ),
    ];
    let request = support::validated_request_envelope(
        "quota.refresh_auth",
        quota_refresh_auth_params(),
        runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaRefreshAuthRequest",
    );
    support::ensure_default_runtime_settings(&request);

    let mut first_reconciliation = None;
    for attempt in 0..2 {
        let output =
            support::invoke_with_request_and_env("quota.refresh_auth", request.clone(), &env);
        assert_eq!(
            output.status.code(),
            Some(2),
            "attempt={attempt} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let response = json_stdout(&output);
        support::assert_valid(
            &response,
            "quota.schema.json#/$defs/QuotaRefreshAuthErrorResponse",
        );
        assert_eq!(
            response["error"]["code"],
            "quota_refresh_reconciliation_required"
        );
        assert_eq!(
            response["error"]["details"]["reconciliation_evidence"]["reason"],
            expected_reason
        );
        assert_eq!(
            response["error"]["details"]["reconciliation_evidence"]["credential_effect"],
            "credentials_changed"
        );
        let reconciliation = response["error"]["details"]["reconciliation_evidence"].clone();
        assert!(
            reconciliation["detail"]
                .as_str()
                .is_some_and(|detail| detail.len() <= 500),
            "persisted reconciliation detail must honor its UTF-8 byte bound"
        );
        match first_reconciliation.as_ref() {
            Some(first) => assert_eq!(
                &reconciliation, first,
                "exact replay must preserve identical reconciliation evidence"
            ),
            None => first_reconciliation = Some(reconciliation),
        }
    }

    assert_ne!(
        file_sha256(&auth_path),
        before,
        "the native participant must expose the changed credential effect"
    );
    if reconcile {
        let mut stale_reconciliation_request = request.clone();
        stale_reconciliation_request["params"]["context"]["reconciliation"] = json!({
            "disposition": "accept_current_credentials",
            "credential_source_sha256": "0".repeat(64),
        });
        let stale = support::invoke_with_request_and_env(
            "quota.refresh_auth",
            stale_reconciliation_request,
            &env,
        );
        assert_eq!(stale.status.code(), Some(2));
        let stale_response = json_stdout(&stale);
        support::assert_valid(
            &stale_response,
            "quota.schema.json#/$defs/QuotaRefreshAuthErrorResponse",
        );
        assert_eq!(
            stale_response["error"]["code"],
            "quota_refresh_reconciliation_mismatch"
        );

        let mut reconciliation_request = request.clone();
        reconciliation_request["params"]["context"]["reconciliation"] = json!({
            "disposition": "accept_current_credentials",
            "credential_source_sha256": file_sha256(&auth_path),
        });
        let resolved = success_result(
            support::invoke_with_request_and_env(
                "quota.refresh_auth",
                reconciliation_request,
                &env,
            ),
            "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
            "quota.schema.json#/$defs/QuotaRefreshAuthResult",
        );
        assert_eq!(resolved["refreshed"], true);
        let replay = success_result(
            support::invoke_with_request_and_env("quota.refresh_auth", request, &env),
            "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
            "quota.schema.json#/$defs/QuotaRefreshAuthResult",
        );
        assert_eq!(replay, resolved, "terminal reconciliation must replay");
    }
    assert_eq!(
        optional_usage_log(&usage_log)
            .matches("auth argv=auth list")
            .count(),
        1,
        "exact reconciliation replay must not repeat the changed credential effect"
    );
}

#[test]
fn contract_quota_refresh_hanging_auth_releases_account_capability_lock() {
    let runtime = IsolatedQuotaSettings::new();
    let home = HomeFixture::new("agent-runner-opencode-quota-refresh-timeout-home");
    let auth_path =
        home.write_paired_auth(opencode_auth_json("refresh-sentinel", "acct").as_bytes());
    let timeout_marker = home.path.join("auth-timeout-observed");
    let fake_curl = FakeNativeCurl::transport_failure(17, "probe unavailable during refresh");
    let fake_auth = FakeOpencodeAuth::with_script(
        "opencode3",
        fake_opencode_auth_timeout_once_script(&timeout_marker, &auth_path),
    );
    let path = prepend_paths(&[fake_auth.dir(), &fake_curl.dir]);
    let env = [("HOME", home.path_str()), ("PATH", path.as_str())];

    let first_request = support::validated_request_envelope(
        "quota.refresh_auth",
        quota_refresh_auth_params(),
        runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaRefreshAuthRequest",
    );
    support::ensure_default_runtime_settings(&first_request);
    let (first, bounded_elapsed) = support::invoke_with_request_and_env_fresh_deadline(
        "quota.refresh_auth",
        first_request,
        &env,
        std::time::Duration::from_secs(20),
    );
    assert!(
        bounded_elapsed < std::time::Duration::from_secs(60),
        "a stalled auth child must be terminated within the request bound"
    );
    assert_eq!(
        first.status.code(),
        Some(2),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    let first_response = json_stdout(&first);
    support::assert_valid(
        &first_response,
        "quota.schema.json#/$defs/QuotaRefreshAuthErrorResponse",
    );
    assert_eq!(
        first_response["error"]["code"],
        "quota_refresh_reconciliation_required"
    );
    assert!(timeout_marker.exists(), "the hanging auth path must run");

    let second_request = support::validated_request_envelope(
        "quota.refresh_auth",
        quota_refresh_auth_params(),
        runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaRefreshAuthRequest",
    );
    support::ensure_default_runtime_settings(&second_request);
    let second = success_result(
        support::invoke_with_request_and_env_fresh_deadline(
            "quota.refresh_auth",
            second_request,
            &env,
            std::time::Duration::from_secs(20),
        )
        .0,
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );
    assert_refresh_auth_result(&second);
}

#[test]
fn contract_quota_refresh_auth_replays_committed_observation_after_response_loss() {
    let runtime = IsolatedQuotaSettings::new();
    let fixture = RefreshAuthFixture::new();
    let mut request = support::validated_request_envelope(
        "quota.refresh_auth",
        quota_refresh_auth_params(),
        runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaRefreshAuthRequest",
    );
    request["request_id"] = json!(format!(
        "req-quota-refresh-response-loss-{}",
        std::process::id()
    ));
    support::ensure_default_runtime_settings(&request);

    let env = fixture.env();
    let prior_env = env
        .iter()
        .map(|(key, _)| (*key, std::env::var_os(key)))
        .collect::<Vec<_>>();
    for (key, value) in env {
        std::env::set_var(key, value);
    }
    let args = vec![
        "agent-runner-opencode".to_string(),
        "quota.refresh_auth".to_string(),
    ];
    let lost_response_exit = agent_runner_opencode::write_invocation(
        &args,
        &serde_json::to_vec(&request).expect("serialize quota request"),
        &mut RejectWrites,
    );
    for (key, value) in prior_env {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
    assert_eq!(
        lost_response_exit, 1,
        "the first invocation must commit before its closed response route fails"
    );
    fixture.assert_auth_changed();
    assert_eq!(
        optional_usage_log(&fixture.quota_log_path)
            .matches("auth argv=auth list")
            .count(),
        1,
        "the response-lost invocation must execute exactly one native auth refresh"
    );
    runtime.delete_settings_record();

    let replay = success_result(
        support::invoke_with_request_and_env("quota.refresh_auth", request.clone(), &fixture.env()),
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );
    assert_refresh_auth_result(&replay);
    let replay_again = success_result(
        support::invoke_with_request_and_env("quota.refresh_auth", request.clone(), &fixture.env()),
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );
    assert_eq!(
        replay_again, replay,
        "exact retries must replay one receipt"
    );
    assert_eq!(
        optional_usage_log(&fixture.quota_log_path)
            .matches("auth argv=auth list")
            .count(),
        1,
        "receipt replay must not invoke native auth again"
    );

    let mut changed_binding = request;
    changed_binding["params"]["context"]["reason"] = json!("different-binding");
    let conflict =
        support::invoke_with_request_and_env("quota.refresh_auth", changed_binding, &fixture.env());
    assert_eq!(conflict.status.code(), Some(2));
    let conflict_response = json_stdout(&conflict);
    support::assert_valid(
        &conflict_response,
        "quota.schema.json#/$defs/QuotaRefreshAuthErrorResponse",
    );
    assert_eq!(
        conflict_response["error"]["code"],
        "quota_refresh_request_conflict"
    );
    assert_eq!(
        optional_usage_log(&fixture.quota_log_path)
            .matches("auth argv=auth list")
            .count(),
        1,
        "a changed binding on the same request_id must not invoke native auth"
    );
}

#[test]
fn contract_quota_refresh_auth_does_not_repeat_an_unsettled_native_effect() {
    let runtime = IsolatedQuotaSettings::new();
    let fixture = RefreshAuthFixture::new();
    let request = support::validated_request_envelope(
        "quota.refresh_auth",
        quota_refresh_auth_params(),
        runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaRefreshAuthRequest",
    );
    let first = success_result(
        support::invoke_with_request_and_env("quota.refresh_auth", request.clone(), &fixture.env()),
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );
    assert_refresh_auth_result(&first);

    let request_id = request["request_id"].as_str().expect("quota request id");
    let data_root = request["host"]["data_root"]
        .as_str()
        .expect("quota data root");
    let operation_path = std::path::Path::new(data_root)
        .join("provider-state/opencode/quota/auth-refresh/requests")
        .join(format!(
            "{}.json",
            agent_runner_opencode::encoding::sha256_hex(request_id.as_bytes())
        ));
    let mut operation: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&operation_path).expect("read committed quota refresh operation"),
    )
    .expect("parse committed quota refresh operation");
    operation["phase"] = json!("native_effect_admitted");
    operation
        .as_object_mut()
        .expect("quota operation object")
        .remove("committed_at_unix_ms");
    operation
        .as_object_mut()
        .expect("quota operation object")
        .remove("result");
    operation
        .as_object_mut()
        .expect("quota operation object")
        .remove("actor_terminal_at_unix_ms");
    std::fs::write(
        &operation_path,
        serde_json::to_vec_pretty(&operation).expect("serialize unsettled quota operation"),
    )
    .expect("simulate provider loss after native effect admission");
    runtime.delete_settings_record();

    let retry = support::invoke_with_request_and_env("quota.refresh_auth", request, &fixture.env());
    assert_eq!(retry.status.code(), Some(2));
    let response = json_stdout(&retry);
    support::assert_valid(
        &response,
        "quota.schema.json#/$defs/QuotaRefreshAuthErrorResponse",
    );
    assert_eq!(
        response["error"]["code"],
        "quota_refresh_reconciliation_required"
    );
    assert_eq!(
        optional_usage_log(&fixture.quota_log_path)
            .matches("auth argv=auth list")
            .count(),
        1,
        "an admitted but unsettled native effect must never be invoked again"
    );
    let unresolved: serde_json::Value = serde_json::from_slice(
        &std::fs::read(operation_path).expect("read unresolved quota refresh operation"),
    )
    .expect("parse unresolved quota refresh operation");
    assert_eq!(unresolved["phase"], "reconciliation_required");
}

#[test]
fn contract_auth_list_without_credential_change_is_not_reported_as_refresh() {
    let home = HomeFixture::new("agent-runner-opencode-quota-list-only-home");
    home.write_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());
    let curl = FakeNativeCurl::new();
    let fake_auth = FakeOpencodeAuth::success("opencode3");
    let path = prepend_paths(&[fake_auth.dir(), &curl.dir]);
    let result = success_result(
        invoke_with_env(
            "quota.refresh_auth",
            quota_refresh_auth_params(),
            &[("HOME", home.path_str()), ("PATH", path.as_str())],
        ),
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );

    assert_eq!(result["refreshed"], false);
    assert_eq!(result["available"], true);
    assert!(result["detail"]
        .as_str()
        .is_some_and(|detail| detail.contains("without an observed credential change")));
}

#[test]
fn contract_quota_refresh_serializes_shared_credentials_across_data_roots() {
    let first_runtime = IsolatedQuotaSettings::new();
    let second_runtime = IsolatedQuotaSettings::new();
    let home = HomeFixture::new("agent-runner-opencode-cross-root-refresh-home");
    let auth_path = home.write_paired_auth(opencode_auth_json("sentinel", "acct").as_bytes());
    let first_started = home.path.join("first-auth-started");
    let first_auth = FakeOpencodeAuth::with_script(
        "opencode3",
        format!(
            "#!/bin/sh\n: > {}\n/bin/sleep 2\nexit 0\n",
            shell_single_quote(&first_started.to_string_lossy())
        ),
    );
    let second_auth = FakeOpencodeAuth::rewrites_auth("opencode3", &auth_path);
    let first_curl = FakeNativeCurl::new();
    let second_curl = FakeNativeCurl::new();
    let first_path = prepend_paths(&[first_auth.dir(), &first_curl.dir]);
    let second_path = prepend_paths(&[second_auth.dir(), &second_curl.dir]);
    let first_request = support::validated_request_envelope(
        "quota.refresh_auth",
        quota_refresh_auth_params(),
        first_runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaRefreshAuthRequest",
    );
    let second_request = support::validated_request_envelope(
        "quota.refresh_auth",
        quota_refresh_auth_params(),
        second_runtime.host_overrides(),
        "quota.schema.json#/$defs/QuotaRefreshAuthRequest",
    );
    let first_home = home.path_str().to_string();
    let first = std::thread::spawn(move || {
        support::invoke_with_request_and_env(
            "quota.refresh_auth",
            first_request,
            &[("HOME", first_home.as_str()), ("PATH", first_path.as_str())],
        )
    });
    let wait_started = std::time::Instant::now();
    while !first_started.exists() && wait_started.elapsed() < std::time::Duration::from_secs(20) {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        first_started.exists(),
        "the no-change refresh must enter its native observation interval"
    );

    let mut second = support::invoke_with_request_and_env(
        "quota.refresh_auth",
        second_request.clone(),
        &[("HOME", home.path_str()), ("PATH", second_path.as_str())],
    );
    let first = first.join().expect("join first cross-root refresh");
    if !second.status.success()
        && json_stdout(&second)["error"]["code"] == "quota_refresh_lock_timeout"
    {
        second = support::invoke_with_request_and_env(
            "quota.refresh_auth",
            second_request,
            &[("HOME", home.path_str()), ("PATH", second_path.as_str())],
        );
    }
    let first_result = success_result(
        first,
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );
    let second_result = success_result(
        second,
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );
    assert_eq!(
        first_result["refreshed"], false,
        "a no-change command must not observe the sibling request's later credential mutation"
    );
    assert_eq!(second_result["refreshed"], true);
}

#[test]
#[ignore]
fn integration_quota_probe_live() {
    let path = std::env::var("PATH").expect("live PATH");
    let home = std::env::var("HOME").expect("live HOME");
    let output = invoke_with_host_and_env(
        "quota.probe",
        quota_base_params(),
        json!({}),
        &[("PATH", path.as_str()), ("HOME", home.as_str())],
    );
    assert!(
        output.status.success(),
        "live quota provider process failed"
    );
    let response = json_stdout(&output);
    assert_quota_probe_response(&response);
    assert_live_probe_result(&response["result"]);
    assert_eq!(response["result"]["available"], true, "{response}");
    assert!(
        !response["result"]["windows"]
            .as_array()
            .expect("live quota windows")
            .is_empty(),
        "{response}"
    );
}
