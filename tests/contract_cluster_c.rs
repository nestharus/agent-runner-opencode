#[allow(dead_code)]
mod support;

use chrono::DateTime;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use support::{assert_valid, invoke_with_env, invoke_with_host_and_env, json_stdout};

const CHATGPT_USAGE_WINDOWS_RAW: &str = include_str!("fixtures/chatgpt_usage_windows.json");
const QUOTA_SETTINGS_ID: &str = "opencode3";
const PAIRED_CODEX_AUTH_RELATIVE: &str = ".codex2/auth.json";
const WRONG_WRAPPER_NUMBER_AUTH_RELATIVE: &str = ".codex3/auth.json";

#[test]
fn characterization_codex_chatgpt_usage_windows() {
    let windows = parse_chatgpt_usage_windows(CHATGPT_USAGE_WINDOWS_RAW)
        .expect("captured chatgpt-usage fixture should parse");
    assert_eq!(
        windows.len(),
        2,
        "fixture should contain exactly two windows"
    );
    assert_eq!(windows[0].used_percent, 4.0);
    assert_eq!(windows[0].resets_at, "2026-06-11T06:24:05Z");
    assert_eq!(windows[1].used_percent, 25.0);
    assert_eq!(windows[1].resets_at, "2026-06-04T11:24:05Z");

    for window in &windows {
        assert!(
            (0.0..=100.0).contains(&window.used_percent),
            "used_percent must be in 0..=100: {}",
            window.used_percent
        );
        assert!(
            epoch_ms(&window.resets_at) > 0,
            "resets_at must be RFC3339 and convert to a Unix millisecond timestamp"
        );
    }

    assert!(
        parse_chatgpt_usage_windows("{").is_err(),
        "invalid JSON should be rejected before contract projection"
    );
    assert!(
        parse_chatgpt_usage_windows(
            r#"{"windows":[{"used_percent":150,"resets_at":"2026-06-04T11:24:05Z"}]}"#
        )
        .is_err(),
        "out-of-range used_percent should be rejected before contract projection"
    );
    assert!(
        parse_chatgpt_usage_windows(
            r#"{"windows":[{"used_percent":25,"resets_at":"not-rfc3339"}]}"#
        )
        .is_err(),
        "bad RFC3339 resets_at should be rejected before contract projection"
    );
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

    assert_eq!(result["has_source"], true);
    assert!(
        result["freshness"]
            .as_str()
            .is_some_and(|freshness| !freshness.is_empty()),
        "quota.source must report freshness"
    );
    let source_id = result["source_id"]
        .as_str()
        .expect("quota.source should identify the resolved auth source when present");
    assert!(
        source_id.contains(PAIRED_CODEX_AUTH_RELATIVE),
        "opencode3 quota source must resolve paired codex auth path; source_id={source_id}"
    );
    assert!(
        !source_id.contains(WRONG_WRAPPER_NUMBER_AUTH_RELATIVE),
        "opencode3 quota source must not use wrapper-number auth path; source_id={source_id}"
    );
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
    assert_eq!(missing["has_source"], false);
    assert!(
        missing["freshness"]
            .as_str()
            .is_some_and(|freshness| !freshness.is_empty()),
        "missing-auth source response must still report freshness"
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
    assert_eq!(unreadable["has_source"], false);
    assert!(
        unreadable["freshness"]
            .as_str()
            .is_some_and(|freshness| !freshness.is_empty()),
        "unreadable-auth source response must still report freshness"
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

        assert_eq!(
            result["has_source"], true,
            "{} should find its paired codex auth source",
            mapping.settings_id
        );
        let source_id = result["source_id"].as_str().unwrap_or_else(|| {
            panic!(
                "{} quota.source should identify the resolved auth source: {result}",
                mapping.settings_id
            )
        });
        assert!(
            source_id.contains(mapping.codex_auth_relative),
            "{} quota.source must resolve paired codex auth path {}; source_id={source_id}",
            mapping.settings_id,
            mapping.codex_auth_relative
        );
        if let Some(wrapper_derived_relative) = mapping.wrapper_derived_relative {
            assert!(
                !source_id.contains(wrapper_derived_relative),
                "{} quota.source must not derive auth from wrapper number {}; source_id={source_id}",
                mapping.settings_id,
                wrapper_derived_relative
            );
        }
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
    assert_eq!(result["available"], true);
    assert!(
        result["checked_at_unix_ms"].as_u64().is_some(),
        "probe must include checked_at_unix_ms"
    );
    let windows = result["windows"].as_array().expect("probe windows array");
    assert_eq!(windows.len(), raw_windows.len());
    for (projected, raw) in windows.iter().zip(raw_windows.iter()) {
        assert_valid(projected, "quota.schema.json#/$defs/QuotaProbeWindow");
        let remaining_ratio = projected["remaining_ratio"]
            .as_f64()
            .expect("remaining_ratio number");
        let expected = (100.0 - raw.used_percent) / 100.0;
        assert_eq!(
            remaining_ratio, expected,
            "remaining_ratio must exactly project chatgpt-usage used_percent"
        );
        assert!(
            (0.0..=1.0).contains(&remaining_ratio),
            "remaining_ratio must stay in host-accepted 0.0..=1.0 range"
        );
        assert_eq!(
            projected["resets_at_unix_ms"].as_i64(),
            Some(epoch_ms(&raw.resets_at)),
            "resets_at_unix_ms must be derived from RFC3339 resets_at"
        );
    }
    let invocation = fs::read_to_string(fake_usage.log_path()).expect("read fake usage log");
    assert!(
        invocation.contains(auth_path.to_string_lossy().as_ref()),
        "quota.probe must invoke chatgpt-usage with paired opencode3 auth path; log={invocation:?}"
    );
    assert!(
        !invocation.contains(WRONG_WRAPPER_NUMBER_AUTH_RELATIVE),
        "quota.probe must not invoke wrapper-number auth path; log={invocation:?}"
    );

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
    assert_eq!(unavailable["available"], false);
    assert_eq!(
        unavailable["windows"]
            .as_array()
            .expect("windows array")
            .len(),
        0,
        "nonzero chatgpt-usage exit should not fabricate quota windows"
    );
    assert!(
        unavailable["detail"].as_str().is_some_and(|detail| detail
            .contains("forced quota probe failure")
            || detail.contains("17")),
        "unavailable result should truthfully carry script failure evidence: {unavailable}"
    );

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
    let home = HomeFixture::new("agent-runner-opencode-quota-refresh-home");
    let auth_path = home.write_paired_auth(
        b"{\"tokens\":{\"access_token\":\"refresh-sentinel\",\"account_id\":\"acct\"}}\n",
    );
    let before = file_sha256(&auth_path);
    let fake_usage = FakeChatgptUsage::failure(17, "probe unavailable during refresh");
    let path = prepend_path(fake_usage.dir());

    let result = success_result(
        invoke_with_env(
            "quota.refresh_auth",
            json!({
                "settings_id": QUOTA_SETTINGS_ID,
                "force": true,
                "context": { "reason": "contract-test" }
            }),
            &[
                ("HOME", home.path_str()),
                ("PATH", path.as_str()),
                (
                    "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                    fake_usage.log_path_str(),
                ),
            ],
        ),
        "quota.schema.json#/$defs/QuotaRefreshAuthResponse",
        "quota.schema.json#/$defs/QuotaRefreshAuthResult",
    );
    assert_eq!(result["refreshed"], false);
    assert_eq!(result["available"], false);
    assert!(
        result["checked_at_unix_ms"].as_u64().is_some(),
        "refresh_auth must include checked_at_unix_ms"
    );
    let detail = result["detail"].as_str().unwrap_or("").to_ascii_lowercase();
    assert!(
        (detail.contains("cli") || detail.contains("codex"))
            && (detail.contains("owned") || detail.contains("unavailable")),
        "refresh_auth detail should explain that auth is CLI-owned/unavailable; detail={detail:?}"
    );
    assert_eq!(
        file_sha256(&auth_path),
        before,
        "quota.refresh_auth must never mutate codex auth tokens"
    );
}

#[test]
#[ignore]
fn integration_quota_probe_live() {
    let output = invoke_with_host_and_env("quota.probe", quota_base_params(), json!({}), &[]);
    let response = json_stdout(&output);
    assert_valid(&response, "quota.schema.json#/$defs/QuotaProbeResponse");
    assert_valid(
        &response["result"],
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
    assert!(
        response["result"]["available"].as_bool().is_some(),
        "live probe should be either available or truthfully unavailable"
    );
    assert!(
        response["result"]["checked_at_unix_ms"].as_u64().is_some(),
        "live probe should include checked_at_unix_ms"
    );
    for window in response["result"]["windows"]
        .as_array()
        .expect("windows array")
    {
        let remaining_ratio = window["remaining_ratio"]
            .as_f64()
            .expect("remaining_ratio number");
        assert!((0.0..=1.0).contains(&remaining_ratio));
        assert!(window["resets_at_unix_ms"].as_u64().is_some());
    }
}

fn quota_base_params() -> Value {
    quota_params(QUOTA_SETTINGS_ID)
}

fn quota_params(settings_id: &str) -> Value {
    json!({
        "settings_id": settings_id,
        "model_name": "gpt-high",
        "context": { "source": "contract-cluster-c" }
    })
}

fn success_result(
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
    response["result"].clone()
}

fn assert_malformed_probe_rejected(output: std::process::Output, case_name: &str) -> Value {
    let response = json_stdout(&output);
    if response["ok"] == false {
        assert!(
            !output.status.success(),
            "{case_name} malformed probe error envelope should exit nonzero"
        );
        assert_valid(&response, "common.schema.json#/$defs/ErrorResponseEnvelope");
    } else {
        assert!(
            output.status.success(),
            "{case_name} malformed probe unavailable envelope should exit zero; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_valid(&response, "quota.schema.json#/$defs/QuotaProbeResponse");
        assert_valid(
            &response["result"],
            "quota.schema.json#/$defs/QuotaProbeResult",
        );
        assert_eq!(
            response["result"]["available"], false,
            "{case_name} malformed chatgpt-usage stdout must not be reported as available"
        );
        assert_eq!(
            response["result"]["windows"]
                .as_array()
                .expect("windows array")
                .len(),
            0,
            "{case_name} malformed chatgpt-usage stdout must not fabricate quota windows"
        );
        assert!(
            response["result"]["detail"]
                .as_str()
                .is_some_and(|detail| !detail.is_empty()),
            "{case_name} malformed unavailable result should include diagnostic detail: {response}"
        );
    }
    response
}

struct F6AccountMapping {
    settings_id: &'static str,
    codex_auth_relative: &'static str,
    wrapper_derived_relative: Option<&'static str>,
}

const F6_ACCOUNT_MAPPINGS: &[F6AccountMapping] = &[
    F6AccountMapping {
        settings_id: "opencode1",
        codex_auth_relative: ".codex/auth.json",
        wrapper_derived_relative: None,
    },
    F6AccountMapping {
        settings_id: "opencode2",
        codex_auth_relative: ".codex5/auth.json",
        wrapper_derived_relative: Some(".codex2/auth.json"),
    },
    F6AccountMapping {
        settings_id: "opencode3",
        codex_auth_relative: ".codex2/auth.json",
        wrapper_derived_relative: Some(".codex3/auth.json"),
    },
    F6AccountMapping {
        settings_id: "opencode4",
        codex_auth_relative: ".codex3/auth.json",
        wrapper_derived_relative: Some(".codex4/auth.json"),
    },
    F6AccountMapping {
        settings_id: "opencode5",
        codex_auth_relative: ".codex4/auth.json",
        wrapper_derived_relative: Some(".codex5/auth.json"),
    },
];

#[derive(Debug)]
struct RawUsageWindow {
    used_percent: f64,
    resets_at: String,
}

fn parse_chatgpt_usage_windows(raw: &str) -> Result<Vec<RawUsageWindow>, String> {
    let parsed: Value = serde_json::from_str(raw).map_err(|err| err.to_string())?;
    let windows = parsed
        .get("windows")
        .and_then(Value::as_array)
        .ok_or_else(|| "windows must be an array".to_string())?;
    let mut out = Vec::with_capacity(windows.len());
    for (index, window) in windows.iter().enumerate() {
        let used_percent = window
            .get("used_percent")
            .and_then(Value::as_f64)
            .ok_or_else(|| format!("windows[{index}].used_percent must be numeric"))?;
        if !(0.0..=100.0).contains(&used_percent) {
            return Err(format!(
                "windows[{index}].used_percent out of range: {used_percent}"
            ));
        }
        let resets_at = window
            .get("resets_at")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("windows[{index}].resets_at must be a string"))?;
        DateTime::parse_from_rfc3339(resets_at)
            .map_err(|err| format!("windows[{index}].resets_at invalid RFC3339: {err}"))?;
        out.push(RawUsageWindow {
            used_percent,
            resets_at: resets_at.to_owned(),
        });
    }
    Ok(out)
}

fn epoch_ms(rfc3339: &str) -> i64 {
    DateTime::parse_from_rfc3339(rfc3339)
        .unwrap_or_else(|err| panic!("invalid RFC3339 {rfc3339}: {err}"))
        .timestamp_millis()
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

    fn write_paired_auth(&self, bytes: &[u8]) -> PathBuf {
        self.write_auth_at(PAIRED_CODEX_AUTH_RELATIVE, bytes)
    }

    fn write_auth_at(&self, relative_path: &str, bytes: &[u8]) -> PathBuf {
        let auth_path = self.path.join(relative_path);
        let parent = auth_path.parent().expect("auth parent");
        fs::create_dir_all(parent).expect("create paired auth parent");
        fs::write(&auth_path, bytes).expect("write paired auth");
        auth_path
    }

    fn write_unreadable_paired_auth(&self, bytes: &[u8]) -> PathBuf {
        let auth_path = self.write_paired_auth(bytes);
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&auth_path)
                .expect("unreadable auth metadata")
                .permissions();
            permissions.set_mode(0o000);
            fs::set_permissions(&auth_path, permissions).expect("chmod unreadable auth");
        }
        auth_path
    }
}

impl Drop for HomeFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct FakeChatgptUsage {
    dir: PathBuf,
    log_path: PathBuf,
    log_path_string: String,
}

impl FakeChatgptUsage {
    fn success(stdout: &str) -> Self {
        Self::with_script(fake_chatgpt_usage_success_script(stdout))
    }

    fn failure(exit_code: u8, stderr: &str) -> Self {
        Self::with_script(fake_chatgpt_usage_failure_script(exit_code, stderr))
    }

    fn with_script(script: String) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-fake-chatgpt-usage");
        fs::create_dir_all(&dir).expect("create fake chatgpt-usage dir");
        let script_path = dir.join("chatgpt-usage");
        fs::write(&script_path, script).expect("write fake chatgpt-usage");
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&script_path)
                .expect("fake chatgpt-usage metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script_path, permissions).expect("chmod fake chatgpt-usage");
        }
        let log_path = dir.join("chatgpt-usage.log");
        let log_path_string = log_path.to_string_lossy().into_owned();
        Self {
            dir,
            log_path,
            log_path_string,
        }
    }

    fn dir(&self) -> &Path {
        &self.dir
    }

    fn log_path(&self) -> &Path {
        &self.log_path
    }

    fn log_path_str(&self) -> &str {
        &self.log_path_string
    }
}

impl Drop for FakeChatgptUsage {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn fake_chatgpt_usage_success_script(stdout: &str) -> String {
    format!(
        "#!/bin/sh\n\
if [ -n \"${{AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}}\" ]; then\n\
  printf 'argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
printf '%s' {}\n\
exit 0\n",
        shell_single_quote(stdout)
    )
}

fn fake_chatgpt_usage_failure_script(exit_code: u8, stderr: &str) -> String {
    format!(
        "#!/bin/sh\n\
if [ -n \"${{AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}}\" ]; then\n\
  printf 'argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
printf '%s\\n' {} >&2\n\
exit {exit_code}\n",
        shell_single_quote(stderr)
    )
}

fn assert_no_chatgpt_usage_invocation(log_path: &Path) {
    let log = fs::read_to_string(log_path).unwrap_or_default();
    assert!(
        log.trim().is_empty(),
        "quota.source must not invoke chatgpt-usage; log={log:?}"
    );
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

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn file_sha256(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
