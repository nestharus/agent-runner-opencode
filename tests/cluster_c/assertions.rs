// declared_role: validator, accessor, predicate, formatter, mapper, parser, orchestration
#![allow(unused_imports)]

use super::*;

pub fn assert_present_source_result(result: &Value) {
    assert_eq!(result["has_source"], true);
    assert_freshness_present(result, "quota.source must report freshness");
    let source_id = result["source_id"]
        .as_str()
        .expect("quota.source should identify the resolved auth source when present");
    assert!(
        source_id.contains(PAIRED_OPENCODE_AUTH_RELATIVE),
        "opencode3 quota source must resolve native opencode auth path; source_id={source_id}"
    );
    assert!(
        !source_id.contains(WRONG_CODEX_AUTH_RELATIVE),
        "opencode3 quota source must not use stale codex auth path; source_id={source_id}"
    );
}

pub fn assert_missing_source_result(result: &Value, message: &str) {
    assert_eq!(result["has_source"], false);
    assert_freshness_present(result, message);
}

pub fn assert_freshness_present(result: &Value, message: &str) {
    assert!(
        result["freshness"]
            .as_str()
            .is_some_and(|freshness| !freshness.is_empty()),
        "{message}"
    );
}

pub fn assert_f6_source_mapping(result: &Value, mapping: &F6AccountMapping) {
    assert_eq!(
        result["has_source"], true,
        "{} should find its native opencode auth source",
        mapping.settings_id
    );
    let source_id = result["source_id"].as_str().unwrap_or_else(|| {
        panic!(
            "{} quota.source should identify the resolved auth source: {result}",
            mapping.settings_id
        )
    });
    assert!(
        source_id.contains(mapping.opencode_auth_relative),
        "{} quota.source must resolve native opencode auth path {}; source_id={source_id}",
        mapping.settings_id,
        mapping.opencode_auth_relative
    );
    assert!(
        !source_id.contains(WRONG_CODEX_AUTH_RELATIVE),
        "{} quota.source must not use stale codex auth path {}; source_id={source_id}",
        mapping.settings_id,
        WRONG_CODEX_AUTH_RELATIVE
    );
}

pub fn assert_available_probe_result(result: &Value, raw_windows: &[RawUsageWindow]) {
    assert_eq!(result["available"], true, "probe result={result}");
    assert!(
        result["checked_at_unix_ms"].as_u64().is_some(),
        "probe must include checked_at_unix_ms"
    );
    let windows = result["windows"].as_array().expect("probe windows array");
    assert_eq!(windows.len(), raw_windows.len());
    for (projected, raw) in windows.iter().zip(raw_windows.iter()) {
        assert_projected_window(projected, raw);
    }
}

pub fn assert_projected_window(projected: &Value, raw: &RawUsageWindow) {
    assert_valid(projected, "quota.schema.json#/$defs/QuotaProbeWindow");
    let remaining_ratio = remaining_ratio(projected);
    let expected = projected_remaining_ratio(raw);
    assert_eq!(
        remaining_ratio, expected,
        "remaining_ratio must exactly project WHAM used_percent"
    );
    assert!(
        (0.0..=1.0).contains(&remaining_ratio),
        "remaining_ratio must stay in host-accepted 0.0..=1.0 range"
    );
    assert_eq!(
        projected["resets_at_unix_ms"].as_i64(),
        Some(raw_resets_at_epoch_ms(raw)),
        "resets_at_unix_ms must be derived from RFC3339 resets_at"
    );
}

pub fn remaining_ratio(projected: &Value) -> f64 {
    projected["remaining_ratio"]
        .as_f64()
        .expect("remaining_ratio number")
}

pub fn projected_remaining_ratio(raw: &RawUsageWindow) -> f64 {
    (100.0 - raw.used_percent) / 100.0
}

pub fn raw_resets_at_epoch_ms(raw: &RawUsageWindow) -> i64 {
    epoch_ms(&raw.resets_at)
}

pub fn assert_unavailable_probe_result(unavailable: &Value) {
    assert_eq!(unavailable["available"], false);
    assert_eq!(
        unavailable["windows"]
            .as_array()
            .expect("windows array")
            .len(),
        0,
        "quota-observer transport failure should not fabricate quota windows"
    );
    assert!(
        unavailable["detail"].as_str().is_some_and(|detail| detail
            .contains("forced quota probe failure")
            || detail.contains("17")),
        "unavailable result should truthfully carry script failure evidence: {unavailable}"
    );
}

pub fn assert_refresh_auth_result(result: &Value) {
    assert_eq!(result["refreshed"], true);
    assert_eq!(result["available"], false);
    assert!(
        result["checked_at_unix_ms"].as_u64().is_some(),
        "refresh_auth must include checked_at_unix_ms"
    );
    let detail = normalized_refresh_detail(refresh_detail(result));
    assert_refresh_detail_explains_native_refresh(&detail);
}

pub fn refresh_detail(result: &Value) -> &str {
    result["detail"].as_str().unwrap_or("")
}

pub fn normalized_refresh_detail(detail: &str) -> String {
    detail.to_ascii_lowercase()
}

pub fn assert_refresh_detail_explains_native_refresh(detail: &str) {
    assert!(
        detail.contains("opencode") && detail.contains("auth"),
        "refresh_auth detail should explain native opencode auth refresh; detail={detail:?}"
    );
}

pub fn assert_live_probe_result(result: &Value) {
    assert!(
        result["available"].as_bool().is_some(),
        "live probe should be either available or truthfully unavailable"
    );
    assert!(
        result["checked_at_unix_ms"].as_u64().is_some(),
        "live probe should include checked_at_unix_ms"
    );
    for window in result["windows"].as_array().expect("windows array") {
        assert_live_probe_window(window);
    }
}

pub fn assert_live_probe_window(window: &Value) {
    let remaining_ratio = window["remaining_ratio"]
        .as_f64()
        .expect("remaining_ratio number");
    assert!((0.0..=1.0).contains(&remaining_ratio));
    assert!(window["resets_at_unix_ms"].as_u64().is_some());
}

pub fn quota_base_params() -> Value {
    quota_params(QUOTA_SETTINGS_ID)
}

pub fn quota_refresh_auth_params() -> Value {
    json!({
        "settings_id": QUOTA_SETTINGS_ID,
        "force": true,
        "context": { "reason": "contract-test" }
    })
}

pub fn quota_params(settings_id: &str) -> Value {
    json!({
        "settings_id": settings_id,
        "model_name": "gpt-high",
        "context": { "source": "contract-cluster-c" }
    })
}

pub fn assert_quota_probe_response(response: &Value) {
    assert_valid(response, "quota.schema.json#/$defs/QuotaProbeResponse");
    assert_valid(
        &response["result"],
        "quota.schema.json#/$defs/QuotaProbeResult",
    );
}

pub fn success_result(
    output: std::process::Output,
    response_schema: &str,
    result_schema: &str,
) -> Value {
    assert_success_output(&output, response_schema);
    let response = validated_response(&output, response_schema, result_schema);
    response_result(&response)
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

pub fn assert_malformed_probe_rejected(output: std::process::Output, case_name: &str) -> Value {
    let response = malformed_probe_response(&output);
    if malformed_probe_is_error(&response) {
        assert_malformed_probe_error(&output, &response, case_name);
    } else {
        assert_malformed_probe_unavailable(&output, &response, case_name);
    }
    response
}

pub fn malformed_probe_response(output: &std::process::Output) -> Value {
    json_stdout(output)
}

pub fn malformed_probe_is_error(response: &Value) -> bool {
    response["ok"] == false
}

pub fn assert_malformed_probe_error(
    output: &std::process::Output,
    response: &Value,
    case_name: &str,
) {
    assert!(
        !output.status.success(),
        "{case_name} malformed probe error envelope should exit nonzero"
    );
    assert_valid(response, "common.schema.json#/$defs/ErrorResponseEnvelope");
}

pub fn assert_malformed_probe_unavailable(
    output: &std::process::Output,
    response: &Value,
    case_name: &str,
) {
    assert!(
        output.status.success(),
        "{case_name} malformed probe unavailable envelope should exit zero; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_quota_probe_response(response);
    assert_eq!(
        response["result"]["available"], false,
        "{case_name} malformed WHAM response must not be reported as available"
    );
    assert_empty_probe_windows(response, case_name);
    assert_probe_detail_present(response, case_name);
}

pub fn assert_empty_probe_windows(response: &Value, case_name: &str) {
    assert_eq!(
        response["result"]["windows"]
            .as_array()
            .expect("windows array")
            .len(),
        0,
        "{case_name} malformed WHAM response must not fabricate quota windows"
    );
}

pub fn assert_probe_detail_present(response: &Value, case_name: &str) {
    assert!(
        response["result"]["detail"]
            .as_str()
            .is_some_and(|detail| !detail.is_empty()),
        "{case_name} malformed unavailable result should include diagnostic detail: {response}"
    );
}

pub struct F6AccountMapping {
    pub settings_id: &'static str,
    pub opencode_auth_relative: &'static str,
}

pub const F6_ACCOUNT_MAPPINGS: &[F6AccountMapping] = &[
    F6AccountMapping {
        settings_id: "opencode1",
        opencode_auth_relative: ".local/share/opencode/auth.json",
    },
    F6AccountMapping {
        settings_id: "opencode2",
        opencode_auth_relative: ".opencode2/opencode/auth.json",
    },
    F6AccountMapping {
        settings_id: "opencode3",
        opencode_auth_relative: ".opencode3/opencode/auth.json",
    },
    F6AccountMapping {
        settings_id: "opencode4",
        opencode_auth_relative: ".opencode4/opencode/auth.json",
    },
    F6AccountMapping {
        settings_id: "opencode5",
        opencode_auth_relative: ".opencode5/opencode/auth.json",
    },
];

#[derive(Debug)]
pub struct RawUsageWindow {
    pub used_percent: f64,
    pub resets_at: String,
}

pub fn raw_usage_window(used_percent: f64, resets_at: &str) -> RawUsageWindow {
    RawUsageWindow {
        used_percent,
        resets_at: resets_at.to_owned(),
    }
}

pub fn epoch_ms(rfc3339: &str) -> i64 {
    parsed_rfc3339(rfc3339).timestamp_millis()
}

pub fn parsed_rfc3339(rfc3339: &str) -> DateTime<chrono::FixedOffset> {
    DateTime::parse_from_rfc3339(rfc3339)
        .unwrap_or_else(|err| panic!("invalid RFC3339 {rfc3339}: {err}"))
}

pub struct HomeFixture {
    pub path: PathBuf,
    pub path_string: String,
}

impl HomeFixture {
    pub fn new(prefix: &str) -> Self {
        let path = unique_temp_dir(prefix);
        fs::create_dir_all(&path).expect("create temp HOME");
        let path_string = path.to_string_lossy().into_owned();
        Self { path, path_string }
    }

    pub fn path_str(&self) -> &str {
        &self.path_string
    }

    pub fn write_paired_auth(&self, bytes: &[u8]) -> PathBuf {
        self.write_auth_at(PAIRED_OPENCODE_AUTH_RELATIVE, bytes)
    }

    pub fn write_auth_at(&self, relative_path: &str, bytes: &[u8]) -> PathBuf {
        let auth_path = self.auth_path(relative_path);
        write_auth_file(&auth_path, bytes);
        auth_path
    }

    pub fn auth_path(&self, relative_path: &str) -> PathBuf {
        self.path.join(relative_path)
    }
}

pub fn write_auth_file(auth_path: &Path, bytes: &[u8]) {
    let parent = auth_path.parent().expect("auth parent");
    fs::create_dir_all(parent).expect("create paired auth parent");
    fs::write(auth_path, bytes).expect("write paired auth");
}

impl HomeFixture {
    pub fn write_unreadable_paired_auth(&self, bytes: &[u8]) -> PathBuf {
        let auth_path = self.write_paired_auth(bytes);
        #[cfg(unix)]
        make_path_unreadable(&auth_path);
        auth_path
    }
}

#[cfg(unix)]
pub fn make_path_unreadable(path: &Path) {
    set_path_permissions(path, permissions_with_mode(path_permissions(path), 0o000));
}

#[cfg(unix)]
pub fn path_permissions(path: &Path) -> fs::Permissions {
    fs::metadata(path).expect("path metadata").permissions()
}

#[cfg(unix)]
pub fn permissions_with_mode(mut permissions: fs::Permissions, mode: u32) -> fs::Permissions {
    permissions.set_mode(mode);
    permissions
}

#[cfg(unix)]
pub fn set_path_permissions(path: &Path, permissions: fs::Permissions) {
    fs::set_permissions(path, permissions).expect("chmod path");
}

impl Drop for HomeFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub struct FakeNativeCurl {
    pub dir: PathBuf,
    pub invocation_path: PathBuf,
}

impl FakeNativeCurl {
    pub fn new() -> Self {
        Self::with_response(
            200,
            r#"{"rate_limit":{"secondary_window":{"used_percent":4,"reset_at":1781159045},"primary_window":{"used_percent":25,"reset_at":1780572245}}}"#,
        )
    }

    pub fn http_failure(status: u16, body: &str) -> Self {
        Self::with_response(status, body)
    }

    pub fn with_response(status: u16, body: &str) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-fake-native-curl");
        fs::create_dir_all(&dir).expect("create fake native curl dir");
        let invocation_path = dir.join("curl-invocation.log");
        let script = format!(
            "#!/bin/sh\n\
config=$(/bin/cat)\n\
case \"$config\" in\n\
  *'Authorization: Bearer '*'ChatGPT-Account-Id: acct'*) ;;\n\
  *) printf '%s\\n' 'missing expected auth stdin' >&2; exit 64 ;;\n\
esac\n\
printf '%s\\n' \"$*\" > {}\n\
printf '%s\\n' {}\n\
printf '%s' '__oulipoly_http_status__:{}'\n",
            shell_single_quote(&invocation_path.to_string_lossy()),
            shell_single_quote(body),
            status,
        );
        let curl_path = dir.join("curl");
        fs::write(&curl_path, script).expect("write fake curl");
        make_path_executable(&curl_path);
        Self {
            dir,
            invocation_path,
        }
    }

    pub fn transport_failure(exit_code: u8, stderr: &str) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-fake-native-curl-failure");
        fs::create_dir_all(&dir).expect("create failing fake curl dir");
        let invocation_path = dir.join("curl-invocation.log");
        let script = format!(
            "#!/bin/sh\n\
/bin/cat >/dev/null\n\
printf '%s\\n' \"$*\" > {}\n\
printf '%s\\n' {} >&2\n\
exit {exit_code}\n",
            shell_single_quote(&invocation_path.to_string_lossy()),
            shell_single_quote(stderr),
        );
        let curl_path = dir.join("curl");
        fs::write(&curl_path, script).expect("write failing fake curl");
        make_path_executable(&curl_path);
        Self {
            dir,
            invocation_path,
        }
    }

    pub fn path_env(&self) -> String {
        prepend_path(&self.dir)
    }

    pub fn assert_native_invocation(&self) {
        let argv = fs::read_to_string(&self.invocation_path).expect("fake curl invocation");
        assert!(
            argv.starts_with("-q "),
            "curl must disable ambient config: {argv:?}"
        );
        assert!(argv.contains("--max-time 20"), "curl argv={argv:?}");
        assert!(argv.contains("-K -"), "curl argv={argv:?}");
        assert!(
            argv.contains("https://chatgpt.com/backend-api/wham/usage"),
            "curl argv={argv:?}"
        );
        assert!(!argv.contains("sentinel"));
        assert!(!argv.contains("acct"));
    }
}

pub fn native_wham_expected_windows() -> Vec<RawUsageWindow> {
    vec![
        raw_usage_window(4.0, "2026-06-11T06:24:05Z"),
        raw_usage_window(25.0, "2026-06-04T11:24:05Z"),
    ]
}

impl Drop for FakeNativeCurl {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub struct FakeOpencodeAuth {
    pub dir: PathBuf,
}

impl FakeOpencodeAuth {
    pub fn success(wrapper: &str) -> Self {
        Self::with_script(wrapper, fake_opencode_auth_success_script())
    }

    pub fn rewrites_auth(wrapper: &str, auth_path: &Path) -> Self {
        Self::with_script(wrapper, fake_opencode_auth_rewrite_script(auth_path))
    }

    pub fn touches_marker_and_rewrites_auth(
        wrapper: &str,
        marker: &Path,
        auth_path: &Path,
    ) -> Self {
        Self::with_script(
            wrapper,
            fake_opencode_auth_touch_and_rewrite_script(marker, auth_path),
        )
    }

    pub fn with_script(wrapper: &str, script: String) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-fake-auth");
        fs::create_dir_all(&dir).expect("create fake opencode auth dir");
        let script_path = dir.join(wrapper);
        fs::write(&script_path, script).expect("write fake opencode auth");
        make_path_executable(&script_path);
        crate::support::write_fake_opencode_dispatcher(&dir);
        Self { dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(unix)]
pub fn make_path_executable(path: &Path) {
    set_path_permissions(path, permissions_with_mode(path_permissions(path), 0o755));
}

#[cfg(not(unix))]
pub fn make_path_executable(_path: &Path) {}

pub struct RefreshAuthFixture {
    pub home: HomeFixture,
    pub auth_path: PathBuf,
    pub before: String,
    pub quota_log_path: PathBuf,
    pub _fake_curl: FakeNativeCurl,
    pub _fake_auth: FakeOpencodeAuth,
    pub path: String,
}

impl RefreshAuthFixture {
    pub fn new() -> Self {
        let home = HomeFixture::new("agent-runner-opencode-quota-refresh-home");
        let auth_path =
            home.write_paired_auth(opencode_auth_json("refresh-sentinel", "acct").as_bytes());
        let before = file_sha256(&auth_path);
        let quota_log_path = home.path.join("quota-operation.log");
        let fake_curl = FakeNativeCurl::transport_failure(17, "probe unavailable during refresh");
        let fake_auth = FakeOpencodeAuth::rewrites_auth("opencode3", &auth_path);
        let path = prepend_paths(&[fake_auth.dir(), &fake_curl.dir]);
        Self {
            home,
            auth_path,
            before,
            quota_log_path,
            _fake_curl: fake_curl,
            _fake_auth: fake_auth,
            path,
        }
    }

    pub fn env(&self) -> [(&str, &str); 3] {
        [
            ("HOME", self.home.path_str()),
            ("PATH", self.path.as_str()),
            (
                "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG",
                self.quota_log_path.to_str().expect("quota log path UTF-8"),
            ),
        ]
    }

    pub fn assert_auth_changed(&self) {
        assert_ne!(
            file_sha256(&self.auth_path),
            self.before,
            "the fake OpenCode auth operation must expose an observed credential change"
        );
    }

    pub fn assert_auth_command_invoked(&self) {
        let log = optional_usage_log(&self.quota_log_path);
        assert!(
            log.contains("auth list"),
            "quota.refresh_auth must invoke opencode auth list; log={log:?}"
        );
    }
}

impl Drop for FakeOpencodeAuth {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub fn opencode_auth_json(access: &str, account_id: &str) -> String {
    format!(
        r#"{{"openai":{{"access":"{access}","accountId":"{account_id}","refresh":"refresh-sentinel","expires":4102444800,"type":"oauth"}}}}"#
    )
}

pub fn fake_opencode_auth_success_script() -> String {
    "#!/bin/sh\n\
if [ -n \"${AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}\" ]; then\n\
  printf 'auth argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
exit 0\n"
        .to_string()
}

pub fn fake_opencode_auth_rewrite_script(auth_path: &Path) -> String {
    format!(
        "#!/bin/sh\n\
if [ -n \"${{AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}}\" ]; then\n\
  printf 'auth argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
printf '%s' {} > {}\n\
exit 0\n",
        shell_single_quote(&opencode_auth_json("sentinel-refreshed", "acct")),
        shell_single_quote(&auth_path.to_string_lossy()),
    )
}

pub fn fake_opencode_auth_rewrite_then_fail_script(auth_path: &Path) -> String {
    format!(
        "#!/bin/sh\n\
if [ -n \"${{AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}}\" ]; then\n\
  printf 'auth argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
printf '%s' {} > {}\n\
exit 17\n",
        shell_single_quote(&opencode_auth_json("sentinel-refreshed", "acct")),
        shell_single_quote(&auth_path.to_string_lossy()),
    )
}

pub fn fake_opencode_auth_rewrite_then_unicode_fail_script(auth_path: &Path) -> String {
    format!(
        "#!/bin/sh\n\
if [ -n \"${{AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}}\" ]; then\n\
  printf 'auth argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
printf '%s' {} > {}\n\
printf '%s' {} >&2\n\
exit 17\n",
        shell_single_quote(&opencode_auth_json("sentinel-refreshed", "acct")),
        shell_single_quote(&auth_path.to_string_lossy()),
        shell_single_quote(&"é".repeat(600)),
    )
}

pub fn fake_opencode_auth_rewrite_then_oversize_script(auth_path: &Path) -> String {
    format!(
        "#!/bin/sh\n\
if [ -n \"${{AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}}\" ]; then\n\
  printf 'auth argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
printf '%s' {} > {}\n\
/usr/bin/dd if=/dev/zero bs=1024 count=65 2>/dev/null\n\
exit 0\n",
        shell_single_quote(&opencode_auth_json("sentinel-refreshed", "acct")),
        shell_single_quote(&auth_path.to_string_lossy()),
    )
}

pub fn fake_opencode_auth_touch_and_rewrite_script(marker: &Path, auth_path: &Path) -> String {
    format!(
        "#!/bin/sh\n\
if [ -n \"${{AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}}\" ]; then\n\
  printf 'auth argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
: > {}\n\
printf '%s' {} > {}\n\
exit 0\n",
        shell_single_quote(&marker.to_string_lossy()),
        shell_single_quote(&opencode_auth_json("sentinel-refreshed", "acct")),
        shell_single_quote(&auth_path.to_string_lossy()),
    )
}

pub fn fake_opencode_auth_timeout_once_script(marker: &Path, auth_path: &Path) -> String {
    format!(
        "#!/bin/sh\n\
if [ ! -e {} ]; then\n\
  : > {}\n\
  exec /bin/sleep 30\n\
fi\n\
printf '%s' {} > {}\n\
exit 0\n",
        shell_single_quote(&marker.to_string_lossy()),
        shell_single_quote(&marker.to_string_lossy()),
        shell_single_quote(&opencode_auth_json("sentinel-refreshed", "acct")),
        shell_single_quote(&auth_path.to_string_lossy()),
    )
}

pub fn fake_opencode_auth_rewrite_with_live_descendant_script(
    release_marker: &Path,
    auth_path: &Path,
) -> String {
    format!(
        "#!/bin/sh\n\
if [ -n \"${{AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG:-}}\" ]; then\n\
  printf 'auth argv=%s\\n' \"$*\" >> \"$AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG\"\n\
fi\n\
(while [ ! -e {} ]; do /bin/sleep 0.05; done) </dev/null >/dev/null 2>&1 &\n\
printf '%s' {} > {}\n\
exit 0\n",
        shell_single_quote(&release_marker.to_string_lossy()),
        shell_single_quote(&opencode_auth_json("sentinel-refreshed", "acct")),
        shell_single_quote(&auth_path.to_string_lossy()),
    )
}

pub fn optional_usage_log(log_path: &Path) -> String {
    fs::read_to_string(log_path).unwrap_or_default()
}

pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

pub fn prepend_path(dir: &Path) -> String {
    prepend_paths(&[dir])
}

pub fn prepend_paths(dirs: &[&Path]) -> String {
    std::env::join_paths(dirs.iter().copied())
        .expect("join PATH entries")
        .to_string_lossy()
        .into_owned()
}

pub fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub fn file_sha256(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
