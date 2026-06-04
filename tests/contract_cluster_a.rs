mod support;

use serde_json::{json, Value};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use support::{
    assert_stderr_diagnostics_only, assert_valid, invoke, invoke_with_env,
    invoke_with_host_and_env, json_stdout, CONTRACT,
};

const OBSERVED_AT_UNIX_MS: u64 = 1_780_565_973_556;
const FAKE_LAUNCH_STDOUT: &[u8] = include_bytes!("fixtures/opencode_launch_events.jsonl");
const FAKE_LAUNCH_STDERR: &[u8] = b"fake wrapper stderr bytes\n";
const SLOW_WRAPPER_SLEEP_SECONDS: u64 = 2;

#[test]
fn characterization_opencode_launch_json_events() {
    let fixture = include_str!("fixtures/opencode_launch_events.jsonl");
    let mut saw_step_start = false;
    let mut saw_text = false;
    let mut saw_step_finish = false;

    for (line_number, line) in fixture
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        let event: Value = serde_json::from_str(line).unwrap_or_else(|err| {
            panic!("fixture line {} is invalid JSON: {err}", line_number + 1)
        });
        let event_type = event["type"]
            .as_str()
            .unwrap_or_else(|| panic!("fixture line {} missing top-level type", line_number + 1));
        assert!(
            matches!(event_type, "step_start" | "text" | "step_finish"),
            "unexpected opencode event type {event_type} on fixture line {}",
            line_number + 1
        );
        assert!(
            event["timestamp"].as_u64().is_some(),
            "fixture line {} missing millisecond timestamp",
            line_number + 1
        );
        let session_id = event["sessionID"]
            .as_str()
            .unwrap_or_else(|| panic!("fixture line {} missing sessionID", line_number + 1));
        assert!(
            session_id.starts_with("ses_"),
            "unexpected sessionID {session_id}"
        );
        let part = event["part"]
            .as_object()
            .unwrap_or_else(|| panic!("fixture line {} missing nested part", line_number + 1));
        let part_type = part
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("fixture line {} missing part.type", line_number + 1));
        assert!(
            matches!(part_type, "step-start" | "text" | "step-finish"),
            "unexpected part.type {part_type} on fixture line {}",
            line_number + 1
        );
        assert_eq!(
            part.get("sessionID").and_then(Value::as_str),
            Some(session_id),
            "nested part sessionID should match top-level sessionID"
        );

        assert!(
            event.get("contract").is_none(),
            "native opencode event is not a contract event"
        );
        assert!(
            event.get("request_id").is_none(),
            "native opencode event is not a contract event"
        );
        assert!(
            event.get("seq").is_none(),
            "native opencode event is not a contract event"
        );
        assert!(
            event.get("kind").is_none(),
            "native opencode event is not a contract event"
        );

        match event_type {
            "step_start" => saw_step_start = true,
            "text" => {
                saw_text = true;
                assert_eq!(part.get("text").and_then(Value::as_str), Some("ok"));
                assert!(
                    part.get("time").is_some(),
                    "text part should carry timing metadata"
                );
            }
            "step_finish" => {
                saw_step_finish = true;
                let tokens = part
                    .get("tokens")
                    .and_then(Value::as_object)
                    .expect("step_finish part should carry token metadata");
                for token_field in ["total", "input", "output", "reasoning"] {
                    assert!(
                        tokens.get(token_field).and_then(Value::as_u64).is_some(),
                        "tokens.{token_field} should be present"
                    );
                }
                assert!(
                    part.get("cost").and_then(Value::as_f64).is_some(),
                    "step_finish part should carry numeric cost metadata"
                );
            }
            _ => unreachable!(),
        }
    }

    assert!(saw_step_start, "fixture should include step_start");
    assert!(saw_text, "fixture should include text");
    assert!(saw_step_finish, "fixture should include step_finish");
}

#[test]
fn contract_launch_stream() {
    let fake_wrapper = FakeOpencodeWrapper::new();
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_string();
    let fixture_session_id = fixture_session_id();

    let output = invoke_with_env(
        "launch",
        launch_params("low"),
        &[
            ("PATH", path.as_str()),
            ("AGENT_RUNNER_OPENCODE_WRAPPER_LOG", log_path.as_str()),
        ],
    );
    assert_stderr_diagnostics_only(&output);

    let events = parse_launch_events(&output.stdout);
    assert!(
        !events.is_empty(),
        "launch stdout must contain NDJSON events"
    );
    assert_monotonic_launch_events(&events);

    let stdout_bytes = collect_stream_bytes(&events, "stdout");
    assert_eq!(
        stdout_bytes, FAKE_LAUNCH_STDOUT,
        "stdout events must byte-preserve the selected opencodeN wrapper output"
    );
    let stderr_bytes = collect_stream_bytes(&events, "stderr");
    assert_eq!(
        stderr_bytes, FAKE_LAUNCH_STDERR,
        "stderr events must byte-preserve the selected opencodeN wrapper output"
    );

    let session_marker = events
        .iter()
        .find(|event| {
            event["kind"] == "marker"
                && event["name"]
                    .as_str()
                    .is_some_and(|name| name.contains(fixture_session_id))
        })
        .unwrap_or_else(|| {
            panic!(
                "launch stream must emit a marker naming captured opencode sessionID {fixture_session_id}; events={events:?}"
            )
        });
    assert_eq!(
        session_marker["value"], true,
        "session marker should use a truthy marker value"
    );

    let final_event = events.last().expect("final event");
    assert_eq!(
        final_event["kind"], "exit",
        "final launch line must be exit"
    );
    assert!(
        final_event.get("status").is_some(),
        "exit event must carry status"
    );
    assert!(
        final_event.get("terminal_signal").is_some(),
        "exit event must carry terminal_signal"
    );
    assert_process_status_kind(&final_event["status"]);
    assert_eq!(
        final_event["status"],
        json!({ "kind": "exited", "code": 7 }),
        "final status should truthfully report the controlled wrapper exit status"
    );
    assert_eq!(
        final_event["terminal_signal"]["kind"],
        expected_signal_kind_for_status(&final_event["status"]),
        "terminal_signal should be status-derived"
    );
    assert!(
        final_event.get("session").is_some(),
        "exit event must carry captured session evidence"
    );
    assert!(
        json_contains_string(&final_event["session"], fixture_session_id),
        "exit.session must carry the same opencode sessionID evidence as the marker; session={}",
        final_event["session"]
    );
    assert_eq!(
        output.status.code(),
        Some(7),
        "provider process should preserve nonzero child exit-code parity"
    );

    let wrapper_log = fs::read_to_string(fake_wrapper.log_path())
        .expect("selected opencodeN wrapper should record its invocation");
    assert!(
        wrapper_log
            .lines()
            .any(|line| line == "argv0=opencode1" || line.ends_with("/opencode1")),
        "launch should cross the selected opencode1 wrapper boundary; log={wrapper_log:?}"
    );
    assert!(
        wrapper_log.lines().any(|line| line == "arg=run"),
        "wrapper should receive opencode run argv; log={wrapper_log:?}"
    );
}

#[test]
fn contract_launch_stream_heartbeat_policy() {
    let fake_wrapper = FakeOpencodeWrapper::with_script(slow_opencode_script(0));
    let path = prepend_path(fake_wrapper.dir());
    let log_path = fake_wrapper.log_path_string();

    let output = invoke_with_env(
        "launch",
        launch_params("low"),
        &[
            ("PATH", path.as_str()),
            ("AGENT_RUNNER_OPENCODE_WRAPPER_LOG", log_path.as_str()),
        ],
    );
    assert_stderr_diagnostics_only(&output);

    let events = parse_launch_events(&output.stdout);
    assert!(
        !events.is_empty(),
        "launch stdout must contain NDJSON events"
    );
    assert_monotonic_launch_events(&events);
    assert!(
        events.iter().any(|event| event["kind"] == "heartbeat"),
        "slow launch should deterministically emit at least one heartbeat before exit; events={events:?}"
    );
    let final_event = events.last().expect("final event");
    assert_eq!(
        final_event["kind"], "exit",
        "final launch line must be exit"
    );
    assert_process_status_kind(&final_event["status"]);
    assert_eq!(
        final_event["terminal_signal"]["kind"],
        expected_signal_kind_for_status(&final_event["status"]),
        "terminal_signal should be status-derived"
    );

    let deadline_wrapper = FakeOpencodeWrapper::with_script(slow_opencode_script(0));
    let deadline_path = prepend_path(deadline_wrapper.dir());
    let deadline_log_path = deadline_wrapper.log_path_string();
    let deadline_unix_ms = unix_ms_now() + 250;
    let deadline_output = invoke_with_host_and_env(
        "launch",
        launch_params("low"),
        json!({ "deadline_unix_ms": deadline_unix_ms }),
        &[
            ("PATH", deadline_path.as_str()),
            (
                "AGENT_RUNNER_OPENCODE_WRAPPER_LOG",
                deadline_log_path.as_str(),
            ),
        ],
    );
    assert_stderr_diagnostics_only(&deadline_output);

    let deadline_events = parse_launch_events(&deadline_output.stdout);
    assert!(
        !deadline_events.is_empty(),
        "deadline launch stdout must contain NDJSON events"
    );
    assert_monotonic_launch_events(&deadline_events);
    let deadline_final_event = deadline_events.last().expect("final event");
    assert_eq!(
        deadline_final_event["kind"], "exit",
        "final deadline launch line must be exit"
    );
    assert_eq!(
        deadline_final_event["status"]["kind"], "prolonged_silence",
        "deadline-enforced silence should be represented as prolonged_silence"
    );
    assert_eq!(
        deadline_final_event["terminal_signal"]["kind"], "prolonged_silence",
        "prolonged_silence status should derive a prolonged_silence terminal signal"
    );
    assert_eq!(
        deadline_output.status.code(),
        Some(124),
        "provider exit code should preserve prolonged_silence host parity"
    );
}

#[test]
#[ignore = "live opencode auth/network smoke; run explicitly when external dependencies are available"]
fn integration_launch_live_smoke() {
    let output = invoke("launch", launch_params("low"));
    assert_stderr_diagnostics_only(&output);

    let events = parse_launch_events(&output.stdout);
    assert!(
        !events.is_empty(),
        "launch stdout must contain NDJSON events"
    );
    assert_monotonic_launch_events(&events);
    let final_event = events.last().expect("final event");
    assert_eq!(
        final_event["kind"], "exit",
        "final launch line must be exit"
    );
    assert_eq!(
        final_event["terminal_signal"]["kind"],
        expected_signal_kind_for_status(&final_event["status"]),
        "terminal_signal should truthfully reflect the final process status"
    );
    assert_eq!(
        output.status.code(),
        Some(expected_exit_code_for_status(&final_event["status"])),
        "provider process exit should preserve host parity for the final launch status; stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn contract_policy_evaluate() {
    let output = invoke_with_env(
        "policy.evaluate",
        json!({
            "settings_id": "opencode1",
            "mode": "agent",
            "model": model_request("low"),
            "launch": {
                "argv": ["reply with the single word: ok"],
                "working_directory": env!("CARGO_MANIFEST_DIR")
            }
        }),
        &[("OPENAI_API_KEY", "SENTINEL_DO_NOT_LEAK")],
    );
    assert!(
        output.status.success(),
        "policy.evaluate exited {:?}; stderr: {}\nstdout: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let response = json_stdout(&output);
    let response_json = response.to_string();
    assert!(
        !response_json.contains("SENTINEL_DO_NOT_LEAK"),
        "policy response JSON must not leak process OPENAI_API_KEY value: {response_json}"
    );
    assert_valid(
        &response,
        "policy.schema.json#/$defs/PolicyEvaluateResponse",
    );
    assert_valid(
        &response["result"],
        "policy.schema.json#/$defs/PolicyEvaluateResult",
    );

    let result = &response["result"];
    assert_eq!(result["accepted"], true);
    let argv = string_array(&result["argv"], "result.argv");
    assert_eq!(argv.first().map(String::as_str), Some("opencode1"));
    assert_contains_subsequence(
        &argv,
        &[
            "run",
            "--format",
            "json",
            "--dangerously-skip-permissions",
            "-m",
            "openai/gpt-5.5",
            "--variant",
            "low",
        ],
    );
    assert!(
        !argv.iter().any(|arg| arg == "opencode"),
        "policy should preserve wrapper semantics instead of bypassing opencodeN"
    );
    let pure_preserved = argv.iter().any(|arg| arg == "--pure")
        || argv.first().is_some_and(|arg| {
            matches!(
                arg.as_str(),
                "opencode1" | "opencode2" | "opencode3" | "opencode4" | "opencode5"
            )
        });
    assert!(
        pure_preserved,
        "policy must preserve --pure semantics either by retaining --pure or by invoking an opencodeN wrapper; argv={argv:?}"
    );

    let env = result["env"]
        .as_object()
        .expect("result.env should be present");
    assert!(
        !env.contains_key("OPENAI_API_KEY"),
        "policy result env must not leak OPENAI_API_KEY"
    );
    assert!(
        !env.values().any(|value| value == "SENTINEL_DO_NOT_LEAK"),
        "policy result env must not leak process OPENAI_API_KEY value"
    );
}

#[test]
fn contract_terminal_classify_status_only() {
    for (status, expected) in [
        (json!({ "kind": "exited", "code": 0 }), "clean_exit"),
        (json!({ "kind": "exited", "code": 17 }), "nonzero_exit"),
        (
            json!({ "kind": "signal_terminated", "signal": 15 }),
            "signal_exit",
        ),
        (
            json!({ "kind": "spawn_error", "reason": "ENOENT" }),
            "spawn_error",
        ),
        (
            json!({ "kind": "prolonged_silence", "reason": "no output before deadline" }),
            "prolonged_silence",
        ),
        (json!({ "kind": "cancelled" }), "cancelled"),
        (json!({ "kind": "unknown" }), "unknown"),
    ] {
        assert_terminal_classification(status, "", "", expected);
    }

    let quota_stdout = "usage limit reached; quota exhausted";
    let quota_stderr = "rate limit: try again later";
    assert_terminal_classification(
        json!({ "kind": "exited", "code": 0 }),
        quota_stdout,
        quota_stderr,
        "clean_exit",
    );
    assert_terminal_classification(
        json!({ "kind": "exited", "code": 2 }),
        quota_stdout,
        quota_stderr,
        "nonzero_exit",
    );
}

fn launch_params(effort: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "mode": "agent",
        "model": model_request(effort),
        "argv": ["reply with the single word: ok"],
        "working_directory": env!("CARGO_MANIFEST_DIR")
    })
}

fn model_request(effort: &str) -> Value {
    json!({
        "name": format!("gpt-{effort}"),
        "provider_args": ["-m", "openai/gpt-5.5", "--variant", effort],
        "inputs": {
            "prompt": "reply with the single word: ok",
            "named": {}
        }
    })
}

fn parse_launch_events(stdout: &[u8]) -> Vec<Value> {
    let stdout = std::str::from_utf8(stdout).expect("launch stdout should be UTF-8 NDJSON");
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
        .map(|(index, line)| {
            let event: Value = serde_json::from_str(line).unwrap_or_else(|err| {
                panic!(
                    "launch stdout line {} invalid JSON: {err}\n{line}",
                    index + 1
                )
            });
            let schema_id = match event["kind"].as_str() {
                Some("stdout") => "launch.schema.json#/$defs/LaunchStdoutEvent",
                Some("stderr") => "launch.schema.json#/$defs/LaunchStderrEvent",
                Some("marker") => "launch.schema.json#/$defs/LaunchMarkerEvent",
                Some("heartbeat") => "launch.schema.json#/$defs/LaunchHeartbeatEvent",
                Some("exit") => "launch.schema.json#/$defs/LaunchExitEvent",
                other => panic!(
                    "launch stdout line {} has unknown event kind {other:?}: {event}",
                    index + 1
                ),
            };
            assert_valid(&event, schema_id);
            event
        })
        .collect()
}

fn assert_monotonic_launch_events(events: &[Value]) {
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["contract"], CONTRACT);
        assert_eq!(event["request_id"], "req-launch");
        assert!(
            event["time_unix_ms"].as_u64().is_some(),
            "launch event line {} must carry time_unix_ms",
            index + 1
        );
        let seq = event["seq"]
            .as_u64()
            .unwrap_or_else(|| panic!("launch event line {} missing seq", index + 1));
        assert_eq!(
            seq,
            (index + 1) as u64,
            "seq must be strictly monotonic from 1 with no gaps"
        );
    }
}

fn collect_stream_bytes(events: &[Value], kind: &str) -> Vec<u8> {
    events
        .iter()
        .filter(|event| event["kind"] == kind)
        .flat_map(|event| {
            let data_base64 = event["data_base64"]
                .as_str()
                .unwrap_or_else(|| panic!("{kind} event data_base64 must be a string"));
            let decoded = decode_base64(data_base64);
            assert_eq!(
                decode_base64(&encode_base64(&decoded)),
                decoded,
                "{kind} event data_base64 should round-trip to bytes"
            );
            decoded
        })
        .collect()
}

fn assert_process_status_kind(status: &Value) {
    let kind = status["kind"].as_str().expect("status.kind");
    assert!(
        matches!(
            kind,
            "exited"
                | "signal_terminated"
                | "spawn_error"
                | "prolonged_silence"
                | "cancelled"
                | "unknown"
        ),
        "status.kind must be one of the contract ProcessStatus kinds; status={status}"
    );
}

struct FakeOpencodeWrapper {
    dir: PathBuf,
    log_path: PathBuf,
}

impl FakeOpencodeWrapper {
    fn new() -> Self {
        Self::with_script(fake_opencode_script())
    }

    fn with_script(script: String) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-contract-launch");
        fs::create_dir_all(&dir).expect("create fake wrapper temp dir");
        let wrapper_path = dir.join("opencode1");
        let log_path = dir.join("wrapper.log");
        fs::write(&wrapper_path, script).expect("write fake opencode1 wrapper");
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&wrapper_path)
                .expect("fake wrapper metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&wrapper_path, permissions).expect("chmod fake wrapper");
        }
        Self { dir, log_path }
    }

    fn dir(&self) -> &Path {
        &self.dir
    }

    fn log_path(&self) -> &Path {
        &self.log_path
    }

    fn log_path_string(&self) -> String {
        self.log_path.to_string_lossy().into_owned()
    }
}

impl Drop for FakeOpencodeWrapper {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn fake_opencode_script() -> String {
    let stdout = std::str::from_utf8(FAKE_LAUNCH_STDOUT)
        .expect("fake launch stdout fixture should be UTF-8");
    let stderr = std::str::from_utf8(FAKE_LAUNCH_STDERR)
        .expect("fake launch stderr fixture should be UTF-8");
    format!(
        "{}\nprintf '%s' {}\nprintf '%s' {} >&2\nexit 7\n",
        fake_wrapper_log_script(),
        shell_single_quote(stdout),
        shell_single_quote(stderr)
    )
}

fn slow_opencode_script(exit_code: i32) -> String {
    format!(
        "{}\nsleep {}\nexit {}\n",
        fake_wrapper_log_script(),
        SLOW_WRAPPER_SLEEP_SECONDS,
        exit_code
    )
}

fn fake_wrapper_log_script() -> &'static str {
    "#!/bin/sh\n\
{\n\
  printf 'argv0=%s\\n' \"$0\"\n\
  for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
} > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\""
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
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

fn assert_terminal_classification(status: Value, stdout: &str, stderr: &str, expected_kind: &str) {
    let output = invoke(
        "terminal.classify",
        json!({
            "stdout_base64": encode_base64(stdout.as_bytes()),
            "stderr_base64": encode_base64(stderr.as_bytes()),
            "status": status,
            "observed_at_unix_ms": OBSERVED_AT_UNIX_MS
        }),
    );
    assert!(
        output.status.success(),
        "terminal.classify exited {:?}; stderr: {}\nstdout: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let response = json_stdout(&output);
    assert_valid(
        &response,
        "terminal.schema.json#/$defs/TerminalClassifyResponse",
    );
    assert_valid(
        &response["result"],
        "terminal.schema.json#/$defs/TerminalClassifyResult",
    );
    assert_eq!(response["result"]["terminal_signal"]["kind"], expected_kind);
}

fn expected_signal_kind_for_status(status: &Value) -> &'static str {
    match status["kind"].as_str().expect("status.kind") {
        "exited" if status["code"].as_i64() == Some(0) => "clean_exit",
        "exited" => "nonzero_exit",
        "signal_terminated" => "signal_exit",
        "spawn_error" => "spawn_error",
        "prolonged_silence" => "prolonged_silence",
        "cancelled" => "cancelled",
        "unknown" => "unknown",
        other => panic!("unexpected ProcessStatus kind {other}"),
    }
}

fn expected_exit_code_for_status(status: &Value) -> i32 {
    match status["kind"].as_str().expect("status.kind") {
        "exited" => status["code"].as_i64().expect("status.code") as i32,
        "signal_terminated" => 128 + status["signal"].as_i64().expect("status.signal") as i32,
        "prolonged_silence" => 124,
        "cancelled" => 130,
        "spawn_error" | "unknown" => 1,
        other => panic!("unexpected ProcessStatus kind {other}"),
    }
}

fn fixture_session_id() -> &'static str {
    let first_line = include_str!("fixtures/opencode_launch_events.jsonl")
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("opencode launch fixture should not be empty");
    let event: Value = serde_json::from_str(first_line).expect("fixture first line should be JSON");
    let session_id = event["sessionID"]
        .as_str()
        .expect("fixture first line should carry sessionID")
        .to_owned();
    Box::leak(session_id.into_boxed_str())
}

fn json_contains_string(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(value) => value == needle,
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, needle)),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key == needle || json_contains_string(value, needle)),
        _ => false,
    }
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_millis() as u64
}

fn string_array(value: &Value, label: &str) -> Vec<String> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{label} should be an array"))
        .iter()
        .map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("{label} entries should be strings"))
                .to_owned()
        })
        .collect()
}

fn assert_contains_subsequence(argv: &[String], expected: &[&str]) {
    assert!(
        expected.len() <= argv.len(),
        "argv too short to contain expected subsequence; argv={argv:?} expected={expected:?}"
    );
    let found = argv.windows(expected.len()).any(|window| {
        window
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
    });
    assert!(
        found,
        "argv must contain expected subsequence; argv={argv:?} expected={expected:?}"
    );
}

fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        encoded.push(TABLE[(b0 >> 2) as usize] as char);
        encoded.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

fn decode_base64(input: &str) -> Vec<u8> {
    let mut output = Vec::new();
    let mut buffer = 0_u32;
    let mut bits = 0_u8;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            b'\r' | b'\n' | b'\t' | b' ' => continue,
            _ => panic!("invalid base64 byte {byte}"),
        } as u32;
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push(((buffer >> bits) & 0xff) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    output
}
