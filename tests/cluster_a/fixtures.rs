// declared_role: orchestration, parser, formatter, accessor, mapper, filter, predicate, validator
#![allow(unused_imports)]

use super::*;

pub const OBSERVED_AT_UNIX_MS: u64 = 1_780_565_973_556;

pub const FAKE_LAUNCH_STDOUT: &[u8] = include_bytes!("../fixtures/opencode_launch_events.jsonl");

pub const FAKE_LAUNCH_STDERR: &[u8] = b"fake wrapper stderr bytes\n";

pub const INCIDENT_ERROR_EVENT_TIMESTAMP: u64 = 1_780_808_654_364;

pub const INCIDENT_ERROR_EVENT_MESSAGE: &str = "Failed to execute statement";

pub const INCIDENT_ERROR_EVENT_SESSION_ID: &str = "ses_15f9407ccffelCcB6CyXvpzdXK";

pub const INCIDENT_ERROR_EVENT_LINE: &str = "{\"type\":\"error\",\"timestamp\":1780808654364,\"sessionID\":\"ses_15f9407ccffelCcB6CyXvpzdXK\",\"error\":{\"name\":\"UnknownError\",\"data\":{\"message\":\"Failed to execute statement\"}}}";

pub const SLOW_WRAPPER_SLEEP_SECONDS: u64 = 2;

pub const SUBMITTED_USER_TURN_MARKER_FOR_TEST: &str = "oulipoly.submitted_user_turn";

pub const PRODUCED_ASSISTANT_RESPONSE_MARKER_FOR_TEST: &str =
    "oulipoly.produced_assistant_response";

pub const OPENCODE_SESSION_FLAG_FOR_TEST: &str = "--session";

pub const NOTIFICATION_PAYLOAD_NEEDLE_FOR_TEST: &str = "[OULIPOLY NOTIFICATIONS]";

pub struct NumberedFixtureEvent {
    pub line_number: usize,
    pub event: Value,
}

#[derive(Default)]
pub struct FixtureCoverage {
    pub saw_step_start: bool,
    pub saw_text: bool,
    pub saw_step_finish: bool,
}

impl FixtureCoverage {
    pub fn record(&mut self, event_type: &str) {
        match event_type {
            "step_start" => self.saw_step_start = true,
            "text" => self.saw_text = true,
            "step_finish" => self.saw_step_finish = true,
            _ => {}
        }
    }
}

pub fn parse_opencode_fixture_events(fixture: &str) -> Vec<NumberedFixtureEvent> {
    fixture
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            let line_number = index + 1;
            let event = serde_json::from_str(line)
                .unwrap_or_else(|err| panic!("fixture line {line_number} is invalid JSON: {err}"));
            NumberedFixtureEvent { line_number, event }
        })
        .collect()
}

pub fn fixture_event_type(numbered: &NumberedFixtureEvent) -> &str {
    numbered.event["type"].as_str().unwrap_or_else(|| {
        panic!(
            "fixture line {} missing top-level type",
            numbered.line_number
        )
    })
}

pub fn fixture_event_session_id(numbered: &NumberedFixtureEvent) -> &str {
    numbered.event["sessionID"]
        .as_str()
        .unwrap_or_else(|| panic!("fixture line {} missing sessionID", numbered.line_number))
}

pub fn fixture_event_part(numbered: &NumberedFixtureEvent) -> &serde_json::Map<String, Value> {
    numbered.event["part"]
        .as_object()
        .unwrap_or_else(|| panic!("fixture line {} missing nested part", numbered.line_number))
}

pub fn fixture_part_type(part: &serde_json::Map<String, Value>, line_number: usize) -> &str {
    part.get("type")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("fixture line {line_number} missing part.type"))
}

pub fn expected_session_marker<'a>(events: &'a [Value], fixture_session_id: &str) -> &'a Value {
    find_session_marker(events, fixture_session_id).unwrap_or_else(|| {
        panic!(
            "launch stream must emit a marker naming captured opencode sessionID {fixture_session_id}; events={events:?}"
        )
    })
}

pub fn find_session_marker<'a>(events: &'a [Value], fixture_session_id: &str) -> Option<&'a Value> {
    events
        .iter()
        .find(|event| marker_mentions_session(event, fixture_session_id))
}

pub fn marker_mentions_session(event: &Value, fixture_session_id: &str) -> bool {
    event["kind"] == "marker"
        && event["name"]
            .as_str()
            .is_some_and(|name| name.contains(fixture_session_id))
}

pub fn wrapper_log_text(wrapper_log_path: &Path) -> String {
    fs::read_to_string(wrapper_log_path)
        .expect("selected opencodeN wrapper should record its invocation")
}

pub fn wrapper_nul_log_args(wrapper_log_path: &Path) -> Vec<String> {
    let bytes = fs::read(wrapper_log_path)
        .expect("selected opencodeN wrapper should record its invocation");
    let args = bytes
        .strip_suffix(&[0])
        .expect("NUL argv log should end with a terminator");
    args.split(|byte| *byte == 0)
        .map(|arg| String::from_utf8(arg.to_vec()).expect("wrapper argv should be UTF-8"))
        .collect()
}

pub fn wrapper_log_args(wrapper_log: &str) -> Vec<&str> {
    wrapper_log
        .lines()
        .filter_map(|line| line.strip_prefix("arg="))
        .collect()
}

pub fn argv_arg_index(argv: &[&str], needle: &str) -> usize {
    argv.iter()
        .position(|arg| *arg == needle)
        .unwrap_or_else(|| panic!("argv missing {needle:?}: {argv:?}"))
}

pub fn argv_arg_index_containing(argv: &[&str], needle: &str) -> usize {
    argv.iter()
        .position(|arg| arg.contains(needle))
        .unwrap_or_else(|| panic!("argv missing {needle:?}: {argv:?}"))
}

pub fn wrapper_arg_log_line(value: &str) -> String {
    format!("arg={value}")
}

pub fn wrapper_stdin_log_line(value: &str) -> String {
    format!("stdin={value}")
}

pub fn declared_env_log_text(wrapper_log_path: &Path) -> String {
    fs::read_to_string(wrapper_log_path)
        .expect("selected opencodeN wrapper should record env evidence")
}

pub fn has_heartbeat_event(events: &[Value]) -> bool {
    events.iter().any(|event| event["kind"] == "heartbeat")
}

pub fn expected_submitted_user_turn_marker(events: &[Value]) -> &Value {
    submitted_user_turn_marker(events)
        .unwrap_or_else(|| panic!("missing submitted user turn marker; events={events:?}"))
}

pub fn submitted_user_turn_marker(events: &[Value]) -> Option<&Value> {
    events
        .iter()
        .find(|event| is_submitted_user_turn_marker(event))
}

pub fn is_submitted_user_turn_marker(event: &Value) -> bool {
    event["kind"] == "marker" && event["name"] == SUBMITTED_USER_TURN_MARKER_FOR_TEST
}

pub fn policy_result(response: &Value) -> &Value {
    &response["result"]
}

pub fn policy_result_argv(result: &Value) -> Vec<String> {
    string_array(&result["argv"], "result.argv")
}

pub fn policy_result_env(result: &Value) -> &Value {
    &result["env"]
}

pub fn expected_policy_argv_subsequence() -> &'static [&'static str] {
    &[
        "run",
        "--format",
        "json",
        "--dangerously-skip-permissions",
        "-m",
        "openai/gpt-5.6-sol",
        "--variant",
        "low",
    ]
}

pub fn pure_semantics_preserved(argv: &[String]) -> bool {
    argv.iter().any(|arg| arg == "--pure")
        || argv.first().is_some_and(|arg| {
            matches!(
                arg.rsplit('/').next().unwrap_or(arg),
                "opencode1" | "opencode2" | "opencode3" | "opencode4" | "opencode5"
            )
        })
}

pub fn policy_diagnostic_matches(diagnostic: &Value, code: &str, needle: &str) -> bool {
    policy_diagnostic_has_severity_and_code(diagnostic, "error", code)
        && diagnostic_text_contains(diagnostic, needle)
}

pub fn policy_diagnostic_has_severity_and_code(
    diagnostic: &Value,
    severity: &str,
    code: &str,
) -> bool {
    diagnostic["severity"] == severity && diagnostic["code"] == code
}

pub fn diagnostic_text_contains(diagnostic: &Value, needle: &str) -> bool {
    text_contains(&diagnostic_json_text(diagnostic), needle)
}

pub fn diagnostic_json_text(diagnostic: &Value) -> String {
    diagnostic.to_string()
}

pub fn value_json_text(value: &Value) -> String {
    value.to_string()
}

pub fn text_contains(text: &str, needle: &str) -> bool {
    text.contains(needle)
}

pub struct FakeOpencodeWrapper {
    pub dir: PathBuf,
    pub log_path: PathBuf,
    pub log_path_string: String,
}

impl FakeOpencodeWrapper {
    pub fn new() -> Self {
        Self::with_script(fake_opencode_script())
    }

    pub fn with_script(script: String) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-contract-launch");
        create_fake_wrapper_dir(&dir);
        let wrapper_path = fake_wrapper_path(&dir);
        let log_path = fake_wrapper_log_path(&dir);
        write_fake_wrapper(&wrapper_path, script);
        let log_path_string = path_string(&log_path);
        Self::from_parts(dir, log_path, log_path_string)
    }

    pub fn with_counted_new_session(session_id: &str) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-counted-new-session");
        create_fake_wrapper_dir(&dir);
        let wrapper_path = fake_wrapper_path(&dir);
        let log_path = fake_wrapper_log_path(&dir);
        write_fake_wrapper(
            &wrapper_path,
            fake_counted_new_session_script(&log_path, session_id),
        );
        let log_path_string = path_string(&log_path);
        Self::from_parts(dir, log_path, log_path_string)
    }

    pub fn with_counted_resume() -> Self {
        Self::with_counted_resume_payload(resume_payload())
    }

    pub fn with_counted_resume_payload(payload: &str) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-counted-resume");
        create_fake_wrapper_dir(&dir);
        let wrapper_path = fake_wrapper_path(&dir);
        let log_path = fake_wrapper_log_path(&dir);
        write_fake_wrapper(
            &wrapper_path,
            fake_counted_resume_script(&log_path, payload),
        );
        let log_path_string = path_string(&log_path);
        Self::from_parts(dir, log_path, log_path_string)
    }

    pub fn with_counted_resume_late_completion() -> (Self, PathBuf) {
        let dir = unique_temp_dir("agent-runner-opencode-counted-resume-reconciliation");
        create_fake_wrapper_dir(&dir);
        let wrapper_path = fake_wrapper_path(&dir);
        let log_path = fake_wrapper_log_path(&dir);
        let completion_marker = dir.join("completion-ready");
        write_fake_wrapper(
            &wrapper_path,
            fake_counted_resume_late_completion_script(
                &log_path,
                &completion_marker,
                resume_payload(),
            ),
        );
        let log_path_string = path_string(&log_path);
        (
            Self::from_parts(dir, log_path, log_path_string),
            completion_marker,
        )
    }

    fn from_parts(dir: PathBuf, log_path: PathBuf, log_path_string: String) -> Self {
        Self {
            dir,
            log_path,
            log_path_string,
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn log_path_str(&self) -> &str {
        &self.log_path_string
    }
}

pub fn create_fake_wrapper_dir(dir: &Path) {
    fs::create_dir_all(dir).expect("create fake wrapper temp dir");
}

pub fn fake_wrapper_path(dir: &Path) -> PathBuf {
    dir.join("opencode1")
}

pub fn fake_wrapper_log_path(dir: &Path) -> PathBuf {
    dir.join("wrapper.log")
}

pub fn write_fake_wrapper(wrapper_path: &Path, script: String) {
    fs::write(wrapper_path, script).expect("write fake opencode1 wrapper");
    make_executable(wrapper_path);
}

#[cfg(unix)]
pub fn make_executable(path: &Path) {
    set_path_permissions(path, permissions_with_mode(path_permissions(path), 0o755));
}

#[cfg(unix)]
pub fn path_permissions(path: &Path) -> fs::Permissions {
    fs::metadata(path)
        .expect("fake wrapper metadata")
        .permissions()
}

#[cfg(unix)]
pub fn permissions_with_mode(mut permissions: fs::Permissions, mode: u32) -> fs::Permissions {
    permissions.set_mode(mode);
    permissions
}

#[cfg(unix)]
pub fn set_path_permissions(path: &Path, permissions: fs::Permissions) {
    fs::set_permissions(path, permissions).expect("chmod fake wrapper");
}

#[cfg(not(unix))]
pub fn make_executable(_path: &Path) {}

impl Drop for FakeOpencodeWrapper {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub fn fake_opencode_script() -> String {
    fake_opencode_script_with_output(fake_launch_stdout_text(), fake_launch_stderr_text())
}

pub fn fake_launch_stdout_text() -> &'static str {
    std::str::from_utf8(FAKE_LAUNCH_STDOUT).expect("fake launch stdout fixture should be UTF-8")
}

pub fn fake_launch_stderr_text() -> &'static str {
    std::str::from_utf8(FAKE_LAUNCH_STDERR).expect("fake launch stderr fixture should be UTF-8")
}

pub fn fake_opencode_script_with_output(stdout: &str, stderr: &str) -> String {
    fake_opencode_script_with_output_and_status(stdout, stderr, 7)
}

pub fn fake_opencode_script_with_output_and_status(
    stdout: &str,
    stderr: &str,
    exit_code: i32,
) -> String {
    format!(
        "{}\nprintf '%s' {}\nprintf '%s' {} >&2\nexit {}\n",
        fake_wrapper_log_script(),
        shell_single_quote(stdout),
        shell_single_quote(stderr),
        exit_code
    )
}

pub fn incident_error_event_stdout() -> String {
    format!("{INCIDENT_ERROR_EVENT_LINE}\n")
}

pub fn recovered_after_incident_error_event_stdout() -> String {
    format!(
        "{}{}\n",
        incident_error_event_stdout(),
        recovered_after_incident_error_event_line()
    )
}

pub fn recovered_after_incident_error_event_line() -> String {
    format!(
        "{{\"type\":\"step_start\",\"timestamp\":{},\"sessionID\":\"{}\",\"part\":{{\"type\":\"step-start\",\"sessionID\":\"{}\"}}}}",
        INCIDENT_ERROR_EVENT_TIMESTAMP + 1,
        INCIDENT_ERROR_EVENT_SESSION_ID,
        INCIDENT_ERROR_EVENT_SESSION_ID
    )
}

pub fn slow_opencode_script(exit_code: i32) -> String {
    format!(
        "{}\n/bin/sleep {}\nexit {}\n",
        fake_wrapper_log_script(),
        SLOW_WRAPPER_SLEEP_SECONDS,
        exit_code
    )
}

pub fn env_probe_opencode_script() -> String {
    "#!/bin/sh\n\
{\n\
  printf 'declared=%s\\n' \"${DECLARED_CHILD_ENV-}\"\n\
  printf 'xdg=%s\\n' \"${XDG_DATA_HOME-}\"\n\
  printf 'oulipoly_data=%s\\n' \"${OULIPOLY_DATA_DIR-<unset>}\"\n\
  printf 'oulipoly_parent=%s\\n' \"${OULIPOLY_PARENT_INVOCATION-<unset>}\"\n\
  printf 'agent_runner_bin=%s\\n' \"${AGENT_BASH_AGENT_RUNNER_BIN-<unset>}\"\n\
  if [ \"${UNDECLARED_PARENT_ENV+x}\" = x ]; then\n\
    printf 'undeclared=%s\\n' \"$UNDECLARED_PARENT_ENV\"\n\
  else\n\
    printf 'undeclared=<unset>\\n'\n\
  fi\n\
  if [ \"${OPENAI_API_KEY+x}\" = x ]; then\n\
    printf 'openai=%s\\n' \"$OPENAI_API_KEY\"\n\
  else\n\
    printf 'openai=<unset>\\n'\n\
  fi\n\
} > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
exit 0\n"
        .to_string()
}

pub fn fake_wrapper_log_script() -> &'static str {
    "#!/bin/sh\n\
{\n\
   printf 'argv0=%s\\n' \"$0\"\n\
   for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
} > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\""
}

pub fn fake_wrapper_log_stdin_script() -> &'static str {
    "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
  printf '{\"info\":{\"id\":\"%s\",\"title\":\"empty resume fixture\"},\"messages\":[]}\\n' \"$2\"\n\
  exit 0\n\
fi\n\
{\n\
  printf 'argv0=%s\\n' \"$0\"\n\
  for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
  printf 'stdin='\n\
  /bin/cat\n\
  printf '\\n'\n\
} > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
exit 0\n"
}

pub fn fake_wrapper_log_only_script() -> String {
    "#!/bin/sh\n\
for arg in \"$@\"; do printf '%s\\0' \"$arg\"; done > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
exit 0\n"
        .to_string()
}

pub fn fake_wrapper_runtime_identity_script() -> String {
    "#!/bin/sh\n\
if [ \"${CONTEXT_SELECTOR-}\" != \"runtime-a\" ]; then\n\
  printf '%s\\n' 'native runtime selector mismatch' >&2\n\
  exit 66\n\
fi\n\
if [ \"$1\" = \"export\" ]; then\n\
  printf '{\"info\":{\"id\":\"%s\",\"title\":\"runtime identity fixture\"},\"messages\":[]}\\n' \"$2\"\n\
fi\n\
exit 0\n"
        .to_string()
}

pub fn fake_counted_new_session_script(count_path: &Path, session_id: &str) -> String {
    let event = json!({
        "type": "step_start",
        "timestamp": OBSERVED_AT_UNIX_MS,
        "sessionID": session_id,
        "part": {
            "type": "step-start",
            "sessionID": session_id
        }
    })
    .to_string();
    format!(
        "#!/bin/sh\n\
if [ \"$1\" = \"session\" ] && [ \"${{2:-}}\" = \"list\" ]; then\n\
  if [ \"${{XDG_DATA_HOME-}}\" != \"/tmp/agent-runner-opencode-recovery-xdg\" ]; then\n\
    printf '%s\\n' 'recovery XDG_DATA_HOME mismatch' >&2\n\
    exit 65\n\
  fi\n\
  printf '%s\\n' '[]'\n\
  exit 0\n\
fi\n\
count=0\n\
if [ -f {count_path} ]; then count=$(/bin/cat {count_path}); fi\n\
count=$((count + 1))\n\
printf '%s\\n' \"$count\" > {count_path}\n\
printf '%s\\n' {event}\n\
exit 0\n",
        count_path = shell_single_quote(&path_string(count_path)),
        event = shell_single_quote(&event),
    )
}

pub fn fake_counted_resume_script(count_path: &Path, payload: &str) -> String {
    let export = json!({
        "info": {"id": resume_session_id(), "title": "durable resume contract"},
        "messages": [{
            "info": {
                "id": "msg-durable-resume-user",
                "role": "user",
                "sessionID": resume_session_id(),
                "model": {
                    "providerID": "openai",
                    "modelID": "gpt-5.6-sol",
                    "variant": "low"
                },
                "time": {"created": 4_102_444_800_000_u64}
            },
            "parts": [{"type": "text", "text": payload}]
        }]
    })
    .to_string();
    let event = json!({
        "type": "step_start",
        "timestamp": OBSERVED_AT_UNIX_MS,
        "sessionID": resume_session_id(),
        "part": {
            "type": "step-start",
            "sessionID": resume_session_id()
        }
    })
    .to_string();
    format!(
        "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
  printf '%s\\n' {export}\n\
  exit 0\n\
fi\n\
count=0\n\
if [ -f {count_path} ]; then count=$(/bin/cat {count_path}); fi\n\
count=$((count + 1))\n\
printf '%s\\n' \"$count\" > {count_path}\n\
printf '%s\\n' {event}\n\
exit 0\n",
        count_path = shell_single_quote(&path_string(count_path)),
        export = shell_single_quote(&export),
        event = shell_single_quote(&event),
    )
}

pub fn fake_counted_resume_late_completion_script(
    count_path: &Path,
    completion_marker: &Path,
    payload: &str,
) -> String {
    let user = json!({
        "info": {
            "id": "msg-durable-resume-user",
            "role": "user",
            "sessionID": resume_session_id(),
            "model": {
                "providerID": "openai",
                "modelID": "gpt-5.6-sol",
                "variant": "low"
            },
            "time": {"created": 4_102_444_800_000_u64}
        },
        "parts": [{"type": "text", "text": payload}]
    });
    let pending_export = json!({
        "info": {"id": resume_session_id(), "title": "durable resume contract"},
        "messages": [user.clone()]
    })
    .to_string();
    let completed_export = json!({
        "info": {"id": resume_session_id(), "title": "durable resume contract"},
        "messages": [user, {
            "info": {
                "id": "msg-durable-resume-assistant",
                "role": "assistant",
                "sessionID": resume_session_id(),
                "providerID": "openai",
                "modelID": "gpt-5.6-sol",
                "variant": "low",
                "time": {
                    "created": 4_102_444_800_001_u64,
                    "completed": 4_102_444_800_002_u64
                }
            },
            "parts": [{"type": "text", "text": "done"}]
        }]
    })
    .to_string();
    let event = json!({
        "type": "step_start",
        "timestamp": OBSERVED_AT_UNIX_MS,
        "sessionID": resume_session_id(),
        "part": {"type": "step-start", "sessionID": resume_session_id()}
    })
    .to_string();
    format!(
        "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
  if [ -e {completion_marker} ]; then\n\
    printf '%s\\n' {completed_export}\n\
  else\n\
    printf '%s\\n' {pending_export}\n\
  fi\n\
  exit 0\n\
fi\n\
count=0\n\
if [ -f {count_path} ]; then count=$(/bin/cat {count_path}); fi\n\
count=$((count + 1))\n\
printf '%s\\n' \"$count\" > {count_path}\n\
printf '%s\\n' {event}\n\
exit 0\n",
        completion_marker = shell_single_quote(&path_string(completion_marker)),
        count_path = shell_single_quote(&path_string(count_path)),
        completed_export = shell_single_quote(&completed_export),
        pending_export = shell_single_quote(&pending_export),
        event = shell_single_quote(&event),
    )
}

pub fn fake_wrapper_nul_log_resume_confirming_export_script(prompt: &str) -> String {
    let export = json!({
        "info": {"id": resume_session_id(), "title": "resume contract"},
        "messages": [
            {
                "info": {
                    "id": "msg-user",
                    "role": "user",
                    "sessionID": resume_session_id(),
                    "model": {
                        "providerID": "openai",
                        "modelID": "gpt-5.6-sol",
                        "variant": "low"
                    },
                    "time": {"created": 4_102_444_800_000_u64}
                },
                "parts": [{"type": "text", "text": prompt}]
            },
            {
                "info": {
                    "id": "msg-assistant",
                    "role": "assistant",
                    "sessionID": resume_session_id(),
                    "providerID": "openai",
                    "modelID": "gpt-5.6-sol",
                    "variant": "low",
                    "time": {
                        "created": 4_102_444_800_001_u64,
                        "completed": 4_102_444_800_002_u64
                    }
                },
                "parts": [{"type": "text", "text": "done"}]
            }
        ]
    })
    .to_string();
    format!(
        "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
  printf '%s\\n' {}\n\
  exit 0\n\
fi\n\
for arg in \"$@\"; do printf '%s\\0' \"$arg\"; done > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
printf '%s\\n' '{{\"type\":\"step_start\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000001,\"part\":{{\"type\":\"step-start\",\"sessionID\":\"ses_resume_contract\"}}}}'\n\
exit 0\n",
        shell_single_quote(&export)
    )
}

pub fn fake_wrapper_resume_confirming_export_script() -> &'static str {
    "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
	  printf '%s\\n' '{\"info\":{\"id\":\"ses_resume_contract\",\"title\":\"resume contract\"},\"messages\":[{\"info\":{\"id\":\"msg-user\",\"role\":\"user\",\"sessionID\":\"ses_resume_contract\",\"model\":{\"providerID\":\"openai\",\"modelID\":\"gpt-5.6-sol\",\"variant\":\"low\"},\"time\":{\"created\":4102444800000}},\"parts\":[{\"type\":\"text\",\"text\":\"Notifications delivered:\\n- agent_bash_complete h-s11-external\\n\\n[OULIPOLY-DELIVERY 5169694d-de0f-40d1-890c-6e28e55bab27]\\n\"}]}]}'\n\
  exit 0\n\
fi\n\
{\n\
  printf 'argv0=%s\\n' \"$0\"\n\
  for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
} > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
printf '{\"type\":\"step_start\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000001,\"part\":{\"type\":\"step-start\",\"sessionID\":\"ses_resume_contract\"}}\\n'\n\
exit 0\n"
}

pub fn fake_wrapper_resume_unconfirmed_export_script() -> &'static str {
    "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
	  printf '%s\\n' '{\"info\":{\"id\":\"ses_resume_contract\",\"title\":\"resume contract\"},\"messages\":[{\"info\":{\"id\":\"msg-user\",\"role\":\"user\",\"sessionID\":\"ses_resume_contract\",\"model\":{\"providerID\":\"openai\",\"modelID\":\"gpt-5.6-sol\",\"variant\":\"low\"},\"time\":{\"created\":4102444800000}},\"parts\":[{\"type\":\"text\",\"text\":\"different prompt\"}]}]}'\n\
  exit 0\n\
fi\n\
{\n\
  printf 'argv0=%s\\n' \"$0\"\n\
  for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
} > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
printf '{\"type\":\"step_start\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000001,\"part\":{\"type\":\"step-start\",\"sessionID\":\"ses_resume_contract\"}}\\n'\n\
exit 0\n"
}

pub fn fake_wrapper_completed_resume_then_hang_script() -> String {
    "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
	  printf '%s\\n' '{\"info\":{\"id\":\"ses_resume_contract\",\"title\":\"resume contract\"},\"messages\":[{\"info\":{\"id\":\"msg-user\",\"role\":\"user\",\"sessionID\":\"ses_resume_contract\",\"model\":{\"providerID\":\"openai\",\"modelID\":\"gpt-5.6-sol\",\"variant\":\"low\"},\"time\":{\"created\":4102444800000}},\"parts\":[{\"type\":\"text\",\"text\":\"Notifications delivered:\\n- agent_bash_complete h-s11-external\\n\\n[OULIPOLY-DELIVERY 5169694d-de0f-40d1-890c-6e28e55bab27]\\n\"}]},{\"info\":{\"id\":\"msg-assistant\",\"role\":\"assistant\",\"sessionID\":\"ses_resume_contract\",\"providerID\":\"openai\",\"modelID\":\"gpt-5.6-sol\",\"variant\":\"low\",\"time\":{\"created\":4102444800001,\"completed\":4102444800002}},\"parts\":[{\"type\":\"text\",\"text\":\"done\"}]}]}'\n\
  exit 0\n\
fi\n\
printf '{\"type\":\"step_start\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000001,\"part\":{\"type\":\"step-start\",\"sessionID\":\"ses_resume_contract\"}}\\n'\n\
printf '{\"type\":\"text\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000002,\"part\":{\"type\":\"text\",\"sessionID\":\"ses_resume_contract\",\"text\":\"done\"}}\\n'\n\
printf '{\"type\":\"step_finish\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000003,\"part\":{\"type\":\"step-finish\",\"sessionID\":\"ses_resume_contract\",\"reason\":\"stop\"}}\\n'\n\
/bin/sleep 5\n\
exit 9\n"
        .to_string()
}

pub fn fake_wrapper_completed_resume_with_non_terminal_tail_script() -> &'static str {
    "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
	  printf '%s\\n' '{\"info\":{\"id\":\"ses_resume_contract\",\"title\":\"resume contract\"},\"messages\":[{\"info\":{\"id\":\"msg-user\",\"role\":\"user\",\"sessionID\":\"ses_resume_contract\",\"model\":{\"providerID\":\"openai\",\"modelID\":\"gpt-5.6-sol\",\"variant\":\"low\"},\"time\":{\"created\":4102444800000}},\"parts\":[{\"type\":\"text\",\"text\":\"Notifications delivered:\\n- agent_bash_complete h-s11-external\\n\\n[OULIPOLY-DELIVERY 5169694d-de0f-40d1-890c-6e28e55bab27]\\n\"}]},{\"info\":{\"id\":\"msg-assistant\",\"role\":\"assistant\",\"sessionID\":\"ses_resume_contract\",\"providerID\":\"openai\",\"modelID\":\"gpt-5.6-sol\",\"variant\":\"low\",\"time\":{\"created\":4102444800001,\"completed\":4102444800002}},\"parts\":[{\"type\":\"text\",\"text\":\"done\"}]}]}'\n\
  exit 0\n\
fi\n\
printf '%s\\n' '{\"type\":\"step_finish\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000003,\"part\":{\"type\":\"step-finish\",\"sessionID\":\"ses_resume_contract\",\"reason\":\"stop\"}}'\n\
printf '%s' '{\"type\":\"step_start\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000004,\"part\":{\"type\":\"step-start\",\"sessionID\":\"ses_resume_contract\"}}'\n\
exit 0\n"
}

pub fn fake_wrapper_completed_export_then_hang_script() -> String {
    "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
  printf '%s\\n' 'unexpected full transcript export' > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
  exit 70\n\
fi\n\
/bin/sleep 1\n\
exit 9\n"
        .to_string()
}

pub fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

pub fn prepend_path(dir: &Path) -> String {
    std::env::join_paths([dir])
        .expect("join PATH entries")
        .to_string_lossy()
        .into_owned()
}

pub fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

pub fn expected_signal_kind_for_status(status: &Value) -> &'static str {
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

pub fn fixture_session_id() -> &'static str {
    let first_line = include_str!("../fixtures/opencode_launch_events.jsonl")
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("opencode launch fixture should not be empty")
        .to_string();
    let event: Value =
        serde_json::from_str(&first_line).expect("fixture first line should be JSON");
    Box::leak(
        event["sessionID"]
            .as_str()
            .expect("fixture first line should carry sessionID")
            .to_owned()
            .into_boxed_str(),
    )
}

pub fn short_deadline_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_millis() as u64
        + 1_500
}

pub fn string_array(value: &Value, label: &str) -> Vec<String> {
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

pub fn contains_subsequence(argv: &[String], expected: &[&str]) -> bool {
    argv.windows(expected.len()).any(|window| {
        window
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
    })
}
