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
    numbered_fixture_lines(non_empty_fixture_lines(fixture))
        .into_iter()
        .map(parse_numbered_fixture_line)
        .collect()
}

pub fn non_empty_fixture_lines(fixture: &str) -> Vec<&str> {
    non_empty_lines(fixture_lines(fixture))
}

pub fn fixture_lines(fixture: &str) -> Vec<&str> {
    fixture.lines().collect()
}

pub fn non_empty_lines(lines: Vec<&str>) -> Vec<&str> {
    lines
        .into_iter()
        .filter(|line| line_has_text(line))
        .collect()
}

pub fn line_has_text(line: &str) -> bool {
    !line.trim().is_empty()
}

pub fn numbered_fixture_lines(lines: Vec<&str>) -> Vec<(usize, &str)> {
    lines
        .into_iter()
        .enumerate()
        .map(|(index, line)| (index + 1, line))
        .collect()
}

pub fn parse_numbered_fixture_line((line_number, line): (usize, &str)) -> NumberedFixtureEvent {
    parse_opencode_fixture_event(line_number, line)
}

pub fn parse_opencode_fixture_event(line_number: usize, line: &str) -> NumberedFixtureEvent {
    let event = serde_json::from_str(line)
        .unwrap_or_else(|err| panic!("fixture line {line_number} is invalid JSON: {err}"));
    NumberedFixtureEvent { line_number, event }
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

pub fn wrapper_log_args(wrapper_log: &str) -> Vec<&str> {
    wrapper_log_arg_values(wrapper_log_arg_lines(wrapper_log))
}

pub fn wrapper_log_arg_lines(wrapper_log: &str) -> Vec<&str> {
    arg_log_lines(wrapper_log_lines(wrapper_log))
}

pub fn wrapper_log_lines(wrapper_log: &str) -> Vec<&str> {
    wrapper_log.lines().collect()
}

pub fn arg_log_lines(lines: Vec<&str>) -> Vec<&str> {
    lines
        .into_iter()
        .filter(|line| is_wrapper_log_arg_line(line))
        .collect()
}

pub fn is_wrapper_log_arg_line(line: &str) -> bool {
    line.starts_with("arg=")
}

pub fn wrapper_log_arg_values(lines: Vec<&str>) -> Vec<&str> {
    lines.into_iter().map(wrapper_log_arg_value).collect()
}

pub fn wrapper_log_arg_value(line: &str) -> &str {
    line.strip_prefix("arg=").expect("wrapper arg log line")
}

pub fn argv_arg_index(argv: &[&str], needle: &str) -> usize {
    required_argv_arg_index(optional_argv_arg_index(argv, needle), needle, argv)
}

pub fn optional_argv_arg_index(argv: &[&str], needle: &str) -> Option<usize> {
    argv.iter().position(|arg| *arg == needle)
}

pub fn argv_arg_index_containing(argv: &[&str], needle: &str) -> usize {
    required_argv_arg_index(
        optional_argv_arg_index_containing(argv, needle),
        needle,
        argv,
    )
}

pub fn optional_argv_arg_index_containing(argv: &[&str], needle: &str) -> Option<usize> {
    argv.iter().position(|arg| arg.contains(needle))
}

pub fn required_argv_arg_index(index: Option<usize>, needle: &str, argv: &[&str]) -> usize {
    index.unwrap_or_else(|| missing_argv_arg_index(needle, argv))
}

pub fn missing_argv_arg_index(needle: &str, argv: &[&str]) -> ! {
    panic!("argv missing {needle:?}: {argv:?}")
}

pub fn argv_index_before(left: usize, right: usize) -> bool {
    left < right
}

pub fn wrapper_arg_log_line(value: &str) -> String {
    format!("arg={value}")
}

pub fn wrapper_stdin_log_line(value: &str) -> String {
    format!("stdin={value}")
}

pub fn wrapper_log_has_selected_wrapper(wrapper_log: &str) -> bool {
    wrapper_log_lines_has_selected_wrapper(&wrapper_log_lines(wrapper_log))
}

pub fn wrapper_log_has_run_arg(wrapper_log: &str) -> bool {
    wrapper_log_lines_has_run_arg(&wrapper_log_lines(wrapper_log))
}

pub fn wrapper_log_lines_has_selected_wrapper(lines: &[&str]) -> bool {
    lines
        .iter()
        .any(|line| wrapper_log_line_is_selected_wrapper(line))
}

pub fn wrapper_log_line_is_selected_wrapper(line: &str) -> bool {
    line == "argv0=opencode1" || line.ends_with("/opencode1")
}

pub fn wrapper_log_lines_has_run_arg(lines: &[&str]) -> bool {
    lines.iter().any(|line| wrapper_log_line_is_run_arg(line))
}

pub fn wrapper_log_line_is_run_arg(line: &str) -> bool {
    line == "arg=run"
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

pub fn argv_contains_plain_opencode(argv: &[String]) -> bool {
    argv.iter().any(|arg| arg == "opencode")
}

pub fn expected_policy_argv_subsequence() -> &'static [&'static str] {
    &[
        "run",
        "--format",
        "json",
        "--dangerously-skip-permissions",
        "-m",
        "openai/gpt-5.5",
        "--variant",
        "low",
    ]
}

pub fn pure_semantics_preserved(argv: &[String]) -> bool {
    argv.iter().any(|arg| arg == "--pure")
        || argv.first().is_some_and(|arg| {
            matches!(
                arg.as_str(),
                "opencode1" | "opencode2" | "opencode3" | "opencode4" | "opencode5"
            )
        })
}

pub fn policy_diagnostic_matches(diagnostic: &Value, code: &str, needle: &str) -> bool {
    policy_diagnostic_has_error_code(diagnostic, code)
        && diagnostic_text_contains(diagnostic, needle)
}

pub fn policy_diagnostic_has_error_code(diagnostic: &Value, code: &str) -> bool {
    diagnostic["severity"] == "error" && diagnostic["code"] == code
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

pub fn non_empty_launch_stdout_lines(stdout: &[u8]) -> Vec<&str> {
    non_empty_lines(launch_stdout_lines(stdout))
}

pub fn launch_stdout_lines(stdout: &[u8]) -> Vec<&str> {
    launch_stdout_text(stdout).lines().collect()
}

pub fn launch_stdout_text(stdout: &[u8]) -> &str {
    std::str::from_utf8(stdout).expect("launch stdout should be UTF-8 NDJSON")
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
{\n\
  printf 'argv0=%s\\n' \"$0\"\n\
  for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
  printf 'stdin='\n\
  /bin/cat\n\
  printf '\\n'\n\
} > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
exit 0\n"
}

pub fn fake_wrapper_resume_confirming_export_script() -> &'static str {
    "#!/bin/sh\n\
if [ \"$1\" = \"export\" ]; then\n\
  printf '%s\\n' '{\"info\":{\"id\":\"ses_resume_contract\",\"title\":\"resume contract\"},\"messages\":[{\"info\":{\"id\":\"msg-user\",\"role\":\"user\",\"sessionID\":\"ses_resume_contract\",\"time\":{\"created\":1780000000000}},\"parts\":[{\"type\":\"text\",\"text\":\"Notifications delivered:\\n- agent_bash_complete h-s11-external\\n\\n[OULIPOLY-DELIVERY 5169694d-de0f-40d1-890c-6e28e55bab27]\\n\"}]}]}'\n\
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
  printf '%s\\n' '{\"info\":{\"id\":\"ses_resume_contract\",\"title\":\"resume contract\"},\"messages\":[{\"info\":{\"id\":\"msg-user\",\"role\":\"user\",\"sessionID\":\"ses_resume_contract\",\"time\":{\"created\":1780000000000}},\"parts\":[{\"type\":\"text\",\"text\":\"different prompt\"}]}]}'\n\
  exit 0\n\
fi\n\
{\n\
  printf 'argv0=%s\\n' \"$0\"\n\
  for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
} > \"$AGENT_RUNNER_OPENCODE_WRAPPER_LOG\"\n\
printf '{\"type\":\"step_start\",\"sessionID\":\"ses_resume_contract\",\"timestamp\":1780000000001,\"part\":{\"type\":\"step-start\",\"sessionID\":\"ses_resume_contract\"}}\\n'\n\
exit 0\n"
}

pub fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(unique_temp_dir_name(prefix))
}

pub fn unique_temp_dir_name(prefix: &str) -> String {
    formatted_temp_dir_name(prefix, current_time_nanos(), current_process_id())
}

pub fn current_time_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos()
}

pub fn current_process_id() -> u32 {
    std::process::id()
}

pub fn formatted_temp_dir_name(prefix: &str, nanos: u128, process_id: u32) -> String {
    format!("{prefix}-{process_id}-{nanos}")
}

pub fn prepend_path(dir: &Path) -> String {
    joined_path_string(prepended_path_entries(dir))
}

pub fn prepended_path_entries(dir: &Path) -> Vec<PathBuf> {
    vec![dir.to_path_buf()]
}

pub fn joined_path_string(paths: Vec<PathBuf>) -> String {
    std::env::join_paths(paths)
        .expect("join PATH entries")
        .to_string_lossy()
        .into_owned()
}

pub fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

pub fn expected_signal_kind_for_status(status: &Value) -> &'static str {
    signal_kind_for_process_status(terminal_signal_status_kind(status), status)
}

pub fn terminal_signal_status_kind(status: &Value) -> &str {
    required_terminal_signal_status_kind(terminal_signal_status_kind_value(status))
}

pub fn terminal_signal_status_kind_value(status: &Value) -> Option<&str> {
    status["kind"].as_str()
}

pub fn required_terminal_signal_status_kind(kind: Option<&str>) -> &str {
    kind.expect("status.kind")
}

pub fn signal_kind_for_process_status(kind: &str, status: &Value) -> &'static str {
    match kind {
        "exited" => signal_kind_for_exited_status(status),
        "signal_terminated" => "signal_exit",
        "spawn_error" => "spawn_error",
        "prolonged_silence" => "prolonged_silence",
        "cancelled" => "cancelled",
        "unknown" => "unknown",
        other => unexpected_process_status_kind(other),
    }
}

pub fn signal_kind_for_exited_status(status: &Value) -> &'static str {
    if process_status_exit_success(status) {
        "clean_exit"
    } else {
        "nonzero_exit"
    }
}

pub fn process_status_exit_success(status: &Value) -> bool {
    status["code"].as_i64() == Some(0)
}

pub fn unexpected_process_status_kind(other: &str) -> ! {
    panic!("unexpected ProcessStatus kind {other}")
}

pub fn fixture_session_id() -> &'static str {
    let event = fixture_first_event();
    Box::leak(fixture_event_session_id_string(&event).into_boxed_str())
}

pub fn fixture_first_event() -> Value {
    let first_line = fixture_first_line();
    serde_json::from_str(first_line).expect("fixture first line should be JSON")
}

pub fn fixture_first_line() -> &'static str {
    first_fixture_line(fixture_non_empty_lines())
}

pub fn fixture_non_empty_lines() -> Vec<&'static str> {
    first_non_empty_fixture_lines(static_fixture_lines())
}

pub fn static_fixture_lines() -> Vec<&'static str> {
    include_str!("../fixtures/opencode_launch_events.jsonl")
        .lines()
        .collect()
}

pub fn first_non_empty_fixture_lines(lines: Vec<&'static str>) -> Vec<&'static str> {
    first_non_empty_line(lines).into_iter().collect()
}

pub fn first_non_empty_line(lines: Vec<&str>) -> Option<&str> {
    lines.into_iter().find(|line| line_has_text(line))
}

pub fn first_fixture_line(lines: Vec<&'static str>) -> &'static str {
    lines
        .into_iter()
        .next()
        .expect("opencode launch fixture should not be empty")
}

pub fn fixture_event_session_id_string(event: &Value) -> String {
    owned_fixture_session_id(required_fixture_event_session_id(
        fixture_event_session_id_value(event),
    ))
}

pub fn fixture_event_session_id_value(event: &Value) -> Option<&str> {
    event["sessionID"].as_str()
}

pub fn required_fixture_event_session_id(session_id: Option<&str>) -> &str {
    session_id.expect("fixture first line should carry sessionID")
}

pub fn owned_fixture_session_id(session_id: &str) -> String {
    session_id.to_owned()
}

pub fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_millis() as u64
}

pub fn short_deadline_unix_ms() -> u64 {
    unix_ms_now() + 250
}

pub fn string_array(value: &Value, label: &str) -> Vec<String> {
    owned_string_array_entries(string_array_entries(value_array(value, label), label))
}

pub fn value_array<'a>(value: &'a Value, label: &str) -> &'a [Value] {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{label} should be an array"))
}

pub fn string_array_entries<'a>(values: &'a [Value], label: &str) -> Vec<&'a str> {
    values
        .iter()
        .map(|value| string_array_entry(value, label))
        .collect()
}

pub fn string_array_entry<'a>(value: &'a Value, label: &str) -> &'a str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{label} entries should be strings"))
}

pub fn owned_string_array_entries(entries: Vec<&str>) -> Vec<String> {
    entries.into_iter().map(str::to_owned).collect()
}

pub fn contains_subsequence(argv: &[String], expected: &[&str]) -> bool {
    argv.windows(expected.len())
        .any(|window| window_matches_expected(window, expected))
}

pub fn window_matches_expected(window: &[String], expected: &[&str]) -> bool {
    window
        .iter()
        .map(String::as_str)
        .eq(expected.iter().copied())
}
