// declared_role: orchestration, parser, formatter, accessor, mapper, filter, predicate, validator
#![allow(unused_imports)]

use super::*;

pub const OPENCODE_EXPORT_RAW: &str = include_str!("../fixtures/opencode_export.json");

pub fn native_part_type(part: &Value) -> &str {
    part["type"].as_str().expect("part.type string")
}

pub fn record_part_type(part_types: &mut BTreeSet<String>, part_type: &str) {
    part_types.insert(part_type.to_owned());
}

pub fn replacement_record_bytes() -> &'static [u8] {
    b"{\"role\":\"user\",\"text\":\"replacement\"}\n"
}

pub fn native_export_fixture() -> Value {
    parse_native_export(OPENCODE_EXPORT_RAW)
}

pub fn parse_native_export(raw: &str) -> Value {
    let json_start = raw
        .find('{')
        .expect("opencode export fixture should contain a JSON object");
    serde_json::from_str(&raw[json_start..]).expect("opencode export JSON body should parse")
}

pub fn live_opencode_session_id() -> String {
    let output = run_live_opencode();
    let stdout = live_opencode_stdout_text(&output);
    live_opencode_session_id_from_output(&output, &stdout)
}

pub fn live_opencode_stdout_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

pub fn live_opencode_session_id_from_output(output: &std::process::Output, stdout: &str) -> String {
    live_session_id_from_stdout(stdout)
        .unwrap_or_else(|| successful_live_opencode_missing_session_id(output, stdout))
}

pub fn successful_live_opencode_missing_session_id(
    output: &std::process::Output,
    stdout: &str,
) -> ! {
    assert_live_opencode_success(output, stdout);
    panic!("live opencode1 run did not emit sessionID; stdout: {stdout}")
}

pub fn run_live_opencode() -> std::process::Output {
    Command::new("opencode1")
        .args(live_opencode_args())
        .output()
        .expect("spawn live opencode1 run")
}

pub fn live_opencode_args() -> [&'static str; 8] {
    [
        "run",
        "--format",
        "json",
        "-m",
        "openai/gpt-5.6-sol",
        "--variant",
        "low",
        "reply with the single word: ok",
    ]
}

pub fn assert_live_opencode_success(output: &std::process::Output, stdout: &str) {
    assert!(
        output.status.success(),
        "live opencode1 run failed; exit {:?}; stderr: {}; stdout: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
        stdout
    );
}

pub fn live_session_id_from_stdout(stdout: &str) -> Option<String> {
    first_session_id(stdout_events_with_session_id(parse_stdout_events(stdout)))
}

pub fn parse_stdout_events(stdout: &str) -> Vec<Value> {
    present_stdout_events(parse_stdout_lines(stdout_lines(stdout)))
}

pub fn stdout_lines(stdout: &str) -> Vec<&str> {
    stdout.lines().collect()
}

pub fn parse_stdout_lines(lines: Vec<&str>) -> Vec<Option<Value>> {
    lines.into_iter().map(parse_json_line).collect()
}

pub fn present_stdout_events(events: Vec<Option<Value>>) -> Vec<Value> {
    events.into_iter().flatten().collect()
}

pub fn stdout_events_with_session_id(events: Vec<Value>) -> Vec<Value> {
    events.into_iter().filter(event_has_session_id).collect()
}

pub fn event_has_session_id(event: &Value) -> bool {
    event["sessionID"].as_str().is_some()
}

pub fn first_session_id(events: Vec<Value>) -> Option<String> {
    events
        .into_iter()
        .find_map(|event| event_session_id(&event))
}

pub fn parse_json_line(line: &str) -> Option<Value> {
    serde_json::from_str(line).ok()
}

pub fn event_session_id(event: &Value) -> Option<String> {
    event["sessionID"].as_str().map(str::to_owned)
}

pub fn fixture_session_id() -> &'static str {
    Box::leak(fixture_session_id_string().into_boxed_str())
}

pub fn fixture_session_id_string() -> String {
    native_export_session_id(&native_export_fixture()).to_owned()
}

pub fn native_export_session_id(export: &Value) -> &str {
    export["info"]["id"].as_str().expect("fixture info.id")
}

pub fn fixture_message_count() -> usize {
    native_export_message_count(&native_export_fixture())
}

pub fn native_export_message_count(export: &Value) -> usize {
    export["messages"]
        .as_array()
        .expect("fixture messages array")
        .len()
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

pub fn assert_error_envelope(output: std::process::Output) -> Value {
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
    assert_eq!(response["ok"], false);
}

pub fn turn_ids(result: &Value) -> Vec<String> {
    turns(result).iter().map(turn_id).collect()
}

pub fn turns(result: &Value) -> &[Value] {
    result["turns"].as_array().expect("turns array")
}

pub fn turn_id(turn: &Value) -> String {
    turn["id"]
        .as_str()
        .unwrap_or_else(|| panic!("turn must have stable string id: {turn}"))
        .to_owned()
}

pub fn canonical_record_count(bytes: &[u8]) -> usize {
    let text = canonical_export_text(bytes);
    let trimmed = trimmed_canonical_export_text(text);
    assert_canonical_export_not_empty(trimmed);
    json_record_count(trimmed).unwrap_or_else(|| jsonl_record_count(trimmed))
}

pub fn canonical_result_decoded_bytes(result: &Value) -> Vec<u8> {
    decode_base64(required_canonical_result_data_base64(
        canonical_result_data_base64_value(result),
    ))
}

pub fn canonical_result_data_base64_value(result: &Value) -> Option<&str> {
    result["data_base64"].as_str()
}

pub fn required_canonical_result_data_base64(data_base64: Option<&str>) -> &str {
    data_base64.expect("data_base64 string")
}

pub fn canonical_result_sha(result: &Value) -> &str {
    required_canonical_result_sha(canonical_result_sha_value(result))
}

pub fn canonical_result_sha_value(result: &Value) -> Option<&str> {
    result["sha256"].as_str()
}

pub fn required_canonical_result_sha(sha: Option<&str>) -> &str {
    sha.expect("sha256 string")
}

pub fn canonical_bytes_sha(bytes: &[u8]) -> String {
    sha256_hex(bytes)
}

pub fn canonical_result_turn_count(result: &Value) -> usize {
    usize_turn_count(required_canonical_result_turn_count(
        canonical_result_turn_count_value(result),
    ))
}

pub fn canonical_result_turn_count_value(result: &Value) -> Option<u64> {
    result["turn_count"].as_u64()
}

pub fn required_canonical_result_turn_count(count: Option<u64>) -> u64 {
    count.expect("turn_count integer")
}

pub fn usize_turn_count(count: u64) -> usize {
    count as usize
}

pub fn canonical_export_text(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).expect("canonical export should be UTF-8")
}

pub fn trimmed_canonical_export_text(text: &str) -> &str {
    text.trim()
}

pub fn assert_canonical_export_not_empty(trimmed: &str) {
    assert!(!trimmed.is_empty(), "canonical export should not be empty");
}

pub fn json_record_count(trimmed: &str) -> Option<usize> {
    let value = parse_json_record_container(trimmed)?;
    json_records(&value).map(record_count)
}

pub fn parse_json_record_container(trimmed: &str) -> Option<Value> {
    serde_json::from_str::<Value>(trimmed).ok()
}

pub fn json_records(value: &Value) -> Option<&[Value]> {
    value
        .as_array()
        .or_else(|| value.get("records").and_then(Value::as_array))
        .map(Vec::as_slice)
}

pub fn record_count(records: &[Value]) -> usize {
    records.len()
}

pub fn jsonl_record_count(trimmed: &str) -> usize {
    let records = canonical_jsonl_records(jsonl_lines(trimmed));
    assert_canonical_jsonl_records_valid(&records);
    jsonl_records_count(&records)
}

pub fn jsonl_lines(trimmed: &str) -> Vec<&str> {
    trimmed.lines().collect()
}

pub fn canonical_jsonl_records(lines: Vec<&str>) -> Vec<&str> {
    lines
        .into_iter()
        .filter(|line| canonical_jsonl_line_present(line))
        .collect()
}

pub fn canonical_jsonl_line_present(line: &str) -> bool {
    !line.trim().is_empty()
}

pub fn assert_canonical_jsonl_records_valid(records: &[&str]) {
    for record in records {
        assert_canonical_jsonl_line_valid(record);
    }
}

pub fn assert_canonical_jsonl_line_valid(line: &str) {
    serde_json::from_str::<Value>(line).expect("canonical JSONL record");
}

pub fn jsonl_records_count(records: &[&str]) -> usize {
    records.len()
}

pub struct SessionReplaceFixture {
    pub data_root: PathBuf,
    pub db_path: PathBuf,
    pub wal_path: PathBuf,
    pub before_db: String,
    pub before_wal: String,
}

impl SessionReplaceFixture {
    pub fn new() -> Self {
        let paths = session_replace_paths();
        write_session_replace_fixture(&paths);
        let hashes = session_replace_hashes(&paths);
        session_replace_fixture(paths, hashes)
    }

    pub fn host_override(&self) -> Value {
        json!({ "data_root": self.data_root.to_string_lossy() })
    }

    pub fn assert_unchanged(&self) {
        assert_file_unchanged(&self.db_path, &self.before_db, "opencode.db was mutated");
        assert_file_unchanged(
            &self.wal_path,
            &self.before_wal,
            "opencode.db-wal was mutated",
        );
    }
}

pub struct SessionReplacePaths {
    pub data_root: PathBuf,
    pub db_path: PathBuf,
    pub wal_path: PathBuf,
}

pub struct SessionReplaceHashes {
    pub before_db: String,
    pub before_wal: String,
}

pub fn session_replace_paths() -> SessionReplacePaths {
    session_replace_paths_for_root(unique_temp_dir(
        "agent-runner-opencode-contract-session-replace",
    ))
}

pub fn session_replace_paths_for_root(data_root: PathBuf) -> SessionReplacePaths {
    let db_path = data_root.join("opencode.db");
    let wal_path = data_root.join("opencode.db-wal");
    SessionReplacePaths {
        data_root,
        db_path,
        wal_path,
    }
}

pub fn write_session_replace_fixture(paths: &SessionReplacePaths) {
    fs::create_dir_all(&paths.data_root).expect("create fake opencode data root");
    write_session_replace_file(&paths.db_path, b"fake sqlite main db\n", "write fake db");
    write_session_replace_file(&paths.wal_path, b"fake sqlite wal\n", "write fake wal");
}

pub fn session_replace_hashes(paths: &SessionReplacePaths) -> SessionReplaceHashes {
    SessionReplaceHashes {
        before_db: file_sha256(&paths.db_path),
        before_wal: file_sha256(&paths.wal_path),
    }
}

pub fn session_replace_fixture(
    paths: SessionReplacePaths,
    hashes: SessionReplaceHashes,
) -> SessionReplaceFixture {
    SessionReplaceFixture {
        data_root: paths.data_root,
        db_path: paths.db_path,
        wal_path: paths.wal_path,
        before_db: hashes.before_db,
        before_wal: hashes.before_wal,
    }
}

impl Drop for SessionReplaceFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.data_root);
    }
}

pub fn write_session_replace_file(path: &Path, bytes: &[u8], message: &str) {
    fs::write(path, bytes).expect(message);
}

pub struct FakeOpencodeExport {
    pub dir: PathBuf,
}

impl FakeOpencodeExport {
    pub fn new(session_id: &str) -> Self {
        let paths = fake_opencode_export_paths();
        write_fake_opencode_export(&paths, session_id);
        fake_opencode_export_fixture(paths)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

pub struct FakeOpencodeExportPaths {
    pub dir: PathBuf,
    pub wrapper_path: PathBuf,
}

pub fn fake_opencode_export_paths() -> FakeOpencodeExportPaths {
    fake_opencode_export_paths_for_dir(unique_temp_dir(
        "agent-runner-opencode-contract-session-export",
    ))
}

pub fn fake_opencode_export_paths_for_dir(dir: PathBuf) -> FakeOpencodeExportPaths {
    let wrapper_path = dir.join("opencode1");
    FakeOpencodeExportPaths { dir, wrapper_path }
}

pub fn write_fake_opencode_export(paths: &FakeOpencodeExportPaths, session_id: &str) {
    fs::create_dir_all(&paths.dir).expect("create fake opencode dir");
    fs::write(&paths.wrapper_path, fake_opencode_export_script(session_id))
        .expect("write fake opencode1 export wrapper");
    make_fake_opencode_export_executable(&paths.wrapper_path);
}

pub fn fake_opencode_export_fixture(paths: FakeOpencodeExportPaths) -> FakeOpencodeExport {
    FakeOpencodeExport { dir: paths.dir }
}

#[cfg(unix)]
pub fn make_fake_opencode_export_executable(path: &Path) {
    let permissions = executable_permissions(fake_wrapper_permissions(path));
    fs::set_permissions(path, permissions).expect("chmod fake wrapper");
}

#[cfg(not(unix))]
pub fn make_fake_opencode_export_executable(_path: &Path) {}

#[cfg(unix)]
pub fn fake_wrapper_permissions(path: &Path) -> fs::Permissions {
    fs::metadata(path)
        .expect("fake wrapper metadata")
        .permissions()
}

#[cfg(unix)]
pub fn executable_permissions(mut permissions: fs::Permissions) -> fs::Permissions {
    permissions.set_mode(0o755);
    permissions
}

impl Drop for FakeOpencodeExport {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub struct FakeOpencodeSessionList {
    pub dir: PathBuf,
    pub log_path: PathBuf,
}

impl FakeOpencodeSessionList {
    pub fn with_output(stdout: &str, stderr: &str, exit_code: i32) -> Self {
        let paths = fake_opencode_session_list_paths();
        write_fake_opencode_session_list(&paths, stdout, stderr, exit_code);
        FakeOpencodeSessionList {
            dir: paths.dir,
            log_path: paths.log_path,
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }
}

impl Drop for FakeOpencodeSessionList {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub struct FakeOpencodeSessionListPaths {
    pub dir: PathBuf,
    pub wrapper_path: PathBuf,
    pub log_path: PathBuf,
}

pub fn fake_opencode_session_list_paths() -> FakeOpencodeSessionListPaths {
    fake_opencode_session_list_paths_for_dir(unique_temp_dir(
        "agent-runner-opencode-contract-session-list",
    ))
}

pub fn fake_opencode_session_list_paths_for_dir(dir: PathBuf) -> FakeOpencodeSessionListPaths {
    let wrapper_path = dir.join("opencode1");
    let log_path = dir.join("wrapper.log");
    FakeOpencodeSessionListPaths {
        dir,
        wrapper_path,
        log_path,
    }
}

pub fn write_fake_opencode_session_list(
    paths: &FakeOpencodeSessionListPaths,
    stdout: &str,
    stderr: &str,
    exit_code: i32,
) {
    fs::create_dir_all(&paths.dir).expect("create fake opencode session list dir");
    fs::write(
        &paths.wrapper_path,
        fake_opencode_session_list_script(stdout, stderr, exit_code, &paths.log_path),
    )
    .expect("write fake opencode1 session list wrapper");
    make_fake_opencode_export_executable(&paths.wrapper_path);
}

pub fn fake_opencode_session_list_script(
    stdout: &str,
    stderr: &str,
    exit_code: i32,
    log_path: &Path,
) -> String {
    format!(
        "#!/bin/sh\n\
{{\n\
  printf 'argv0=%s\\n' \"$0\"\n\
  for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
}} > {}\n\
if [ \"$1\" = \"session\" ] && [ \"${{2:-}}\" = \"list\" ]; then\n\
  printf '%s' {}\n\
  printf '%s' {} >&2\n\
  exit {}\n\
fi\n\
printf 'unsupported fake opencode invocation\\n' >&2\n\
exit 64\n",
        shell_single_quote(&path_string(log_path)),
        shell_single_quote(stdout),
        shell_single_quote(stderr),
        exit_code
    )
}

pub fn session_list_multiple_json() -> &'static str {
    r#"[
  {
    "id": "ses_list_one",
    "title": "First session",
    "directory": "/tmp/project-one",
    "created": 111,
    "updated": 222,
    "messageCount": 3
  },
  {
    "id": "ses_list_two",
    "title": null,
    "directory": "/var/tmp/project-two",
    "time": { "created": 333, "updated": 444 },
    "turn_count": 0
  }
]"#
}

pub fn session_list_bad_cwd_json() -> &'static str {
    r#"[
  {
    "id": "ses_relative_cwd",
    "title": "Relative cwd",
    "directory": "relative/path",
    "created": 111,
    "updated": 222
  },
  {
    "id": "ses_missing_cwd",
    "title": "Missing cwd",
    "created": 333,
    "updated": 444
  }
]"#
}

pub fn session_list_limit_json() -> &'static str {
    r#"[
  { "id": "ses_limit_one", "title": "One", "directory": "/tmp/one" },
  { "id": "ses_limit_two", "title": "Two", "directory": "/tmp/two" },
  { "id": "ses_limit_three", "title": "Three", "directory": "/tmp/three" }
]"#
}

pub fn fake_opencode_export_script(session_id: &str) -> String {
    format!(
        "#!/bin/sh\n\
if [ \"$1\" = \"export\" ] && [ \"${{2:-}}\" = {} ]; then\n\
  printf '%s' {}\n\
  exit 0\n\
fi\n\
if [ \"$1\" = \"export\" ]; then\n\
  printf 'session not found: %s\\n' \"${{2:-}}\" >&2\n\
  exit 2\n\
fi\n\
printf 'unsupported fake opencode invocation\\n' >&2\n\
exit 64\n",
        shell_single_quote(session_id),
        shell_single_quote(OPENCODE_EXPORT_RAW)
    )
}

pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(unique_temp_dir_name(prefix))
}

pub fn unique_temp_dir_name(prefix: &str) -> String {
    unique_temp_dir_name_from_parts(prefix, process_id(), current_time_nanos())
}

pub fn current_time_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos()
}

pub fn process_id() -> u32 {
    std::process::id()
}

pub fn unique_temp_dir_name_from_parts(prefix: &str, process_id: u32, nanos: u128) -> String {
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

pub fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub fn file_sha256(path: &Path) -> String {
    sha256_hex(&file_bytes(path))
}

pub fn file_bytes(path: &Path) -> Vec<u8> {
    fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex_digest(&sha256_digest(bytes))
}

pub fn sha256_digest(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}

pub fn hex_digest(digest: &[u8]) -> String {
    digest.iter().map(hex_byte).collect()
}

pub fn hex_byte(byte: &u8) -> String {
    format!("{byte:02x}")
}

pub fn encode_base64(bytes: &[u8]) -> String {
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

pub fn decode_base64(input: &str) -> Vec<u8> {
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
