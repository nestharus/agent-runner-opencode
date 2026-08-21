// declared_role: orchestration, parser, formatter, accessor, mapper, filter, predicate, validator
#![allow(unused_imports)]

use super::*;

pub const OPENCODE_EXPORT_RAW: &str = include_str!("../fixtures/opencode_export.json");

pub fn replacement_record_bytes() -> &'static [u8] {
    b"{\"role\":\"user\",\"text\":\"replacement\"}\n"
}

pub fn native_export_fixture() -> Value {
    let json_start = OPENCODE_EXPORT_RAW
        .find('{')
        .expect("opencode export fixture should contain a JSON object");
    serde_json::from_str(&OPENCODE_EXPORT_RAW[json_start..])
        .expect("opencode export JSON body should parse")
}

pub fn live_opencode_session_id() -> String {
    let output = Command::new("opencode5")
        .args([
            "run",
            "--format",
            "json",
            "-m",
            "openai/gpt-5.6-luna",
            "--variant",
            "low",
            "reply with the single word: ok",
        ])
        .output()
        .expect("spawn live opencode5 Luna run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    if let Some(session_id) = stdout.lines().find_map(|line| {
        serde_json::from_str::<Value>(line)
            .ok()
            .and_then(|event| event["sessionID"].as_str().map(str::to_owned))
    }) {
        return session_id;
    }
    assert!(
        output.status.success(),
        "live opencode5 Luna low run failed; exit {:?}; stderr: {}; stdout: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
        stdout
    );
    panic!("live opencode5 Luna run did not emit sessionID; stdout: {stdout}")
}

pub fn fixture_session_id() -> &'static str {
    Box::leak(
        native_export_fixture()["info"]["id"]
            .as_str()
            .expect("fixture info.id")
            .to_owned()
            .into_boxed_str(),
    )
}

pub fn fixture_message_count() -> usize {
    native_export_fixture()["messages"]
        .as_array()
        .expect("fixture messages array")
        .len()
}

pub fn success_result(
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

pub fn assert_error_envelope(output: std::process::Output) -> Value {
    assert!(
        !output.status.success(),
        "expected nonzero error envelope; stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let response = json_stdout(&output);
    assert_valid(&response, "common.schema.json#/$defs/ErrorResponseEnvelope");
    assert_eq!(response["ok"], false);
    response
}

pub fn turn_ids(result: &Value) -> Vec<String> {
    turns(result)
        .iter()
        .map(|turn| {
            turn["turn_id"]
                .as_str()
                .unwrap_or_else(|| panic!("turn must have stable string id: {turn}"))
                .to_owned()
        })
        .collect()
}

pub fn turns(result: &Value) -> &[Value] {
    result["turns"].as_array().expect("turns array")
}

pub fn canonical_record_count(bytes: &[u8]) -> usize {
    let text = std::str::from_utf8(bytes).expect("canonical export should be UTF-8");
    let trimmed = text.trim();
    assert!(!trimmed.is_empty(), "canonical export should not be empty");
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(records) = value
            .as_array()
            .or_else(|| value.get("records").and_then(Value::as_array))
        {
            return records.len();
        }
    }
    trimmed
        .lines()
        .filter(|line| !line.trim().is_empty())
        .inspect(|line| {
            serde_json::from_str::<Value>(line).expect("canonical JSONL record");
        })
        .count()
}

pub fn canonical_result_decoded_bytes(result: &Value) -> Vec<u8> {
    decode_base64(result["data_base64"].as_str().expect("data_base64 string"))
}

pub fn canonical_result_sha(result: &Value) -> &str {
    result["sha256"].as_str().expect("sha256 string")
}

pub fn canonical_bytes_sha(bytes: &[u8]) -> String {
    sha256_hex(bytes)
}

pub fn canonical_result_turn_count(result: &Value) -> usize {
    result["turn_count"].as_u64().expect("turn_count integer") as usize
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
        let data_root = unique_temp_dir("agent-runner-opencode-contract-session-replace");
        let db_path = data_root.join("opencode.db");
        let wal_path = data_root.join("opencode.db-wal");
        fs::create_dir_all(&data_root).expect("create fake opencode data root");
        fs::write(&db_path, b"fake sqlite main db\n").expect("write fake db");
        fs::write(&wal_path, b"fake sqlite wal\n").expect("write fake wal");
        Self {
            data_root,
            before_db: file_sha256(&db_path),
            before_wal: file_sha256(&wal_path),
            db_path,
            wal_path,
        }
    }

    pub fn host_override(&self) -> Value {
        json!({ "data_root": self.data_root.to_string_lossy() })
    }

    pub fn assert_unchanged(&self) {
        assert_eq!(
            file_sha256(&self.db_path),
            self.before_db,
            "opencode.db was mutated"
        );
        assert_eq!(
            file_sha256(&self.wal_path),
            self.before_wal,
            "opencode.db-wal was mutated"
        );
    }
}

impl Drop for SessionReplaceFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.data_root);
    }
}

pub struct FakeOpencodeExport {
    pub dir: PathBuf,
}

impl FakeOpencodeExport {
    pub fn new(session_id: &str) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-contract-session-export");
        let wrapper_path = dir.join("opencode1");
        fs::create_dir_all(&dir).expect("create fake opencode dir");
        fs::write(&wrapper_path, fake_opencode_export_script(session_id))
            .expect("write fake opencode1 export wrapper");
        make_fake_opencode_export_executable(&wrapper_path);
        Self { dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn pipe_truncating(session_id: &str) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-contract-session-export");
        let wrapper_path = dir.join("opencode1");
        fs::create_dir_all(&dir).expect("create pipe-sensitive fake opencode dir");
        fs::write(
            &wrapper_path,
            pipe_truncating_opencode_export_script(session_id),
        )
        .expect("write pipe-sensitive fake opencode1 export wrapper");
        make_fake_opencode_export_executable(&wrapper_path);
        Self { dir }
    }
}

#[cfg(unix)]
pub fn make_fake_opencode_export_executable(path: &Path) {
    let mut permissions = fs::metadata(path)
        .expect("fake wrapper metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod fake wrapper");
}

#[cfg(not(unix))]
pub fn make_fake_opencode_export_executable(_path: &Path) {}

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
        let dir = unique_temp_dir("agent-runner-opencode-contract-session-list");
        let wrapper_path = dir.join("opencode1");
        let log_path = dir.join("wrapper.log");
        fs::create_dir_all(&dir).expect("create fake opencode session list dir");
        fs::write(
            &wrapper_path,
            fake_opencode_session_list_script(stdout, stderr, exit_code, &log_path),
        )
        .expect("write fake opencode1 session list wrapper");
        make_fake_opencode_export_executable(&wrapper_path);
        Self { dir, log_path }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    pub fn replace_output(&self, stdout: &str, stderr: &str, exit_code: i32) {
        let wrapper_path = self.dir.join("opencode1");
        fs::write(
            &wrapper_path,
            fake_opencode_session_list_script(stdout, stderr, exit_code, &self.log_path),
        )
        .expect("replace fake opencode1 session list wrapper");
        make_fake_opencode_export_executable(&wrapper_path);
    }
}

impl Drop for FakeOpencodeSessionList {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
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

pub fn changed_session_list_limit_json() -> &'static str {
    r#"[
  { "id": "ses_changed_one", "title": "Changed one", "directory": "/tmp/changed-one" },
  { "id": "ses_changed_two", "title": "Changed two", "directory": "/tmp/changed-two" },
  { "id": "ses_changed_three", "title": "Changed three", "directory": "/tmp/changed-three" }
]"#
}

pub fn session_list_initial_replay_json() -> &'static str {
    r#"[
  { "id": "ses_replay_one", "title": "Replay one", "directory": "relative/replay-one" },
  { "id": "ses_replay_two", "title": "Replay two", "directory": "/tmp/replay-two" },
  { "id": "ses_replay_three", "title": "Replay three", "directory": "/tmp/replay-three" }
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

pub fn pipe_truncating_opencode_export_script(session_id: &str) -> String {
    format!(
        "#!/bin/sh\n\
if [ \"$1\" = \"export\" ] && [ \"${{2:-}}\" = {} ]; then\n\
  if [ -p /dev/stdout ]; then\n\
    printf '%s' '{{\"info\":{{\"id\":\"truncated\"}}'\n\
    exit 0\n\
  fi\n\
  printf '%s' {}\n\
  exit 0\n\
fi\n\
printf 'unsupported fake opencode invocation\n' >&2\n\
exit 64\n",
        shell_single_quote(session_id),
        shell_single_quote(OPENCODE_EXPORT_RAW)
    )
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

pub fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub fn file_sha256(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    sha256_hex(&bytes)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
