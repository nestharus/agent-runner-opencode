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

pub struct FakeOpencodeDatabase {
    pub dir: PathBuf,
    pub db_path: PathBuf,
    pub log_path: PathBuf,
    session_id: String,
}

impl FakeOpencodeDatabase {
    pub fn new(session_id: &str) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-contract-session-db");
        let db_path = dir.join("opencode.db");
        let log_path = dir.join("wrapper.log");
        let wrapper_path = dir.join("opencode1");
        fs::create_dir_all(&dir).expect("create fake opencode database dir");
        let directory = native_export_fixture()["info"]["directory"]
            .as_str()
            .expect("native fixture directory")
            .to_string();
        run_sqlite(
            &db_path,
            &format!(
                "BEGIN;
                 CREATE TABLE session (
                   id TEXT PRIMARY KEY, project_id TEXT, directory TEXT NOT NULL, title TEXT,
                   version TEXT, time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL,
                   time_archived INTEGER
                 );
                 CREATE TABLE message (
                   id TEXT PRIMARY KEY, session_id TEXT NOT NULL, time_created INTEGER NOT NULL,
                   time_updated INTEGER NOT NULL, data TEXT NOT NULL
                 );
                 CREATE TABLE part (
                   id TEXT PRIMARY KEY, message_id TEXT NOT NULL, session_id TEXT NOT NULL,
                   time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL, data TEXT NOT NULL
                 );
                 CREATE INDEX message_session_time_created_id_idx
                   ON message(session_id,time_created,id);
                 CREATE INDEX part_message_id_id_idx ON part(message_id,id);
                 INSERT INTO session(id,project_id,directory,title,version,time_created,time_updated)
                   VALUES ({},'project-fixture',{},'Paging fixture','1.18.25',1000,4000);
                 COMMIT;",
                sqlite_string(session_id),
                sqlite_string(&directory),
            ),
        );
        let oversized_text = "x".repeat(4096);
        for (id, created, role, completed, parent, text) in [
            ("msg_001", 1000, "user", true, None, "first user"),
            (
                "msg_002",
                2000,
                "assistant",
                true,
                Some("msg_001"),
                "first assistant",
            ),
            (
                "msg_003",
                3000,
                "user",
                true,
                Some("msg_002"),
                oversized_text.as_str(),
            ),
            (
                "msg_004",
                4000,
                "assistant",
                false,
                Some("msg_003"),
                "mutable trailing assistant",
            ),
        ] {
            insert_database_message(
                &db_path,
                session_id,
                DatabaseMessage {
                    id,
                    created,
                    role,
                    completed,
                    parent,
                    text,
                },
            );
        }
        fs::write(
            &wrapper_path,
            fake_opencode_database_script(&db_path, &log_path, None),
        )
        .expect("write fake opencode database wrapper");
        make_fake_opencode_export_executable(&wrapper_path);
        crate::support::write_fake_opencode_dispatcher(&dir);
        Self {
            dir,
            db_path,
            log_path,
            session_id: session_id.to_string(),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn append_user(&self, id: &str, created: i64, text: &str) {
        insert_database_message(
            &self.db_path,
            &self.session_id,
            DatabaseMessage {
                id,
                created,
                role: "user",
                completed: true,
                parent: Some("msg_003"),
                text,
            },
        );
    }

    pub fn insert_incomplete_assistant(&self, id: &str, created: i64, parent: &str, text: &str) {
        insert_database_message(
            &self.db_path,
            &self.session_id,
            DatabaseMessage {
                id,
                created,
                role: "assistant",
                completed: false,
                parent: Some(parent),
                text,
            },
        );
    }

    pub fn update_message(&self, id: &str) {
        run_sqlite(
            &self.db_path,
            &format!(
                "UPDATE message SET data=json_set(data,'$.parentID','mutated-parent'), \
                 time_updated=time_updated+100 WHERE id={};",
                sqlite_string(id),
            ),
        );
    }

    pub fn delete_message(&self, id: &str) {
        run_sqlite(
            &self.db_path,
            &format!(
                "DELETE FROM part WHERE message_id={}; DELETE FROM message WHERE id={};",
                sqlite_string(id),
                sqlite_string(id),
            ),
        );
    }

    pub fn update_text_part(&self, message_id: &str) {
        run_sqlite(
            &self.db_path,
            &format!(
                "UPDATE part SET data=json_set(data,'$.text','mutated text'), \
                 time_updated=time_updated+100 WHERE id={};",
                sqlite_string(&format!("part_{message_id}_text")),
            ),
        );
    }

    pub fn remove_trailing_assistant(&self) {
        run_sqlite(
            &self.db_path,
            "DELETE FROM part WHERE message_id='msg_004'; DELETE FROM message WHERE id='msg_004';",
        );
    }

    pub fn clear_messages(&self) {
        run_sqlite(&self.db_path, "DELETE FROM part; DELETE FROM message;");
    }

    pub fn remove_required_message_index(&self) {
        run_sqlite(
            &self.db_path,
            "DROP INDEX message_session_time_created_id_idx;",
        );
    }

    pub fn emit_bracketed_database_prefix(&self) {
        let wrapper_path = self.dir.join("opencode1");
        fs::write(
            &wrapper_path,
            fake_opencode_database_script(
                &self.db_path,
                &self.log_path,
                Some("[native startup notice]"),
            ),
        )
        .expect("replace fake opencode database wrapper");
        make_fake_opencode_export_executable(&wrapper_path);
    }

    pub fn replace_source_file(&self) {
        let replacement = self.dir.join("replacement.db");
        fs::copy(&self.db_path, &replacement).expect("copy replacement database");
        fs::rename(&replacement, &self.db_path).expect("replace database inode");
    }

    pub fn assert_no_export(&self) {
        let log = fs::read_to_string(&self.log_path).expect("read database wrapper log");
        assert!(
            log.contains("arg=db"),
            "database command was not used: {log}"
        );
        assert!(
            !log.contains("arg=export"),
            "session paging/capture must never invoke export: {log}"
        );
    }
}

impl Drop for FakeOpencodeDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

struct DatabaseMessage<'a> {
    id: &'a str,
    created: i64,
    role: &'a str,
    completed: bool,
    parent: Option<&'a str>,
    text: &'a str,
}

fn insert_database_message(db_path: &Path, session_id: &str, message_fixture: DatabaseMessage<'_>) {
    let DatabaseMessage {
        id,
        created,
        role,
        completed,
        parent,
        text,
    } = message_fixture;
    let mut message = json!({
        "id": id,
        "sessionID": session_id,
        "role": role,
        "time": { "created": created },
    });
    if completed {
        message["time"]["completed"] = json!(created + 1);
    }
    if let Some(parent) = parent {
        message["parentID"] = json!(parent);
    }
    let text_part = format!(
        "{{\"type\":\"text\",\"id\":{},\"messageID\":{},\"sessionID\":{},\"text\":{}}}",
        serde_json::to_string(&format!("part_{id}_text")).expect("serialize part id"),
        serde_json::to_string(id).expect("serialize message id"),
        serde_json::to_string(session_id).expect("serialize session id"),
        serde_json::to_string(text).expect("serialize part text"),
    );
    let mut sql = format!(
        "BEGIN;
         INSERT INTO message(id,session_id,time_created,time_updated,data)
           VALUES ({},{},{},{},{});
         INSERT INTO part(id,message_id,session_id,time_created,time_updated,data)
           VALUES ({},{},{},{},{},{});",
        sqlite_string(id),
        sqlite_string(session_id),
        created,
        created + 1,
        sqlite_string(&message.to_string()),
        sqlite_string(&format!("part_{id}_text")),
        sqlite_string(id),
        sqlite_string(session_id),
        created,
        created + 1,
        sqlite_string(&text_part),
    );
    if role == "assistant" {
        for (suffix, part_type) in [("start", "step-start"), ("finish", "step-finish")] {
            let part = json!({
                "id": format!("part_{id}_{suffix}"),
                "messageID": id,
                "sessionID": session_id,
                "type": part_type,
            });
            sql.push_str(&format!(
                "INSERT INTO part(id,message_id,session_id,time_created,time_updated,data)
                   VALUES ({},{},{},{},{},{});",
                sqlite_string(&format!("part_{id}_{suffix}")),
                sqlite_string(id),
                sqlite_string(session_id),
                created,
                created + 1,
                sqlite_string(&part.to_string()),
            ));
        }
    }
    sql.push_str("COMMIT;");
    run_sqlite(db_path, &sql);
}

fn run_sqlite(db_path: &Path, sql: &str) {
    let output = Command::new("/usr/bin/sqlite3")
        .arg(db_path)
        .arg(sql)
        .output()
        .expect("run sqlite3 fixture command");
    assert!(
        output.status.success(),
        "sqlite fixture command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn sqlite_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn fake_opencode_database_script(
    db_path: &Path,
    log_path: &Path,
    database_prefix: Option<&str>,
) -> String {
    let database_prefix = database_prefix
        .map(|prefix| format!("printf '%s\\n' {}\n", shell_single_quote(prefix)))
        .unwrap_or_default();
    format!(
        "#!/bin/sh\n\
{{\n\
  printf 'invocation\\n'\n\
  for arg in \"$@\"; do printf 'arg=%s\\n' \"$arg\"; done\n\
}} >> {}\n\
if [ \"$1\" = \"db\" ] && [ \"${{2:-}}\" = \"path\" ]; then\n\
  printf '%s\\n' {}\n\
  exit 0\n\
fi\n\
if [ \"$1\" = \"db\" ] && [ \"${{3:-}}\" = \"--format\" ] && [ \"${{4:-}}\" = \"json\" ]; then\n\
  {}  exec /usr/bin/sqlite3 -json {} \"$2\"\n\
fi\n\
if [ \"$1\" = \"export\" ]; then\n\
  printf 'export is forbidden for this fixture\\n' >&2\n\
  exit 97\n\
fi\n\
printf 'unsupported fake opencode database invocation\\n' >&2\n\
exit 64\n",
        shell_single_quote(&path_string(log_path)),
        shell_single_quote(&path_string(db_path)),
        database_prefix,
        shell_single_quote(&path_string(db_path)),
    )
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
        crate::support::write_fake_opencode_dispatcher(&dir);
        Self { dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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
        crate::support::write_fake_opencode_dispatcher(&dir);
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
