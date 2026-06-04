#[allow(dead_code)]
mod support;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use support::{assert_valid, invoke, invoke_with_env, invoke_with_host_and_env, json_stdout};

const CANONICAL_FORMAT: &str = "oulipoly.canonical_transcript/v1";
const OPENCODE_EXPORT_RAW: &str = include_str!("fixtures/opencode_export.json");

#[test]
fn characterization_opencode_session_export_json() {
    let export = native_export_fixture();
    let info = export["info"].as_object().expect("export.info object");
    let session_id = info["id"].as_str().expect("info.id string");
    assert!(
        session_id.starts_with("ses_"),
        "unexpected session id {session_id}"
    );
    assert!(
        info["title"]
            .as_str()
            .is_some_and(|title| !title.is_empty()),
        "info.title should be a non-empty native opencode title"
    );

    let messages = export["messages"].as_array().expect("messages array");
    assert!(
        !messages.is_empty(),
        "native export should include messages"
    );

    let mut part_types = BTreeSet::new();
    for message in messages {
        let role = message["info"]["role"]
            .as_str()
            .expect("message.info.role string");
        assert!(
            matches!(role, "user" | "assistant"),
            "unexpected native message role {role}"
        );
        assert_eq!(
            message["info"]["sessionID"].as_str(),
            Some(session_id),
            "message sessionID should match export info.id"
        );

        let parts = message["parts"].as_array().expect("message.parts array");
        assert!(!parts.is_empty(), "native message should include parts");
        for part in parts {
            let part_type = part["type"].as_str().expect("part.type string");
            part_types.insert(part_type.to_owned());
            assert_eq!(
                part["sessionID"].as_str(),
                Some(session_id),
                "part sessionID should match export info.id"
            );
        }
    }

    for expected in ["step-start", "text", "step-finish"] {
        assert!(
            part_types.contains(expected),
            "native export should include a {expected} part; saw {part_types:?}"
        );
    }
    assert!(
        export.get("contract").is_none(),
        "native opencode export is source material, not a provider contract envelope"
    );
}

#[test]
fn contract_session_read_turns() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let params = session_params(session_id);

    let result = success_result(
        invoke_with_env(
            "session.read_turns",
            params.clone(),
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    );
    assert_eq!(
        result["turn_count"].as_u64(),
        Some(fixture_message_count() as u64),
        "turn_count should match the native opencode export message count"
    );
    assert!(result["complete"].is_boolean(), "complete should be a bool");
    assert_eq!(
        result["turns"].as_array().expect("turns array").len(),
        fixture_message_count(),
        "turns length should match turn_count and native message count"
    );
    let first_ids = turn_ids(&result);
    assert_eq!(first_ids.len(), fixture_message_count());

    let second = success_result(
        invoke_with_env("session.read_turns", params, &[("PATH", path.as_str())]),
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    );
    assert_eq!(
        turn_ids(&second),
        first_ids,
        "turn ids must be stable across repeated reads of the same opencode export"
    );

    let missing = invoke_with_env(
        "session.read_turns",
        session_params("ses_missing_contract_cluster_b"),
        &[("PATH", path.as_str())],
    );
    assert_error_envelope(missing);
}

#[test]
fn contract_session_export_canonical() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let params = session_params(session_id);

    let first = success_result(
        invoke_with_env("session.export", params.clone(), &[("PATH", path.as_str())]),
        "session.schema.json#/$defs/SessionExportResponse",
        "session.schema.json#/$defs/SessionExportResult",
    );
    assert_eq!(first["canonical_format"], CANONICAL_FORMAT);
    let decoded = decode_base64(first["data_base64"].as_str().expect("data_base64 string"));
    assert_eq!(
        sha256_hex(&decoded),
        first["sha256"].as_str().expect("sha256 string"),
        "sha256 must be computed over decoded data_base64 bytes"
    );
    assert_eq!(
        canonical_record_count(&decoded),
        first["turn_count"].as_u64().expect("turn_count integer") as usize,
        "canonical record count must match turn_count"
    );

    let second = success_result(
        invoke_with_env("session.export", params, &[("PATH", path.as_str())]),
        "session.schema.json#/$defs/SessionExportResponse",
        "session.schema.json#/$defs/SessionExportResult",
    );
    assert_eq!(
        second["data_base64"], first["data_base64"],
        "canonical export bytes must be deterministic for the same native export"
    );
    assert_eq!(
        second["sha256"], first["sha256"],
        "canonical export sha256 must be deterministic for the same native export"
    );
}

#[test]
#[ignore = "live opencode auth/network session export proof; run explicitly when external dependencies are available"]
fn integration_session_export_live() {
    let session_id = live_opencode_session_id();
    let result = success_result(
        invoke("session.export", session_params(&session_id)),
        "session.schema.json#/$defs/SessionExportResponse",
        "session.schema.json#/$defs/SessionExportResult",
    );

    assert_eq!(result["canonical_format"], CANONICAL_FORMAT);
    let decoded = decode_base64(result["data_base64"].as_str().expect("data_base64 string"));
    assert_eq!(
        sha256_hex(&decoded),
        result["sha256"].as_str().expect("sha256 string"),
        "live session.export sha256 must be computed over decoded data_base64 bytes"
    );
    assert_eq!(
        canonical_record_count(&decoded),
        result["turn_count"].as_u64().expect("turn_count integer") as usize,
        "live canonical record count must match turn_count"
    );
}

#[test]
fn contract_session_capture() {
    let session_id = fixture_session_id();
    let result = success_result(
        invoke_with_env(
            "session.capture",
            json!({
                "settings_id": "opencode1",
                "session_id": session_id,
                "launch": {
                    "sessionID": session_id,
                    "session_id": session_id,
                    "source": "launch.exit.session"
                },
                "evidence": {
                    "provider_session_id": session_id,
                    "sessionID": session_id
                }
            }),
            &[],
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_eq!(
        result["provider_session_id"].as_str(),
        Some(session_id),
        "capture should preserve the launch-derived opencode sessionID"
    );
    let artifacts = result["artifacts"].as_array().expect("artifacts array");
    assert!(
        !artifacts.is_empty(),
        "capture should return source artifacts"
    );
    for artifact in artifacts {
        if let Some(path) = artifact.get("path").and_then(Value::as_str) {
            assert!(
                !path.contains("opencode.db") && !path.contains(".opencode"),
                "capture artifacts should avoid private DB path assumptions: {artifact}"
            );
        }
    }
}

#[test]
fn contract_session_locate_not_located() {
    let session_id = fixture_session_id();
    let result = success_result(
        invoke_with_env("session.locate_transcript", session_params(session_id), &[]),
        "session.schema.json#/$defs/SessionLocateTranscriptResponse",
        "session.schema.json#/$defs/SessionLocateTranscriptResult",
    );
    assert_eq!(result["located"], false);
    assert!(
        result["format_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "not-located response should still identify the transcript/export format"
    );
    assert!(
        result["source_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "not-located response should still identify the opencode source"
    );
    assert!(
        result.get("path").is_none(),
        "opencode has no transcript file, so locate_transcript must omit path"
    );
}

#[test]
fn contract_session_replace_unsupported() {
    let session_id = fixture_session_id();
    let data_root = unique_temp_dir("agent-runner-opencode-contract-session-replace");
    fs::create_dir_all(&data_root).expect("create fake opencode data root");
    let db_path = data_root.join("opencode.db");
    let wal_path = data_root.join("opencode.db-wal");
    fs::write(&db_path, b"fake sqlite main db\n").expect("write fake db");
    fs::write(&wal_path, b"fake sqlite wal\n").expect("write fake wal");
    let before_db = file_sha256(&db_path);
    let before_wal = file_sha256(&wal_path);

    let output = invoke_with_host_and_env(
        "session.replace",
        json!({
            "settings_id": "opencode1",
            "session_id": session_id,
            "canonical_format": CANONICAL_FORMAT,
            "data_base64": encode_base64(b"{\"role\":\"user\",\"text\":\"replacement\"}\n"),
            "sha256": sha256_hex(b"{\"role\":\"user\",\"text\":\"replacement\"}\n"),
            "turn_count": 1
        }),
        json!({ "data_root": data_root.to_string_lossy() }),
        &[],
    );

    let response = json_stdout(&output);
    if response["ok"] == false {
        assert_valid(&response, "common.schema.json#/$defs/ErrorResponseEnvelope");
        assert_eq!(
            response["error"]["category"], "unsupported",
            "session.replace should be honestly unsupported rather than mutating opencode storage"
        );
    } else {
        assert!(
            output.status.success(),
            "successful session.replace envelope should exit zero; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_valid(
            &response,
            "session.schema.json#/$defs/SessionReplaceResponse",
        );
        assert_valid(
            &response["result"],
            "session.schema.json#/$defs/SessionReplaceResult",
        );
        assert_eq!(response["result"]["changed"], false);
        assert_eq!(
            response["result"]["artifacts"]
                .as_array()
                .expect("artifacts array")
                .len(),
            0,
            "changed=false replace fallback should not report storage artifacts"
        );
    }

    assert_eq!(file_sha256(&db_path), before_db, "opencode.db was mutated");
    assert_eq!(
        file_sha256(&wal_path),
        before_wal,
        "opencode.db-wal was mutated"
    );
    fs::remove_dir_all(&data_root).expect("remove fake opencode data root");
}

fn session_params(session_id: &str) -> Value {
    json!({
        "settings_id": "opencode1",
        "session_id": session_id
    })
}

fn native_export_fixture() -> Value {
    parse_native_export(OPENCODE_EXPORT_RAW)
}

fn parse_native_export(raw: &str) -> Value {
    let json_start = raw
        .find('{')
        .expect("opencode export fixture should contain a JSON object");
    serde_json::from_str(&raw[json_start..]).expect("opencode export JSON body should parse")
}

fn live_opencode_session_id() -> String {
    let output = Command::new("opencode1")
        .arg("run")
        .arg("--format")
        .arg("json")
        .arg("-m")
        .arg("openai/gpt-5.5")
        .arg("--variant")
        .arg("low")
        .arg("reply with the single word: ok")
        .output()
        .expect("spawn live opencode1 run");
    assert!(
        output.status.success(),
        "live opencode1 run failed; exit {:?}; stderr: {}; stdout: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|event| event["sessionID"].as_str().map(str::to_owned))
        .unwrap_or_else(|| panic!("live opencode1 run did not emit sessionID; stdout: {stdout}"))
}

fn fixture_session_id() -> &'static str {
    let session_id = native_export_fixture()["info"]["id"]
        .as_str()
        .expect("fixture info.id")
        .to_owned();
    Box::leak(session_id.into_boxed_str())
}

fn fixture_message_count() -> usize {
    native_export_fixture()["messages"]
        .as_array()
        .expect("fixture messages array")
        .len()
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

fn assert_error_envelope(output: std::process::Output) -> Value {
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

fn turn_ids(result: &Value) -> Vec<String> {
    result["turns"]
        .as_array()
        .expect("turns array")
        .iter()
        .map(|turn| {
            turn["id"]
                .as_str()
                .unwrap_or_else(|| panic!("turn must have stable string id: {turn}"))
                .to_owned()
        })
        .collect()
}

fn canonical_record_count(bytes: &[u8]) -> usize {
    let text = std::str::from_utf8(bytes).expect("canonical export should be UTF-8");
    let trimmed = text.trim();
    assert!(!trimmed.is_empty(), "canonical export should not be empty");

    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(records) = value.as_array() {
            return records.len();
        }
        if let Some(records) = value.get("records").and_then(Value::as_array) {
            return records.len();
        }
    }

    trimmed
        .lines()
        .filter(|line| {
            if line.trim().is_empty() {
                return false;
            }
            serde_json::from_str::<Value>(line).expect("canonical JSONL record");
            true
        })
        .count()
}

struct FakeOpencodeExport {
    dir: PathBuf,
}

impl FakeOpencodeExport {
    fn new(session_id: &str) -> Self {
        let dir = unique_temp_dir("agent-runner-opencode-contract-session-export");
        fs::create_dir_all(&dir).expect("create fake opencode dir");
        let wrapper_path = dir.join("opencode1");
        fs::write(&wrapper_path, fake_opencode_export_script(session_id))
            .expect("write fake opencode1 export wrapper");
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&wrapper_path)
                .expect("fake wrapper metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&wrapper_path, permissions).expect("chmod fake wrapper");
        }
        Self { dir }
    }

    fn dir(&self) -> &Path {
        &self.dir
    }
}

impl Drop for FakeOpencodeExport {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn fake_opencode_export_script(session_id: &str) -> String {
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
    sha256_hex(&bytes)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
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
