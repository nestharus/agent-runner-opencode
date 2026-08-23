// declared_role: parser, filter, mapper, accessor, validator, predicate, formatter
#![allow(unused_imports)]

use super::*;

pub fn launch_events_from_output(output: &std::process::Output, label: &str) -> Vec<Value> {
    let events = parse_launch_events(&output.stdout);
    assert_launch_events_not_empty(&events, label);
    events
}

pub fn final_launch_event(events: &[Value]) -> &Value {
    events.last().expect("final event")
}

pub fn expected_provider_exit_code(final_event: &Value) -> Option<i32> {
    let status = &final_event["status"];
    Some(match status["kind"].as_str().expect("status.kind") {
        "exited" => status["code"].as_i64().expect("status.code") as i32,
        "signal_terminated" => 128 + status["signal"].as_i64().expect("status.signal") as i32,
        "prolonged_silence" => 124,
        "cancelled" => 130,
        "spawn_error" | "unknown" => 1,
        other => panic!("unexpected ProcessStatus kind {other}"),
    })
}

pub fn parse_launch_events(stdout: &[u8]) -> Vec<Value> {
    std::str::from_utf8(stdout)
        .expect("launch stdout should be UTF-8 NDJSON")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
        .map(|(index, line)| {
            let line_number = index + 1;
            let event = serde_json::from_str(line).unwrap_or_else(|err| {
                panic!("launch stdout line {line_number} invalid JSON: {err}\n{line}")
            });
            assert_valid_launch_event(line_number, &event);
            event
        })
        .collect()
}

pub fn launch_event_schema_id(line_number: usize, event: &Value) -> &'static str {
    match event["kind"].as_str() {
        Some("stdout") => "launch.schema.json#/$defs/LaunchStdoutEvent",
        Some("stderr") => "launch.schema.json#/$defs/LaunchStderrEvent",
        Some("marker") => "launch.schema.json#/$defs/LaunchMarkerEvent",
        Some("heartbeat") => "launch.schema.json#/$defs/LaunchHeartbeatEvent",
        Some("exit") => "launch.schema.json#/$defs/LaunchExitEvent",
        other => {
            panic!("launch stdout line {line_number} has unknown event kind {other:?}: {event}")
        }
    }
}

pub fn collect_stream_bytes(events: &[Value], kind: &str) -> Vec<u8> {
    events
        .iter()
        .filter(|event| event["kind"] == kind)
        .flat_map(|event| {
            let encoded = event["data_base64"]
                .as_str()
                .unwrap_or_else(|| panic!("{kind} event data_base64 must be a string"));
            let decoded = decode_base64(encoded);
            assert_base64_round_trip(kind, &decoded);
            decoded
        })
        .collect()
}

pub fn json_contains_string(value: &Value, needle: &str) -> bool {
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
