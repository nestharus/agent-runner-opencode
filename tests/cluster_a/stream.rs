// declared_role: parser, filter, mapper, accessor, validator, predicate, formatter
#![allow(unused_imports)]

use super::*;

pub fn launch_events_from_output(output: &std::process::Output, label: &str) -> Vec<Value> {
    parse_non_empty_launch_events(&output.stdout, label)
}

pub fn parse_non_empty_launch_events(stdout: &[u8], label: &str) -> Vec<Value> {
    let events = parse_launch_events(stdout);
    assert_launch_events_not_empty(&events, label);
    events
}

pub fn final_launch_event(events: &[Value]) -> &Value {
    events.last().expect("final event")
}

pub fn expected_provider_exit_code(final_event: &Value) -> Option<i32> {
    Some(expected_exit_code_for_status(&final_event["status"]))
}

pub fn parse_launch_events(stdout: &[u8]) -> Vec<Value> {
    non_empty_launch_stdout_lines(stdout)
        .into_iter()
        .enumerate()
        .map(|(index, line)| parse_valid_launch_event(index + 1, line))
        .collect()
}

pub fn parse_valid_launch_event(line_number: usize, line: &str) -> Value {
    let event = parse_launch_event_line(line_number, line);
    assert_valid_launch_event(line_number, &event);
    event
}

pub fn parse_launch_event_line(line_number: usize, line: &str) -> Value {
    serde_json::from_str(line).unwrap_or_else(|err| {
        panic!("launch stdout line {line_number} invalid JSON: {err}\n{line}")
    })
}

pub fn launch_event_schema_id(line_number: usize, event: &Value) -> &'static str {
    schema_id_for_launch_event_kind(line_number, event, launch_event_kind(event))
}

pub fn launch_event_kind(event: &Value) -> Option<&str> {
    event["kind"].as_str()
}

pub fn schema_id_for_launch_event_kind(
    line_number: usize,
    event: &Value,
    kind: Option<&str>,
) -> &'static str {
    match kind {
        Some("stdout") => "launch.schema.json#/$defs/LaunchStdoutEvent",
        Some("stderr") => "launch.schema.json#/$defs/LaunchStderrEvent",
        Some("marker") => "launch.schema.json#/$defs/LaunchMarkerEvent",
        Some("heartbeat") => "launch.schema.json#/$defs/LaunchHeartbeatEvent",
        Some("exit") => "launch.schema.json#/$defs/LaunchExitEvent",
        other => unknown_launch_event_schema_id(line_number, event, other),
    }
}

pub fn unknown_launch_event_schema_id(line_number: usize, event: &Value, other: Option<&str>) -> ! {
    panic!("launch stdout line {line_number} has unknown event kind {other:?}: {event}")
}

pub fn collect_stream_bytes(events: &[Value], kind: &str) -> Vec<u8> {
    flatten_stream_byte_chunks(stream_byte_chunks(stream_events(events, kind), kind))
}

pub fn stream_events<'a>(events: &'a [Value], kind: &str) -> Vec<&'a Value> {
    events
        .iter()
        .filter(|event| launch_event_kind_is(event, kind))
        .collect()
}

pub fn stream_byte_chunks(events: Vec<&Value>, kind: &str) -> Vec<Vec<u8>> {
    events
        .into_iter()
        .map(|event| stream_event_bytes(event, kind))
        .collect()
}

pub fn flatten_stream_byte_chunks(chunks: Vec<Vec<u8>>) -> Vec<u8> {
    chunks.into_iter().flatten().collect()
}

pub fn launch_event_kind_is(event: &Value, kind: &str) -> bool {
    event["kind"] == kind
}

pub fn stream_event_bytes(event: &Value, kind: &str) -> Vec<u8> {
    let decoded = decode_base64(stream_event_data_base64(event, kind));
    assert_base64_round_trip(kind, &decoded);
    decoded
}

pub fn stream_event_data_base64<'a>(event: &'a Value, kind: &str) -> &'a str {
    event["data_base64"]
        .as_str()
        .unwrap_or_else(|| panic!("{kind} event data_base64 must be a string"))
}

pub fn expected_exit_code_for_status(status: &Value) -> i32 {
    exit_code_for_stream_status(stream_status_kind(status), status)
}

pub fn stream_status_kind(status: &Value) -> &str {
    required_stream_status_kind(stream_status_kind_value(status))
}

pub fn stream_status_kind_value(status: &Value) -> Option<&str> {
    status["kind"].as_str()
}

pub fn required_stream_status_kind(kind: Option<&str>) -> &str {
    kind.expect("status.kind")
}

pub fn exit_code_for_stream_status(kind: &str, status: &Value) -> i32 {
    match kind {
        "exited" => exited_status_exit_code(status),
        "signal_terminated" => 128 + signal_status_exit_code(status),
        "prolonged_silence" => 124,
        "cancelled" => 130,
        "spawn_error" | "unknown" => 1,
        other => unexpected_exit_code_status_kind(other),
    }
}

pub fn exited_status_exit_code(status: &Value) -> i32 {
    i64_status_code(required_exited_status_code(exited_status_code_value(
        status,
    )))
}

pub fn exited_status_code_value(status: &Value) -> Option<i64> {
    status["code"].as_i64()
}

pub fn required_exited_status_code(code: Option<i64>) -> i64 {
    code.expect("status.code")
}

pub fn signal_status_exit_code(status: &Value) -> i32 {
    i64_status_code(required_signal_status_code(signal_status_code_value(
        status,
    )))
}

pub fn signal_status_code_value(status: &Value) -> Option<i64> {
    status["signal"].as_i64()
}

pub fn required_signal_status_code(code: Option<i64>) -> i64 {
    code.expect("status.signal")
}

pub fn i64_status_code(code: i64) -> i32 {
    code as i32
}

pub fn unexpected_exit_code_status_kind(other: &str) -> ! {
    panic!("unexpected ProcessStatus kind {other}")
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
