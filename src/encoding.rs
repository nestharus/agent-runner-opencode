//! Declared roles: formatter, parser, accessor, filter, validator

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::new();
    for chunk in bytes.chunks(3) {
        push_base64_chunk(&mut encoded, chunk, TABLE);
    }
    encoded
}

pub fn decode_base64(input: &str) -> Result<Vec<u8>, String> {
    let clean = clean_base64(input);
    validate_base64_len(&clean)?;
    decode_base64_chunks(&clean)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn bounded_text(text: &str, max_len: usize) -> String {
    text.chars().take(max_len).collect()
}

pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("canonical JSON serialization is infallible")
}

pub fn write_canonical_json<W: std::io::Write>(
    writer: &mut W,
    value: &Value,
) -> std::io::Result<()> {
    writer.write_all(&canonical_json_bytes(value))
}

fn push_base64_chunk(encoded: &mut String, chunk: &[u8], table: &[u8; 64]) {
    let b0 = chunk[0];
    let b1 = *chunk.get(1).unwrap_or(&0);
    let b2 = *chunk.get(2).unwrap_or(&0);
    encoded.push(table[(b0 >> 2) as usize] as char);
    encoded.push(table[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
    encoded.push(third_base64_char(chunk, b1, b2, table));
    encoded.push(fourth_base64_char(chunk, b2, table));
}

fn third_base64_char(chunk: &[u8], b1: u8, b2: u8, table: &[u8; 64]) -> char {
    if chunk.len() > 1 {
        return table[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char;
    }
    '='
}

fn fourth_base64_char(chunk: &[u8], b2: u8, table: &[u8; 64]) -> char {
    if chunk.len() > 2 {
        return table[(b2 & 0b0011_1111) as usize] as char;
    }
    '='
}

fn clean_base64(input: &str) -> Vec<u8> {
    input
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect()
}

fn validate_base64_len(clean: &[u8]) -> Result<(), String> {
    if clean.len().is_multiple_of(4) {
        return Ok(());
    }
    Err(invalid_base64_len_error())
}

fn decode_base64_chunks(clean: &[u8]) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    for chunk in clean.chunks(4) {
        append_decoded_chunk(&mut bytes, chunk)?;
    }
    Ok(bytes)
}

fn append_decoded_chunk(bytes: &mut Vec<u8>, chunk: &[u8]) -> Result<(), String> {
    let (values, padding) = base64_chunk_values(chunk)?;
    bytes.push((values[0] << 2) | (values[1] >> 4));
    if padding < 2 {
        bytes.push((values[1] << 4) | (values[2] >> 2));
    }
    if padding == 0 {
        bytes.push((values[2] << 6) | values[3]);
    }
    Ok(())
}

fn base64_chunk_values(chunk: &[u8]) -> Result<([u8; 4], usize), String> {
    let mut values = [0u8; 4];
    let mut padding = 0;
    for (index, byte) in chunk.iter().copied().enumerate() {
        if byte == b'=' {
            padding += 1;
        } else {
            values[index] = base64_value(byte)?;
        }
    }
    Ok((values, padding))
}

fn base64_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(invalid_base64_byte_error(byte)),
    }
}

fn invalid_base64_len_error() -> String {
    "base64 length must be a multiple of four".to_string()
}

fn invalid_base64_byte_error(byte: u8) -> String {
    format!("invalid base64 byte 0x{byte:02x}")
}
