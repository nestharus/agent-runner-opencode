//! Declared roles: adapter, parser, validator, mapper, formatter
//! adapter_declarations:
//!   - component: src/quota_adapter.rs
//!     role: adapter
//!     Translates:
//!       - authenticated ChatGPT WHAM HTTP responses to QuotaObservation
//!       - source-specific transport and protocol failures to QuotaObservationFailure

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
use crate::child_custody::ChildCustody;
use crate::durable_fs;
use crate::quota_observer::QuotaObserverContext;
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
use crate::shell::ShellOutput;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;
#[cfg(all(
    target_os = "linux",
    not(all(feature = "contract-test-fixtures", debug_assertions))
))]
use std::io::Read;
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
use std::io::Write;
use std::path::Path;
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
use std::process::Stdio;
use std::time::Duration;

const CHATGPT_USAGE_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
const HTTP_STATUS_MARKER: &str = "__oulipoly_http_status__:";
const MAX_ACCESS_TOKEN_BYTES: usize = 32 * 1024;
const MAX_ACCOUNT_ID_BYTES: usize = 1024;
const MAX_WHAM_STDOUT_BYTES: usize = 512 * 1024;
#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
const MAX_WHAM_STDERR_BYTES: usize = 64 * 1024;
const WHAM_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaObservationSource {
    WhamApi,
}

#[derive(Debug)]
pub struct QuotaWindow {
    pub name: Option<String>,
    pub used_percent: f64,
    pub resets_at: String,
}

#[derive(Debug)]
pub struct QuotaObservation {
    pub source: QuotaObservationSource,
    pub windows: Vec<QuotaWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaFailureSource {
    AuthFile,
    WhamTransport,
    WhamHttp,
    WhamProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaRefreshAdvice {
    DoNotRefresh,
    RefreshAuthentication,
}

#[derive(Debug)]
pub struct QuotaObservationFailure {
    pub source: QuotaFailureSource,
    pub detail: String,
    refresh_advice: QuotaRefreshAdvice,
}

impl QuotaObservationFailure {
    pub fn needs_auth_refresh(&self) -> bool {
        self.refresh_advice == QuotaRefreshAdvice::RefreshAuthentication
    }
}

pub fn observe_quota(
    auth_path: &Path,
    observer: &QuotaObserverContext,
) -> Result<QuotaObservation, QuotaObservationFailure> {
    observe_wham_api(auth_path, observer)
}

fn observe_wham_api(
    auth_path: &Path,
    observer: &QuotaObserverContext,
) -> Result<QuotaObservation, QuotaObservationFailure> {
    let tokens = read_auth_tokens(auth_path)
        .map_err(|detail| failure(QuotaFailureSource::AuthFile, detail))?;
    let (body, status_code) = run_wham_transport(&tokens, observer).map_err(|err| {
        failure(
            QuotaFailureSource::WhamTransport,
            format!("ChatGPT WHAM request failed to start: {err}"),
        )
    })?;
    if !(200..300).contains(&status_code) {
        return Err(http_failure(status_code, &body));
    }
    let parsed = parse_wham_usage_json(&body).map_err(|err| {
        failure(
            QuotaFailureSource::WhamProtocol,
            format!("ChatGPT WHAM response must be JSON: {err}"),
        )
    })?;
    let windows = parse_wham_windows(&parsed)
        .map_err(|detail| failure(QuotaFailureSource::WhamProtocol, detail))?;
    Ok(QuotaObservation {
        source: QuotaObservationSource::WhamApi,
        windows,
    })
}

fn failure(source: QuotaFailureSource, detail: String) -> QuotaObservationFailure {
    QuotaObservationFailure {
        source,
        detail,
        refresh_advice: QuotaRefreshAdvice::DoNotRefresh,
    }
}

fn http_failure(status: u16, body: &str) -> QuotaObservationFailure {
    QuotaObservationFailure {
        source: QuotaFailureSource::WhamHttp,
        detail: format!(
            "ChatGPT WHAM API returned HTTP {status}: {}",
            http_error_detail(body)
        ),
        refresh_advice: if status == 401 {
            QuotaRefreshAdvice::RefreshAuthentication
        } else {
            QuotaRefreshAdvice::DoNotRefresh
        },
    }
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn shell_failure_detail(participant: &str, output: &ShellOutput) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        return format!("{participant} exited with status {}", output.status);
    }
    format!(
        "{participant} exited with status {}: {stderr}",
        output.status
    )
}

struct AuthTokens {
    access_token: String,
    account_id: String,
}

fn read_auth_tokens(path: &Path) -> Result<AuthTokens, String> {
    let raw = durable_fs::read_file_bounded(path, durable_fs::MAX_AUTH_FILE_BYTES)
        .map_err(|err| format!("failed to read bounded OpenCode auth file: {err}"))?;
    let parsed: Value = serde_json::from_slice(&raw)
        .map_err(|err| format!("OpenCode auth file must be JSON: {err}"))?;
    let access_token = parsed
        .pointer("/openai/access")
        .and_then(Value::as_str)
        .and_then(nonempty_string)
        .ok_or_else(missing_auth_tokens_error)?;
    let account_id = parsed
        .pointer("/openai/accountId")
        .and_then(Value::as_str)
        .and_then(nonempty_string)
        .ok_or_else(missing_auth_tokens_error)?;
    validate_curl_header_value("access token", &access_token)?;
    validate_curl_header_value("account id", &account_id)?;
    if access_token.len() > MAX_ACCESS_TOKEN_BYTES || account_id.len() > MAX_ACCOUNT_ID_BYTES {
        return Err(
            "OpenCode auth token identity exceeds the supported bounded WHAM request".into(),
        );
    }
    Ok(AuthTokens {
        access_token,
        account_id,
    })
}

fn validate_curl_header_value(label: &str, value: &str) -> Result<(), String> {
    if value.chars().all(|character| !character.is_control()) {
        return Ok(());
    }
    Err(format!(
        "OpenCode auth {label} contains a control character that cannot enter the fixed WHAM request"
    ))
}

fn missing_auth_tokens_error() -> String {
    "OpenCode auth file is missing a ChatGPT access token or account id".to_string()
}

fn nonempty_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn curl_usage_argv() -> Vec<String> {
    vec![
        "curl".to_string(),
        "-q".to_string(),
        "-sS".to_string(),
        "--max-time".to_string(),
        "20".to_string(),
        "-w".to_string(),
        format!("\n{HTTP_STATUS_MARKER}%{{http_code}}"),
        "-K".to_string(),
        "-".to_string(),
        CHATGPT_USAGE_URL.to_string(),
    ]
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn run_wham_transport(
    tokens: &AuthTokens,
    observer: &QuotaObserverContext,
) -> std::io::Result<(String, u16)> {
    let argv = curl_usage_argv();
    let (program, args) = argv
        .split_first()
        .expect("curl usage argv is constructed with a program");
    debug_assert_eq!(program, "curl");
    let child = observer
        .command()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut custody = ChildCustody::new(child);
    custody
        .child_mut()
        .stdin
        .as_mut()
        .expect("curl stdin is piped")
        .write_all(curl_usage_config(tokens).as_bytes())?;
    let output = custody
        .wait_with_bounded_output_timeout(
            WHAM_TIMEOUT,
            MAX_WHAM_STDOUT_BYTES,
            MAX_WHAM_STDERR_BYTES,
        )?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "WHAM curl timed out"))?;
    if output.stdout.len() > MAX_WHAM_STDOUT_BYTES || output.stderr.len() > MAX_WHAM_STDERR_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WHAM curl output exceeds the supported bounded response",
        ));
    }
    let output = ShellOutput {
        stdout: output.stdout,
        stderr: output.stderr,
        status: output.status.code().unwrap_or(1),
    };
    if output.status != 0 {
        return Err(std::io::Error::other(shell_failure_detail(
            "ChatGPT WHAM contract-test curl transport",
            &output,
        )));
    }
    let (body, status) = split_http_body_and_status(&output.stdout)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let status = parse_http_status(status)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok((body.to_string(), status))
}

#[cfg(all(
    target_os = "linux",
    not(all(feature = "contract-test-fixtures", debug_assertions))
))]
fn run_wham_transport(
    tokens: &AuthTokens,
    _observer: &QuotaObserverContext,
) -> std::io::Result<(String, u16)> {
    let client = reqwest::blocking::Client::builder()
        .timeout(WHAM_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(std::io::Error::other)?;
    let mut response = client
        .get(CHATGPT_USAGE_URL)
        .bearer_auth(&tokens.access_token)
        .header("ChatGPT-Account-Id", &tokens.account_id)
        .send()
        .map_err(std::io::Error::other)?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_WHAM_STDOUT_BYTES as u64)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WHAM response exceeds the supported bounded response",
        ));
    }
    let status = response.status().as_u16();
    let mut bytes = Vec::new();
    (&mut response)
        .take(MAX_WHAM_STDOUT_BYTES.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_WHAM_STDOUT_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WHAM response exceeds the supported bounded response",
        ));
    }
    let body = String::from_utf8(bytes).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("ChatGPT WHAM response must be UTF-8: {error}"),
        )
    })?;
    Ok((body, status))
}

#[cfg(all(
    not(target_os = "linux"),
    not(all(feature = "contract-test-fixtures", debug_assertions))
))]
fn run_wham_transport(
    _tokens: &AuthTokens,
    _observer: &QuotaObserverContext,
) -> std::io::Result<(String, u16)> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "the in-process WHAM transport is currently reviewed only for Linux targets",
    ))
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn curl_usage_config(tokens: &AuthTokens) -> String {
    format!(
        "header = \"Authorization: Bearer {}\"\nheader = \"ChatGPT-Account-Id: {}\"\n",
        curl_config_escape(&tokens.access_token),
        curl_config_escape(&tokens.account_id)
    )
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn curl_config_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn split_http_body_and_status(raw: &[u8]) -> Result<(&str, &str), String> {
    let text = std::str::from_utf8(raw)
        .map_err(|err| format!("ChatGPT WHAM response must be UTF-8: {err}"))?;
    let (body, status) = text
        .rsplit_once(HTTP_STATUS_MARKER)
        .ok_or_else(|| "curl output missing ChatGPT WHAM HTTP status marker".to_string())?;
    Ok((body.trim_end_matches('\n'), status.trim()))
}

#[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
fn parse_http_status(status: &str) -> Result<u16, String> {
    status
        .parse::<u16>()
        .map_err(|err| format!("ChatGPT WHAM HTTP status is invalid: {err}"))
}

fn parse_wham_usage_json(body: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str(body)
}

fn parse_wham_windows(parsed: &Value) -> Result<Vec<QuotaWindow>, String> {
    ["secondary_window", "primary_window"]
        .into_iter()
        .filter_map(|name| {
            parsed
                .pointer(&format!("/rate_limit/{name}"))
                .map(|window| parse_wham_window(name, window))
        })
        .collect()
}

fn parse_wham_window(name: &str, window: &Value) -> Result<QuotaWindow, String> {
    let reset_at = window
        .get("reset_at")
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("ChatGPT WHAM {name}.reset_at must be Unix seconds"))?;
    let used_percent = window
        .get("used_percent")
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("ChatGPT WHAM {name}.used_percent must be numeric"))?;
    validate_used_percent(name, used_percent)?;
    let resets_at = DateTime::<Utc>::from_timestamp(reset_at, 0)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Secs, true))
        .ok_or_else(|| format!("ChatGPT WHAM {name}.reset_at is out of range"))?;
    Ok(QuotaWindow {
        name: None,
        used_percent,
        resets_at,
    })
}

fn http_error_detail(body: &str) -> String {
    let parsed = serde_json::from_str::<Value>(body).ok();
    let detail = parsed.as_ref().and_then(|value| {
        value
            .pointer("/detail")
            .or_else(|| value.pointer("/error/message"))
            .and_then(Value::as_str)
    });
    detail
        .filter(|detail| !detail.trim().is_empty())
        .unwrap_or_else(|| body.trim())
        .to_string()
}

fn validate_used_percent(label: &str, used_percent: f64) -> Result<(), String> {
    if (0.0..=100.0).contains(&used_percent) {
        return Ok(());
    }
    Err(format!("{label}.used_percent out of range: {used_percent}"))
}
