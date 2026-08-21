//! Declared roles: orchestration

mod cluster_b;
#[allow(dead_code)]
mod support;

use cluster_b::*;
use serde_json::{json, Value};
use std::fs;
use support::{invoke, invoke_validated, invoke_with_env, invoke_with_host_and_env};

#[derive(Default)]
struct RejectFlush {
    accepted: Vec<u8>,
}

struct InvokeDuringFlush {
    accepted: Vec<u8>,
    args: Vec<String>,
    competing_request: Vec<u8>,
    competing_exit: Option<i32>,
    competing_stdout: Vec<u8>,
}

struct RecreateSnapshotDuringTerminalFlush {
    accepted: Vec<u8>,
    args: Vec<String>,
    terminal_retry: Vec<u8>,
    initial_retry: Vec<u8>,
    terminal_retry_exit: Option<i32>,
    terminal_retry_stdout: Vec<u8>,
    initial_retry_exit: Option<i32>,
    initial_retry_stdout: Vec<u8>,
}

impl std::io::Write for RejectFlush {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.accepted.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "simulated buffered terminal enumeration response loss",
        ))
    }
}

impl std::io::Write for InvokeDuringFlush {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.accepted.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.competing_exit.is_none() {
            self.competing_exit = Some(agent_runner_opencode::write_invocation(
                &self.args,
                &self.competing_request,
                &mut self.competing_stdout,
            ));
        }
        Ok(())
    }
}

impl std::io::Write for RecreateSnapshotDuringTerminalFlush {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.accepted.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.terminal_retry_exit.is_none() {
            self.terminal_retry_exit = Some(agent_runner_opencode::write_invocation(
                &self.args,
                &self.terminal_retry,
                &mut self.terminal_retry_stdout,
            ));
            self.initial_retry_exit = Some(agent_runner_opencode::write_invocation(
                &self.args,
                &self.initial_retry,
                &mut self.initial_retry_stdout,
            ));
        }
        Ok(())
    }
}

#[test]
fn characterization_opencode_session_export_json() {
    let export = native_export_fixture();
    assert_native_export_fixture(&export);
}

#[test]
fn contract_session_read_turns() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let params = session_params(session_id);

    let result = read_turns_result(params.clone(), &path);
    let first_ids = assert_first_read_turns_result(&result);

    let second = read_turns_result(params, &path);
    assert_stable_turn_ids(&second, &first_ids);

    assert_missing_read_turns_error(&path);
}

#[test]
fn contract_session_read_turns_uses_regular_file_for_export_stdout() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::pipe_truncating(session_id);
    let path = prepend_path(fake_opencode.dir());

    let result = read_turns_result(session_params(session_id), &path);

    assert_first_read_turns_result(&result);
}

#[test]
fn contract_session_read_turns_projects_bounded_user_observation() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let result = read_turns_result(
        json!({
            "settings_id": "opencode1",
            "session_id": session_id,
            "turn_projection": "user_observation",
            "body_tail_limit": 4
        }),
        &path,
    );

    assert_eq!(result["turn_count"], 1);
    let turns = result["turns"].as_array().expect("projected turns array");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0]["role"], "user");
    assert_eq!(turns[0]["session_id"], session_id);
    assert!(turns[0].get("native").is_none());
    assert_eq!(
        turns[0]["body"][0]["text"],
        "\"reply with the single word: ok\""
    );
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
    assert_canonical_export_result(
        &first,
        "sha256 must be computed over decoded data_base64 bytes",
    );

    let second = success_result(
        invoke_with_env("session.export", params, &[("PATH", path.as_str())]),
        "session.schema.json#/$defs/SessionExportResponse",
        "session.schema.json#/$defs/SessionExportResult",
    );
    assert_deterministic_export(&first, &second);
}

#[test]
fn contract_session_enumerate_empty_list() {
    let fake_opencode = FakeOpencodeSessionList::with_output("[]", "", 0);
    let path = prepend_path(fake_opencode.dir());

    let result = enumerate_result(session_enumerate_params(), &path);

    assert_empty_enumerate_result(&result);
}

#[test]
fn contract_session_enumerate_maps_multiple_sessions() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_multiple_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());

    let result = enumerate_result(session_enumerate_params(), &path);

    assert_multiple_enumerate_result(&result);
}

#[test]
fn contract_session_enumerate_returns_warning_for_bad_cwd_rows() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_bad_cwd_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());

    let result = enumerate_result(session_enumerate_params(), &path);

    assert_bad_cwd_enumerate_result(&result);
}

#[test]
fn contract_session_enumerate_honors_limit() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());

    let result = enumerate_result(session_enumerate_limit_params(2), &path);

    assert_limited_enumerate_result(&result, 2);
    assert_session_list_uses_bounded_snapshot(fake_opencode.log_path());

    let cursor = result["next_cursor"]
        .as_str()
        .expect("truncated page cursor");
    let second = enumerate_result(session_enumerate_cursor_params(2, cursor), &path);
    assert_second_enumerate_page(&second);
    assert_session_list_uses_bounded_snapshot(fake_opencode.log_path());
}

#[test]
fn contract_session_enumerate_retires_consumed_snapshot() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-consumed-session-snapshot");
    fs::create_dir_all(&data_root).expect("create isolated session snapshot data root");
    let host = json!({"data_root": data_root.to_string_lossy()});

    let first = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_limit_params(2),
            host.clone(),
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    let cursor = first["next_cursor"]
        .as_str()
        .expect("truncated page cursor");
    let second = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_cursor_params(2, cursor),
            host.clone(),
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_second_enumerate_page(&second);

    let consumed = assert_error_envelope(invoke_with_host_and_env(
        "session.enumerate",
        session_enumerate_cursor_params(2, cursor),
        host,
        &[("PATH", path.as_str())],
    ));
    assert_eq!(
        consumed["error"]["code"], "invalid_session_enumerate_cursor",
        "a consumed cursor must be retired immediately"
    );
    fs::remove_dir_all(&data_root).expect("remove isolated session snapshot data root");
}

#[test]
fn contract_session_enumerate_distinct_requests_have_independent_snapshot_owners() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-independent-session-snapshots");
    fs::create_dir_all(&data_root).expect("create independent session snapshot data root");
    let host = json!({"data_root": data_root.to_string_lossy()});
    let env = [("PATH", path.as_str())];

    let first_a = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_limit_params(2),
            host.clone(),
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    let first_b = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_limit_params(2),
            host.clone(),
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    let cursor_a = first_a["next_cursor"].as_str().expect("first cursor A");
    let cursor_b = first_b["next_cursor"].as_str().expect("first cursor B");
    assert_ne!(
        cursor_a, cursor_b,
        "distinct initial requests must not share a cursor owner"
    );

    let terminal_a = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_cursor_params(2, cursor_a),
            host.clone(),
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_second_enumerate_page(&terminal_a);
    let terminal_b = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_cursor_params(2, cursor_b),
            host,
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_second_enumerate_page(&terminal_b);
    fs::remove_dir_all(&data_root).expect("remove independent session snapshot data root");
}

#[test]
fn contract_session_enumerate_exact_initial_retry_replays_claimed_native_population() {
    let fake_opencode =
        FakeOpencodeSessionList::with_output(session_list_initial_replay_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-initial-snapshot-replay");
    let config_root = support::isolated_test_config_root("initial-snapshot-replay");
    fs::create_dir_all(&data_root).expect("create initial replay snapshot data root");
    let host = json!({
        "config_root": config_root.to_string_lossy(),
        "data_root": data_root.to_string_lossy()
    });
    let mut request = support::validated_request_envelope(
        "session.enumerate",
        session_enumerate_limit_params(2),
        host,
        "session.schema.json#/$defs/SessionEnumerateRequest",
    );
    request["request_id"] = json!("req-session-enumerate-initial-snapshot-replay");
    support::ensure_default_runtime_settings(&request);
    let request_bytes = serde_json::to_vec(&request).expect("serialize initial enumeration");
    let args = vec![
        "agent-runner-opencode".to_string(),
        "session.enumerate".to_string(),
    ];
    let prior_path = std::env::var_os("PATH");
    std::env::set_var("PATH", &path);
    let mut rejected_flush = RejectFlush::default();
    assert_eq!(
        agent_runner_opencode::write_invocation(&args, &request_bytes, &mut rejected_flush),
        1,
        "the first-page response must be lost only after its bytes are accepted"
    );
    let lost_response: Value = serde_json::Deserializer::from_slice(&rejected_flush.accepted)
        .into_iter()
        .next()
        .expect("accepted first-page response")
        .expect("parse accepted first-page response bytes");
    support::assert_valid(
        &lost_response,
        "session.schema.json#/$defs/SessionEnumerateResponse",
    );
    assert!(
        !lost_response["result"]["warnings"]
            .as_array()
            .expect("first-page warnings")
            .is_empty(),
        "the durable first-page claim must include its warning projection"
    );
    fs::remove_file(fake_opencode.log_path()).expect("clear first native-list evidence");
    fake_opencode.replace_output(changed_session_list_limit_json(), "", 0);

    let mut retry_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(&args, &request_bytes, &mut retry_stdout),
        0
    );
    let retry_response: Value =
        serde_json::from_slice(&retry_stdout).expect("parse exact initial retry response");
    support::assert_valid(
        &retry_response,
        "session.schema.json#/$defs/SessionEnumerateResponse",
    );
    assert_eq!(
        retry_response["result"], lost_response["result"],
        "exact retry must replay the claimed first page even after native rows change"
    );
    assert!(
        !fake_opencode.log_path().exists(),
        "exact retry must consult durable request custody before relisting native sessions"
    );

    let cursor = retry_response["result"]["next_cursor"]
        .as_str()
        .expect("replayed first-page cursor");
    let mut continuation = request;
    continuation["request_id"] = json!("req-session-enumerate-initial-snapshot-continuation");
    continuation["params"] = session_enumerate_cursor_params(2, cursor);
    let mut continuation_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&continuation).expect("serialize replay continuation"),
            &mut continuation_stdout,
        ),
        0
    );
    let continuation_response: Value =
        serde_json::from_slice(&continuation_stdout).expect("parse replay continuation response");
    assert_eq!(
        continuation_response["result"]["sessions"][0]["provider_session_id"], "ses_replay_three",
        "continuation must remain on the originally claimed native population"
    );

    match prior_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    fs::remove_dir_all(&data_root).expect("remove initial replay snapshot data root");
    fs::remove_dir_all(&config_root).expect("remove initial replay config root");
}

#[test]
fn contract_session_enumerate_retires_snapshot_only_after_terminal_response_handoff() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-response-loss-session-snapshot");
    fs::create_dir_all(&data_root).expect("create isolated session snapshot data root");
    let host = json!({"data_root": data_root.to_string_lossy()});
    let mut first_request = support::validated_request_envelope(
        "session.enumerate",
        session_enumerate_limit_params(2),
        host,
        "session.schema.json#/$defs/SessionEnumerateRequest",
    );
    first_request["request_id"] = json!("req-session-enumerate-flush-loss-first");
    support::ensure_default_runtime_settings(&first_request);
    let prior_path = std::env::var_os("PATH");
    std::env::set_var("PATH", &path);
    let args = vec![
        "agent-runner-opencode".to_string(),
        "session.enumerate".to_string(),
    ];
    let mut first_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&first_request).expect("serialize first enumeration request"),
            &mut first_stdout,
        ),
        0
    );
    let first_response: Value = serde_json::from_slice(&first_stdout)
        .expect("parse first enumeration response after successful flush");
    support::assert_valid(
        &first_response,
        "session.schema.json#/$defs/SessionEnumerateResponse",
    );
    let first = &first_response["result"];
    support::assert_valid(first, "session.schema.json#/$defs/SessionEnumerateResult");
    let cursor = first["next_cursor"]
        .as_str()
        .expect("truncated page cursor");
    let mut request = first_request;
    request["request_id"] = json!("req-session-enumerate-flush-loss-terminal");
    request["params"] = session_enumerate_cursor_params(2, cursor);
    support::assert_valid_request_envelope(
        &request,
        "session.schema.json#/$defs/SessionEnumerateRequest",
    );
    let mut rejected_flush = RejectFlush::default();
    let lost_exit = agent_runner_opencode::write_invocation(
        &args,
        &serde_json::to_vec(&request).expect("serialize enumeration request"),
        &mut rejected_flush,
    );
    assert_eq!(
        lost_exit,
        1,
        "failed buffered response handoff must fail the invocation; accepted={}",
        String::from_utf8_lossy(&rejected_flush.accepted),
    );
    assert!(
        !rejected_flush.accepted.is_empty(),
        "the response bytes must be accepted before the simulated flush failure"
    );

    let mut retry_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&request).expect("serialize terminal enumeration retry"),
            &mut retry_stdout,
        ),
        0
    );
    let retry_response: Value = serde_json::from_slice(&retry_stdout)
        .expect("parse terminal enumeration response after successful flush");
    support::assert_valid(
        &retry_response,
        "session.schema.json#/$defs/SessionEnumerateResponse",
    );
    support::assert_valid(
        &retry_response["result"],
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_second_enumerate_page(&retry_response["result"]);

    let mut consumed_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&request).expect("serialize consumed enumeration cursor"),
            &mut consumed_stdout,
        ),
        2
    );
    match prior_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    let consumed: Value =
        serde_json::from_slice(&consumed_stdout).expect("parse consumed cursor response");
    support::assert_valid(
        &consumed,
        "session.schema.json#/$defs/SessionEnumerateErrorResponse",
    );
    assert_eq!(
        consumed["error"]["code"],
        "invalid_session_enumerate_cursor"
    );
    fs::remove_dir_all(&data_root).expect("remove isolated session snapshot data root");
}

#[test]
fn contract_terminal_snapshot_claim_blocks_initial_retry_during_response_handoff() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-terminal-snapshot-claim");
    fs::create_dir_all(&data_root).expect("create isolated session snapshot data root");
    let host = json!({"data_root": data_root.to_string_lossy()});
    let mut initial_request = support::validated_request_envelope(
        "session.enumerate",
        session_enumerate_limit_params(2),
        host,
        "session.schema.json#/$defs/SessionEnumerateRequest",
    );
    initial_request["request_id"] = json!("req-session-enumerate-terminal-claim-initial");
    support::ensure_default_runtime_settings(&initial_request);
    let prior_path = std::env::var_os("PATH");
    std::env::set_var("PATH", &path);
    let args = vec![
        "agent-runner-opencode".to_string(),
        "session.enumerate".to_string(),
    ];
    let initial_bytes = serde_json::to_vec(&initial_request).expect("serialize initial request");
    let mut initial_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(&args, &initial_bytes, &mut initial_stdout),
        0
    );
    let initial_response: Value =
        serde_json::from_slice(&initial_stdout).expect("parse initial response");
    let cursor = initial_response["result"]["next_cursor"]
        .as_str()
        .expect("terminal continuation cursor");
    let mut terminal_request = initial_request.clone();
    terminal_request["request_id"] = json!("req-session-enumerate-terminal-claim-owner");
    terminal_request["params"] = session_enumerate_cursor_params(2, cursor);
    let mut handoff = InvokeDuringFlush {
        accepted: Vec::new(),
        args: args.clone(),
        competing_request: initial_bytes,
        competing_exit: None,
        competing_stdout: Vec::new(),
    };

    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&terminal_request).expect("serialize terminal request"),
            &mut handoff,
        ),
        0
    );
    assert_eq!(handoff.competing_exit, Some(2));
    let competing: Value = serde_json::from_slice(&handoff.competing_stdout)
        .expect("parse competing initial retry response");
    assert_eq!(
        competing["error"]["code"],
        "session_enumeration_snapshot_terminal_handoff_in_progress"
    );
    let terminal: Value =
        serde_json::from_slice(&handoff.accepted).expect("parse terminal response");
    assert_second_enumerate_page(&terminal["result"]);

    match prior_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    fs::remove_dir_all(&data_root).expect("remove isolated session snapshot data root");
}

#[test]
fn contract_session_enumerate_advances_cursor_monotonically() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-monotonic-session-cursor");
    fs::create_dir_all(&data_root).expect("create monotonic session cursor data root");
    let host = json!({"data_root": data_root.to_string_lossy()});
    let env = [("PATH", path.as_str())];

    let first = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_limit_params(1),
            host.clone(),
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    let first_cursor = first["next_cursor"].as_str().expect("first page cursor");
    let second = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_cursor_params(1, first_cursor),
            host.clone(),
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    let terminal_cursor = second["next_cursor"]
        .as_str()
        .expect("terminal page cursor");

    let replay = assert_error_envelope(invoke_with_host_and_env(
        "session.enumerate",
        session_enumerate_cursor_params(1, first_cursor),
        host.clone(),
        &env,
    ));
    assert_eq!(
        replay["error"]["code"], "session_enumeration_cursor_superseded",
        "a later issued cursor must make an older cursor unavailable to a new request"
    );

    let terminal = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_cursor_params(1, terminal_cursor),
            host,
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_eq!(terminal["sessions"].as_array().expect("sessions").len(), 1);
    assert_eq!(terminal["complete"], true);
    assert!(terminal["next_cursor"].is_null());
    fs::remove_dir_all(&data_root).expect("remove monotonic session cursor data root");
}

#[test]
fn contract_terminal_claim_blocks_older_cursor_during_response_handoff() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-terminal-older-cursor-race");
    fs::create_dir_all(&data_root).expect("create terminal older-cursor data root");
    let host = json!({"data_root": data_root.to_string_lossy()});
    let prior_path = std::env::var_os("PATH");
    std::env::set_var("PATH", &path);
    let args = vec![
        "agent-runner-opencode".to_string(),
        "session.enumerate".to_string(),
    ];
    let mut initial_request = support::validated_request_envelope(
        "session.enumerate",
        session_enumerate_limit_params(1),
        host,
        "session.schema.json#/$defs/SessionEnumerateRequest",
    );
    initial_request["request_id"] = json!("req-session-enumerate-race-initial");
    support::ensure_default_runtime_settings(&initial_request);
    let mut initial_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&initial_request).expect("serialize initial request"),
            &mut initial_stdout,
        ),
        0
    );
    let initial_response: Value =
        serde_json::from_slice(&initial_stdout).expect("parse initial response");
    let older_cursor = initial_response["result"]["next_cursor"]
        .as_str()
        .expect("older cursor")
        .to_string();

    let mut middle_request = initial_request.clone();
    middle_request["request_id"] = json!("req-session-enumerate-race-middle");
    middle_request["params"] = session_enumerate_cursor_params(1, &older_cursor);
    let mut middle_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&middle_request).expect("serialize middle request"),
            &mut middle_stdout,
        ),
        0
    );
    let middle_response: Value =
        serde_json::from_slice(&middle_stdout).expect("parse middle response");
    let terminal_cursor = middle_response["result"]["next_cursor"]
        .as_str()
        .expect("terminal cursor");

    let mut terminal_request = initial_request.clone();
    terminal_request["request_id"] = json!("req-session-enumerate-race-terminal");
    terminal_request["params"] = session_enumerate_cursor_params(1, terminal_cursor);
    let mut competing_request = initial_request;
    competing_request["request_id"] = json!("req-session-enumerate-race-older-replay");
    competing_request["params"] = session_enumerate_cursor_params(1, &older_cursor);
    let mut handoff = InvokeDuringFlush {
        accepted: Vec::new(),
        args: args.clone(),
        competing_request: serde_json::to_vec(&competing_request)
            .expect("serialize competing older-cursor request"),
        competing_exit: None,
        competing_stdout: Vec::new(),
    };

    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&terminal_request).expect("serialize terminal request"),
            &mut handoff,
        ),
        0
    );
    assert_eq!(handoff.competing_exit, Some(2));
    let competing: Value = serde_json::from_slice(&handoff.competing_stdout)
        .expect("parse competing older-cursor response");
    assert_eq!(
        competing["error"]["code"],
        "session_enumeration_snapshot_terminal_handoff_in_progress"
    );
    let terminal: Value =
        serde_json::from_slice(&handoff.accepted).expect("parse terminal response");
    assert_eq!(terminal["result"]["complete"], true);
    assert!(terminal["result"]["next_cursor"].is_null());

    match prior_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    fs::remove_dir_all(&data_root).expect("remove terminal older-cursor data root");
}

#[test]
fn contract_stale_terminal_cleanup_cannot_delete_recreated_snapshot() {
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-snapshot-cleanup-generation");
    let config_root = support::isolated_test_config_root("snapshot-cleanup-generation");
    fs::create_dir_all(&data_root).expect("create snapshot cleanup generation data root");
    let host = json!({
        "config_root": config_root.to_string_lossy(),
        "data_root": data_root.to_string_lossy()
    });
    let prior_path = std::env::var_os("PATH");
    std::env::set_var("PATH", &path);
    let args = vec![
        "agent-runner-opencode".to_string(),
        "session.enumerate".to_string(),
    ];
    let mut initial_request = support::validated_request_envelope(
        "session.enumerate",
        session_enumerate_limit_params(2),
        host,
        "session.schema.json#/$defs/SessionEnumerateRequest",
    );
    initial_request["request_id"] = json!("req-session-enumerate-cleanup-generation-initial");
    support::ensure_default_runtime_settings(&initial_request);
    let initial_bytes = serde_json::to_vec(&initial_request).expect("serialize initial request");
    let mut initial_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(&args, &initial_bytes, &mut initial_stdout),
        0
    );
    let initial_response: Value =
        serde_json::from_slice(&initial_stdout).expect("parse initial response");
    let terminal_cursor = initial_response["result"]["next_cursor"]
        .as_str()
        .expect("terminal cursor");
    let mut terminal_request = initial_request.clone();
    terminal_request["request_id"] = json!("req-session-enumerate-cleanup-generation-terminal");
    terminal_request["params"] = session_enumerate_cursor_params(2, terminal_cursor);
    let terminal_bytes = serde_json::to_vec(&terminal_request).expect("serialize terminal request");
    let mut interleaving = RecreateSnapshotDuringTerminalFlush {
        accepted: Vec::new(),
        args: args.clone(),
        terminal_retry: terminal_bytes.clone(),
        initial_retry: initial_bytes,
        terminal_retry_exit: None,
        terminal_retry_stdout: Vec::new(),
        initial_retry_exit: None,
        initial_retry_stdout: Vec::new(),
    };

    assert_eq!(
        agent_runner_opencode::write_invocation(&args, &terminal_bytes, &mut interleaving),
        0,
        "the first terminal owner must complete after the nested retries"
    );
    assert_eq!(interleaving.terminal_retry_exit, Some(0));
    assert_eq!(
        interleaving.initial_retry_exit,
        Some(0),
        "recreated initial retry failed: {}",
        String::from_utf8_lossy(&interleaving.initial_retry_stdout)
    );
    let replacement: Value = serde_json::from_slice(&interleaving.initial_retry_stdout)
        .expect("parse recreated initial response");
    let replacement_cursor = replacement["result"]["next_cursor"]
        .as_str()
        .expect("replacement cursor");
    let mut replacement_continuation = initial_request;
    replacement_continuation["request_id"] =
        json!("req-session-enumerate-cleanup-generation-replacement-terminal");
    replacement_continuation["params"] = session_enumerate_cursor_params(2, replacement_cursor);
    let mut replacement_terminal_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&replacement_continuation)
                .expect("serialize replacement continuation"),
            &mut replacement_terminal_stdout,
        ),
        0,
        "stale cleanup must not delete the replacement snapshot"
    );
    let replacement_terminal: Value = serde_json::from_slice(&replacement_terminal_stdout)
        .expect("parse replacement terminal response");
    assert_eq!(replacement_terminal["result"]["complete"], true);

    match prior_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    fs::remove_dir_all(&data_root).expect("remove snapshot cleanup generation data root");
    fs::remove_dir_all(&config_root).expect("remove snapshot cleanup generation config root");
}

#[test]
fn contract_session_enumerate_invalid_json_is_provider_error() {
    let fake_opencode = FakeOpencodeSessionList::with_output("not json", "", 0);
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_env(
        "session.enumerate",
        session_enumerate_params(),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_opencode_session_list");
}

#[test]
fn contract_session_enumerate_nonzero_wrapper_exit_is_provider_error() {
    let fake_opencode = FakeOpencodeSessionList::with_output("[]", "list failed", 9);
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_env(
        "session.enumerate",
        session_enumerate_params(),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "session_list_failed");
    assert!(response["error"]["message"]
        .as_str()
        .expect("error message string")
        .contains("list failed"));
}

#[test]
#[ignore = "live opencode auth/network session export proof; run explicitly when external dependencies are available"]
fn integration_session_export_live() {
    let session_id = live_opencode_session_id();
    let path = std::env::var("PATH").expect("live PATH");
    let home = std::env::var("HOME").expect("live HOME");
    let result = success_result(
        invoke_with_env(
            "session.export",
            session_params_for_settings("opencode5", &session_id),
            &[("PATH", path.as_str()), ("HOME", home.as_str())],
        ),
        "session.schema.json#/$defs/SessionExportResponse",
        "session.schema.json#/$defs/SessionExportResult",
    );

    assert_canonical_export_result(
        &result,
        "live session.export sha256 must be computed over decoded data_base64 bytes",
    );
}

#[test]
fn contract_session_capture() {
    let session_id = fixture_session_id();
    let result = success_result(
        invoke_validated(
            "session.capture",
            launch_capture_params(session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_launch_capture_result(&result, session_id);

    let bare_session_result = success_result(
        invoke_validated(
            "session.capture",
            bare_capture_params(session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_bare_capture_result(&bare_session_result, session_id);

    let lifecycle_result = success_result(
        invoke_validated(
            "session.capture",
            lifecycle_capture_params(session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_lifecycle_capture_result(&lifecycle_result, session_id);

    let pinned_session_id = "ses_pinned_lifecycle";
    let pinned_result = success_result(
        invoke_validated(
            "session.capture",
            pinned_lifecycle_capture_params(pinned_session_id, pinned_session_id),
            "session.schema.json#/$defs/SessionCaptureRequest",
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );
    assert_pinned_capture_result(&pinned_result, pinned_session_id);
}

#[test]
fn contract_session_capture_rejects_conflicting_identity_carriers() {
    let session_id = fixture_session_id();
    for params in [
        conflicting_launch_capture_params(session_id),
        pinned_lifecycle_capture_params("ses_pinned_conflict", session_id),
    ] {
        let response = assert_error_envelope(invoke_validated(
            "session.capture",
            params,
            "session.schema.json#/$defs/SessionCaptureRequest",
        ));
        assert_eq!(response["error"]["code"], "invalid_session_capture_params");
        assert!(response["error"]["message"]
            .as_str()
            .expect("error message string")
            .contains("conflicting session evidence"));
    }
}

#[test]
fn contract_session_capture_validates_exact_live_report() {
    let session_id = fixture_session_id();
    let invocation_uuid = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let working_directory = native_export_fixture()["info"]["directory"]
        .as_str()
        .unwrap()
        .to_string();

    let result = success_result(
        invoke_with_host_and_env(
            "session.capture",
            live_capture_params(session_id, invocation_uuid),
            json!({"working_directory": working_directory}),
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionCaptureResponse",
        "session.schema.json#/$defs/SessionCaptureResult",
    );

    assert_live_capture_result(&result, session_id);
}

#[test]
fn contract_session_capture_rejects_live_report_workspace_mismatch() {
    let session_id = fixture_session_id();
    let invocation_uuid = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_host_and_env(
        "session.capture",
        live_capture_params(session_id, invocation_uuid),
        json!({"working_directory": "/tmp/not-the-exported-workspace"}),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_session_capture_params");
}

#[test]
fn contract_session_capture_rejects_live_report_invocation_mismatch() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeExport::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let mut params = live_capture_params(session_id, "cccccccc-cccc-4ccc-8ccc-cccccccccccc");
    params["live_report"]["invocation_uuid"] =
        Value::String("dddddddd-dddd-4ddd-8ddd-dddddddddddd".to_string());

    let response = assert_error_envelope(invoke_with_env(
        "session.capture",
        params,
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_session_capture_params");
}

#[test]
fn contract_session_capture_rejects_removed_evidence_shape() {
    let session_id = fixture_session_id();
    let response = assert_error_envelope(invoke(
        "session.capture",
        removed_evidence_capture_params(session_id),
    ));
    assert_removed_evidence_capture_error(&response);
}

#[test]
fn contract_session_locate_not_located() {
    let session_id = fixture_session_id();
    let result = success_result(
        invoke_with_env("session.locate_transcript", session_params(session_id), &[]),
        "session.schema.json#/$defs/SessionLocateTranscriptResponse",
        "session.schema.json#/$defs/SessionLocateTranscriptResult",
    );
    assert_not_located_result(&result);
}

#[test]
fn contract_session_replace_unsupported() {
    let session_id = fixture_session_id();
    let fixture = SessionReplaceFixture::new();

    let output = invoke_with_host_and_env(
        "session.replace",
        session_replace_params(session_id),
        fixture.host_override(),
        &[],
    );

    assert_replace_response(&output);
    fixture.assert_unchanged();
}
