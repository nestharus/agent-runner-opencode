//! Declared roles: orchestration

mod session_projection;
#[allow(dead_code)]
mod support;

use base64::Engine as _;
use serde_json::{json, Value};
use session_projection::*;
use sha2::Digest as _;
use std::fs;
use std::io::Write as _;
use std::sync::Mutex;
use support::{
    invoke, invoke_validated, invoke_with_env, invoke_with_host_and_env,
    invoke_with_request_and_env,
};

static PATH_ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

#[derive(Default)]
struct RejectFlush {
    accepted: Vec<u8>,
}

#[derive(Default)]
struct AcceptAndDiscard;

struct InvokeDuringFlush {
    accepted: Vec<u8>,
    args: Vec<String>,
    competing_request: Vec<u8>,
    competing_exit: Option<i32>,
    competing_stdout: Vec<u8>,
}

struct RetrySnapshotDuringTerminalFlush {
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

impl std::io::Write for AcceptAndDiscard {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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

impl std::io::Write for RetrySnapshotDuringTerminalFlush {
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
fn contract_session_read_turns_pages_a_bounded_immutable_snapshot_without_export() {
    let session_id = "ses_turn_pages_fixture";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let params = session_turn_page_beginning_params(session_id, "canonical_ingest", None);

    let first = read_turns_result(params.clone(), &path);
    let exact_retry = read_turns_result(params.clone(), &path);
    assert_eq!(exact_retry, first, "the same bounded read must be stable");
    assert_eq!(first["read_protocol"], "oulipoly.session_turn_pages/v1");
    assert_eq!(first["provider_instance_id"], "opencode-primary");
    assert_eq!(first["settings_id"], "opencode1");
    assert_eq!(first["session_id"], session_id);
    assert_eq!(first["turn_projection"], "canonical_ingest");
    assert_eq!(first["page_index"], 0);
    assert_eq!(first["page_start_sequence"], 0);

    let snapshot_id = first["snapshot_id"]
        .as_str()
        .expect("snapshot id")
        .to_string();
    let mut pages = vec![first];
    while !pages
        .last()
        .expect("page")
        .get("snapshot_complete")
        .and_then(Value::as_bool)
        .expect("snapshot_complete bool")
    {
        let page = continue_turn_page(&params, pages.last().expect("prior page"), &path);
        assert_eq!(page["snapshot_id"], snapshot_id);
        pages.push(page);
    }

    assert_eq!(pages.len(), 3);
    let turns = pages
        .iter()
        .flat_map(|page| page["turns"].as_array().expect("turns array"))
        .collect::<Vec<_>>();
    assert_eq!(turns.len(), 3, "trailing mutable assistant is excluded");
    assert_eq!(
        turns
            .iter()
            .map(|turn| turn["role"].as_str().expect("turn role"))
            .collect::<Vec<_>>(),
        vec!["user", "assistant", "user"]
    );
    assert_eq!(
        turns
            .iter()
            .map(|turn| turn["snapshot_sequence"].as_u64().expect("sequence"))
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(turns[0]["body_state"], "inline");
    assert_eq!(turns[0]["body"][0]["text"], "first user");
    assert_eq!(turns[0]["body_bytes"], 37);
    assert_eq!(
        turns[0]["body_sha256"],
        "1ecf0eba2029e64b01236bd8add7ee02082b97889cdf7ddbe4b578dc136a843d"
    );
    assert_eq!(turns[1]["body_state"], "inline");
    assert_eq!(turns[2]["body_state"], "omitted_oversize");
    assert!(turns[2]["body"].is_null());
    assert!(turns[0]["parent_turn_id"].is_null());
    assert_eq!(turns[1]["parent_turn_id"], turns[0]["turn_id"]);
    assert_eq!(turns[2]["parent_turn_id"], turns[1]["turn_id"]);
    assert!(turns.iter().all(|turn| {
        turn["turn_id"]
            .as_str()
            .is_some_and(|turn_id| !turn_id.is_empty())
    }));
    for page in &pages {
        assert_eq!(page["page_turn_count"], 1);
        assert!(page["source_bytes_examined"].as_u64().unwrap() <= 131072);
        assert_eq!(page["scan_progress"], false);
    }
    let terminal = pages.last().expect("terminal page");
    assert_eq!(terminal["snapshot_complete"], true);
    assert!(terminal["next_page_token"].is_null());
    assert!(terminal["resume_token"].as_str().is_some());
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_stops_before_an_incomplete_interior_assistant() {
    let session_id = "ses_turn_pages_interior_gap";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.insert_incomplete_assistant(
        "msg_002_gap",
        2500,
        "msg_002",
        "interior assistant still receiving parts",
    );
    let path = prepend_path(fake_opencode.dir());
    let params = session_turn_page_beginning_params(session_id, "canonical_ingest", None);

    let first = read_turns_result(params.clone(), &path);
    let terminal = continue_turn_page(&params, &first, &path);
    let turns = [&first, &terminal]
        .into_iter()
        .flat_map(|page| page["turns"].as_array().expect("turns array"))
        .collect::<Vec<_>>();

    assert_eq!(terminal["snapshot_complete"], true);
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["body"][0]["text"], "first user");
    assert_eq!(turns[1]["body"][0]["text"], "first assistant");
    assert!(turns.iter().all(|turn| {
        turn["body"].as_array().is_some_and(|body| {
            body.iter().all(|part| {
                part["text"].as_str() != Some("interior assistant still receiving parts")
            })
        })
    }));
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_splits_before_the_selected_response_budget() {
    let session_id = "ses_turn_pages_response_budget";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.remove_trailing_assistant();
    let path = prepend_path(fake_opencode.dir());
    let mut params = session_turn_page_beginning_params(session_id, "canonical_ingest", None);
    params["max_turns"] = json!(3);
    params["max_response_bytes"] = json!(2560);

    let output = invoke_with_host_and_env(
        "session.read_turns",
        params,
        json!({
            "env": {
                "TERM": "xterm-256color",
                "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
            }
        }),
        &[("PATH", path.as_str())],
    );
    assert!(
        output.stdout.len() <= 2560,
        "success envelope exceeded the selected response budget"
    );
    let page = success_result(
        output,
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    );

    let count = page["page_turn_count"].as_u64().expect("page turn count");
    assert!(
        count > 0 && count < 3,
        "the response budget must split the page"
    );
    assert_eq!(page["snapshot_complete"], false);
    assert!(page["next_page_token"].as_str().is_some());
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_preserves_metadata_when_response_budget_omits_inline_body() {
    let session_id = "ses_turn_pages_response_omission";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.clear_messages();
    fake_opencode.append_user("msg_large", 1000, &"z".repeat(1500));
    let path = prepend_path(fake_opencode.dir());
    let mut inline_params =
        session_turn_page_beginning_params(session_id, "canonical_ingest", None);
    inline_params["max_inline_body_bytes"] = json!(2048);
    let inline = read_turns_result(inline_params.clone(), &path);
    assert_eq!(inline["turns"][0]["body_state"], "inline");

    let mut omitted_params = inline_params;
    omitted_params["max_response_bytes"] = json!(2048);
    let omitted = read_turns_result(omitted_params, &path);
    assert_eq!(omitted["turns"][0]["body_state"], "omitted_oversize");
    assert!(omitted["turns"][0]["body"].is_null());
    for field in ["body_bytes", "body_sha256", "canonical_text_sha256"] {
        assert_eq!(omitted["turns"][0][field], inline["turns"][0][field]);
    }
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_uses_the_actual_request_id_for_small_response_budgets() {
    let session_id = "s";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let mut params = session_turn_page_beginning_params(session_id, "canonical_ingest", None);
    params["max_response_bytes"] = json!(2048);

    let mut request = support::request_envelope(
        "session.read_turns",
        params,
        json!({
            "env": {
                "TERM": "xterm-256color",
                "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
            }
        }),
    );
    request["request_id"] = json!("r".repeat(256));
    let output =
        invoke_with_request_and_env("session.read_turns", request, &[("PATH", path.as_str())]);
    assert!(output.stdout.len() <= 2048);
    let page = success_result(
        output,
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    );

    assert_eq!(page["page_turn_count"], 1);
    fake_opencode.assert_no_export();
}

#[test]
fn contract_adjacent_tool_paths_reach_the_native_opencode_child() {
    let session_id = "ses_adjacent_tool_paths";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fs::write(
        fake_opencode.dir().join("opencode1"),
        format!(
            "#!/bin/sh\n\
             printf 'agent_bash_bin=%s\\n' \"${{AGENT_BASH_BIN-}}\" >> '{}'\n\
             printf 'agent_runner_bin=%s\\n' \"${{AGENT_BASH_AGENT_RUNNER_BIN-}}\" >> '{}'\n\
             if [ \"$1\" = db ] && [ \"${{2:-}}\" = path ]; then printf '%s\\n' '{}'; exit 0; fi\n\
             if [ \"$1\" = db ] && [ \"${{3:-}}\" = --format ] && [ \"${{4:-}}\" = json ]; then exec /usr/bin/sqlite3 -json '{}' \"$2\"; fi\n\
             exit 64\n",
            fake_opencode.log_path.display(),
            fake_opencode.log_path.display(),
            fake_opencode.db_path.display(),
            fake_opencode.db_path.display(),
        ),
    )
    .expect("write environment-observing OpenCode fixture");
    make_fake_opencode_export_executable(&fake_opencode.dir().join("opencode1"));
    let install_dir = fake_opencode.dir().join("provider-install");
    fs::create_dir_all(&install_dir).expect("create provider install directory");
    let provider = install_dir.join("agent-runner-opencode");
    fs::copy(env!("CARGO_BIN_EXE_agent-runner-opencode"), &provider).expect("copy provider binary");
    let configured_agent_bash = install_dir.join("agent-bash-configured");
    let configured_agent_runner = install_dir.join("agents-configured");
    fs::write(
        install_dir.join("config.toml"),
        format!(
            "opencode_bin = {:?}\nagent_bash_bin = {:?}\nagent_runner_bin = {:?}\n",
            fake_opencode.dir().join("opencode").display().to_string(),
            configured_agent_bash.display().to_string(),
            configured_agent_runner.display().to_string(),
        ),
    )
    .expect("write adjacent provider config");
    let config_root = support::isolated_test_config_root("adjacent-tool-paths");
    let data_root = config_root.join("data");
    let request = support::request_envelope(
        "session.read_turns",
        session_turn_page_beginning_params(session_id, "canonical_ingest", None),
        json!({
            "config_root": config_root,
            "data_root": data_root,
            "env": {
                "TERM": "xterm-256color",
                "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
            }
        }),
    );
    support::ensure_default_runtime_settings(&request);
    fs::create_dir_all(install_dir.join("home")).expect("create configured provider home");
    let mut child = std::process::Command::new(&provider)
        .arg("session.read_turns")
        .env_clear()
        .env("HOME", install_dir.join("home"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn configured provider");
    child
        .stdin
        .take()
        .expect("provider stdin")
        .write_all(request.to_string().as_bytes())
        .expect("write provider request");
    let output = child.wait_with_output().expect("wait configured provider");
    let _ = success_result(
        output,
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    );
    let log = fs::read_to_string(&fake_opencode.log_path).expect("read fake OpenCode log");

    assert!(
        log.contains(&format!(
            "agent_bash_bin={}\n",
            configured_agent_bash.display()
        )),
        "{log}"
    );
    assert!(
        log.contains(&format!(
            "agent_runner_bin={}\n",
            configured_agent_runner.display()
        )),
        "{log}"
    );
}

#[test]
fn contract_session_read_turns_completes_an_empty_source_without_export() {
    let session_id = "ses_turn_pages_empty";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.clear_messages();
    let path = prepend_path(fake_opencode.dir());

    let page = read_turns_result(
        session_turn_page_beginning_params(session_id, "canonical_ingest", None),
        &path,
    );

    assert_eq!(page["page_index"], 0);
    assert_eq!(page["page_turn_count"], 0);
    assert_eq!(page["turns"], json!([]));
    assert_eq!(page["snapshot_complete"], true);
    assert!(page["next_page_token"].is_null());
    assert!(page["resume_token"].as_str().is_some());
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_freezes_snapshots_and_resumes_after_the_high_watermark() {
    let session_id = "ses_turn_pages_snapshot";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.remove_trailing_assistant();
    let path = prepend_path(fake_opencode.dir());
    let params = session_turn_page_beginning_params(session_id, "canonical_ingest", None);

    let first = read_turns_result(params.clone(), &path);
    fake_opencode.append_user("msg_005", 5000, "appended after snapshot start");
    let second = continue_turn_page(&params, &first, &path);
    let terminal = continue_turn_page(&params, &second, &path);
    assert_eq!(terminal["snapshot_complete"], true);
    let frozen_bodies = [&first, &second, &terminal]
        .into_iter()
        .flat_map(|page| page["turns"].as_array().expect("frozen turns"))
        .filter_map(|turn| turn["body"].get(0))
        .filter_map(|part| part["text"].as_str())
        .collect::<Vec<_>>();
    assert!(
        !frozen_bodies.contains(&"appended after snapshot start"),
        "a continuation must not widen its captured high-watermark"
    );

    let resumed = read_turns_result(
        session_turn_page_beginning_params(
            session_id,
            "canonical_ingest",
            terminal["resume_token"].as_str(),
        ),
        &path,
    );
    assert_eq!(
        resumed["turns"][0]["body"][0]["text"],
        "appended after snapshot start"
    );
    assert_eq!(resumed["snapshot_complete"], true);
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_invalidates_an_unread_message_update() {
    assert_unread_snapshot_mutation_invalidated("message update", |database| {
        database.update_message("msg_002");
    });
}

#[test]
fn contract_session_read_turns_invalidates_an_unread_message_delete() {
    assert_unread_snapshot_mutation_invalidated("message delete", |database| {
        database.delete_message("msg_002");
    });
}

#[test]
fn contract_session_read_turns_invalidates_an_unread_part_mutation() {
    assert_unread_snapshot_mutation_invalidated("part mutation", |database| {
        database.update_text_part("msg_002");
    });
}

#[test]
fn contract_session_read_turns_invalidates_a_backdated_incomplete_assistant() {
    assert_unread_snapshot_mutation_invalidated("backdated incomplete assistant", |database| {
        database.insert_incomplete_assistant(
            "msg_001_gap",
            1500,
            "msg_001",
            "inserted inside frozen prefix",
        );
    });
}

fn assert_unread_snapshot_mutation_invalidated(
    label: &str,
    mutate: impl FnOnce(&FakeOpencodeDatabase),
) {
    let session_id = format!("ses_turn_pages_{}", label.replace(' ', "_"));
    let fake_opencode = FakeOpencodeDatabase::new(&session_id);
    let path = prepend_path(fake_opencode.dir());
    let params = session_turn_page_beginning_params(&session_id, "canonical_ingest", None);
    let first = read_turns_result(params.clone(), &path);
    mutate(&fake_opencode);

    let response = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        session_turn_page_continuation_params(
            &params,
            first["snapshot_id"].as_str().expect("snapshot id"),
            first["next_page_token"].as_str().expect("next page token"),
        ),
        json!({
            "env": {
                "TERM": "xterm-256color",
                "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
            }
        }),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "snapshot_invalidated", "{label}");
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_observation_tail_anchors_before_following_user_turns() {
    let session_id = "ses_turn_pages_observation";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.remove_trailing_assistant();
    let path = prepend_path(fake_opencode.dir());

    let anchor = read_turns_result(session_turn_page_tail_params(session_id), &path);
    assert_eq!(anchor["turn_projection"], "user_observation");
    assert_eq!(anchor["page_turn_count"], 0);
    assert_eq!(anchor["snapshot_complete"], true);
    let resume_token = anchor["resume_token"].as_str().expect("tail resume token");

    let prompt = "new user observation";
    fake_opencode.append_user(
        "msg_005",
        5000,
        &format!(
            "{prompt}\n\n[OULIPOLY-DELIVERY 5169694dde0f40d1890c6e28e55bab275169694dde0f40d1890c6e28e55bab27]\n"
        ),
    );
    let mut params =
        session_turn_page_beginning_params(session_id, "user_observation", Some(resume_token));
    params["max_inline_body_bytes"] = json!(0);
    let observed = read_turns_result(params, &path);
    assert_eq!(observed["page_turn_count"], 1);
    assert_eq!(observed["turns"][0]["role"], "user");
    assert_eq!(observed["turns"][0]["body_state"], "omitted_oversize");
    assert!(observed["turns"][0]["body"].is_null());
    assert!(observed["turns"][0]["body_bytes"].as_u64().is_some());
    assert!(observed["turns"][0]["body_sha256"].as_str().is_some());
    assert_eq!(
        observed["turns"][0]["canonical_text_sha256"],
        format!("{:x}", sha2::Sha256::digest(prompt.as_bytes()))
    );
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_preserves_a_different_valid_user_authored_delivery_marker() {
    let session_id = "ses_turn_pages_authored_delivery_marker";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.remove_trailing_assistant();
    let path = prepend_path(fake_opencode.dir());
    let anchor = read_turns_result(session_turn_page_tail_params(session_id), &path);
    let authored = "user text\n\n[OULIPOLY-DELIVERY bbbbbbbb99999999bbbbbbbb99999999bbbbbbbb99999999bbbbbbbb99999999]";
    fake_opencode.append_user("msg_005", 5000, authored);
    let observed = read_turns_result(
        session_turn_page_beginning_params(
            session_id,
            "user_observation",
            anchor["resume_token"].as_str(),
        ),
        &path,
    );

    assert_eq!(observed["turns"][0]["body"][0]["text"], authored);
    assert_eq!(
        observed["turns"][0]["canonical_text_sha256"],
        format!("{:x}", sha2::Sha256::digest(authored.as_bytes()))
    );
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_rejects_observation_tokens_in_a_different_nonce_context() {
    let session_id = "ses_turn_pages_nonce_replay";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.remove_trailing_assistant();
    let path = prepend_path(fake_opencode.dir());
    let anchor_params = session_turn_page_tail_params(session_id);
    let anchor = read_turns_result(anchor_params, &path);
    let mut replay = session_turn_page_beginning_params(
        session_id,
        "user_observation",
        anchor["resume_token"].as_str(),
    );
    replay["expected_delivery_nonce"] =
        json!("aaaaaaaa99999999aaaaaaaa99999999aaaaaaaa99999999aaaaaaaa99999999");

    let response = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        replay,
        json!({
            "env": {
                "TERM": "xterm-256color",
                "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
            }
        }),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_session_page_token");
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_enforces_projection_specific_delivery_nonce_shape() {
    let session_id = "ses_turn_pages_nonce_validation";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let selected_host = json!({
        "env": {
            "TERM": "xterm-256color",
            "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
        }
    });
    let mut missing = session_turn_page_beginning_params(session_id, "user_observation", None);
    missing
        .as_object_mut()
        .expect("paging params object")
        .remove("expected_delivery_nonce");
    let missing_response = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        missing,
        selected_host.clone(),
        &[("PATH", path.as_str())],
    ));
    assert_eq!(
        missing_response["error"]["code"],
        "invalid_session_read_turns_params"
    );

    let mut forbidden = session_turn_page_beginning_params(session_id, "canonical_ingest", None);
    forbidden["expected_delivery_nonce"] = json!(OBSERVATION_DELIVERY_NONCE);
    let forbidden_response = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        forbidden,
        selected_host,
        &[("PATH", path.as_str())],
    ));
    assert_eq!(
        forbidden_response["error"]["code"],
        "invalid_session_read_turns_params"
    );
}

#[test]
fn contract_session_read_turns_rejects_tampering_and_source_replacement() {
    let session_id = "ses_turn_pages_invalid";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    let path = prepend_path(fake_opencode.dir());
    let selected_host = json!({
        "env": {
            "TERM": "xterm-256color",
            "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
        }
    });

    let unselected = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        session_turn_page_beginning_params(session_id, "canonical_ingest", None),
        json!({"env": {"TERM": "xterm-256color"}}),
        &[("PATH", path.as_str())],
    ));
    assert_eq!(
        unselected["error"]["code"],
        "session_turn_pages_not_selected"
    );

    let params = session_turn_page_beginning_params(session_id, "canonical_ingest", None);
    let first = read_turns_result(params.clone(), &path);
    let mut tampered = first["next_page_token"]
        .as_str()
        .expect("next page token")
        .to_string();
    let last = tampered.pop().expect("token byte");
    tampered.push(if last == '0' { '1' } else { '0' });
    let tampered_params = session_turn_page_continuation_params(
        &params,
        first["snapshot_id"].as_str().expect("snapshot id"),
        &tampered,
    );
    let tampered_response = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        tampered_params,
        selected_host.clone(),
        &[("PATH", path.as_str())],
    ));
    assert_eq!(
        tampered_response["error"]["code"],
        "invalid_session_page_token"
    );

    let token = first["next_page_token"].as_str().expect("next page token");
    let fields = token.split('.').collect::<Vec<_>>();
    let mut uppercase_digest = fields[2].to_uppercase();
    if uppercase_digest == fields[2] {
        uppercase_digest.replace_range(..1, "A");
    }
    let noncanonical_tokens = [
        format!("{}.{}.{}", fields[0], fields[1], uppercase_digest),
        format!(
            "{}.{}\n{}.{}",
            fields[0],
            &fields[1][..4],
            &fields[1][4..],
            fields[2]
        ),
    ];
    for noncanonical in noncanonical_tokens {
        let response = assert_error_envelope(invoke_with_host_and_env(
            "session.read_turns",
            session_turn_page_continuation_params(
                &params,
                first["snapshot_id"].as_str().expect("snapshot id"),
                &noncanonical,
            ),
            selected_host.clone(),
            &[("PATH", path.as_str())],
        ));
        assert_eq!(response["error"]["code"], "invalid_session_page_token");
    }

    let payload = base64::engine::general_purpose::STANDARD
        .decode(fields[1])
        .expect("decode public token payload");
    let mut forged_payload: Value = serde_json::from_slice(&payload).expect("token JSON");
    forged_payload["p"] = json!(forged_payload["p"].as_u64().expect("page index") + 10);
    let forged_payload = serde_json::to_vec(&forged_payload).expect("serialize forged payload");
    let runtime_identity = serde_json::from_slice::<Value>(&payload).expect("original token JSON")
        ["b"]["r"]
        .as_str()
        .expect("public runtime identity")
        .to_string();
    let mut old_public_preimage = b"agent-runner-opencode.session-turn-token.v1\0".to_vec();
    old_public_preimage.extend_from_slice(runtime_identity.as_bytes());
    old_public_preimage.push(0);
    old_public_preimage.extend_from_slice(&forged_payload);
    let old_public_digest = format!("{:x}", sha2::Sha256::digest(&old_public_preimage));
    let forged = format!(
        "{}.{}.{}",
        fields[0],
        base64::engine::general_purpose::STANDARD.encode(&forged_payload),
        old_public_digest
    );
    let forged_response = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        session_turn_page_continuation_params(
            &params,
            first["snapshot_id"].as_str().expect("snapshot id"),
            &forged,
        ),
        selected_host.clone(),
        &[("PATH", path.as_str())],
    ));
    assert_eq!(
        forged_response["error"]["code"],
        "invalid_session_page_token"
    );

    fake_opencode.replace_source_file();
    let replacement_response = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        session_turn_page_continuation_params(
            &params,
            first["snapshot_id"].as_str().expect("snapshot id"),
            first["next_page_token"].as_str().expect("next page token"),
        ),
        selected_host,
        &[("PATH", path.as_str())],
    ));
    assert_eq!(
        replacement_response["error"]["code"],
        "snapshot_invalidated"
    );
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_rejects_an_incompatible_source_schema_without_export() {
    let session_id = "ses_turn_pages_schema_mismatch";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.remove_required_message_index();
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_host_and_env(
        "session.read_turns",
        session_turn_page_beginning_params(session_id, "canonical_ingest", None),
        json!({
            "env": {
                "TERM": "xterm-256color",
                "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
            }
        }),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(
        response["error"]["code"],
        "opencode_session_source_schema_unsupported"
    );
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_ignores_a_bracketed_native_prefix() {
    let session_id = "ses_turn_pages_native_prefix";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.emit_bracketed_database_prefix();
    let path = prepend_path(fake_opencode.dir());

    let result = success_result(
        invoke_with_host_and_env(
            "session.read_turns",
            session_turn_page_beginning_params(session_id, "canonical_ingest", None),
            json!({
                "env": {
                    "TERM": "xterm-256color",
                    "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
                }
            }),
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    );

    assert_eq!(result["read_protocol"], "oulipoly.session_turn_pages/v1");
    assert_eq!(result["turn_projection"], "canonical_ingest");
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_read_turns_chunks_native_database_output_below_pipe_capacity() {
    let session_id = "ses_turn_pages_chunked_native_output";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    fake_opencode.clear_messages();
    let text = "x".repeat(40 * 1024);
    fake_opencode.append_user("msg_large_user", 5000, &text);
    let path = prepend_path(fake_opencode.dir());
    let mut params = session_turn_page_beginning_params(session_id, "canonical_ingest", None);
    params["max_inline_body_bytes"] = json!(64 * 1024);
    params["max_response_bytes"] = json!(128 * 1024);

    let result = success_result(
        invoke_with_host_and_env(
            "session.read_turns",
            params,
            json!({
                "env": {
                    "TERM": "xterm-256color",
                    "OULIPOLY_HOST_SESSION_TURN_PAGES_V1": "1"
                }
            }),
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionReadTurnsResponse",
        "session.schema.json#/$defs/SessionReadTurnsResult",
    );

    assert_eq!(result["page_turn_count"], 1);
    assert_eq!(result["turns"][0]["body_state"], "inline");
    assert_eq!(result["turns"][0]["body"][0]["text"], text);
    fake_opencode.assert_no_export();
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
fn contract_session_enumerate_packs_the_maximum_snapshot_population() {
    let native_rows = (0..256)
        .map(|index| {
            json!({
                "id": format!("ses-packed-{index:03}"),
                "title": format!("Packed {index}"),
                "directory": format!("/tmp/packed-{index:03}")
            })
        })
        .collect::<Vec<_>>();
    let native_output = serde_json::to_string(&native_rows).expect("serialize maximum native rows");
    let fake_opencode = FakeOpencodeSessionList::with_output(&native_output, "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-packed-session-snapshot");
    let config_root = support::isolated_test_config_root("packed-session-snapshot");
    fs::create_dir_all(&data_root).expect("create packed session snapshot data root");
    let host = json!({
        "config_root": config_root.to_string_lossy(),
        "data_root": data_root.to_string_lossy()
    });

    let first_request = support::request_envelope(
        "session.enumerate",
        session_enumerate_limit_params(1),
        host.clone(),
    );
    let first = success_result(
        support::invoke_with_request_and_env(
            "session.enumerate",
            first_request.clone(),
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_eq!(first["sessions"].as_array().map(Vec::len), Some(1));
    assert_eq!(first["complete"], false);

    let exact_first_retry = success_result(
        support::invoke_with_request_and_env(
            "session.enumerate",
            first_request,
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_eq!(exact_first_retry, first);

    let snapshot_root = data_root.join("provider-state/opencode/session-enumeration-snapshots");
    let snapshot_directories = fs::read_dir(&snapshot_root)
        .expect("read packed snapshot root")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .collect::<Vec<_>>();
    assert_eq!(snapshot_directories.len(), 1);
    let snapshot_directory = snapshot_directories[0].path();
    let mut snapshot_files = fs::read_dir(&snapshot_directory)
        .expect("read packed snapshot")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    snapshot_files.sort();
    assert_eq!(
        snapshot_files,
        vec!["manifest.json", "rows.bin", "warnings.json"],
        "the maximum admitted population must use a fixed file count"
    );
    let manifest_bytes = fs::metadata(snapshot_directory.join("manifest.json"))
        .expect("read maximum-population manifest metadata")
        .len();
    assert!(
        manifest_bytes > 16 * 1024 && manifest_bytes <= 32 * 1024,
        "the maximum-population manifest must fit its authored read/write envelope: {manifest_bytes}"
    );

    let cursor = first["next_cursor"]
        .as_str()
        .expect("maximum-population first-page cursor");
    let terminal = success_result(
        invoke_with_host_and_env(
            "session.enumerate",
            session_enumerate_cursor_params(255, cursor),
            host,
            &[("PATH", path.as_str())],
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    let terminal_sessions = terminal["sessions"]
        .as_array()
        .expect("maximum-population terminal sessions");
    assert_eq!(terminal_sessions.len(), 255);
    assert_eq!(
        terminal_sessions[0]["provider_session_id"],
        "ses-packed-001"
    );
    assert_eq!(
        terminal_sessions[254]["provider_session_id"],
        "ses-packed-255"
    );
    assert_eq!(terminal["complete"], true);
    assert!(terminal["next_cursor"].is_null());

    let snapshot_directories = fs::read_dir(&snapshot_root)
        .expect("read packed snapshot root")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .collect::<Vec<_>>();
    assert_eq!(
        snapshot_directories.len(),
        1,
        "the maximum admitted population must remain available for bounded exact terminal replay after response flush"
    );

    fs::remove_dir_all(&data_root).expect("remove packed session snapshot data root");
    fs::remove_dir_all(&config_root).expect("remove packed session snapshot config root");
}

#[test]
fn contract_session_enumerate_terminal_claim_rejects_a_different_request() {
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
        consumed["error"]["code"], "session_enumeration_snapshot_terminal_handoff_in_progress",
        "a terminal cursor remains bound to its exact request during bounded replay retention"
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
    let _path_environment = PATH_ENVIRONMENT_LOCK.lock().expect("lock process PATH");
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
fn contract_session_enumerate_terminal_initial_retry_replays_after_successful_flush_loss() {
    let _path_environment = PATH_ENVIRONMENT_LOCK.lock().expect("lock process PATH");
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_multiple_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-terminal-initial-replay");
    let config_root = support::isolated_test_config_root("terminal-initial-replay");
    fs::create_dir_all(&data_root).expect("create terminal initial replay data root");
    let host = json!({
        "config_root": config_root.to_string_lossy(),
        "data_root": data_root.to_string_lossy()
    });
    let mut request = support::validated_request_envelope(
        "session.enumerate",
        session_enumerate_params(),
        host,
        "session.schema.json#/$defs/SessionEnumerateRequest",
    );
    request["request_id"] = json!("req-session-enumerate-terminal-initial-replay");
    support::ensure_default_runtime_settings(&request);
    let request_bytes = serde_json::to_vec(&request).expect("serialize terminal enumeration");
    let args = vec![
        "agent-runner-opencode".to_string(),
        "session.enumerate".to_string(),
    ];
    let prior_path = std::env::var_os("PATH");
    std::env::set_var("PATH", &path);
    let mut discarded_response = AcceptAndDiscard;
    assert_eq!(
        agent_runner_opencode::write_invocation(&args, &request_bytes, &mut discarded_response),
        0,
        "provider-local write and flush succeed even though the simulated consumer discards the response"
    );
    fs::remove_file(fake_opencode.log_path()).expect("clear first native-list evidence");
    fake_opencode.replace_output("[]", "", 0);

    let mut retry_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(&args, &request_bytes, &mut retry_stdout),
        0
    );
    let retry_response: Value =
        serde_json::from_slice(&retry_stdout).expect("parse terminal initial retry response");
    assert_multiple_enumerate_result(&retry_response["result"]);
    assert!(
        !fake_opencode.log_path().exists(),
        "terminal exact retry must consult durable request custody before native relisting"
    );
    let snapshot_root = data_root.join("provider-state/opencode/session-enumeration-snapshots");
    assert!(
        fs::read_dir(&snapshot_root)
            .expect("enumeration snapshot root")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .count()
            == 1,
        "successful local flush cannot retire terminal replay without consumer acknowledgement"
    );

    let mut second_retry_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(&args, &request_bytes, &mut second_retry_stdout),
        0
    );
    let second_retry: Value = serde_json::from_slice(&second_retry_stdout)
        .expect("parse second terminal initial retry response");
    assert_eq!(second_retry["result"], retry_response["result"]);

    match prior_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    fs::remove_dir_all(&data_root).expect("remove terminal initial replay data root");
    fs::remove_dir_all(&config_root).expect("remove terminal initial replay config root");
}

#[test]
fn contract_session_enumerate_rejects_cursor_from_expired_recreated_snapshot() {
    let _path_environment = PATH_ENVIRONMENT_LOCK.lock().expect("lock process PATH");
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-snapshot-incarnation-cursor");
    let config_root = support::isolated_test_config_root("snapshot-incarnation-cursor");
    fs::create_dir_all(&data_root).expect("create snapshot incarnation data root");
    let host = json!({
        "config_root": config_root.to_string_lossy(),
        "data_root": data_root.to_string_lossy()
    });
    let mut initial_request = support::request_envelope(
        "session.enumerate",
        session_enumerate_limit_params(1),
        host.clone(),
    );
    initial_request["request_id"] = json!("req-session-enumerate-snapshot-incarnation");
    support::ensure_default_runtime_settings(&initial_request);
    let env = [("PATH", path.as_str())];
    let first = success_result(
        support::invoke_with_request_and_env("session.enumerate", initial_request.clone(), &env),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_eq!(first["sessions"][0]["provider_session_id"], "ses_limit_one");
    let stale_cursor = first["next_cursor"]
        .as_str()
        .expect("first snapshot continuation cursor")
        .to_string();

    let snapshot_root = data_root.join("provider-state/opencode/session-enumeration-snapshots");
    let snapshot = fs::read_dir(&snapshot_root)
        .expect("read snapshot incarnation root")
        .filter_map(Result::ok)
        .find(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .expect("first snapshot incarnation")
        .path();
    let manifest_path = snapshot.join("manifest.json");
    let mut manifest: Value = serde_json::from_slice(
        &fs::read(&manifest_path).expect("read first snapshot incarnation manifest"),
    )
    .expect("parse first snapshot incarnation manifest");
    manifest["expires_at_unix_ms"] = json!(0);
    fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest).expect("encode expired snapshot incarnation manifest"),
    )
    .expect("expire first snapshot incarnation");

    fake_opencode.replace_output(changed_session_list_limit_json(), "", 0);
    let recreated = success_result(
        support::invoke_with_request_and_env("session.enumerate", initial_request, &env),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_eq!(
        recreated["sessions"][0]["provider_session_id"],
        "ses_changed_one"
    );
    let recreated_cursor = recreated["next_cursor"]
        .as_str()
        .expect("recreated snapshot continuation cursor");
    assert_ne!(
        recreated_cursor, stale_cursor,
        "a fresh snapshot incarnation must issue fresh continuation authority"
    );

    let stale = assert_error_envelope(support::invoke_with_request_and_env(
        "session.enumerate",
        support::request_envelope(
            "session.enumerate",
            session_enumerate_cursor_params(2, &stale_cursor),
            host.clone(),
        ),
        &env,
    ));
    assert_eq!(stale["error"]["code"], "invalid_session_enumerate_cursor");

    let continuation = success_result(
        support::invoke_with_request_and_env(
            "session.enumerate",
            support::request_envelope(
                "session.enumerate",
                session_enumerate_cursor_params(2, recreated_cursor),
                host,
            ),
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_eq!(
        continuation["sessions"][0]["provider_session_id"],
        "ses_changed_two"
    );
    assert_eq!(continuation["complete"], true);

    fs::remove_dir_all(&data_root).expect("remove snapshot incarnation data root");
    fs::remove_dir_all(&config_root).expect("remove snapshot incarnation config root");
}

#[test]
fn contract_terminal_snapshot_replay_is_bounded_and_terminal_eviction_preserves_throughput() {
    let _path_environment = PATH_ENVIRONMENT_LOCK.lock().expect("lock process PATH");
    let fake_opencode = FakeOpencodeSessionList::with_output("[]", "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-terminal-snapshot-capacity");
    let config_root = support::isolated_test_config_root("terminal-snapshot-capacity");
    fs::create_dir_all(&data_root).expect("create terminal snapshot capacity data root");
    let host = json!({
        "config_root": config_root.to_string_lossy(),
        "data_root": data_root.to_string_lossy()
    });
    let env = [("PATH", path.as_str())];
    let mut first_request = None;
    for _ in 0..32 {
        let request = support::validated_request_envelope(
            "session.enumerate",
            session_enumerate_params(),
            host.clone(),
            "session.schema.json#/$defs/SessionEnumerateRequest",
        );
        support::ensure_default_runtime_settings(&request);
        let result = success_result(
            support::invoke_with_request_and_env("session.enumerate", request.clone(), &env),
            "session.schema.json#/$defs/SessionEnumerateResponse",
            "session.schema.json#/$defs/SessionEnumerateResult",
        );
        assert_empty_enumerate_result(&result);
        first_request.get_or_insert(request);
    }
    fs::remove_file(fake_opencode.log_path()).expect("clear capacity native-list evidence");
    let exact_replay = success_result(
        support::invoke_with_request_and_env(
            "session.enumerate",
            first_request.expect("first terminal request"),
            &env,
        ),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_empty_enumerate_result(&exact_replay);
    assert!(
        !fake_opencode.log_path().exists(),
        "capacity saturation must not deny or relist an exact retained terminal request"
    );

    let overflow_request = support::validated_request_envelope(
        "session.enumerate",
        session_enumerate_params(),
        host.clone(),
        "session.schema.json#/$defs/SessionEnumerateRequest",
    );
    support::ensure_default_runtime_settings(&overflow_request);
    let admitted = success_result(
        support::invoke_with_request_and_env("session.enumerate", overflow_request, &env),
        "session.schema.json#/$defs/SessionEnumerateResponse",
        "session.schema.json#/$defs/SessionEnumerateResult",
    );
    assert_empty_enumerate_result(&admitted);

    let snapshot_root = data_root.join("provider-state/opencode/session-enumeration-snapshots");
    assert_eq!(
        fs::read_dir(&snapshot_root)
            .expect("read reclaimed snapshot capacity root")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .count(),
        32,
        "the oldest terminal replay must be evicted before admitting useful new work"
    );

    fs::remove_dir_all(&data_root).expect("remove terminal snapshot capacity data root");
    fs::remove_dir_all(&config_root).expect("remove terminal snapshot capacity config root");
}

#[test]
fn contract_session_enumerate_terminal_continuation_replays_after_successful_flush_loss() {
    let _path_environment = PATH_ENVIRONMENT_LOCK.lock().expect("lock process PATH");
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-response-loss-session-snapshot");
    let config_root = support::isolated_test_config_root("response-loss-session-snapshot");
    fs::create_dir_all(&data_root).expect("create isolated session snapshot data root");
    let host = json!({
        "config_root": config_root.to_string_lossy(),
        "data_root": data_root.to_string_lossy()
    });
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
    let mut discarded_response = AcceptAndDiscard;
    let lost_exit = agent_runner_opencode::write_invocation(
        &args,
        &serde_json::to_vec(&request).expect("serialize enumeration request"),
        &mut discarded_response,
    );
    assert_eq!(
        lost_exit, 0,
        "provider-local write and flush succeed even though the terminal page is discarded"
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

    let mut replay_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &serde_json::to_vec(&request).expect("serialize repeated terminal enumeration retry"),
            &mut replay_stdout,
        ),
        0
    );
    match prior_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    let replay: Value =
        serde_json::from_slice(&replay_stdout).expect("parse repeated terminal cursor response");
    support::assert_valid(
        &replay,
        "session.schema.json#/$defs/SessionEnumerateResponse",
    );
    assert_eq!(
        replay["result"], retry_response["result"],
        "the exact terminal continuation remains replayable for the bounded retention window"
    );
    fs::remove_dir_all(&data_root).expect("remove isolated session snapshot data root");
    fs::remove_dir_all(&config_root).expect("remove isolated session snapshot config root");
}

#[test]
fn contract_terminal_snapshot_claim_blocks_initial_retry_during_response_handoff() {
    let _path_environment = PATH_ENVIRONMENT_LOCK.lock().expect("lock process PATH");
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-terminal-snapshot-claim");
    let config_root = support::isolated_test_config_root("terminal-snapshot-claim");
    fs::create_dir_all(&data_root).expect("create isolated session snapshot data root");
    let host = json!({
        "config_root": config_root.to_string_lossy(),
        "data_root": data_root.to_string_lossy()
    });
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
    fs::remove_dir_all(&config_root).expect("remove isolated session snapshot config root");
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
    let _path_environment = PATH_ENVIRONMENT_LOCK.lock().expect("lock process PATH");
    let fake_opencode = FakeOpencodeSessionList::with_output(session_list_limit_json(), "", 0);
    let path = prepend_path(fake_opencode.dir());
    let data_root = unique_temp_dir("agent-runner-opencode-terminal-older-cursor-race");
    let config_root = support::isolated_test_config_root("terminal-older-cursor-race");
    fs::create_dir_all(&data_root).expect("create terminal older-cursor data root");
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
    fs::remove_dir_all(&config_root).expect("remove terminal older-cursor config root");
}

#[test]
fn contract_terminal_snapshot_retention_survives_nested_successful_flushes() {
    let _path_environment = PATH_ENVIRONMENT_LOCK.lock().expect("lock process PATH");
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
    let mut interleaving = RetrySnapshotDuringTerminalFlush {
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
        Some(2),
        "a competing initial request must remain blocked by terminal ownership: {}",
        String::from_utf8_lossy(&interleaving.initial_retry_stdout)
    );
    let competing: Value = serde_json::from_slice(&interleaving.initial_retry_stdout)
        .expect("parse competing initial response");
    assert_eq!(
        competing["error"]["code"],
        "session_enumeration_snapshot_terminal_handoff_in_progress"
    );
    let mut retained_terminal_stdout = Vec::new();
    assert_eq!(
        agent_runner_opencode::write_invocation(
            &args,
            &terminal_bytes,
            &mut retained_terminal_stdout,
        ),
        0,
        "nested successful flushes must retain exact terminal replay"
    );
    let retained_terminal: Value = serde_json::from_slice(&retained_terminal_stdout)
        .expect("parse retained terminal response");
    assert_eq!(retained_terminal["result"]["complete"], true);

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
fn contract_session_enumerate_rejects_an_untyped_native_row() {
    let fake_opencode =
        FakeOpencodeSessionList::with_output(r#"[{"created":"4102444800000"}]"#, "", 0);
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_env(
        "session.enumerate",
        session_enumerate_params(),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_opencode_session_list");
    assert!(
        response["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("row 0")),
        "the adapter-owned row failure must retain its source index"
    );
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
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
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
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_capture_rejects_live_report_workspace_mismatch() {
    let session_id = fixture_session_id();
    let invocation_uuid = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
    let path = prepend_path(fake_opencode.dir());

    let response = assert_error_envelope(invoke_with_host_and_env(
        "session.capture",
        live_capture_params(session_id, invocation_uuid),
        json!({"working_directory": "/tmp/not-the-exported-workspace"}),
        &[("PATH", path.as_str())],
    ));

    assert_eq!(response["error"]["code"], "invalid_session_capture_params");
    fake_opencode.assert_no_export();
}

#[test]
fn contract_session_capture_rejects_live_report_invocation_mismatch() {
    let session_id = fixture_session_id();
    let fake_opencode = FakeOpencodeDatabase::new(session_id);
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
