//! Declared roles: recorder, mapper, validator
//!
//! Provider-owned, redacted operation evidence. The host contract does not
//! currently carry an authenticated principal or delegation, so this ledger
//! records that absence explicitly instead of inventing an identity.

use crate::durable_fs;
use crate::encoding::{now_unix_ms, sha256_hex};
use crate::envelope::{ProviderFailure, RequestEnvelope};
use crate::operation_bounds;
use crate::path_guard;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const ACTIVITY_DIR: &str = "provider-state/opencode/activity";
const ACTIVITY_FILE: &str = "operations.jsonl";
const ACTIVITY_ALTERNATE_FILE: &str = ".operations-alternate.jsonl";
const ACTIVITY_HEAD_FILE: &str = ".operations-head.json";
const ACTIVITY_LOCK: &str = ".operations.lock";
const ACTIVITY_SCHEMA_VERSION: u32 = 2;
const MAX_ACTIVITY_BYTES: usize = 8 * 1024 * 1024;
const MAX_ACTIVITY_EVENTS: usize = 4_096;
const MAX_ACTIVITY_EVENT_BYTES: usize = 64 * 1024;
const MAX_ACTIVITY_HEAD_BYTES: usize = 16 * 1024;

pub struct ActivityContext {
    host: HostContextSnapshot,
    request_id: String,
    provider_instance_id: Option<String>,
    subcommand: String,
}

struct HostContextSnapshot {
    app: String,
    app_version: Option<String>,
    data_root: Option<String>,
}

#[derive(Clone, Default, Serialize)]
pub struct ActivityTargets {
    identities: Vec<ActivityIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempted_provider_args_sha256: Option<String>,
    #[serde(skip)]
    seen: HashSet<ActivityIdentity>,
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize)]
struct ActivityIdentity {
    kind: &'static str,
    value: String,
    status: ActivityIdentityStatus,
    provenance: String,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityIdentityStatus {
    Attempted,
    Resolved,
    Generated,
}

#[derive(Deserialize, Serialize)]
struct ActivityLedgerHead {
    schema_version: u32,
    active_file: String,
    sequence: u64,
    previous_event_sha256: String,
    committed_bytes: u64,
    event_count: usize,
}

impl ActivityContext {
    pub fn from_request(request: &RequestEnvelope, subcommand: &str) -> Self {
        Self {
            host: HostContextSnapshot {
                app: request.host.app.clone(),
                app_version: request.host.app_version.clone(),
                data_root: request.host.data_root.clone(),
            },
            request_id: request.request_id.clone(),
            provider_instance_id: request.provider_instance_id.clone(),
            subcommand: subcommand.to_string(),
        }
    }

    pub fn started(&self, targets: &ActivityTargets) -> Result<(), ProviderFailure> {
        self.record("started", targets, json!({}))
    }

    pub fn succeeded(
        &self,
        exit_code: i32,
        targets: &ActivityTargets,
    ) -> Result<(), ProviderFailure> {
        self.record(
            "completed",
            targets,
            json!({ "ok": true, "exit_code": exit_code }),
        )
    }

    pub fn failed(
        &self,
        failure: &ProviderFailure,
        targets: &ActivityTargets,
    ) -> Result<(), ProviderFailure> {
        self.record(
            "completed",
            targets,
            json!({
                "ok": false,
                "category": failure.category,
                "code": failure.code,
                "retryable": failure.retryable,
                "exit_code": failure.exit_code,
            }),
        )
    }

    fn record(
        &self,
        phase: &str,
        targets: &ActivityTargets,
        outcome: Value,
    ) -> Result<(), ProviderFailure> {
        let Some(root) = self.root() else {
            return Ok(());
        };
        write_activity(&root, self, phase, targets, outcome).map_err(|error| {
            ProviderFailure::internal(
                &self.request_id,
                "activity_evidence_write_failed",
                format!("failed to persist provider activity evidence: {error}"),
            )
        })
    }

    fn root(&self) -> Option<PathBuf> {
        self.host
            .data_root
            .as_deref()
            .filter(|root| !root.trim().is_empty())
            .map(|root| Path::new(root).join(ACTIVITY_DIR))
    }
}

impl ActivityTargets {
    pub fn push(
        &mut self,
        kind: &'static str,
        value: impl Into<String>,
        status: ActivityIdentityStatus,
        provenance: impl Into<String>,
    ) {
        let value = value.into();
        let provenance = provenance.into();
        if value.trim().is_empty() {
            return;
        }
        let identity = ActivityIdentity {
            kind,
            value,
            status,
            provenance,
        };
        if self.seen.insert(identity.clone()) {
            self.identities.push(identity);
        }
    }

    pub fn attempted(
        &mut self,
        kind: &'static str,
        value: impl Into<String>,
        provenance: impl Into<String>,
    ) {
        self.push(kind, value, ActivityIdentityStatus::Attempted, provenance);
    }

    pub fn resolved(
        &mut self,
        kind: &'static str,
        value: impl Into<String>,
        provenance: impl Into<String>,
    ) {
        self.push(kind, value, ActivityIdentityStatus::Resolved, provenance);
    }

    pub fn generated(
        &mut self,
        kind: &'static str,
        value: impl Into<String>,
        provenance: impl Into<String>,
    ) {
        self.push(kind, value, ActivityIdentityStatus::Generated, provenance);
    }

    pub fn provider_args(&mut self, args: &Value) {
        self.attempted_provider_args_sha256 = Some(sha256_hex(args.to_string().as_bytes()));
    }

    pub fn extend(&mut self, other: ActivityTargets) {
        for identity in other.identities {
            self.push(
                identity.kind,
                identity.value,
                identity.status,
                identity.provenance,
            );
        }
        if other.attempted_provider_args_sha256.is_some() {
            self.attempted_provider_args_sha256 = other.attempted_provider_args_sha256;
        }
    }
}

fn write_activity(
    root: &Path,
    context: &ActivityContext,
    phase: &str,
    targets: &ActivityTargets,
    outcome: Value,
) -> std::io::Result<()> {
    let data_root = context
        .host
        .data_root
        .as_deref()
        .map(Path::new)
        .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "missing data root"))?;
    path_guard::confined_target(data_root, root)?;
    let lock_path = path_guard::confined_target(data_root, &root.join(ACTIVITY_LOCK))?;
    let path = path_guard::confined_target(data_root, &root.join(ACTIVITY_FILE))?;
    durable_fs::create_private_directories(root)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    if !operation_bounds::lock_exclusive_for(&lock, std::time::Duration::ZERO)? {
        return Err(std::io::Error::new(
            ErrorKind::WouldBlock,
            "activity ledger is busy; best-effort evidence was skipped",
        ));
    }
    let mut head = read_or_initialize_activity_head(root, &path)?;
    let mut event = json!({
        "schema_version": ACTIVITY_SCHEMA_VERSION,
        "sequence": head.sequence + 1,
        "phase": phase,
        "subcommand": context.subcommand,
        "request_id": context.request_id,
        "provider_instance_id": context.provider_instance_id,
        "host": {
            "app": context.host.app,
            "app_version": context.host.app_version,
        },
        "authenticated_principal": null,
        "delegation": null,
        "principal_binding": "not_supplied_by_oulipoly.provider/v1",
        "targets": targets,
        "outcome": outcome,
        "recorded_at_unix_ms": now_unix_ms(),
        "previous_event_sha256": head.previous_event_sha256,
    });
    event["event_sha256"] = json!(sha256_hex(event.to_string().as_bytes()));
    let event_line = serde_json::to_string(&event)?;
    if event_line.len() + 1 > MAX_ACTIVITY_EVENT_BYTES {
        return Err(invalid_ledger(
            "activity event exceeds its supported encoded-size bound",
        ));
    }
    let mut event_bytes = event_line.into_bytes();
    event_bytes.push(b'\n');
    let rotate = head.event_count >= MAX_ACTIVITY_EVENTS
        || head
            .committed_bytes
            .saturating_add(event_bytes.len() as u64)
            > MAX_ACTIVITY_BYTES as u64;
    if rotate {
        let next_file = if head.active_file == ACTIVITY_FILE {
            ACTIVITY_ALTERNATE_FILE
        } else {
            ACTIVITY_FILE
        };
        let next_path = root.join(next_file);
        write_activity_file_atomic(&next_path, &event_bytes)?;
        head.active_file = next_file.to_string();
        head.event_count = 1;
        head.committed_bytes = event_bytes.len() as u64;
    } else {
        let active_path = activity_file_path(root, &head.active_file)?;
        let mut active = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        active.write_all(&event_bytes)?;
        active.sync_all()?;
        set_private_file(&active_path)?;
        head.event_count += 1;
        head.committed_bytes = head
            .committed_bytes
            .saturating_add(event_bytes.len() as u64);
    }
    head.sequence += 1;
    head.previous_event_sha256 = event["event_sha256"]
        .as_str()
        .expect("activity event digest was just authored")
        .to_string();
    write_activity_head(root, &head)
}

fn read_or_initialize_activity_head(
    root: &Path,
    legacy_path: &Path,
) -> std::io::Result<ActivityLedgerHead> {
    let head_path = root.join(ACTIVITY_HEAD_FILE);
    let mut head = match durable_fs::read_file_bounded(&head_path, MAX_ACTIVITY_HEAD_BYTES) {
        Ok(bytes) => serde_json::from_slice::<ActivityLedgerHead>(&bytes)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let existing = match durable_fs::read_file_bounded(legacy_path, MAX_ACTIVITY_BYTES) {
                Ok(bytes) => String::from_utf8(bytes)
                    .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?,
                Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
                Err(error) => return Err(error),
            };
            let (sequence, previous_event_sha256, event_count) = validate_ledger(&existing)?;
            let initialized = ActivityLedgerHead {
                schema_version: 1,
                active_file: ACTIVITY_FILE.to_string(),
                sequence,
                previous_event_sha256,
                committed_bytes: existing.len() as u64,
                event_count,
            };
            write_activity_head(root, &initialized)?;
            initialized
        }
        Err(error) => return Err(error),
    };
    validate_activity_head(&head)?;
    repair_activity_tail(root, &mut head)?;
    Ok(head)
}

fn validate_activity_head(head: &ActivityLedgerHead) -> std::io::Result<()> {
    if head.schema_version != 1
        || !matches!(
            head.active_file.as_str(),
            ACTIVITY_FILE | ACTIVITY_ALTERNATE_FILE
        )
        || head.event_count > MAX_ACTIVITY_EVENTS
        || head.committed_bytes > MAX_ACTIVITY_BYTES as u64
        || (head.event_count == 0
            && (head.sequence != 0
                || !head.previous_event_sha256.is_empty()
                || head.committed_bytes != 0))
    {
        return Err(invalid_ledger("activity head is inconsistent"));
    }
    Ok(())
}

fn repair_activity_tail(root: &Path, head: &mut ActivityLedgerHead) -> std::io::Result<()> {
    let path = activity_file_path(root, &head.active_file)?;
    let mut file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound && head.committed_bytes == 0 => {
            return Ok(())
        }
        Err(error) => return Err(error),
    };
    let actual_bytes = file.metadata()?.len();
    if actual_bytes < head.committed_bytes {
        return Err(invalid_ledger(
            "activity file is shorter than its committed head",
        ));
    }
    if actual_bytes > head.committed_bytes {
        file.set_len(head.committed_bytes)?;
        file.sync_all()?;
        durable_fs::sync_directory(root)?;
    }
    if head.event_count == 0 {
        return Ok(());
    }
    validate_activity_tail(&mut file, head)
}

fn validate_activity_tail(file: &mut fs::File, head: &ActivityLedgerHead) -> std::io::Result<()> {
    let retained = head.committed_bytes.min(MAX_ACTIVITY_EVENT_BYTES as u64);
    file.seek(SeekFrom::Start(head.committed_bytes - retained))?;
    let mut bytes = Vec::with_capacity(retained as usize);
    file.take(retained).read_to_end(&mut bytes)?;
    let line = bytes
        .split(|byte| *byte == b'\n')
        .rev()
        .find(|line| !line.trim_ascii().is_empty())
        .ok_or_else(|| invalid_ledger("activity file has no committed tail event"))?;
    let event: Value = serde_json::from_slice(line)
        .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
    validate_activity_event(&event, head.sequence, &head.previous_event_sha256)
}

fn activity_file_path(root: &Path, active_file: &str) -> std::io::Result<PathBuf> {
    if !matches!(active_file, ACTIVITY_FILE | ACTIVITY_ALTERNATE_FILE) {
        return Err(invalid_ledger("activity head names an unsupported file"));
    }
    Ok(root.join(active_file))
}

fn write_activity_head(root: &Path, head: &ActivityLedgerHead) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(head)?;
    if bytes.len() > MAX_ACTIVITY_HEAD_BYTES {
        return Err(invalid_ledger(
            "activity head exceeds its encoded-size bound",
        ));
    }
    write_activity_file_atomic(&root.join(ACTIVITY_HEAD_FILE), &bytes)
}

fn write_activity_file_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("activity path has no parent"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    set_private_file(path)?;
    durable_fs::sync_directory(parent)
}

fn validate_ledger(existing: &str) -> std::io::Result<(u64, String, usize)> {
    let mut sequence = 0;
    let mut prior_hash = String::new();
    let mut first = true;
    let mut event_count = 0_usize;
    for line in existing.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event: Value = serde_json::from_str(line)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
        let event_sequence = event["sequence"]
            .as_u64()
            .ok_or_else(|| invalid_ledger("activity event lacks a sequence"))?;
        if first {
            if event_sequence == 0 {
                return Err(invalid_ledger("activity event sequence must be positive"));
            }
            sequence = event_sequence - 1;
            prior_hash = event["previous_event_sha256"]
                .as_str()
                .ok_or_else(|| invalid_ledger("activity event lacks its predecessor digest"))?
                .to_string();
            first = false;
        }
        if event_sequence != sequence + 1
            || event["previous_event_sha256"].as_str() != Some(prior_hash.as_str())
        {
            return Err(invalid_ledger("activity hash chain is discontinuous"));
        }
        let recorded_hash = event["event_sha256"]
            .as_str()
            .ok_or_else(|| invalid_ledger("activity event lacks its digest"))?;
        let mut unhashed = event.clone();
        unhashed
            .as_object_mut()
            .expect("activity event is an object when sequence is present")
            .remove("event_sha256");
        if sha256_hex(unhashed.to_string().as_bytes()) != recorded_hash {
            return Err(invalid_ledger("activity event digest does not match"));
        }
        sequence = event_sequence;
        prior_hash = recorded_hash.to_string();
        event_count += 1;
    }
    Ok((sequence, prior_hash, event_count))
}

fn validate_activity_event(
    event: &Value,
    expected_sequence: u64,
    expected_hash: &str,
) -> std::io::Result<()> {
    if event["sequence"].as_u64() != Some(expected_sequence)
        || event["event_sha256"].as_str() != Some(expected_hash)
    {
        return Err(invalid_ledger(
            "activity tail does not match its committed head",
        ));
    }
    let mut unhashed = event.clone();
    unhashed
        .as_object_mut()
        .ok_or_else(|| invalid_ledger("activity tail is not an object"))?
        .remove("event_sha256");
    if sha256_hex(unhashed.to_string().as_bytes()) != expected_hash {
        return Err(invalid_ledger("activity tail digest does not match"));
    }
    Ok(())
}

fn invalid_ledger(message: &str) -> std::io::Error {
    std::io::Error::new(ErrorKind::InvalidData, message)
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_target_deduplication_is_constant_time_state() {
        let mut targets = ActivityTargets::default();
        targets.attempted("settings_record", "one", "params.id");
        targets.attempted("settings_record", "one", "params.id");
        targets.attempted("settings_record", "two", "params.id");
        assert_eq!(targets.identities.len(), 2);
        assert_eq!(targets.seen.len(), 2);
    }
}
