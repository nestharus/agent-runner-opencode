//! Declared roles: recorder, mapper, validator
//!
//! Provider-owned, redacted operation evidence. The host contract does not
//! currently carry an authenticated principal or delegation, so this ledger
//! records that absence explicitly instead of inventing an identity.

use crate::durable_fs;
use crate::encoding::{now_unix_ms, sha256_hex};
use crate::envelope::{ProviderFailure, RequestEnvelope};
use crate::path_guard;
use fs2::FileExt;
use serde::Serialize;
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

const ACTIVITY_DIR: &str = "provider-state/opencode/activity";
const ACTIVITY_FILE: &str = "operations.jsonl";
const ACTIVITY_LOCK: &str = ".operations.lock";
const ACTIVITY_SCHEMA_VERSION: u32 = 2;

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
}

#[derive(Clone, Serialize)]
struct ActivityIdentity {
    kind: &'static str,
    value: String,
    status: ActivityIdentityStatus,
    provenance: String,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityIdentityStatus {
    Attempted,
    Resolved,
    Generated,
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
        if value.trim().is_empty()
            || self.identities.iter().any(|identity| {
                identity.kind == kind
                    && identity.value == value
                    && identity.status == status
                    && identity.provenance == provenance
            })
        {
            return;
        }
        self.identities.push(ActivityIdentity {
            kind,
            value,
            status,
            provenance,
        });
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
    lock.lock_exclusive()?;
    let existing = match fs::read_to_string(&path) {
        Ok(existing) => existing,
        Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error),
    };
    let (sequence, previous_event_sha256) = validate_ledger(&existing)?;
    let mut event = json!({
        "schema_version": ACTIVITY_SCHEMA_VERSION,
        "sequence": sequence + 1,
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
        "previous_event_sha256": previous_event_sha256,
    });
    event["event_sha256"] = json!(sha256_hex(event.to_string().as_bytes()));
    let mut temporary = tempfile::NamedTempFile::new_in(root)?;
    temporary.write_all(existing.as_bytes())?;
    if !existing.is_empty() && !existing.ends_with('\n') {
        temporary.write_all(b"\n")?;
    }
    serde_json::to_writer(&mut temporary, &event)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary.persist(&path).map_err(|error| error.error)?;
    set_private_file(&path)?;
    durable_fs::sync_directory(root)
}

fn validate_ledger(existing: &str) -> std::io::Result<(u64, String)> {
    let mut sequence = 0;
    let mut prior_hash = String::new();
    for line in existing.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event: Value = serde_json::from_str(line)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
        let event_sequence = event["sequence"]
            .as_u64()
            .ok_or_else(|| invalid_ledger("activity event lacks a sequence"))?;
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
    }
    Ok((sequence, prior_hash))
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
