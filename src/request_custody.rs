//! Declared roles: orchestration, validator, accessor

use crate::durable_fs;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const INDEX_SCHEMA_VERSION: u64 = 6;
const POLICY_ACTIVE_INDEX_SCHEMA_VERSION: u64 = 5;
const BOUND_ACTIVE_INDEX_SCHEMA_VERSION: u64 = 4;
const UNBOUND_ACTIVE_INDEX_SCHEMA_VERSION: u64 = 3;
const PREDECESSOR_INDEX_SCHEMA_VERSION: u64 = 2;
const MAX_INDEX_RECORD_BYTES: usize = 1024;
const MAX_ACTIVE_INDEX_BYTES: usize = 16 * 1024;
const MAX_POLICY_ACTIVE_INDEX_MIGRATION_BYTES: usize = 128 * 1024;
const MAX_POLICY_ACTIVE_INDEX_MIGRATION_SLOTS: usize = 512;
const MAX_INDEXLESS_MIGRATION_REQUEST_LOCKS: usize = 512;
const MAX_INDEXLESS_MIGRATION_DIRECTORY_ENTRIES: usize = 2_048;
const MAX_INDEXLESS_MIGRATION_STATE_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_REPLAY_EVICTION_PROBES: usize = 64;
pub(crate) const MAX_ACTIVE_MAINTENANCE_PROBES: usize = 16;
pub(crate) const PRE_EFFECT_MAINTENANCE_SHARDS: usize = 64;
pub(crate) const PRE_EFFECT_CANDIDATES_PER_SHARD: usize = 2;
pub(crate) const MAX_PRE_EFFECT_MAINTENANCE_BYTES: usize = 16 * 1024;
const MAX_PRE_EFFECT_SHARD_BYTES: usize =
    MAX_PRE_EFFECT_MAINTENANCE_BYTES / PRE_EFFECT_MAINTENANCE_SHARDS;
const EMPTY_ACTIVE_DIGEST: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
// Current distributed admissions publish only their request-local marker.
// Older schema-v6 markers retain their finite maintenance sequence so a
// rolling replacement can finish consuming the predecessor queue.
const DIRECT_MAINTENANCE_SEQUENCE: u64 = u64::MAX;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ActiveSlot {
    occupied: u8,
    request_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binding_sha256: Option<String>,
}

impl ActiveSlot {
    fn empty() -> Self {
        Self {
            occupied: 0,
            request_sha256: EMPTY_ACTIVE_DIGEST.to_string(),
            binding_sha256: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActiveReservation {
    Absent,
    Matching,
    Unbound,
    Conflicting,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ActiveIndex {
    next_probe: usize,
    slots: Vec<ActiveSlot>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ActiveMarker {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    binding_sha256: Option<String>,
    maintenance_sequence: u64,
}

impl ActiveMarker {
    fn validate(&self) -> Result<(), CustodyError> {
        if self
            .binding_sha256
            .as_deref()
            .is_some_and(|binding| !valid_digest(binding))
        {
            return Err(CustodyError::Invalid(
                "request custody active marker binding is invalid".to_string(),
            ));
        }
        Ok(())
    }

    fn reservation(&self, binding_sha256: &str) -> ActiveReservation {
        match self.binding_sha256.as_deref() {
            Some(binding) if binding == binding_sha256 => ActiveReservation::Matching,
            Some(_) => ActiveReservation::Conflicting,
            None => ActiveReservation::Unbound,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct MaintenanceIndex {
    next_probe: u64,
    next_sequence: u64,
}

impl MaintenanceIndex {
    fn empty() -> Self {
        Self {
            next_probe: 0,
            next_sequence: 0,
        }
    }

    fn validate(&self) -> Result<(), CustodyError> {
        if self.next_probe > self.next_sequence {
            return Err(CustodyError::Invalid(
                "request custody maintenance index is inconsistent".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct MaintenanceTicket {
    request_sha256: String,
}

#[derive(Default, Deserialize, Serialize)]
struct PreEffectMaintenanceShard {
    candidates: Vec<String>,
}

impl PreEffectMaintenanceShard {
    fn validate(&self) -> Result<(), CustodyError> {
        let mut unique = HashSet::with_capacity(self.candidates.len());
        if self.candidates.len() > PRE_EFFECT_CANDIDATES_PER_SHARD
            || self
                .candidates
                .iter()
                .any(|candidate| !valid_digest(candidate) || !unique.insert(candidate))
        {
            return Err(CustodyError::Invalid(
                "request custody pre-effect maintenance shard is inconsistent".to_string(),
            ));
        }
        Ok(())
    }
}

enum PreEffectCandidateOutcome {
    Reaped,
    NoLongerPreEffect,
    Busy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ReplaySlot {
    #[serde(default)]
    sequence: Option<u64>,
    request_sha256: String,
}

impl ReplaySlot {
    fn current(sequence: u64, request_sha256: String) -> Self {
        Self {
            sequence: Some(sequence),
            request_sha256,
        }
    }

    fn validate(&self, replay_slots: usize, slot: u64) -> Result<(), CustodyError> {
        if !valid_digest(&self.request_sha256)
            || self
                .sequence
                .is_some_and(|sequence| sequence % replay_slots as u64 != slot)
        {
            return Err(CustodyError::Invalid(
                "request custody replay slot is inconsistent".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReplayOwner {
    sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    displaced: Option<ReplaySlot>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ReplayReservation {
    sequence: u64,
    request_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    displaced: Option<ReplaySlot>,
}

impl ReplayReservation {
    fn validate(&self, replay_slots: usize, slot: u64) -> Result<(), CustodyError> {
        if !valid_digest(&self.request_sha256)
            || self.sequence % replay_slots as u64 != slot
            || self
                .displaced
                .as_ref()
                .is_some_and(|displaced| displaced.validate(replay_slots, slot).is_err())
        {
            return Err(CustodyError::Invalid(
                "request custody replay reservation is inconsistent".to_string(),
            ));
        }
        Ok(())
    }
}

struct ReplayDisplacement {
    replay: ReplaySlot,
    pin: Option<(fs::File, PathBuf)>,
    _lock: fs::File,
}

enum ShardedReplaySlotOutcome {
    Published,
    Occupied,
    Unavailable,
}

impl ActiveIndex {
    fn empty(active_slots: usize) -> Self {
        Self {
            next_probe: 0,
            slots: (0..active_slots).map(|_| ActiveSlot::empty()).collect(),
        }
    }

    fn validate(&self, policy: ActiveIndexPolicy) -> Result<(), CustodyError> {
        let active_size_is_valid = match policy {
            ActiveIndexPolicy::Fixed { limit } => self.slots.len() == limit,
            ActiveIndexPolicy::Distributed {
                predecessor_initial_slots,
            } => self.slots.len() >= predecessor_initial_slots,
        };
        if !active_size_is_valid
            || (!self.slots.is_empty() && self.next_probe >= self.slots.len())
            || self.slots.iter().any(|slot| {
                !matches!(slot.occupied, 0 | 1)
                    || !valid_digest(&slot.request_sha256)
                    || (slot.occupied == 0 && slot.request_sha256 != EMPTY_ACTIVE_DIGEST)
                    || (slot.occupied == 0 && slot.binding_sha256.is_some())
                    || slot
                        .binding_sha256
                        .as_deref()
                        .is_some_and(|binding| !valid_digest(binding))
            })
        {
            return Err(CustodyError::Invalid(
                "request custody active index is inconsistent".to_string(),
            ));
        }
        let mut occupied = HashSet::with_capacity(self.slots.len());
        if self
            .slots
            .iter()
            .filter(|slot| slot.occupied == 1)
            .any(|slot| !occupied.insert(slot.request_sha256.as_str()))
        {
            return Err(CustodyError::Invalid(
                "request custody active index contains duplicate requests".to_string(),
            ));
        }
        Ok(())
    }

    fn active(&self) -> usize {
        self.slots.iter().filter(|slot| slot.occupied == 1).count()
    }

    fn contains(&self, stem: &str) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.occupied == 1 && slot.request_sha256 == stem)
    }

    fn reservation(&self, stem: &str, binding_sha256: &str) -> ActiveReservation {
        let Some(slot) = self
            .slots
            .iter()
            .find(|slot| slot.occupied == 1 && slot.request_sha256 == stem)
        else {
            return ActiveReservation::Absent;
        };
        match slot.binding_sha256.as_deref() {
            Some(binding) if binding == binding_sha256 => ActiveReservation::Matching,
            Some(_) => ActiveReservation::Conflicting,
            None => ActiveReservation::Unbound,
        }
    }

    fn reserve(&mut self, stem: String, binding_sha256: String) -> Result<(), CustodyError> {
        if !valid_digest(&binding_sha256) {
            return Err(CustodyError::Invalid(
                "request custody active reservation binding is invalid".to_string(),
            ));
        }
        match self.reservation(&stem, &binding_sha256) {
            ActiveReservation::Matching => return Ok(()),
            ActiveReservation::Unbound | ActiveReservation::Conflicting => {
                return Err(CustodyError::Invalid(
                    "request custody active reservation conflicts with the attempted binding"
                        .to_string(),
                ))
            }
            ActiveReservation::Absent => {}
        }
        let slot = match self.slots.iter_mut().find(|slot| slot.occupied == 0) {
            Some(slot) => slot,
            None => return Err(CustodyError::Capacity),
        };
        slot.occupied = 1;
        slot.request_sha256 = stem;
        slot.binding_sha256 = Some(binding_sha256);
        Ok(())
    }

    fn reserve_unbound(&mut self, stem: String) -> Result<(), CustodyError> {
        if self.contains(&stem) {
            return Ok(());
        }
        let slot = match self.slots.iter_mut().find(|slot| slot.occupied == 0) {
            Some(slot) => slot,
            None => return Err(CustodyError::Capacity),
        };
        slot.occupied = 1;
        slot.request_sha256 = stem;
        slot.binding_sha256 = None;
        Ok(())
    }

    fn bind_unbound(&mut self, stem: &str, binding_sha256: &str) -> Result<(), CustodyError> {
        if !valid_digest(binding_sha256) {
            return Err(CustodyError::Invalid(
                "request custody active reservation binding is invalid".to_string(),
            ));
        }
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.occupied == 1 && slot.request_sha256 == stem)
            .ok_or_else(|| {
                CustodyError::Invalid(
                    "request custody active reservation disappeared before binding".to_string(),
                )
            })?;
        match slot.binding_sha256.as_deref() {
            Some(binding) if binding == binding_sha256 => Ok(()),
            Some(_) => Err(CustodyError::Invalid(
                "request custody active reservation conflicts with the attempted binding"
                    .to_string(),
            )),
            None => {
                slot.binding_sha256 = Some(binding_sha256.to_string());
                Ok(())
            }
        }
    }

    fn remove(&mut self, stem: &str) -> bool {
        let Some(slot) = self
            .slots
            .iter_mut()
            .find(|slot| slot.occupied == 1 && slot.request_sha256 == stem)
        else {
            return false;
        };
        *slot = ActiveSlot::empty();
        true
    }

    fn next_occupied(&mut self) -> Option<String> {
        if self.slots.is_empty() {
            return None;
        }
        let mut selected = None;
        for offset in 0..self.slots.len() {
            let index = (self.next_probe + offset) % self.slots.len();
            if selected.is_none() && self.slots[index].occupied == 1 {
                selected = Some((index, self.slots[index].request_sha256.clone()));
            }
        }
        if let Some((index, _)) = selected.as_ref() {
            self.next_probe = (*index + 1) % self.slots.len();
        }
        selected.map(|(_, stem)| stem)
    }

    fn retry_next(&mut self, stem: &str) {
        if let Some(index) = self
            .slots
            .iter()
            .position(|slot| slot.occupied == 1 && slot.request_sha256 == stem)
        {
            self.next_probe = index;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActiveIndexPolicy {
    Fixed { limit: usize },
    Distributed { predecessor_initial_slots: usize },
}

impl ActiveIndexPolicy {
    fn initial_slots(self) -> usize {
        match self {
            Self::Fixed { limit } => limit,
            Self::Distributed {
                predecessor_initial_slots,
            } => predecessor_initial_slots,
        }
    }

    fn is_distributed(self) -> bool {
        matches!(self, Self::Distributed { .. })
    }
}

#[derive(Debug)]
pub(crate) enum CustodyError {
    Io(std::io::Error),
    Invalid(String),
    Migration(String),
    Capacity,
}

#[derive(Clone, Debug)]
pub(crate) struct CustodyTransitionPreflight {
    pub(crate) format: &'static str,
    pub(crate) request_locks: usize,
    pub(crate) state_bytes: u64,
    pub(crate) blocker: Option<String>,
}

impl CustodyTransitionPreflight {
    fn ready(format: &'static str) -> Self {
        Self {
            format,
            request_locks: 0,
            state_bytes: 0,
            blocker: None,
        }
    }
}

impl std::fmt::Display for CustodyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Invalid(message) => formatter.write_str(message),
            Self::Migration(message) => formatter.write_str(message),
            Self::Capacity => formatter.write_str("request custody reached its supported bound"),
        }
    }
}

impl From<std::io::Error> for CustodyError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub(crate) struct RequestCustody {
    state_root: PathBuf,
    lock_root: PathBuf,
    index_root: PathBuf,
    state_byte_limit: usize,
    active_policy: ActiveIndexPolicy,
    replay_slots: usize,
    orphan_retention: Duration,
}

impl RequestCustody {
    pub(crate) fn new_fixed(
        state_root: PathBuf,
        lock_root: PathBuf,
        index_root: PathBuf,
        state_byte_limit: usize,
        active_limit: usize,
        replay_slots: usize,
        orphan_retention: Duration,
    ) -> Self {
        Self {
            state_root,
            lock_root,
            index_root,
            state_byte_limit,
            active_policy: ActiveIndexPolicy::Fixed {
                limit: active_limit,
            },
            replay_slots,
            orphan_retention,
        }
    }

    /// Represent each active request independently. `predecessor_initial_slots`
    /// identifies the fixed v2-v5 format accepted for one-time migration; it
    /// is not a current runtime population limit.
    pub(crate) fn new_distributed(
        state_root: PathBuf,
        lock_root: PathBuf,
        index_root: PathBuf,
        state_byte_limit: usize,
        predecessor_initial_slots: usize,
        replay_slots: usize,
        orphan_retention: Duration,
    ) -> Self {
        Self {
            state_root,
            lock_root,
            index_root,
            state_byte_limit,
            active_policy: ActiveIndexPolicy::Distributed {
                predecessor_initial_slots,
            },
            replay_slots,
            orphan_retention,
        }
    }

    /// Advance bounded custody maintenance while the capability's registration
    /// lock is held. The caller must retain that lock until any newly reserved
    /// request marker has been followed by creation of its request-lock file.
    pub(crate) fn maintain(
        &self,
        current_lock_path: &Path,
        classify_replay: impl Fn(&[u8]) -> Result<bool, String>,
    ) -> Result<usize, CustodyError> {
        self.initialize(current_lock_path, &classify_replay)?;
        if self.active_policy.is_distributed() {
            self.maintain_distributed(current_lock_path, &classify_replay)?;
            return Ok(0);
        }
        let mut index = self.read_active_index()?;
        if let Some(stem) = index.next_occupied() {
            let state_path = self.state_path(&stem);
            let lock_path = self.lock_path(&stem);
            let state = match durable_fs::read_file_bounded(&state_path, self.state_byte_limit) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(CustodyError::Io(error)),
            };
            if let Some(bytes) = state {
                if classify_replay(&bytes).map_err(CustodyError::Invalid)? {
                    if self.place_replay(&stem, current_lock_path)? {
                        index.remove(&stem);
                    } else if self.read_replay_owner(&stem)?.is_some() {
                        index.retry_next(&stem);
                    }
                }
            // Callers hold the capability's registration lock from before this
            // maintenance pass until after a newly reserved request lock has
            // been created. A state-less marker with no lock therefore proves
            // that its reserving process ended before transferring custody.
            // Preserve the exact caller's marker so that it can resume even at
            // the active bound; any other successor may retire the abandoned
            // pre-effect reservation immediately. Once a lock exists, retain
            // the prior age-and-exclusive-lock rule.
            } else if self.read_replay_owner(&stem)?.is_none()
                && (!lock_path.exists() || record_expired(&lock_path, self.orphan_retention))
                && self.remove_abandoned_active(&stem, &lock_path, current_lock_path)?
            {
                index.remove(&stem);
            }
            self.write_active_index(&index)?;
        }
        Ok(index.active())
    }

    /// Inspect the exact predecessor transition without creating, deleting,
    /// or rewriting custody. Setup uses this to block an unsafe cutover, and
    /// initialization repeats it under the registration lock before mutation.
    pub(crate) fn transition_preflight(&self) -> Result<CustodyTransitionPreflight, CustodyError> {
        let schema_path = self.index_root.join("schema.json");
        let schema_bytes = match durable_fs::read_file_bounded(&schema_path, MAX_INDEX_RECORD_BYTES)
        {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(CustodyError::Io(error)),
        };
        let Some(schema_bytes) = schema_bytes else {
            return self.preflight_indexless_transition();
        };
        let schema: Value = serde_json::from_slice(&schema_bytes)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        if schema["replay_slots"].as_u64() != Some(self.replay_slots as u64) {
            return Err(CustodyError::Invalid(
                "request custody index schema or bounds are inconsistent".to_string(),
            ));
        }
        match schema["schema_version"].as_u64() {
            Some(INDEX_SCHEMA_VERSION) => {
                self.validate_current_schema(&schema)?;
                Ok(CustodyTransitionPreflight::ready("current"))
            }
            Some(POLICY_ACTIVE_INDEX_SCHEMA_VERSION) => {
                self.validate_policy_index_schema(&schema)?;
                let active = self.read_active_index()?;
                let mut preflight = CustodyTransitionPreflight::ready("schema_v5");
                preflight.request_locks = active
                    .slots
                    .iter()
                    .filter(|slot| slot.occupied == 1)
                    .count();
                if self.active_policy.is_distributed()
                    && active.slots.len() > MAX_POLICY_ACTIVE_INDEX_MIGRATION_SLOTS
                {
                    preflight.blocker = Some(format!(
                        "schema-v5 launch custody has {} slots, above the supported one-time migration envelope of {}; keep the schema-v5 provider installed until direct terminal cleanup compacts completed launch custody below that envelope, then retry the upgrade; do not delete provider sessions (schema-v6 runtime population itself has no fixed limit)",
                        active.slots.len(),
                        MAX_POLICY_ACTIVE_INDEX_MIGRATION_SLOTS
                    ));
                }
                Ok(preflight)
            }
            Some(BOUND_ACTIVE_INDEX_SCHEMA_VERSION) => {
                self.validate_predecessor_active_slots(&schema)?;
                Ok(CustodyTransitionPreflight::ready("schema_v4"))
            }
            Some(UNBOUND_ACTIVE_INDEX_SCHEMA_VERSION) => {
                self.validate_predecessor_active_slots(&schema)?;
                Ok(CustodyTransitionPreflight::ready("schema_v3"))
            }
            Some(PREDECESSOR_INDEX_SCHEMA_VERSION) => {
                self.validate_predecessor_active_slots(&schema)?;
                Ok(CustodyTransitionPreflight::ready("schema_v2"))
            }
            _ => Err(CustodyError::Invalid(
                "request custody index schema or bounds are inconsistent".to_string(),
            )),
        }
    }

    /// Prove that independent request records may be used without taking the
    /// predecessor transition lock. This check is read-only: a missing or old
    /// representation returns `false`, while malformed current custody fails
    /// closed instead of silently rebuilding it alongside live requests.
    pub(crate) fn independent_requests_ready(&self) -> Result<bool, CustodyError> {
        let preflight = self.transition_preflight()?;
        if preflight.format != "current" || preflight.blocker.is_some() {
            return Ok(false);
        }
        let schema_bytes = durable_fs::read_file_bounded(
            &self.index_root.join("schema.json"),
            MAX_INDEX_RECORD_BYTES,
        )?;
        let schema: Value = serde_json::from_slice(&schema_bytes)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        if self.active_policy.is_distributed()
            && (schema["active_registration"].as_str() != Some("request_local")
                || schema["replay_allocation"].as_str() != Some("hash_sharded"))
        {
            return Ok(false);
        }
        for root in [
            self.replay_root(),
            self.replay_slot_lock_root(),
            self.owner_root(),
            self.pin_root(),
            self.temporary_root(),
            self.active_root(),
            self.maintenance_root(),
        ] {
            match fs::metadata(&root) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => {
                    return Err(CustodyError::Invalid(
                        "request custody index child is not a directory".to_string(),
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(CustodyError::Io(error)),
            }
        }
        self.read_maintenance_index()?;
        Ok(!self.active_index_path().exists())
    }

    fn preflight_indexless_transition(&self) -> Result<CustodyTransitionPreflight, CustodyError> {
        let entries = match fs::read_dir(&self.lock_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(CustodyTransitionPreflight::ready("fresh"));
            }
            Err(error) => return Err(CustodyError::Io(error)),
        };
        let mut directory_entries = 0_usize;
        let mut request_locks = 0_usize;
        let mut state_bytes = 0_u64;
        let mut blocker = None;
        for entry in entries {
            let entry = entry?;
            directory_entries += 1;
            if directory_entries > MAX_INDEXLESS_MIGRATION_DIRECTORY_ENTRIES {
                blocker = Some(format!(
                    "indexless predecessor launch custody has more than {MAX_INDEXLESS_MIGRATION_DIRECTORY_ENTRIES} directory entries, above the bounded one-time inspection envelope"
                ));
                break;
            }
            let lock_path = entry.path();
            if lock_path.file_name().and_then(|name| name.to_str()) == Some(".capacity.lock")
                || lock_path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    != Some("lock")
            {
                continue;
            }
            request_locks += 1;
            if request_locks > MAX_INDEXLESS_MIGRATION_REQUEST_LOCKS {
                blocker = Some(format!(
                    "indexless predecessor launch custody has more than {MAX_INDEXLESS_MIGRATION_REQUEST_LOCKS} request locks, above the bounded one-time migration envelope"
                ));
                break;
            }
            let stem = required_digest_stem(&lock_path)?;
            match fs::metadata(self.state_path(&stem)) {
                Ok(metadata) if metadata.is_file() => {
                    state_bytes = state_bytes.checked_add(metadata.len()).ok_or_else(|| {
                        CustodyError::Invalid(
                            "indexless predecessor launch custody state bytes overflowed"
                                .to_string(),
                        )
                    })?;
                    if state_bytes > MAX_INDEXLESS_MIGRATION_STATE_BYTES {
                        blocker = Some(format!(
                            "indexless predecessor launch custody has more than {MAX_INDEXLESS_MIGRATION_STATE_BYTES} bytes of request state, above the bounded one-time migration envelope"
                        ));
                        break;
                    }
                }
                Ok(_) => {
                    return Err(CustodyError::Invalid(
                        "indexless predecessor launch request state is not a file".to_string(),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(CustodyError::Io(error)),
            }
        }
        if let Some(reason) = blocker.as_mut() {
            reason.push_str("; keep the predecessor provider installed and allow normal exact terminal cleanup to reduce custody below the envelope before retrying; do not delete provider sessions (current distributed runtime custody has no fixed population limit)");
        }
        Ok(CustodyTransitionPreflight {
            format: "indexless_predecessor",
            request_locks,
            state_bytes,
            blocker,
        })
    }

    fn maintain_distributed(
        &self,
        current_lock_path: &Path,
        classify_replay: &impl Fn(&[u8]) -> Result<bool, String>,
    ) -> Result<(), CustodyError> {
        let mut maintenance = self.read_maintenance_index()?;
        let mut write_index = false;
        for _ in 0..MAX_ACTIVE_MAINTENANCE_PROBES {
            if maintenance.next_probe >= maintenance.next_sequence {
                break;
            }
            let sequence = maintenance.next_probe;
            maintenance.next_probe += 1;
            write_index = true;
            let Some(ticket) = self.read_maintenance_ticket(sequence)? else {
                continue;
            };
            let Some(mut marker) = self.read_active_marker(&ticket.request_sha256)? else {
                self.remove_maintenance_ticket(sequence)?;
                break;
            };
            if marker.maintenance_sequence != sequence {
                self.remove_maintenance_ticket(sequence)?;
                break;
            }
            let state_path = self.state_path(&ticket.request_sha256);
            let lock_path = self.lock_path(&ticket.request_sha256);
            let state = match durable_fs::read_file_bounded(&state_path, self.state_byte_limit) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(CustodyError::Io(error)),
            };
            let mut retained = true;
            if let Some(bytes) = state {
                if classify_replay(&bytes).map_err(CustodyError::Invalid)?
                    && self.place_replay(&ticket.request_sha256, current_lock_path)?
                {
                    self.remove_active_marker(&state_path)?;
                    retained = false;
                }
            } else if self.read_replay_owner(&ticket.request_sha256)?.is_none()
                && (!lock_path.exists() || record_expired(&lock_path, self.orphan_retention))
                && self.remove_abandoned_active(
                    &ticket.request_sha256,
                    &lock_path,
                    current_lock_path,
                )?
            {
                self.remove_active_marker(&state_path)?;
                retained = false;
            }
            if retained {
                let next_sequence =
                    self.enqueue_maintenance(&ticket.request_sha256, &mut maintenance)?;
                marker.maintenance_sequence = next_sequence;
                self.write_active_marker(&ticket.request_sha256, &marker)?;
                self.remove_maintenance_ticket(sequence)?;
                write_index = false;
            }
            break;
        }
        if write_index {
            self.write_maintenance_index(&maintenance)?;
        }
        Ok(())
    }

    pub(crate) fn reserve_active(
        &self,
        state_path: &Path,
        binding_sha256: &str,
    ) -> Result<(), CustodyError> {
        let stem = required_digest_stem(state_path)?;
        if self.active_policy.is_distributed() {
            return self.reserve_active_marker(&stem, binding_sha256);
        }
        let mut index = self.read_active_index()?;
        index.reserve(stem, binding_sha256.to_string())?;
        self.write_active_index(&index)
    }

    /// Reserve an independently keyed distributed marker without updating the
    /// predecessor maintenance queue. Fixed-policy callers retain their
    /// existing indexed reservation semantics.
    pub(crate) fn reserve_independent_active(
        &self,
        state_path: &Path,
        binding_sha256: &str,
    ) -> Result<(), CustodyError> {
        if !self.active_policy.is_distributed() {
            return self.reserve_active(state_path, binding_sha256);
        }
        let stem = required_digest_stem(state_path)?;
        self.reserve_independent_active_marker(&stem, binding_sha256)
    }

    /// Remember a short-lived pre-effect request in bounded, sharded cleanup
    /// bookkeeping. A later request in the same shard may retire a prior state
    /// only after it owns that request's replay pin and exact lock and the
    /// capability classifier proves that no native effect was admitted.
    ///
    /// Contention never becomes admission pressure: a busy shard or candidate
    /// simply leaves the current request untracked by this best-effort cleanup
    /// accelerator. Exact request custody remains authoritative either way.
    pub(crate) fn register_pre_effect_candidate(
        &self,
        state_path: &Path,
        classify_pre_effect: impl Fn(&str, &[u8]) -> Result<bool, String>,
    ) -> Result<(), CustodyError> {
        if !self.active_policy.is_distributed() {
            return Ok(());
        }
        let stem = required_digest_stem(state_path)?;
        self.prepare_pre_effect_maintenance_roots()?;
        let shard = pre_effect_shard(&stem)?;
        let Some(_shard_lock) = self.try_lock_pre_effect_shard(shard)? else {
            return Ok(());
        };
        let mut record = self.read_pre_effect_shard(shard)?;
        let mut retained = Vec::with_capacity(PRE_EFFECT_CANDIDATES_PER_SHARD);
        let mut current_present = false;
        for candidate in record.candidates.drain(..) {
            if candidate == stem {
                current_present = true;
                retained.push(candidate);
                continue;
            }
            match self.try_reap_pre_effect_candidate(&candidate, &classify_pre_effect)? {
                PreEffectCandidateOutcome::Reaped
                | PreEffectCandidateOutcome::NoLongerPreEffect => {}
                PreEffectCandidateOutcome::Busy => retained.push(candidate),
            }
        }
        if !current_present && retained.len() < PRE_EFFECT_CANDIDATES_PER_SHARD {
            retained.push(stem);
        }
        record.candidates = retained;
        self.write_pre_effect_shard(shard, &record)
    }

    /// Remove this request from the optional pre-effect cleanup accelerator
    /// after actor publication or ordinary pre-spawn abandonment. A stale
    /// entry is harmless: its next observer rechecks durable request state
    /// while owning the exact lock before it can retire anything.
    pub(crate) fn release_pre_effect_candidate(
        &self,
        state_path: &Path,
    ) -> Result<(), CustodyError> {
        if !self.active_policy.is_distributed() {
            return Ok(());
        }
        let stem = required_digest_stem(state_path)?;
        self.prepare_pre_effect_maintenance_roots()?;
        let shard = pre_effect_shard(&stem)?;
        let Some(_shard_lock) = self.try_lock_pre_effect_shard(shard)? else {
            return Ok(());
        };
        let mut record = self.read_pre_effect_shard(shard)?;
        record.candidates.retain(|candidate| candidate != &stem);
        self.write_pre_effect_shard(shard, &record)
    }

    /// Report how the active index owns this exact request and attempt.
    ///
    /// Callers use this after `maintain` while still holding their registration
    /// lock, so a pre-state reservation can resume without being classified as
    /// unrelated new work at the active bound.
    pub(crate) fn active_reservation(
        &self,
        state_path: &Path,
        binding_sha256: &str,
    ) -> Result<ActiveReservation, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        if !valid_digest(binding_sha256) {
            return Err(CustodyError::Invalid(
                "request custody active reservation binding is invalid".to_string(),
            ));
        }
        if self.active_policy.is_distributed() {
            return Ok(self
                .read_active_marker(&stem)?
                .map_or(ActiveReservation::Absent, |marker| {
                    marker.reservation(binding_sha256)
                }));
        }
        Ok(self.read_active_index()?.reservation(&stem, binding_sha256))
    }

    /// Claim an unbound schema-v3 reservation only after the caller proves,
    /// under the registration lock, that neither request state nor a request lock
    /// exists. Current-schema reservations are bound when first published.
    pub(crate) fn bind_unbound_active(
        &self,
        state_path: &Path,
        binding_sha256: &str,
    ) -> Result<(), CustodyError> {
        let stem = required_digest_stem(state_path)?;
        if self.active_policy.is_distributed() {
            let mut marker = self.read_active_marker(&stem)?.ok_or_else(|| {
                CustodyError::Invalid(
                    "request custody active marker disappeared before binding".to_string(),
                )
            })?;
            match marker.reservation(binding_sha256) {
                ActiveReservation::Matching => return Ok(()),
                ActiveReservation::Conflicting => {
                    return Err(CustodyError::Invalid(
                        "request custody active marker conflicts with the attempted binding"
                            .to_string(),
                    ))
                }
                ActiveReservation::Absent => unreachable!("a loaded active marker is present"),
                ActiveReservation::Unbound => {}
            }
            marker.binding_sha256 = Some(binding_sha256.to_string());
            return self.write_active_marker(&stem, &marker);
        }
        let mut index = self.read_active_index()?;
        index.bind_unbound(&stem, binding_sha256)?;
        self.write_active_index(&index)
    }

    pub(crate) fn pin_existing(&self, state_path: &Path) -> Result<fs::File, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        let pin = open_lock(&self.pin_root().join(format!("{stem}.pin")))?;
        fs2::FileExt::lock_shared(&pin)?;
        Ok(pin)
    }

    pub(crate) fn replay_owner_exists(&self, state_path: &Path) -> Result<bool, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        match fs::metadata(self.owner_path(&stem)) {
            Ok(metadata) if metadata.is_file() => Ok(true),
            Ok(_) => Err(CustodyError::Invalid(
                "request custody replay owner is not a file".to_string(),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    pub(crate) fn release_pin_after_lock(&self, state_path: &Path) -> Result<(), CustodyError> {
        let stem = required_digest_stem(state_path)?;
        let Some((pin, pin_path)) = self.try_lock_pin_exclusive(&stem)? else {
            return Ok(());
        };
        remove_file_if_present(&pin_path)?;
        drop(pin);
        durable_fs::sync_directory(&self.pin_root())?;
        Ok(())
    }

    pub(crate) fn remove_active_marker(&self, state_path: &Path) -> Result<(), CustodyError> {
        let stem = required_digest_stem(state_path)?;
        if self.active_policy.is_distributed() {
            if let Some(marker) = self.read_active_marker(&stem)? {
                remove_file_if_present(&self.active_marker_path(&stem))?;
                durable_fs::sync_directory(&self.active_root())?;
                if marker.maintenance_sequence != DIRECT_MAINTENANCE_SEQUENCE {
                    self.remove_maintenance_ticket(marker.maintenance_sequence)?;
                }
            }
            return Ok(());
        }
        let mut index = self.read_active_index()?;
        if index.remove(&stem) {
            self.write_active_index(&index)?;
        }
        Ok(())
    }

    /// Publish an exact terminal request into the replay ring, then retire only
    /// its active admission marker. The caller holds the capability registration
    /// lock and must not hold the request lock named by `current_lock_path`.
    #[cfg(test)]
    pub(crate) fn publish_replay_and_retire_active(
        &self,
        state_path: &Path,
        current_lock_path: &Path,
    ) -> Result<bool, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        if !self.place_replay(&stem, current_lock_path)? {
            return Ok(false);
        }
        self.remove_active_marker(state_path)?;
        Ok(true)
    }

    /// Publish terminal history without a capability-wide registration lock.
    /// Only completions selecting the same physical replay slot serialize.
    /// Exhausting the bounded probes leaves the active request authoritative
    /// for exact recovery instead of turning cleanup contention into a launch
    /// failure.
    pub(crate) fn publish_sharded_replay_and_retire_active(
        &self,
        state_path: &Path,
    ) -> Result<bool, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        if self.replay_slots == 0 {
            return Err(CustodyError::Capacity);
        }
        if let Some(owner) = self.read_replay_owner(&stem)? {
            let slot = owner.sequence % self.replay_slots as u64;
            let Some(_slot_lock) = self.try_lock_replay_slot(slot)? else {
                return Ok(false);
            };
            if !self.complete_replay_placement(&stem, &owner, Path::new(""), false)? {
                return Ok(false);
            }
            self.remove_active_marker(state_path)?;
            return Ok(true);
        }

        let start = u64::from_str_radix(&stem[stem.len() - 16..], 16)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?
            % self.replay_slots as u64;
        let mut replacement_slots = Vec::new();
        for offset in 0..self.replay_slots.min(MAX_REPLAY_EVICTION_PROBES) {
            let slot = (start + offset as u64) % self.replay_slots as u64;
            match self.try_publish_sharded_replay_slot(&stem, slot, false)? {
                ShardedReplaySlotOutcome::Published => {
                    self.remove_active_marker(state_path)?;
                    return Ok(true);
                }
                ShardedReplaySlotOutcome::Occupied => {
                    replacement_slots.push(slot);
                }
                ShardedReplaySlotOutcome::Unavailable => {}
            }
        }
        for slot in replacement_slots {
            if matches!(
                self.try_publish_sharded_replay_slot(&stem, slot, true)?,
                ShardedReplaySlotOutcome::Published
            ) {
                self.remove_active_marker(state_path)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn try_publish_sharded_replay_slot(
        &self,
        stem: &str,
        slot: u64,
        allow_displacement: bool,
    ) -> Result<ShardedReplaySlotOutcome, CustodyError> {
        let Some(_slot_lock) = self.try_lock_replay_slot(slot)? else {
            return Ok(ShardedReplaySlotOutcome::Unavailable);
        };
        if let Some(reservation) = self.read_replay_reservation(slot)? {
            let owner = self.read_replay_owner(&reservation.request_sha256)?;
            if owner.as_ref().is_some_and(|owner| {
                owner.sequence == reservation.sequence && owner.displaced == reservation.displaced
            }) {
                return Ok(ShardedReplaySlotOutcome::Unavailable);
            }
            remove_file_if_present(&self.replay_reservation_path(slot))?;
            durable_fs::sync_directory(&self.owner_root())?;
        }
        let prior = self.read_replay_slot(slot)?;
        if prior
            .as_ref()
            .is_some_and(|prior| prior.request_sha256 == stem)
        {
            return Err(CustodyError::Invalid(
                "request custody replay slot has no matching owner".to_string(),
            ));
        }
        if prior.is_some() && !allow_displacement {
            return Ok(ShardedReplaySlotOutcome::Occupied);
        }
        let displacement = match prior.as_ref() {
            Some(prior) => match self.acquire_replay_displacement(prior, Path::new(""))? {
                Some(displacement) => Some(displacement),
                None => return Ok(ShardedReplaySlotOutcome::Unavailable),
            },
            None => None,
        };
        let owner = ReplayOwner {
            sequence: slot,
            displaced: prior.clone(),
        };
        self.write_replay_reservation(
            slot,
            &ReplayReservation {
                sequence: slot,
                request_sha256: stem.to_string(),
                displaced: prior,
            },
        )?;
        self.write_replay_owner(stem, &owner)?;
        if !self.publish_replay_placement(stem, &owner, displacement)? {
            return Ok(ShardedReplaySlotOutcome::Unavailable);
        }
        Ok(ShardedReplaySlotOutcome::Published)
    }

    #[cfg(test)]
    pub(crate) fn publish_replay_without_retiring_active(
        &self,
        state_path: &Path,
        current_lock_path: &Path,
    ) -> Result<bool, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        self.place_replay(&stem, current_lock_path)
    }

    #[cfg(test)]
    fn reserve_replay_slot_without_publication(
        &self,
        state_path: &Path,
        current_lock_path: &Path,
    ) -> Result<bool, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        Ok(self
            .reserve_replay_owner(&stem, current_lock_path)?
            .is_some())
    }

    fn initialize(
        &self,
        current_lock_path: &Path,
        classify_replay: &impl Fn(&[u8]) -> Result<bool, String>,
    ) -> Result<(), CustodyError> {
        let preflight = self.transition_preflight()?;
        if let Some(blocker) = preflight.blocker {
            return Err(CustodyError::Migration(blocker));
        }
        let schema_path = self.index_root.join("schema.json");
        match durable_fs::read_file_bounded(&schema_path, MAX_INDEX_RECORD_BYTES) {
            Ok(bytes) => {
                let schema: Value = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                if schema["replay_slots"].as_u64() != Some(self.replay_slots as u64) {
                    return Err(CustodyError::Invalid(
                        "request custody index schema or bounds are inconsistent".to_string(),
                    ));
                }
                match schema["schema_version"].as_u64() {
                    Some(INDEX_SCHEMA_VERSION) => {
                        self.validate_current_schema(&schema)?;
                        self.require_index_directory(&self.replay_root())?;
                        self.prepare_replay_slot_lock_root()?;
                        self.require_index_directory(&self.owner_root())?;
                        if self.active_policy.is_distributed() {
                            self.require_index_directory(&self.active_root())?;
                            self.require_index_directory(&self.maintenance_root())?;
                            self.read_maintenance_index()?;
                            if self.active_index_path().exists() {
                                remove_file_if_present(&self.active_index_path())?;
                                durable_fs::sync_directory(&self.index_root)?;
                            }
                        }
                        if self.active_policy.is_distributed()
                            && (schema["active_registration"].as_str() != Some("request_local")
                                || schema["replay_allocation"].as_str() != Some("hash_sharded"))
                        {
                            self.write_current_schema(&schema_path)?;
                        }
                    }
                    Some(POLICY_ACTIVE_INDEX_SCHEMA_VERSION) => {
                        self.validate_policy_index_schema(&schema)?;
                        self.upgrade_policy_active_index(
                            &schema_path,
                            current_lock_path,
                            classify_replay,
                        )?;
                    }
                    Some(BOUND_ACTIVE_INDEX_SCHEMA_VERSION) => {
                        self.validate_predecessor_active_slots(&schema)?;
                        self.upgrade_bound_active_index(&schema_path)?;
                    }
                    Some(UNBOUND_ACTIVE_INDEX_SCHEMA_VERSION) => {
                        self.validate_predecessor_active_slots(&schema)?;
                        self.upgrade_unbound_active_index(&schema_path)?;
                    }
                    Some(PREDECESSOR_INDEX_SCHEMA_VERSION) => {
                        self.validate_predecessor_active_slots(&schema)?;
                        self.upgrade_predecessor_index(&schema_path)?;
                    }
                    _ => {
                        return Err(CustodyError::Invalid(
                            "request custody index schema or bounds are inconsistent".to_string(),
                        ))
                    }
                }
                self.prepare_temporary_root()?;
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(CustodyError::Io(error)),
        }
        match fs::metadata(&self.index_root) {
            Ok(metadata) if metadata.is_dir() => {
                fs::remove_dir_all(&self.index_root)?;
                durable_fs::sync_directory(
                    self.index_root
                        .parent()
                        .expect("custody index root always has a parent"),
                )?;
            }
            Ok(_) => {
                return Err(CustodyError::Invalid(
                    "request custody index root is not a directory".to_string(),
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(CustodyError::Io(error)),
        }
        create_private_index_subtree(
            &self.index_root,
            &[
                self.replay_root(),
                self.replay_slot_lock_root(),
                self.owner_root(),
                self.pin_root(),
                self.temporary_root(),
                self.active_root(),
                self.maintenance_root(),
            ],
        )?;
        if self.active_policy.is_distributed() {
            self.write_maintenance_index(&MaintenanceIndex::empty())?;
        }
        let mut migrated = 0_usize;
        let mut active_index = (!self.active_policy.is_distributed())
            .then(|| ActiveIndex::empty(self.active_policy.initial_slots()));
        let mut replay_candidates = Vec::new();
        let migration_limit = self
            .active_policy
            .initial_slots()
            .saturating_add(self.replay_slots);
        for entry in fs::read_dir(&self.lock_root)? {
            let entry = entry?;
            let lock_path = entry.path();
            if lock_path.file_name().and_then(|name| name.to_str()) == Some(".capacity.lock")
                || lock_path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    != Some("lock")
            {
                continue;
            }
            migrated += 1;
            if !self.active_policy.is_distributed() && migrated > migration_limit {
                return Err(CustodyError::Capacity);
            }
            let stem = required_digest_stem(&lock_path)?;
            let state_path = self.state_path(&stem);
            let replay = match durable_fs::read_file_bounded(&state_path, self.state_byte_limit) {
                Ok(bytes) => classify_replay(&bytes).map_err(CustodyError::Invalid)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(CustodyError::Io(error)),
            };
            if replay {
                replay_candidates.push((record_modified(&state_path, &lock_path), stem));
            } else if self.active_policy.is_distributed() {
                let mut maintenance = self.read_maintenance_index()?;
                let sequence = self.enqueue_maintenance(&stem, &mut maintenance)?;
                self.write_active_marker(
                    &stem,
                    &ActiveMarker {
                        binding_sha256: None,
                        maintenance_sequence: sequence,
                    },
                )?;
            } else {
                active_index
                    .as_mut()
                    .expect("indexed custody has an active index")
                    .reserve_unbound(stem)?;
            }
        }
        replay_candidates.sort_by_key(|candidate| candidate.0);
        if let Some(active_index) = active_index.as_ref() {
            self.write_active_index(active_index)?;
        }
        for (_, stem) in replay_candidates {
            if !self.place_replay(&stem, Path::new(""))? {
                return Err(CustodyError::Capacity);
            }
        }
        self.write_current_schema(&schema_path)?;
        Ok(())
    }

    fn upgrade_policy_active_index(
        &self,
        schema_path: &Path,
        current_lock_path: &Path,
        classify_replay: &impl Fn(&[u8]) -> Result<bool, String>,
    ) -> Result<(), CustodyError> {
        self.require_index_directory(&self.replay_root())?;
        self.require_index_directory(&self.owner_root())?;
        self.prepare_temporary_root()?;
        let active = self.read_active_index()?;
        if self.active_policy.is_distributed() {
            if active.slots.len() > MAX_POLICY_ACTIVE_INDEX_MIGRATION_SLOTS {
                return Err(CustodyError::Migration(format!(
                    "schema-v5 launch custody has {} slots, above the supported one-time migration envelope of {}; keep the schema-v5 provider installed until direct terminal cleanup compacts completed launch custody below that envelope, then retry the upgrade; do not delete provider sessions (schema-v6 runtime population itself has no fixed limit)",
                    active.slots.len(),
                    MAX_POLICY_ACTIVE_INDEX_MIGRATION_SLOTS
                )));
            }
            self.prepare_distributed_roots()?;
            for slot in active.slots.iter().filter(|slot| slot.occupied == 1) {
                let replay = match durable_fs::read_file_bounded(
                    &self.state_path(&slot.request_sha256),
                    self.state_byte_limit,
                ) {
                    Ok(bytes) => classify_replay(&bytes).map_err(CustodyError::Invalid)?,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                    Err(error) => return Err(CustodyError::Io(error)),
                };
                if replay && self.place_replay(&slot.request_sha256, current_lock_path)? {
                    continue;
                }
                let mut maintenance = self.read_maintenance_index()?;
                let sequence = self.enqueue_maintenance(&slot.request_sha256, &mut maintenance)?;
                self.write_active_marker(
                    &slot.request_sha256,
                    &ActiveMarker {
                        binding_sha256: slot.binding_sha256.clone(),
                        maintenance_sequence: sequence,
                    },
                )?;
            }
            self.write_current_schema(schema_path)?;
            remove_file_if_present(&self.active_index_path())?;
            durable_fs::sync_directory(&self.index_root)?;
            return Ok(());
        }
        self.publish_active_representation(active, schema_path)
    }

    fn upgrade_bound_active_index(&self, schema_path: &Path) -> Result<(), CustodyError> {
        self.require_index_directory(&self.replay_root())?;
        self.require_index_directory(&self.owner_root())?;
        self.prepare_temporary_root()?;
        let active = self.read_active_index()?;
        self.publish_active_representation(active, schema_path)
    }

    fn upgrade_unbound_active_index(&self, schema_path: &Path) -> Result<(), CustodyError> {
        self.require_index_directory(&self.replay_root())?;
        self.require_index_directory(&self.owner_root())?;
        self.prepare_temporary_root()?;
        let active = self.read_active_index()?;
        self.publish_active_representation(active, schema_path)
    }

    fn upgrade_predecessor_index(&self, schema_path: &Path) -> Result<(), CustodyError> {
        self.prepare_temporary_root()?;
        match fs::metadata(self.owner_root()) {
            Ok(metadata) if metadata.is_dir() => {
                fs::remove_dir_all(self.owner_root())?;
                durable_fs::sync_directory(&self.index_root)?;
            }
            Ok(_) => {
                return Err(CustodyError::Invalid(
                    "request custody replay owner root is not a directory".to_string(),
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(CustodyError::Io(error)),
        }
        create_private_child_directory(&self.owner_root())?;

        let head = self.read_replay_head()?;
        let mut records = Vec::new();
        let mut authoritative = HashMap::<String, (u64, u64)>::new();
        for slot in 0..self.replay_slots as u64 {
            let path = self.replay_slot_path(slot);
            let bytes = match durable_fs::read_file_bounded(&path, MAX_INDEX_RECORD_BYTES) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(CustodyError::Io(error)),
            };
            let mut replay: ReplaySlot = serde_json::from_slice(&bytes)
                .map_err(|error| CustodyError::Invalid(error.to_string()))?;
            replay.validate(self.replay_slots, slot)?;
            let sequence = match replay.sequence {
                Some(sequence) if sequence < head => sequence,
                Some(_) => {
                    return Err(CustodyError::Invalid(
                        "request custody replay slot is ahead of its head".to_string(),
                    ))
                }
                None => predecessor_replay_sequence(head, slot, self.replay_slots)?,
            };
            replay.sequence = Some(sequence);
            let candidate = (sequence, slot);
            authoritative
                .entry(replay.request_sha256.clone())
                .and_modify(|selected| {
                    if candidate.0 > selected.0 {
                        *selected = candidate;
                    }
                })
                .or_insert(candidate);
            records.push((slot, replay));
        }

        let mut removed_duplicate = false;
        for (slot, replay) in records {
            let sequence = replay.sequence.expect("predecessor sequence was assigned");
            if authoritative.get(&replay.request_sha256) == Some(&(sequence, slot)) {
                self.write_replay_slot(&self.replay_slot_path(slot), &replay)?;
                self.write_replay_owner(
                    &replay.request_sha256,
                    &ReplayOwner {
                        sequence,
                        displaced: None,
                    },
                )?;
            } else {
                remove_file_if_present(&self.replay_slot_path(slot))?;
                removed_duplicate = true;
            }
        }
        if removed_duplicate {
            durable_fs::sync_directory(&self.replay_root())?;
        }
        let active = self.read_active_index()?;
        self.publish_active_representation(active, schema_path)
    }

    fn publish_active_representation(
        &self,
        active: ActiveIndex,
        schema_path: &Path,
    ) -> Result<(), CustodyError> {
        if self.active_policy.is_distributed() {
            self.prepare_distributed_roots()?;
            for slot in active.slots.iter().filter(|slot| slot.occupied == 1) {
                let mut maintenance = self.read_maintenance_index()?;
                let sequence = self.enqueue_maintenance(&slot.request_sha256, &mut maintenance)?;
                self.write_active_marker(
                    &slot.request_sha256,
                    &ActiveMarker {
                        binding_sha256: slot.binding_sha256.clone(),
                        maintenance_sequence: sequence,
                    },
                )?;
            }
            self.write_current_schema(schema_path)?;
            remove_file_if_present(&self.active_index_path())?;
            durable_fs::sync_directory(&self.index_root)?;
            return Ok(());
        }
        self.write_active_index(&active)?;
        self.write_current_schema(schema_path)
    }

    fn place_replay(&self, stem: &str, current_lock_path: &Path) -> Result<bool, CustodyError> {
        if self.replay_slots == 0 {
            return Err(CustodyError::Capacity);
        }
        if let Some(owner) = self.read_replay_owner(stem)? {
            return self.complete_replay_placement(stem, &owner, current_lock_path, true);
        }
        let Some((owner, displacement)) = self.reserve_replay_owner(stem, current_lock_path)?
        else {
            return Ok(false);
        };
        self.publish_replay_placement(stem, &owner, displacement)
    }

    fn reserve_replay_owner(
        &self,
        stem: &str,
        current_lock_path: &Path,
    ) -> Result<Option<(ReplayOwner, Option<ReplayDisplacement>)>, CustodyError> {
        let head = self.read_replay_head()?;
        let probes = self.replay_slots.min(MAX_REPLAY_EVICTION_PROBES);
        for offset in 0..probes {
            let sequence = head.wrapping_add(offset as u64);
            let slot = sequence % self.replay_slots as u64;
            if let Some(reservation) = self.read_replay_reservation(slot)? {
                let owner = self.read_replay_owner(&reservation.request_sha256)?;
                if owner.as_ref().is_some_and(|owner| {
                    owner.sequence == reservation.sequence
                        && owner.displaced == reservation.displaced
                }) {
                    continue;
                }
                // A slot reservation becomes globally authoritative only when
                // its matching request-local owner is durable. If the process
                // stops between those two publications, the active request is
                // still authoritative and may allocate afresh; reclaim this
                // owner-less reservation without scanning the ring population.
                remove_file_if_present(&self.replay_reservation_path(slot))?;
                durable_fs::sync_directory(&self.owner_root())?;
            }
            let prior = self.read_replay_slot(slot)?;
            if prior.as_ref().is_some_and(|prior| {
                prior.request_sha256 == stem && prior.sequence == Some(sequence)
            }) {
                return Err(CustodyError::Invalid(
                    "request custody replay slot has no matching owner".to_string(),
                ));
            }
            let displacement = match prior.as_ref() {
                Some(prior) => match self.acquire_replay_displacement(prior, current_lock_path)? {
                    Some(displacement) => Some(displacement),
                    None => continue,
                },
                None => None,
            };
            let owner = ReplayOwner {
                sequence,
                displaced: prior.clone(),
            };
            self.write_replay_reservation(
                slot,
                &ReplayReservation {
                    sequence,
                    request_sha256: stem.to_string(),
                    displaced: prior,
                },
            )?;
            // The slot reservation is the durable serialization point. Once
            // it exists, another completion must skip this physical ring slot
            // even if this process stops before publishing the request-local
            // owner or advancing the head.
            self.advance_replay_head(&owner)?;
            self.write_replay_owner(stem, &owner)?;
            return Ok(Some((owner, displacement)));
        }
        Ok(None)
    }

    fn complete_replay_placement(
        &self,
        stem: &str,
        owner: &ReplayOwner,
        current_lock_path: &Path,
        advance_head: bool,
    ) -> Result<bool, CustodyError> {
        if advance_head {
            self.advance_replay_head(owner)?;
        }
        let slot = owner.sequence % self.replay_slots as u64;
        let current = self.read_replay_slot(slot)?;
        let target_is_published = current.as_ref().is_some_and(|current| {
            current.request_sha256 == stem && current.sequence == Some(owner.sequence)
        });
        if target_is_published && owner.displaced.is_none() {
            self.remove_matching_replay_reservation(slot, stem, owner.sequence)?;
            return Ok(true);
        }
        match self.read_replay_reservation(slot)? {
            Some(reservation)
                if reservation.request_sha256 == stem && reservation.sequence == owner.sequence => {
            }
            Some(_) if target_is_published => return Ok(true),
            Some(_) => {
                // A predecessor binary could publish a request-local owner
                // without a global slot reservation. If another request has
                // since claimed the slot, discard only this incomplete owner
                // and allocate the still-active terminal request afresh.
                remove_file_if_present(&self.owner_path(stem))?;
                durable_fs::sync_directory(&self.owner_root())?;
                return if advance_head {
                    self.place_replay(stem, current_lock_path)
                } else {
                    Ok(false)
                };
            }
            None => {
                if !target_is_published && current != owner.displaced {
                    remove_file_if_present(&self.owner_path(stem))?;
                    durable_fs::sync_directory(&self.owner_root())?;
                    return if advance_head {
                        self.place_replay(stem, current_lock_path)
                    } else {
                        Ok(false)
                    };
                }
                self.write_replay_reservation(
                    slot,
                    &ReplayReservation {
                        sequence: owner.sequence,
                        request_sha256: stem.to_string(),
                        displaced: owner.displaced.clone(),
                    },
                )?;
            }
        }
        if !target_is_published && current != owner.displaced {
            return Err(CustodyError::Invalid(
                "request custody pending replay placement lost its reserved slot".to_string(),
            ));
        }
        let displacement = match owner.displaced.as_ref() {
            Some(displaced) => {
                match self.acquire_replay_displacement(displaced, current_lock_path)? {
                    Some(displacement) => Some(displacement),
                    None => return Ok(false),
                }
            }
            None => None,
        };
        self.publish_replay_placement(stem, owner, displacement)
    }

    fn advance_replay_head(&self, owner: &ReplayOwner) -> Result<(), CustodyError> {
        let head_path = self.index_root.join("head.json");
        let head = self.read_replay_head()?;
        if head <= owner.sequence {
            self.write_replay_head(&head_path, owner.sequence.wrapping_add(1))?;
        }
        Ok(())
    }

    fn publish_replay_placement(
        &self,
        stem: &str,
        owner: &ReplayOwner,
        displacement: Option<ReplayDisplacement>,
    ) -> Result<bool, CustodyError> {
        let slot = owner.sequence % self.replay_slots as u64;
        let slot_path = self.replay_slot_path(slot);
        let current = self.read_replay_slot(slot)?;
        if !current.as_ref().is_some_and(|current| {
            current.request_sha256 == stem && current.sequence == Some(owner.sequence)
        }) {
            if current != owner.displaced {
                return Err(CustodyError::Invalid(
                    "request custody replay placement changed before publication".to_string(),
                ));
            }
            let replay = ReplaySlot::current(owner.sequence, stem.to_string());
            self.write_replay_slot(&slot_path, &replay)?;
        }
        if let Some(displacement) = displacement {
            self.retire_replay_displacement(displacement)?;
        }
        if owner.displaced.is_some() {
            self.write_replay_owner(
                stem,
                &ReplayOwner {
                    sequence: owner.sequence,
                    displaced: None,
                },
            )?;
        }
        self.remove_matching_replay_reservation(slot, stem, owner.sequence)?;
        Ok(true)
    }

    fn acquire_replay_displacement(
        &self,
        replay: &ReplaySlot,
        current_lock_path: &Path,
    ) -> Result<Option<ReplayDisplacement>, CustodyError> {
        let sequence = replay.sequence.ok_or_else(|| {
            CustodyError::Invalid("request custody replay slot has no sequence".to_string())
        })?;
        let slot = sequence % self.replay_slots as u64;
        replay.validate(self.replay_slots, slot)?;
        let pin_path = self
            .pin_root()
            .join(format!("{}.pin", replay.request_sha256));
        let pin = match self.try_lock_pin_exclusive(&replay.request_sha256)? {
            Some(pin) => Some(pin),
            None if pin_path.exists() => return Ok(None),
            None => None,
        };
        let lock_path = self.lock_path(&replay.request_sha256);
        if lock_path == current_lock_path {
            return Ok(None);
        }
        let lock = open_lock(&lock_path)?;
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(CustodyError::Io(error)),
        }
        Ok(Some(ReplayDisplacement {
            replay: replay.clone(),
            pin,
            _lock: lock,
        }))
    }

    fn retire_replay_displacement(
        &self,
        displacement: ReplayDisplacement,
    ) -> Result<(), CustodyError> {
        let stem = &displacement.replay.request_sha256;
        let owner = self.read_replay_owner(stem)?;
        if owner
            .as_ref()
            .is_some_and(|owner| owner.sequence != displacement.replay.sequence.unwrap_or(u64::MAX))
        {
            return Ok(());
        }
        if self.active_contains(stem)? {
            if owner.is_some() {
                remove_file_if_present(&self.owner_path(stem))?;
                durable_fs::sync_directory(&self.owner_root())?;
            }
            return Ok(());
        }
        remove_file_if_present(&self.state_path(stem))?;
        remove_file_if_present(&self.lock_path(stem))?;
        if let Some((pin, pin_path)) = displacement.pin {
            remove_file_if_present(&pin_path)?;
            drop(pin);
            durable_fs::sync_directory(&self.pin_root())?;
        }
        durable_fs::sync_directory(&self.state_root)?;
        durable_fs::sync_directory(&self.lock_root)?;
        if owner.is_some() {
            remove_file_if_present(&self.owner_path(stem))?;
            durable_fs::sync_directory(&self.owner_root())?;
        }
        Ok(())
    }

    fn active_contains(&self, stem: &str) -> Result<bool, CustodyError> {
        if self.active_policy.is_distributed() {
            return Ok(self.read_active_marker(stem)?.is_some());
        }
        Ok(self.read_active_index()?.contains(stem))
    }

    fn remove_abandoned_active(
        &self,
        stem: &str,
        lock_path: &Path,
        current_lock_path: &Path,
    ) -> Result<bool, CustodyError> {
        if lock_path == current_lock_path {
            return Ok(false);
        }
        let pin_path = self.pin_root().join(format!("{stem}.pin"));
        let pin = match self.try_lock_pin_exclusive(stem)? {
            Some(pin) => Some(pin),
            None if pin_path.exists() => return Ok(false),
            None => None,
        };
        let lock = open_lock(lock_path)?;
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(CustodyError::Io(error)),
        }
        remove_file_if_present(lock_path)?;
        if let Some((pin, pin_path)) = pin {
            remove_file_if_present(&pin_path)?;
            drop(pin);
            durable_fs::sync_directory(&self.pin_root())?;
        }
        durable_fs::sync_directory(&self.lock_root)?;
        Ok(true)
    }

    fn try_reap_pre_effect_candidate(
        &self,
        stem: &str,
        classify_pre_effect: &impl Fn(&str, &[u8]) -> Result<bool, String>,
    ) -> Result<PreEffectCandidateOutcome, CustodyError> {
        let pin_path = self.pin_root().join(format!("{stem}.pin"));
        let pin = match self.try_lock_pin_exclusive(stem)? {
            Some(pin) => Some(pin),
            None if pin_path.exists() => return Ok(PreEffectCandidateOutcome::Busy),
            None => None,
        };
        let lock_path = self.lock_path(stem);
        let lock = open_lock(&lock_path)?;
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(PreEffectCandidateOutcome::Busy)
            }
            Err(error) => return Err(CustodyError::Io(error)),
        }
        if self.read_replay_owner(stem)?.is_some() {
            return Ok(PreEffectCandidateOutcome::NoLongerPreEffect);
        }
        let state_path = self.state_path(stem);
        let bytes = match durable_fs::read_file_bounded(&state_path, self.state_byte_limit) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(CustodyError::Io(error)),
        };
        if let Some(bytes) = bytes.as_deref() {
            if !classify_pre_effect(stem, bytes).map_err(CustodyError::Invalid)? {
                return Ok(PreEffectCandidateOutcome::NoLongerPreEffect);
            }
            remove_file_if_present(&state_path)?;
            durable_fs::sync_directory(&self.state_root)?;
        }
        self.remove_active_marker(&state_path)?;
        remove_file_if_present(&lock_path)?;
        if let Some((pin, pin_path)) = pin {
            remove_file_if_present(&pin_path)?;
            drop(pin);
            durable_fs::sync_directory(&self.pin_root())?;
        }
        durable_fs::sync_directory(&self.lock_root)?;
        Ok(PreEffectCandidateOutcome::Reaped)
    }

    fn try_lock_pin_exclusive(
        &self,
        stem: &str,
    ) -> Result<Option<(fs::File, PathBuf)>, CustodyError> {
        let path = self.pin_root().join(format!("{stem}.pin"));
        let Some(pin) = open_existing_lock(&path)? else {
            return Ok(None);
        };
        match fs2::FileExt::try_lock_exclusive(&pin) {
            Ok(()) => Ok(Some((pin, path))),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn state_path(&self, stem: &str) -> PathBuf {
        self.state_root.join(format!("{stem}.json"))
    }

    fn lock_path(&self, stem: &str) -> PathBuf {
        self.lock_root.join(format!("{stem}.lock"))
    }

    fn active_index_path(&self) -> PathBuf {
        self.index_root.join("active.json")
    }

    fn active_root(&self) -> PathBuf {
        self.index_root.join("active")
    }

    fn active_marker_path(&self, stem: &str) -> PathBuf {
        self.active_root().join(format!("{stem}.json"))
    }

    fn maintenance_root(&self) -> PathBuf {
        self.index_root.join("maintenance")
    }

    fn maintenance_index_path(&self) -> PathBuf {
        self.index_root.join("maintenance.json")
    }

    fn maintenance_ticket_path(&self, sequence: u64) -> PathBuf {
        self.maintenance_root().join(format!("{sequence:020}.json"))
    }

    fn pre_effect_maintenance_root(&self) -> PathBuf {
        self.index_root.join("pre-effect-maintenance")
    }

    fn pre_effect_maintenance_lock_root(&self) -> PathBuf {
        self.index_root.join("pre-effect-maintenance-locks")
    }

    fn pre_effect_shard_path(&self, shard: u64) -> PathBuf {
        self.pre_effect_maintenance_root()
            .join(format!("{shard:02}.json"))
    }

    fn pre_effect_shard_lock_path(&self, shard: u64) -> PathBuf {
        self.pre_effect_maintenance_lock_root()
            .join(format!("{shard:02}.lock"))
    }

    fn try_lock_pre_effect_shard(&self, shard: u64) -> Result<Option<fs::File>, CustodyError> {
        let lock = open_lock(&self.pre_effect_shard_lock_path(shard))?;
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => Ok(Some(lock)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn replay_root(&self) -> PathBuf {
        self.index_root.join("replay")
    }

    fn replay_slot_lock_root(&self) -> PathBuf {
        self.index_root.join("replay-slot-locks")
    }

    fn replay_slot_lock_path(&self, slot: u64) -> PathBuf {
        self.replay_slot_lock_root().join(format!("{slot:04}.lock"))
    }

    fn try_lock_replay_slot(&self, slot: u64) -> Result<Option<fs::File>, CustodyError> {
        let lock = open_lock(&self.replay_slot_lock_path(slot))?;
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => Ok(Some(lock)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn replay_slot_path(&self, slot: u64) -> PathBuf {
        self.replay_root().join(format!("{slot:04}.json"))
    }

    fn owner_root(&self) -> PathBuf {
        self.index_root.join("owners")
    }

    fn owner_path(&self, stem: &str) -> PathBuf {
        self.owner_root().join(format!("{stem}.json"))
    }

    fn replay_reservation_path(&self, slot: u64) -> PathBuf {
        self.owner_root()
            .join(format!("slot-{slot:04}.reservation.json"))
    }

    fn pin_root(&self) -> PathBuf {
        self.index_root.join("pins")
    }

    fn temporary_root(&self) -> PathBuf {
        self.index_root.join(".write-tmp")
    }

    fn prepare_temporary_root(&self) -> Result<(), CustodyError> {
        let root = self.temporary_root();
        match fs::metadata(&root) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(CustodyError::Invalid(
                    "request custody temporary publication root is not a directory".to_string(),
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_private_child_directory(&root)?;
            }
            Err(error) => return Err(CustodyError::Io(error)),
        }
        let mut visited = 0_usize;
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            visited += 1;
            if visited > 1 || !entry.file_type()?.is_file() {
                return Err(CustodyError::Invalid(
                    "request custody temporary publication state is inconsistent".to_string(),
                ));
            }
            fs::remove_file(entry.path())?;
        }
        if visited != 0 {
            durable_fs::sync_directory(&root)?;
        }
        Ok(())
    }

    fn prepare_active_root(&self) -> Result<(), CustodyError> {
        match fs::metadata(self.active_root()) {
            Ok(metadata) if metadata.is_dir() => Ok(()),
            Ok(_) => Err(CustodyError::Invalid(
                "request custody active marker root is not a directory".to_string(),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_private_child_directory(&self.active_root())
            }
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn prepare_distributed_roots(&self) -> Result<(), CustodyError> {
        self.prepare_active_root()?;
        match fs::metadata(self.maintenance_root()) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(CustodyError::Invalid(
                    "request custody maintenance root is not a directory".to_string(),
                ))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_private_child_directory(&self.maintenance_root())?;
            }
            Err(error) => return Err(CustodyError::Io(error)),
        }
        match self.read_maintenance_index() {
            Ok(_) => Ok(()),
            Err(CustodyError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                self.write_maintenance_index(&MaintenanceIndex::empty())
            }
            Err(error) => Err(error),
        }
    }

    fn prepare_pre_effect_maintenance_roots(&self) -> Result<(), CustodyError> {
        for root in [
            self.pre_effect_maintenance_root(),
            self.pre_effect_maintenance_lock_root(),
        ] {
            match fs::metadata(&root) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => {
                    return Err(CustodyError::Invalid(
                        "request custody pre-effect maintenance root is not a directory"
                            .to_string(),
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    create_private_child_directory(&root)?;
                }
                Err(error) => return Err(CustodyError::Io(error)),
            }
        }
        Ok(())
    }

    fn prepare_replay_slot_lock_root(&self) -> Result<(), CustodyError> {
        match fs::metadata(self.replay_slot_lock_root()) {
            Ok(metadata) if metadata.is_dir() => Ok(()),
            Ok(_) => Err(CustodyError::Invalid(
                "request custody replay slot lock root is not a directory".to_string(),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_private_child_directory(&self.replay_slot_lock_root())
            }
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn read_active_marker(&self, stem: &str) -> Result<Option<ActiveMarker>, CustodyError> {
        if !valid_digest(stem) {
            return Err(CustodyError::Invalid(
                "request custody active marker has an invalid digest".to_string(),
            ));
        }
        match durable_fs::read_file_bounded(&self.active_marker_path(stem), MAX_INDEX_RECORD_BYTES)
        {
            Ok(bytes) => {
                let marker: ActiveMarker = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                marker.validate()?;
                Ok(Some(marker))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn write_active_marker(&self, stem: &str, marker: &ActiveMarker) -> Result<(), CustodyError> {
        if !valid_digest(stem) {
            return Err(CustodyError::Invalid(
                "request custody active marker has an invalid digest".to_string(),
            ));
        }
        marker.validate()?;
        let value = serde_json::to_value(marker)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_json_atomic(&self.active_marker_path(stem), &value)
    }

    fn reserve_active_marker(&self, stem: &str, binding_sha256: &str) -> Result<(), CustodyError> {
        if !valid_digest(binding_sha256) {
            return Err(CustodyError::Invalid(
                "request custody active marker binding is invalid".to_string(),
            ));
        }
        if let Some(marker) = self.read_active_marker(stem)? {
            return match marker.reservation(binding_sha256) {
                ActiveReservation::Matching => Ok(()),
                ActiveReservation::Unbound | ActiveReservation::Conflicting => {
                    Err(CustodyError::Invalid(
                        "request custody active marker conflicts with the attempted binding"
                            .to_string(),
                    ))
                }
                ActiveReservation::Absent => unreachable!("a loaded active marker is present"),
            };
        }
        let mut maintenance = self.read_maintenance_index()?;
        let sequence = self.enqueue_maintenance(stem, &mut maintenance)?;
        self.write_active_marker(
            stem,
            &ActiveMarker {
                binding_sha256: Some(binding_sha256.to_string()),
                maintenance_sequence: sequence,
            },
        )
    }

    fn reserve_independent_active_marker(
        &self,
        stem: &str,
        binding_sha256: &str,
    ) -> Result<(), CustodyError> {
        if !valid_digest(binding_sha256) {
            return Err(CustodyError::Invalid(
                "request custody active marker binding is invalid".to_string(),
            ));
        }
        if let Some(marker) = self.read_active_marker(stem)? {
            return match marker.reservation(binding_sha256) {
                ActiveReservation::Matching => Ok(()),
                ActiveReservation::Unbound | ActiveReservation::Conflicting => {
                    Err(CustodyError::Invalid(
                        "request custody active marker conflicts with the attempted binding"
                            .to_string(),
                    ))
                }
                ActiveReservation::Absent => unreachable!("a loaded active marker is present"),
            };
        }
        self.write_active_marker(
            stem,
            &ActiveMarker {
                binding_sha256: Some(binding_sha256.to_string()),
                maintenance_sequence: DIRECT_MAINTENANCE_SEQUENCE,
            },
        )
    }

    fn read_maintenance_index(&self) -> Result<MaintenanceIndex, CustodyError> {
        let bytes =
            durable_fs::read_file_bounded(&self.maintenance_index_path(), MAX_INDEX_RECORD_BYTES)?;
        let index: MaintenanceIndex = serde_json::from_slice(&bytes)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        index.validate()?;
        Ok(index)
    }

    fn write_maintenance_index(&self, index: &MaintenanceIndex) -> Result<(), CustodyError> {
        index.validate()?;
        let value = serde_json::to_value(index)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_json_atomic(&self.maintenance_index_path(), &value)
    }

    fn enqueue_maintenance(
        &self,
        stem: &str,
        index: &mut MaintenanceIndex,
    ) -> Result<u64, CustodyError> {
        if !valid_digest(stem) || index.next_sequence == u64::MAX {
            return Err(CustodyError::Invalid(
                "request custody maintenance sequence is exhausted or invalid".to_string(),
            ));
        }
        let sequence = index.next_sequence;
        let value = serde_json::to_value(MaintenanceTicket {
            request_sha256: stem.to_string(),
        })
        .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_json_atomic(&self.maintenance_ticket_path(sequence), &value)?;
        index.next_sequence += 1;
        self.write_maintenance_index(index)?;
        Ok(sequence)
    }

    fn read_maintenance_ticket(
        &self,
        sequence: u64,
    ) -> Result<Option<MaintenanceTicket>, CustodyError> {
        match durable_fs::read_file_bounded(
            &self.maintenance_ticket_path(sequence),
            MAX_INDEX_RECORD_BYTES,
        ) {
            Ok(bytes) => {
                let ticket: MaintenanceTicket = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                if !valid_digest(&ticket.request_sha256) {
                    return Err(CustodyError::Invalid(
                        "request custody maintenance ticket is invalid".to_string(),
                    ));
                }
                Ok(Some(ticket))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn remove_maintenance_ticket(&self, sequence: u64) -> Result<(), CustodyError> {
        remove_file_if_present(&self.maintenance_ticket_path(sequence))?;
        durable_fs::sync_directory(&self.maintenance_root())?;
        Ok(())
    }

    fn read_pre_effect_shard(&self, shard: u64) -> Result<PreEffectMaintenanceShard, CustodyError> {
        match durable_fs::read_file_bounded(
            &self.pre_effect_shard_path(shard),
            MAX_PRE_EFFECT_SHARD_BYTES,
        ) {
            Ok(bytes) => {
                let record: PreEffectMaintenanceShard = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                record.validate()?;
                Ok(record)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(PreEffectMaintenanceShard::default())
            }
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn write_pre_effect_shard(
        &self,
        shard: u64,
        record: &PreEffectMaintenanceShard,
    ) -> Result<(), CustodyError> {
        record.validate()?;
        let path = self.pre_effect_shard_path(shard);
        if record.candidates.is_empty() {
            remove_file_if_present(&path)?;
            durable_fs::sync_directory(&self.pre_effect_maintenance_root())?;
            return Ok(());
        }
        let value = serde_json::to_value(record)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_json_atomic_bounded(&path, &value, MAX_PRE_EFFECT_SHARD_BYTES)
    }

    fn write_replay_head(&self, path: &Path, next_slot: u64) -> Result<(), CustodyError> {
        self.write_json_atomic(path, &json!({"next_slot": next_slot}))
    }

    fn read_replay_head(&self) -> Result<u64, CustodyError> {
        let path = self.index_root.join("head.json");
        match durable_fs::read_file_bounded(&path, MAX_INDEX_RECORD_BYTES) {
            Ok(bytes) => {
                let value: Value = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                value["next_slot"].as_u64().ok_or_else(|| {
                    CustodyError::Invalid("custody replay head has no next slot".to_string())
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn read_replay_slot(&self, slot: u64) -> Result<Option<ReplaySlot>, CustodyError> {
        let path = self.replay_slot_path(slot);
        match durable_fs::read_file_bounded(&path, MAX_INDEX_RECORD_BYTES) {
            Ok(bytes) => {
                let replay: ReplaySlot = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                replay.validate(self.replay_slots, slot)?;
                if replay.sequence.is_none() {
                    return Err(CustodyError::Invalid(
                        "request custody replay slot has no sequence".to_string(),
                    ));
                }
                Ok(Some(replay))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn write_replay_slot(&self, path: &Path, replay: &ReplaySlot) -> Result<(), CustodyError> {
        let value = serde_json::to_value(replay)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_json_atomic(path, &value)
    }

    fn read_replay_owner(&self, stem: &str) -> Result<Option<ReplayOwner>, CustodyError> {
        let path = self.owner_path(stem);
        match durable_fs::read_file_bounded(&path, MAX_INDEX_RECORD_BYTES) {
            Ok(bytes) => {
                let owner: ReplayOwner = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                let slot = owner.sequence % self.replay_slots as u64;
                if owner
                    .displaced
                    .as_ref()
                    .is_some_and(|displaced| displaced.validate(self.replay_slots, slot).is_err())
                {
                    return Err(CustodyError::Invalid(
                        "request custody replay owner is inconsistent".to_string(),
                    ));
                }
                Ok(Some(owner))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn write_replay_owner(&self, stem: &str, owner: &ReplayOwner) -> Result<(), CustodyError> {
        if !valid_digest(stem) {
            return Err(CustodyError::Invalid(
                "request custody replay owner has an invalid digest".to_string(),
            ));
        }
        let value = serde_json::to_value(owner)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_json_atomic(&self.owner_path(stem), &value)
    }

    fn read_replay_reservation(
        &self,
        slot: u64,
    ) -> Result<Option<ReplayReservation>, CustodyError> {
        let path = self.replay_reservation_path(slot);
        match durable_fs::read_file_bounded(&path, MAX_INDEX_RECORD_BYTES) {
            Ok(bytes) => {
                let reservation: ReplayReservation = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                reservation.validate(self.replay_slots, slot)?;
                Ok(Some(reservation))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn write_replay_reservation(
        &self,
        slot: u64,
        reservation: &ReplayReservation,
    ) -> Result<(), CustodyError> {
        reservation.validate(self.replay_slots, slot)?;
        let value = serde_json::to_value(reservation)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_json_atomic(&self.replay_reservation_path(slot), &value)
    }

    fn remove_matching_replay_reservation(
        &self,
        slot: u64,
        stem: &str,
        sequence: u64,
    ) -> Result<(), CustodyError> {
        if self
            .read_replay_reservation(slot)?
            .is_some_and(|reservation| {
                reservation.request_sha256 == stem && reservation.sequence == sequence
            })
        {
            remove_file_if_present(&self.replay_reservation_path(slot))?;
            durable_fs::sync_directory(&self.owner_root())?;
        }
        Ok(())
    }

    fn require_index_directory(&self, path: &Path) -> Result<(), CustodyError> {
        match fs::metadata(path) {
            Ok(metadata) if metadata.is_dir() => Ok(()),
            Ok(_) => Err(CustodyError::Invalid(
                "request custody index child is not a directory".to_string(),
            )),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn validate_predecessor_active_slots(&self, schema: &Value) -> Result<(), CustodyError> {
        if schema["active_limit"].as_u64() != Some(self.active_policy.initial_slots() as u64) {
            return Err(CustodyError::Invalid(
                "request custody predecessor active slot count is inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_policy_index_schema(&self, schema: &Value) -> Result<(), CustodyError> {
        let matches = match self.active_policy {
            ActiveIndexPolicy::Fixed { limit } => {
                schema["active_policy"].as_str() == Some("fixed")
                    && schema["active_limit"].as_u64() == Some(limit as u64)
            }
            ActiveIndexPolicy::Distributed {
                predecessor_initial_slots: initial_slots,
            } => {
                schema["active_policy"].as_str() == Some("elastic")
                    && schema["initial_active_slots"].as_u64() == Some(initial_slots as u64)
            }
        };
        if !matches {
            return Err(CustodyError::Invalid(
                "request custody predecessor active policy is inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_current_schema(&self, schema: &Value) -> Result<(), CustodyError> {
        let matches = match self.active_policy {
            ActiveIndexPolicy::Fixed { limit } => {
                schema["active_policy"].as_str() == Some("fixed")
                    && schema["active_limit"].as_u64() == Some(limit as u64)
                    && schema.get("initial_active_slots").is_none()
            }
            ActiveIndexPolicy::Distributed { .. } => {
                schema["active_policy"].as_str() == Some("distributed")
                    && schema["maintenance_probe_limit"].as_u64()
                        == Some(MAX_ACTIVE_MAINTENANCE_PROBES as u64)
                    && schema.get("active_limit").is_none()
                    && schema.get("initial_active_slots").is_none()
            }
        };
        if !matches {
            return Err(CustodyError::Invalid(
                "request custody active index policy is inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    fn write_current_schema(&self, path: &Path) -> Result<(), CustodyError> {
        let schema = match self.active_policy {
            ActiveIndexPolicy::Fixed { limit } => json!({
                "schema_version": INDEX_SCHEMA_VERSION,
                "active_policy": "fixed",
                "active_limit": limit,
                "active_registration": "indexed",
                "replay_allocation": "sequenced_ring",
                "replay_slots": self.replay_slots,
            }),
            ActiveIndexPolicy::Distributed { .. } => json!({
                "schema_version": INDEX_SCHEMA_VERSION,
                "active_policy": "distributed",
                "active_registration": "request_local",
                "replay_allocation": "hash_sharded",
                "maintenance_probe_limit": MAX_ACTIVE_MAINTENANCE_PROBES,
                "replay_slots": self.replay_slots,
            }),
        };
        self.write_json_atomic(path, &schema)
    }

    fn read_active_index(&self) -> Result<ActiveIndex, CustodyError> {
        let bytes = if self.active_policy.is_distributed() {
            let path = self.active_index_path();
            let bytes = fs::metadata(&path)?.len();
            if bytes > MAX_POLICY_ACTIVE_INDEX_MIGRATION_BYTES as u64 {
                return Err(CustodyError::Migration(format!(
                    "predecessor launch custody index is {bytes} bytes, above the supported {}-byte one-time migration envelope; keep the predecessor provider installed until direct terminal cleanup compacts completed launch custody below that envelope, then retry the upgrade; do not delete provider sessions (distributed runtime custody has no fixed population limit)",
                    MAX_POLICY_ACTIVE_INDEX_MIGRATION_BYTES
                )));
            }
            durable_fs::read_file_bounded(&path, MAX_POLICY_ACTIVE_INDEX_MIGRATION_BYTES)?
        } else {
            durable_fs::read_file_bounded(&self.active_index_path(), MAX_ACTIVE_INDEX_BYTES)?
        };
        let index: ActiveIndex = serde_json::from_slice(&bytes)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        index.validate(self.active_policy)?;
        Ok(index)
    }

    fn write_active_index(&self, index: &ActiveIndex) -> Result<(), CustodyError> {
        if self.active_policy.is_distributed() {
            return Err(CustodyError::Invalid(
                "distributed request custody cannot publish a shared active index".to_string(),
            ));
        }
        index.validate(self.active_policy)?;
        let value = serde_json::to_value(index)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_json_atomic_bounded(&self.active_index_path(), &value, MAX_ACTIVE_INDEX_BYTES)
    }

    fn write_json_atomic(&self, path: &Path, value: &Value) -> Result<(), CustodyError> {
        self.write_json_atomic_bounded(path, value, MAX_INDEX_RECORD_BYTES)
    }

    fn write_json_atomic_bounded(
        &self,
        path: &Path,
        value: &Value,
        byte_limit: usize,
    ) -> Result<(), CustodyError> {
        let bytes =
            serde_json::to_vec(value).map_err(|error| CustodyError::Invalid(error.to_string()))?;
        if bytes.len() > byte_limit {
            return Err(CustodyError::Capacity);
        }
        self.write_bytes_atomic(path, &bytes)
    }

    fn write_bytes_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), CustodyError> {
        let parent = path.parent().ok_or_else(|| {
            CustodyError::Invalid("request custody record has no parent".to_string())
        })?;
        if !fs::metadata(parent)?.is_dir() {
            return Err(CustodyError::Invalid(
                "request custody record parent is not a directory".to_string(),
            ));
        }
        // Unique request and replay records may be published concurrently.
        // Keeping the temporary in the destination directory preserves atomic
        // rename semantics without making the shared migration scratch root a
        // steady-state writer rendezvous.
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(bytes)?;
        temporary.as_file_mut().sync_all()?;
        temporary
            .persist(path)
            .map_err(|error| CustodyError::Io(error.error))?;
        durable_fs::sync_directory(parent)?;
        Ok(())
    }
}

fn predecessor_replay_sequence(
    head: u64,
    slot: u64,
    replay_slots: usize,
) -> Result<u64, CustodyError> {
    if replay_slots == 0 || head == 0 {
        return Err(CustodyError::Invalid(
            "request custody predecessor replay head is inconsistent".to_string(),
        ));
    }
    let last = head - 1;
    if slot > last {
        return Err(CustodyError::Invalid(
            "request custody predecessor replay slot is ahead of its head".to_string(),
        ));
    }
    Ok(last - ((last - slot) % replay_slots as u64))
}

fn create_private_child_directory(path: &Path) -> Result<(), CustodyError> {
    create_private_directory_unsynced(path)?;
    durable_fs::sync_directory(
        path.parent()
            .ok_or_else(|| CustodyError::Invalid("custody directory has no parent".to_string()))?,
    )?;
    Ok(())
}

fn create_private_index_subtree(root: &Path, children: &[PathBuf]) -> Result<(), CustodyError> {
    create_private_directory_unsynced(root)?;
    for child in children {
        if child.parent() != Some(root) {
            return Err(CustodyError::Invalid(
                "custody index child escaped its root".to_string(),
            ));
        }
        create_private_directory_unsynced(child)?;
    }
    durable_fs::sync_directory(root)?;
    durable_fs::sync_directory(
        root.parent()
            .ok_or_else(|| CustodyError::Invalid("custody index has no parent".to_string()))?,
    )?;
    Ok(())
}

fn create_private_directory_unsynced(path: &Path) -> Result<(), CustodyError> {
    #[cfg(unix)]
    let builder = {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder
    };
    #[cfg(not(unix))]
    let builder = fs::DirBuilder::new();
    match builder.create(path) {
        Ok(()) => {}
        Err(error)
            if error.kind() == std::io::ErrorKind::AlreadyExists
                && fs::metadata(path).is_ok_and(|metadata| metadata.is_dir()) => {}
        Err(error) => return Err(CustodyError::Io(error)),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn required_digest_stem(path: &Path) -> Result<String, CustodyError> {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| stem.len() == 64 && stem.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            CustodyError::Invalid(format!(
                "request custody entry has no SHA-256 file stem: {}",
                path.display()
            ))
        })?;
    Ok(stem.to_string())
}

fn pre_effect_shard(stem: &str) -> Result<u64, CustodyError> {
    if !valid_digest(stem) {
        return Err(CustodyError::Invalid(
            "request custody pre-effect candidate has an invalid digest".to_string(),
        ));
    }
    let prefix = u16::from_str_radix(&stem[..4], 16).map_err(|error| {
        CustodyError::Invalid(format!(
            "request custody pre-effect candidate has an invalid digest prefix: {error}"
        ))
    })?;
    Ok(u64::from(prefix) % PRE_EFFECT_MAINTENANCE_SHARDS as u64)
}

fn valid_digest(stem: &str) -> bool {
    stem.len() == 64 && stem.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn open_lock(path: &Path) -> Result<fs::File, CustodyError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(CustodyError::Io)
}

fn open_existing_lock(path: &Path) -> std::io::Result<Option<fs::File>> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).truncate(false);
    match options.open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn remove_file_if_present(path: &Path) -> Result<(), CustodyError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CustodyError::Io(error)),
    }
}

fn record_expired(path: &Path, retention: Duration) -> bool {
    fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age >= retention)
}

fn record_modified(primary: &Path, fallback: &Path) -> SystemTime {
    fs::metadata(primary)
        .or_else(|_| fs::metadata(fallback))
        .and_then(|metadata| metadata.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_less_marker_preserves_exact_retry_and_is_reclaimable_by_a_successor() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let custody = RequestCustody::new_fixed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            directory.path().join(".custody-v2"),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        let first_binding = format!("{:064x}", 101);
        let second_binding = format!("{:064x}", 102);
        let first = directory.path().join(format!("{:064x}.json", 1));
        let second = directory.path().join(format!("{:064x}.json", 2));
        assert_eq!(
            custody
                .maintain(&directory.path().join("initialize.lock"), |_| Ok(false))
                .expect("initialize request custody"),
            0
        );
        custody
            .reserve_active(&first, &first_binding)
            .expect("reserve first pre-state request");
        custody
            .reserve_active(&second, &second_binding)
            .expect("reserve second pre-state request");

        assert_eq!(
            custody
                .maintain(&first.with_extension("lock"), |_| Ok(false))
                .expect("preserve the exact pre-state reservation"),
            2
        );
        assert_eq!(
            custody
                .active_reservation(&first, &first_binding)
                .expect("first active marker"),
            ActiveReservation::Matching
        );
        assert_eq!(
            custody
                .active_reservation(&first, &second_binding)
                .expect("conflicting first active marker"),
            ActiveReservation::Conflicting
        );

        assert_eq!(
            custody
                .maintain(&directory.path().join("unrelated.lock"), |_| Ok(false))
                .expect("retire an abandoned pre-state reservation"),
            1
        );
        assert_eq!(
            custody
                .active_reservation(&second, &second_binding)
                .expect("second active marker was retired"),
            ActiveReservation::Absent
        );
    }

    #[test]
    fn schema_v3_active_index_upgrades_to_an_explicit_unbound_reservation() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let index_root = directory.path().join(".custody-v2");
        let custody = RequestCustody::new_fixed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            index_root.clone(),
            1024,
            1,
            1,
            Duration::from_secs(60),
        );
        let state = directory.path().join(format!("{:064x}.json", 1));
        let binding = format!("{:064x}", 101);
        custody
            .maintain(&directory.path().join("initialize.lock"), |_| Ok(false))
            .expect("initialize current custody");
        custody
            .reserve_active(&state, &binding)
            .expect("reserve current active marker");

        let mut active: Value = serde_json::from_slice(
            &fs::read(index_root.join("active.json")).expect("current active index"),
        )
        .expect("parse current active index");
        active["slots"][0]
            .as_object_mut()
            .expect("active slot")
            .remove("binding_sha256");
        fs::write(
            index_root.join("active.json"),
            serde_json::to_vec(&active).expect("serialize schema-v3 active index"),
        )
        .expect("schema-v3 active index");
        fs::write(
            index_root.join("schema.json"),
            serde_json::to_vec(&json!({
                "schema_version": UNBOUND_ACTIVE_INDEX_SCHEMA_VERSION,
                "active_limit": 1,
                "replay_slots": 1,
            }))
            .expect("serialize schema-v3 custody"),
        )
        .expect("schema-v3 custody");

        assert_eq!(
            custody
                .maintain(&state.with_extension("lock"), |_| Ok(false))
                .expect("upgrade schema-v3 custody"),
            1
        );
        assert_eq!(
            custody
                .active_reservation(&state, &binding)
                .expect("upgraded unbound reservation"),
            ActiveReservation::Unbound
        );
        custody
            .bind_unbound_active(&state, &binding)
            .expect("bind upgraded pre-state reservation");
        assert_eq!(
            custody
                .active_reservation(&state, &binding)
                .expect("bound upgraded reservation"),
            ActiveReservation::Matching
        );
    }

    #[test]
    fn schema_v5_elastic_index_migrates_to_distributed_markers() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let index_root = directory.path().join(".custody-v2");
        let custody = RequestCustody::new_distributed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            index_root.clone(),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        custody
            .maintain(&directory.path().join("initialize.lock"), |_| Ok(false))
            .expect("initialize current elastic custody");

        let first = format!("{:064x}", 1);
        let second = format!("{:064x}", 2);
        for stem in [&first, &second] {
            fs::write(
                directory.path().join(format!("{stem}.json")),
                br#"{"phase":"prepared"}"#,
            )
            .expect("active request state");
            fs::write(directory.path().join(format!("{stem}.lock")), b"")
                .expect("active request lock");
        }
        fs::write(
            index_root.join("active.json"),
            serde_json::to_vec(&json!({
                "next_probe": 1,
                "slots": [
                    {"occupied": 1, "request_sha256": first, "binding_sha256": format!("{:064x}", 101)},
                    {"occupied": 0, "request_sha256": EMPTY_ACTIVE_DIGEST},
                    {"occupied": 1, "request_sha256": second, "binding_sha256": format!("{:064x}", 102)},
                    {"occupied": 0, "request_sha256": EMPTY_ACTIVE_DIGEST},
                    {"occupied": 0, "request_sha256": EMPTY_ACTIVE_DIGEST},
                ],
            }))
            .expect("serialize schema-v4 active index"),
        )
        .expect("schema-v4 active index");
        fs::write(
            index_root.join("schema.json"),
            serde_json::to_vec(&json!({
                "schema_version": POLICY_ACTIVE_INDEX_SCHEMA_VERSION,
                "active_policy": "elastic",
                "initial_active_slots": 2,
                "replay_slots": 2,
            }))
            .expect("serialize schema-v4 custody"),
        )
        .expect("schema-v5 custody");

        assert_eq!(
            custody
                .maintain(&directory.path().join("current.lock"), |_| Ok(false))
                .expect("migrate schema-v5 elastic custody"),
            0
        );
        let schema: Value = serde_json::from_slice(
            &fs::read(index_root.join("schema.json")).expect("migrated schema"),
        )
        .expect("parse migrated schema");
        assert_eq!(schema["schema_version"], INDEX_SCHEMA_VERSION);
        assert_eq!(schema["active_policy"], "distributed");
        assert!(schema.get("initial_active_slots").is_none());
        assert!(schema.get("active_limit").is_none());
        assert!(!index_root.join("active.json").exists());
        assert_eq!(
            fs::read_dir(index_root.join("active"))
                .expect("distributed active markers")
                .count(),
            2
        );
        assert_eq!(
            custody
                .active_reservation(
                    &directory.path().join(format!("{first}.json")),
                    &format!("{:064x}", 101)
                )
                .expect("first migrated active marker"),
            ActiveReservation::Matching
        );
        assert_eq!(
            custody
                .active_reservation(
                    &directory.path().join(format!("{second}.json")),
                    &format!("{:064x}", 102)
                )
                .expect("second migrated active marker"),
            ActiveReservation::Matching
        );
        fs::write(index_root.join("active.json"), b"stale predecessor index")
            .expect("simulate interruption after schema publication");
        custody
            .maintain(&directory.path().join("current.lock"), |_| Ok(false))
            .expect("finish predecessor index cleanup");
        assert!(
            !index_root.join("active.json").exists(),
            "current-schema recovery removes only the obsolete predecessor index"
        );
    }

    #[test]
    fn schema_v5_transition_envelope_fails_before_mutating_predecessor_custody() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let index_root = directory.path().join(".custody-v2");
        let custody = RequestCustody::new_distributed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            index_root.clone(),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        custody
            .maintain(&directory.path().join("initialize.lock"), |_| Ok(false))
            .expect("initialize current custody");
        let active = ActiveIndex {
            next_probe: 0,
            slots: (0..=MAX_POLICY_ACTIVE_INDEX_MIGRATION_SLOTS)
                .map(|index| ActiveSlot {
                    occupied: 1,
                    request_sha256: format!("{index:064x}"),
                    binding_sha256: Some(format!("{:064x}", index + 10_000)),
                })
                .collect(),
        };
        fs::write(
            index_root.join("active.json"),
            serde_json::to_vec(&active).expect("serialize predecessor active index"),
        )
        .expect("publish predecessor active index");
        fs::write(
            index_root.join("schema.json"),
            serde_json::to_vec(&json!({
                "schema_version": POLICY_ACTIVE_INDEX_SCHEMA_VERSION,
                "active_policy": "elastic",
                "initial_active_slots": 2,
                "replay_slots": 2,
            }))
            .expect("serialize predecessor schema"),
        )
        .expect("publish predecessor schema");

        let error = custody
            .maintain(&directory.path().join("current.lock"), |_| Ok(false))
            .expect_err("oversized predecessor population requires reduction first");
        assert!(error
            .to_string()
            .contains("do not delete provider sessions"));
        let schema: Value = serde_json::from_slice(
            &fs::read(index_root.join("schema.json")).expect("predecessor schema remains"),
        )
        .expect("parse predecessor schema");
        assert_eq!(schema["schema_version"], POLICY_ACTIVE_INDEX_SCHEMA_VERSION);
        assert!(index_root.join("active.json").exists());
        assert_eq!(
            fs::read_dir(index_root.join("active"))
                .expect("distributed marker root")
                .count(),
            0
        );
    }

    #[test]
    fn indexless_transition_envelope_fails_before_mutating_predecessor_custody() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let index_root = directory.path().join(".custody-v2");
        fs::create_dir_all(&index_root).expect("interrupted predecessor index root");
        let sentinel = index_root.join("predecessor-sentinel");
        fs::write(&sentinel, b"unchanged").expect("predecessor sentinel");
        for index in 0..MAX_INDEXLESS_MIGRATION_REQUEST_LOCKS {
            fs::write(directory.path().join(format!("{index:064x}.lock")), b"")
                .expect("predecessor request lock");
        }
        let custody = RequestCustody::new_distributed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            index_root.clone(),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );

        let maximum = custody
            .transition_preflight()
            .expect("read-only predecessor preflight");
        assert_eq!(maximum.request_locks, MAX_INDEXLESS_MIGRATION_REQUEST_LOCKS);
        assert!(maximum.blocker.is_none());
        fs::write(
            directory.path().join(format!(
                "{:064x}.lock",
                MAX_INDEXLESS_MIGRATION_REQUEST_LOCKS
            )),
            b"",
        )
        .expect("first above-envelope predecessor request lock");
        let preflight = custody
            .transition_preflight()
            .expect("above-envelope predecessor preflight");
        assert_eq!(preflight.format, "indexless_predecessor");
        assert!(preflight.blocker.is_some());
        let error = custody
            .maintain(&directory.path().join("current.lock"), |_| Ok(false))
            .expect_err("oversized indexless population requires reduction first");
        assert!(error
            .to_string()
            .contains("do not delete provider sessions"));
        assert_eq!(
            fs::read(&sentinel).expect("predecessor remains"),
            b"unchanged"
        );
        assert!(!index_root.join("schema.json").exists());
    }

    #[test]
    fn distributed_terminal_handoff_retires_only_the_exact_active_marker() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let custody = RequestCustody::new_distributed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            directory.path().join(".custody-v2"),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        custody
            .maintain(&directory.path().join("initialize.lock"), |_| Ok(false))
            .expect("initialize distributed custody");
        let first = format!("{:064x}", 1);
        let second = format!("{:064x}", 2);
        let first_state = directory.path().join(format!("{first}.json"));
        let second_state = directory.path().join(format!("{second}.json"));
        custody
            .reserve_active(&first_state, &format!("{:064x}", 101))
            .expect("reserve first marker");
        custody
            .reserve_active(&second_state, &format!("{:064x}", 102))
            .expect("reserve second marker");
        fs::write(&first_state, br#"{"terminal":true}"#).expect("first terminal state");
        fs::write(directory.path().join(format!("{first}.lock")), b"").expect("first request lock");

        assert!(custody
            .publish_replay_and_retire_active(&first_state, Path::new(""))
            .expect("publish exact terminal replay"));
        assert_eq!(
            custody
                .active_reservation(&first_state, &format!("{:064x}", 101))
                .expect("retired first active marker"),
            ActiveReservation::Absent
        );
        assert_eq!(
            custody
                .active_reservation(&second_state, &format!("{:064x}", 102))
                .expect("retained second active marker"),
            ActiveReservation::Matching
        );
        assert!(custody
            .replay_owner_exists(&first_state)
            .expect("first replay owner"));
        assert!(first_state.exists(), "terminal state remains replayable");
    }

    #[test]
    fn sharded_terminal_handoffs_complete_concurrently_without_a_global_gate() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let root = directory.path().to_path_buf();
        let custody = RequestCustody::new_distributed(
            root.clone(),
            root.clone(),
            root.join(".custody-v2"),
            1024,
            64,
            4096,
            Duration::from_secs(60),
        );
        custody
            .maintain(&root.join("initialize.lock"), |_| Ok(false))
            .expect("initialize distributed custody");
        let completions = 320;
        for index in 1..=completions {
            let stem = format!("{index:064x}");
            let state = root.join(format!("{stem}.json"));
            custody
                .reserve_independent_active(&state, &format!("{:064x}", index + 10_000))
                .expect("reserve independent active marker");
            fs::write(&state, br#"{"terminal":true}"#).expect("terminal state");
            fs::write(root.join(format!("{stem}.lock")), b"").expect("request lock");
        }

        let workers = (1..=completions)
            .map(|index| {
                let root = root.clone();
                std::thread::spawn(move || {
                    let custody = RequestCustody::new_distributed(
                        root.clone(),
                        root.clone(),
                        root.join(".custody-v2"),
                        1024,
                        64,
                        4096,
                        Duration::from_secs(60),
                    );
                    let state = root.join(format!("{index:064x}.json"));
                    assert!(custody
                        .publish_sharded_replay_and_retire_active(&state)
                        .expect("publish sharded replay"));
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("concurrent terminal handoff");
        }

        assert_eq!(
            fs::read_dir(root.join(".custody-v2/active"))
                .expect("active marker root")
                .count(),
            0
        );
        assert_eq!(
            fs::read_dir(root.join(".custody-v2/replay"))
                .expect("replay root")
                .count(),
            completions
        );
    }

    #[test]
    fn interrupted_sharded_handoff_recovers_around_an_interleaved_completion() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let root = directory.path().to_path_buf();
        let custody = RequestCustody::new_distributed(
            root.clone(),
            root.clone(),
            root.join(".custody-v2"),
            1024,
            2,
            4,
            Duration::from_secs(60),
        );
        custody
            .maintain(&root.join("initialize.lock"), |_| Ok(false))
            .expect("initialize distributed custody");
        let first = format!("{:064x}", 1);
        let second = format!("{:064x}", 5);
        let first_state = root.join(format!("{first}.json"));
        let second_state = root.join(format!("{second}.json"));
        for (state, binding) in [(&first_state, 101), (&second_state, 102)] {
            custody
                .reserve_independent_active(state, &format!("{binding:064x}"))
                .expect("reserve active marker");
            fs::write(state, br#"{"terminal":true}"#).expect("terminal state");
            fs::write(state.with_extension("lock"), b"").expect("request lock");
        }

        let interrupted_slot = 1;
        let owner = ReplayOwner {
            sequence: interrupted_slot,
            displaced: None,
        };
        custody
            .write_replay_reservation(
                interrupted_slot,
                &ReplayReservation {
                    sequence: interrupted_slot,
                    request_sha256: first.clone(),
                    displaced: None,
                },
            )
            .expect("reserve physical slot before interruption");
        custody
            .write_replay_owner(&first, &owner)
            .expect("publish request owner before interruption");

        assert!(custody
            .publish_sharded_replay_and_retire_active(&second_state)
            .expect("interleaved completion uses another probe"));
        assert!(custody
            .publish_sharded_replay_and_retire_active(&first_state)
            .expect("exact interrupted completion resumes its slot"));
        for state in [&first_state, &second_state] {
            assert!(custody
                .replay_owner_exists(state)
                .expect("durable replay owner"));
        }
        assert_eq!(
            fs::read_dir(root.join(".custody-v2/active"))
                .expect("active marker root")
                .count(),
            0
        );
    }

    #[test]
    fn sharded_replay_tries_every_replacement_when_the_first_is_pinned() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let root = directory.path().to_path_buf();
        let custody = RequestCustody::new_distributed(
            root.clone(),
            root.clone(),
            root.join(".custody-v2"),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        custody
            .maintain(Path::new(""), |_| Ok(false))
            .expect("initialize distributed custody");
        let mut states = Vec::new();
        for index in 1..=2 {
            let stem = format!("{index:064x}");
            let state = root.join(format!("{stem}.json"));
            custody
                .reserve_independent_active(&state, &format!("{:064x}", index + 100))
                .expect("reserve terminal request");
            fs::write(&state, br#"{"terminal":true}"#).expect("terminal state");
            fs::write(state.with_extension("lock"), b"").expect("request lock");
            assert!(custody
                .publish_sharded_replay_and_retire_active(&state)
                .expect("fill replay slot"));
            states.push(state);
        }
        let pin = custody
            .pin_existing(&states[0])
            .expect("pin the first replacement");
        let third = root.join(format!("{:064x}.json", 3));
        custody
            .reserve_independent_active(&third, &format!("{:064x}", 103))
            .expect("reserve third request");
        fs::write(&third, br#"{"terminal":true}"#).expect("third terminal state");
        fs::write(third.with_extension("lock"), b"").expect("third request lock");

        assert!(custody
            .publish_sharded_replay_and_retire_active(&third)
            .expect("search every replacement candidate"));
        assert!(custody
            .replay_owner_exists(&states[0])
            .expect("pinned replay remains"));
        assert!(custody
            .replay_owner_exists(&third)
            .expect("new replay is published"));
        assert!(!states[1].exists(), "the unpinned replacement is retired");
        drop(pin);
    }

    #[test]
    fn current_schema_enables_independent_requests_only_after_durable_bootstrap() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let root = directory.path().to_path_buf();
        let index_root = root.join(".custody-v2");
        let custody = RequestCustody::new_distributed(
            root.clone(),
            root,
            index_root.clone(),
            1024,
            2,
            4,
            Duration::from_secs(60),
        );
        custody
            .maintain(Path::new(""), |_| Ok(false))
            .expect("initialize distributed custody");
        let mut schema: Value = serde_json::from_slice(
            &fs::read(index_root.join("schema.json")).expect("current schema"),
        )
        .expect("parse current schema");
        schema
            .as_object_mut()
            .expect("schema object")
            .remove("active_registration");
        schema
            .as_object_mut()
            .expect("schema object")
            .remove("replay_allocation");
        fs::write(
            index_root.join("schema.json"),
            serde_json::to_vec(&schema).expect("serialize predecessor schema"),
        )
        .expect("simulate predecessor current schema");
        fs::remove_dir(index_root.join("replay-slot-locks"))
            .expect("simulate absent sharded lock root");

        assert!(!custody
            .independent_requests_ready()
            .expect("read-only readiness check"));
        custody
            .maintain(Path::new(""), |_| Ok(false))
            .expect("bootstrap independent request representation");
        assert!(custody
            .independent_requests_ready()
            .expect("durable independent readiness"));
    }

    #[test]
    fn distributed_maintenance_requeues_live_work_and_reaps_abandoned_pre_state() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let custody = RequestCustody::new_distributed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            directory.path().join(".custody-v2"),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        custody
            .maintain(&directory.path().join("initialize.lock"), |_| Ok(false))
            .expect("initialize distributed custody");
        let first = directory.path().join(format!("{:064x}.json", 1));
        let second = directory.path().join(format!("{:064x}.json", 2));
        let first_binding = format!("{:064x}", 101);
        let second_binding = format!("{:064x}", 102);
        custody
            .reserve_active(&first, &first_binding)
            .expect("reserve live work");
        custody
            .reserve_active(&second, &second_binding)
            .expect("reserve abandoned pre-state work");
        fs::write(&first, br#"{"phase":"prepared"}"#).expect("live request state");
        fs::write(first.with_extension("lock"), b"").expect("live request lock");

        custody
            .maintain(&directory.path().join("current.lock"), |_| Ok(false))
            .expect("requeue live request");
        custody
            .maintain(&directory.path().join("current.lock"), |_| Ok(false))
            .expect("reap abandoned request");

        assert_eq!(
            custody
                .active_reservation(&first, &first_binding)
                .expect("retained live marker"),
            ActiveReservation::Matching
        );
        assert_eq!(
            custody
                .active_reservation(&second, &second_binding)
                .expect("retired abandoned marker"),
            ActiveReservation::Absent
        );
        let maintenance = custody
            .read_maintenance_index()
            .expect("bounded maintenance index");
        assert_eq!(maintenance.next_probe, 2);
        assert_eq!(maintenance.next_sequence, 3);
    }

    #[test]
    fn sharded_pre_effect_maintenance_reaps_only_proven_unlocked_state() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let root = directory.path().to_path_buf();
        let custody = RequestCustody::new_distributed(
            root.clone(),
            root.clone(),
            root.join(".custody-v2"),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        custody
            .maintain(&root.join("initialize.lock"), |_| Ok(false))
            .expect("initialize distributed custody");
        let abandoned = root.join(format!("{:064x}.json", 1));
        let successor = root.join(format!("{:064x}.json", 2));
        custody
            .reserve_independent_active(&abandoned, &format!("{:064x}", 101))
            .expect("reserve abandoned pre-effect state");
        fs::write(&abandoned, b"pre-effect").expect("write pre-effect state");
        fs::write(abandoned.with_extension("lock"), b"").expect("write abandoned lock");
        custody
            .register_pre_effect_candidate(&abandoned, |_, bytes| Ok(bytes == b"pre-effect"))
            .expect("register abandoned pre-effect state");

        custody
            .reserve_independent_active(&successor, &format!("{:064x}", 102))
            .expect("reserve successor");
        custody
            .register_pre_effect_candidate(&successor, |_, bytes| Ok(bytes == b"pre-effect"))
            .expect("successor reaps abandoned state");

        assert!(!abandoned.exists());
        assert!(!abandoned.with_extension("lock").exists());
        assert_eq!(
            custody
                .active_reservation(&abandoned, &format!("{:064x}", 101))
                .expect("abandoned marker retired"),
            ActiveReservation::Absent
        );
        let shard = pre_effect_shard(
            successor
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("successor digest"),
        )
        .expect("maintenance shard");
        let record = custody
            .read_pre_effect_shard(shard)
            .expect("bounded maintenance record");
        assert_eq!(record.candidates.len(), 1);
        assert!(
            fs::metadata(custody.pre_effect_shard_path(shard))
                .expect("maintenance record metadata")
                .len()
                <= MAX_PRE_EFFECT_SHARD_BYTES as u64
        );
    }

    #[test]
    fn sharded_pre_effect_maintenance_preserves_effect_capable_state() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let root = directory.path().to_path_buf();
        let custody = RequestCustody::new_distributed(
            root.clone(),
            root.clone(),
            root.join(".custody-v2"),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        custody
            .maintain(&root.join("initialize.lock"), |_| Ok(false))
            .expect("initialize distributed custody");
        let effect_capable = root.join(format!("{:064x}.json", 1));
        let successor = root.join(format!("{:064x}.json", 2));
        custody
            .reserve_independent_active(&effect_capable, &format!("{:064x}", 101))
            .expect("reserve effect-capable state");
        fs::write(&effect_capable, b"effect-capable").expect("write effect-capable state");
        fs::write(effect_capable.with_extension("lock"), b"").expect("write request lock");
        custody
            .register_pre_effect_candidate(&effect_capable, |_, bytes| Ok(bytes == b"pre-effect"))
            .expect("register stale accelerator entry");

        custody
            .reserve_independent_active(&successor, &format!("{:064x}", 102))
            .expect("reserve successor");
        custody
            .register_pre_effect_candidate(&successor, |_, bytes| Ok(bytes == b"pre-effect"))
            .expect("inspect effect-capable state");

        assert!(effect_capable.exists());
        assert!(effect_capable.with_extension("lock").exists());
        assert_eq!(
            custody
                .active_reservation(&effect_capable, &format!("{:064x}", 101))
                .expect("effect-capable marker retained"),
            ActiveReservation::Matching
        );
    }

    #[test]
    fn predecessor_upgrade_deduplicates_interrupted_replay_handoff() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let index_root = directory.path().join(".custody-v2");
        let replay_root = index_root.join("replay");
        fs::create_dir_all(&replay_root).expect("predecessor replay root");
        fs::create_dir_all(index_root.join("pins")).expect("predecessor pin root");
        fs::create_dir_all(index_root.join(".write-tmp")).expect("predecessor temporary root");
        let stem = format!("{:064x}", 1);
        fs::write(
            index_root.join("schema.json"),
            serde_json::to_vec(&json!({
                "schema_version": PREDECESSOR_INDEX_SCHEMA_VERSION,
                "active_limit": 2,
                "replay_slots": 2,
            }))
            .expect("serialize predecessor schema"),
        )
        .expect("predecessor schema");
        fs::write(
            index_root.join("active.json"),
            serde_json::to_vec(&json!({
                "next_probe": 0,
                "slots": [
                    {"occupied": 1, "request_sha256": stem},
                    {"occupied": 0, "request_sha256": EMPTY_ACTIVE_DIGEST},
                ],
            }))
            .expect("serialize predecessor active index"),
        )
        .expect("predecessor active index");
        fs::write(index_root.join("head.json"), br#"{"next_slot":2}"#)
            .expect("predecessor replay head");
        for slot in 0..2 {
            fs::write(
                replay_root.join(format!("{slot:04}.json")),
                serde_json::to_vec(&json!({"request_sha256": stem}))
                    .expect("serialize predecessor replay slot"),
            )
            .expect("duplicate predecessor replay slot");
        }
        let state_path = directory.path().join(format!("{stem}.json"));
        fs::write(&state_path, br#"{"terminal":true}"#).expect("terminal request state");
        fs::write(directory.path().join(format!("{stem}.lock")), b"")
            .expect("terminal request lock");

        let custody = RequestCustody::new_fixed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            index_root.clone(),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        assert_eq!(
            custody
                .maintain(&directory.path().join("current.lock"), |_| Ok(true))
                .expect("upgrade interrupted predecessor handoff"),
            0
        );
        assert_eq!(replay_references(&replay_root, &stem), 1);
        assert!(
            state_path.exists(),
            "the authoritative terminal remains replayable"
        );
        let schema: Value = serde_json::from_slice(
            &fs::read(index_root.join("schema.json")).expect("upgraded schema"),
        )
        .expect("parse upgraded schema");
        assert_eq!(
            schema["schema_version"].as_u64(),
            Some(INDEX_SCHEMA_VERSION)
        );
        let owner: ReplayOwner = serde_json::from_slice(
            &fs::read(index_root.join("owners").join(format!("{stem}.json")))
                .expect("upgraded replay owner"),
        )
        .expect("parse upgraded replay owner");
        assert_eq!(owner.sequence, 1);
        assert!(owner.displaced.is_none());
    }

    #[test]
    fn replay_eviction_preserves_state_still_owned_by_active_index() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let custody = RequestCustody::new_fixed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            directory.path().join(".custody-v2"),
            1024,
            2,
            1,
            Duration::from_secs(60),
        );
        let first = format!("{:064x}", 1);
        let first_state = directory.path().join(format!("{first}.json"));
        fs::write(directory.path().join(format!("{first}.lock")), b"").expect("first request lock");
        fs::write(&first_state, br#"{"terminal":false}"#).expect("first active request");
        assert_eq!(
            custody
                .maintain(&directory.path().join("current.lock"), |bytes| {
                    let state: Value =
                        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
                    Ok(state["terminal"].as_bool() == Some(true))
                })
                .expect("initialize active request"),
            1
        );
        fs::write(&first_state, br#"{"terminal":true}"#).expect("complete first request");
        assert!(custody
            .publish_replay_without_retiring_active(
                &first_state,
                &directory.path().join("current.lock")
            )
            .expect("publish first replay without active retirement"));

        let second = format!("{:064x}", 2);
        let second_state = directory.path().join(format!("{second}.json"));
        fs::write(directory.path().join(format!("{second}.lock")), b"")
            .expect("second request lock");
        fs::write(&second_state, br#"{"terminal":true}"#).expect("second terminal request");
        custody
            .reserve_active(&second_state, &second)
            .expect("reserve second request");
        assert!(custody
            .publish_replay_without_retiring_active(
                &second_state,
                &directory.path().join("current.lock")
            )
            .expect("replace first replay while its active owner remains"));
        assert!(
            first_state.exists(),
            "replay replacement must not delete state referenced by the active index"
        );
        assert_eq!(
            replay_references(&directory.path().join(".custody-v2/replay"), &first),
            0
        );
        assert_eq!(
            replay_references(&directory.path().join(".custody-v2/replay"), &second),
            1
        );

        let classify = |bytes: &[u8]| {
            let state: Value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
            Ok(state["terminal"].as_bool() == Some(true))
        };
        assert_eq!(
            custody
                .maintain(&directory.path().join("current.lock"), classify)
                .expect("retire second active owner"),
            1
        );
        assert_eq!(
            custody
                .maintain(&directory.path().join("current.lock"), classify)
                .expect("restore first replay owner"),
            0
        );
        assert!(first_state.exists(), "first terminal remains replayable");
        assert_eq!(
            replay_references(&directory.path().join(".custody-v2/replay"), &first),
            1
        );
    }

    #[test]
    fn reserved_slot_survives_an_interleaved_completion() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let index_root = directory.path().join(".custody-v2");
        let custody = RequestCustody::new_fixed(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            index_root.clone(),
            1024,
            2,
            2,
            Duration::from_secs(60),
        );
        let current_lock = directory.path().join("current.lock");
        assert_eq!(
            custody
                .maintain(&current_lock, |_| Ok(true))
                .expect("initialize request custody"),
            0
        );
        for index in 1..=2 {
            let stem = format!("{index:064x}");
            let state = directory.path().join(format!("{stem}.json"));
            fs::write(directory.path().join(format!("{stem}.lock")), b"")
                .expect("completed request lock");
            fs::write(&state, br#"{"terminal":true}"#).expect("completed request state");
            custody
                .reserve_active(&state, &stem)
                .expect("reserve completion");
            assert_eq!(
                custody
                    .maintain(&current_lock, |_| Ok(true))
                    .expect("fill replay ring"),
                0
            );
        }

        let current = format!("{:064x}", 3);
        let current_state = directory.path().join(format!("{current}.json"));
        fs::write(directory.path().join(format!("{current}.lock")), b"")
            .expect("current request lock");
        fs::write(&current_state, br#"{"terminal":true}"#).expect("current terminal state");
        custody
            .reserve_active(&current_state, &current)
            .expect("reserve current request");
        assert!(custody
            .reserve_replay_slot_without_publication(&current_state, &current_lock)
            .expect("reserve slot before simulated interruption"));
        assert_eq!(custody.read_replay_head().expect("reserved replay head"), 3);
        assert!(custody
            .read_replay_owner(&current)
            .expect("current owner")
            .is_some());
        fs::remove_file(custody.owner_path(&current))
            .expect("simulate interruption before request-local owner publication");

        let interleaved = format!("{:064x}", 4);
        let interleaved_state = directory.path().join(format!("{interleaved}.json"));
        fs::write(directory.path().join(format!("{interleaved}.lock")), b"")
            .expect("interleaved request lock");
        fs::write(&interleaved_state, br#"{"terminal":true}"#).expect("interleaved terminal state");
        custody
            .reserve_active(&interleaved_state, &interleaved)
            .expect("reserve interleaved request");
        assert!(custody
            .publish_replay_and_retire_active(&interleaved_state, &current_lock)
            .expect("publish interleaved completion"));

        assert_eq!(
            custody
                .maintain(&current_lock, |_| Ok(true))
                .expect("resume slot-only replay reservation"),
            0
        );
        let oldest = format!("{:064x}", 1);
        let next_oldest = format!("{:064x}", 2);
        assert!(
            !directory.path().join(format!("{oldest}.json")).exists(),
            "the true oldest replay is retired"
        );
        assert!(
            !directory
                .path()
                .join(format!("{next_oldest}.json"))
                .exists(),
            "the interleaved completion retires the next-oldest replay"
        );
        assert_eq!(replay_references(&index_root.join("replay"), &current), 1);
        assert_eq!(
            replay_references(&index_root.join("replay"), &interleaved),
            1
        );
        assert_eq!(custody.read_replay_head().expect("advanced replay head"), 5);
        assert!(custody.read_replay_reservation(0).unwrap().is_none());
        assert!(custody.read_replay_reservation(1).unwrap().is_none());
    }

    fn replay_references(root: &Path, stem: &str) -> usize {
        fs::read_dir(root)
            .expect("replay root")
            .map(|entry| entry.expect("replay entry").path())
            .filter_map(|path| fs::read(path).ok())
            .filter_map(|bytes| serde_json::from_slice::<ReplaySlot>(&bytes).ok())
            .filter(|record| record.request_sha256 == stem)
            .count()
    }
}
