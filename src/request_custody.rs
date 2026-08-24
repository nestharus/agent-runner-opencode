//! Declared roles: orchestration, validator, accessor

use crate::durable_fs;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const INDEX_SCHEMA_VERSION: u64 = 4;
const UNBOUND_ACTIVE_INDEX_SCHEMA_VERSION: u64 = 3;
const PREDECESSOR_INDEX_SCHEMA_VERSION: u64 = 2;
const MAX_INDEX_RECORD_BYTES: usize = 1024;
const MAX_ACTIVE_INDEX_BYTES: usize = 16 * 1024;
const MAX_REPLAY_EVICTION_PROBES: usize = 64;
const EMPTY_ACTIVE_DIGEST: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
struct ActiveIndex {
    next_probe: usize,
    slots: Vec<ActiveSlot>,
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

struct ReplayDisplacement {
    replay: ReplaySlot,
    pin: Option<(fs::File, PathBuf)>,
    _lock: fs::File,
}

impl ActiveIndex {
    fn empty(active_limit: usize) -> Self {
        Self {
            next_probe: 0,
            slots: (0..active_limit).map(|_| ActiveSlot::empty()).collect(),
        }
    }

    fn validate(&self, active_limit: usize, grow_active: bool) -> Result<(), CustodyError> {
        let active_size_is_valid = if grow_active {
            self.slots.len() >= active_limit
        } else {
            self.slots.len() == active_limit
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

    fn reserve(
        &mut self,
        stem: String,
        binding_sha256: String,
        grow_active: bool,
    ) -> Result<(), CustodyError> {
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
            None if grow_active => {
                self.slots.push(ActiveSlot::empty());
                self.slots
                    .last_mut()
                    .expect("the growing active index contains the appended slot")
            }
            None => return Err(CustodyError::Capacity),
        };
        slot.occupied = 1;
        slot.request_sha256 = stem;
        slot.binding_sha256 = Some(binding_sha256);
        Ok(())
    }

    fn reserve_unbound(&mut self, stem: String, grow_active: bool) -> Result<(), CustodyError> {
        if self.contains(&stem) {
            return Ok(());
        }
        let slot = match self.slots.iter_mut().find(|slot| slot.occupied == 0) {
            Some(slot) => slot,
            None if grow_active => {
                self.slots.push(ActiveSlot::empty());
                self.slots
                    .last_mut()
                    .expect("the growing active index contains the appended slot")
            }
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

#[derive(Debug)]
pub(crate) enum CustodyError {
    Io(std::io::Error),
    Invalid(String),
    Capacity,
}

impl std::fmt::Display for CustodyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Invalid(message) => formatter.write_str(message),
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
    active_limit: usize,
    grow_active: bool,
    replay_slots: usize,
    orphan_retention: Duration,
}

impl RequestCustody {
    pub(crate) fn new(
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
            active_limit,
            grow_active: false,
            replay_slots,
            orphan_retention,
        }
    }

    /// Allow active request custody to grow beyond its initial compact index.
    ///
    /// Launch uses this mode because active records describe independent
    /// runtime obligations, not a global concurrency budget. Quota refresh
    /// retains fixed-capacity behavior through the default constructor.
    pub(crate) fn with_growing_active(mut self) -> Self {
        self.grow_active = true;
        self
    }

    /// Advance bounded custody maintenance while the capability's capacity
    /// lock is held. The caller must retain that lock until any newly reserved
    /// request marker has been followed by creation of its request-lock file.
    pub(crate) fn maintain(
        &self,
        current_lock_path: &Path,
        classify_replay: impl Fn(&[u8]) -> Result<bool, String>,
    ) -> Result<usize, CustodyError> {
        self.initialize(&classify_replay)?;
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
            // Callers hold the capability's capacity lock from before this
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

    pub(crate) fn reserve_active(
        &self,
        state_path: &Path,
        binding_sha256: &str,
    ) -> Result<(), CustodyError> {
        let stem = required_digest_stem(state_path)?;
        let mut index = self.read_active_index()?;
        index.reserve(stem, binding_sha256.to_string(), self.grow_active)?;
        self.write_active_index(&index)
    }

    /// Report how the active index owns this exact request and attempt.
    ///
    /// Callers use this after `maintain` while still holding their capacity
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
        Ok(self.read_active_index()?.reservation(&stem, binding_sha256))
    }

    /// Claim an unbound schema-v3 reservation only after the caller proves,
    /// under the capacity lock, that neither request state nor a request lock
    /// exists. Current-schema reservations are bound when first published.
    pub(crate) fn bind_unbound_active(
        &self,
        state_path: &Path,
        binding_sha256: &str,
    ) -> Result<(), CustodyError> {
        let stem = required_digest_stem(state_path)?;
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
        let mut index = self.read_active_index()?;
        if index.remove(&stem) {
            self.write_active_index(&index)?;
        }
        Ok(())
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
    fn reserve_replay_owner_without_advancing_head(
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
        classify_replay: &impl Fn(&[u8]) -> Result<bool, String>,
    ) -> Result<(), CustodyError> {
        let schema_path = self.index_root.join("schema.json");
        match durable_fs::read_file_bounded(&schema_path, MAX_INDEX_RECORD_BYTES) {
            Ok(bytes) => {
                let schema: Value = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                if schema["active_limit"].as_u64() != Some(self.active_limit as u64)
                    || schema["replay_slots"].as_u64() != Some(self.replay_slots as u64)
                {
                    return Err(CustodyError::Invalid(
                        "request custody index schema or bounds are inconsistent".to_string(),
                    ));
                }
                match schema["schema_version"].as_u64() {
                    Some(INDEX_SCHEMA_VERSION) => {
                        self.require_index_directory(&self.replay_root())?;
                        self.require_index_directory(&self.owner_root())?;
                    }
                    Some(UNBOUND_ACTIVE_INDEX_SCHEMA_VERSION) => {
                        self.upgrade_unbound_active_index(&schema_path)?;
                    }
                    Some(PREDECESSOR_INDEX_SCHEMA_VERSION) => {
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
                self.owner_root(),
                self.pin_root(),
                self.temporary_root(),
            ],
        )?;
        let mut migrated = 0_usize;
        let mut active_index = ActiveIndex::empty(self.active_limit);
        let mut replay_candidates = Vec::new();
        let migration_limit = self.active_limit.saturating_add(self.replay_slots);
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
            if !self.grow_active && migrated > migration_limit {
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
            } else {
                active_index.reserve_unbound(stem, self.grow_active)?;
            }
        }
        replay_candidates.sort_by_key(|candidate| candidate.0);
        self.write_active_index(&active_index)?;
        for (_, stem) in replay_candidates {
            if !self.place_replay(&stem, Path::new(""))? {
                return Err(CustodyError::Capacity);
            }
        }
        self.write_json_atomic(
            &schema_path,
            &json!({
                "schema_version": INDEX_SCHEMA_VERSION,
                "active_limit": self.active_limit,
                "replay_slots": self.replay_slots,
            }),
        )?;
        Ok(())
    }

    fn upgrade_unbound_active_index(&self, schema_path: &Path) -> Result<(), CustodyError> {
        self.require_index_directory(&self.replay_root())?;
        self.require_index_directory(&self.owner_root())?;
        self.prepare_temporary_root()?;
        let active = self.read_active_index()?;
        self.write_active_index(&active)?;
        self.write_json_atomic(
            schema_path,
            &json!({
                "schema_version": INDEX_SCHEMA_VERSION,
                "active_limit": self.active_limit,
                "replay_slots": self.replay_slots,
            }),
        )
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
        self.write_active_index(&active)?;
        self.write_json_atomic(
            schema_path,
            &json!({
                "schema_version": INDEX_SCHEMA_VERSION,
                "active_limit": self.active_limit,
                "replay_slots": self.replay_slots,
            }),
        )
    }

    fn place_replay(&self, stem: &str, current_lock_path: &Path) -> Result<bool, CustodyError> {
        if self.replay_slots == 0 {
            return Err(CustodyError::Capacity);
        }
        if let Some(owner) = self.read_replay_owner(stem)? {
            return self.complete_replay_placement(stem, &owner, current_lock_path);
        }
        let Some((owner, displacement)) = self.reserve_replay_owner(stem, current_lock_path)?
        else {
            return Ok(false);
        };
        self.advance_replay_head(&owner)?;
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
                displaced: prior,
            };
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
    ) -> Result<bool, CustodyError> {
        self.advance_replay_head(owner)?;
        let slot = owner.sequence % self.replay_slots as u64;
        let current = self.read_replay_slot(slot)?;
        let target_is_published = current.as_ref().is_some_and(|current| {
            current.request_sha256 == stem && current.sequence == Some(owner.sequence)
        });
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
        if self.read_active_index()?.contains(stem) {
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

    fn replay_root(&self) -> PathBuf {
        self.index_root.join("replay")
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

    fn require_index_directory(&self, path: &Path) -> Result<(), CustodyError> {
        match fs::metadata(path) {
            Ok(metadata) if metadata.is_dir() => Ok(()),
            Ok(_) => Err(CustodyError::Invalid(
                "request custody index child is not a directory".to_string(),
            )),
            Err(error) => Err(CustodyError::Io(error)),
        }
    }

    fn read_active_index(&self) -> Result<ActiveIndex, CustodyError> {
        let bytes = if self.grow_active {
            fs::read(self.active_index_path())?
        } else {
            durable_fs::read_file_bounded(&self.active_index_path(), MAX_ACTIVE_INDEX_BYTES)?
        };
        let index: ActiveIndex = serde_json::from_slice(&bytes)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        index.validate(self.active_limit, self.grow_active)?;
        Ok(index)
    }

    fn write_active_index(&self, index: &ActiveIndex) -> Result<(), CustodyError> {
        index.validate(self.active_limit, self.grow_active)?;
        let value = serde_json::to_value(index)
            .map_err(|error| CustodyError::Invalid(error.to_string()))?;
        if self.grow_active {
            self.write_json_atomic_unbounded(&self.active_index_path(), &value)
        } else {
            self.write_json_atomic_bounded(
                &self.active_index_path(),
                &value,
                MAX_ACTIVE_INDEX_BYTES,
            )
        }
    }

    fn write_json_atomic_unbounded(&self, path: &Path, value: &Value) -> Result<(), CustodyError> {
        let bytes =
            serde_json::to_vec(value).map_err(|error| CustodyError::Invalid(error.to_string()))?;
        self.write_bytes_atomic(path, &bytes)
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
        let temporary_root = self.temporary_root();
        let mut temporary = tempfile::NamedTempFile::new_in(&temporary_root)?;
        temporary.write_all(&bytes)?;
        temporary.as_file_mut().sync_all()?;
        temporary
            .persist(path)
            .map_err(|error| CustodyError::Io(error.error))?;
        durable_fs::sync_directory(parent)?;
        durable_fs::sync_directory(&temporary_root)?;
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
        let custody = RequestCustody::new(
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
        let custody = RequestCustody::new(
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

        let custody = RequestCustody::new(
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
        let custody = RequestCustody::new(
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
    fn owner_only_interruption_preserves_oldest_first_replay_order() {
        let directory = tempfile::tempdir().expect("request custody directory");
        let index_root = directory.path().join(".custody-v2");
        let custody = RequestCustody::new(
            directory.path().to_path_buf(),
            directory.path().to_path_buf(),
            index_root.clone(),
            1024,
            1,
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
            .reserve_replay_owner_without_advancing_head(&current_state, &current_lock)
            .expect("reserve owner before simulated interruption"));
        assert_eq!(
            custody.read_replay_head().expect("unchanged replay head"),
            2
        );
        assert!(custody
            .read_replay_owner(&current)
            .expect("current owner")
            .is_some());

        assert_eq!(
            custody
                .maintain(&current_lock, |_| Ok(true))
                .expect("resume owner-only replay reservation"),
            0
        );
        let oldest = format!("{:064x}", 1);
        let next_oldest = format!("{:064x}", 2);
        assert!(
            !directory.path().join(format!("{oldest}.json")).exists(),
            "the true oldest replay is retired"
        );
        assert!(
            directory
                .path()
                .join(format!("{next_oldest}.json"))
                .exists(),
            "the next-oldest replay remains available"
        );
        assert_eq!(
            replay_references(&index_root.join("replay"), &next_oldest),
            1
        );
        assert_eq!(replay_references(&index_root.join("replay"), &current), 1);
        assert_eq!(custody.read_replay_head().expect("advanced replay head"), 3);
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
