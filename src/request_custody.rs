//! Declared roles: orchestration, validator, accessor

use crate::durable_fs;
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const INDEX_SCHEMA_VERSION: u64 = 1;
const MAX_INDEX_RECORD_BYTES: usize = 1024;
const MAX_REPLAY_EVICTION_PROBES: usize = 64;

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
            replay_slots,
            orphan_retention,
        }
    }

    pub(crate) fn maintain(
        &self,
        current_lock_path: &Path,
        classify_replay: impl Fn(&[u8]) -> Result<bool, String>,
    ) -> Result<usize, CustodyError> {
        self.initialize(&classify_replay)?;
        let active_root = self.active_root();
        let mut active = 0_usize;
        let mut visited = 0_usize;
        let mut replay_candidates = Vec::new();
        for entry in fs::read_dir(&active_root)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("marker") {
                return Err(CustodyError::Invalid(format!(
                    "active custody entry has an unsupported shape: {}",
                    path.display()
                )));
            }
            if !entry.file_type()?.is_file() {
                return Err(CustodyError::Invalid(format!(
                    "active custody entry is not a file: {}",
                    path.display()
                )));
            }
            visited += 1;
            if visited > self.active_limit.saturating_add(1) {
                return Err(CustodyError::Capacity);
            }
            let stem = required_digest_stem(&path)?;
            let state_path = self.state_path(&stem);
            let lock_path = self.lock_path(&stem);
            let state = match durable_fs::read_file_bounded(&state_path, self.state_byte_limit) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(CustodyError::Io(error)),
            };
            if let Some(bytes) = state {
                if classify_replay(&bytes).map_err(CustodyError::Invalid)? {
                    replay_candidates.push((record_modified(&state_path, &path), stem, path));
                    continue;
                }
            } else if record_expired(&path, self.orphan_retention)
                && self.remove_abandoned_active(&stem, &path, &lock_path, current_lock_path)?
            {
                continue;
            }
            active += 1;
        }
        replay_candidates.sort_by_key(|candidate| candidate.0);
        for (_, stem, marker_path) in replay_candidates {
            if self.place_replay(&stem, current_lock_path)? {
                remove_file_synced(&marker_path)?;
            } else {
                active += 1;
            }
        }
        if active > self.active_limit {
            return Err(CustodyError::Capacity);
        }
        Ok(active)
    }

    pub(crate) fn reserve_active(&self, state_path: &Path) -> Result<(), CustodyError> {
        write_empty_marker(&self.active_marker_path(state_path)?)
    }

    pub(crate) fn pin_existing(&self, state_path: &Path) -> Result<fs::File, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        let pin = open_lock(&self.pin_root().join(format!("{stem}.pin")))?;
        fs2::FileExt::lock_shared(&pin)?;
        Ok(pin)
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
        let active = self.active_marker_path(state_path)?;
        match fs::remove_file(&active) {
            Ok(()) => durable_fs::sync_directory(
                active
                    .parent()
                    .expect("active marker always has a parent directory"),
            )?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(CustodyError::Io(error)),
        }
        Ok(())
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
                if schema["schema_version"].as_u64() != Some(INDEX_SCHEMA_VERSION)
                    || schema["active_limit"].as_u64() != Some(self.active_limit as u64)
                    || schema["replay_slots"].as_u64() != Some(self.replay_slots as u64)
                {
                    return Err(CustodyError::Invalid(
                        "request custody index schema or bounds are inconsistent".to_string(),
                    ));
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
                self.active_root(),
                self.replay_root(),
                self.pin_root(),
                self.temporary_root(),
            ],
        )?;
        let mut migrated = 0_usize;
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
            if migrated > migration_limit {
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
                write_empty_marker(&self.active_root().join(format!("{stem}.marker")))?;
            }
        }
        replay_candidates.sort_by_key(|candidate| candidate.0);
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

    fn place_replay(&self, stem: &str, current_lock_path: &Path) -> Result<bool, CustodyError> {
        if self.replay_slots == 0 {
            return Err(CustodyError::Capacity);
        }
        let head_path = self.index_root.join("head.json");
        let head = match durable_fs::read_file_bounded(&head_path, MAX_INDEX_RECORD_BYTES) {
            Ok(bytes) => {
                let value: Value = serde_json::from_slice(&bytes)
                    .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                value["next_slot"].as_u64().ok_or_else(|| {
                    CustodyError::Invalid("custody replay head has no next slot".to_string())
                })?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(CustodyError::Io(error)),
        };
        let probes = self.replay_slots.min(MAX_REPLAY_EVICTION_PROBES);
        for offset in 0..probes {
            let sequence = head.wrapping_add(offset as u64);
            let slot = sequence % self.replay_slots as u64;
            let slot_path = self.replay_root().join(format!("{slot:04}.json"));
            let prior = match durable_fs::read_file_bounded(&slot_path, MAX_INDEX_RECORD_BYTES) {
                Ok(bytes) => {
                    let value: Value = serde_json::from_slice(&bytes)
                        .map_err(|error| CustodyError::Invalid(error.to_string()))?;
                    Some(
                        value["request_sha256"]
                            .as_str()
                            .ok_or_else(|| {
                                CustodyError::Invalid(
                                    "custody replay slot has no request digest".to_string(),
                                )
                            })?
                            .to_string(),
                    )
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(CustodyError::Io(error)),
            };
            if prior.as_deref() == Some(stem) {
                self.write_replay_head(&head_path, sequence.wrapping_add(1))?;
                return Ok(true);
            }
            if let Some(prior) = prior {
                let prior_pin = match self.try_lock_pin_exclusive(&prior)? {
                    Some(pin) => Some(pin),
                    None if self.pin_root().join(format!("{prior}.pin")).exists() => continue,
                    None => None,
                };
                let prior_lock_path = self.lock_path(&prior);
                if prior_lock_path == current_lock_path {
                    continue;
                }
                let lock = open_lock(&prior_lock_path)?;
                match fs2::FileExt::try_lock_exclusive(&lock) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(error) => return Err(CustodyError::Io(error)),
                }
                remove_file_if_present(&self.state_path(&prior))?;
                remove_file_if_present(&prior_lock_path)?;
                if let Some((pin, pin_path)) = prior_pin {
                    remove_file_if_present(&pin_path)?;
                    drop(pin);
                    durable_fs::sync_directory(&self.pin_root())?;
                }
                durable_fs::sync_directory(&self.state_root)?;
                durable_fs::sync_directory(&self.lock_root)?;
            }
            self.write_json_atomic(&slot_path, &json!({"request_sha256": stem}))?;
            self.write_replay_head(&head_path, sequence.wrapping_add(1))?;
            return Ok(true);
        }
        Ok(false)
    }

    fn remove_abandoned_active(
        &self,
        stem: &str,
        marker_path: &Path,
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
        remove_file_synced(marker_path)?;
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

    fn active_marker_path(&self, state_path: &Path) -> Result<PathBuf, CustodyError> {
        let stem = required_digest_stem(state_path)?;
        Ok(self.active_root().join(format!("{stem}.marker")))
    }

    fn state_path(&self, stem: &str) -> PathBuf {
        self.state_root.join(format!("{stem}.json"))
    }

    fn lock_path(&self, stem: &str) -> PathBuf {
        self.lock_root.join(format!("{stem}.lock"))
    }

    fn active_root(&self) -> PathBuf {
        self.index_root.join("active")
    }

    fn replay_root(&self) -> PathBuf {
        self.index_root.join("replay")
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

    fn write_json_atomic(&self, path: &Path, value: &Value) -> Result<(), CustodyError> {
        let bytes =
            serde_json::to_vec(value).map_err(|error| CustodyError::Invalid(error.to_string()))?;
        if bytes.len() > MAX_INDEX_RECORD_BYTES {
            return Err(CustodyError::Capacity);
        }
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

fn write_empty_marker(path: &Path) -> Result<(), CustodyError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?;
    durable_fs::sync_directory(
        path.parent()
            .ok_or_else(|| CustodyError::Invalid("custody marker has no parent".to_string()))?,
    )?;
    Ok(())
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

fn remove_file_synced(path: &Path) -> Result<(), CustodyError> {
    remove_file_if_present(path)?;
    durable_fs::sync_directory(
        path.parent()
            .expect("request custody file always has a parent"),
    )?;
    Ok(())
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
