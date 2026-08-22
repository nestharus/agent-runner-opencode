//! Declared roles: orchestration, validator
//! intrinsic_surface_declarations:
//!   - component: src/durable_fs.rs
//!     role: intrinsic-surface
//!     Domain: durable provider-owned directory publication
//!     Owns:
//!       - ordered creation and parent synchronization of directory links
//!       - private directory permission persistence

use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Error, ErrorKind, Read};
use std::path::{Path, PathBuf};

pub(crate) const MAX_BOUND_EXECUTABLE_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const MAX_AUTH_FILE_BYTES: usize = 1024 * 1024;

pub(crate) fn create_directories(path: &Path) -> std::io::Result<()> {
    create_directory_chain(path, false)
}

pub(crate) fn create_private_directories(path: &Path) -> std::io::Result<()> {
    create_directory_chain(path, true)
}

pub(crate) fn prepare_best_effort_private_directory(path: &Path) -> std::io::Result<()> {
    prepare_best_effort_private_directory_with_sync(path, sync_directory)
}

pub(crate) fn is_executable_file(path: &Path) -> std::io::Result<bool> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        Ok(metadata.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        Ok(true)
    }
}

pub(crate) fn read_file_bounded(path: &Path, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    read_file_bounded_or(path, maximum_bytes, maximum_bytes, |_| false).map(|(bytes, _)| bytes)
}

pub(crate) fn sha256_file_bounded(
    path: &Path,
    maximum_bytes: usize,
) -> std::io::Result<(String, usize)> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > maximum_bytes as u64 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("file exceeds supported {maximum_bytes}-byte bound"),
        ));
    }
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut observed_bytes = 0_usize;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        observed_bytes = observed_bytes.saturating_add(read);
        if observed_bytes > maximum_bytes {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("file exceeds supported {maximum_bytes}-byte bound"),
            ));
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    Ok((
        digest.iter().map(|byte| format!("{byte:02x}")).collect(),
        observed_bytes,
    ))
}

pub(crate) fn read_file_bounded_or(
    path: &Path,
    maximum_bytes: usize,
    maximum_oversized_bytes: usize,
    allow_oversized: impl FnOnce(&[u8]) -> bool,
) -> std::io::Result<(Vec<u8>, bool)> {
    if maximum_oversized_bytes < maximum_bytes {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "oversized file bound is smaller than the steady-state bound",
        ));
    }
    let mut file = fs::File::open(path)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(maximum_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    let oversized = bytes.len() > maximum_bytes;
    if oversized && !allow_oversized(&bytes) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("file exceeds supported {maximum_bytes}-byte bound"),
        ));
    }
    if oversized {
        let remaining_with_sentinel = maximum_oversized_bytes
            .saturating_sub(bytes.len())
            .saturating_add(1);
        (&mut file)
            .take(remaining_with_sentinel as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > maximum_oversized_bytes {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "file exceeds supported {maximum_oversized_bytes}-byte compatibility bound"
                ),
            ));
        }
    }
    sync_directory(
        path.parent()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "file has no parent directory"))?,
    )?;
    Ok((bytes, oversized))
}

fn create_directory_chain(path: &Path, private: bool) -> std::io::Result<()> {
    create_directory_chain_with_sync(path, private, sync_directory)
}

fn create_directory_chain_with_sync<F>(
    path: &Path,
    private: bool,
    mut sync: F,
) -> std::io::Result<()>
where
    F: FnMut(&Path) -> std::io::Result<()>,
{
    let mut missing = Vec::<PathBuf>::new();
    let mut ancestor = path;
    loop {
        match fs::metadata(ancestor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    "directory ancestor is not a directory",
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                missing.push(ancestor.to_path_buf());
                ancestor = ancestor.parent().ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        "directory has no existing ancestor",
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }
    for directory in missing.into_iter().rev() {
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error)
                if error.kind() == ErrorKind::AlreadyExists
                    && fs::metadata(&directory).is_ok_and(|metadata| metadata.is_dir()) => {}
            Err(error) => return Err(error),
        }
        if private {
            set_private_directory_permissions(&directory)?;
        }
        sync(&directory)?;
        sync(
            directory
                .parent()
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "directory has no parent"))?,
        )?;
    }
    if private {
        set_private_directory_permissions(path)?;
    }
    sync_directory_lineage(path, sync)
}

fn prepare_best_effort_private_directory_with_sync<F>(
    path: &Path,
    mut sync: F,
) -> std::io::Result<()>
where
    F: FnMut(&Path) -> std::io::Result<()>,
{
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            set_private_directory_permissions(path)?;
            sync(path)
        }
        Ok(_) => Err(Error::new(
            ErrorKind::NotADirectory,
            "best-effort evidence root is not a directory",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            create_directory_chain_with_sync(path, true, sync)
        }
        Err(error) => Err(error),
    }
}

fn sync_directory_lineage<F>(path: &Path, mut sync: F) -> std::io::Result<()>
where
    F: FnMut(&Path) -> std::io::Result<()>,
{
    for directory in path.ancestors().filter(|path| !path.as_os_str().is_empty()) {
        let metadata = fs::metadata(directory)?;
        if !metadata.is_dir() {
            return Err(Error::new(
                ErrorKind::NotADirectory,
                "directory lineage entry is not a directory",
            ));
        }
        sync(directory)?;
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> std::io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(not(unix))]
pub(crate) fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_resynchronizes_a_visible_directory_after_parent_sync_failure() {
        let temporary = tempfile::tempdir().expect("create durable directory test root");
        let first_directory = temporary.path().join("first");
        let target = first_directory.join("target");
        let mut failed_parent_sync = false;
        let first = create_directory_chain_with_sync(&target, false, |directory| {
            if directory == temporary.path() && !failed_parent_sync {
                failed_parent_sync = true;
                return Err(Error::other("injected parent sync failure"));
            }
            Ok(())
        });
        assert!(first.is_err());
        assert!(first_directory.is_dir());
        assert!(!target.exists());

        let mut retry_syncs = Vec::new();
        create_directory_chain_with_sync(&target, false, |directory| {
            retry_syncs.push(directory.to_path_buf());
            Ok(())
        })
        .expect("retry should republish the complete visible lineage");

        assert!(target.is_dir());
        assert!(retry_syncs.iter().any(|path| path == &first_directory));
        assert!(retry_syncs.iter().any(|path| path == temporary.path()));
    }

    #[test]
    fn best_effort_existing_directory_sync_cost_is_independent_of_lineage_depth() {
        let temporary = tempfile::tempdir().expect("create best-effort directory test root");
        let target = (0..64).fold(temporary.path().to_path_buf(), |path, index| {
            path.join(format!("depth-{index}"))
        });
        fs::create_dir_all(&target).expect("create deep existing directory lineage");

        let mut syncs = Vec::new();
        prepare_best_effort_private_directory_with_sync(&target, |directory| {
            syncs.push(directory.to_path_buf());
            Ok(())
        })
        .expect("prepare existing best-effort directory");

        assert_eq!(syncs, vec![target]);
    }

    #[test]
    fn bounded_read_rejects_oversized_files_without_loading_the_tail() {
        let temporary = tempfile::tempdir().expect("create bounded-read root");
        let path = temporary.path().join("bounded");
        fs::write(&path, vec![b'x'; 33]).expect("write oversized fixture");
        let error = read_file_bounded(&path, 32).expect_err("oversized file must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn bounded_streaming_digest_matches_the_in_memory_identity() {
        let temporary = tempfile::tempdir().expect("create bounded-digest root");
        let path = temporary.path().join("executable");
        let bytes = vec![b'x'; 96 * 1024 + 17];
        fs::write(&path, &bytes).expect("write digest fixture");
        let (digest, observed_bytes) =
            sha256_file_bounded(&path, bytes.len()).expect("digest boundary fixture");
        assert_eq!(digest, crate::encoding::sha256_hex(&bytes));
        assert_eq!(observed_bytes, bytes.len());
        assert_eq!(
            sha256_file_bounded(&path, bytes.len() - 1)
                .expect_err("oversized digest input must fail")
                .kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn compatibility_read_stops_at_its_distinct_upper_bound() {
        let temporary = tempfile::tempdir().expect("create compatibility-read root");
        let path = temporary.path().join("bounded-compatibility");
        fs::write(&path, vec![b'x'; 65]).expect("write oversized compatibility fixture");
        let error = read_file_bounded_or(&path, 32, 64, |_| true)
            .expect_err("compatibility read above its upper bound must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidData);

        fs::write(&path, vec![b'x'; 64]).expect("write boundary compatibility fixture");
        let (bytes, compatibility) = read_file_bounded_or(&path, 32, 64, |_| true)
            .expect("compatibility read at the upper bound");
        assert_eq!(bytes.len(), 64);
        assert!(compatibility);
    }
}
