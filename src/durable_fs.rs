//! Declared roles: orchestration, validator
//! intrinsic_surface_declarations:
//!   - component: src/durable_fs.rs
//!     role: intrinsic-surface
//!     Domain: durable provider-owned directory publication
//!     Owns:
//!       - ordered creation and parent synchronization of directory links
//!       - private directory permission persistence

use std::fs;
use std::io::{Error, ErrorKind, Read};
use std::path::{Path, PathBuf};

pub(crate) fn create_directories(path: &Path) -> std::io::Result<()> {
    create_directory_chain(path, false)
}

pub(crate) fn create_private_directories(path: &Path) -> std::io::Result<()> {
    create_directory_chain(path, true)
}

pub(crate) fn read_file(path: &Path) -> std::io::Result<Vec<u8>> {
    let bytes = fs::read(path)?;
    sync_directory(
        path.parent()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "file has no parent directory"))?,
    )?;
    Ok(bytes)
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

#[cfg(test)]
pub(crate) fn read_file_bounded(path: &Path, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    read_file_bounded_or(path, maximum_bytes, |_| false).map(|(bytes, _)| bytes)
}

pub(crate) fn read_file_bounded_or(
    path: &Path,
    maximum_bytes: usize,
    allow_oversized: impl FnOnce(&[u8]) -> bool,
) -> std::io::Result<(Vec<u8>, bool)> {
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
        file.read_to_end(&mut bytes)?;
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
    fn bounded_read_rejects_oversized_files_without_loading_the_tail() {
        let temporary = tempfile::tempdir().expect("create bounded-read root");
        let path = temporary.path().join("bounded");
        fs::write(&path, vec![b'x'; 33]).expect("write oversized fixture");
        let error = read_file_bounded(&path, 32).expect_err("oversized file must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidData);
    }
}
