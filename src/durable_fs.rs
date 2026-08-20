//! Declared roles: orchestration, validator
//! intrinsic_surface_declarations:
//!   - component: src/durable_fs.rs
//!     role: intrinsic-surface
//!     Domain: durable provider-owned directory publication
//!     Owns:
//!       - ordered creation and parent synchronization of directory links
//!       - private directory permission persistence

use std::fs;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

pub(crate) fn create_directories(path: &Path) -> std::io::Result<()> {
    create_directory_chain(path, false)
}

pub(crate) fn create_private_directories(path: &Path) -> std::io::Result<()> {
    create_directory_chain(path, true)
}

fn create_directory_chain(path: &Path, private: bool) -> std::io::Result<()> {
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
        sync_directory(&directory)?;
        sync_directory(
            directory
                .parent()
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "directory has no parent"))?,
        )?;
    }
    if private {
        set_private_directory_permissions(path)?;
        sync_directory(path)?;
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
