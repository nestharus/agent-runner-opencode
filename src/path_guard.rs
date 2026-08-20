//! Declared roles: validator, mapper
//! intrinsic_surface_declarations:
//!   - component: src/path_guard.rs
//!     role: intrinsic-surface
//!     Domain: provider-owned filesystem target confinement
//!     Owns:
//!       - lexical root-relative target admission
//!       - existing-ancestor canonicalization for not-yet-created targets
//!       - pre-side-effect canonical containment decisions

use std::fs;
use std::io::{Error, ErrorKind};
use std::path::{Component, Path, PathBuf};

/// Validate a provider-owned target before any directory or file side effect.
/// The host root must already exist. Targets may contain a not-yet-created
/// suffix, but every suffix component must be normal and the nearest existing
/// ancestor must canonically remain inside the canonical host root. The
/// original target is returned after admission so callers preserve their
/// configured path spelling.
pub fn confined_target(root: &Path, target: &Path) -> std::io::Result<PathBuf> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| confinement_error("target is not lexically rooted under the host root"))?;
    validate_suffix(relative)?;
    let canonical_root = fs::canonicalize(root)?;
    let existing = target
        .ancestors()
        .find(|ancestor| ancestor.exists())
        .ok_or_else(|| confinement_error("target has no existing ancestor"))?;
    let suffix = target
        .strip_prefix(existing)
        .map_err(|_| confinement_error("target suffix cannot be resolved"))?;
    validate_suffix(suffix)?;
    let mut canonical_target = fs::canonicalize(existing)?;
    append_suffix(&mut canonical_target, suffix);
    if !canonical_target.starts_with(&canonical_root) {
        return Err(confinement_error("target escapes the canonical host root"));
    }
    Ok(target.to_path_buf())
}

fn validate_suffix(suffix: &Path) -> std::io::Result<()> {
    if suffix.components().all(pushable_component) {
        return Ok(());
    }
    Err(confinement_error(
        "target suffix contains an escaping or rooted component",
    ))
}

fn pushable_component(component: Component<'_>) -> bool {
    matches!(component, Component::Normal(_) | Component::CurDir)
}

fn append_suffix(target: &mut PathBuf, suffix: &Path) {
    for component in suffix.components() {
        if let Component::Normal(part) = component {
            target.push(part);
        }
    }
}

fn confinement_error(message: &str) -> Error {
    Error::new(ErrorKind::PermissionDenied, message)
}

#[cfg(test)]
mod tests {
    use super::confined_target;
    use std::fs;

    #[test]
    fn admits_a_safe_not_yet_created_target() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("root");
        fs::create_dir(&root).expect("host root");
        let target = root.join("new").join("nested").join("record.json");

        assert_eq!(
            confined_target(&root, &target).expect("safe target"),
            target
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlinked_existing_ancestor_that_escapes_the_root() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("root");
        let outside = directory.path().join("outside");
        fs::create_dir(&root).expect("host root");
        fs::create_dir(&outside).expect("outside directory");
        symlink(&outside, root.join("escaped")).expect("escaping symlink");
        let target = root.join("escaped").join("record.json");

        let error = confined_target(&root, &target).expect_err("escaping target must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }
}
