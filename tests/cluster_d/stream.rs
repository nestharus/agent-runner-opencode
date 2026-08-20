// declared_role: parser, filter, mapper, accessor, validator, predicate, formatter
#![allow(unused_imports)]

use super::*;

pub fn forbidden_live_route_paths(
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
) -> BTreeSet<PathBuf> {
    before
        .keys()
        .chain(after.keys())
        .filter(|path| is_forbidden_live_route_path(path))
        .cloned()
        .collect()
}

pub fn changed_tree_paths(
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
) -> BTreeSet<PathBuf> {
    before
        .keys()
        .chain(after.keys())
        .filter(|path| before.get(*path) != after.get(*path))
        .cloned()
        .collect()
}

pub fn is_forbidden_live_route_path(path: &Path) -> bool {
    let file_name = path.file_name().and_then(|name| name.to_str());
    file_name == Some("providers.toml")
        || file_name.is_some_and(|name| name.starts_with("gpt-") && name.ends_with(".toml"))
        || path
            .components()
            .any(|component| component.as_os_str().to_str() == Some("models"))
}

pub fn snapshot_tree(root: &Path) -> BTreeMap<PathBuf, String> {
    let mut files = BTreeMap::new();
    collect_tree_hashes(root, root, &mut files);
    files
}

pub fn collect_tree_hashes(root: &Path, current: &Path, files: &mut BTreeMap<PathBuf, String>) {
    let entries =
        fs::read_dir(current).unwrap_or_else(|err| panic!("read_dir {}: {err}", current.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|err| panic!("read_dir entry {}: {err}", current.display()))
            .path();
        if path.is_dir() {
            collect_tree_hashes(root, &path, files);
        } else {
            let relative = path
                .strip_prefix(root)
                .unwrap_or_else(|err| panic!("strip prefix {}: {err}", path.display()))
                .to_path_buf();
            files.insert(relative, file_sha256(&path));
        }
    }
}

pub fn file_hashes<'a>(paths: impl IntoIterator<Item = &'a Path>) -> BTreeMap<PathBuf, String> {
    paths
        .into_iter()
        .map(|path| (path.to_path_buf(), file_sha256(path)))
        .collect()
}

pub fn file_sha256(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    sha256_hex(&bytes)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn json_contains_string(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(value) => value.contains(needle),
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, needle)),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains(needle) || json_contains_string(value, needle)),
        _ => false,
    }
}
