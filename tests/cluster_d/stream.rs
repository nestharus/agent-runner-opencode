// declared_role: parser, filter, mapper, accessor, validator, predicate, formatter
#![allow(unused_imports)]

use super::*;

pub fn forbidden_live_route_paths(
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
) -> BTreeSet<PathBuf> {
    clone_paths(filter_forbidden_live_route_paths(merged_tree_keys(
        before, after,
    )))
}

pub fn merged_tree_keys<'a>(
    before: &'a BTreeMap<PathBuf, String>,
    after: &'a BTreeMap<PathBuf, String>,
) -> Vec<&'a PathBuf> {
    before.keys().chain(after.keys()).collect()
}

pub fn filter_forbidden_live_route_paths(paths: Vec<&PathBuf>) -> Vec<&PathBuf> {
    paths
        .into_iter()
        .filter(|path| is_forbidden_live_route_path(path))
        .collect()
}

pub fn clone_paths(paths: Vec<&PathBuf>) -> BTreeSet<PathBuf> {
    paths.into_iter().cloned().collect()
}

pub fn changed_tree_paths(
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
) -> BTreeSet<PathBuf> {
    clone_paths(filter_changed_tree_paths(
        merged_tree_keys(before, after),
        before,
        after,
    ))
}

pub fn filter_changed_tree_paths<'a>(
    paths: Vec<&'a PathBuf>,
    before: &BTreeMap<PathBuf, String>,
    after: &BTreeMap<PathBuf, String>,
) -> Vec<&'a PathBuf> {
    paths
        .into_iter()
        .filter(|path| before.get(*path) != after.get(*path))
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
    for path in directory_paths(current) {
        collect_tree_path_hash(root, &path, files);
    }
}

pub fn collect_tree_path_hash(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, String>) {
    if path.is_dir() {
        collect_tree_hashes(root, path, files);
    } else {
        insert_tree_file_hash(root, path, files);
    }
}

pub fn insert_tree_file_hash(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, String>) {
    files.insert(relative_tree_path(root, path), file_sha256(path));
}

pub fn directory_paths(current: &Path) -> Vec<PathBuf> {
    read_directory(current)
        .map(|entry| directory_entry_path(current, entry))
        .collect()
}

pub fn read_directory(current: &Path) -> fs::ReadDir {
    fs::read_dir(current).unwrap_or_else(|err| panic!("read_dir {}: {err}", current.display()))
}

pub fn directory_entry_path(current: &Path, entry: std::io::Result<fs::DirEntry>) -> PathBuf {
    entry
        .unwrap_or_else(|err| panic!("read_dir entry {}: {err}", current.display()))
        .path()
}

pub fn relative_tree_path(root: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(root)
        .unwrap_or_else(|err| panic!("strip prefix {}: {err}", path.display()))
        .to_path_buf()
}

pub fn file_hashes<'a>(paths: impl IntoIterator<Item = &'a Path>) -> BTreeMap<PathBuf, String> {
    paths
        .into_iter()
        .map(|path| (path.to_path_buf(), file_sha256(path)))
        .collect()
}

pub fn file_sha256(path: &Path) -> String {
    sha256_hex(&file_bytes(path))
}

pub fn file_bytes(path: &Path) -> Vec<u8> {
    fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
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
