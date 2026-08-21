// declared_role: orchestration, parser, formatter, accessor, mapper
#![allow(unused_imports)]

use super::*;

pub const SECRET_TOKEN: &str = "opencode_contract_secret_token_must_not_echo";

pub const UPDATE_SECRET_TOKEN: &str = "opencode_contract_update_secret_token_must_not_echo";

pub const SETUP_AUTH_SENTINEL: &str = "SETUP_AUTH_SENTINEL_DO_NOT_LEAK";

pub const ROTATION_SOURCE_SESSION: &str = "ses_source_contract_d";

pub const PROVIDERS_TOML: &str = r#"
[opencode]
command = "opencode1"
args = ["run", "--dangerously-skip-permissions"]
quota_script = "chatgpt-usage ~/.codex/auth.json"
refresh_auth_command = "/bin/false"

[opencode2]
command = "opencode2"
args = ["run", "--dangerously-skip-permissions"]
quota_script = "chatgpt-usage ~/.codex5/auth.json"
refresh_auth_command = "/bin/false"

[opencode3]
command = "opencode3"
args = ["run", "--dangerously-skip-permissions"]

[opencode4]
command = "opencode4"
args = ["run", "--dangerously-skip-permissions"]

[opencode5]
command = "opencode5"
args = ["run", "--dangerously-skip-permissions"]
"#;

pub const MODEL_TOML: &str = r#"
name = "gpt-high"
provider = "opencode"
model = "openai/gpt-5.6-sol"
args = ["--variant", "high"]
"#;

pub fn settings_create_id(create: &Value) -> String {
    create["record"]["id"]
        .as_str()
        .expect("created id")
        .to_owned()
}

pub fn settings_create_version(create: &Value) -> String {
    create["record"]["version"]
        .as_str()
        .expect("created version")
        .to_owned()
}

pub fn settings_update_version(update_response: &Value) -> String {
    update_response["result"]["record"]["version"]
        .as_str()
        .expect("updated version")
        .to_owned()
}

pub fn legacy_fixture() -> Value {
    json!({
        "providers_toml": PROVIDERS_TOML,
        "models": {
            "gpt-high.toml": MODEL_TOML
        }
    })
}

pub struct LiveConfigFixture {
    pub config_root: PathBuf,
    pub provider_artifact_root: PathBuf,
}

impl LiveConfigFixture {
    pub fn new(host_config_root: &Path) -> Self {
        let fixture = live_config_fixture(host_config_root);
        setup_live_config_fixture(&fixture);
        fixture
    }

    pub fn config_root(&self) -> &Path {
        &self.config_root
    }

    pub fn provider_artifact_root(&self) -> &Path {
        &self.provider_artifact_root
    }

    pub fn write_live_routes(&self) {
        let model_dir = live_model_dir(&self.config_root);
        create_live_model_dir(&model_dir);
        write_live_route_sentinels(&self.config_root, &model_dir);
    }
}

fn live_config_fixture(host_config_root: &Path) -> LiveConfigFixture {
    LiveConfigFixture {
        config_root: host_config_root.join("live-config"),
        provider_artifact_root: host_config_root.join("provider-owned-migration-artifacts"),
    }
}

fn setup_live_config_fixture(fixture: &LiveConfigFixture) {
    fixture.write_live_routes();
}

fn live_model_dir(config_root: &Path) -> PathBuf {
    config_root.join("models")
}

fn create_live_model_dir(model_dir: &Path) {
    fs::create_dir_all(model_dir).expect("create live model sentinel dir");
}

fn write_live_route_sentinels(config_root: &Path, model_dir: &Path) {
    write_live_route(&config_root.join("providers.toml"), PROVIDERS_TOML);
    write_live_route(&model_dir.join("gpt-high.toml"), MODEL_TOML);
    write_live_route(&model_dir.join("gpt-medium.toml"), MODEL_TOML);
    write_live_route(&config_root.join("gpt-low.toml"), MODEL_TOML);
    write_live_route(&config_root.join("gpt-xhigh.toml"), MODEL_TOML);
}

pub fn write_live_route(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write live route sentinel");
}

pub struct RotationOpencodeFixture {
    root: PathBuf,
    import_record: PathBuf,
    import_cwd_record: PathBuf,
    import_count_record: PathBuf,
    #[cfg_attr(not(unix), allow(dead_code))]
    finalization_fault_marker: Option<PathBuf>,
}

impl RotationOpencodeFixture {
    pub fn new() -> Self {
        Self::configured(false, None, false, 0)
    }

    #[cfg_attr(not(unix), allow(dead_code))]
    pub fn with_post_import_finalization_fault() -> Self {
        Self::configured(true, None, false, 0)
    }

    #[cfg_attr(not(unix), allow(dead_code))]
    pub fn with_post_import_finalization_fault_and_target_id(target_session_id: &str) -> Self {
        Self::configured(true, Some(target_session_id), false, 0)
    }

    pub fn with_hanging_import() -> Self {
        Self::configured(false, None, true, 0)
    }

    pub fn with_oversized_export() -> Self {
        Self::configured(false, None, false, 16 * 1024 * 1024)
    }

    pub fn with_large_export() -> Self {
        Self::configured(false, None, false, 15 * 1024 * 1024)
    }

    fn configured(
        inject_fault: bool,
        target_session_id: Option<&str>,
        hang_import: bool,
        export_payload_bytes: usize,
    ) -> Self {
        let root = unique_temp_dir("agent-runner-opencode-rotation-native");
        fs::create_dir_all(&root).expect("create rotation native fixture");
        let import_record = root.join("imported-session.json");
        let import_cwd_record = root.join("imported-session.cwd");
        let import_count_record = root.join("imported-session.count");
        let finalization_fault_marker =
            inject_fault.then(|| root.join("fail-post-import-finalization"));
        if let Some(marker) = &finalization_fault_marker {
            fs::write(marker, b"armed\n").expect("arm post-import finalization fault");
        }
        write_executable(
            &root.join("opencode1"),
            &rotation_source_script(export_payload_bytes),
        );
        write_executable(
            &root.join("opencode2"),
            &rotation_target_script(
                &import_record,
                &import_cwd_record,
                &import_count_record,
                finalization_fault_marker.as_deref(),
                target_session_id,
                hang_import,
            ),
        );
        Self {
            root,
            import_record,
            import_cwd_record,
            import_count_record,
            finalization_fault_marker,
        }
    }

    pub fn path_env(&self) -> String {
        prepend_path(&self.root)
    }

    pub fn imported_session(&self) -> Value {
        serde_json::from_slice(
            &fs::read(&self.import_record).expect("target wrapper should record imported session"),
        )
        .expect("recorded imported session JSON")
    }

    pub fn imported_cwd(&self) -> PathBuf {
        PathBuf::from(
            fs::read_to_string(&self.import_cwd_record)
                .expect("target wrapper should record its working directory"),
        )
    }

    pub fn import_count(&self) -> u64 {
        fs::read_to_string(&self.import_count_record)
            .expect("target wrapper should record import count")
            .parse()
            .expect("recorded import count")
    }

    pub fn import_was_attempted(&self) -> bool {
        self.import_record.exists() || self.import_count_record.exists()
    }

    #[cfg_attr(not(unix), allow(dead_code))]
    pub fn restore_operation_state_writes(&self, data_root: &Path) {
        let operation_root = data_root.join("provider-state/opencode/rotation/operations");
        fs::remove_file(&operation_root).expect("remove blocked operation-state path");
        fs::rename(
            operation_root.with_file_name("operations-blocked"),
            &operation_root,
        )
        .expect("restore prepared operation-state directory");
        assert!(
            self.finalization_fault_marker
                .as_ref()
                .is_some_and(|marker| !marker.exists()),
            "target import should consume the armed finalization fault"
        );
    }
}

impl Drop for RotationOpencodeFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn rotation_source_script(export_payload_bytes: usize) -> String {
    r#"#!/usr/bin/python3
import json
import sys

if len(sys.argv) != 3 or sys.argv[1] != "export":
    raise SystemExit(64)
session_id = sys.argv[2]
native = {
    "info": {
        "id": session_id,
        "title": "Rotation source",
        "projectID": "project_rotation_native",
        "directory": "/workspace/rotation-native"
    },
    "messages": [{
        "info": {
            "id": "msg_rotation_source",
            "role": "user",
            "sessionID": session_id,
            "parentID": "msg_rotation_parent",
            "time": {"created": 1782864000000, "updated": 1782864005000}
        },
        "parts": [{"type": "text", "text": "rotation source turn", "synthetic": False}],
        "mode": "build"
    }],
    "nativeRoot": {"preserved": True}
}
if __EXPORT_PAYLOAD_BYTES__:
    native["nativeRoot"]["payload"] = "x" * __EXPORT_PAYLOAD_BYTES__
print("Exporting session: " + session_id)
print(json.dumps(native))
"#
    .replace(
        "__EXPORT_PAYLOAD_BYTES__",
        &export_payload_bytes.to_string(),
    )
}

fn rotation_target_script(
    import_record: &Path,
    import_cwd_record: &Path,
    import_count_record: &Path,
    finalization_fault_marker: Option<&Path>,
    target_session_id: Option<&str>,
    hang_import: bool,
) -> String {
    let fault_marker = finalization_fault_marker
        .map(path_string)
        .unwrap_or_default();
    format!(
        r#"#!/usr/bin/python3
import json
import pathlib
import sys
import time

if len(sys.argv) != 3:
    raise SystemExit(64)
if sys.argv[1] == "export":
    if not pathlib.Path({record}).exists():
        raise SystemExit(2)
    native = json.loads(pathlib.Path({record}).read_text())
    if native["info"]["id"] != sys.argv[2]:
        raise SystemExit(2)
    print(json.dumps(native, separators=(",", ":")))
    raise SystemExit(0)
if sys.argv[1] != "import":
    raise SystemExit(64)
if {hang_import}:
    time.sleep(30)
native = json.loads(pathlib.Path(sys.argv[2]).read_text())
target_session_id = {target_session_id}
if target_session_id:
    native["info"]["id"] = target_session_id
    for message in native.get("messages", []):
        message.get("info", {{}})["sessionID"] = target_session_id
pathlib.Path({record}).write_text(json.dumps(native, separators=(",", ":")))
pathlib.Path({cwd_record}).write_text(str(pathlib.Path.cwd()))
count_path = pathlib.Path({count_record})
count = int(count_path.read_text()) if count_path.exists() else 0
count_path.write_text(str(count + 1))
fault_marker = pathlib.Path({fault_marker}) if {fault_enabled} else None
if fault_marker is not None and fault_marker.exists():
    operation_root = pathlib.Path(sys.argv[2]).parents[3] / "provider-state" / "opencode" / "rotation" / "operations"
    operation_root.rename(operation_root.with_name("operations-blocked"))
    operation_root.write_text("blocked\n")
    fault_marker.unlink()
print("Imported session: " + native["info"]["id"])
"#,
        record = serde_json::to_string(&path_string(import_record)).expect("record path JSON"),
        cwd_record =
            serde_json::to_string(&path_string(import_cwd_record)).expect("cwd record path JSON"),
        count_record = serde_json::to_string(&path_string(import_count_record))
            .expect("count record path JSON"),
        fault_marker = serde_json::to_string(&fault_marker).expect("fault marker path JSON"),
        fault_enabled = if finalization_fault_marker.is_some() {
            "True"
        } else {
            "False"
        },
        target_session_id =
            serde_json::to_string(target_session_id.unwrap_or("")).expect("target session id JSON"),
        hang_import = if hang_import { "True" } else { "False" },
    )
}

pub struct HostRoots {
    pub root: PathBuf,
    pub config_root: PathBuf,
    pub data_root: PathBuf,
    pub working_directory: PathBuf,
}

impl HostRoots {
    pub fn new(prefix: &str) -> Self {
        let roots = host_roots(prefix);
        create_host_root_dirs(&roots);
        roots
    }

    pub fn overrides(&self) -> Value {
        json!({
            "config_root": self.config_root.to_string_lossy(),
            "data_root": self.data_root.to_string_lossy(),
            "working_directory": self.working_directory.to_string_lossy()
        })
    }

    pub fn config_root(&self) -> &Path {
        &self.config_root
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    pub fn working_directory(&self) -> &Path {
        &self.working_directory
    }
}

fn host_roots(prefix: &str) -> HostRoots {
    let root = unique_temp_dir(prefix);
    let config_root = root.join("config");
    let data_root = root.join("data");
    let working_directory = root.join("workspace");
    HostRoots {
        root,
        config_root,
        data_root,
        working_directory,
    }
}

fn create_host_root_dirs(roots: &HostRoots) {
    create_host_config_root(roots.config_root());
    create_host_data_root(roots.data_root());
    fs::create_dir_all(roots.working_directory()).expect("create temp working_directory");
}

fn create_host_config_root(config_root: &Path) {
    fs::create_dir_all(config_root).expect("create temp config_root");
}

fn create_host_data_root(data_root: &Path) {
    fs::create_dir_all(data_root).expect("create temp data_root");
}

impl Drop for HostRoots {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub struct HomeFixture {
    pub path: PathBuf,
    pub path_string: String,
}

impl HomeFixture {
    pub fn new(prefix: &str) -> Self {
        let fixture = home_fixture(prefix);
        setup_home_fixture(&fixture);
        fixture
    }

    pub fn path_str(&self) -> &str {
        &self.path_string
    }

    pub fn write_all_opencode_auths(&self) {
        for relative in opencode_auth_relatives() {
            write_opencode_auth(&self.path.join(relative));
        }
    }
}

fn home_fixture(prefix: &str) -> HomeFixture {
    let path = unique_temp_dir(prefix);
    let path_string = path_string(&path);
    HomeFixture { path, path_string }
}

fn setup_home_fixture(fixture: &HomeFixture) {
    create_home_dir(&fixture.path);
}

fn create_home_dir(path: &Path) {
    fs::create_dir_all(path).expect("create temp HOME");
}

pub fn opencode_auth_relatives() -> [&'static str; 5] {
    [
        ".local/share/opencode/auth.json",
        ".opencode2/opencode/auth.json",
        ".opencode3/opencode/auth.json",
        ".opencode4/opencode/auth.json",
        ".opencode5/opencode/auth.json",
    ]
}

pub fn write_opencode_auth(path: &Path) {
    fs::create_dir_all(path.parent().expect("auth parent")).expect("create auth parent");
    fs::write(path, opencode_auth_fixture()).expect("write auth fixture");
}

pub fn opencode_auth_fixture() -> String {
    format!("{{\"openai\":{{\"access\":\"{SETUP_AUTH_SENTINEL}\",\"accountId\":\"acct\"}}}}\n")
}

impl Drop for HomeFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub struct FakeToolchain {
    pub dir: PathBuf,
}

impl FakeToolchain {
    pub fn new() -> Self {
        let fixture = fake_toolchain();
        setup_fake_toolchain(&fixture);
        fixture
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

fn fake_toolchain() -> FakeToolchain {
    FakeToolchain {
        dir: unique_temp_dir("agent-runner-opencode-setup-tools"),
    }
}

fn setup_fake_toolchain(fixture: &FakeToolchain) {
    create_fake_toolchain_dir(fixture.dir());
    write_fake_toolchain(fixture.dir());
}

fn create_fake_toolchain_dir(dir: &Path) {
    fs::create_dir_all(dir).expect("create fake toolchain dir");
}

pub fn write_fake_toolchain(dir: &Path) {
    write_executable(&dir.join("opencode"), fake_opencode_binary_script());
    write_executable(&dir.join("curl"), fake_curl_binary_script());
    for wrapper in opencode_wrappers() {
        write_executable(&dir.join(wrapper), fake_wrapper_script());
    }
}

pub fn fake_opencode_binary_script() -> &'static str {
    "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'opencode 0.0.0-contract\\n'; exit 0; fi\nprintf 'fake opencode\\n'\nexit 0\n"
}

pub fn fake_curl_binary_script() -> &'static str {
    "#!/bin/sh\nprintf 'curl 0.0.0-contract\\n'\nexit 0\n"
}

pub fn opencode_wrappers() -> [&'static str; 5] {
    [
        "opencode1",
        "opencode2",
        "opencode3",
        "opencode4",
        "opencode5",
    ]
}

pub fn fake_wrapper_script() -> &'static str {
    "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'wrapper contract\\n'; exit 0; fi\nexit 0\n"
}

impl Drop for FakeToolchain {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub fn write_executable(path: &Path, script: &str) {
    fs::write(path, script)
        .unwrap_or_else(|err| panic!("{}", write_executable_write_error(path, &err)));
    #[cfg(unix)]
    make_executable(path);
}

#[cfg(unix)]
pub fn make_executable(path: &Path) {
    let permissions = permissions_with_mode(path_permissions(path), 0o755);
    set_path_permissions(path, permissions);
}

#[cfg(unix)]
pub fn path_permissions(path: &Path) -> fs::Permissions {
    fs::metadata(path)
        .unwrap_or_else(|err| panic!("{}", write_executable_metadata_error(path, &err)))
        .permissions()
}

#[cfg(unix)]
pub fn permissions_with_mode(mut permissions: fs::Permissions, mode: u32) -> fs::Permissions {
    permissions.set_mode(mode);
    permissions
}

#[cfg(unix)]
pub fn set_path_permissions(path: &Path, permissions: fs::Permissions) {
    fs::set_permissions(path, permissions)
        .unwrap_or_else(|err| panic!("{}", write_executable_chmod_error(path, &err)));
}

pub fn write_executable_write_error(path: &Path, err: &std::io::Error) -> String {
    format!("write {}: {err}", path.display())
}

#[cfg(unix)]
pub fn write_executable_metadata_error(path: &Path, err: &std::io::Error) -> String {
    format!("metadata {}: {err}", path.display())
}

#[cfg(unix)]
pub fn write_executable_chmod_error(path: &Path, err: &std::io::Error) -> String {
    format!("chmod {}: {err}", path.display())
}

pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

pub fn prepend_path(dir: &Path) -> String {
    std::env::join_paths([dir])
        .expect("join PATH entries")
        .to_string_lossy()
        .into_owned()
}

pub fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
