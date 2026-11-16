// declared_role: orchestration, parser, formatter, accessor, mapper
#![allow(unused_imports)]

use super::*;

pub const SECRET_TOKEN: &str = "opencode_contract_secret_token_must_not_echo";

pub const UPDATE_SECRET_TOKEN: &str = "opencode_contract_update_secret_token_must_not_echo";

pub const SETUP_AUTH_SENTINEL: &str = "SETUP_AUTH_SENTINEL_DO_NOT_LEAK";

pub const OPENCODE_VERSION_SENTINEL: &str = "opencode 0.0.0-contract";

pub const CHATGPT_USAGE_READY_SENTINEL: &str = "contract_chatgpt_usage_ready";

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

pub struct HostRoots {
    pub root: PathBuf,
    pub config_root: PathBuf,
    pub data_root: PathBuf,
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
            "data_root": self.data_root.to_string_lossy()
        })
    }

    pub fn config_root(&self) -> &Path {
        &self.config_root
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }
}

fn host_roots(prefix: &str) -> HostRoots {
    let root = unique_temp_dir(prefix);
    let config_root = root.join("config");
    let data_root = root.join("data");
    HostRoots {
        root,
        config_root,
        data_root,
    }
}

fn create_host_root_dirs(roots: &HostRoots) {
    create_host_config_root(roots.config_root());
    create_host_data_root(roots.data_root());
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

    pub fn write_all_codex_auths(&self) {
        for relative in codex_auth_relatives() {
            write_codex_auth(&self.path.join(relative));
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

pub fn codex_auth_relatives() -> [&'static str; 5] {
    [
        ".codex/auth.json",
        ".codex5/auth.json",
        ".codex2/auth.json",
        ".codex3/auth.json",
        ".codex4/auth.json",
    ]
}

pub fn write_codex_auth(path: &Path) {
    fs::create_dir_all(path.parent().expect("auth parent")).expect("create auth parent");
    fs::write(path, codex_auth_fixture()).expect("write auth fixture");
}

pub fn codex_auth_fixture() -> String {
    format!(
        "{{\"tokens\":{{\"access_token\":\"{SETUP_AUTH_SENTINEL}\",\"account_id\":\"acct\"}}}}\n"
    )
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
    write_executable(&dir.join("chatgpt-usage"), fake_chatgpt_usage_script());
    for wrapper in opencode_wrappers() {
        write_executable(&dir.join(wrapper), fake_wrapper_script());
    }
}

pub fn fake_opencode_binary_script() -> &'static str {
    "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'opencode 0.0.0-contract\\n'; exit 0; fi\nprintf 'fake opencode\\n'\nexit 0\n"
}

pub fn fake_chatgpt_usage_script() -> &'static str {
    "#!/bin/sh\nprintf '{\"contract_chatgpt_usage_ready\":true,\"windows\":[]}\\n'\nexit 0\n"
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

pub fn write_executable_metadata_error(path: &Path, err: &std::io::Error) -> String {
    format!("metadata {}: {err}", path.display())
}

pub fn write_executable_chmod_error(path: &Path, err: &std::io::Error) -> String {
    format!("chmod {}: {err}", path.display())
}

pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(unique_temp_dir_name(prefix))
}

pub fn unique_temp_dir_name(prefix: &str) -> String {
    formatted_temp_dir_name(prefix, current_time_nanos(), current_process_id())
}

pub fn current_time_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos()
}

pub fn current_process_id() -> u32 {
    std::process::id()
}

pub fn formatted_temp_dir_name(prefix: &str, nanos: u128, process_id: u32) -> String {
    format!("{prefix}-{process_id}-{nanos}")
}

pub fn prepend_path(dir: &Path) -> String {
    joined_path_string(prepended_path_entries(dir))
}

pub fn prepended_path_entries(dir: &Path) -> Vec<PathBuf> {
    vec![dir.to_path_buf()]
}

pub fn joined_path_string(paths: Vec<PathBuf>) -> String {
    std::env::join_paths(paths)
        .expect("join PATH entries")
        .to_string_lossy()
        .into_owned()
}

pub fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
