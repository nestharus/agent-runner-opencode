//! Static machine configuration for the installed OpenCode provider binary.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const CONFIG_FILE_NAME: &str = "config.toml";
const OPENCODE_BIN_ENV: &str = "AGENT_RUNNER_OPENCODE_BIN";
const AGENT_BASH_BIN_ENV: &str = "AGENT_BASH_BIN";
const AGENT_RUNNER_BIN_ENV: &str = "AGENT_BASH_AGENT_RUNNER_BIN";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BinaryConfig {
    opencode_bin: PathBuf,
    agent_bash_bin: PathBuf,
    agent_runner_bin: PathBuf,
}

pub(crate) fn load() -> Result<Option<BinaryConfig>, String> {
    let executable = std::env::current_exe()
        .map_err(|error| format!("could not resolve the current executable: {error}"))?;
    load_for_executable(&executable)
}

pub(crate) fn program_override(program: &str) -> Option<PathBuf> {
    if program != "opencode" {
        return None;
    }
    match load() {
        Ok(Some(config)) => Some(config.opencode_bin),
        Ok(None) => std::env::var_os(OPENCODE_BIN_ENV)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from),
        Err(_) => None,
    }
}

pub(crate) fn apply_tool_paths(environment: &mut BTreeMap<String, String>) {
    let Ok(Some(config)) = load() else {
        return;
    };
    environment.insert(
        AGENT_BASH_BIN_ENV.to_string(),
        config.agent_bash_bin.to_string_lossy().into_owned(),
    );
    environment.insert(
        AGENT_RUNNER_BIN_ENV.to_string(),
        config.agent_runner_bin.to_string_lossy().into_owned(),
    );
}

fn load_for_executable(executable: &Path) -> Result<Option<BinaryConfig>, String> {
    let executable = std::fs::canonicalize(executable).map_err(|error| {
        format!(
            "could not canonicalize executable {}: {error}",
            executable.display()
        )
    })?;
    let directory = executable.parent().ok_or_else(|| {
        format!(
            "could not resolve the directory containing executable {}",
            executable.display()
        )
    })?;
    let path = directory.join(CONFIG_FILE_NAME);
    match path.try_exists() {
        Ok(false) => Ok(None),
        Ok(true) => read_config(&path).map(Some),
        Err(error) => Err(format!(
            "could not inspect OpenCode provider config {}: {error}",
            path.display()
        )),
    }
}

fn read_config(path: &Path) -> Result<BinaryConfig, String> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        format!(
            "could not read OpenCode provider config {}: {error}",
            path.display()
        )
    })?;
    let config: BinaryConfig = toml::from_str(&text).map_err(|error| {
        format!(
            "could not parse OpenCode provider config {}: {error}",
            path.display()
        )
    })?;
    require_absolute(path, "opencode_bin", &config.opencode_bin)?;
    require_absolute(path, "agent_bash_bin", &config.agent_bash_bin)?;
    require_absolute(path, "agent_runner_bin", &config.agent_runner_bin)?;
    Ok(config)
}

fn require_absolute(source: &Path, field: &str, value: &Path) -> Result<(), String> {
    if value.is_absolute() {
        return Ok(());
    }
    Err(format!(
        "OpenCode provider config {} field {field} must be an absolute path",
        source.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjacent_config_is_loaded_and_validated() {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("agent-runner-opencode");
        std::fs::write(&executable, "fixture").unwrap();
        let opencode_bin = directory.path().join("opencode");
        let agent_bash_bin = directory.path().join("agent-bash");
        let agent_runner_bin = directory.path().join("agents");
        std::fs::write(
            directory.path().join(CONFIG_FILE_NAME),
            format!(
                "opencode_bin = {:?}\nagent_bash_bin = {:?}\nagent_runner_bin = {:?}\n",
                opencode_bin.display().to_string(),
                agent_bash_bin.display().to_string(),
                agent_runner_bin.display().to_string()
            ),
        )
        .unwrap();

        assert_eq!(
            load_for_executable(&executable).unwrap(),
            Some(BinaryConfig {
                opencode_bin,
                agent_bash_bin,
                agent_runner_bin,
            })
        );
    }

    #[test]
    fn absent_adjacent_config_selects_environment_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("agent-runner-opencode");
        std::fs::write(&executable, "fixture").unwrap();

        assert_eq!(load_for_executable(&executable).unwrap(), None);
    }

    #[test]
    fn invalid_adjacent_config_does_not_fall_back() {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("agent-runner-opencode");
        std::fs::write(&executable, "fixture").unwrap();
        std::fs::write(directory.path().join(CONFIG_FILE_NAME), "opencode_bin = [").unwrap();

        assert!(load_for_executable(&executable).is_err());
    }
}
