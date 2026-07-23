//! Declared roles: orchestration, mapper, validator, accessor

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const ENV_PASSTHROUGH_KEYS: &[&str] = &["PATH", "HOME", "AGENT_RUNNER_OPENCODE_QUOTA_SCRIPT_LOG"];

#[derive(Debug)]
pub struct ShellOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub status: i32,
}

pub fn run(argv: &[String]) -> io::Result<ShellOutput> {
    let (program, args) = validate_argv(argv)?;
    let output = command(program).args(args).output()?;
    Ok(shell_output(output))
}

pub fn command(program: &str) -> Command {
    let mut command = Command::new(resolved_program(program));
    command.env_clear();
    command.envs(env_passthrough_pairs());
    command
}

fn validate_argv(argv: &[String]) -> io::Result<(&String, &[String])> {
    argv.split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "argv must not be empty"))
}

fn shell_output(output: Output) -> ShellOutput {
    ShellOutput {
        stdout: output.stdout,
        stderr: output.stderr,
        status: output.status.code().unwrap_or(1),
    }
}

fn resolved_program(program: &str) -> PathBuf {
    let path = Path::new(program);
    if program_has_path_component(path) {
        return path.to_path_buf();
    }
    find_on_path(program).unwrap_or_else(|| PathBuf::from(program))
}

fn program_has_path_component(path: &Path) -> bool {
    path.is_absolute() || path.components().count() > 1
}

fn find_on_path(program: &str) -> Option<PathBuf> {
    let path = path_env()?;
    existing_path_candidate(path_candidates(path_entries(&path), program))
}

fn path_env() -> Option<OsString> {
    std::env::var_os("PATH")
}

fn path_entries(path: &OsString) -> Vec<PathBuf> {
    std::env::split_paths(path).collect()
}

fn path_candidates(entries: Vec<PathBuf>, program: &str) -> Vec<PathBuf> {
    entries.into_iter().map(|dir| dir.join(program)).collect()
}

fn existing_path_candidate(candidates: Vec<PathBuf>) -> Option<PathBuf> {
    candidates.into_iter().find(|candidate| candidate.is_file())
}

fn env_passthrough_pairs() -> Vec<(&'static str, OsString)> {
    ENV_PASSTHROUGH_KEYS
        .iter()
        .filter_map(|key| optional_env_pair(key))
        .collect()
}

fn optional_env_pair(key: &'static str) -> Option<(&'static str, OsString)> {
    env_value(key).map(|value| env_pair(key, value))
}

fn env_value(key: &str) -> Option<OsString> {
    std::env::var_os(key)
}

fn env_pair(key: &'static str, value: OsString) -> (&'static str, OsString) {
    (key, value)
}
