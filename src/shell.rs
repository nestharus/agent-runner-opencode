//! Declared roles: orchestration, mapper, validator, accessor

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

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
    Command::new(resolved_program(program))
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
    if let Some(configured) = crate::config::program_override(program) {
        return configured;
    }
    let path = Path::new(program);
    if program_has_path_component(path) {
        return path.to_path_buf();
    }
    std::env::var_os("PATH")
        .and_then(|path| {
            std::env::split_paths(&path)
                .map(|directory| directory.join(program))
                .find(|candidate| candidate.is_file())
        })
        .unwrap_or_else(|| PathBuf::from(program))
}

fn program_has_path_component(path: &Path) -> bool {
    path.is_absolute() || path.components().count() > 1
}
