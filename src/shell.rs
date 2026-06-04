//! Declared roles: orchestration, mapper

use std::io;
use std::process::Command;

#[derive(Debug)]
pub struct ShellOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub status: i32,
}

pub fn run(argv: &[String]) -> io::Result<ShellOutput> {
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "argv must not be empty"))?;
    let output = Command::new(program).args(args).output()?;
    Ok(ShellOutput {
        stdout: output.stdout,
        stderr: output.stderr,
        status: output.status.code().unwrap_or(1),
    })
}
