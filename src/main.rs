//! Declared roles: facade

use std::io::{Read, Write};

fn main() {
    let mut stdin = Vec::new();
    if let Err(err) = std::io::stdin().read_to_end(&mut stdin) {
        eprintln!("failed to read stdin: {err}");
        std::process::exit(2);
    }

    let args = std::env::args().collect::<Vec<_>>();
    let (stdout, exit_code) = agent_runner_opencode::handle_invocation(&args, &stdin);
    if let Err(err) = std::io::stdout().write_all(&stdout) {
        eprintln!("failed to write stdout: {err}");
        std::process::exit(1);
    }
    std::process::exit(exit_code);
}
