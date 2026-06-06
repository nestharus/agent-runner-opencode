//! Declared roles: orchestration, formatter

use std::io::Read;

fn main() {
    let stdin = read_stdin_or_exit();
    let args = std::env::args().collect::<Vec<_>>();
    let exit_code = agent_runner_opencode::write_invocation(&args, &stdin, &mut std::io::stdout());
    std::process::exit(exit_code);
}

fn read_stdin_or_exit() -> Vec<u8> {
    let mut stdin = Vec::new();
    if let Err(err) = std::io::stdin().read_to_end(&mut stdin) {
        exit_stdin_read_failure(&stdin_read_failure_message(&err));
    }
    stdin
}

fn stdin_read_failure_message(err: &std::io::Error) -> String {
    format!("failed to read stdin: {err}")
}

fn exit_stdin_read_failure(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}
