//! Declared roles: orchestration, formatter

use std::io::Read;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some(agent_runner_opencode::NATIVE_EFFECT_GATE_ARG) {
        std::process::exit(agent_runner_opencode::run_native_effect_gate(&args));
    }
    let stdin = read_stdin_or_exit();
    let exit_code = agent_runner_opencode::write_invocation(&args, &stdin, &mut std::io::stdout());
    std::process::exit(exit_code);
}

fn read_stdin_or_exit() -> Vec<u8> {
    let mut stdin = Vec::new();
    if let Err(err) = std::io::stdin()
        .take(agent_runner_opencode::envelope::MAX_REQUEST_ENVELOPE_BYTES.saturating_add(1) as u64)
        .read_to_end(&mut stdin)
    {
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
