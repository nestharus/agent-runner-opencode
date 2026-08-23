//! Declared roles: orchestration, formatter

use std::io::Read;

const NATIVE_EXEC_GATE_ARG: &str = "__launch_exec_gate";
#[cfg(unix)]
const NATIVE_EXEC_GATE_FD_ENV: &str = "AGENT_RUNNER_OPENCODE_LAUNCH_GATE_FD";

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some(NATIVE_EXEC_GATE_ARG) {
        std::process::exit(run_native_exec_gate(&args));
    }
    let stdin = read_stdin_or_exit();
    let exit_code = agent_runner_opencode::write_invocation(&args, &stdin, &mut std::io::stdout());
    std::process::exit(exit_code);
}

#[cfg(unix)]
fn run_native_exec_gate(args: &[String]) -> i32 {
    use std::fs::File;
    use std::os::fd::FromRawFd;
    use std::os::unix::process::CommandExt;

    let Some((program, program_args)) = args.get(2..).and_then(|args| args.split_first()) else {
        eprintln!("native exec gate is missing its command");
        return 126;
    };
    let gate_fd = match std::env::var(NATIVE_EXEC_GATE_FD_ENV)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value >= 3)
    {
        Some(gate_fd) => gate_fd,
        None => {
            eprintln!("native exec gate has no valid inherited gate descriptor");
            return 126;
        }
    };
    let mut gate = unsafe { File::from_raw_fd(gate_fd) };
    let mut release = [0_u8; 1];
    if let Err(error) = gate.read_exact(&mut release) {
        eprintln!("native exec gate closed before actor publication: {error}");
        return 126;
    }
    if release != [1] {
        eprintln!("native exec gate received an invalid release token");
        return 126;
    }
    drop(gate);
    std::env::remove_var(NATIVE_EXEC_GATE_FD_ENV);
    let error = std::process::Command::new(program)
        .args(program_args)
        .exec();
    eprintln!("native exec gate could not execute native command: {error}");
    126
}

#[cfg(not(unix))]
fn run_native_exec_gate(_args: &[String]) -> i32 {
    eprintln!("native exec gate requires Unix process-group custody");
    126
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
