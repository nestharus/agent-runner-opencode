//! Declared roles: facade

use std::io::Read;

fn main() {
    let mut stdin = Vec::new();
    if let Err(err) = std::io::stdin().read_to_end(&mut stdin) {
        eprintln!("failed to read stdin: {err}");
        std::process::exit(2);
    }

    let args = std::env::args().collect::<Vec<_>>();
    let exit_code = agent_runner_opencode::write_invocation(&args, &stdin, &mut std::io::stdout());
    std::process::exit(exit_code);
}
