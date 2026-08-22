//! Durable native-process admission and process-group custody.

use std::ffi::OsStr;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus};
use std::time::Duration;
#[cfg(unix)]
use std::{os::fd::AsRawFd, os::unix::net::UnixStream};

const EXEC_GATE_ARG: &str = "__launch_exec_gate";
const EXEC_GATE_FD_ENV: &str = "AGENT_RUNNER_OPENCODE_LAUNCH_GATE_FD";
#[cfg(unix)]
const TERMINATION_GRACE: Duration = Duration::from_millis(100);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessGroupActor {
    pub(crate) process_group_id: u32,
    pub(crate) incarnation: String,
}

pub(crate) struct ExecGate {
    #[cfg(unix)]
    writer: UnixStream,
}

#[cfg(unix)]
pub(crate) struct GatedCommand {
    command: Command,
    writer: UnixStream,
    inherited_gate: UnixStream,
}

#[cfg(not(unix))]
pub(crate) struct GatedCommand;

#[cfg(unix)]
impl GatedCommand {
    pub(crate) fn new<I, S>(program: impl AsRef<OsStr>, args: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        use std::os::unix::process::CommandExt;

        let gate_program = exec_gate_program()?;
        let (writer, inherited_gate) = UnixStream::pair()?;
        let inherited_gate_fd = inherited_gate.as_raw_fd();
        let retained_gate = inherited_gate.try_clone()?;
        let mut command = Command::new(gate_program);
        command.arg(EXEC_GATE_ARG).arg(program).args(args);
        unsafe {
            command.pre_exec(move || {
                let _keep_gate_open = &retained_gate;
                let flags = libc::fcntl(inherited_gate_fd, libc::F_GETFD);
                if flags == -1
                    || libc::fcntl(inherited_gate_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC)
                        == -1
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(Self {
            command,
            writer,
            inherited_gate,
        })
    }

    pub(crate) fn command_mut(&mut self) -> &mut Command {
        &mut self.command
    }

    pub(crate) fn spawn(mut self) -> io::Result<(Child, ExecGate)> {
        configure_process_group(&mut self.command);
        self.command.env(
            EXEC_GATE_FD_ENV,
            self.inherited_gate.as_raw_fd().to_string(),
        );
        let child = self.command.spawn()?;
        drop(self.inherited_gate);
        Ok((
            child,
            ExecGate {
                writer: self.writer,
            },
        ))
    }
}

#[cfg(not(unix))]
impl GatedCommand {
    pub(crate) fn new<I, S>(_program: impl AsRef<OsStr>, _args: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native commands require Unix process-group custody",
        ))
    }

    pub(crate) fn command_mut(&mut self) -> &mut Command {
        unreachable!("unsupported native gated command cannot be configured")
    }

    pub(crate) fn spawn(self) -> io::Result<(Child, ExecGate)> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "native commands require Unix process-group custody",
        ))
    }
}

impl ExecGate {
    #[cfg(unix)]
    pub(crate) fn release(mut self) -> io::Result<()> {
        self.writer.write_all(&[1])?;
        self.writer.flush()
    }

    #[cfg(not(unix))]
    pub(crate) fn release(self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn actor_for_child(child: &Child) -> io::Result<ProcessGroupActor> {
    let process_group_id = child.id();
    Ok(ProcessGroupActor {
        process_group_id,
        incarnation: process_group_incarnation(process_group_id)?,
    })
}

pub(crate) fn actor_is_terminal_or_recycled(actor: &ProcessGroupActor) -> io::Result<bool> {
    if !process_group_is_live(actor.process_group_id) {
        return Ok(true);
    }
    match process_group_incarnation(actor.process_group_id) {
        Ok(incarnation) => Ok(incarnation != actor.incarnation),
        Err(_) if !process_group_is_live(actor.process_group_id) => Ok(true),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
pub(crate) fn terminate_process_group_child(child: &mut Child) -> Option<ExitStatus> {
    let pgid = -(child.id() as i32);
    send_process_group_signal(pgid, SIGTERM);
    std::thread::sleep(TERMINATION_GRACE);
    send_process_group_signal(pgid, SIGKILL);
    child.wait().ok()
}

#[cfg(not(unix))]
pub(crate) fn terminate_process_group_child(child: &mut Child) -> Option<ExitStatus> {
    let _ = child.kill();
    child.wait().ok()
}

#[cfg(unix)]
fn exec_gate_program() -> io::Result<PathBuf> {
    let current = std::env::current_exe()?;
    let binary_name = "agent-runner-opencode";
    if current.file_name().and_then(|name| name.to_str()) == Some(binary_name) {
        return Ok(current);
    }
    let Some(parent) = current.parent() else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "provider executable has no containing directory",
        ));
    };
    let candidate = if parent.file_name().and_then(|name| name.to_str()) == Some("deps") {
        parent.parent().unwrap_or(parent).join(binary_name)
    } else {
        parent.join(binary_name)
    };
    candidate.is_file().then_some(candidate).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "could not locate the provider native-command gate executable",
        )
    })
}

#[cfg(target_os = "linux")]
pub(crate) fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    let parent_pid = unsafe { getpid() };
    unsafe {
        command.pre_exec(move || set_current_process_group_with_parent_death(parent_pid));
    }
}

#[cfg(target_os = "linux")]
fn set_current_process_group_with_parent_death(parent_pid: i32) -> io::Result<()> {
    set_current_process_group()?;
    if unsafe { prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { getppid() } != parent_pid {
        return Err(io::Error::other(
            "provider parent exited before child custody was established",
        ));
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    unsafe {
        command.pre_exec(set_current_process_group);
    }
}

#[cfg(unix)]
fn set_current_process_group() -> io::Result<()> {
    if unsafe { setpgid(0, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn process_group_incarnation(process_id: u32) -> io::Result<String> {
    let stat = fs::read_to_string(format!("/proc/{process_id}/stat"))?;
    let command_end = stat.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat has no command terminator",
        )
    })?;
    let start_ticks = stat[command_end + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process stat has no start-time field",
            )
        })?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "kernel boot identity is empty",
        ));
    }
    Ok(format!("linux:{boot_id}:{start_ticks}"))
}

#[cfg(target_os = "macos")]
pub(crate) fn process_group_incarnation(process_id: u32) -> io::Result<String> {
    let process_id = i32::try_from(process_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "process identity exceeds the platform pid range",
        )
    })?;
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let info_size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "process identity structure exceeds the platform query range",
        )
    })?;
    let read_size = unsafe {
        libc::proc_pidinfo(
            process_id,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            info_size,
        )
    };
    if read_size <= 0 {
        return Err(io::Error::last_os_error());
    }
    if read_size != info_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "process identity query returned a partial record",
        ));
    }
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != process_id as u32 || info.pbi_pgid != process_id as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native actor is not the leader of its registered process group",
        ));
    }
    Ok(format!(
        "macos:{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub(crate) fn process_group_incarnation(_process_id: u32) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "durable native actor incarnation is unsupported on this Unix platform",
    ))
}

#[cfg(not(unix))]
pub(crate) fn process_group_incarnation(_process_id: u32) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "durable native actor incarnation requires Unix process custody",
    ))
}

#[cfg(unix)]
pub(crate) fn process_group_is_live(process_group_id: u32) -> bool {
    let Ok(process_group_id) = i32::try_from(process_group_id) else {
        return true;
    };
    if unsafe { kill(-process_group_id, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(not(unix))]
pub(crate) fn process_group_is_live(_process_group_id: u32) -> bool {
    true
}

#[cfg(unix)]
fn send_process_group_signal(pgid: i32, signal: i32) {
    unsafe {
        let _ = kill(pgid, signal);
    }
}

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(target_os = "linux")]
const PR_SET_PDEATHSIG: i32 = 1;

#[cfg(unix)]
unsafe extern "C" {
    fn setpgid(pid: i32, pgid: i32) -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn getpid() -> i32;
    fn getppid() -> i32;
    fn prctl(option: i32, arg2: i32, arg3: usize, arg4: usize, arg5: usize) -> i32;
}
