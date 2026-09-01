//! Durable native-process admission and process-group custody.

use std::ffi::OsStr;
#[cfg(target_os = "linux")]
use std::fs;
use std::io;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(target_os = "linux")]
use std::os::fd::{FromRawFd, OwnedFd};
#[cfg(unix)]
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus};
#[cfg(unix)]
use std::time::{Duration, Instant};
#[cfg(unix)]
use std::{os::fd::AsRawFd, os::unix::net::UnixStream};

pub const NATIVE_EFFECT_GATE_ARG: &str = "__native_effect_gate";
#[cfg(unix)]
pub const NATIVE_EFFECT_GATE_FD_ENV: &str = "AGENT_RUNNER_OPENCODE_NATIVE_EFFECT_GATE_FD";
#[cfg(unix)]
const TERMINATION_GRACE: Duration = Duration::from_millis(100);
#[cfg(unix)]
const ACTOR_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(unix)]
const INITIAL_SETTLEMENT_BACKOFF: Duration = Duration::from_millis(2);
#[cfg(unix)]
const MAX_SETTLEMENT_BACKOFF: Duration = Duration::from_millis(50);

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
        command.arg(NATIVE_EFFECT_GATE_ARG).arg(program).args(args);
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
        #[cfg(target_os = "linux")]
        enable_child_subreaper()?;
        self.command.env(
            NATIVE_EFFECT_GATE_FD_ENV,
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

#[cfg(unix)]
pub fn run_native_effect_gate(args: &[String]) -> i32 {
    use std::fs::File;
    use std::os::fd::FromRawFd;
    use std::os::unix::process::CommandExt;

    let Some((program, program_args)) = args.get(2..).and_then(|args| args.split_first()) else {
        eprintln!("native effect gate is missing its command");
        return 126;
    };
    let gate_fd = match std::env::var(NATIVE_EFFECT_GATE_FD_ENV)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value >= 3)
    {
        Some(gate_fd) => gate_fd,
        None => {
            eprintln!("native effect gate has no valid inherited gate descriptor");
            return 126;
        }
    };
    let mut gate = unsafe { File::from_raw_fd(gate_fd) };
    let mut release = [0_u8; 1];
    if let Err(error) = gate.read_exact(&mut release) {
        eprintln!("native effect gate closed before actor publication: {error}");
        return 126;
    }
    if release != [1] {
        eprintln!("native effect gate received an invalid release token");
        return 126;
    }
    drop(gate);
    std::env::remove_var(NATIVE_EFFECT_GATE_FD_ENV);
    let error = Command::new(program).args(program_args).exec();
    eprintln!("native effect gate could not execute native command: {error}");
    126
}

#[cfg(not(unix))]
pub fn run_native_effect_gate(_args: &[String]) -> i32 {
    eprintln!("native effect gate requires Unix process-group custody");
    126
}

pub(crate) fn actor_for_child(child: &Child) -> io::Result<ProcessGroupActor> {
    let process_group_id = child.id();
    Ok(ProcessGroupActor {
        process_group_id,
        incarnation: process_group_incarnation(process_group_id)?,
    })
}

#[cfg(test)]
pub(crate) fn actor_is_terminal_or_recycled(actor: &ProcessGroupActor) -> io::Result<bool> {
    Ok(!process_group_actor_requires_signal(actor)?)
}

/// Discharge custody for a durably recorded process-group incarnation after
/// its in-process owner has been lost. Linux signals only pidfd-pinned members;
/// a numeric PGID is never a signal target after its incarnation can drain.
#[cfg(unix)]
pub(crate) fn terminate_process_group_actor(actor: &ProcessGroupActor) -> io::Result<()> {
    terminate_process_group_actor_inner(actor, None).map(|_| ())
}

#[cfg(unix)]
pub(crate) fn terminate_process_group_actor_with_child(
    actor: &ProcessGroupActor,
    child: &mut Child,
) -> io::Result<Option<ExitStatus>> {
    terminate_process_group_actor_inner(actor, Some(child))
}

#[cfg(unix)]
fn terminate_process_group_actor_inner(
    actor: &ProcessGroupActor,
    child: Option<&mut Child>,
) -> io::Result<Option<ExitStatus>> {
    #[cfg(target_os = "linux")]
    {
        terminate_linux_process_group_actor(actor, child)
    }
    #[cfg(not(target_os = "linux"))]
    {
        terminate_pinned_process_group_actor(actor, child)
    }
}

#[cfg(target_os = "linux")]
fn terminate_linux_process_group_actor(
    actor: &ProcessGroupActor,
    child: Option<&mut Child>,
) -> io::Result<Option<ExitStatus>> {
    let started = Instant::now();
    let deadline = started + ACTOR_SETTLEMENT_TIMEOUT;
    let child_id = child.as_ref().map(|child| child.id());
    let child_pins_identity =
        child_id == Some(actor.process_group_id) && linux_actor_leader_matches(actor)?;
    if child.is_some() && !child_pins_identity {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "direct actor leader was reaped before process-group settlement could pin its incarnation",
        ));
    }

    let initial = linux_process_group_snapshot(actor.process_group_id)?;
    if linux_snapshot_is_recycled(actor, &initial)? {
        return reap_settled_direct_child(child);
    }
    if !initial.iter().any(LinuxProcess::is_effect_capable) {
        if child_pins_identity {
            reap_adopted_process_group_descendants(actor, child_id)?;
        }
        return reap_settled_direct_child(child);
    }

    let mut members = Vec::new();
    pin_and_stop_snapshot_members(&initial, &mut members)?;
    if !child_pins_identity && !members_have_effect(&members)? {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "native process group lost its stable member anchor before settlement",
        ));
    }

    freeze_actor_members(actor, &mut members, child_pins_identity, deadline)?;
    signal_actor_members(&members, SIGTERM, None)?;
    let retained_anchor = if child_pins_identity {
        None
    } else {
        first_effect_capable_identity(&members)?
    };
    signal_actor_members(&members, SIGCONT, retained_anchor)?;
    wait_for_pinned_members(&members, (started + TERMINATION_GRACE).min(deadline))?;

    freeze_actor_members(actor, &mut members, child_pins_identity, deadline)?;
    signal_actor_members(&members, SIGKILL, None)?;
    if !wait_for_pinned_members(&members, deadline)? {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "native process group retains effect-capable members after bounded termination",
        ));
    }
    reap_pinned_descendants(&members, child_id)?;
    if child_pins_identity {
        reap_adopted_process_group_descendants(actor, child_id)?;
    }
    reap_settled_direct_child(child)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn terminate_pinned_process_group_actor(
    actor: &ProcessGroupActor,
    mut child: Option<&mut Child>,
) -> io::Result<Option<ExitStatus>> {
    if !process_group_actor_requires_signal(actor)? {
        return reap_settled_direct_child(child);
    }
    let Some(child) = child.as_deref_mut() else {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "durable process-group recovery has no waitable leader to pin the numeric group identity",
        ));
    };
    if child.id() != actor.process_group_id
        || process_group_incarnation(child.id())? != actor.incarnation
    {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "direct actor leader no longer pins the recorded process-group incarnation",
        ));
    }
    let process_group_id = i32::try_from(actor.process_group_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "native process-group identity exceeds the platform PID range",
        )
    })?;
    send_process_group_signal_checked(-process_group_id, SIGTERM)?;
    std::thread::sleep(TERMINATION_GRACE);
    send_process_group_signal_checked(-process_group_id, SIGKILL)?;
    reap_settled_direct_child(Some(child))
}

#[cfg(unix)]
fn reap_settled_direct_child(child: Option<&mut Child>) -> io::Result<Option<ExitStatus>> {
    child.map(Child::wait).transpose()
}

#[cfg(unix)]
pub(crate) fn child_exit_status_unreaped(child: &Child) -> io::Result<Option<ExitStatus>> {
    use std::os::unix::process::ExitStatusExt;

    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    if unsafe {
        libc::waitid(
            libc::P_PID,
            child.id() as libc::id_t,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    let info = unsafe { info.assume_init() };
    if unsafe { info.si_pid() } == 0 {
        return Ok(None);
    }
    let status = unsafe { info.si_status() };
    match info.si_code {
        libc::CLD_EXITED => Ok(Some(ExitStatus::from_raw(status << 8))),
        libc::CLD_KILLED => Ok(Some(ExitStatus::from_raw(status))),
        libc::CLD_DUMPED => Ok(Some(ExitStatus::from_raw(status | 0x80))),
        code => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("waitid returned unexpected child exit code {code}"),
        )),
    }
}

#[cfg(not(unix))]
pub(crate) fn child_exit_status_unreaped(child: &mut Child) -> io::Result<Option<ExitStatus>> {
    child.try_wait()
}

#[cfg(all(target_os = "linux", test))]
fn process_group_actor_requires_signal(actor: &ProcessGroupActor) -> io::Result<bool> {
    linux_process_group_actor_requires_signal(actor)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_group_actor_requires_signal(actor: &ProcessGroupActor) -> io::Result<bool> {
    if !process_group_is_live(actor.process_group_id) {
        return Ok(false);
    }
    match process_group_incarnation(actor.process_group_id) {
        Ok(incarnation) => Ok(incarnation == actor.incarnation),
        Err(error) if !process_group_is_live(actor.process_group_id) => Ok(false),
        Err(error) if process_group_leader_is_missing(&error) => Ok(true),
        Err(error) => Err(error),
    }
}

#[cfg(all(target_os = "linux", test))]
fn linux_process_group_actor_requires_signal(actor: &ProcessGroupActor) -> io::Result<bool> {
    let snapshot = linux_process_group_snapshot(actor.process_group_id)?;
    if snapshot.is_empty() {
        return Ok(false);
    }
    if let Some(leader) = snapshot
        .iter()
        .find(|process| process.process_id == actor.process_group_id)
    {
        if linux_incarnation(leader.start_ticks)? != actor.incarnation {
            return Ok(false);
        }
    }
    Ok(snapshot.iter().any(LinuxProcess::is_effect_capable))
}

#[cfg(target_os = "linux")]
fn reap_adopted_process_group_descendants(
    actor: &ProcessGroupActor,
    direct_child_id: Option<u32>,
) -> io::Result<()> {
    let snapshot = linux_process_group_snapshot(actor.process_group_id)?;
    if snapshot
        .iter()
        .find(|process| process.process_id == actor.process_group_id)
        .is_some_and(|leader| {
            linux_incarnation(leader.start_ticks)
                .is_ok_and(|incarnation| incarnation != actor.incarnation)
        })
    {
        return Ok(());
    }
    for process in snapshot
        .into_iter()
        .filter(|process| process.is_dead() && Some(process.process_id) != direct_child_id)
    {
        let Ok(process_id) = i32::try_from(process.process_id) else {
            continue;
        };
        let mut status = 0;
        let waited = unsafe { libc::waitpid(process_id, &mut status, libc::WNOHANG) };
        if waited == process_id || waited == 0 {
            continue;
        }
        let error = io::Error::last_os_error();
        if !matches!(error.raw_os_error(), Some(libc::ECHILD) | Some(libc::ESRCH)) {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn reap_adopted_process_group_descendants(
    _actor: &ProcessGroupActor,
    _direct_child_id: Option<u32>,
) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct LinuxProcess {
    process_id: u32,
    process_group_id: u32,
    state: u8,
    start_ticks: u64,
}

#[cfg(target_os = "linux")]
impl LinuxProcess {
    fn is_dead(&self) -> bool {
        matches!(self.state, b'Z' | b'X' | b'x')
    }

    fn is_effect_capable(&self) -> bool {
        !self.is_dead()
    }

    fn is_stopped(&self) -> bool {
        matches!(self.state, b'T' | b't')
    }
}

#[cfg(target_os = "linux")]
struct LinuxProcessPin {
    process: LinuxProcess,
    pidfd: OwnedFd,
}

#[cfg(target_os = "linux")]
trait StableMemberSignal {
    fn send(&self, signal: i32) -> io::Result<()>;
}

#[cfg(target_os = "linux")]
impl StableMemberSignal for LinuxProcessPin {
    fn send(&self, signal: i32) -> io::Result<()> {
        send_pidfd_signal(&self.pidfd, signal)
    }
}

#[cfg(target_os = "linux")]
fn send_stable_member_signal(target: &impl StableMemberSignal, signal: i32) -> io::Result<()> {
    target.send(signal)
}

#[cfg(target_os = "linux")]
impl LinuxProcessPin {
    fn identity(&self) -> (u32, u64) {
        (self.process.process_id, self.process.start_ticks)
    }

    fn is_effect_capable(&self) -> io::Result<bool> {
        if pidfd_has_exited(&self.pidfd)? {
            return Ok(false);
        }
        let Some(current) = read_linux_process(self.process.process_id)? else {
            return Ok(false);
        };
        Ok(current.start_ticks == self.process.start_ticks && current.is_effect_capable())
    }

    fn is_stopped_or_terminal(&self) -> io::Result<bool> {
        if pidfd_has_exited(&self.pidfd)? {
            return Ok(true);
        }
        let Some(current) = read_linux_process(self.process.process_id)? else {
            return Ok(true);
        };
        Ok(current.start_ticks != self.process.start_ticks
            || current.is_dead()
            || current.is_stopped())
    }
}

#[cfg(target_os = "linux")]
fn linux_actor_leader_matches(actor: &ProcessGroupActor) -> io::Result<bool> {
    let Some(leader) = read_linux_process(actor.process_group_id)? else {
        return Ok(false);
    };
    Ok(leader.process_group_id == actor.process_group_id
        && linux_incarnation(leader.start_ticks)? == actor.incarnation)
}

#[cfg(target_os = "linux")]
fn linux_snapshot_is_recycled(
    actor: &ProcessGroupActor,
    snapshot: &[LinuxProcess],
) -> io::Result<bool> {
    snapshot
        .iter()
        .find(|process| process.process_id == actor.process_group_id)
        .map(|leader| Ok(linux_incarnation(leader.start_ticks)? != actor.incarnation))
        .unwrap_or(Ok(false))
}

#[cfg(target_os = "linux")]
fn pin_and_stop_snapshot_members(
    snapshot: &[LinuxProcess],
    members: &mut Vec<LinuxProcessPin>,
) -> io::Result<usize> {
    let mut pinned = 0;
    for process in snapshot
        .iter()
        .filter(|process| process.is_effect_capable())
    {
        if members
            .iter()
            .any(|member| member.identity() == (process.process_id, process.start_ticks))
        {
            continue;
        }
        let Some(member) = pin_linux_process(process)? else {
            continue;
        };
        send_stable_member_signal(&member, SIGSTOP)?;
        members.push(member);
        pinned += 1;
    }
    Ok(pinned)
}

#[cfg(target_os = "linux")]
fn freeze_actor_members(
    actor: &ProcessGroupActor,
    members: &mut Vec<LinuxProcessPin>,
    child_pins_identity: bool,
    deadline: Instant,
) -> io::Result<()> {
    let mut backoff = INITIAL_SETTLEMENT_BACKOFF;
    loop {
        if !child_pins_identity && !members_have_effect(members)? {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "native process group lost its stable member anchor during settlement",
            ));
        }
        let snapshot = linux_process_group_snapshot(actor.process_group_id)?;
        if linux_snapshot_is_recycled(actor, &snapshot)? {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "native process-group incarnation changed while stable members were being pinned",
            ));
        }
        let added = pin_and_stop_snapshot_members(&snapshot, members)?;
        for member in members.iter() {
            if member.is_effect_capable()? {
                send_stable_member_signal(member, SIGSTOP)?;
            }
        }
        if wait_for_members_stopped(members, deadline)? {
            let verification = linux_process_group_snapshot(actor.process_group_id)?;
            if linux_snapshot_is_recycled(actor, &verification)? {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "native process-group incarnation changed during stable-member verification",
                ));
            }
            let has_unpinned_member = verification.iter().any(|process| {
                process.is_effect_capable()
                    && !members.iter().any(|member| {
                        member.identity() == (process.process_id, process.start_ticks)
                    })
            });
            if added == 0 && !has_unpinned_member {
                return Ok(());
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "native process group could not be frozen within the bounded settlement interval",
            ));
        }
        std::thread::sleep(remaining.min(backoff));
        backoff = (backoff * 2).min(MAX_SETTLEMENT_BACKOFF);
    }
}

#[cfg(target_os = "linux")]
fn wait_for_members_stopped(members: &[LinuxProcessPin], deadline: Instant) -> io::Result<bool> {
    let mut backoff = INITIAL_SETTLEMENT_BACKOFF;
    loop {
        if members_all_stopped_or_terminal(members)? {
            return Ok(true);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        std::thread::sleep(remaining.min(backoff));
        backoff = (backoff * 2).min(MAX_SETTLEMENT_BACKOFF);
    }
}

#[cfg(target_os = "linux")]
fn signal_actor_members(
    members: &[LinuxProcessPin],
    signal: i32,
    retained_anchor: Option<(u32, u64)>,
) -> io::Result<()> {
    for member in members {
        if retained_anchor == Some(member.identity()) || !member.is_effect_capable()? {
            continue;
        }
        send_stable_member_signal(member, signal)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn wait_for_pinned_members(members: &[LinuxProcessPin], deadline: Instant) -> io::Result<bool> {
    if retain_effect_capable_test_member() {
        return Ok(!members_have_effect(members)?);
    }
    let mut backoff = INITIAL_SETTLEMENT_BACKOFF;
    loop {
        if !members_have_effect(members)? {
            return Ok(true);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        std::thread::sleep(remaining.min(backoff));
        backoff = (backoff * 2).min(MAX_SETTLEMENT_BACKOFF);
    }
}

#[cfg(target_os = "linux")]
fn members_have_effect(members: &[LinuxProcessPin]) -> io::Result<bool> {
    for member in members {
        if member.is_effect_capable()? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "linux")]
fn first_effect_capable_identity(members: &[LinuxProcessPin]) -> io::Result<Option<(u32, u64)>> {
    for member in members {
        if member.is_effect_capable()? {
            return Ok(Some(member.identity()));
        }
    }
    Ok(None)
}

#[cfg(target_os = "linux")]
fn members_all_stopped_or_terminal(members: &[LinuxProcessPin]) -> io::Result<bool> {
    for member in members {
        if !member.is_stopped_or_terminal()? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(target_os = "linux")]
fn pin_linux_process(process: &LinuxProcess) -> io::Result<Option<LinuxProcessPin>> {
    let process_id = i32::try_from(process.process_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "native process identity exceeds the platform PID range",
        )
    })?;
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, process_id, 0_u32) };
    if fd == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(None);
        }
        return Err(error);
    }
    let pidfd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
    let Some(current) = read_linux_process(process.process_id)? else {
        return Ok(None);
    };
    if current.process_group_id != process.process_group_id
        || current.start_ticks != process.start_ticks
        || pidfd_has_exited(&pidfd)?
    {
        return Ok(None);
    }
    Ok(Some(LinuxProcessPin {
        process: current,
        pidfd,
    }))
}

#[cfg(target_os = "linux")]
fn send_pidfd_signal(pidfd: &OwnedFd, signal: i32) -> io::Result<()> {
    if retain_effect_capable_test_member() && matches!(signal, SIGTERM | SIGKILL) {
        return Ok(());
    }
    if unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0_u32,
        )
    } == 0
    {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(error)
}

#[cfg(target_os = "linux")]
fn pidfd_has_exited(pidfd: &OwnedFd) -> io::Result<bool> {
    let mut pollfd = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let result = unsafe { libc::poll(&mut pollfd, 1, 0) };
        if result >= 0 {
            return Ok(result == 1);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(target_os = "linux")]
fn read_linux_process(process_id: u32) -> io::Result<Option<LinuxProcess>> {
    let stat = match fs::read_to_string(format!("/proc/{process_id}/stat")) {
        Ok(stat) => stat,
        Err(error) if linux_process_disappeared(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    parse_linux_process_stat(process_id, &stat).map(Some)
}

#[cfg(target_os = "linux")]
fn linux_process_disappeared(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

#[cfg(target_os = "linux")]
fn reap_pinned_descendants(
    members: &[LinuxProcessPin],
    direct_child_id: Option<u32>,
) -> io::Result<()> {
    for member in members
        .iter()
        .filter(|member| Some(member.process.process_id) != direct_child_id)
    {
        let process_id = i32::try_from(member.process.process_id).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "native process identity exceeds the platform PID range",
            )
        })?;
        let mut status = 0;
        let waited = unsafe { libc::waitpid(process_id, &mut status, libc::WNOHANG) };
        if waited == process_id || waited == 0 {
            continue;
        }
        let error = io::Error::last_os_error();
        if !matches!(error.raw_os_error(), Some(libc::ECHILD) | Some(libc::ESRCH)) {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn retain_effect_capable_test_member() -> bool {
    #[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
    {
        std::env::var_os("AGENT_RUNNER_OPENCODE_TEST_RETAIN_EFFECT_CAPABLE_ACTOR").is_some()
    }
    #[cfg(not(all(feature = "contract-test-fixtures", debug_assertions)))]
    {
        false
    }
}

#[cfg(target_os = "linux")]
fn linux_process_group_snapshot(process_group_id: u32) -> io::Result<Vec<LinuxProcess>> {
    let mut snapshot = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(process_id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let stat = match fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if linux_process_disappeared(&error) => continue,
            Err(error) => return Err(error),
        };
        let process = parse_linux_process_stat(process_id, &stat)?;
        if process.process_group_id == process_group_id {
            snapshot.push(process);
        }
    }
    Ok(snapshot)
}

#[cfg(target_os = "linux")]
fn parse_linux_process_stat(process_id: u32, stat: &str) -> io::Result<LinuxProcess> {
    let command_end = stat.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat has no command terminator",
        )
    })?;
    let fields = stat[command_end + 1..]
        .split_whitespace()
        .collect::<Vec<_>>();
    let state = fields
        .first()
        .and_then(|state| state.as_bytes().first())
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "process stat has no state"))?;
    let process_group_id = parse_linux_stat_field::<u32>(&fields, 2, "process group")?;
    let start_ticks = parse_linux_stat_field::<u64>(&fields, 19, "start time")?;
    Ok(LinuxProcess {
        process_id,
        process_group_id,
        state,
        start_ticks,
    })
}

#[cfg(target_os = "linux")]
fn parse_linux_stat_field<T>(fields: &[&str], index: usize, name: &str) -> io::Result<T>
where
    T: std::str::FromStr,
    T::Err: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fields
        .get(index)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process stat has no {name} field"),
            )
        })?
        .parse::<T>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(target_os = "linux")]
fn linux_incarnation(start_ticks: u64) -> io::Result<String> {
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

#[cfg(target_os = "linux")]
fn enable_child_subreaper() -> io::Result<()> {
    if unsafe { prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn terminate_process_group_actor(_actor: &ProcessGroupActor) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "durable process-group recovery requires Unix process custody",
    ))
}

#[cfg(not(unix))]
pub(crate) fn terminate_process_group_actor_with_child(
    _actor: &ProcessGroupActor,
    _child: &mut Child,
) -> io::Result<Option<ExitStatus>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "durable process-group recovery requires Unix process custody",
    ))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_group_leader_is_missing(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::ESRCH)
}

#[cfg(unix)]
pub(crate) fn terminate_process_group_child(child: &mut Child) -> Option<ExitStatus> {
    let actor = actor_for_child(child).ok();
    match actor {
        Some(actor) => terminate_process_group_actor_with_child(&actor, child)
            .ok()
            .flatten(),
        None => match child.try_wait().ok()? {
            Some(status) => Some(status),
            None => {
                let _ = child.kill();
                child.wait().ok()
            }
        },
    }
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
    linux_incarnation(start_ticks)
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

#[cfg(all(unix, not(target_os = "linux")))]
fn send_process_group_signal_checked(pgid: i32, signal: i32) -> io::Result<()> {
    if unsafe { kill(pgid, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(error)
}

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(target_os = "linux")]
const SIGCONT: i32 = 18;
#[cfg(target_os = "linux")]
const SIGSTOP: i32 = 19;
#[cfg(target_os = "linux")]
const PR_SET_PDEATHSIG: i32 = 1;
#[cfg(target_os = "linux")]
const PR_SET_CHILD_SUBREAPER: i32 = 36;

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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct InjectedStableSignal {
        pinned_generation: u64,
        signalled_generations: Arc<Mutex<Vec<u64>>>,
    }

    impl StableMemberSignal for InjectedStableSignal {
        fn send(&self, _signal: i32) -> io::Result<()> {
            self.signalled_generations
                .lock()
                .expect("stable signal evidence lock")
                .push(self.pinned_generation);
            Ok(())
        }
    }

    #[test]
    fn stable_member_signal_cannot_follow_numeric_identity_reuse() {
        let signalled_generations = Arc::new(Mutex::new(Vec::new()));
        let original = InjectedStableSignal {
            pinned_generation: 1,
            signalled_generations: Arc::clone(&signalled_generations),
        };
        let numeric_identity_generation = Arc::new(Mutex::new(1_u64));

        *numeric_identity_generation
            .lock()
            .expect("numeric identity generation lock") = 2;
        send_stable_member_signal(&original, SIGKILL).expect("signal pinned original member");

        assert_eq!(
            *numeric_identity_generation
                .lock()
                .expect("numeric identity generation lock"),
            2,
            "the same numeric identity now names foreign work"
        );
        assert_eq!(
            *signalled_generations
                .lock()
                .expect("stable signal evidence lock"),
            vec![1],
            "signalling must remain bound to the pinned original incarnation"
        );
    }

    #[test]
    fn durable_actor_recovery_terminates_a_group_after_its_leader_exits() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 30 </dev/null >/dev/null 2>&1 & exit 0");
        configure_process_group(&mut command);
        let mut child = command.spawn().expect("spawn orphanable process group");
        let actor = actor_for_child(&child).expect("capture durable actor identity");
        child.wait().expect("process-group leader exits");
        assert!(
            process_group_is_live(actor.process_group_id),
            "background descendant retains the original process group"
        );

        terminate_process_group_actor(&actor).expect("terminate orphaned process group");
        assert!(!process_group_is_live(actor.process_group_id));
    }

    #[test]
    fn durable_actor_recovery_never_signals_a_recycled_group_identity() {
        let mut command = Command::new("sleep");
        command.arg("30");
        configure_process_group(&mut command);
        let mut child = command.spawn().expect("spawn isolated process group");
        let mut actor = actor_for_child(&child).expect("capture process-group incarnation");
        actor.incarnation.push_str(":recycled");

        terminate_process_group_actor(&actor).expect("ignore recycled process-group identity");
        assert!(
            child
                .try_wait()
                .expect("observe isolated process")
                .is_none(),
            "a mismatched process-group incarnation must never be signalled"
        );

        child.kill().expect("terminate isolated process group");
        child.wait().expect("reap isolated process group");
    }
}
