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
#[cfg(target_os = "linux")]
const LINUX_SNAPSHOT_MAX_ATTEMPTS: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessGroupActor {
    pub(crate) process_group_id: u32,
    pub(crate) incarnation: String,
}

#[derive(Debug)]
struct UnresolvedActorOwnership(&'static str);

impl std::fmt::Display for UnresolvedActorOwnership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for UnresolvedActorOwnership {}

fn unresolved_actor_ownership(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, UnresolvedActorOwnership(message))
}

pub(crate) fn actor_cleanup_ownership_is_unresolved(error: &io::Error) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<UnresolvedActorOwnership>())
        .is_some()
}

#[cfg(test)]
pub(crate) fn unresolved_actor_ownership_for_test() -> io::Error {
    unresolved_actor_ownership(
        "durable recovery ownership is unresolved: injected leaderless actor",
    )
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
/// its in-process owner has been lost. Linux requires the recorded leader to be
/// present and stably pinned before it signals any current group member.
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
    inject_actor_settlement_failure()?;
    #[cfg(target_os = "linux")]
    {
        terminate_linux_process_group_actor(actor, child)
    }
    #[cfg(not(target_os = "linux"))]
    {
        terminate_pinned_process_group_actor(actor, child)
    }
}

#[cfg(unix)]
fn inject_actor_settlement_failure() -> io::Result<()> {
    #[cfg(all(feature = "contract-test-fixtures", debug_assertions))]
    if let Some(attempt_path) =
        std::env::var_os("AGENT_RUNNER_OPENCODE_TEST_ACTOR_SETTLEMENT_FAILURE_FILE")
    {
        let mut attempts = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(attempt_path)?;
        attempts.write_all(b"attempt\n")?;
        attempts.flush()?;
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "injected persistent actor-settlement failure",
        ));
    }
    Ok(())
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

    let initial = linux_process_group_snapshot_before(actor.process_group_id, deadline)?;
    let mut members = Vec::new();
    if child_pins_identity {
        if linux_snapshot_is_recycled(actor, &initial)? {
            return reap_settled_direct_child(child);
        }
    } else {
        match linux_durable_recovery_provenance(actor, &initial)? {
            LinuxDurableRecoveryProvenance::Vacant | LinuxDurableRecoveryProvenance::Recycled => {
                return reap_settled_direct_child(child);
            }
            LinuxDurableRecoveryProvenance::RecordedLeader(leader) => {
                let Some(leader) = pin_linux_process(leader)? else {
                    return Err(unresolved_actor_ownership(
                        "durable recovery ownership is unresolved: the recorded process-group leader incarnation disappeared before a stable kernel pin could be acquired",
                    ));
                };
                members.push(leader);
            }
        }
    }
    if !initial.iter().any(LinuxProcess::is_effect_capable) {
        if child_pins_identity {
            reap_adopted_process_group_descendants(actor, child_id, deadline)?;
        }
        return reap_settled_direct_child(child);
    }

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
        reap_adopted_process_group_descendants(actor, child_id, deadline)?;
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
        return Err(unresolved_actor_ownership(
            "durable recovery ownership is unresolved: no waitable leader or restart-surviving kernel containment identity pins the numeric process group",
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
    let mut settlement = PinnedProcessGroupSettlement::new(process_group_id);
    settle_effect_capable_process_group(&mut settlement)?;
    reap_settled_direct_child(Some(child))
}

#[cfg(unix)]
fn reap_settled_direct_child(child: Option<&mut Child>) -> io::Result<Option<ExitStatus>> {
    let Some(child) = child else {
        return Ok(None);
    };
    child.try_wait()?.map(Some).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "direct actor leader was not waitable after process-group settlement",
        )
    })
}

#[cfg(all(unix, any(not(target_os = "linux"), test)))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EffectCapableGroupObservation {
    Gone,
    Present,
}

#[cfg(all(unix, any(not(target_os = "linux"), test)))]
trait ProcessGroupSettlementDriver {
    fn observe(&mut self) -> io::Result<EffectCapableGroupObservation>;
    fn signal(&mut self, signal: i32) -> io::Result<()>;
    fn elapsed(&self) -> Duration;
    fn wait(&mut self, duration: Duration);
}

#[cfg(all(unix, any(not(target_os = "linux"), test)))]
enum BoundedGroupObservation {
    Gone,
    Present,
    Uncertain(String),
}

#[cfg(all(unix, any(not(target_os = "linux"), test)))]
fn settle_effect_capable_process_group(
    driver: &mut impl ProcessGroupSettlementDriver,
) -> io::Result<()> {
    driver.signal(SIGTERM)?;
    if matches!(
        observe_effect_capable_group_until(driver, TERMINATION_GRACE),
        BoundedGroupObservation::Gone
    ) {
        return Ok(());
    }

    driver.signal(SIGKILL)?;
    match observe_effect_capable_group_until(driver, ACTOR_SETTLEMENT_TIMEOUT) {
        BoundedGroupObservation::Gone => Ok(()),
        BoundedGroupObservation::Present => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "native process group retains effect-capable members after bounded termination",
        )),
        BoundedGroupObservation::Uncertain(error) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "native process-group membership remained uncertain after bounded termination: {error}"
            ),
        )),
    }
}

#[cfg(all(unix, any(not(target_os = "linux"), test)))]
fn observe_effect_capable_group_until(
    driver: &mut impl ProcessGroupSettlementDriver,
    deadline: Duration,
) -> BoundedGroupObservation {
    let mut backoff = INITIAL_SETTLEMENT_BACKOFF;
    loop {
        let last = match driver.observe() {
            Ok(EffectCapableGroupObservation::Gone) => return BoundedGroupObservation::Gone,
            Ok(EffectCapableGroupObservation::Present) => BoundedGroupObservation::Present,
            Err(error) => BoundedGroupObservation::Uncertain(error.to_string()),
        };
        let remaining = deadline.saturating_sub(driver.elapsed());
        if remaining.is_zero() {
            return last;
        }
        driver.wait(remaining.min(backoff));
        backoff = (backoff * 2).min(MAX_SETTLEMENT_BACKOFF);
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
struct PinnedProcessGroupSettlement {
    process_group_id: i32,
    started: Instant,
}

#[cfg(all(unix, not(target_os = "linux")))]
impl PinnedProcessGroupSettlement {
    fn new(process_group_id: i32) -> Self {
        Self {
            process_group_id,
            started: Instant::now(),
        }
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
impl ProcessGroupSettlementDriver for PinnedProcessGroupSettlement {
    fn observe(&mut self) -> io::Result<EffectCapableGroupObservation> {
        non_linux_process_group_observation(self.process_group_id)
    }

    fn signal(&mut self, signal: i32) -> io::Result<()> {
        send_process_group_signal_checked(-self.process_group_id, signal)
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn wait(&mut self, duration: Duration) {
        std::thread::sleep(duration);
    }
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

#[cfg(target_os = "macos")]
fn non_linux_process_group_observation(
    process_group_id: i32,
) -> io::Result<EffectCapableGroupObservation> {
    const MEMBER_SLACK: usize = 16;
    const MEMBER_LIMIT: usize = 4_096;

    let required = unsafe { libc::proc_listpgrppids(process_group_id, std::ptr::null_mut(), 0) };
    if required < 0 {
        return Err(io::Error::last_os_error());
    }
    if required == 0 {
        return if process_group_is_live(process_group_id as u32) {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "macOS returned no membership records for a live process group",
            ))
        } else {
            Ok(EffectCapableGroupObservation::Gone)
        };
    }
    let required = usize::try_from(required).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "macOS returned a negative process-group member count",
        )
    })?;
    let capacity = required.checked_add(MEMBER_SLACK).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "macOS process-group member count overflowed",
        )
    })?;
    if capacity > MEMBER_LIMIT {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "macOS process group requires more than the bounded {MEMBER_LIMIT}-member observation capacity"
            ),
        ));
    }
    let byte_capacity = capacity
        .checked_mul(std::mem::size_of::<libc::pid_t>())
        .and_then(|bytes| i32::try_from(bytes).ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "macOS process-group observation buffer exceeds the platform range",
            )
        })?;
    let mut process_ids = vec![0_i32; capacity];
    let observed = unsafe {
        libc::proc_listpgrppids(
            process_group_id,
            process_ids.as_mut_ptr().cast(),
            byte_capacity,
        )
    };
    if observed < 0 {
        return Err(io::Error::last_os_error());
    }
    let observed = usize::try_from(observed).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "macOS returned a negative process-group observation count",
        )
    })?;
    if observed >= capacity {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "macOS process-group membership changed beyond the bounded observation buffer",
        ));
    }

    for process_id in process_ids
        .into_iter()
        .take(observed)
        .filter(|pid| *pid > 0)
    {
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
        if read_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "macOS process-group member {process_id} disappeared or became unreadable during observation"
                ),
            ));
        }
        if read_size != info_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("macOS process-group member {process_id} returned a partial record"),
            ));
        }
        let info = unsafe { info.assume_init() };
        if info.pbi_pid != process_id as u32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "macOS process-group observation returned a mismatched process identity",
            ));
        }
        if info.pbi_pgid == process_group_id as u32 && info.pbi_status != libc::SZOMB {
            return Ok(EffectCapableGroupObservation::Present);
        }
    }
    Ok(EffectCapableGroupObservation::Gone)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn non_linux_process_group_observation(
    _process_group_id: i32,
) -> io::Result<EffectCapableGroupObservation> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "effect-capable process-group observation is unsupported on this Unix platform",
    ))
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
    deadline: Instant,
) -> io::Result<()> {
    let snapshot = linux_process_group_snapshot_before(actor.process_group_id, deadline)?;
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
#[derive(Debug)]
enum LinuxDurableRecoveryProvenance<'a> {
    Vacant,
    Recycled,
    RecordedLeader(&'a LinuxProcess),
}

#[cfg(target_os = "linux")]
fn linux_durable_recovery_provenance<'a>(
    actor: &ProcessGroupActor,
    snapshot: &'a [LinuxProcess],
) -> io::Result<LinuxDurableRecoveryProvenance<'a>> {
    if snapshot.is_empty() {
        return Ok(LinuxDurableRecoveryProvenance::Vacant);
    }
    let Some(leader) = snapshot
        .iter()
        .find(|process| process.process_id == actor.process_group_id)
    else {
        return Err(unresolved_actor_ownership(
            "durable recovery ownership is unresolved: the recorded process-group leader incarnation is absent and no restart-surviving kernel containment identity is available",
        ));
    };
    if linux_incarnation(leader.start_ticks)? != actor.incarnation {
        return Ok(LinuxDurableRecoveryProvenance::Recycled);
    }
    Ok(LinuxDurableRecoveryProvenance::RecordedLeader(leader))
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
        let snapshot = linux_process_group_snapshot_before(actor.process_group_id, deadline)?;
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
            let verification =
                linux_process_group_snapshot_before(actor.process_group_id, deadline)?;
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
    let stat = match fs::read(format!("/proc/{process_id}/stat")) {
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
trait LinuxProcessTable {
    fn process_ids(&mut self) -> io::Result<Vec<u32>>;
    fn read_stat(&mut self, process_id: u32) -> io::Result<Option<Vec<u8>>>;
}

#[cfg(target_os = "linux")]
struct ProcLinuxProcessTable;

#[cfg(target_os = "linux")]
impl LinuxProcessTable for ProcLinuxProcessTable {
    fn process_ids(&mut self) -> io::Result<Vec<u32>> {
        let mut process_ids = Vec::new();
        for entry in fs::read_dir("/proc")? {
            let entry = entry?;
            let Some(process_id) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            process_ids.push(process_id);
        }
        Ok(process_ids)
    }

    fn read_stat(&mut self, process_id: u32) -> io::Result<Option<Vec<u8>>> {
        match fs::read(format!("/proc/{process_id}/stat")) {
            Ok(stat) => Ok(Some(stat)),
            Err(error) if linux_process_disappeared(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(all(target_os = "linux", test))]
fn linux_process_group_snapshot(process_group_id: u32) -> io::Result<Vec<LinuxProcess>> {
    linux_process_group_snapshot_before(process_group_id, Instant::now() + ACTOR_SETTLEMENT_TIMEOUT)
}

#[cfg(target_os = "linux")]
fn linux_process_group_snapshot_before(
    process_group_id: u32,
    deadline: Instant,
) -> io::Result<Vec<LinuxProcess>> {
    linux_process_group_snapshot_from(&mut ProcLinuxProcessTable, process_group_id, deadline)
}

#[cfg(target_os = "linux")]
fn linux_process_group_snapshot_from(
    process_table: &mut impl LinuxProcessTable,
    process_group_id: u32,
    deadline: Instant,
) -> io::Result<Vec<LinuxProcess>> {
    let mut backoff = INITIAL_SETTLEMENT_BACKOFF;
    let mut last_error = None;
    for attempt in 0..LINUX_SNAPSHOT_MAX_ATTEMPTS {
        match linux_process_group_snapshot_once(process_table, process_group_id) {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) => last_error = Some(error),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || attempt + 1 == LINUX_SNAPSHOT_MAX_ATTEMPTS {
            break;
        }
        std::thread::sleep(remaining.min(backoff));
        backoff = (backoff * 2).min(MAX_SETTLEMENT_BACKOFF);
    }
    let error = last_error
        .map(|error| error.to_string())
        .unwrap_or_else(|| "no complete process-table observation".to_string());
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        format!(
            "Linux target-group membership remained uncertain after bounded process-table observations: {error}"
        ),
    ))
}

#[cfg(target_os = "linux")]
fn linux_process_group_snapshot_once(
    process_table: &mut impl LinuxProcessTable,
    process_group_id: u32,
) -> io::Result<Vec<LinuxProcess>> {
    let mut snapshot = Vec::new();
    for process_id in process_table.process_ids()? {
        let Some(stat) = process_table.read_stat(process_id)? else {
            continue;
        };
        let fields = parse_linux_process_stat_fields(process_id, &stat)?;
        if parse_linux_stat_field::<u32>(&fields, 2, "process group")? != process_group_id {
            continue;
        }
        snapshot.push(parse_linux_process_stat_from_fields(process_id, &fields)?);
    }
    Ok(snapshot)
}

#[cfg(target_os = "linux")]
fn parse_linux_process_stat(process_id: u32, stat: &[u8]) -> io::Result<LinuxProcess> {
    let fields = parse_linux_process_stat_fields(process_id, stat)?;
    parse_linux_process_stat_from_fields(process_id, &fields)
}

#[cfg(target_os = "linux")]
fn parse_linux_process_stat_fields(process_id: u32, stat: &[u8]) -> io::Result<Vec<&[u8]>> {
    let process_id = process_id.to_string();
    let command_start = process_id.len() + 2;
    if !stat.starts_with(process_id.as_bytes())
        || stat.get(process_id.len()..command_start) != Some(b" (")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat identity does not match its procfs path",
        ));
    }
    let command_end = stat.iter().rposition(|byte| *byte == b')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat has no command terminator",
        )
    })?;
    if command_end < command_start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat command terminates before it begins",
        ));
    }
    if stat.get(command_end + 1) != Some(&b' ') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat command terminator is not followed by a field separator",
        ));
    }
    Ok(stat[command_end + 1..]
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|field| !field.is_empty())
        .collect())
}

#[cfg(target_os = "linux")]
fn parse_linux_process_stat_from_fields(
    process_id: u32,
    fields: &[&[u8]],
) -> io::Result<LinuxProcess> {
    let state = fields
        .first()
        .filter(|state| state.len() == 1)
        .map(|state| state[0])
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "process stat has no state"))?;
    let process_group_id = parse_linux_stat_field::<u32>(fields, 2, "process group")?;
    let start_ticks = parse_linux_stat_field::<u64>(fields, 19, "start time")?;
    Ok(LinuxProcess {
        process_id,
        process_group_id,
        state,
        start_ticks,
    })
}

#[cfg(target_os = "linux")]
fn parse_linux_stat_field<T>(fields: &[&[u8]], index: usize, name: &str) -> io::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let field = fields.get(index).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process stat has no {name} field"),
        )
    })?;
    std::str::from_utf8(field)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        .parse::<T>()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process stat has invalid {name} field: {error}"),
            )
        })
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
pub(crate) fn terminate_process_group_child(child: &mut Child) -> io::Result<Option<ExitStatus>> {
    let actor = actor_for_child(child)?;
    terminate_process_group_actor_with_child(&actor, child)
}

#[cfg(not(unix))]
pub(crate) fn terminate_process_group_child(_child: &mut Child) -> io::Result<Option<ExitStatus>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process-group child settlement requires Unix process custody",
    ))
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
    let process = read_linux_process(process_id)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "process disappeared before its incarnation could be observed",
        )
    })?;
    linux_incarnation(process.start_ticks)
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
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    enum InjectedStatRead {
        Stat(Vec<u8>),
        Missing,
        Error(io::ErrorKind),
    }

    struct InjectedLinuxProcessTable {
        process_ids: Vec<u32>,
        stat_reads: BTreeMap<u32, VecDeque<InjectedStatRead>>,
        read_counts: BTreeMap<u32, usize>,
    }

    impl LinuxProcessTable for InjectedLinuxProcessTable {
        fn process_ids(&mut self) -> io::Result<Vec<u32>> {
            Ok(self.process_ids.clone())
        }

        fn read_stat(&mut self, process_id: u32) -> io::Result<Option<Vec<u8>>> {
            *self.read_counts.entry(process_id).or_default() += 1;
            let reads = self
                .stat_reads
                .get_mut(&process_id)
                .expect("injected process stat sequence");
            let read = if reads.len() > 1 {
                reads.pop_front().expect("injected process stat read")
            } else {
                reads.front().expect("retained process stat read").clone()
            };
            match read {
                InjectedStatRead::Stat(stat) => Ok(Some(stat)),
                InjectedStatRead::Missing => Ok(None),
                InjectedStatRead::Error(kind) => {
                    Err(io::Error::new(kind, "injected transient stat read failure"))
                }
            }
        }
    }

    fn injected_linux_stat(
        process_id: u32,
        command: &[u8],
        state: u8,
        process_group_id: u32,
        start_ticks: u64,
    ) -> Vec<u8> {
        let mut fields = vec![b"0".to_vec(); 20];
        fields[0] = vec![state];
        fields[1] = b"1".to_vec();
        fields[2] = process_group_id.to_string().into_bytes();
        fields[19] = start_ticks.to_string().into_bytes();
        let mut stat = format!("{process_id} (").into_bytes();
        stat.extend_from_slice(command);
        stat.extend_from_slice(b") ");
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                stat.push(b' ');
            }
            stat.extend_from_slice(field);
        }
        stat.push(b'\n');
        stat
    }

    #[derive(Clone)]
    enum InjectedGroupObservation {
        Gone,
        Present,
        Error(&'static str),
    }

    struct InjectedSettlementDriver {
        after_term: VecDeque<InjectedGroupObservation>,
        after_kill: VecDeque<InjectedGroupObservation>,
        signals: Vec<i32>,
        elapsed: Duration,
    }

    impl InjectedSettlementDriver {
        fn observation(queue: &mut VecDeque<InjectedGroupObservation>) -> InjectedGroupObservation {
            if queue.len() > 1 {
                queue.pop_front().expect("injected group observation")
            } else {
                queue.front().expect("retained group observation").clone()
            }
        }
    }

    impl ProcessGroupSettlementDriver for InjectedSettlementDriver {
        fn observe(&mut self) -> io::Result<EffectCapableGroupObservation> {
            let observation = if self.signals.last() == Some(&SIGKILL) {
                Self::observation(&mut self.after_kill)
            } else {
                Self::observation(&mut self.after_term)
            };
            match observation {
                InjectedGroupObservation::Gone => Ok(EffectCapableGroupObservation::Gone),
                InjectedGroupObservation::Present => Ok(EffectCapableGroupObservation::Present),
                InjectedGroupObservation::Error(message) => Err(io::Error::other(message)),
            }
        }

        fn signal(&mut self, signal: i32) -> io::Result<()> {
            self.signals.push(signal);
            Ok(())
        }

        fn elapsed(&self) -> Duration {
            self.elapsed
        }

        fn wait(&mut self, duration: Duration) {
            self.elapsed += duration;
        }
    }

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
    fn linux_stat_parser_accepts_arbitrary_command_bytes_and_delimiters() {
        let stat = injected_linux_stat(
            700,
            b"name with spaces ) parens\n and invalid utf8 \xff(",
            b'S',
            701,
            9_876_543,
        );
        let process = parse_linux_process_stat(700, &stat).expect("parse valid unusual stat");

        assert_eq!(process.process_id, 700);
        assert_eq!(process.process_group_id, 701);
        assert_eq!(process.state, b'S');
        assert_eq!(process.start_ticks, 9_876_543);
    }

    #[test]
    fn linux_snapshot_retries_transient_unrelated_records_without_partial_proof() {
        let target_process = 800;
        let unrelated_process = 801;
        let disappeared_process = 802;
        let mut process_table = InjectedLinuxProcessTable {
            process_ids: vec![unrelated_process, disappeared_process, target_process],
            stat_reads: BTreeMap::from([
                (
                    unrelated_process,
                    VecDeque::from([
                        InjectedStatRead::Error(io::ErrorKind::PermissionDenied),
                        InjectedStatRead::Stat(injected_linux_stat(
                            unrelated_process,
                            b"unrelated ) name\n\xff",
                            b'R',
                            900,
                            11,
                        )),
                    ]),
                ),
                (
                    disappeared_process,
                    VecDeque::from([InjectedStatRead::Missing]),
                ),
                (
                    target_process,
                    VecDeque::from([InjectedStatRead::Stat(injected_linux_stat(
                        target_process,
                        b"target (worker)",
                        b'S',
                        target_process,
                        22,
                    ))]),
                ),
            ]),
            read_counts: BTreeMap::new(),
        };

        let snapshot = linux_process_group_snapshot_from(
            &mut process_table,
            target_process,
            Instant::now() + Duration::from_secs(1),
        )
        .expect("retry complete target-group snapshot");

        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].process_id, target_process);
        assert_eq!(process_table.read_counts[&unrelated_process], 2);
        assert_eq!(process_table.read_counts[&disappeared_process], 1);
        assert_eq!(process_table.read_counts[&target_process], 1);
    }

    #[test]
    fn linux_snapshot_fails_closed_after_bounded_persistent_uncertainty() {
        let uncertain_process = 810;
        let mut process_table = InjectedLinuxProcessTable {
            process_ids: vec![uncertain_process],
            stat_reads: BTreeMap::from([(
                uncertain_process,
                VecDeque::from([InjectedStatRead::Error(io::ErrorKind::PermissionDenied)]),
            )]),
            read_counts: BTreeMap::new(),
        };

        let error = linux_process_group_snapshot_from(
            &mut process_table,
            811,
            Instant::now() + Duration::from_secs(1),
        )
        .expect_err("persistent uncertainty must not return partial membership proof");

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(error.to_string().contains("membership remained uncertain"));
        assert_eq!(
            process_table.read_counts[&uncertain_process],
            LINUX_SNAPSHOT_MAX_ATTEMPTS
        );
    }

    #[test]
    fn bounded_group_settlement_requires_post_kill_terminality_proof() {
        let mut driver = InjectedSettlementDriver {
            after_term: VecDeque::from([InjectedGroupObservation::Present]),
            after_kill: VecDeque::from([
                InjectedGroupObservation::Present,
                InjectedGroupObservation::Gone,
            ]),
            signals: Vec::new(),
            elapsed: Duration::ZERO,
        };

        settle_effect_capable_process_group(&mut driver)
            .expect("post-kill observation proves terminality");

        assert_eq!(driver.signals, vec![SIGTERM, SIGKILL]);
        assert!(driver.elapsed >= TERMINATION_GRACE);
        assert!(driver.elapsed < ACTOR_SETTLEMENT_TIMEOUT);
    }

    #[test]
    fn bounded_group_settlement_fails_closed_for_surviving_members() {
        let mut driver = InjectedSettlementDriver {
            after_term: VecDeque::from([InjectedGroupObservation::Present]),
            after_kill: VecDeque::from([InjectedGroupObservation::Present]),
            signals: Vec::new(),
            elapsed: Duration::ZERO,
        };

        let error = settle_effect_capable_process_group(&mut driver)
            .expect_err("surviving member prevents settlement");

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(error.to_string().contains("retains effect-capable members"));
        assert_eq!(driver.signals, vec![SIGTERM, SIGKILL]);
        assert_eq!(driver.elapsed, ACTOR_SETTLEMENT_TIMEOUT);
    }

    #[test]
    fn bounded_group_settlement_fails_closed_for_observation_uncertainty() {
        let mut driver = InjectedSettlementDriver {
            after_term: VecDeque::from([InjectedGroupObservation::Error("term uncertainty")]),
            after_kill: VecDeque::from([InjectedGroupObservation::Error("kill uncertainty")]),
            signals: Vec::new(),
            elapsed: Duration::ZERO,
        };

        let error = settle_effect_capable_process_group(&mut driver)
            .expect_err("uncertainty prevents terminality proof");

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(error.to_string().contains("kill uncertainty"));
        assert_eq!(driver.signals, vec![SIGTERM, SIGKILL]);
        assert_eq!(driver.elapsed, ACTOR_SETTLEMENT_TIMEOUT);
    }

    #[test]
    fn bounded_group_settlement_avoids_kill_after_terminality_is_proven() {
        let mut driver = InjectedSettlementDriver {
            after_term: VecDeque::from([InjectedGroupObservation::Gone]),
            after_kill: VecDeque::from([InjectedGroupObservation::Present]),
            signals: Vec::new(),
            elapsed: Duration::ZERO,
        };

        settle_effect_capable_process_group(&mut driver)
            .expect("term observation proves terminality");

        assert_eq!(driver.signals, vec![SIGTERM]);
        assert_eq!(driver.elapsed, Duration::ZERO);
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
    fn durable_recovery_rejects_a_leaderless_recycled_group_before_signalling() {
        let process_group_id = 900;
        let actor = ProcessGroupActor {
            process_group_id,
            incarnation: linux_incarnation(100).expect("record original leader incarnation"),
        };
        let original_group = vec![
            LinuxProcess {
                process_id: process_group_id,
                process_group_id,
                state: b'S',
                start_ticks: 100,
            },
            LinuxProcess {
                process_id: 901,
                process_group_id,
                state: b'S',
                start_ticks: 101,
            },
        ];
        assert!(matches!(
            linux_durable_recovery_provenance(&actor, &original_group)
                .expect("classify original group"),
            LinuxDurableRecoveryProvenance::RecordedLeader(_)
        ));

        let replacement_group = vec![
            LinuxProcess {
                process_id: process_group_id,
                process_group_id,
                state: b'S',
                start_ticks: 200,
            },
            LinuxProcess {
                process_id: 902,
                process_group_id,
                state: b'S',
                start_ticks: 201,
            },
        ];
        assert!(matches!(
            linux_durable_recovery_provenance(&actor, &replacement_group)
                .expect("classify replacement group while its leader is live"),
            LinuxDurableRecoveryProvenance::Recycled
        ));

        let leaderless_replacement = vec![LinuxProcess {
            process_id: 902,
            process_group_id,
            state: b'S',
            start_ticks: 201,
        }];
        let error = linux_durable_recovery_provenance(&actor, &leaderless_replacement)
            .expect_err("leaderless replacement ownership must remain unresolved");

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(actor_cleanup_ownership_is_unresolved(&error));
        assert!(error.to_string().contains("leader incarnation is absent"));
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
