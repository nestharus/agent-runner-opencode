//! Direct-child cleanup ownership from spawn through wait/reap.

use std::io::{self, Read};
use std::process::{Child, Output};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

pub(crate) struct ChildCustody {
    child: Option<Child>,
    cleanup: fn(&mut Child),
}

impl ChildCustody {
    pub(crate) fn new(child: Child) -> Self {
        Self::with_cleanup(child, terminate_direct_child)
    }

    pub(crate) fn with_cleanup(child: Child, cleanup: fn(&mut Child)) -> Self {
        Self {
            child: Some(child),
            cleanup,
        }
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("child custody is active")
    }

    pub(crate) fn child_ref(&self) -> Option<&Child> {
        self.child.as_ref()
    }

    pub(crate) fn wait_with_output_timeout(
        mut self,
        timeout: Duration,
    ) -> io::Result<Option<Output>> {
        self.child_mut().stdin.take();
        let stdout = self.child_mut().stdout.take().map(spawn_drain);
        let stderr = self.child_mut().stderr.take().map(spawn_drain);
        let status = match self.child_mut().wait_timeout(timeout) {
            Ok(Some(status)) => {
                self.child.take();
                Some(status)
            }
            Ok(None) => {
                self.cleanup_now();
                None
            }
            Err(error) => {
                self.cleanup_now();
                let _ = join_drain(stdout);
                let _ = join_drain(stderr);
                return Err(error);
            }
        };
        let stdout = join_drain(stdout)?;
        let stderr = join_drain(stderr)?;
        Ok(status.map(|status| Output {
            status,
            stdout,
            stderr,
        }))
    }

    pub(crate) fn wait_with_bounded_output_timeout(
        mut self,
        timeout: Duration,
        maximum_stdout_bytes: usize,
        maximum_stderr_bytes: usize,
    ) -> io::Result<Option<Output>> {
        let started = Instant::now();
        self.child_mut().stdin.take();
        let stdout = self
            .child_mut()
            .stdout
            .take()
            .map(|reader| spawn_bounded_drain(reader, maximum_stdout_bytes));
        let stderr = self
            .child_mut()
            .stderr
            .take()
            .map(|reader| spawn_bounded_drain(reader, maximum_stderr_bytes));
        let status = match self.child_mut().wait_timeout(timeout) {
            Ok(Some(status)) => Some(status),
            Ok(None) => {
                self.cleanup_now();
                return Ok(None);
            }
            Err(error) => {
                self.cleanup_now();
                return Err(error);
            }
        };
        let Some(stdout) = join_drain_before(stdout, started, timeout)? else {
            self.cleanup_now();
            return Ok(None);
        };
        let Some(stderr) = join_drain_before(stderr, started, timeout)? else {
            self.cleanup_now();
            return Ok(None);
        };
        self.child.take();
        Ok(status.map(|status| Output {
            status,
            stdout,
            stderr,
        }))
    }

    fn cleanup_now(&mut self) {
        if let Some(child) = self.child.as_mut() {
            (self.cleanup)(child);
        }
        self.child.take();
    }
}

impl Drop for ChildCustody {
    fn drop(&mut self) {
        self.cleanup_now();
    }
}

fn spawn_drain<R: Read + Send + 'static>(mut reader: R) -> JoinHandle<io::Result<Vec<u8>>> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(bytes)
    })
}

fn spawn_bounded_drain<R: Read + Send + 'static>(
    mut reader: R,
    maximum_bytes: usize,
) -> JoinHandle<io::Result<Vec<u8>>> {
    thread::spawn(move || {
        let retained_limit = maximum_bytes.saturating_add(1);
        let mut retained = Vec::new();
        let mut buffer = [0_u8; 8 * 1024];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let remaining = retained_limit.saturating_sub(retained.len());
            retained.extend_from_slice(&buffer[..read.min(remaining)]);
        }
        Ok(retained)
    })
}

fn join_drain(drain: Option<JoinHandle<io::Result<Vec<u8>>>>) -> io::Result<Vec<u8>> {
    match drain {
        Some(drain) => drain
            .join()
            .map_err(|_| io::Error::other("child output drain panicked"))?,
        None => Ok(Vec::new()),
    }
}

fn join_drain_before(
    drain: Option<JoinHandle<io::Result<Vec<u8>>>>,
    started: Instant,
    timeout: Duration,
) -> io::Result<Option<Vec<u8>>> {
    let Some(drain) = drain else {
        return Ok(Some(Vec::new()));
    };
    while !drain.is_finished() {
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Ok(None);
        }
        thread::sleep(remaining.min(Duration::from_millis(1)));
    }
    drain
        .join()
        .map_err(|_| io::Error::other("child output drain panicked"))?
        .map(Some)
}

fn terminate_direct_child(child: &mut Child) {
    match child.try_wait() {
        Ok(Some(_)) => {
            let _ = child.wait();
        }
        _ => {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn bounded_drain_consumes_the_stream_but_retains_only_one_over_the_limit() {
        let drain = spawn_bounded_drain(Cursor::new(vec![b'x'; 64 * 1024]), 32);
        let retained = join_drain(Some(drain)).expect("bounded drain");
        assert_eq!(retained.len(), 33);
    }
}
