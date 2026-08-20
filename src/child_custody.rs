//! Direct-child cleanup ownership from spawn through wait/reap.

use std::io::{self, Read};
use std::process::{Child, Output};
use std::thread::{self, JoinHandle};
use std::time::Duration;
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

    pub(crate) fn wait_with_output(mut self) -> io::Result<Output> {
        self.child_mut().stdin.take();
        let stdout = self.child_mut().stdout.take().map(spawn_drain);
        let stderr = self.child_mut().stderr.take().map(spawn_drain);
        let status = self.child_mut().wait();
        if status.is_err() {
            self.cleanup_now();
        }
        let stdout = join_drain(stdout)?;
        let stderr = join_drain(stderr)?;
        let status = status?;
        self.child.take();
        Ok(Output {
            status,
            stdout,
            stderr,
        })
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

fn join_drain(drain: Option<JoinHandle<io::Result<Vec<u8>>>>) -> io::Result<Vec<u8>> {
    match drain {
        Some(drain) => drain
            .join()
            .map_err(|_| io::Error::other("child output drain panicked"))?,
        None => Ok(Vec::new()),
    }
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
