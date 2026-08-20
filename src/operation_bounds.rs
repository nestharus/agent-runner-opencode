//! Declared roles: validator, orchestration

use crate::encoding::now_unix_ms;
use std::fs::File;
use std::io;
use std::thread;
use std::time::{Duration, Instant};

const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) fn remaining_timeout(
    deadline_unix_ms: Option<u64>,
    maximum: Duration,
) -> Option<Duration> {
    match deadline_unix_ms {
        Some(deadline) => {
            let remaining_ms = deadline.saturating_sub(now_unix_ms());
            (remaining_ms > 0)
                .then(|| Duration::from_millis(remaining_ms.min(maximum.as_millis() as u64)))
        }
        None => Some(maximum),
    }
}

pub(crate) fn lock_exclusive_for(lock: &File, timeout: Duration) -> io::Result<bool> {
    let started = Instant::now();
    loop {
        match fs2::FileExt::try_lock_exclusive(lock) {
            Ok(()) => return Ok(true),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Ok(false);
                }
                thread::sleep(remaining.min(LOCK_RETRY_INTERVAL));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_deadline_has_no_remaining_operation_time() {
        assert_eq!(remaining_timeout(Some(0), Duration::from_secs(20)), None);
    }
}
