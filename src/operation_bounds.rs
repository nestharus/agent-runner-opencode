//! Declared roles: validator, orchestration

use crate::encoding::now_unix_ms;
use agent_provider_contract::UnixMilliseconds;
use std::fs::File;
use std::io;
use std::thread;
use std::time::{Duration, Instant};

const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) fn remaining_timeout(
    deadline_unix_ms: Option<&UnixMilliseconds>,
    maximum: Duration,
) -> Option<Duration> {
    remaining_at(deadline_unix_ms, now_unix_ms(), maximum)
}

pub(crate) fn remaining_at(
    deadline_unix_ms: Option<&UnixMilliseconds>,
    now_unix_ms: u64,
    maximum: Duration,
) -> Option<Duration> {
    match deadline_unix_ms {
        Some(deadline) => deadline.remaining_at(now_unix_ms, maximum),
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
        assert_eq!(
            remaining_timeout(
                Some(&UnixMilliseconds::new(0.into()).unwrap()),
                Duration::from_secs(20)
            ),
            None
        );
    }
    #[test]
    fn exact_deadline_remaining_at() {
        for token in ["80", "80.0", "8e1", "800e-1"] {
            let deadline = UnixMilliseconds::new(serde_json::from_str(token).unwrap()).unwrap();
            for (now, expected) in [
                (79, Some(Duration::from_millis(1))),
                (80, None),
                (100, None),
            ] {
                let actual = remaining_at(Some(&deadline), now, Duration::from_millis(20));
                println!(
                    "deadline={} now={now} max_ms=20 actual={actual:?}",
                    deadline.as_number()
                );
                assert_eq!(actual, expected);
            }
        }
        for token in [
            "1000",
            "18446744073709551616",
            "18446744073709551617",
            "1e1000001",
            "1e999999999999999999999",
        ] {
            let deadline = UnixMilliseconds::new(serde_json::from_str(token).unwrap()).unwrap();
            let actual = remaining_at(Some(&deadline), 100, Duration::from_millis(20));
            println!(
                "deadline={} now=100 max_ms=20 actual={actual:?}",
                deadline.as_number()
            );
            assert_eq!(actual, Some(Duration::from_millis(20)));
            assert_eq!(
                remaining_at(Some(&deadline), 100, Duration::ZERO),
                Some(Duration::ZERO)
            );
            assert_eq!(
                remaining_at(Some(&deadline), 100, Duration::from_nanos(999_999)),
                Some(Duration::ZERO)
            );
        }
        for token in ["0", "-0.0", "-0e-999999999999999999999"] {
            let deadline = UnixMilliseconds::new(serde_json::from_str(token).unwrap()).unwrap();
            assert_eq!(remaining_at(Some(&deadline), 0, Duration::ZERO), None);
        }
        for maximum in [
            Duration::ZERO,
            Duration::from_nanos(999_999),
            Duration::from_millis(20),
        ] {
            assert_eq!(remaining_at(None, 100, maximum), Some(maximum));
        }
    }
}
