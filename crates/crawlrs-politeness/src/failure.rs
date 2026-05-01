//! Per-host failure state and exponential backoff calculation.

use std::time::Duration;

use crawlrs_core::FailureKind;

use crate::config::BackoffPolicy;

/// Per-host failure tracking. Stored as a Redis Hash; this struct is
/// the in-memory mirror.
#[derive(Debug, Clone, Default)]
pub struct FailureState {
    /// Number of consecutive failures since the last successful fetch.
    /// Reset to 0 on `record_fetch`.
    pub consecutive_failures: u32,

    /// Wall-clock millis after which this host is allowed to be
    /// fetched again. Computed from `consecutive_failures` and the
    /// configured backoff policy. Stored as a raw integer because
    /// it goes straight into Redis as a ZSET score.
    pub backoff_until_ms: u64,

    /// The last failure category, kept for debugging. Influences only
    /// the *initial* backoff multiplier (e.g. transport resets get a
    /// shorter first wait than 429s).
    pub last_kind: Option<FailureKind>,
}

/// Decide how far in the future the next fetch is allowed, given the
/// current failure count and the failure category.
///
/// Algorithm: `min(initial * multiplier^(failures - 1), max)`.
/// `Timeout` and `Other` get a 0.5x discount on the initial because
/// they're often transient (network blip, slow server) rather than
/// the server actively refusing us; 429/503/ConnectReset run at full
/// strength.
pub fn compute_backoff(
    consecutive_failures: u32,
    kind: FailureKind,
    policy: &BackoffPolicy,
) -> Duration {
    if consecutive_failures == 0 {
        return Duration::ZERO;
    }
    let base = policy.initial_backoff.as_secs_f64();
    let scale = match kind {
        FailureKind::Timeout | FailureKind::Other => 0.5,
        _ => 1.0,
    };
    let exponent = (consecutive_failures - 1) as i32;
    let backoff_secs = base * scale * policy.multiplier.powi(exponent);
    let backoff = Duration::try_from_secs_f64(backoff_secs.max(0.0))
        .unwrap_or(Duration::ZERO);
    backoff.min(policy.max_backoff)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> BackoffPolicy {
        BackoffPolicy {
            initial_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(600),
            multiplier: 2.0,
            circuit_open_after_failures: 10,
        }
    }

    #[test]
    fn first_failure_uses_initial_backoff() {
        let p = policy();
        assert_eq!(
            compute_backoff(1, FailureKind::TooManyRequests, &p),
            Duration::from_secs(30),
        );
    }

    #[test]
    fn backoff_doubles_each_consecutive_failure() {
        let p = policy();
        assert_eq!(
            compute_backoff(2, FailureKind::TooManyRequests, &p),
            Duration::from_secs(60),
        );
        assert_eq!(
            compute_backoff(3, FailureKind::TooManyRequests, &p),
            Duration::from_secs(120),
        );
        assert_eq!(
            compute_backoff(4, FailureKind::TooManyRequests, &p),
            Duration::from_secs(240),
        );
    }

    #[test]
    fn backoff_caps_at_max() {
        let p = policy();
        // 30s * 2^9 = 15360s; far above max_backoff (600s).
        assert_eq!(
            compute_backoff(10, FailureKind::TooManyRequests, &p),
            Duration::from_secs(600),
        );
    }

    #[test]
    fn timeout_gets_softer_initial_backoff() {
        let p = policy();
        // 30s * 0.5 = 15s.
        assert_eq!(
            compute_backoff(1, FailureKind::Timeout, &p),
            Duration::from_secs(15),
        );
    }

    #[test]
    fn zero_failures_means_zero_backoff() {
        let p = policy();
        assert_eq!(
            compute_backoff(0, FailureKind::TooManyRequests, &p),
            Duration::ZERO,
        );
    }
}
