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
/// current failure count, the failure category, and any
/// server-supplied `Retry-After` hint.
///
/// Algorithm:
///   computed = min(initial * multiplier^(failures - 1), max)
///   result   = max(computed, server_hint)
///
/// `Timeout` and `Other` get a 0.5x discount on the initial because
/// they're often transient (network blip, slow server) rather than
/// the server actively refusing us; 429/503/ConnectReset run at full
/// strength.
///
/// `server_hint`, when present, acts as a floor: we never undercut
/// what the server told us to wait, but we still apply our own
/// (possibly harsher) backoff if the failure pattern has been
/// repeated. Capping at `max_backoff` is also applied to the
/// server hint to bound malicious or buggy servers that send
/// `Retry-After: 99999`.
pub fn compute_backoff(
    consecutive_failures: u32,
    kind: FailureKind,
    server_hint: Option<Duration>,
    policy: &BackoffPolicy,
) -> Duration {
    let computed = if consecutive_failures == 0 {
        Duration::ZERO
    } else {
        let base = policy.initial_backoff.as_secs_f64();
        let scale = match kind {
            FailureKind::Timeout | FailureKind::Other => 0.5,
            _ => 1.0,
        };
        let exponent = (consecutive_failures - 1) as i32;
        let backoff_secs = base * scale * policy.multiplier.powi(exponent);
        Duration::try_from_secs_f64(backoff_secs.max(0.0))
            .unwrap_or(Duration::ZERO)
            .min(policy.max_backoff)
    };

    match server_hint {
        Some(hint) => computed.max(hint.min(policy.max_backoff)),
        None => computed,
    }
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
            compute_backoff(1, FailureKind::TooManyRequests, None, &p),
            Duration::from_secs(30),
        );
    }

    #[test]
    fn backoff_doubles_each_consecutive_failure() {
        let p = policy();
        assert_eq!(
            compute_backoff(2, FailureKind::TooManyRequests, None, &p),
            Duration::from_secs(60),
        );
        assert_eq!(
            compute_backoff(3, FailureKind::TooManyRequests, None, &p),
            Duration::from_secs(120),
        );
        assert_eq!(
            compute_backoff(4, FailureKind::TooManyRequests, None, &p),
            Duration::from_secs(240),
        );
    }

    #[test]
    fn backoff_caps_at_max() {
        let p = policy();
        // 30s * 2^9 = 15360s; far above max_backoff (600s).
        assert_eq!(
            compute_backoff(10, FailureKind::TooManyRequests, None, &p),
            Duration::from_secs(600),
        );
    }

    #[test]
    fn timeout_gets_softer_initial_backoff() {
        let p = policy();
        // 30s * 0.5 = 15s.
        assert_eq!(
            compute_backoff(1, FailureKind::Timeout, None, &p),
            Duration::from_secs(15),
        );
    }

    #[test]
    fn zero_failures_means_zero_backoff() {
        let p = policy();
        assert_eq!(
            compute_backoff(0, FailureKind::TooManyRequests, None, &p),
            Duration::ZERO,
        );
    }

    #[test]
    fn server_hint_acts_as_floor() {
        let p = policy();
        // Computed backoff for 1 failure is 30s; hint of 60s should win.
        assert_eq!(
            compute_backoff(
                1,
                FailureKind::TooManyRequests,
                Some(Duration::from_secs(60)),
                &p,
            ),
            Duration::from_secs(60),
        );
    }

    #[test]
    fn computed_wins_when_harsher_than_hint() {
        let p = policy();
        // 5th failure: 30 * 2^4 = 480s. Hint of 10s is much smaller; ours wins.
        assert_eq!(
            compute_backoff(
                5,
                FailureKind::TooManyRequests,
                Some(Duration::from_secs(10)),
                &p,
            ),
            Duration::from_secs(480),
        );
    }

    #[test]
    fn server_hint_is_capped_at_max_backoff() {
        let p = policy();
        // Hint of 99999s; max is 600s. Cap kicks in even on the hint.
        assert_eq!(
            compute_backoff(
                1,
                FailureKind::TooManyRequests,
                Some(Duration::from_secs(99_999)),
                &p,
            ),
            Duration::from_secs(600),
        );
    }

    #[test]
    fn zero_failures_with_hint_uses_only_hint() {
        let p = policy();
        // No failures recorded but server says retry-after 5s.
        assert_eq!(
            compute_backoff(
                0,
                FailureKind::TooManyRequests,
                Some(Duration::from_secs(5)),
                &p,
            ),
            Duration::from_secs(5),
        );
    }
}
