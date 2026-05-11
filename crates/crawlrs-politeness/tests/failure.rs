//! Tests for `compute_backoff` - the math that turns "failures + kind +
//! Retry-After hint" into "how long until next allowed."

use std::time::Duration;

use crawlrs_core::FailureKind;
use crawlrs_politeness::{BackoffPolicy, compute_backoff};

fn policy() -> BackoffPolicy {
    BackoffPolicy {
        initial_backoff: Duration::from_secs(30),
        max_backoff: Duration::from_secs(600),
        multiplier: 2.0,
        failure_threshold: 10,
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
