//! Backoff calculation for per-host failure state.
//!
//! The actual failure state lives in Redis (per-shard
//! `hoststate:{host}` Hash); this module's job is the math that turns
//! "failures so far + kind + optional Retry-After hint" into "how long
//! until next allowed."

use std::time::Duration;

use crawlrs_core::FailureKind;

use crate::config::BackoffPolicy;

/// Decide how far in the future the next fetch is allowed, given the
/// current failure count, the failure category, and any
/// server-supplied `Retry-After` hint.
///
/// Algorithm:
///   computed = min(initial * multiplier^(failures - 1), max)
///   result   = max(computed, server_hint)
///
/// `TooManyRequests`, `ServiceUnavailable`, and `ConnectReset` run at
/// full strength because the server is actively pushing back. Every
/// other variant gets a 0.5x discount on the initial because the
/// failure is often transient (timeout, DNS hiccup, TLS retry) or
/// our-side (resource exhaustion) rather than the server refusing us.
///
/// `server_hint`, when present, acts as a floor: we never undercut
/// what the server told us to wait, but we still apply our own
/// (possibly harsher) backoff if the failure pattern has been
/// repeated. Capping at `max_backoff` is also applied to the
/// server hint to bound malicious or buggy servers that send
/// `Retry-After: 99999`.
#[must_use]
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
            FailureKind::TooManyRequests
            | FailureKind::ServiceUnavailable
            | FailureKind::ConnectReset => 1.0,
            // Everything else is transient or our-side; the gentler
            // 0.5x preserves the original semantics for `Timeout` /
            // `Other` and applies the same logic to the newly-split
            // variants (DNS / TLS / Unreachable / ResourceExhausted /
            // NotFound / ClientError / ServerError).
            _ => 0.5,
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
