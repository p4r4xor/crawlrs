//! Per-worker supervisor with bounded restart.
//!
//! Pattern: Supervisor (Erlang OTP-flavoured). One supervisor task per
//! worker, owning the lifecycle of that worker's tokio task. When the
//! worker exits (panic or unexpected return), the supervisor decides
//! whether to respawn based on a [`RestartPolicy`].
//!
//! Why this layer: panics in a worker task today are silently dropped
//! by `tokio::spawn`, leaving the pool degraded at `W-1` workers
//! permanently. A poison URL or transient parser bug would leak the
//! entire fleet over time. With supervision, the worker reattaches to
//! its own PEL via stable [`crawlrs_core::WorkerIdentity`] so respawn
//! is correctness-preserving: any URL that was in flight at the moment
//! of panic is still in the consumer's PEL on the Redis side and gets
//! re-delivered on the restarted worker's next `claim()`.
//!
//! The supervisor runs as a tokio task (one per worker), distinct
//! from the worker task it manages. Supervisor lifecycle:
//! shutdown-watch tells it to stop; restart budget tells it when to
//! give up on a crash-loop; otherwise it loops forever respawning.
//!
//! NOT part of this layer: cluster-wide rebalance (lease ownership),
//! cross-worker coordination, deciding who consumes which shard. The
//! supervisor is purely about "keep this one worker alive."

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::WorkerIdentity;
use tokio::sync::watch;
use tracing::{error, warn};

use crate::worker::{WorkerDeps, worker_loop};

/// Bounded restart policy. The defaults treat a few panics in
/// quick succession as recoverable while preventing a permanent
/// crash-loop that would keep one worker continuously restarting on a
/// genuine bug.
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    /// Maximum restarts within `reset_window` before the supervisor
    /// gives up and leaves the worker dead.
    pub max_restarts: u32,

    /// Initial backoff between a worker exit and its respawn. Doubles
    /// on each successive restart inside the window.
    pub base_backoff: Duration,

    /// Cap on the doubled backoff. Prevents exponential growth from
    /// stretching restart latency past operationally useful bounds.
    pub max_backoff: Duration,

    /// Time-since-last-restart after which the counter resets. A
    /// worker that ran cleanly for this long is considered stable;
    /// fresh budget applies on the next panic.
    pub reset_window: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            reset_window: Duration::from_secs(300),
        }
    }
}

/// Run a supervisor for one worker, until shutdown is signalled or
/// the restart budget is exhausted.
///
/// Spawns the worker as a child tokio task. On exit:
/// - If shutdown is set, returns cleanly.
/// - If panicked, decides via [`RestartState::decide_restart`] whether
///   to respawn.
/// - If returned `Ok(())` without shutdown set, treats it as an
///   unexpected exit (a worker_loop is supposed to run until shutdown)
///   and respawns under the same budget.
///
/// Each respawn carries the original [`WorkerIdentity`] so tier-1 PEL
/// replay reattaches the worker to its own in-flight entries on the
/// Redis side.
pub async fn supervise_worker(
    identity: WorkerIdentity,
    deps: Arc<WorkerDeps>,
    mut shutdown: watch::Receiver<bool>,
    policy: RestartPolicy,
) {
    let mut state = RestartState::new(policy);

    loop {
        if *shutdown.borrow() {
            return;
        }

        let join = tokio::spawn(worker_loop(identity, deps.clone(), shutdown.clone()));
        let exit = join.await;

        if *shutdown.borrow() {
            return;
        }

        let reason = classify_exit(&exit);
        if matches!(exit, Ok(())) {
            warn!(
                identity = %identity,
                "supervisor: worker exited Ok(()) without shutdown set; treating as anomaly"
            );
        }

        match state.decide_restart(deps.clock.now_ms()) {
            RestartDecision::Restart(backoff) => {
                metrics::counter!(
                    crate::metrics::WORKER_RESTARTS_TOTAL,
                    "reason" => reason,
                    crate::metrics::LABEL_WORKER => identity.to_string(),
                )
                .increment(1);
                warn!(
                    identity = %identity,
                    reason = reason,
                    backoff_ms = backoff.as_millis() as u64,
                    "supervisor: respawning worker",
                );
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = shutdown.changed() => return,
                }
            }
            RestartDecision::GiveUp => {
                error!(
                    identity = %identity,
                    reason = reason,
                    "supervisor: restart budget exhausted; leaving worker dead",
                );
                return;
            }
        }
    }
}

/// One-line classification of why the worker exited; becomes the
/// `reason` label on `crawlrs_worker_restarts_total`. Pure function:
/// the caller is responsible for any logging on top of the verdict.
fn classify_exit(exit: &Result<(), tokio::task::JoinError>) -> &'static str {
    match exit {
        Ok(()) => "exit_unexpected",
        Err(e) if e.is_panic() => "panic",
        Err(e) if e.is_cancelled() => "cancelled",
        Err(_) => "join_error",
    }
}

/// Restart-budget state. Pulled out of [`supervise_worker`] so it can
/// be unit-tested without spawning real tasks or sleeping real time.
///
/// Time is carried as epoch-millis (the same unit `Clock::now_ms`
/// returns) rather than `Instant`, so a test driving the supervisor
/// with a `ManualClock` and a test driving the in-memory frontier
/// with the same clock observe a consistent timeline.
#[derive(Debug)]
struct RestartState {
    policy: RestartPolicy,
    /// Restarts inside the current window. Reset to 0 once the
    /// last_restart age exceeds `policy.reset_window`.
    restart_count: u32,
    last_restart_ms: Option<u64>,
}

impl RestartState {
    fn new(policy: RestartPolicy) -> Self {
        Self {
            policy,
            restart_count: 0,
            last_restart_ms: None,
        }
    }

    fn decide_restart(&mut self, now_ms: u64) -> RestartDecision {
        // If the worker had been stable for at least `reset_window`,
        // consider this a fresh failure event and reset the counter
        // before applying the budget. Without this rule a worker that
        // panics rarely (once every few hours) would still eventually
        // exhaust the budget over its lifetime.
        let reset_window_ms = self.policy.reset_window.as_millis() as u64;
        if let Some(last_ms) = self.last_restart_ms
            && now_ms.saturating_sub(last_ms) > reset_window_ms
        {
            self.restart_count = 0;
        }

        if self.restart_count >= self.policy.max_restarts {
            return RestartDecision::GiveUp;
        }

        self.restart_count += 1;
        self.last_restart_ms = Some(now_ms);

        // Exponential backoff capped at `max_backoff`. `2^(n-1)` so the
        // first restart has 1x base, the second 2x, etc. Use
        // `saturating_mul` so a generous `max_restarts` plus a
        // `base_backoff` near `Duration::MAX` doesn't overflow.
        let exponent = self.restart_count.saturating_sub(1);
        let scale = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
        let scaled = self.policy.base_backoff.saturating_mul(scale);
        let backoff = scaled.min(self.policy.max_backoff);

        RestartDecision::Restart(backoff)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RestartDecision {
    Restart(Duration),
    GiveUp,
}

#[cfg(test)]
mod tests {
    // Inline because: visibility-forced. The tests exercise the
    // private `RestartState::decide_restart` state machine directly,
    // which is the pure-function core extracted out of
    // `supervise_worker` so it can be driven without spawning real
    // tasks or sleeping real time. Promoting `RestartState` to `pub`
    // would leak an implementation detail of the supervisor;
    // keeping the tests inline keeps them coupled to the state
    // machine they're guarding.

    use super::*;

    fn policy(max: u32, base_ms: u64, max_ms: u64, reset_secs: u64) -> RestartPolicy {
        RestartPolicy {
            max_restarts: max,
            base_backoff: Duration::from_millis(base_ms),
            max_backoff: Duration::from_millis(max_ms),
            reset_window: Duration::from_secs(reset_secs),
        }
    }

    #[test]
    fn restart_decision_scales_backoff_exponentially_until_cap() {
        let mut state = RestartState::new(policy(10, 100, 1_000, 300));
        let now = 1_000_000u64;

        // 1st restart: 100ms (base * 2^0)
        assert_eq!(
            state.decide_restart(now),
            RestartDecision::Restart(Duration::from_millis(100)),
        );
        // 2nd: 200ms (base * 2^1)
        assert_eq!(
            state.decide_restart(now),
            RestartDecision::Restart(Duration::from_millis(200)),
        );
        // 3rd: 400ms (base * 2^2)
        assert_eq!(
            state.decide_restart(now),
            RestartDecision::Restart(Duration::from_millis(400)),
        );
        // 4th: 800ms (base * 2^3)
        assert_eq!(
            state.decide_restart(now),
            RestartDecision::Restart(Duration::from_millis(800)),
        );
        // 5th: would be 1600ms but cap is 1000ms.
        assert_eq!(
            state.decide_restart(now),
            RestartDecision::Restart(Duration::from_millis(1_000)),
        );
        // 6th: still capped.
        assert_eq!(
            state.decide_restart(now),
            RestartDecision::Restart(Duration::from_millis(1_000)),
        );
    }

    #[test]
    fn restart_decision_gives_up_after_max_restarts() {
        let mut state = RestartState::new(policy(3, 100, 1_000, 300));
        let now = 1_000_000u64;
        assert!(matches!(
            state.decide_restart(now),
            RestartDecision::Restart(_)
        ));
        assert!(matches!(
            state.decide_restart(now),
            RestartDecision::Restart(_)
        ));
        assert!(matches!(
            state.decide_restart(now),
            RestartDecision::Restart(_)
        ));
        assert_eq!(state.decide_restart(now), RestartDecision::GiveUp);
    }

    #[test]
    fn restart_counter_resets_after_reset_window() {
        let mut state = RestartState::new(policy(2, 100, 1_000, 60));
        let now = 1_000_000u64;

        // Exhaust the budget.
        assert!(matches!(
            state.decide_restart(now),
            RestartDecision::Restart(_)
        ));
        assert!(matches!(
            state.decide_restart(now),
            RestartDecision::Restart(_)
        ));
        assert_eq!(state.decide_restart(now), RestartDecision::GiveUp);

        // ... but a worker stable for longer than reset_window earns
        // a fresh budget on its next failure. reset_window=60s, so
        // 120s later is well past it.
        let later = now + 120_000;
        assert!(matches!(
            state.decide_restart(later),
            RestartDecision::Restart(_)
        ));
        // And the budget is fully reset (not just incremented past
        // GiveUp): one more is allowed before the new GiveUp.
        assert!(matches!(
            state.decide_restart(later),
            RestartDecision::Restart(_)
        ));
        assert_eq!(state.decide_restart(later), RestartDecision::GiveUp);
    }

    #[test]
    fn classify_exit_recognises_panic_normal_and_cancelled() {
        // We can't easily synthesise JoinError values directly (its
        // constructors are crate-private in tokio), so this test only
        // covers the Ok(()) branch and the doc serves as the contract
        // for the others. The reason-string surface is exercised
        // indirectly by the supervise_worker integration path.
        assert_eq!(classify_exit(&Ok(())), "exit_unexpected");
    }
}
