//! `ManualClock`: a [`Clock`] test double whose value advances only
//! when tests explicitly call [`ManualClock::advance_ms`].
//!
//! The production [`Clock`] impl reads the OS monotonic clock; tests
//! that need to drive time-dependent behavior (visibility timeouts,
//! XAUTOCLAIM idle thresholds, supervisor reset windows) instead pin
//! the clock to a known value, exercise the path, advance, and
//! re-exercise. Backing storage is `AtomicU64` so the double is
//! cheap to share across tasks via `Arc`.

use std::sync::atomic::{AtomicU64, Ordering};

use crawlrs_core::Clock;

/// A clock whose `now_ms()` is whatever the test most recently set.
///
/// Construction takes the initial epoch-millis value; tests usually
/// pick a round number well clear of zero so off-by-one bugs are
/// visible. Use [`Self::advance_ms`] to step forward; the type is
/// not designed to step backward (real clocks don't, and the tests
/// that motivate this helper want a forward-only sequence).
#[derive(Debug, Default)]
pub struct ManualClock {
    ms: AtomicU64,
}

impl ManualClock {
    /// Pin the clock at `start_ms`.
    pub fn new(start_ms: u64) -> Self {
        Self {
            ms: AtomicU64::new(start_ms),
        }
    }

    /// Advance the clock by `delta` milliseconds. Tests that need to
    /// model "wall time elapsed past T" call this between actions.
    pub fn advance_ms(&self, delta: u64) {
        self.ms.fetch_add(delta, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.ms.load(Ordering::Relaxed)
    }
}
