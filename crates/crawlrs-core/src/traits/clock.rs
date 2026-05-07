//! `Clock` trait: an injectable time source.
//!
//! Pattern: Strategy. Production code uses [`SystemClock`] which
//! delegates to [`std::time::SystemTime::now`]. Tests inject an
//! advanceable manual clock (see `crawlrs-fakes::ManualClock`) so that
//! time-dependent invariants (XAUTOCLAIM idle thresholds, retry-after
//! windows, restart-budget reset windows) are deterministic.
//!
//! All timestamps are exposed as **wall-clock milliseconds since the
//! Unix epoch** (`u64`). Monotonic time is not modelled here; tokio's
//! own `tokio::time::pause` / `advance` is the right tool for
//! virtualising monotonic clocks. This abstraction is deliberately
//! narrow: a single method on a single shape, sufficient for adapter
//! impls that need a controllable wall-clock for idle / age decisions.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Source of wall-clock milliseconds. Implementations must be
/// `Send + Sync` so a single shared `Arc<dyn Clock>` can be threaded
/// through long-lived adapters and across worker tasks.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// Current wall-clock time, expressed as ms since the Unix epoch.
    fn now_ms(&self) -> u64;
}

/// The production clock: reads the OS wall-clock via
/// [`SystemTime::now`]. Stateless, cheap to construct.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            // Pre-epoch system clocks are not a thing we model; fall
            // back to 0 so the trait remains infallible in the
            // pathological "user set the clock to 1969" case.
            .unwrap_or(0)
    }
}

/// Convenience: a default-constructed `Arc<dyn Clock>` that reads the
/// system clock. Used by adapter constructors that take an optional
/// clock and fall back to wall-time when one isn't supplied.
pub fn system_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}
