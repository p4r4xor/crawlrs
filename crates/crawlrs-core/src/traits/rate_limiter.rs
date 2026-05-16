//! Rate-limiting port: per-host wake-time gating.
//!
//! One of the three sub-trait splits of `Politeness`. The
//! aggregate `Politeness::check` runs four sub-checks (blocklist,
//! backoff, robots, rate) and delegates the rate-gating one here.
//! Keeping the sub-traits independent means a deployment can swap
//! the rate limiter in isolation (e.g. an in-memory test double
//! while leaving robots and backoff against real Redis).

use async_trait::async_trait;

use crate::error::Result;
use crate::traits::politeness::PoliteDecision;
use crate::types::NextWake;
use crate::url::CanonicalUrl;

/// Per-host pacing. Decides whether the host's next-fetch window
/// has opened and records the wake-time plan after a fetch
/// completes. Wake-time *storage* belongs to the frontier; this
/// trait only computes what to store.
#[async_trait]
pub trait RateLimiter: Send + Sync {
    /// May this URL be fetched right now per the configured
    /// host-delay (plus any per-domain override)? Returns
    /// `Allow` when the host's wake-time has elapsed, `Disallow`
    /// when it hasn't.
    ///
    /// Errors propagate from the backing store (e.g. Redis being
    /// unreachable). The runtime treats an error as "do not
    /// fetch right now"; silently dropping it risks over-fetching.
    async fn check(&self, url: &CanonicalUrl) -> Result<PoliteDecision>;

    /// A fetch just completed. Returns the wake-time plan the
    /// runtime should apply to the host: the earliest next-fetch
    /// time given the configured host-delay (plus any per-domain
    /// override). The runtime applies the plan via
    /// `Frontier::advance_wake`; this trait does not write.
    async fn record_fetch(&self, host: &str) -> Result<NextWake>;
}
