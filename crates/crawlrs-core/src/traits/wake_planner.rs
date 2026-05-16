//! Wake-time planning port: per-host next-fetch scheduling math.
//!
//! One of the three sub-trait splits of `Politeness`. After a
//! fetch completes, the planner computes when the host's next
//! fetch is allowed (now + host_delay, modulo per-domain
//! overrides). The frontier owns wake-time *storage* (the wake
//! ZSET); this trait only computes the plan and returns it.
//!
//! Rate-limiting *enforcement* is split across two places, neither
//! of which goes through this trait:
//!
//! - **Fetch-time pacing**: the runtime writes the plan via
//!   `Frontier::advance_wake`; the frontier's claim loop gates
//!   subsequent claims of the host until the wake-time elapses.
//! - **Submit-time quota**: the frontier's `submit_batch.lua`
//!   enforces the per-host `[crawl].max_urls` cap atomically
//!   alongside the bloom check.
//!
//! The planner's one job is to produce a `NextWake` plan; the
//! runtime + frontier do the rest.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::NextWake;

/// Per-host wake-time planner. Produces a `NextWake` plan after
/// a successful fetch; the runtime applies the plan via
/// `Frontier::advance_wake`.
#[async_trait]
pub trait WakePlanner: Send + Sync {
    /// A fetch just completed. Returns the wake-time plan for the
    /// host: the earliest next-fetch time given the configured
    /// host-delay (plus any per-domain override). The runtime
    /// writes the plan; this trait does not.
    async fn record_fetch(&self, host: &str) -> Result<NextWake>;
}
