//! Per-host politeness, robots.txt cache, and failure backoff.
//!
//! [`CompositePoliteness`] satisfies [`crawlrs_core::Politeness`]
//! as a policy layer: it answers `check` (allow / disallow) and
//! returns `NextWake` plans from `record_fetch` / `record_failure`.
//! Internally it composes three sub-trait impls plus two
//! pure-config sub-services:
//!
//! - [`RedisWakePlanner`] implements
//!   [`crawlrs_core::WakePlanner`]: produces the `NextWake` plan
//!   from the configured `host_delay` plus any per-domain
//!   override. The frontier owns wake-time storage and quota
//!   enforcement; the planner is pure math.
//! - [`RedisRobotsChecker`] implements
//!   [`crawlrs_core::RobotsChecker`]: thin wrapper around the
//!   in-process LRU + Redis-cached robots.txt fetcher.
//! - [`RedisBackoffTracker`] implements
//!   [`crawlrs_core::BackoffTracker`]: per-host failure counter
//!   in Redis (`hoststate:{host}` Hash), exponential-backoff math
//!   via [`compute_backoff`], and the per-host circuit breaker
//!   exposed via `is_open`.
//! - [`crawlrs_core::Blocklist`] (in-memory `HashSet<String>`):
//!   sync first-gate exclusion.
//!
//! Robots fetching bypasses the politeness gate (you can't ask
//! permission to ask for permission). The constructor takes
//! `Arc<dyn Fetcher>` so the runtime can compose the same
//! `WreqFetcher` used for normal fetches; tests use a fake.
//!
//! [`Fetcher`]: crawlrs_core::Fetcher
//! [`crawlrs_core::Politeness`]: crawlrs_core::Politeness

pub mod backoff_tracker;
pub mod composite;
pub mod config;
pub mod failure;
pub mod keys;
pub mod metrics;
pub mod noop;
pub mod robots;
pub mod robots_checker;
pub mod wake_planner;

pub(crate) mod error;

pub use composite::CompositePoliteness;
pub use config::{BackoffPolicy, PolitenessConfig, PolitenessOverride};
pub use failure::compute_backoff;
pub use keys::KeyPrefix;
pub use noop::{NoopBackoffTracker, NoopRobotsChecker, NoopWakePlanner};
