//! Per-host politeness, robots.txt cache, and failure backoff.
//!
//! [`CompositePoliteness`] satisfies [`crawlrs_core::Politeness`]
//! as a policy layer: it answers `check` (allow / disallow) and
//! returns `NextWake` plans from `record_fetch` / `record_failure`.
//! Internally it composes three sub-trait impls plus two
//! pure-config sub-services:
//!
//! - [`RedisRateLimiter`] implements [`crawlrs_core::RateLimiter`]:
//!   per-host URL-count quota check + `record_fetch` increment +
//!   wake-time plan from the configured `host_delay`.
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
//! - [`crawlrs_core::CrawlScope`] (pure config): per-host
//!   `depth_cap` lookup.
//!
//! Robots fetching bypasses the politeness gate (you can't ask
//! permission to ask for permission). The constructor takes
//! `Arc<dyn Fetcher>` so the runtime can compose the same
//! `WreqFetcher` used for normal fetches; tests use a fake.
//!
//! `RedisPoliteness` is a backwards-compatible type alias for
//! `CompositePoliteness`. New code SHOULD use the latter; the
//! alias lets pre-decomposition call sites keep compiling.
//!
//! [`Fetcher`]: crawlrs_core::Fetcher
//! [`crawlrs_core::Politeness`]: crawlrs_core::Politeness

pub mod backoff_tracker;
pub mod composite;
pub mod config;
pub mod error;
pub mod failure;
pub mod keys;
pub mod metrics;
pub mod noop;
pub mod rate_limiter;
pub mod robots;
pub mod robots_checker;

pub use backoff_tracker::RedisBackoffTracker;
pub use composite::CompositePoliteness;
pub use config::{BackoffPolicy, PolitenessConfig, PolitenessOverride};
pub use error::RedisPolitenessError;
pub use failure::compute_backoff;
pub use keys::KeyPrefix;
pub use noop::{NoopBackoffTracker, NoopRateLimiter, NoopRobotsChecker};
pub use rate_limiter::RedisRateLimiter;
pub use robots::RobotsCache;
pub use robots_checker::RedisRobotsChecker;

/// Backwards-compatible alias for [`CompositePoliteness`].
///
/// The Redis-backed wiring is the production wiring, and pre-
/// decomposition call sites refer to `RedisPoliteness`. The
/// composite IS that wiring when constructed via
/// [`CompositePoliteness::new`]; the alias keeps those call
/// sites unchanged.
pub type RedisPoliteness = CompositePoliteness;
