//! Per-host politeness, robots.txt cache, and failure backoff.
//!
//! [`RedisPoliteness`] satisfies [`crawlrs_core::Politeness`] using the
//! same per-shard Redis keyspace shape as `crawlrs-frontier-redis`. Each
//! host has:
//!
//! - **One slot in a sorted set** (`hostsched`) keyed by host with score
//!   = next-allowed-fetch wall-clock millis. `next_ready_at` reads this
//!   set's smallest score; `record_fetch` writes a future score.
//! - **One Hash** (`hoststate:{host}`) with consecutive-failure counter,
//!   backoff-until-ms, and last failure category. Drives exponential
//!   backoff on 429/503/transport errors.
//! - **One Hash** (`robots:{host}`) caching the raw robots.txt body and
//!   its TTL, fetched on first need via the supplied [`Fetcher`].
//!
//! Robots fetching bypasses the politeness gate (you can't ask
//! permission to ask for permission). The constructor takes
//! `Arc<dyn Fetcher>` so the runtime can compose the same `WreqFetcher`
//! used for normal fetches; tests use a fake.
//!
//! Per-domain overrides (delay, robots opt-out) live in
//! [`PolitenessConfig`]; manual exclude list is on the same config.
//!
//! [`Fetcher`]: crawlrs_core::Fetcher
//! [`crawlrs_core::Politeness`]: crawlrs_core::Politeness

pub mod config;
pub mod failure;
pub mod keys;
pub mod politeness;
pub mod robots;

pub use config::{BackoffPolicy, PolitenessConfig, PolitenessOverride};
pub use failure::compute_backoff;
pub use keys::KeyPrefix;
pub use politeness::{RedisPoliteness, RedisPolitenessError};
pub use robots::RobotsCache;
