//! Configuration: defaults + per-domain overrides + backoff policy.
//!
//! Politeness in this crate is behavior toward a host *as a guest*:
//! pacing (`host_delay`), robots.txt honoring, exponential backoff
//! on failures, and a master switch (`enabled`) that flips the
//! whole layer to no-op when an operator is crawling infrastructure
//! they own. Operator-mandated scope (per-host depth + URL caps)
//! and access exclusion (blocklist) are separate concerns owned by
//! `crawlrs_core::CrawlScope` and `crawlrs_core::Blocklist`, and
//! the factory wires them alongside this config.
//!
//! All time-valued fields are [`Duration`]; `humantime_serde` lets
//! TOML use human-readable forms like `"1s"`, `"30m"`, `"24h"`.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Top-level politeness config. Defaults are sensible for a generic
/// crawl; tighten per-host via [`PolitenessConfig::per_domain`] when a
/// site needs special handling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolitenessConfig {
    /// Master switch. When `false`, the factory wires the
    /// politeness layer's three sub-traits to no-op impls and the
    /// runtime sees a `Politeness` that allows every URL with no
    /// Redis I/O. The `host_delay`, `obey_robots_txt`, `robots_ttl`,
    /// `backoff`, and `per_domain` fields are ignored in that
    /// state.
    pub enabled: bool,

    /// Floor on the delay between consecutive fetches to the same host.
    /// The effective wait can be longer when the server's Crawl-Delay
    /// directive or a Retry-After header pushes it out, but never
    /// shorter. Per-domain overrides supersede this.
    #[serde(with = "humantime_serde")]
    pub host_delay: Duration,

    /// Whether to consult robots.txt before fetching. Per-domain
    /// overrides supersede this (e.g. for our own staging hosts).
    pub obey_robots_txt: bool,

    /// How long to keep a fetched robots.txt cached.
    /// 24 h is the long-standing convention.
    #[serde(with = "humantime_serde")]
    pub robots_ttl: Duration,

    /// Product token used for robots.txt rule matching (RFC 9309).
    /// This is *local* matching against `User-Agent:` directives in a
    /// fetched robots.txt; the value never goes on the wire. Most
    /// operators set this to the same string as the wire UA so
    /// the on-wire identity and the robots-respecting identity agree.
    /// The default is a polite identifier with a contact URL so site
    /// owners can find us if they need to.
    pub user_agent: String,

    /// Backoff parameters used when [`record_failure`] reports a
    /// 429 / 503 / transport error.
    ///
    /// [`record_failure`]: crawlrs_core::Politeness::record_failure
    pub backoff: BackoffPolicy,

    /// Per-domain overrides keyed by registrable domain. Resolution is
    /// exact-match in v1; glob/regex matching can be added without
    /// changing the type if a real use case appears.
    pub per_domain: HashMap<String, PolitenessOverride>,
}

impl Default for PolitenessConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host_delay: Duration::from_secs(1),
            obey_robots_txt: true,
            robots_ttl: Duration::from_secs(24 * 60 * 60),
            user_agent: "crawlrs/0.0.1 (+https://github.com/p4r4xor/crawlrs)".into(),
            backoff: BackoffPolicy::default(),
            per_domain: HashMap::new(),
        }
    }
}

impl PolitenessConfig {
    /// `true` when at least one host has a non-zero `host_delay`
    /// configured (the global default, or any `per_domain`
    /// override). `false` means every effective delay resolves
    /// to zero, so callers can skip per-host lookups.
    ///
    /// Read once at wake-planner construction; the planner caches
    /// the verdict and uses it to short-circuit the per-host
    /// delay lookup on the hot path. See `RedisWakePlanner` and
    /// the worker's `apply_wake_plan` for the downstream
    /// `Frontier::advance_wake` skip when this returns `false`.
    #[must_use]
    pub fn has_host_delay(&self) -> bool {
        if !self.host_delay.is_zero() {
            return true;
        }
        self.per_domain
            .values()
            .any(|o| o.host_delay.is_some_and(|d| !d.is_zero()))
    }
}

/// Per-domain overrides of the global politeness defaults.
///
/// Every field is `Option`; `None` means "inherit the default".
/// The struct intentionally only carries politeness-layer settings;
/// scope-level overrides (per-host `max_depth` / `max_urls`) live
/// on `crawlrs_core::CrawlOverride` under the `[crawl]` config
/// table; access-level overrides will live under `[access]`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolitenessOverride {
    #[serde(default, with = "humantime_serde::option")]
    pub host_delay: Option<Duration>,
    pub obey_robots_txt: Option<bool>,
    #[serde(default, with = "humantime_serde::option")]
    pub robots_ttl: Option<Duration>,
}

/// Exponential backoff parameters for failure recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackoffPolicy {
    /// Backoff after the first failure. Multiplied by `multiplier`
    /// for each subsequent consecutive failure.
    #[serde(with = "humantime_serde")]
    pub initial_backoff: Duration,

    /// Hard ceiling on backoff. Without this, repeated failures push
    /// the next-allowed time arbitrarily far into the future.
    #[serde(with = "humantime_serde")]
    pub max_backoff: Duration,

    /// Each consecutive failure multiplies the backoff by this.
    /// 2.0 doubles each time; 1.5 grows more gently.
    pub multiplier: f64,

    /// After this many consecutive failures, the host's circuit is
    /// considered "open" and `check` returns `Disallow` rather than
    /// `Delay`. The runtime can probe with a manual reset later.
    pub failure_threshold: u32,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(600),
            multiplier: 2.0,
            failure_threshold: 10,
        }
    }
}
