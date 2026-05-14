//! Configuration: defaults + per-domain overrides + backoff policy.
//!
//! All time-valued fields are [`Duration`]; `humantime_serde` lets
//! TOML use human-readable forms like `"1s"`, `"30m"`, `"24h"`.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Top-level politeness config. Defaults are sensible for a generic
/// crawl; tighten per-host via [`PolitenessConfig::per_domain`] when a
/// site needs special handling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PolitenessConfig {
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

    /// Hosts we never want to crawl, regardless of any other rule.
    /// Loaded from a file at startup typically; the runtime adds
    /// to this set if it observes runtime indicators (e.g. operator
    /// emails the project, gets the host blacklisted live).
    pub blocklist: HashSet<String>,

    /// Per-domain overrides keyed by registrable domain. Resolution is
    /// exact-match in v1; glob/regex matching can be added without
    /// changing the type if a real use case appears.
    pub per_domain: HashMap<String, PolitenessOverride>,

    /// Global default depth cap. `None` (the default) leaves crawls
    /// uncapped; setting a value applies it as the baseline for every
    /// host that doesn't have an explicit `per_domain.<host>.max_depth`
    /// override. Depth caps are pure config (no I/O), so the unset
    /// case costs nothing.
    pub max_depth: Option<u32>,

    /// Global default cap on URLs successfully fetched per host. `None`
    /// (the default) leaves crawls uncapped; setting a value applies
    /// it as a baseline to every host that doesn't have an explicit
    /// `per_domain.<host>.max_urls` override. Per-host Redis counter
    /// storage is only allocated when at least one URL cap is
    /// configured (global or per-host), so the unset case costs zero
    /// Redis bytes.
    pub max_urls: Option<u64>,
}

impl Default for PolitenessConfig {
    fn default() -> Self {
        Self {
            host_delay: Duration::from_secs(1),
            obey_robots_txt: true,
            robots_ttl: Duration::from_secs(24 * 60 * 60),
            user_agent: "crawlrs/0.0.1 (+https://github.com/p4r4xor/crawlrs)".into(),
            backoff: BackoffPolicy::default(),
            blocklist: HashSet::new(),
            per_domain: HashMap::new(),
            max_depth: None,
            max_urls: None,
        }
    }
}

/// Per-domain overrides of the global politeness defaults.
///
/// Every field is `Option`; `None` means "inherit the default".
/// The struct intentionally only carries politeness-layer settings;
/// per-component overrides for fetcher / proxy / extraction live in
/// their own crates per CLAUDE.md §1.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PolitenessOverride {
    #[serde(default, with = "humantime_serde::option")]
    pub host_delay: Option<Duration>,
    pub obey_robots_txt: Option<bool>,
    /// Per-host override for `PolitenessConfig::max_depth`. The
    /// worker's link-follow gate uses `override.max_depth.or(global)`,
    /// so leaving this `None` falls back to the global default
    /// (itself `None` = unbounded out of the box).
    pub max_depth: Option<u32>,
    /// Per-host override for `PolitenessConfig::max_urls`. `None`
    /// inherits the global default; both `None` leaves the host
    /// uncapped.
    pub max_urls: Option<u64>,
}

/// Exponential backoff parameters for failure recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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
