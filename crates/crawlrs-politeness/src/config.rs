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
    /// Default delay between consecutive fetches to the same host.
    /// Per-domain overrides supersede this.
    #[serde(with = "humantime_serde")]
    pub min_delay: Duration,

    /// Whether to consult robots.txt before fetching. Per-domain
    /// overrides supersede this (e.g. for our own staging hosts).
    pub honor_robots_txt: bool,

    /// How long to keep a fetched robots.txt cached.
    /// 24 h is the long-standing convention.
    #[serde(with = "humantime_serde")]
    pub robots_cache_ttl: Duration,

    /// User-agent string used when fetching robots.txt. Should match
    /// the UA used by the main fetcher; some sites serve a different
    /// robots.txt to different bots.
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
    pub manual_excludes: HashSet<String>,

    /// Per-domain overrides keyed by registrable domain. Resolution is
    /// exact-match in v1; glob/regex matching can be added without
    /// changing the type if a real use case appears.
    pub per_domain: HashMap<String, PolitenessOverride>,
}

impl Default for PolitenessConfig {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_secs(1),
            honor_robots_txt: true,
            robots_cache_ttl: Duration::from_secs(24 * 60 * 60),
            user_agent: "crawlrs/0.0.1 (+https://github.com/p4r4xor/crawlrs)".into(),
            backoff: BackoffPolicy::default(),
            manual_excludes: HashSet::new(),
            per_domain: HashMap::new(),
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
    pub min_delay: Option<Duration>,
    pub honor_robots_txt: Option<bool>,
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
    pub circuit_open_after_failures: u32,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(600),
            multiplier: 2.0,
            circuit_open_after_failures: 10,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sensible() {
        let c = PolitenessConfig::default();
        assert_eq!(c.min_delay, Duration::from_secs(1));
        assert!(c.honor_robots_txt);
        assert!(c.manual_excludes.is_empty());
        assert!(c.per_domain.is_empty());
    }

    #[test]
    fn override_default_inherits_everything() {
        let o = PolitenessOverride::default();
        assert_eq!(o.min_delay, None);
        assert_eq!(o.honor_robots_txt, None);
    }

    #[test]
    fn backoff_default_caps_at_ten_minutes() {
        let p = BackoffPolicy::default();
        assert_eq!(p.max_backoff, Duration::from_secs(600));
        assert!(p.multiplier > 1.0);
    }
}
