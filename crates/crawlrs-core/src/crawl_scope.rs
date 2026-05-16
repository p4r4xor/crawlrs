//! Operator-mandated crawl scope: per-host depth and URL caps.
//!
//! Distinct from politeness (which is behavior toward a host as a
//! guest). Scope is the operator's view of *their own crawl's
//! shape*: how deep to follow links, how many URLs to take from
//! any one host. Static config loaded at process start; no I/O.

use std::collections::HashMap;

/// Per-host overrides for [`CrawlScope`]. Each field is
/// `Option` so a per-domain entry can override only one cap and
/// fall back to the global default for the other.
#[derive(Debug, Clone, Default)]
pub struct CrawlOverride {
    pub max_depth: Option<u32>,
    pub max_urls: Option<u64>,
}

/// Crawl-scope config. Construct once from the parsed `[crawl]`
/// table and pass into the runtime (worker) and the frontier
/// (submit-time quota check) by reference or by clone.
///
/// Resolution rule for both `depth_cap` and `max_urls_for`:
/// per-domain value wins if set; else the global value; else
/// `None` (uncapped).
#[derive(Debug, Clone, Default)]
pub struct CrawlScope {
    max_depth: Option<u32>,
    max_urls: Option<u64>,
    per_domain: HashMap<String, CrawlOverride>,
}

impl CrawlScope {
    pub fn new(
        max_depth: Option<u32>,
        max_urls: Option<u64>,
        per_domain: HashMap<String, CrawlOverride>,
    ) -> Self {
        Self {
            max_depth,
            max_urls,
            per_domain,
        }
    }

    /// Effective depth cap for the host: per-domain override if
    /// set, else the global default, else `None` (unbounded).
    pub fn depth_cap(&self, host: &str) -> Option<u32> {
        if let Some(override_) = self.per_domain.get(host)
            && let Some(d) = override_.max_depth
        {
            return Some(d);
        }
        self.max_depth
    }

    /// Effective per-host URL cap. Same resolution rule as
    /// `depth_cap`. The cap is enforced atomically at submit time
    /// by the frontier (a per-host counter incremented in the
    /// submit Lua script); the runtime only reads the cap to
    /// pass it through.
    pub fn max_urls_for(&self, host: &str) -> Option<u64> {
        if let Some(override_) = self.per_domain.get(host)
            && let Some(n) = override_.max_urls
        {
            return Some(n);
        }
        self.max_urls
    }

    /// `true` if any host has a URL-count quota configured (global
    /// or any per-domain). Lets callers fast-path the "no quotas
    /// anywhere" case to zero work.
    pub fn has_any_quota(&self) -> bool {
        self.max_urls.is_some()
            || self
                .per_domain
                .values()
                .any(|override_| override_.max_urls.is_some())
    }
}
