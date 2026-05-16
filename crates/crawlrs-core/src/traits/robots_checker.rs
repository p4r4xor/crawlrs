//! Robots.txt compliance port.
//!
//! One of the three sub-trait splits of `Politeness`. The
//! aggregate `Politeness::check` consults this trait when the
//! effective `obey_robots_txt` (global or per-domain) is true and
//! treats `false` as a `Disallow`. Robots-cache storage (TTL,
//! eviction) belongs to the impl, not the trait surface.

use async_trait::async_trait;

use crate::error::Result;
use crate::url::CanonicalUrl;

/// Does the host's robots.txt allow this URL for the crawler's
/// effective User-Agent? Impls own the parse-and-cache machinery;
/// the trait surface is one yes/no question.
#[async_trait]
pub trait RobotsChecker: Send + Sync {
    /// `Ok(true)` when the URL is allowed by the host's robots.txt
    /// (or when robots could not be fetched and the impl's fallback
    /// policy is permissive). `Ok(false)` is a hard refusal that
    /// the aggregate `Politeness` maps to `PoliteDecision::Disallow`.
    ///
    /// Errors propagate; treat them as "do not fetch" and retry.
    async fn allowed(&self, url: &CanonicalUrl) -> Result<bool>;
}
