//! Politeness types and trait.
//!
//! The politeness layer gates fetches per host: minimum delay between
//! requests, robots.txt enforcement, exponential backoff on 429/503,
//! per-host circuit breakers. The trait surface here is the contract;
//! the Redis-backed impl lives in `crawlrs-politeness`.

use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::error::Result;
use crate::url::CanonicalUrl;

/// Politeness gate decision for a single URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoliteDecision {
    /// Safe to fetch right now.
    Allow,
    /// Wait at least this long before fetching.
    Delay(Duration),
    /// Disallowed (robots.txt, host on a deny-list, etc.).
    Disallow,
}

/// Why a fetch failed. The politeness layer cares about the *category*
/// of failure to decide backoff strategy, not the underlying error.
///
/// Mapped from HTTP status codes and transport errors at the boundary
/// between `Fetcher` and `Politeness::record_failure`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// HTTP 429. Server is explicitly rate-limiting; honor `Retry-After`
    /// if present, otherwise apply exponential backoff per host.
    TooManyRequests,
    /// HTTP 503. Server is overloaded or down for maintenance; same
    /// category as 429 for backoff, sometimes with a `Retry-After`.
    ServiceUnavailable,
    /// Transport-level reset (TCP RST, broken pipe). Often indicates
    /// the server is dropping us; back off conservatively.
    ConnectReset,
    /// We gave up waiting on the server. May indicate overload, or just
    /// network slowness; backoff is appropriate but milder than 429.
    Timeout,
    /// Anything else (DNS failure, TLS error, malformed response). Logged
    /// but does not necessarily trigger per-host backoff on its own.
    Other,
}

#[async_trait]
pub trait Politeness: Send + Sync {
    /// May this URL be fetched right now? Honors per-host wake-time,
    /// 429/503 backoff, robots.txt, and any per-domain overrides.
    ///
    /// Errors are propagated from the backing store (e.g. Redis being
    /// unreachable). The runtime should treat an error as "do not
    /// fetch right now" and retry the check; silently dropping the
    /// error would risk over-fetching when the backend recovers.
    async fn check(&self, url: &CanonicalUrl) -> Result<PoliteDecision>;

    /// A successful fetch just completed. Implementations use this to
    /// update per-host last-fetched timestamps so the next `check` for
    /// this host applies the configured delay.
    ///
    /// Returns `Result` because a backend write may fail; callers
    /// should retry rather than swallow, otherwise the next check
    /// after a transient outage would over-fetch the host.
    async fn record_fetch(&self, url: &CanonicalUrl) -> Result<()>;

    /// A fetch failed. Implementations use this to apply per-host
    /// exponential backoff on rate-limit categories (429/503) and to
    /// open per-host circuits after repeated transport failures.
    ///
    /// `retry_after` carries a server-supplied hint when one was
    /// present (HTTP `Retry-After` header, RFC 9110 §10.2.3). When
    /// present, implementations honor it as a *floor*: the next
    /// allowed time is `max(server_hint, computed_backoff)`. Servers
    /// know best how long they need to recover; we don't undercut
    /// them, but we still apply our own backoff if it's harsher
    /// (e.g., after the 5th consecutive 503 we may want longer than
    /// the 5-second hint).
    async fn record_failure(
        &self,
        url: &CanonicalUrl,
        kind: FailureKind,
        retry_after: Option<Duration>,
    ) -> Result<()>;

    /// Soonest moment any host this instance tracks becomes claimable.
    /// Lets the runtime sleep precisely until then instead of
    /// busy-polling every host on every tick.
    ///
    /// Returns `Ok(None)` if the politeness layer has no scheduled work
    /// (every host is currently free, or no hosts are tracked yet).
    /// Implementations typically back this with a time-ordered
    /// structure keyed on host (sorted set, delay-queue, etc.) so the
    /// answer is O(log N) or better.
    async fn next_ready_at(&self) -> Result<Option<Instant>>;

    // Note: there is intentionally no trait-level method for a
    // robots-only decision. `check` already runs the robots gate as
    // part of the full decision; impls that want to expose a
    // robots-only debugging entry point can do so via inherent
    // methods on their concrete type.
}
