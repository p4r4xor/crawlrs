//! Politeness types and trait.
//!
//! Per ADR-0020 the politeness layer is policy-only: it answers
//! "may this URL be fetched?" (`check`) and computes "given this
//! outcome, when should we next be allowed to touch this host?"
//! (`record_fetch` / `record_failure`). The *application* of the
//! plan; writing the wake-time so future claims see it; lives in
//! the frontier crate. Politeness owns: robots.txt, blocklist,
//! circuit breaker, exponential-backoff math. Politeness does NOT
//! own: per-host wake ZSET, ready LIST, lease tracking.

use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::types::NextWake;
use crate::url::CanonicalUrl;

/// Politeness gate decision for a single URL. Two states: the wake
/// time is enforced by the frontier (claim never returns a URL whose
/// host is still in the wake-window), so `check` only has to answer
/// "is this URL allowed at all?" (robots, blocklist, circuit-open).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoliteDecision {
    /// Safe to fetch right now.
    Allow,
    /// Disallowed (robots.txt, blocklist, circuit open).
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

impl FailureKind {
    /// Stable lowercase wire-format string for the variant. Used as
    /// the `kind` label value on failure-related metrics, and as a
    /// structured-log field. The strings are part of the operational
    /// contract: dashboards and alerts depend on them, so don't
    /// rename a variant's `as_str` without also updating consumers.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TooManyRequests => "too_many_requests",
            Self::ServiceUnavailable => "service_unavailable",
            Self::ConnectReset => "connect_reset",
            Self::Timeout => "timeout",
            Self::Other => "other",
        }
    }
}

#[async_trait]
pub trait Politeness: Send + Sync {
    /// May this URL be fetched right now? Honors robots.txt,
    /// blocklist, and any open per-host circuit breaker. Wake-time
    /// gating is the frontier's responsibility, not this method's.
    ///
    /// Errors are propagated from the backing store (e.g. Redis being
    /// unreachable). The runtime should treat an error as "do not
    /// fetch right now" and retry; silently dropping the error would
    /// risk over-fetching when the backend recovers.
    async fn check(&self, url: &CanonicalUrl) -> Result<PoliteDecision>;

    /// A successful fetch just completed. Returns the wake-time plan
    /// the runtime should apply via `Frontier::advance_wake`: the
    /// host's earliest next-allowed fetch given the configured
    /// host-delay (plus any robots.txt Crawl-Delay or per-domain
    /// override). Politeness does not write the plan itself; the
    /// frontier owns wake-time storage per ADR-0020.
    async fn record_fetch(&self, url: &CanonicalUrl) -> Result<NextWake>;

    /// A fetch failed. Returns the wake-time plan after applying
    /// exponential backoff for rate-limit categories (429/503), or
    /// the host-delay floor for the milder failure categories. Also
    /// increments the circuit-breaker counter; once over the
    /// configured threshold subsequent `check` calls for the host
    /// return `Disallow` until the next successful fetch resets it.
    ///
    /// `retry_after` carries a server-supplied hint when one was
    /// present (HTTP `Retry-After` header, RFC 9110 §10.2.3). When
    /// present, implementations honor it as a *floor*: the returned
    /// `NextWake.until` is `max(server_hint, computed_backoff)`.
    /// Servers know best how long they need to recover; we don't
    /// undercut them, but we still apply our own backoff if it's
    /// harsher (e.g., after the 5th consecutive 503 we may want
    /// longer than the 5-second hint).
    async fn record_failure(
        &self,
        url: &CanonicalUrl,
        kind: FailureKind,
        retry_after: Option<Duration>,
    ) -> Result<NextWake>;

    // Note: there is intentionally no trait-level method for a
    // robots-only decision. `check` already runs the robots gate as
    // part of the full decision; impls that want to expose a
    // robots-only debugging entry point can do so via inherent
    // methods on their concrete type.

    /// Effective per-host depth cap. Returns the per-host override if
    /// configured, otherwise the politeness layer's global default,
    /// otherwise `None` (unbounded). The runtime drops discovered
    /// links whose depth would exceed `Some(N)`; `None` lets them
    /// through. Sync because all inputs are in-memory config loaded
    /// at construction time; no I/O on this path.
    fn depth_cap(&self, host: &str) -> Option<u32>;
}
