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
/// of failure to decide backoff strategy; the runtime emits the same
/// category as a label on failure metrics so dashboards can break
/// failures down by cause without parsing free-form error strings.
///
/// Mapped from HTTP status codes and transport errors at the boundary
/// between `Fetcher` and `Politeness::record_failure`.
///
/// The variants split into two groups by how politeness treats them:
///
/// - **Server pushback** (`TooManyRequests`, `ServiceUnavailable`,
///   `ConnectReset`): the remote is actively refusing us. Full-strength
///   exponential backoff.
/// - **Transient / our-side** (everything else): often clears on its
///   own, or the remote isn't the cause. Backoff at half strength.
///
/// `Other` is the catch-all when the underlying error didn't match any
/// known signature. A growing `Other` count is a signal to inspect raw
/// error strings and extend the classifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// HTTP 429. Server is explicitly rate-limiting; honor `Retry-After`
    /// if present, otherwise apply exponential backoff per host.
    TooManyRequests,
    /// HTTP 503. Server is overloaded or down for maintenance; same
    /// category as 429 for backoff, sometimes with a `Retry-After`.
    ServiceUnavailable,
    /// HTTP 404 or 410. Resource is missing; usually a stale link rather
    /// than a politeness-shaped failure. Surfaced as its own variant so
    /// dashboards can see how much of the failure tail is dead links.
    NotFound,
    /// Any 4xx that isn't `TooManyRequests` or `NotFound` (400, 401, 403,
    /// 451, etc.). Usually permanent — auth or policy refusal — and not
    /// something host-level backoff fixes.
    ClientError,
    /// Any 5xx that isn't `ServiceUnavailable` (500, 502, 504, etc.).
    /// May be transient (origin glitch) or persistent (broken backend).
    ServerError,
    /// Transport-level reset (TCP RST, broken pipe, ECONNREFUSED). Often
    /// indicates the server is actively dropping us.
    ConnectReset,
    /// We gave up waiting on the server. May indicate overload, or just
    /// network slowness; backoff is appropriate but milder than 429.
    Timeout,
    /// DNS resolution failed (NXDOMAIN, SERVFAIL, resolver timeout, or
    /// stub-resolver error). Distinguishes "we never reached the network"
    /// from later-stage failures.
    DnsFailure,
    /// TLS / SSL handshake failure (bad certificate, protocol mismatch,
    /// SNI rejection). The connection got to the wire but couldn't
    /// negotiate the secure channel.
    TlsError,
    /// `ENETUNREACH` / "network is unreachable" / "no route to host".
    /// Usually an IPv6-path-broken-but-no-fallback situation, or an
    /// outage of the destination's network segment.
    Unreachable,
    /// Local-side resource exhaustion: `EMFILE` (too many open files),
    /// connection-pool starvation, allocator failure. **Our fault**, not
    /// the remote's; raising the relevant cap usually fixes it.
    ResourceExhausted,
    /// Anything else. A persistently nonzero rate here is a signal that
    /// the classifier needs another arm.
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
            Self::NotFound => "not_found",
            Self::ClientError => "client_error",
            Self::ServerError => "server_error",
            Self::ConnectReset => "connect_reset",
            Self::Timeout => "timeout",
            Self::DnsFailure => "dns_failure",
            Self::TlsError => "tls_error",
            Self::Unreachable => "unreachable",
            Self::ResourceExhausted => "resource_exhausted",
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
    //
    // Per-host depth caps used to live here; they were moved to the
    // pure-config `CrawlScope` (in `crawlrs-core`) since "how deep
    // to crawl" is operator-mandated scope, not host-as-guest
    // behavior, and the worker reads it directly without going
    // through this trait.
}
