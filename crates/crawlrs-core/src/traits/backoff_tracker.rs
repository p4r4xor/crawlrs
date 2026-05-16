//! Per-host circuit-breaker + exponential-backoff port.
//!
//! One of the three sub-trait splits of `Politeness`. Tracks
//! consecutive failures per host, opens a circuit when the count
//! crosses a threshold, and computes the next-wake plan after
//! each failure. The runtime applies the plan via
//! `Frontier::advance_wake`; this trait does not write wake-time.

use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::traits::politeness::FailureKind;
use crate::types::NextWake;
use crate::url::CanonicalUrl;

/// Per-host failure tracking and backoff math. The aggregate
/// `Politeness::check` consults `is_open` before any other gate;
/// the aggregate `Politeness::record_failure` delegates here.
#[async_trait]
pub trait BackoffTracker: Send + Sync {
    /// Is the per-host circuit currently open? When `true`, the
    /// aggregate `Politeness` returns `Disallow` from `check`
    /// without consulting robots or rate. The circuit closes when
    /// the next successful fetch resets the failure counter (or
    /// when the impl's own time-based recovery elapses).
    async fn is_open(&self, host: &str) -> Result<bool>;

    /// Record a failure and return the wake-time plan. Server-
    /// pushback categories (`TooManyRequests`, `ServiceUnavailable`,
    /// `ConnectReset`) get full-strength exponential backoff;
    /// transient categories get the host-delay floor.
    ///
    /// `server_hint` carries an HTTP `Retry-After` value when
    /// present. Impls treat it as a *floor*: the returned plan's
    /// next-wake is `max(server_hint, computed_backoff)`. Servers
    /// know best how long they need to recover, but the impl
    /// still applies its own backoff if harsher (e.g., after the
    /// nth consecutive 503).
    async fn record_failure(
        &self,
        url: &CanonicalUrl,
        kind: FailureKind,
        server_hint: Option<Duration>,
    ) -> Result<NextWake>;
}
