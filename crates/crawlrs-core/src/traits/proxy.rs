//! Proxy abstraction.
//!
//! [`ProxyResolver`] decouples *where* a proxy URL comes from from *how*
//! the fetcher uses it. Two patterns are supported by the same trait:
//!
//! 1. **Direct rotation**: the resolver holds a list of upstream proxy URLs
//!    and picks one per request (round-robin, random, sticky-by-host, etc.).
//!    Health feedback flows back through [`ProxyResolver::report`].
//!
//! 2. **Gateway routing**: the resolver returns a single fixed gateway URL
//!    plus a per-request set of routing-hint headers. The gateway picks the upstream proxy.
//!    The resolver may also expose a CA cert so the client trusts the
//!    gateway's TLS-MITM certificate.
//!
//! Implementations live in `crawlrs-fetch` (built-ins) or in user code
//! (custom rotation strategies, your own gateway dialect, etc.).

use std::collections::HashMap;

use async_trait::async_trait;

use crate::error::Result;
use crate::types::FetchRequest;

/// One proxy decision for one request.
#[derive(Debug, Clone)]
pub struct ProxySelection {
    /// Proxy URL the fetcher should route through. May be an upstream
    /// proxy (rotation pattern) or a gateway URL (gateway pattern).
    pub url: String,

    /// Extra request headers to add. The gateway pattern uses these to
    /// pass routing hints (algorithm, selector, mutex, exhaust-on, etc).
    /// Empty for the rotation pattern.
    pub extra_headers: HashMap<String, String>,
}

/// Outcome that the fetcher reports back to the resolver after each fetch.
/// Resolvers use this to maintain health stats, cooldowns, exhaustion, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyOutcome {
    /// 2xx or expected non-2xx: proxy worked end-to-end.
    Success,
    /// 403/429/captcha/CF challenge: proxy "burned" on this site.
    Banned,
    /// Connect timeout, read timeout: proxy unresponsive.
    Timeout,
    /// Connection refused, TLS error, DNS error: proxy unreachable.
    NetworkError,
}

#[async_trait]
pub trait ProxyResolver: Send + Sync {
    /// PEM-encoded CA bundle the resolver wants the client to trust.
    ///
    /// Used by gateway resolvers that perform TLS interception (e.g. HMA).
    /// Read once at fetcher construction time and merged into the wreq
    /// client's trust store. Returning `None` means use the default roots
    /// only.
    fn trusted_ca_pem(&self) -> Option<&[u8]> {
        None
    }

    /// Pick a proxy for this request. Returning `None` means fetch direct.
    async fn resolve(&self, req: &FetchRequest) -> Result<Option<ProxySelection>>;

    /// Report the outcome of a fetch back to the resolver.
    ///
    /// Default impl is a no-op. Rotation resolvers override this to track
    /// per-proxy health; gateway resolvers can ignore it.
    async fn report(&self, _selection: &ProxySelection, _outcome: ProxyOutcome) {}
}
