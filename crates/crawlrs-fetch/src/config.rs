//! Configuration for [`WreqFetcher`].
//!
//! [`WreqFetcher`]: crate::fetcher::WreqFetcher

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::ProxyResolver;
use wreq_util::Emulation;

use crate::proxy::NoProxyResolver;

#[derive(Clone)]
pub struct WreqFetcherConfig {
    /// Browser-fingerprint profile to emulate (TLS + HTTP/2 + headers).
    /// `None` rotates randomly across the set of profiles `wreq_util`
    /// ships, picking a fresh profile each time a `WreqFetcher` is
    /// built. Pinning to a specific profile only when the operator
    /// has a concrete reason (e.g. reproducible tests, a target site
    /// that gates on a particular fingerprint) keeps our default
    /// behaviour broadly anti-bot-resistant rather than predictable.
    pub emulation: Option<Emulation>,

    /// Override the User-Agent. `None` falls through to the value baked into
    /// the emulation profile (recommended for fingerprint coherence; lying
    /// about the UA while sending a Chrome TLS fingerprint is the textbook
    /// way to get flagged).
    pub user_agent: Option<String>,

    /// Per-request total timeout (used when [`FetchRequest::timeout`] is
    /// not overridden).
    pub default_timeout: Duration,

    /// TCP connect timeout.
    pub connect_timeout: Duration,

    /// Maximum redirect hops to follow before giving up.
    pub max_redirects: usize,

    /// Idle connections held in the pool per host. Each idle conn
    /// is one TCP socket (1 FD), one TLS session in memory, and one
    /// hyper read/write task pair (1-2 tokio tasks). Default is 0
    /// (close every connection at end-of-request, no pooling): with
    /// `Emulation::random()` per request the pool is keyed by
    /// `(host, emulation_profile)` and orphan entries accumulate
    /// faster than they get reused, costing memory + FDs for no
    /// throughput benefit. Raise above 0 only when pinning a single
    /// emulation profile AND when same-host burst rates exceed
    /// `politeness.host_delay`.
    pub pool_max_idle_per_host: usize,

    /// Maximum response body size, in bytes. Bounds memory per fetch
    /// against adversarial servers that advertise (or stream) very
    /// large bodies. The fetcher checks `Content-Length` upfront when
    /// the server provides it and streams chunks otherwise, aborting
    /// once cumulative bytes cross the cap. Default 32 MiB; raise for
    /// large-document corpora, lower if memory is tight.
    pub max_body_bytes: u64,

    /// Proxy resolution strategy. Defaults to [`NoProxyResolver`] (direct).
    pub proxy: Arc<dyn ProxyResolver>,
}

impl Default for WreqFetcherConfig {
    fn default() -> Self {
        Self {
            emulation: None,
            user_agent: None,
            default_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_redirects: 5,
            pool_max_idle_per_host: 0,
            max_body_bytes: 32 * 1024 * 1024,
            proxy: Arc::new(NoProxyResolver),
        }
    }
}
