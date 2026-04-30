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
    pub emulation: Emulation,

    /// Override the User-Agent. `None` falls through to the value baked into
    /// the emulation profile (recommended for fingerprint coherence).
    pub user_agent: Option<String>,

    /// Per-request total timeout (used when [`FetchRequest::timeout`] is
    /// not overridden).
    pub default_timeout: Duration,

    /// TCP connect timeout.
    pub connect_timeout: Duration,

    /// Maximum redirect hops to follow before giving up.
    pub max_redirects: usize,

    /// Idle connections held in the pool per host.
    pub pool_max_idle_per_host: usize,

    /// Proxy resolution strategy. Defaults to [`NoProxyResolver`] (direct).
    pub proxy: Arc<dyn ProxyResolver>,
}

impl Default for WreqFetcherConfig {
    fn default() -> Self {
        Self {
            emulation: Emulation::Chrome145,
            user_agent: None,
            default_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_redirects: 5,
            pool_max_idle_per_host: 8,
            proxy: Arc::new(NoProxyResolver),
        }
    }
}
