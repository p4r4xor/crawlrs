//! Built-in [`ProxyResolver`] implementations.
//!
//! Today we ship three resolvers:
//!
//! - [`NoProxyResolver`]: no proxy (default).
//! - [`EnvProxyResolver`]: read one URL from `HTTPS_PROXY`/`HTTP_PROXY`.
//! - [`GatewayProxyResolver`]: one fixed gateway URL plus per-request
//!   routing headers (HMA-style).
//!
//! Multi-URL rotation isn't built in. The [`ProxyResolver`] trait is the
//! extension point: write a `RotatingProxyResolver` impl when the need
//! arrives; none of the existing code has to change.
//!
//! [`ProxyResolver`]: crawlrs_core::ProxyResolver

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crawlrs_core::{FetchRequest, ProxyResolver, ProxySelection, Result};

/// Always returns `None`, fetch direct.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoProxyResolver;

#[async_trait]
impl ProxyResolver for NoProxyResolver {
    async fn resolve(&self, _request: &FetchRequest) -> Result<Option<ProxySelection>> {
        Ok(None)
    }
}

/// Reads `HTTPS_PROXY`, `HTTP_PROXY`, or `ALL_PROXY` (in that order) from
/// the environment once at construction time. Returns the same proxy URL
/// for every request, with no extra headers.
#[derive(Debug, Clone)]
pub struct EnvProxyResolver {
    url: Option<String>,
}

impl EnvProxyResolver {
    pub fn new() -> Self {
        let url = std::env::var("HTTPS_PROXY")
            .or_else(|_| std::env::var("https_proxy"))
            .or_else(|_| std::env::var("HTTP_PROXY"))
            .or_else(|_| std::env::var("http_proxy"))
            .or_else(|_| std::env::var("ALL_PROXY"))
            .or_else(|_| std::env::var("all_proxy"))
            .ok()
            .filter(|raw| !raw.is_empty());
        Self { url }
    }
}

impl Default for EnvProxyResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProxyResolver for EnvProxyResolver {
    async fn resolve(&self, _request: &FetchRequest) -> Result<Option<ProxySelection>> {
        Ok(self.url.as_ref().map(|proxy_url| ProxySelection {
            url: proxy_url.clone(),
            extra_headers: HashMap::new(),
        }))
    }
}

/// Closure that computes per-request gateway routing headers. The
/// alias is internal; external callers pass a closure directly to
/// [`GatewayProxyResolver::with_header_fn`] (which takes a generic
/// constrained by the same `Fn` bound).
pub(crate) type HeaderFn = Arc<dyn Fn(&FetchRequest) -> HashMap<String, String> + Send + Sync>;

/// **Gateway pattern**: a single fixed proxy URL plus a per-request
/// callback that produces routing-hint headers. Optionally trusts a
/// caller-supplied PEM CA bundle for gateways that perform TLS interception.
pub struct GatewayProxyResolver {
    url: String,
    header_fn: Option<HeaderFn>,
    ca_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for GatewayProxyResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayProxyResolver")
            .field("url", &self.url)
            .field("has_header_fn", &self.header_fn.is_some())
            .field("has_ca_pem", &self.ca_pem.is_some())
            .finish()
    }
}

impl GatewayProxyResolver {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            header_fn: None,
            ca_pem: None,
        }
    }

    /// Provide a callback that computes routing headers per request.
    pub fn with_header_fn<F>(mut self, header_fn: F) -> Self
    where
        F: Fn(&FetchRequest) -> HashMap<String, String> + Send + Sync + 'static,
    {
        self.header_fn = Some(Arc::new(header_fn));
        self
    }

    /// Trust a PEM-encoded CA bundle (e.g. the gateway's MITM root cert).
    pub fn with_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(pem.into());
        self
    }
}

#[async_trait]
impl ProxyResolver for GatewayProxyResolver {
    fn trusted_ca_pem(&self) -> Option<&[u8]> {
        self.ca_pem.as_deref()
    }

    async fn resolve(&self, request: &FetchRequest) -> Result<Option<ProxySelection>> {
        let extra_headers = match &self.header_fn {
            Some(compute_headers) => compute_headers(request),
            None => HashMap::new(),
        };
        Ok(Some(ProxySelection {
            url: self.url.clone(),
            extra_headers,
        }))
    }
}
