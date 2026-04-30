//! `wreq`-backed implementation of [`crawlrs_core::Fetcher`].
//!
//! Public surface:
//!
//! - [`WreqFetcher`]: the `Fetcher` impl.
//! - [`WreqFetcherConfig`]: knobs for emulation, timeouts, redirects, proxy.
//! - [`proxy`]: built-in [`ProxyResolver`] implementations covering both the
//!   direct-rotation pattern and the gateway pattern.
//!
//! [`ProxyResolver`]: crawlrs_core::ProxyResolver

pub mod config;
pub mod fetcher;
pub mod proxy;

pub use config::WreqFetcherConfig;
pub use fetcher::WreqFetcher;
pub use proxy::{EnvProxyResolver, GatewayProxyResolver, NoProxyResolver};
