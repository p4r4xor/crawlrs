//! `Fetcher` trait: HTTP transport.
//!
//! Concrete impl: `crawlrs-fetch::WreqFetcher`.
//! Test double: `crawlrs-fakes::FakeFetcher`.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{FetchRequest, FetchResponse};

#[async_trait]
pub trait Fetcher: Send + Sync {
    /// Fetch one URL over HTTP and return the response.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure (connection refused, DNS
    /// failure, TLS error, timeout) or when a proxy cannot be resolved.
    async fn fetch(&self, req: FetchRequest) -> Result<FetchResponse>;
}
