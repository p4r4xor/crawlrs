//! `Fetcher` trait: HTTP transport.
//!
//! Concrete impl: `crawlrs-fetch::WreqFetcher`.
//! Test double: `crawlrs-testing::FakeFetcher`.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{FetchRequest, FetchResponse};

#[async_trait]
pub trait Fetcher: Send + Sync {
    async fn fetch(&self, req: FetchRequest) -> Result<FetchResponse>;
}
