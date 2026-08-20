//! `Parser` trait: HTML / structured-content extraction.
//!
//! Concrete impl: `crawlrs-parse::LolHtmlParser`.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{FetchResponse, ParsedDocument};

#[async_trait]
pub trait Parser: Send + Sync {
    /// Extract structured content from a fetched response.
    ///
    /// # Errors
    ///
    /// Returns an error when the response body cannot be decoded or
    /// parsed as the expected content type.
    async fn parse(&self, resp: &FetchResponse) -> Result<ParsedDocument>;
}
