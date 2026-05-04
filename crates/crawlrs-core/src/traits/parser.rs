//! `Parser` trait: HTML / structured-content extraction.
//!
//! Concrete impl: `crawlrs-parse::LolHtmlParser`.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{FetchResponse, ParsedDocument};

#[async_trait]
pub trait Parser: Send + Sync {
    async fn parse(&self, resp: &FetchResponse) -> Result<ParsedDocument>;
}
