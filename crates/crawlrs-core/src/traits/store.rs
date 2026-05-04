//! `Store` trait: persist parsed documents + raw bodies.
//!
//! Concrete impls: `crawlrs-store::{JsonLinesStore, ParquetStore,
//! WarcStore}` (Phase 5c).
//! Test double: `crawlrs-testing::InMemoryStore`.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;
use crate::types::ParsedDocument;

#[async_trait]
pub trait Store: Send + Sync {
    /// Persist one parsed document, optionally with the raw response
    /// body. Returns the storage-specific identifier for the persisted
    /// blob: a filesystem path for `JsonLinesStore`, an `s3://` URI for
    /// `ParquetStore` on S3, a `warc://record-id` for `WarcStore`,
    /// `memory://...` for in-memory test stubs.
    ///
    /// The returned string is recorded in the metadata ledger
    /// ([`UrlMetadata::blob_path`](crate::types::UrlMetadata::blob_path))
    /// so a future "where is this URL's body?" question is answered
    /// without searching the data plane.
    async fn write(&self, doc: &ParsedDocument, raw_body: Option<&Bytes>) -> Result<String>;

    /// Flush any buffered writes to durable storage. Implementations that
    /// write synchronously may make this a no-op.
    async fn flush(&self) -> Result<()>;
}
