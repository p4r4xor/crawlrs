//! `Store` trait: persist parsed documents + raw bodies.
//!
//! Concrete impls: `crawlrs-store::{ParquetStore, WarcStore,
//! MultiStore}`.
//! Test double: `crawlrs-fakes::InMemoryStore`.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::StoreRecord;

#[async_trait]
pub trait Store: Send + Sync {
    /// Persist one fetched + parsed record. Returns the storage-specific
    /// identifier for the persisted blob: an `s3://` / `file://` URI for
    /// `ParquetStore` and `WarcStore`, `memory://...` for the in-memory
    /// test double.
    ///
    /// The returned string is recorded in the metadata ledger
    /// ([`UrlMetadata::blob_path`](crate::types::UrlMetadata::blob_path))
    /// so a future "where is this URL's body?" question is answered
    /// without searching the data plane. When a `MultiStore` fans out
    /// the write to several inner stores, the convention is that the
    /// first-configured store's path is the canonical one returned to
    /// the runtime.
    async fn write(&self, record: &StoreRecord<'_>) -> Result<String>;

    /// Flush any buffered writes to durable storage. Implementations that
    /// write synchronously may make this a no-op.
    async fn flush(&self) -> Result<()>;
}
