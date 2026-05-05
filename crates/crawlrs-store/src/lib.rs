//! Concrete `Store` impls for crawlrs.
//!
//! Per [ADR-0013](../../docs/decisions/0013-blob-store-as-per-fetch-event-log.md)
//! v1 ships:
//!
//! - `ParquetStore`: analytical primary (Arrow + Parquet + zstd column
//!   compression + Hive-partitioned paths). LanceDB ingest target.
//! - `WarcStore` (Phase 5c): archival mirror (ISO 28500 records +
//!   per-record gzip + native `WARC-Type: revisit` for body dedup on
//!   recrawl).
//! - `MultiStore` (Phase 5c): Composite over Strategy. Fans out
//!   `write()` to N inner stores. The first store's blob_path is
//!   returned as the canonical pointer the metadata ledger records.
//!
//! All impls share `path::PathBuilder` (path-layout helper) and
//! `rotation::RotationPolicy` (when-to-close-the-file decision) so the
//! convention is one place.

pub mod error;
pub mod metrics;
pub mod multi_store;
pub mod parquet_store;
pub mod path;
pub mod rotation;
pub mod warc_record;
pub mod warc_store;

pub use error::StoreError;
pub use multi_store::MultiStore;
pub use parquet_store::ParquetStore;
pub use path::PathBuilder;
pub use rotation::RotationPolicy;
pub use warc_store::WarcStore;
