//! Per-URL metadata ledger.
//!
//! Implements [`crawlrs_core::MetadataStore`] backed by Postgres. See
//! [ADR-0009](../../../docs/decisions/0009-metadata-store.md) for the
//! design.
//!
//! Two-table shape: `url_metadata` is the mutable current-state row
//! (one per URL); `url_history` is the append-only event log
//! (one row per state transition). Cardinality of `url_history` is
//! bounded per URL by `retry_count + lifecycle` (~N+2 rows for a URL
//! with N transient failures), which keeps the table manageable
//! without a runtime archival policy at v1 scale.
//!
//! The migration in `migrations/0001_init.sql` creates both tables
//! plus the indexes the runtime relies on. Apply it with
//! [`PostgresMetadataStore::migrate`] before constructing a store.

pub mod metrics;
pub mod store;

pub use store::{PostgresMetadataError, PostgresMetadataStore};
