//! `OutboxReader` trait: the read-side of the transactional outbox
//! pattern that decouples Frontier writes from Metadata writes.
//!
//! Pattern: Transactional Outbox + Producer / Consumer.
//!
//! ## Why
//!
//! The crawler's "URL successfully fetched" event has two
//! consequences: (a) update the metadata ledger row + history (a
//! Postgres write), (b) enqueue any newly-discovered outbound URLs
//! into the Frontier (a Redis write). These are two different
//! durability domains; without an atomicity story between them, a
//! crash between (a) and (b) means redelivery via XAUTOCLAIM
//! re-enqueues the outbound URLs, and the seen-set's
//! per-URL-per-shard idempotency is the only thing that prevents
//! duplicate downstream fetches.
//!
//! The outbox pattern moves the atomicity to where it can be
//! enforced: Postgres. The Metadata write and the outbound URL
//! enqueue happen in the **same Postgres transaction** via
//! [`crate::traits::metadata::MetadataStore::mark_succeeded`] taking
//! `outbound: &[UrlEntry]`. A separate publisher task drains the
//! outbox table and pushes the URLs into the Frontier, marking each
//! drained row as published. If the publisher crashes mid-drain, the
//! row stays unpublished; on restart it's re-drained. The Frontier
//! impl's per-URL seen-set absorbs the second-time XADD as a no-op.
//!
//! ## Why a separate trait (and not just methods on MetadataStore)
//!
//! Conceptual separation: `MetadataStore` is the per-URL ledger
//! abstraction; `OutboxReader` is the publisher's view of "outbound
//! URLs awaiting enqueue." The publisher only needs the outbox
//! methods, so it depends on the narrower interface. A single
//! Postgres-backed struct can implement both traits and share the
//! pool; that's the production wiring.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::UrlEntry;

/// One row from the outbox table awaiting publish to the Frontier.
///
/// The `id` is the Postgres row id; the publisher marks rows
/// published by id after a successful Frontier submit. `entry` is the
/// already-canonicalised `UrlEntry` ready for `Frontier::submit_batch`.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub id: i64,
    pub entry: UrlEntry,
}

/// Read-side of the outbox: drain unpublished rows, mark them
/// published once the Frontier has acknowledged the submit.
#[async_trait]
pub trait OutboxReader: Send + Sync {
    /// Fetch up to `max` unpublished rows in stable order (by id).
    /// Returning fewer than `max` (including zero) means the outbox
    /// is empty for now; the publisher should sleep before retrying.
    async fn fetch_unpublished(&self, max: usize) -> Result<Vec<OutboxEntry>>;

    /// Mark the listed row ids as published. The publisher calls
    /// this **after** a successful `Frontier::submit_batch` so a
    /// crash between submit and mark leaves the rows visible for
    /// retry; the Frontier-side seen-set absorbs the duplicate
    /// XADD.
    ///
    /// Must be idempotent: passing an id that's already published
    /// is a no-op, not an error.
    async fn mark_published(&self, ids: &[i64]) -> Result<()>;
}
