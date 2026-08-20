//! `Outbox` trait: the publisher's view of the transactional-outbox
//! pattern that decouples Frontier writes from Metadata writes.
//!
//! Pattern: Transactional Outbox + Producer / Consumer + Lease.
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
//! outbox table and pushes the URLs into the Frontier; the trait
//! below is what that publisher calls.
//!
//! ## Why a single `publish` method (not fetch + mark)
//!
//! At horizontal scale, two publishers querying "the next N
//! unpublished rows by id" each receive the same prefix and
//! double-publish. The Frontier-side seen-set absorbs the duplicate
//! XADD into correctness, but the wasted DB+Redis work is real. The
//! fix is a row-level lease (`SELECT ... FOR UPDATE SKIP LOCKED`
//! inside one txn): concurrent callers receive disjoint batches.
//! That requires the read and the mark to share a transaction, which
//! requires them to share a method.
//!
//! ## Why a separate trait (and not just methods on MetadataStore)
//!
//! Conceptual separation: `MetadataStore` is the per-URL ledger
//! abstraction; `Outbox` is the publisher's view of "outbound URLs
//! awaiting enqueue." The publisher only needs the outbox method, so
//! it depends on the narrower interface. A single Postgres-backed
//! struct can implement both traits and share the pool; that's the
//! production wiring.

use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::types::UrlEntry;

/// The future returned by an [`Outbox::publish`] ship closure.
///
/// Boxed + pinned so the trait method is dyn-compatible: a generic
/// `Fut: Future` parameter would prevent `Arc<dyn Outbox>`. The
/// runtime stores its outbox as a trait object, so this concrete
/// future shape is the price of admission.
pub type ShipFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// The closure shape passed to [`Outbox::publish`].
///
/// Boxed for the same reason as [`ShipFuture`]: generics on the
/// trait method break dyn-compat. Construct one with
/// `Box::new(move |entries| Box::pin(async move { ... }))`.
pub type ShipFn = Box<dyn FnOnce(Vec<OutboxEntry>) -> ShipFuture + Send>;

/// Identifier for one row in the outbox table.
///
/// Newtype around `u64` so callers can't swap an outbox row id for
/// some other numeric id (e.g. a depth, a content hash). Concrete
/// impls translate to whatever the storage backend uses on the wire
/// (the Postgres impl maps to BIGSERIAL); the trait surface stays in
/// domain vocabulary.
///
/// Surfaced on [`OutboxEntry`] for diagnostics, deterministic
/// ordering, and tests asserting on per-row identity. Not threaded
/// across publisher calls: the closure-based [`Outbox::publish`]
/// owns the rows for one batch's lifetime, so the impl uses the id
/// internally to scope its mark-published UPDATE without exposing
/// it to the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutboxRowId(u64);

impl OutboxRowId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(&self) -> u64 {
        self.0
    }
}

/// One row from the outbox table awaiting publish to the Frontier.
///
/// `id` is the row's stable identifier; impls use it internally to
/// scope the "mark as published" UPDATE to the leased rows. `entry`
/// is the already-canonicalised `UrlEntry` ready for
/// `Frontier::submit_batch`.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub id: OutboxRowId,
    pub entry: UrlEntry,
}

/// Drain side of the transactional outbox.
#[async_trait]
pub trait Outbox: Send + Sync {
    /// Lease up to `max` unpublished entries, hand them to `ship`,
    /// and mark as published any entries the ship call resolves
    /// successfully.
    ///
    /// ## Lease semantics
    ///
    /// Concurrent callers (multiple publisher tasks or processes)
    /// receive **disjoint batches**. Impls enforce this via
    /// row-level locks held for the duration of the closure call;
    /// the Postgres impl uses `SELECT ... FOR UPDATE SKIP LOCKED`.
    /// Test fakes must model the same guarantee.
    ///
    /// ## All-or-nothing batch
    ///
    /// If `ship` resolves `Ok(())`, the entire batch is marked
    /// published atomically and the lease is released. If `ship`
    /// resolves `Err`, no rows are marked published, the lease is
    /// released, and the rows reappear for the next caller. Partial
    /// success is not representable; this matches
    /// `Frontier::submit_batch`'s own all-or-nothing contract.
    ///
    /// Returns the number of entries successfully published. Zero
    /// means the outbox was empty (or the contention with peers was
    /// total, which under SKIP LOCKED translates to "no available
    /// rows for me right now"); the caller should sleep before
    /// retrying.
    ///
    /// # Errors
    ///
    /// Returns an error when the outbox cannot be read or leased, or
    /// when marking the shipped batch as published fails. A `ship`
    /// closure that resolves `Err` is not an error here; those rows are
    /// simply left unpublished for the next caller.
    async fn publish(&self, max: usize, ship: ShipFn) -> Result<usize>;
}
