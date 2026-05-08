//! In-memory mirror of the Postgres `frontier_outbox` table.
//!
//! Owns the outbox-side state separately from the URL ledger so the
//! two concepts don't share a file. The state still lives inside the
//! [`crate::metadata_store::InMemoryMetadataStore`]'s single `Mutex`
//! so the metadata write and the outbox write hold the same atomicity
//! contract the Postgres transaction provides.
//!
//! ## Lease modeling
//!
//! `OutboxState::leased` mirrors the row-level lock the Postgres impl
//! gets from `SELECT ... FOR UPDATE SKIP LOCKED`. While a `publish`
//! call is in flight, its leased ids live in the set; concurrent
//! callers' `lease_batch` skips them. Finalize on success (mark
//! published, drop lease) or release on failure (drop lease, leave
//! unpublished). See ADR-0017.
//!
//! The free functions in this module are intended to be called while
//! holding the metadata store's lock; they accept an `&mut OutboxState`
//! reference rather than locking themselves so callers don't have to
//! coordinate two locks.

use std::collections::HashSet;

use crawlrs_core::{AttemptId, CanonicalUrl, OutboxEntry, OutboxRowId, UrlEntry};

/// All outbox rows, the `(parent_url, parent_attempt_id, child_url)`
/// dedupe set, and the in-flight lease set. `next_id` emulates Postgres
/// BIGSERIAL so tests asserting on row ids see monotonically-increasing
/// values.
#[derive(Default)]
pub(crate) struct OutboxState {
    pub(crate) rows: Vec<InMemoryOutboxRow>,
    pub(crate) next_id: u64,
    pub(crate) dedupe: HashSet<(String, String, String)>,
    /// Row ids currently leased by an in-flight `publish` call.
    /// Concurrent `lease_batch` callers skip these, mirroring
    /// `FOR UPDATE SKIP LOCKED`.
    pub(crate) leased: HashSet<u64>,
}

#[derive(Clone)]
pub(crate) struct InMemoryOutboxRow {
    pub(crate) id: OutboxRowId,
    pub(crate) entry: UrlEntry,
    pub(crate) published: bool,
}

/// Insert one outbound URL into the outbox. Mirrors the Postgres
/// `(parent_url_id, parent_attempt_id, url)` UNIQUE constraint: a
/// redelivered attempt's second-pass insert is a no-op so the
/// publisher drains a deterministic set of rows.
pub(crate) fn record_outbound(
    state: &mut OutboxState,
    parent_url: &CanonicalUrl,
    parent_attempt_id: &AttemptId,
    child: &UrlEntry,
) {
    let key = (
        parent_url.as_str().to_string(),
        parent_attempt_id.as_str().to_string(),
        child.url.as_str().to_string(),
    );
    if !state.dedupe.insert(key) {
        return;
    }
    state.next_id += 1;
    state.rows.push(InMemoryOutboxRow {
        id: OutboxRowId::new(state.next_id),
        entry: child.clone(),
        published: false,
    });
}

/// Lease up to `max` unpublished, unleased rows in id order. Models
/// `SELECT ... FOR UPDATE SKIP LOCKED LIMIT max`. The caller MUST
/// pair this with either [`finalize_lease`] (success) or
/// [`release_lease`] (failure) on the returned ids; a forgotten lease
/// keeps rows invisible to peers until the state is dropped.
pub(crate) fn lease_batch(state: &mut OutboxState, max: usize) -> Vec<OutboxEntry> {
    let mut leased = Vec::new();
    for row in &state.rows {
        if leased.len() >= max {
            break;
        }
        if row.published || state.leased.contains(&row.id.value()) {
            continue;
        }
        leased.push(OutboxEntry {
            id: row.id,
            entry: row.entry.clone(),
        });
    }
    for entry in &leased {
        state.leased.insert(entry.id.value());
    }
    leased
}

/// Mark the leased rows as published and drop the lease. Pairs with a
/// successful ship call.
pub(crate) fn finalize_lease(state: &mut OutboxState, ids: &[OutboxRowId]) {
    for row in state.rows.iter_mut() {
        if ids.contains(&row.id) {
            row.published = true;
        }
    }
    for id in ids {
        state.leased.remove(&id.value());
    }
}

/// Drop the lease without publishing. Pairs with a failed ship call:
/// the rows reappear for the next caller, matching the Postgres
/// `ROLLBACK` path.
pub(crate) fn release_lease(state: &mut OutboxState, ids: &[OutboxRowId]) {
    for id in ids {
        state.leased.remove(&id.value());
    }
}
