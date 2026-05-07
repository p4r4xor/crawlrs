//! In-memory mirror of the Postgres `frontier_outbox` table.
//!
//! Owns the outbox-side state separately from the URL ledger so the
//! two concepts don't share a file. The state still lives inside the
//! [`crate::metadata_store::InMemoryMetadataStore`]'s single `Mutex`
//! so the metadata write and the outbox write hold the same atomicity
//! contract the Postgres transaction provides.
//!
//! The free functions in this module are intended to be called while
//! holding the metadata store's lock; they accept an `&mut OutboxState`
//! reference rather than locking themselves so callers don't have to
//! coordinate two locks.

use std::collections::HashSet;

use crawlrs_core::{AttemptId, CanonicalUrl, OutboxEntry, UrlEntry};

/// All outbox rows plus the `(parent_url, parent_attempt_id, child_url)`
/// dedupe set. `next_id` emulates Postgres BIGSERIAL so tests asserting
/// on row ids see monotonically-increasing values.
#[derive(Default)]
pub(crate) struct OutboxState {
    pub(crate) rows: Vec<InMemoryOutboxRow>,
    pub(crate) next_id: i64,
    pub(crate) dedupe: HashSet<(String, String, String)>,
}

#[derive(Clone)]
pub(crate) struct InMemoryOutboxRow {
    pub(crate) id: i64,
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
    let id = state.next_id;
    state.rows.push(InMemoryOutboxRow {
        id,
        entry: child.clone(),
        published: false,
    });
}

/// Snapshot of unpublished rows in id order, capped at `max`. Held
/// behind the outer `Mutex` by the caller; the slice copy is cheap.
pub(crate) fn fetch_unpublished(state: &OutboxState, max: usize) -> Vec<OutboxEntry> {
    state
        .rows
        .iter()
        .filter(|row| !row.published)
        .take(max)
        .map(|row| OutboxEntry {
            id: row.id,
            entry: row.entry.clone(),
        })
        .collect()
}

/// Flip the `published` flag on rows whose ids appear in `ids`.
/// Idempotent: ids already published are silently skipped, matching
/// the Postgres impl's `WHERE published_at IS NULL` filter.
pub(crate) fn mark_published(state: &mut OutboxState, ids: &[i64]) {
    if ids.is_empty() {
        return;
    }
    for row in state.rows.iter_mut() {
        if !row.published && ids.contains(&row.id) {
            row.published = true;
        }
    }
}
