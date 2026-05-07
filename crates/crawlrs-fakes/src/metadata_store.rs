//! `InMemoryMetadataStore`: a `MetadataStore` impl backed by a HashMap.
//!
//! Mirrors the `PostgresMetadataStore` semantics (atomic mark
//! transitions, retry counter preserved across attempts and reset
//! only on success) without needing a Postgres container. Suitable
//! for tests of the runtime; not suitable for production
//! (no persistence, no transactions across restarts).
//!
//! All ledger state lives behind a single `Mutex<LedgerState>` so a
//! mark-transition spans one critical section. The Postgres backing
//! holds these effects atomic via a transaction; the fake holds them
//! atomic via the lock. A test that observes the store mid-transition
//! must see either the pre-state or the post-state, never a halfway
//! commit.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use crawlrs_core::{
    CanonicalUrl, Error, FailureKind, MetadataStore, OutboxEntry, OutboxReader, Result,
    SuccessRecord, UrlMetadata, UrlStatus,
};

use crate::outbox::{self, OutboxState};

/// All in-memory ledger state. Held behind one mutex so each
/// mark-transition is a single critical section.
///
/// `succeeded_attempts` mirrors the Postgres impl's
/// `url_history.attempt_id` UNIQUE constraint: if the same
/// `(url, attempt_id)` pair is `mark_succeeded`'d twice (e.g. because
/// `XAUTOCLAIM` redelivered an attempt that got past the DB write but
/// not the `XACK`), the second call is a no-op on the history side.
/// `succeeded_history_count` exposes that fact for tests.
#[derive(Default)]
struct LedgerState {
    rows: HashMap<String, UrlMetadata>,
    dlq: Vec<(String, String)>,
    succeeded_attempts: HashSet<(String, String)>,
    succeeded_history_count: u64,
    outbox: OutboxState,
}

/// One row per URL, last-write-wins on every mutable field except
/// `discovered_at`. The DLQ list is populated on
/// `mark_permanently_failed` so tests can assert on it.
#[derive(Default)]
pub struct InMemoryMetadataStore {
    ledger: Mutex<LedgerState>,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of URLs currently in `PermanentlyFailed`. Used by tests
    /// asserting that the retry budget actually fired.
    pub fn dlq_count(&self) -> usize {
        self.ledger.lock().unwrap().dlq.len()
    }

    /// Snapshot of the DLQ entries: `(url, reason)` pairs in insert
    /// order. Useful for tests that want to verify the failure reason
    /// string the runtime constructed.
    pub fn dlq_entries(&self) -> Vec<(String, String)> {
        self.ledger.lock().unwrap().dlq.clone()
    }

    /// Number of distinct `mark_succeeded` history rows recorded.
    /// Counts each unique `(url, attempt_id)` once; redelivery of the
    /// same attempt does NOT increment. Mirrors the Postgres history
    /// row count.
    pub fn succeeded_history_count(&self) -> u64 {
        self.ledger.lock().unwrap().succeeded_history_count
    }

    /// Total outbox row count (published + unpublished). Mirrors the
    /// Postgres `SELECT COUNT(*) FROM frontier_outbox` query. Used by
    /// tests to assert that a redelivered attempt's outbox writes
    /// were correctly deduped at the `(parent_url, parent_attempt_id,
    /// child_url)` UNIQUE level.
    pub fn outbox_row_count(&self) -> usize {
        self.ledger.lock().unwrap().outbox.rows.len()
    }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn get(&self, url: &CanonicalUrl) -> Result<Option<UrlMetadata>> {
        Ok(self.ledger.lock().unwrap().rows.get(url.as_str()).cloned())
    }

    async fn mark_attempting(&self, url: &CanonicalUrl, run_id: &str, depth: u32) -> Result<()> {
        let mut ledger = self.ledger.lock().unwrap();
        let now = SystemTime::now();
        ledger
            .rows
            .entry(url.as_str().to_string())
            .and_modify(|m| {
                m.status = UrlStatus::InProgress;
                m.last_run_id = run_id.to_string();
                m.depth = depth;
                m.updated_at = now;
            })
            .or_insert_with(|| UrlMetadata {
                url: url.clone(),
                status: UrlStatus::InProgress,
                retry_count: 0,
                blob_path: None,
                content_hash: None,
                depth,
                last_run_id: run_id.to_string(),
                discovered_at: now,
                updated_at: now,
            });
        Ok(())
    }

    async fn mark_succeeded(&self, record: &SuccessRecord<'_>) -> Result<()> {
        let mut ledger = self.ledger.lock().unwrap();
        let now = SystemTime::now();

        let row = ledger.rows.get_mut(record.url.as_str()).ok_or_else(|| {
            Error::Metadata(format!("mark_succeeded: missing row for {}", record.url))
        })?;
        row.status = UrlStatus::Succeeded;
        row.retry_count = 0;
        row.blob_path = Some(record.blob_path.to_string());
        row.content_hash = Some(record.content_hash);
        row.updated_at = now;

        // Mirror the Postgres `(url_id, attempt_id)` unique constraint:
        // only the first call per `(url, attempt_id)` appends a history
        // row. Redelivery of the same attempt is a no-op on history.
        let history_key = (
            record.url.as_str().to_string(),
            record.attempt_id.as_str().to_string(),
        );
        if ledger.succeeded_attempts.insert(history_key) {
            ledger.succeeded_history_count += 1;
        }

        // Outbox writes happen under the same lock as the metadata
        // write so the two effects are atomic (the Postgres impl
        // achieves this via a transaction; the fake achieves it via
        // the lock).
        for child in record.outbound {
            outbox::record_outbound(&mut ledger.outbox, record.url, record.attempt_id, child);
        }
        Ok(())
    }

    async fn mark_failed(&self, url: &CanonicalUrl, _kind: FailureKind) -> Result<u32> {
        let mut ledger = self.ledger.lock().unwrap();
        let row = ledger
            .rows
            .get_mut(url.as_str())
            .ok_or_else(|| Error::Metadata(format!("mark_failed: missing row for {url}")))?;
        row.retry_count += 1;
        row.status = UrlStatus::FailedTransient;
        row.updated_at = SystemTime::now();
        Ok(row.retry_count)
    }

    async fn mark_permanently_failed(&self, url: &CanonicalUrl, reason: &str) -> Result<()> {
        let mut ledger = self.ledger.lock().unwrap();
        let row = ledger.rows.get_mut(url.as_str()).ok_or_else(|| {
            Error::Metadata(format!("mark_permanently_failed: missing row for {url}"))
        })?;
        row.status = UrlStatus::PermanentlyFailed;
        row.updated_at = SystemTime::now();
        ledger
            .dlq
            .push((url.as_str().to_string(), reason.to_string()));
        Ok(())
    }
}

#[async_trait]
impl OutboxReader for InMemoryMetadataStore {
    async fn fetch_unpublished(&self, max: usize) -> Result<Vec<OutboxEntry>> {
        let ledger = self.ledger.lock().unwrap();
        Ok(outbox::fetch_unpublished(&ledger.outbox, max))
    }

    async fn mark_published(&self, ids: &[i64]) -> Result<()> {
        let mut ledger = self.ledger.lock().unwrap();
        outbox::mark_published(&mut ledger.outbox, ids);
        Ok(())
    }
}

// Inline because: visibility-forced. The dedupe assertions read
// `succeeded_history_count`, which is a `pub` accessor but only
// meaningful next to the `mark_succeeded` invariant it guards
// (one history row per `(url, attempt_id)` regardless of redelivery).
// Keeping the test next to the implementation keeps that link
// visible to whoever edits the dedupe path.
#[cfg(test)]
mod tests {
    use super::*;
    use crawlrs_core::{AttemptId, CanonicalUrl};

    #[tokio::test]
    async fn mark_succeeded_dedupes_by_attempt_id() {
        let store = InMemoryMetadataStore::new();
        let url = CanonicalUrl::parse("https://a.test/").unwrap();
        store.mark_attempting(&url, "run-1", 0).await.unwrap();

        // First attempt: one history row.
        let first = AttemptId::new("attempt-1");
        store
            .mark_succeeded(&SuccessRecord {
                url: &url,
                attempt_id: &first,
                blob_path: "blob://1",
                content_hash: 1,
                outbound: &[],
            })
            .await
            .unwrap();
        assert_eq!(store.succeeded_history_count(), 1);

        // Same attempt_id (re-delivered after a stall between
        // mark_succeeded and frontier.ack): MUST be idempotent.
        store
            .mark_succeeded(&SuccessRecord {
                url: &url,
                attempt_id: &first,
                blob_path: "blob://1",
                content_hash: 1,
                outbound: &[],
            })
            .await
            .unwrap();
        assert_eq!(
            store.succeeded_history_count(),
            1,
            "redelivery of the same attempt must not duplicate the history row",
        );

        // Different attempt_id (a fresh delivery, e.g. the URL was
        // re-discovered): a new history row IS expected.
        let second = AttemptId::new("attempt-2");
        store
            .mark_succeeded(&SuccessRecord {
                url: &url,
                attempt_id: &second,
                blob_path: "blob://2",
                content_hash: 2,
                outbound: &[],
            })
            .await
            .unwrap();
        assert_eq!(
            store.succeeded_history_count(),
            2,
            "distinct attempt_ids must each produce a history row",
        );
    }
}
