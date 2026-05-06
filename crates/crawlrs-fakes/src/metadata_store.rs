//! `InMemoryMetadataStore`: a `MetadataStore` impl backed by a HashMap.
//!
//! Mirrors the `PostgresMetadataStore` semantics (atomic mark
//! transitions, retry counter preserved across attempts and reset
//! only on success) without needing a Postgres container. Suitable
//! for tests of the runtime; not suitable for production
//! (no persistence, no transactions across restarts).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use crawlrs_core::{
    AttemptId, CanonicalUrl, Error, FailureKind, MetadataStore, Result, UrlMetadata, UrlStatus,
};

/// One row per URL, last-write-wins on every mutable field except
/// `discovered_at`. The DLQ list is populated on
/// `mark_permanently_failed` so tests can assert on it.
///
/// `succeeded_attempts` mirrors the Postgres impl's
/// `url_history.attempt_id` UNIQUE constraint: if the same `(url,
/// attempt_id)` pair is `mark_succeeded`'d twice (e.g. because
/// `XAUTOCLAIM` redelivered an attempt that got past the DB write but
/// not the `XACK`), the second call is a no-op on the history side.
/// `succeeded_history_count` exposes that fact for tests.
#[derive(Default)]
pub struct InMemoryMetadataStore {
    rows: Mutex<HashMap<String, UrlMetadata>>,
    dlq: Mutex<Vec<(String, String)>>,
    /// `(url, attempt_id)` pairs already recorded by `mark_succeeded`.
    succeeded_attempts: Mutex<HashSet<(String, String)>>,
    /// Count of distinct succeeded history rows actually appended (i.e.
    /// excluding ON CONFLICT DO NOTHING dedupes).
    succeeded_history_count: Mutex<u64>,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of URLs currently in `PermanentlyFailed`. Used by tests
    /// asserting that the retry budget actually fired.
    pub fn dlq_count(&self) -> usize {
        self.dlq.lock().unwrap().len()
    }

    /// Snapshot of the DLQ entries: `(url, reason)` pairs in insert
    /// order. Useful for tests that want to verify the failure reason
    /// string the runtime constructed.
    pub fn dlq_entries(&self) -> Vec<(String, String)> {
        self.dlq.lock().unwrap().clone()
    }

    /// Number of distinct `mark_succeeded` history rows recorded.
    /// Counts each unique `(url, attempt_id)` once; redelivery of the
    /// same attempt does NOT increment. Mirrors the Postgres history
    /// row count.
    pub fn succeeded_history_count(&self) -> u64 {
        *self.succeeded_history_count.lock().unwrap()
    }
}

#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn get(&self, url: &CanonicalUrl) -> Result<Option<UrlMetadata>> {
        Ok(self.rows.lock().unwrap().get(url.as_str()).cloned())
    }

    async fn mark_attempting(&self, url: &CanonicalUrl, run_id: &str, depth: u32) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        let now = SystemTime::now();
        rows.entry(url.as_str().to_string())
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

    async fn mark_succeeded(
        &self,
        url: &CanonicalUrl,
        attempt_id: &AttemptId,
        blob_path: &str,
        content_hash: u64,
    ) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .get_mut(url.as_str())
            .ok_or_else(|| Error::Metadata(format!("mark_succeeded: missing row for {url}")))?;
        row.status = UrlStatus::Succeeded;
        row.retry_count = 0;
        row.blob_path = Some(blob_path.to_string());
        row.content_hash = Some(content_hash);
        row.updated_at = SystemTime::now();
        drop(rows);

        // Mirror the Postgres `(url_id, attempt_id)` unique constraint:
        // only the first call per `(url, attempt_id)` appends a history
        // row. Redelivery of the same attempt is a no-op on history.
        let key = (url.as_str().to_string(), attempt_id.as_str().to_string());
        let mut seen = self.succeeded_attempts.lock().unwrap();
        if seen.insert(key) {
            *self.succeeded_history_count.lock().unwrap() += 1;
        }
        Ok(())
    }

    async fn mark_failed(&self, url: &CanonicalUrl, _kind: FailureKind) -> Result<u32> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .get_mut(url.as_str())
            .ok_or_else(|| Error::Metadata(format!("mark_failed: missing row for {url}")))?;
        row.retry_count += 1;
        row.status = UrlStatus::FailedTransient;
        row.updated_at = SystemTime::now();
        Ok(row.retry_count)
    }

    async fn mark_permanently_failed(&self, url: &CanonicalUrl, reason: &str) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows.get_mut(url.as_str()).ok_or_else(|| {
            Error::Metadata(format!("mark_permanently_failed: missing row for {url}"))
        })?;
        row.status = UrlStatus::PermanentlyFailed;
        row.updated_at = SystemTime::now();
        drop(rows);
        self.dlq
            .lock()
            .unwrap()
            .push((url.as_str().to_string(), reason.to_string()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crawlrs_core::CanonicalUrl;

    #[tokio::test]
    async fn mark_succeeded_dedupes_by_attempt_id() {
        let store = InMemoryMetadataStore::new();
        let url = CanonicalUrl::parse("https://a.test/").unwrap();
        store.mark_attempting(&url, "run-1", 0).await.unwrap();

        // First attempt: one history row.
        let first = AttemptId::new("attempt-1");
        store
            .mark_succeeded(&url, &first, "blob://1", 1)
            .await
            .unwrap();
        assert_eq!(store.succeeded_history_count(), 1);

        // Same attempt_id (re-delivered after a stall between
        // mark_succeeded and frontier.ack): MUST be idempotent.
        store
            .mark_succeeded(&url, &first, "blob://1", 1)
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
            .mark_succeeded(&url, &second, "blob://2", 2)
            .await
            .unwrap();
        assert_eq!(
            store.succeeded_history_count(),
            2,
            "distinct attempt_ids must each produce a history row",
        );
    }
}
