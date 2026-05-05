//! `InMemoryMetadataStore`: a `MetadataStore` impl backed by a HashMap.
//!
//! Mirrors the `PostgresMetadataStore` semantics (atomic mark
//! transitions, retry counter preserved across attempts and reset
//! only on success) without needing a Postgres container. Suitable
//! for tests of the runtime; not suitable for production
//! (no persistence, no transactions across restarts).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use crawlrs_core::{
    CanonicalUrl, Error, FailureKind, MetadataStore, Result, UrlMetadata, UrlStatus,
};

/// One row per URL, last-write-wins on every mutable field except
/// `discovered_at`. The DLQ list is populated on
/// `mark_permanently_failed` so tests can assert on it.
#[derive(Default)]
pub struct InMemoryMetadataStore {
    rows: Mutex<HashMap<String, UrlMetadata>>,
    dlq: Mutex<Vec<(String, String)>>,
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
