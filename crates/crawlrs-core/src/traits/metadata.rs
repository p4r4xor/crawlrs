//! Per-URL metadata ledger trait.
//!
//! See [ADR-0009](../../../docs/decisions/0009-metadata-store.md) for
//! the design and [ADR-0011](../../../docs/decisions/0011-retry-budget-and-cross-run-dedup.md)
//! for how the runtime drives this trait. The Postgres-backed impl
//! lives in `crawlrs-metadata`.

use async_trait::async_trait;

use crate::error::Result;
use crate::traits::politeness::FailureKind;
use crate::types::UrlMetadata;
use crate::url::CanonicalUrl;

/// Per-URL ledger across all crawl runs. Distinct from the data-plane
/// `Store` (which writes the actual crawled content) and from the
/// `Frontier`'s per-run seen-set (which is in-run dedup only).
///
/// Use cases the metadata store enables:
///
/// - **Cross-run dedup**: "did we already crawl this URL in any
///   previous run?"
/// - **Dead-letter enforcement**: "this URL has failed N times; stop
///   re-trying."
/// - **Reverse blob lookup**: "where in the data-plane store does
///   this URL's body live?"
/// - **Resume**: a fresh run can skip URLs already `Succeeded`.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Read the metadata ledger for `url`. `None` means "never seen."
    async fn get(&self, url: &CanonicalUrl) -> Result<Option<UrlMetadata>>;

    /// Mark the URL as in-flight. Creates the row if absent (with
    /// `discovered_at = updated_at = now`); otherwise updates
    /// `status -> InProgress`, `last_run_id`, `depth`, and
    /// `updated_at`. `retry_count` is preserved across attempts and
    /// only reset by `mark_succeeded`.
    async fn mark_attempting(&self, url: &CanonicalUrl, run_id: &str, depth: u32) -> Result<()>;

    /// Successful fetch + persist. `status -> Succeeded`,
    /// `retry_count` reset to 0, `blob_path` and `content_hash`
    /// recorded, `updated_at` advanced.
    async fn mark_succeeded(
        &self,
        url: &CanonicalUrl,
        blob_path: &str,
        content_hash: u64,
    ) -> Result<()>;

    /// Transient failure: `status -> FailedTransient`, `retry_count`
    /// atomically incremented, `updated_at` advanced. Returns the new
    /// retry count so the caller can decide whether to give up.
    async fn mark_failed(&self, url: &CanonicalUrl, kind: FailureKind) -> Result<u32>;

    /// Give-up. `status -> PermanentlyFailed`, the URL is left in
    /// `url_metadata` (the DLQ is the row-set where status =
    /// `'permanently_failed'`); a `permanently_failed` event row
    /// carrying `reason` is appended to `url_history` so operators can
    /// inspect "what broke?" via SQL (per ADR-0011).
    async fn mark_permanently_failed(&self, url: &CanonicalUrl, reason: &str) -> Result<()>;
}
