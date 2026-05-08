//! Per-URL metadata ledger trait.
//!
//! The Postgres-backed impl lives in `crawlrs-metadata`.

use async_trait::async_trait;

use crate::error::Result;
use crate::traits::politeness::FailureKind;
use crate::types::{AttemptId, UrlEntry, UrlMetadata};
use crate::url::CanonicalUrl;

/// Parameter object for [`MetadataStore::mark_succeeded`].
///
/// Pattern: Introduce Parameter Object. Bundling the five inputs
/// (`url`, `attempt_id`, `blob_path`, `content_hash`, `outbound`)
/// into one struct keeps the trait method's arity at one and gives
/// future additions a place to land without re-shaping the trait.
/// Mirrors [`StoreRecord`](crate::types::StoreRecord), which solves
/// the same problem on the data-plane side.
#[derive(Debug, Clone, Copy)]
pub struct SuccessRecord<'a> {
    /// The URL that was fetched.
    pub url: &'a CanonicalUrl,
    /// Correlation token from the [`crate::traits::frontier::ClaimedMessage`]
    /// that drove this attempt. Implementations use it to dedupe on
    /// redelivery.
    pub attempt_id: &'a AttemptId,
    /// Storage path returned by `Store::write` for the body.
    pub blob_path: &'a str,
    /// Hash of the body bytes. Used for change detection across runs.
    pub content_hash: u64,
    /// Newly-discovered URLs to enqueue. Persisted in the same
    /// atomicity domain as the metadata write; a downstream
    /// publisher drains them into the Frontier.
    pub outbound: &'a [UrlEntry],
}

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
    ///
    /// `record.attempt_id` is the correlation token from the
    /// [`crate::traits::frontier::ClaimedMessage`] that drove this
    /// attempt. Implementations that maintain an append-only history
    /// MUST treat `(url, attempt_id)` as the uniqueness key on the
    /// history row so that re-delivery of the same attempt (e.g. via
    /// `XAUTOCLAIM` after a stall between this call and
    /// `frontier.ack`) does not duplicate ledger entries.
    ///
    /// `record.outbound` is the set of newly-discovered URLs to
    /// enqueue into the Frontier. Implementations MUST persist these
    /// to the outbox in the same transaction as the metadata write,
    /// so the two effects are atomic from the caller's perspective.
    /// The publisher (driven by
    /// [`crate::traits::outbox::Outbox`]) drains the outbox
    /// asynchronously and writes the URLs into the Frontier
    /// at-least-once; per-URL dedupe at the Frontier side absorbs
    /// the redelivery case. The same uniqueness rule applies: a
    /// redelivered `(url, attempt_id)` MUST NOT duplicate outbox
    /// rows.
    async fn mark_succeeded(&self, record: &SuccessRecord<'_>) -> Result<()>;

    /// Transient failure: `status -> FailedTransient`, `retry_count`
    /// atomically incremented, `updated_at` advanced. Returns the new
    /// retry count so the caller can decide whether to give up.
    async fn mark_failed(&self, url: &CanonicalUrl, kind: FailureKind) -> Result<u32>;

    /// Give-up. `status -> PermanentlyFailed`, the URL is left in
    /// `url_metadata` (the DLQ is the row-set where status =
    /// `'permanently_failed'`); a `permanently_failed` event row
    /// carrying `reason` is appended to `url_history` so operators can
    /// inspect "what broke?" via SQL.
    async fn mark_permanently_failed(&self, url: &CanonicalUrl, reason: &str) -> Result<()>;
}
