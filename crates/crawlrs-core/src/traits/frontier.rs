//! `Frontier` trait: durable URL queue with at-least-once delivery.
//!
//! Concrete impl: `crawlrs-frontier-redis::RedisFrontier`.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{AttemptId, ClaimedMessage, UrlEntry, WorkerIdentity};

#[async_trait]
#[allow(clippy::len_without_is_empty)] // `len` is a queue-depth metric, not a Collection contract
pub trait Frontier: Send + Sync {
    /// Add one URL to the queue.
    ///
    /// Returns `true` if the URL was newly enqueued, `false` if the
    /// implementation determined it was already known and dropped the entry.
    async fn submit(&self, entry: UrlEntry) -> Result<bool>;

    /// Add many URLs at once.
    ///
    /// Returns the count of entries that were newly enqueued (i.e. not
    /// already known to the frontier). Implementations should prefer a
    /// single round-trip to the underlying store over N calls to `submit`.
    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<usize>;

    /// Pop the next URL to fetch on behalf of `identity`, or `None` if
    /// the queue is empty.
    ///
    /// Implementations using consumer-group delivery (e.g. Redis
    /// Streams) MUST use `identity` as the consumer name so that
    /// per-worker pending-entry-list (PEL) state is reattributed to the
    /// same worker across restarts. The returned `ClaimedMessage`
    /// carries an `AttemptId` correlating this delivery; the runtime
    /// passes that token to `ack`/`nack` and to downstream stores.
    async fn claim(&self, identity: &WorkerIdentity) -> Result<Option<ClaimedMessage>>;

    /// Pop up to `max` URLs in a single call. May return fewer (including
    /// zero) than `max` if the queue is shallow. Implementations should
    /// prefer one round-trip over `max` calls to `claim`.
    async fn claim_batch(
        &self,
        identity: &WorkerIdentity,
        max: usize,
    ) -> Result<Vec<ClaimedMessage>>;

    /// Approximate queue depth, for metrics and shutdown checks.
    async fn len(&self) -> Result<usize>;

    /// Confirm a previously-claimed URL has been fully processed.
    ///
    /// `attempt` is the `AttemptId` from the `ClaimedMessage` returned
    /// by the matching `claim` call.
    ///
    /// For implementations using at-least-once delivery (Redis Streams
    /// consumer groups, SQS visibility timeouts, etc.) this commits the
    /// claim so the URL is not re-delivered to a different worker.
    /// Implementations without that semantic may no-op.
    ///
    /// Calling `ack` for an `AttemptId` that was never claimed (or has
    /// already been acked) must be a no-op, not an error. This makes
    /// the runtime's pipeline idempotent under retries.
    async fn ack(&self, attempt: &AttemptId) -> Result<()>;

    /// Release a previously-claimed delivery back to the pending pool
    /// without processing.
    ///
    /// Used when the runtime decides the URL should be re-claimed
    /// later (e.g. transient politeness backoff, the worker is
    /// shutting down). Whoever next claims this URL receives a fresh
    /// `AttemptId`; the original delivery's PEL entry remains until the
    /// implementation's reclaim path picks it up.
    ///
    /// Implementations whose delivery model doesn't distinguish ack
    /// from nack may treat this identically to `ack` (the URL is
    /// merely consumed) or may no-op.
    async fn nack(&self, attempt: &AttemptId) -> Result<()>;

    /// Periodic maintenance hook. The runtime invokes this on a
    /// configurable cadence (e.g. every 30s) and once during graceful
    /// shutdown. Implementations use it for whatever bookkeeping
    /// they need: Redis Streams uses it for `XAUTOCLAIM`-based reclaim
    /// of stranded entries; an in-memory impl may have nothing to do.
    ///
    /// Returns the number of items affected (e.g. reclaimed) so the
    /// runtime can log / metric. The default impl returns 0.
    async fn tick(&self) -> Result<usize> {
        Ok(0)
    }
}
