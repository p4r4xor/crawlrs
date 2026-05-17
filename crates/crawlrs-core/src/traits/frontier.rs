//! `Frontier` trait: per-host URL queue with at-least-once delivery
//! and integrated wake-time scheduling.
//!
//! The frontier is the single owner of:
//!   - per-host URL queues (FIFO by submit order),
//!   - a wake ZSET that gates which hosts are eligible to claim,
//!   - a pre-computed ready-host LIST that workers pop from,
//!   - a lease ZSET for crash recovery,
//!   - submit-time bloom dedup,
//!   - a small URL HASH so queue entries hold only ID + lease info.
//!
//! The politeness layer does NOT own wake-time state; it returns a
//! `NextWake` plan that the runtime hands to `advance_wake` here. The
//! trait surface reflects that split: the frontier publishes `submit`,
//! `claim`, `advance_wake`, and `ack` - one verb per intent, no
//! overlap.
//!
//! Concrete impl: `crawlrs-frontier::RedisFrontier`.

use std::time::Instant;

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{AttemptId, UrlEntry, UrlId, WorkerIdentity};

/// Result of one `Frontier::claim` call.
///
/// Three states because a worker that finds nothing must choose
/// between "sleep a little and retry" (a slot is coming up soon -
/// `EmptyHint`) and "sleep the configured idle floor" (truly nothing
/// scheduled - `Empty`). Folding both into `Option<...>` would force
/// every worker to either over-poll or wait for the promoter loop to
/// catch up.
///
/// Lives next to the trait per the project's "types tightly coupled
/// to one trait live with the trait" rule.
#[derive(Debug, Clone)]
pub enum ClaimOutcome {
    /// A URL is ready and the worker now holds its lease. `entry` is
    /// boxed because `UrlEntry` dominates the variant size (URLs are
    /// up to a few KB); without the indirection the enum's stack
    /// footprint would force every `Result<ClaimOutcome>` to carry
    /// that size on the hot path.
    Claimed {
        url_id: UrlId,
        entry: Box<UrlEntry>,
        attempt_id: AttemptId,
    },
    /// Nothing is ready right now, but the wake ZSET has at least one
    /// entry scheduled in the near future. The worker should sleep
    /// until `sleep_until` (capped by its idle floor) before claiming
    /// again.
    EmptyHint { sleep_until: Instant },
    /// Nothing is ready and the wake ZSET is empty. Sleep the
    /// configured idle floor before claiming again.
    Empty,
}

/// Outcome of one `Frontier::submit` call.
///
/// Two states mirror the Lua-script return: "URL accepted into the
/// queue" and "URL already-known (bloom hit)". The bloom-hit case is
/// the dominant outcome under steady-state crawling and the metric /
/// log shapes split on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Newly enqueued onto a per-host queue. Bloom now contains the
    /// URL; the next submit of the same URL hits the dedup path.
    Queued,
    /// Already-known per the bloom filter; silently dropped.
    SkippedDuplicate,
}

/// Aggregated outcome of one `Frontier::submit_batch` call.
///
/// `queued` and `rejected` partition the batch alongside the
/// implicit "bloom-duplicate" count (which equals
/// `batch_size - queued - rejected`). Two named fields rather
/// than three because the duplicate count is derivable and rarely
/// the operator-facing number. Bloom-duplicates are NOT counted
/// under `rejected`; today `rejected` is exclusively the per-host
/// quota signal, and the comment on the field documents that.
///
/// Pattern: Parameter Object on the return side. Keeps the trait
/// method's return monomorphic so adding a future per-batch field
/// (e.g. a per-URL audit log) doesn't break callers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SubmitBatchOutcome {
    /// URLs that the bloom filter accepted as new AND that the
    /// per-host counter accepted as within quota; now waiting on
    /// their host's queue. Symmetric with `SubmitOutcome::Queued`
    /// at the single-URL level (each `Queued` contributes +1).
    pub queued: usize,
    /// URLs the host's `[crawl].max_urls` counter rejected. Counter-first
    /// ordering means these are NOT marked in the bloom and remain
    /// eligible for a future run (where the counter resets to zero).
    /// The only rejection reason at submit time today; if more are
    /// introduced (e.g. submit-time scope checks), break this into
    /// a `RejectionReason` enum rather than overloading the field.
    pub rejected: usize,
}

#[async_trait]
#[allow(clippy::len_without_is_empty)] // `len` is a queue-depth metric, not a Collection contract
pub trait Frontier: Send + Sync {
    /// Add one URL to the queue.
    ///
    /// Returns the [`SubmitOutcome`] for this URL: `Queued` if newly
    /// enqueued, `SkippedDuplicate` if the bloom filter already
    /// contained it. Per-host queues grow unbounded by design; the
    /// politeness layer rate-limits per host so a long queue is just
    /// the work waiting in line for its host's next wake-time slot.
    async fn submit(&self, entry: UrlEntry) -> Result<SubmitOutcome>;

    /// Add many URLs at once.
    ///
    /// Returns a [`SubmitBatchOutcome`] partitioning the batch into
    /// newly-queued URLs and URLs rejected by their host's quota
    /// (`[crawl].max_urls`). Bloom-duplicate URLs are not counted
    /// in either; they're the rest of the batch.
    ///
    /// Implementations should prefer a single round-trip to the
    /// underlying store over N calls to `submit`. Quota enforcement
    /// is per-URL atomic with the bloom check; the
    /// [`crawl_scope`](crate::CrawlScope) passed at construction
    /// resolves the per-host cap.
    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<SubmitBatchOutcome>;

    /// Pop the next URL to fetch on behalf of `identity`, or return
    /// `Empty`/`EmptyHint` if no host is currently claimable.
    ///
    /// On `Claimed`, the worker holds a lease on the URL until the
    /// matching `ack` (or the lease times out and the impl's reclaim
    /// path re-pushes the URL). Each delivery carries a fresh
    /// `AttemptId`; downstream stores correlate side-effects by it.
    ///
    /// Implementations using leased delivery (e.g. Redis with a lease
    /// ZSET) MUST use `identity` to attribute the lease so a
    /// restarting worker with the same identity can recover its
    /// previously-in-flight URLs without waiting for the reclaim
    /// timeout.
    async fn claim(&self, identity: &WorkerIdentity) -> Result<ClaimOutcome>;

    /// Approximate queue depth, for metrics and shutdown checks.
    /// Sum over all owned shards' per-host queues (impl-defined;
    /// details in the metrics surface).
    async fn len(&self) -> Result<usize>;

    /// Update a host's wake time. Idempotent under XX|GT semantics:
    /// only writes if the new `until` is strictly later than any
    /// existing value, so the runtime can apply politeness plans
    /// without worrying about concurrent worker writes from a fan-out
    /// of the same host.
    ///
    /// Called by the runtime after `Politeness::record_fetch` and
    /// `Politeness::record_failure` return a [`NextWake`].
    ///
    /// [`NextWake`]: crate::types::NextWake
    async fn advance_wake(&self, host: &str, until: Instant) -> Result<()>;

    /// Confirm a previously-claimed URL has been fully processed.
    ///
    /// `attempt` is the `AttemptId` from the matching `Claimed`
    /// `ClaimOutcome`. Commits the lease release; the URL leaves the
    /// inflight ZSET and the reclaim path will not re-deliver it.
    ///
    /// Calling `ack` for an `AttemptId` that was never claimed (or has
    /// already been acked) must be a no-op, not an error. This keeps
    /// the runtime's pipeline idempotent under retries.
    async fn ack(&self, attempt: &AttemptId) -> Result<()>;

    /// Periodic maintenance hook. The runtime invokes this on a
    /// configurable cadence (e.g. every 30s) and once during graceful
    /// shutdown. Implementations use it for whatever bookkeeping
    /// they need (reclaim of expired leases, promotion of ready hosts,
    /// metric refresh). The default impl returns 0.
    ///
    /// Returns the number of items affected (e.g. reclaimed) so the
    /// runtime can log / metric.
    async fn tick(&self) -> Result<usize> {
        Ok(0)
    }
}
