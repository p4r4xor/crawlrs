//! Outbox publisher: drains the metadata-side outbox into the
//! Frontier.
//!
//! Pattern: Producer / Consumer with at-least-once delivery, idempotent
//! at the consumer (Frontier seen-set).
//!
//! ## Flow
//!
//! 1. The worker pipeline calls
//!    `MetadataStore::mark_succeeded(url, attempt_id, blob_path,
//!    content_hash, outbound)` inside one Postgres transaction. The
//!    metadata row, the history row, and the outbox rows commit
//!    atomically: there is no observable state where the metadata is
//!    advanced but the outbound URLs are missing.
//!
//! 2. This task wakes on a fixed interval, calls
//!    `OutboxReader::fetch_unpublished(batch_size)`, hands the
//!    resulting [`crawlrs_core::OutboxEntry`]s to
//!    `Frontier::submit_batch`, then calls
//!    `OutboxReader::mark_published(ids)`.
//!
//! 3. If the publisher crashes between submit and mark, the rows
//!    stay unpublished. On the next interval the publisher re-drains
//!    them; the Frontier impl's per-URL seen-set absorbs the
//!    duplicate XADDs as no-ops.
//!
//! ## What this is NOT
//!
//! - **Not** the source of truth for "have we enqueued this URL." The
//!   Frontier owns that. This task is just a transport.
//! - **Not** transactional with the worker pipeline. The worker
//!   commits its writes; this task ships them eventually. Worker
//!   correctness does not depend on this task running.
//! - **Not** order-preserving across batches; within a batch the
//!   `OutboxReader` returns rows in id order, but two consecutive
//!   batches' interleavings on the Frontier side are not guaranteed.
//!   That's fine: Frontier ordering is per-shard via XADD's monotonic
//!   IDs, not per-publisher.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::{Frontier, OutboxReader, UrlEntry};
use tokio::sync::watch;
use tracing::{debug, warn};

/// How many outbox rows to drain per iteration. Bounded so a backlog
/// doesn't translate into one giant `XADD MAXLEN` blast or a
/// long-running Postgres transaction.
const DEFAULT_BATCH_SIZE: usize = 256;

/// Default cadence between drains. Conservative; tune up for
/// high-discovery workloads where outbox backlog matters more than
/// the polling cost.
pub const DEFAULT_PUBLISH_INTERVAL: Duration = Duration::from_millis(250);

/// Run the outbox publisher until shutdown is signalled.
///
/// On each tick: drain at most `batch_size` unpublished rows, submit
/// them to the Frontier, mark them published. Errors are logged and
/// retried on the next tick; the publisher does not abort on a
/// transient backend failure because the rows stay durable in
/// Postgres until they're successfully shipped.
pub async fn outbox_publisher(
    outbox: Arc<dyn OutboxReader>,
    frontier: Arc<dyn Frontier>,
    mut shutdown: watch::Receiver<bool>,
    interval: Duration,
) {
    let batch_size = DEFAULT_BATCH_SIZE;
    debug!(
        batch_size,
        interval_ms = interval.as_millis() as u64,
        "outbox_publisher starting"
    );
    while !*shutdown.borrow() {
        let progressed = drain_once(&outbox, &frontier, batch_size).await;

        // If we just drained a full batch, loop immediately: the
        // outbox is likely backlogged and we don't want to wait
        // `interval` between consecutive batches when we know there
        // is more work pending. Otherwise sleep with shutdown
        // selectability so SIGTERM exits promptly.
        if progressed >= batch_size {
            continue;
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => break,
        }
    }
    // One final drain on graceful shutdown so the metadata-side
    // commits the worker pool already produced are not stranded in
    // the outbox until the next process starts.
    let _ = drain_once(&outbox, &frontier, batch_size).await;
    debug!("outbox_publisher exiting");
}

/// One iteration: fetch unpublished rows, submit them to the
/// Frontier, mark them published. Returns the number of rows
/// successfully shipped (so the caller can decide whether to
/// loop-immediately or sleep).
async fn drain_once(
    outbox: &Arc<dyn OutboxReader>,
    frontier: &Arc<dyn Frontier>,
    batch_size: usize,
) -> usize {
    let rows = match outbox.fetch_unpublished(batch_size).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "outbox.fetch_unpublished failed");
            metrics::counter!(
                crate::metrics::OUTBOX_PUBLISHED_TOTAL,
                "result" => crate::metrics::OUTBOX_RESULT_ERROR,
            )
            .increment(1);
            return 0;
        }
    };
    if rows.is_empty() {
        return 0;
    }

    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let entries: Vec<UrlEntry> = rows.into_iter().map(|r| r.entry).collect();
    let n = entries.len();

    if let Err(e) = frontier.submit_batch(entries).await {
        // We did not advance: leave rows unpublished. Next tick will
        // retry. The Frontier-side seen-set absorbs any duplicate
        // submits in the case where a partial batch DID get through
        // before the error surfaced.
        warn!(error = %e, n, "outbox -> frontier submit_batch failed");
        metrics::counter!(
            crate::metrics::OUTBOX_PUBLISHED_TOTAL,
            "result" => crate::metrics::OUTBOX_RESULT_ERROR,
        )
        .increment(1);
        return 0;
    }

    if let Err(e) = outbox.mark_published(&ids).await {
        // Submitted but not marked: a downstream re-drain will
        // re-XADD; Frontier dedup absorbs it. Surface the error so
        // operators notice; correctness is intact.
        warn!(error = %e, n, "outbox.mark_published failed");
        metrics::counter!(
            crate::metrics::OUTBOX_PUBLISHED_TOTAL,
            "result" => crate::metrics::OUTBOX_RESULT_ERROR,
        )
        .increment(1);
    }
    metrics::counter!(
        crate::metrics::OUTBOX_PUBLISHED_TOTAL,
        "result" => crate::metrics::OUTBOX_RESULT_SUCCESS,
    )
    .increment(n as u64);
    n
}
