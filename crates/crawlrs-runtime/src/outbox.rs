//! Outbox publisher: drains the metadata-side outbox into the
//! Frontier.
//!
//! Pattern: Producer / Consumer with at-least-once delivery, idempotent
//! at the consumer (Frontier seen-set). Each iteration leases a batch
//! of unpublished outbox entries, ships them through
//! `Frontier::submit_batch`, and (on success) marks them published.
//! The lease is acquired and released inside a single `Outbox::publish`
//! call, which makes concurrent publishers safe to scale horizontally:
//! each receives a disjoint batch.
//!
//! ## Flow
//!
//! 1. The worker pipeline calls `MetadataStore::mark_succeeded` inside
//!    one Postgres transaction. The metadata row, the history row,
//!    and the outbox rows commit atomically: there is no observable
//!    state where the metadata is advanced but the outbound URLs are
//!    missing.
//!
//! 2. This task wakes on a fixed interval and calls
//!    `Outbox::publish(batch_size, ship)` where `ship` is
//!    `|entries| frontier.submit_batch(entries)`. The publish method
//!    leases the rows, hands them to `ship`, and on success marks the
//!    leased rows published; on `ship` error it releases the lease
//!    without publishing so the rows reappear for the next caller.
//!
//! 3. If the publisher crashes between lease and ship completion, the
//!    txn rolls back and the rows reappear on the next attempt. The
//!    Frontier impl's per-URL seen-set absorbs duplicate XADDs as
//!    no-ops in the case where ship succeeded but the txn failed to
//!    commit (e.g. crash between submit_batch and COMMIT).
//!
//! ## What this is NOT
//!
//! - **Not** the source of truth for "have we enqueued this URL." The
//!   Frontier owns that. This task is just a transport.
//! - **Not** transactional with the worker pipeline. The worker
//!   commits its writes; this task ships them eventually. Worker
//!   correctness does not depend on this task running.
//! - **Not** order-preserving across batches; within a batch the
//!   `Outbox` returns rows in id order, but two consecutive
//!   batches' interleavings on the Frontier side are not guaranteed.
//!   That's fine: Frontier ordering is per-shard via XADD's monotonic
//!   IDs, not per-publisher.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::{Frontier, Outbox, ShipFn};
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
/// On each tick: lease at most `batch_size` unpublished rows, ship
/// them to the Frontier, mark them published. Errors are logged and
/// retried on the next tick; the publisher does not abort on a
/// transient backend failure because the rows stay durable in
/// Postgres until they're successfully shipped.
pub async fn outbox_publisher(
    outbox: Arc<dyn Outbox>,
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
        let progressed = publish_one_batch(&outbox, &frontier, batch_size).await;

        // If we just shipped a full batch, loop immediately: the
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
    let _ = publish_one_batch(&outbox, &frontier, batch_size).await;
    debug!("outbox_publisher exiting");
}

/// Lease, ship, and mark one batch. Returns the number of rows
/// successfully shipped (so the caller can decide whether to
/// loop-immediately or sleep).
async fn publish_one_batch(
    outbox: &Arc<dyn Outbox>,
    frontier: &Arc<dyn Frontier>,
    batch_size: usize,
) -> usize {
    let frontier = frontier.clone();
    let ship: ShipFn = Box::new(move |entries| {
        Box::pin(async move {
            frontier
                .submit_batch(entries.into_iter().map(|e| e.entry).collect())
                .await
                .map(|_| ())
        })
    });

    match outbox.publish(batch_size, ship).await {
        Ok(n) => {
            if n > 0 {
                metrics::counter!(
                    crate::metrics::OUTBOX_PUBLISHED_TOTAL,
                    "result" => crate::metrics::OUTBOX_RESULT_SUCCESS,
                )
                .increment(n as u64);
            }
            n
        }
        Err(e) => {
            warn!(error = %e, "outbox publish failed");
            metrics::counter!(
                crate::metrics::OUTBOX_PUBLISHED_TOTAL,
                "result" => crate::metrics::OUTBOX_RESULT_ERROR,
            )
            .increment(1);
            0
        }
    }
}
