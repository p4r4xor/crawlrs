//! Periodic maintenance task: refreshes the metrics that aren't on
//! the hot path (gauges that need a poll-and-set rather than
//! event-driven update).
//!
//! Runs at a fixed cadence (15s default, matching the typical
//! Prometheus scrape interval). Each tick:
//!
//! - `frontier.record_pending_metrics()` - emits per-shard PEL size
//!   and the bb8 pool gauge.
//! - `politeness.record_pool_metrics()` - emits the bb8 pool gauge.
//! - `metadata.record_pool_metrics()` - emits the sqlx pool gauge.
//! - `metadata.dlq_size()` - reads the DLQ count, which this loop then
//!   publishes to the DLQ gauge.
//!
//! Errors are logged and swallowed; a transient Postgres / Redis
//! blip shouldn't kill the maintenance loop.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_frontier::RedisFrontier;
use crawlrs_metadata::PostgresMetadataStore;
use crawlrs_politeness::CompositePoliteness;
use tokio::sync::watch;
use tracing::{debug, warn};

const DEFAULT_INTERVAL: Duration = Duration::from_secs(15);

pub async fn run(
    frontier: Arc<RedisFrontier>,
    politeness: Arc<CompositePoliteness>,
    metadata: Arc<PostgresMetadataStore>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(DEFAULT_INTERVAL);
    ticker.tick().await; // consume the immediate-fire tick
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => {
                debug!("maintenance loop: shutdown signal received");
                break;
            }
        }

        if let Err(e) = frontier.record_pending_metrics().await {
            warn!(error = %e, "frontier.record_pending_metrics failed");
        }
        politeness.record_pool_metrics();
        metadata.record_pool_metrics();
        match metadata.dlq_size().await {
            Ok(dlq_size) => {
                metrics::gauge!(crawlrs_metadata::metrics::DLQ_SIZE).set(dlq_size as f64);
            }
            Err(e) => warn!(error = %e, "metadata.dlq_size failed"),
        }
    }
}
