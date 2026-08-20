//! Metadata-layer metric names + descriptors.

use std::time::Instant;

use metrics::{Unit, describe_gauge, describe_histogram};

pub const METADATA_QUERY_SECONDS: &str = "crawlrs_metadata_query_seconds";
pub const METADATA_POOL_PENDING: &str = "crawlrs_metadata_pool_pending";
pub const DLQ_SIZE: &str = "crawlrs_dlq_size";

pub const OP_GET: &str = "get";
pub const OP_MARK_ATTEMPTING: &str = "mark_attempting";
pub const OP_MARK_SUCCEEDED: &str = "mark_succeeded";
pub const OP_MARK_FAILED: &str = "mark_failed";
pub const OP_MARK_PERMANENTLY_FAILED: &str = "mark_permanently_failed";
pub const OP_MARK_DISCOVERED_SKIPPED: &str = "mark_discovered_skipped";

pub fn register() {
    describe_histogram!(
        METADATA_QUERY_SECONDS,
        Unit::Seconds,
        "Wall-clock duration of one MetadataStore trait method, by op."
    );
    describe_gauge!(
        METADATA_POOL_PENDING,
        Unit::Count,
        "Currently-outstanding (checked-out) sqlx pool connections: pool \
         size minus idle. sqlx does not expose the waiter count, so this \
         is the closest available proxy for pool pressure. (The metric \
         name is retained for dashboard compatibility.)"
    );
    describe_gauge!(
        DLQ_SIZE,
        Unit::Count,
        "Count of url_metadata rows in the permanently_failed state (the DLQ)."
    );
}

/// RAII timer: emits `METADATA_QUERY_SECONDS{op}` on drop. Lets each
/// trait-method body keep using `?` for error propagation while still
/// covering the failure path in the histogram (the timer fires
/// regardless of how the body returns).
pub(crate) struct QueryTimer {
    op: &'static str,
    started_at: Instant,
}

impl QueryTimer {
    pub(crate) fn new(op: &'static str) -> Self {
        Self {
            op,
            started_at: Instant::now(),
        }
    }
}

impl Drop for QueryTimer {
    fn drop(&mut self) {
        metrics::histogram!(METADATA_QUERY_SECONDS, "op" => self.op)
            .record(self.started_at.elapsed().as_secs_f64());
    }
}
