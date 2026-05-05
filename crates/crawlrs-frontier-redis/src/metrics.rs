//! Frontier-layer metric names + descriptors.
//!
//! Names are an external operational contract. The `op` label
//! (Redis-command-seconds histogram) is constrained to the set below
//! for cardinality discipline.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub const FRONTIER_CLAIM_TOTAL: &str = "crawlrs_frontier_claim_total";
pub const FRONTIER_PENDING_CLAIMS: &str = "crawlrs_frontier_pending_claims";
pub const FRONTIER_CALL_SECONDS: &str = "crawlrs_frontier_call_seconds";
pub const FRONTIER_SUBMIT_BATCH_SIZE: &str = "crawlrs_frontier_submit_batch_size";
pub const FRONTIER_IN_FLIGHT_SECONDS: &str = "crawlrs_frontier_in_flight_seconds";
pub const FRONTIER_POOL_PENDING: &str = "crawlrs_frontier_pool_pending";

pub const OP_CLAIM: &str = "claim";
pub const OP_SUBMIT_BATCH: &str = "submit_batch";
pub const OP_ACK: &str = "ack";
pub const OP_NACK: &str = "nack";
pub const OP_AUTOCLAIM: &str = "autoclaim";
pub const OP_TICK: &str = "tick";

pub const OUTCOME_CLAIMED: &str = "claimed";
pub const OUTCOME_EMPTY: &str = "empty";
pub const OUTCOME_ERROR: &str = "error";

pub fn register() {
    describe_counter!(
        FRONTIER_CLAIM_TOTAL,
        "Per-shard claim attempts by outcome (claimed / empty / error)."
    );
    describe_gauge!(
        FRONTIER_PENDING_CLAIMS,
        Unit::Count,
        "Per-shard size of the consumer-group's pending entries list."
    );
    describe_histogram!(
        FRONTIER_CALL_SECONDS,
        Unit::Seconds,
        "Wall-clock duration of one Frontier trait method's Redis I/O."
    );
    describe_histogram!(
        FRONTIER_SUBMIT_BATCH_SIZE,
        Unit::Count,
        "Distribution of submit_batch entry counts. Surfaces \
         submit-time pressure profile (one batch of 1M vs 1000 of 1K)."
    );
    describe_histogram!(
        FRONTIER_IN_FLIGHT_SECONDS,
        Unit::Seconds,
        "Time from claim() to ack/nack. Surfaces stranded entries \
         earlier than XAUTOCLAIM's 5-minute reclaim threshold."
    );
    describe_gauge!(
        FRONTIER_POOL_PENDING,
        Unit::Count,
        "Currently-outstanding bb8 Redis pool connections used by the frontier."
    );
}
