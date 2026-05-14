//! Frontier-layer metric names + descriptors.
//!
//! Names are an external operational contract: dashboards and alerts
//! depend on them, so renames are breaking changes. Label cardinality
//! is kept bounded: `op` is a small fixed set; `outcome` is the three
//! claim verdicts; `verdict` is the two bloom paths.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub const FRONTIER_CLAIM_TOTAL: &str = "crawlrs_frontier_claim_total";
pub const FRONTIER_CALL_SECONDS: &str = "crawlrs_frontier_call_seconds";
pub const FRONTIER_SUBMIT_BATCH_SIZE: &str = "crawlrs_frontier_submit_batch_size";
pub const FRONTIER_POOL_PENDING: &str = "crawlrs_frontier_pool_pending";

/// Counter of submit-time bloom outcomes by `verdict={new,duplicate}`.
/// Surfaces the dedup hit rate; sustained 100% `duplicate` means the
/// seed feed is exhausted.
pub const FRONTIER_BLOOM_TOTAL: &str = "crawlrs_frontier_bloom_total";

/// Counter of leases reclaimed by tick by `reason=expired`. Should
/// be near-zero in steady state; spikes mean workers are crashing or
/// the lease timeout is too short for the p99 fetch.
pub const FRONTIER_LEASE_RECLAIM_TOTAL: &str = "crawlrs_frontier_lease_reclaim_total";

/// Counter of hosts moved from `wake` to `ready` per promoter tick.
pub const FRONTIER_PROMOTED_TOTAL: &str = "crawlrs_frontier_promoted_total";

/// Per-shard gauge of `ready` list length. A non-trivial value with
/// idle workers indicates a worker-pool sizing mismatch; a near-zero
/// value with hosts present in `wake` is the steady state.
pub const FRONTIER_READY_LENGTH: &str = "crawlrs_frontier_ready_length";

/// Per-shard gauge of `inflight` ZSET length. Tracks in-flight URLs;
/// a value much larger than workers * shards means leases aren't
/// being acked (long fetches, leaks, or the worker pool is stuck).
pub const FRONTIER_INFLIGHT_LENGTH: &str = "crawlrs_frontier_inflight_length";

pub const OP_SUBMIT: &str = "submit";
pub const OP_SUBMIT_BATCH: &str = "submit_batch";
pub const OP_CLAIM: &str = "claim";
pub const OP_ACK: &str = "ack";
pub const OP_ADVANCE_WAKE: &str = "advance_wake";
pub const OP_TICK: &str = "tick";

pub const OUTCOME_CLAIMED: &str = "claimed";
pub const OUTCOME_EMPTY: &str = "empty";
pub const OUTCOME_EMPTY_HINT: &str = "empty_hint";
pub const OUTCOME_ERROR: &str = "error";

pub const BLOOM_NEW: &str = "new";
pub const BLOOM_DUPLICATE: &str = "duplicate";

pub const RECLAIM_REASON_EXPIRED: &str = "expired";

pub fn register() {
    describe_counter!(
        FRONTIER_CLAIM_TOTAL,
        "Claim attempts by outcome: claimed / empty / empty_hint / error."
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
    describe_gauge!(
        FRONTIER_POOL_PENDING,
        Unit::Count,
        "Currently-outstanding bb8 Redis pool connections used by the frontier."
    );
    describe_counter!(
        FRONTIER_BLOOM_TOTAL,
        "Submit-time bloom outcomes: new (URL accepted into the \
         queue) vs duplicate (URL already submitted in any prior \
         run)."
    );
    describe_counter!(
        FRONTIER_LEASE_RECLAIM_TOTAL,
        "Leases reclaimed by tick (reason=expired). Should be near \
         zero in steady state; sustained value indicates worker \
         crashes or a too-short lease timeout."
    );
    describe_counter!(
        FRONTIER_PROMOTED_TOTAL,
        "Hosts moved from `wake` to `ready` per tick. \
         Stable cadence; spikes track host-fan-in shape."
    );
    describe_gauge!(
        FRONTIER_READY_LENGTH,
        Unit::Count,
        "Per-shard `ready` list length. Watch alongside \
         `crawlrs_workers_active`: idle workers + non-empty ready \
         is a sizing mismatch."
    );
    describe_gauge!(
        FRONTIER_INFLIGHT_LENGTH,
        Unit::Count,
        "Per-shard `inflight` ZSET length. Bounded above by \
         workers * shards in steady state."
    );
}
