//! Politeness-layer metric names + descriptors.
//!
//! The robots cache hit/miss counters are at "did we avoid a network
//! fetch?" granularity (in-process and Redis tiers both count as
//! hits; only network fetches are misses).

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub const ROBOTS_CACHE_HITS_TOTAL: &str = "crawlrs_robots_cache_hits_total";
pub const ROBOTS_CACHE_MISSES_TOTAL: &str = "crawlrs_robots_cache_misses_total";
pub const POLITENESS_BACKOFF_SECONDS: &str = "crawlrs_politeness_backoff_seconds";
pub const POLITENESS_BACKOFF_SOURCE_TOTAL: &str = "crawlrs_politeness_backoff_source_total";
pub const POLITENESS_CIRCUIT_OPEN_TOTAL: &str = "crawlrs_politeness_circuit_open_total";
pub const POLITENESS_CHECK_TOTAL: &str = "crawlrs_politeness_check_total";
pub const POLITENESS_POOL_PENDING: &str = "crawlrs_politeness_pool_pending";

pub const DECISION_ALLOW: &str = "allow";
pub const DECISION_DELAY: &str = "delay";
pub const DECISION_DISALLOW_ROBOTS: &str = "disallow_robots";
pub const DECISION_DISALLOW_EXCLUDED: &str = "disallow_excluded";
pub const DECISION_DISALLOW_CIRCUIT: &str = "disallow_circuit";

pub const SOURCE_SERVER_HINT: &str = "server_hint";
pub const SOURCE_COMPUTED: &str = "computed";
pub const SOURCE_CAPPED: &str = "capped";

pub fn register() {
    describe_counter!(
        ROBOTS_CACHE_HITS_TOTAL,
        "Robots.txt requests answered from in-process LRU or Redis cache."
    );
    describe_counter!(
        ROBOTS_CACHE_MISSES_TOTAL,
        "Robots.txt requests that required a network fetch."
    );
    describe_histogram!(
        POLITENESS_BACKOFF_SECONDS,
        Unit::Seconds,
        "Distribution of computed per-host backoff durations on failures."
    );
    describe_counter!(
        POLITENESS_BACKOFF_SOURCE_TOTAL,
        "Backoff-value attribution: came from server Retry-After hint, our \
         exponential math, or hit the max_backoff cap."
    );
    describe_counter!(
        POLITENESS_CIRCUIT_OPEN_TOTAL,
        "Per-host circuit breaker tripped (Disallow returned by check)."
    );
    describe_counter!(
        POLITENESS_CHECK_TOTAL,
        "Politeness::check verdict distribution."
    );
    describe_gauge!(
        POLITENESS_POOL_PENDING,
        Unit::Count,
        "Currently-outstanding bb8 Redis pool connections used by politeness."
    );
}
