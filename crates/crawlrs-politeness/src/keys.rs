//! Redis key naming, scoped by `run_id` and shard.
//!
//! Mirrors `crawlrs-frontier::keys` by convention (same
//! `crawlrs:{run_id}:s{shard}:` prefix) so an operator can introspect
//! a run with one `redis-cli SCAN` pattern, but is duplicated rather
//! than shared to avoid coupling politeness to the frontier impl.

use crawlrs_core::ShardKey;

/// Builds Redis key strings for one crawl run's politeness state.
#[derive(Debug, Clone)]
pub(crate) struct KeyPrefix {
    run_id: String,
}

impl KeyPrefix {
    pub(crate) fn new(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
        }
    }

    /// `crawlrs:{run_id}:s{shard}:hoststate:{host}`. Hash with failure
    /// state for one host: `consecutive_failures`, `backoff_until_ms`,
    /// `last_kind`. Drives circuit-breaker behaviour.
    pub(crate) fn hoststate(&self, shard: ShardKey, host: &str) -> String {
        format!("crawlrs:{}:s{}:hoststate:{}", self.run_id, shard, host)
    }

    /// `crawlrs:{run_id}:s{shard}:robots:{host}`. Hash caching the raw
    /// robots.txt body and its expiry; TTL is also applied via `EXPIRE`
    /// so stale entries clean themselves up if a host stops being
    /// crawled.
    pub(crate) fn robots(&self, shard: ShardKey, host: &str) -> String {
        format!("crawlrs:{}:s{}:robots:{}", self.run_id, shard, host)
    }
}

#[cfg(test)]
mod tests {
    // Inline because: visibility-forced. `KeyPrefix` is `pub(crate)`
    // (the composite builds it internally; no external caller commits
    // to it), so a `tests/*.rs` file compiled as a separate crate
    // cannot see it. The key-format contract is asserted here.

    use super::*;

    #[test]
    fn keys_are_run_and_shard_scoped() {
        let prefix = KeyPrefix::new("run-x");
        assert_eq!(
            prefix.hoststate(2, "example.com"),
            "crawlrs:run-x:s2:hoststate:example.com"
        );
        assert_eq!(
            prefix.robots(0, "example.com"),
            "crawlrs:run-x:s0:robots:example.com"
        );
    }
}
