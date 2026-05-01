//! Redis key naming, scoped by `run_id` and shard.
//!
//! Mirrors `crawlrs-frontier-redis::keys` by convention (same
//! `crawlrs:{run_id}:s{shard}:` prefix) so an operator can introspect
//! a run with one `redis-cli SCAN` pattern, but is duplicated rather
//! than shared to avoid coupling politeness to the frontier impl.

use crawlrs_core::ShardKey;

/// Builds Redis key strings for one crawl run's politeness state.
#[derive(Debug, Clone)]
pub struct KeyPrefix {
    run_id: String,
}

impl KeyPrefix {
    pub fn new(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// `crawlrs:{run_id}:s{shard}:hostsched`. Sorted set keyed on host;
    /// score is the wall-clock millis after which fetches to that host
    /// are allowed. `record_fetch` writes `now + delay`; `next_ready_at`
    /// reads the smallest score.
    pub fn hostsched(&self, shard: ShardKey) -> String {
        format!("crawlrs:{}:s{}:hostsched", self.run_id, shard)
    }

    /// `crawlrs:{run_id}:s{shard}:hoststate:{host}`. Hash with failure
    /// state for one host: `consecutive_failures`, `backoff_until_ms`,
    /// `last_kind`. Drives circuit-breaker behaviour.
    pub fn hoststate(&self, shard: ShardKey, host: &str) -> String {
        format!("crawlrs:{}:s{}:hoststate:{}", self.run_id, shard, host)
    }

    /// `crawlrs:{run_id}:s{shard}:robots:{host}`. Hash caching the raw
    /// robots.txt body and its expiry; TTL is also applied via `EXPIRE`
    /// so stale entries clean themselves up if a host stops being
    /// crawled.
    pub fn robots(&self, shard: ShardKey, host: &str) -> String {
        format!("crawlrs:{}:s{}:robots:{}", self.run_id, shard, host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_run_and_shard_scoped() {
        let k = KeyPrefix::new("run-x");
        assert_eq!(k.hostsched(0), "crawlrs:run-x:s0:hostsched");
        assert_eq!(
            k.hoststate(2, "example.com"),
            "crawlrs:run-x:s2:hoststate:example.com"
        );
        assert_eq!(
            k.robots(0, "example.com"),
            "crawlrs:run-x:s0:robots:example.com"
        );
    }
}
