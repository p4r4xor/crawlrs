//! Redis key naming, scoped by `run_id` and shard.
//!
//! Every key the frontier reads or writes goes through [`KeyPrefix`].
//! The shape is `crawlrs:{run_id}:s{shard}:{purpose}`, so two crawl
//! runs can share a single Redis instance without collision and an
//! operator can scope `redis-cli SCAN` queries to one run.
//!
//! See `crates/crawlrs-frontier-redis/src/lib.rs` for the full key
//! catalogue. The politeness keyspace (host schedule, host state) lives
//! under the same prefix but is owned by `crawlrs-politeness`, not by
//! this crate.

use crawlrs_core::ShardKey;

/// Builds Redis key strings for one crawl run.
///
/// Cheap to clone (one `String`); call sites that need many keys per
/// request hold a reference rather than allocating per call.
#[derive(Debug, Clone)]
pub struct KeyPrefix {
    run_id: String,
}

impl KeyPrefix {
    pub fn new(run_id: impl Into<String>) -> Self {
        Self { run_id: run_id.into() }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// `crawlrs:{run_id}:s{shard}:queue`. Stream that holds enqueued
    /// `UrlEntry` payloads. Workers consume via `XREADGROUP` and
    /// confirm via `XACK`.
    pub fn queue(&self, shard: ShardKey) -> String {
        format!("crawlrs:{}:s{}:queue", self.run_id, shard)
    }

    /// `crawlrs:{run_id}:s{shard}:seen`. Set of URL hashes already
    /// submitted to the queue, used for submit-time dedup.
    pub fn seen(&self, shard: ShardKey) -> String {
        format!("crawlrs:{}:s{}:seen", self.run_id, shard)
    }

    /// Consumer-group name for the queue stream. Single fixed name
    /// across all consumers; each worker registers as a unique
    /// consumer within the group.
    pub fn consumer_group(&self) -> &'static str {
        "fetchers"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_run_scoped_and_shard_scoped() {
        let keys = KeyPrefix::new("run-x");
        assert_eq!(keys.queue(0), "crawlrs:run-x:s0:queue");
        assert_eq!(keys.queue(7), "crawlrs:run-x:s7:queue");
        assert_eq!(keys.seen(0), "crawlrs:run-x:s0:seen");
    }

    #[test]
    fn different_runs_dont_collide() {
        let a = KeyPrefix::new("run-a");
        let b = KeyPrefix::new("run-b");
        assert_ne!(a.queue(0), b.queue(0));
    }

    #[test]
    fn consumer_group_is_stable() {
        let a = KeyPrefix::new("run-a");
        let b = KeyPrefix::new("run-b");
        assert_eq!(a.consumer_group(), b.consumer_group());
        assert_eq!(a.consumer_group(), "fetchers");
    }
}
