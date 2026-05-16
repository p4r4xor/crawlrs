//! Redis key naming, scoped by `run_id` and shard.
//!
//! Every key the frontier reads or writes goes through [`KeyPrefix`].
//! Shape: `crawlrs:{<run>_s<shard>}:<purpose>[:<sub>]`. The literal
//! braces are a Redis Cluster *hash tag*: Redis hashes only the text
//! between them when picking a slot, so every per-shard key
//! (`host_queue`, `wake`, `ready`, `inflight`, `urls`, `seen`) lands
//! on the same node. This lets the Lua scripts touch multiple keys
//! for one shard atomically even on a clustered deployment.
//!
//! Two crawl runs can share one Redis instance without collision; an
//! operator can scope `redis-cli SCAN` to one run via the `run_id`
//! prefix, and to one shard via the `_s<shard>` suffix inside the
//! tag.

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
        Self {
            run_id: run_id.into(),
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Hash-tag portion shared by every per-shard key. Used by the
    /// Lua scripts as a prefix when computing per-host queue keys
    /// dynamically (the host name is not known at script-load time).
    pub fn shard_tag(&self, shard: ShardKey) -> String {
        format!("crawlrs:{{{}_s{}}}", self.run_id, shard)
    }

    /// `crawlrs:{run_s<shard>}:host_queue:<host>`. Per-host FIFO of URL
    /// IDs awaiting fetch.
    pub fn host_queue(&self, shard: ShardKey, host: &str) -> String {
        format!("{}:host_queue:{}", self.shard_tag(shard), host)
    }

    /// Prefix the Lua scripts use to derive a `host_queue` key from a
    /// dynamically-popped host. Always equals `shard_tag(shard) +
    /// ":host_queue:"`; `host` is appended at script time.
    pub fn host_queue_prefix(&self, shard: ShardKey) -> String {
        format!("{}:host_queue:", self.shard_tag(shard))
    }

    /// `crawlrs:{run_s<shard>}:wake`. Sorted set keyed by host, score
    /// = next-allowed-fetch wall-clock millis. Hosts whose score has
    /// elapsed get promoted into `ready` by the promoter loop.
    pub fn wake(&self, shard: ShardKey) -> String {
        format!("{}:wake", self.shard_tag(shard))
    }

    /// `crawlrs:{run_s<shard>}:ready`. List of hosts ready to claim
    /// from; populated by the promoter task, drained by `claim`.
    pub fn ready(&self, shard: ShardKey) -> String {
        format!("{}:ready", self.shard_tag(shard))
    }

    /// `crawlrs:{run_s<shard>}:inflight`. Sorted set of leased URLs;
    /// member format is `<url_id_hex>|<host>` (so reclaim can re-push
    /// without re-decoding the URL HASH payload), score is the
    /// lease-expiry millis.
    pub fn inflight(&self, shard: ShardKey) -> String {
        format!("{}:inflight", self.shard_tag(shard))
    }

    /// `crawlrs:{run_s<shard>}:urls`. Hash from `url_id_hex` to
    /// postcard-encoded `UrlEntry` payload. Materialised on claim,
    /// deleted on ack.
    pub fn urls(&self, shard: ShardKey) -> String {
        format!("{}:urls", self.shard_tag(shard))
    }

    /// `crawlrs:{run_s<shard>}:host_count:<host>`. Integer counter
    /// per host: number of URLs successfully accepted into the
    /// queue for this run. Compared at submit time against the
    /// effective `[crawl].max_urls` to reject URLs once the host
    /// is at quota. Per-run scope (no cross-run quota inheritance);
    /// shares the same Redis Cluster hash tag as the other
    /// per-shard keys so the atomic submit script touches one slot.
    pub fn host_count(&self, shard: ShardKey, host: &str) -> String {
        format!("{}:host_count:{}", self.shard_tag(shard), host)
    }

    /// `crawlrs:{s<shard>}:seen`. RedisBloom filter for submit-time
    /// dedup keyed on `url_id_hex`.
    ///
    /// Deliberately scoped per-shard but *not* per-run: a URL
    /// submitted under any prior `run_id` for this shard is
    /// recognised as duplicate without re-fetching. That's the
    /// whole point of bloom-based cross-run dedup.
    ///
    /// Tradeoff: this key uses a different Redis Cluster hash tag
    /// than the per-run keys, so the single-RTT `submit.lua` (which
    /// touches `seen` plus the per-run `urls` / `host_queue` /
    /// `wake`) is single-node-only. On a future
    /// clustered deployment, submit would split into two
    /// round-trips: `BF.ADD seen` first, then the per-run atomic
    /// script if and only if the URL was new.
    pub fn seen(&self, shard: ShardKey) -> String {
        format!("crawlrs:{{s{shard}}}:seen")
    }
}

#[cfg(test)]
mod tests {
    // Inline because: locality guards. These tests pin two
    // architectural invariants tied to the `KeyPrefix` impl: cross-run
    // dedup (the `seen` key must NOT scope by `run_id`, so a URL seen
    // in run A is recognised as duplicate in run B) and Redis-cluster
    // colocation (every per-run-per-shard key must share the
    // `{<run>_s<N>}` hash tag so multi-key Lua stays on one slot).
    // Anyone adding a new per-shard key needs that reminder living
    // next to the keys themselves; promoting to `tests/` would
    // separate the contract from the surface it constrains.

    use super::*;

    #[test]
    fn keys_carry_hash_tag_with_run_and_shard() {
        let prefix = KeyPrefix::new("demo");
        assert_eq!(prefix.shard_tag(0), "crawlrs:{demo_s0}");
        assert_eq!(prefix.wake(0), "crawlrs:{demo_s0}:wake");
        assert_eq!(prefix.ready(3), "crawlrs:{demo_s3}:ready");
        assert_eq!(
            prefix.host_queue(7, "example.com"),
            "crawlrs:{demo_s7}:host_queue:example.com"
        );
        assert_eq!(prefix.host_queue_prefix(7), "crawlrs:{demo_s7}:host_queue:");
    }

    #[test]
    fn seen_key_is_deployment_wide_not_run_scoped() {
        // Two different run_ids must land on the same seen key for
        // a given shard: that's the cross-run dedup contract.
        let a = KeyPrefix::new("run-a");
        let b = KeyPrefix::new("run-b");
        assert_eq!(a.seen(0), "crawlrs:{s0}:seen");
        assert_eq!(a.seen(0), b.seen(0));
        // But the per-run keys do still scope by run_id.
        assert_ne!(a.wake(0), b.wake(0));
    }

    #[test]
    fn all_per_run_keys_share_the_same_hash_tag() {
        // Cluster co-location depends on every per-run-per-shard key
        // carrying the same `{<run>_s<N>}` block. Lock that.
        // `seen` is deliberately excluded here: its different hash
        // tag is the tradeoff for cross-run dedup.
        let prefix = KeyPrefix::new("r");
        let tag = "{r_s0}";
        for key in [
            prefix.host_queue(0, "a.test"),
            prefix.wake(0),
            prefix.ready(0),
            prefix.inflight(0),
            prefix.urls(0),
            prefix.host_count(0, "a.test"),
        ] {
            assert!(
                key.contains(tag),
                "key {key:?} missing the shared {tag} hash tag",
            );
        }
    }

    #[test]
    fn host_count_is_run_scoped() {
        // Per-host quota counters reset between runs (the ADR's
        // "counter is per-run" tradeoff); two run_ids must produce
        // distinct keys for the same (shard, host).
        let a = KeyPrefix::new("run-a");
        let b = KeyPrefix::new("run-b");
        assert_ne!(a.host_count(0, "x.test"), b.host_count(0, "x.test"));
    }
}
