//! Redis key naming, scoped by `run_id` and shard.
//!
//! Every key the frontier reads or writes goes through [`KeyPrefix`].
//! Shape: `crawlrs:{<run>_s<shard>}:<purpose>[:<sub>]`. The literal
//! braces are a Redis Cluster *hash tag*: Redis hashes only the text
//! between them when picking a slot, so every per-shard key
//! (`host_queue`, `wake`, `ready`, `inflight`, `overflow`, `urls`,
//! `seen`) lands on the same node. This lets the Lua scripts touch
//! multiple keys for one shard atomically even on a clustered
//! deployment.
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

    /// `crawlrs:{run_s<shard>}:overflow`. Spillover list for URLs
    /// whose host's `host_queue` was at the configured backlog cap.
    pub fn overflow(&self, shard: ShardKey) -> String {
        format!("{}:overflow", self.shard_tag(shard))
    }

    /// `crawlrs:{run_s<shard>}:urls`. Hash from `url_id_hex` to
    /// postcard-encoded `UrlEntry` payload. Materialised on claim,
    /// deleted on ack.
    pub fn urls(&self, shard: ShardKey) -> String {
        format!("{}:urls", self.shard_tag(shard))
    }

    /// `crawlrs:{run_s<shard>}:seen`. RedisBloom filter for
    /// submit-time dedup keyed on `url_id_hex`. Replaces the
    /// per-run-cleared SET from the prior shape.
    pub fn seen(&self, shard: ShardKey) -> String {
        format!("{}:seen", self.shard_tag(shard))
    }
}

#[cfg(test)]
mod tests {
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
    fn all_per_shard_keys_share_the_same_hash_tag() {
        // Cluster co-location depends on every per-shard key carrying
        // the same `{...}` block. Lock that.
        let prefix = KeyPrefix::new("r");
        let tag = "{r_s0}";
        for key in [
            prefix.host_queue(0, "a.test"),
            prefix.wake(0),
            prefix.ready(0),
            prefix.inflight(0),
            prefix.overflow(0),
            prefix.urls(0),
            prefix.seen(0),
        ] {
            assert!(
                key.contains(tag),
                "key {key:?} missing the shared {tag} hash tag",
            );
        }
    }
}
