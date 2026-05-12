//! Redis-backed `Frontier` implementation.
//!
//! [`RedisFrontier`] satisfies [`crawlrs_core::Frontier`] using a
//! per-shard keyspace on a Redis instance with at-least-once delivery
//! via leases on an inflight ZSET. The data shape, per shard:
//!
//! - `host_queue:<host>` (LIST<url_id>): per-host FIFO of URLs awaiting
//!   fetch.
//! - `wake` (ZSET<host>): hosts not yet ready; score = next-allowed
//!   wall-clock ms.
//! - `ready` (LIST<host>): hosts whose wake-time has elapsed,
//!   populated by the promoter background task.
//! - `inflight` (ZSET<"url_id|host">): leases; score = lease expiry
//!   ms. Expired leases are reclaimed by the same background task.
//! - `urls` (HASH<url_id, payload>): content-addressed
//!   postcard-encoded `UrlEntry`.
//! - `seen` (RedisBloom): submit-time dedup keyed on url_id.
//! - `overflow` (LIST<url_id>): spillover for hosts past the
//!   backlog cap.
//!
//! All per-shard keys share the same Redis Cluster hash tag (see
//! [`KeyPrefix`]) so Lua scripts that touch multiple keys per shard
//! stay within one cluster slot.
//!
//! Pattern: Strategy at construction (`ShardingPolicy` selects the
//! shard for each URL); Mediator at runtime (one `RedisFrontier`
//! orchestrates submit / claim / advance_wake / ack / tick across
//! all owned shards).
//!
//! Requires Redis Stack (or stock Redis with the RedisBloom module
//! loaded). The constructor surfaces a clear error if the module is
//! missing.

pub mod bloom;
pub mod codec;
pub mod frontier;
pub mod host_queue;
pub mod keys;
pub mod metrics;
pub mod pool;
pub mod promoter;

pub use bloom::BloomConfig;
pub use frontier::{
    DEFAULT_LEASE_TIMEOUT, DEFAULT_MAX_HOST_BACKLOG, DEFAULT_TICK_BATCH_LIMIT, RedisFrontier,
    RedisFrontierError,
};
pub use keys::KeyPrefix;
pub use pool::{PoolSizeError, validate_pool_size};

/// Re-export the bb8/redis types callers need to construct the pool.
pub use bb8;
pub use bb8_redis;
pub use redis;
