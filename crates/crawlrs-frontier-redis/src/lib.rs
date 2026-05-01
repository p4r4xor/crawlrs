//! Redis-backed `Frontier` implementation.
//!
//! [`RedisFrontier`] satisfies [`crawlrs_core::Frontier`] using a per-shard
//! keyspace on a Redis instance, with at-least-once delivery via Redis
//! Streams and consumer groups (see ADR-0001 / ADR-0006 / ADR-0007).
//!
//! Design pattern: Strategy at construction (`ShardingPolicy` selects the
//! shard for each URL); Composite at runtime (one `RedisFrontier` instance
//! fronts N per-shard sub-keyspaces).
//!
//! Key naming, run-id, and ACK semantics are documented on the inherent
//! methods. See ARCHITECTURE.md §5 for the trait surface and §6 for the
//! Redis schema sketch.

pub mod claims;
pub mod codec;
pub mod frontier;
pub mod keys;
pub mod pool;

pub use claims::PendingClaims;
pub use frontier::{RedisFrontier, RedisFrontierError};
pub use keys::KeyPrefix;
pub use pool::{PoolSizeError, validate_pool_size};

/// Re-export the bb8/redis types callers need to construct the pool.
pub use bb8;
pub use bb8_redis;
pub use redis;
