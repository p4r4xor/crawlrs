//! Sharding strategy for distributing URLs across per-shard keyspaces.
//!
//! Pattern: Strategy. The trait defines the routing decision; concrete
//! impls (single-shard for tests, host-hash for production) plug into
//! every adapter that owns shard-keyed state (frontier + politeness).
//! Production deployments default to `HostHashShardPolicy(8)`: 8 shards
//! bounds hot-domain head-of-line blocking to one shard's worker
//! capacity while staying small enough that one Redis instance can
//! hold all per-shard state.

use crate::hash::fnv1a_64;
use crate::url::CanonicalUrl;

/// Identifier of a shard within a sharded `Frontier`. `u32` is plenty:
/// the practical upper bound on shards is the worker pod count times a
/// small fan-out factor, nowhere near 4 billion.
pub type ShardKey = u32;

/// Routes URLs to shards. Pattern: Strategy.
///
/// `SingleShardPolicy` = Pattern 1 (one shard, every worker consumes it);
/// `HostHashShardPolicy` = Pattern 2 (host-sharded). Same `Frontier` impl
/// in either case; the impl reads the policy at construction.
pub trait ShardingPolicy: Send + Sync {
    /// Map a URL to the shard that owns its queue, seen-set, and
    /// politeness state.
    #[must_use]
    fn shard_key(&self, url: &CanonicalUrl) -> ShardKey;

    /// Map a host directly to its owning shard. Useful when the
    /// caller has the host but no full URL (e.g. resolving a
    /// `Politeness::record_*`-produced `NextWake` plan into the
    /// frontier shard whose wake ZSET owns this host).
    fn shard_key_from_host(&self, host: &str) -> ShardKey;

    /// Total number of shards this policy generates. Used by the
    /// frontier impl to size keyspaces and by deployment tools to
    /// validate ownership coverage.
    #[must_use]
    fn shard_count(&self) -> u32;
}

/// One shard for everyone. Every URL maps to shard `0`. Reserved for
/// tests and one-off ops scripts; production deployments default to
/// [`HostHashShardPolicy`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SingleShardPolicy;

impl ShardingPolicy for SingleShardPolicy {
    fn shard_key(&self, _url: &CanonicalUrl) -> ShardKey {
        0
    }
    fn shard_key_from_host(&self, _host: &str) -> ShardKey {
        0
    }
    fn shard_count(&self) -> u32 {
        1
    }
}

/// Host-hashed sharding. Same registrable host always lands on the
/// same shard, so per-host politeness state stays local to one shard.
///
/// Hash function is FNV-1a over the host string. FNV-1a is deterministic
/// (no per-process seed), stable across releases, dependency-free, and
/// adequate for distributing the host space; we are not relying on
/// cryptographic properties.
#[derive(Debug, Clone, Copy)]
pub struct HostHashShardPolicy {
    // Private so the `new()` invariant (>= 1) cannot be bypassed by a
    // struct literal; `shard_key_from_host` divides by this value.
    num_shards: u32,
}

impl HostHashShardPolicy {
    pub fn new(num_shards: u32) -> Self {
        assert!(num_shards >= 1, "num_shards must be at least 1");
        Self { num_shards }
    }
}

/// Shards owned by replica `ordinal` out of `replicas` total, across
/// `num_shards` shards. Each replica owns a strided subset: `ordinal`,
/// `ordinal + replicas`, `ordinal + 2*replicas`, ... below `num_shards`.
/// Distinct replicas own disjoint shard sets, so no shard is ever
/// double-owned. Returns an empty `Vec` when the replica owns nothing
/// (`ordinal >= num_shards`, or `replicas == 0`).
#[must_use]
pub fn owned_shards_for_replica(ordinal: u32, replicas: u32, num_shards: u32) -> Vec<ShardKey> {
    if replicas == 0 {
        return Vec::new();
    }
    (ordinal..num_shards).step_by(replicas as usize).collect()
}

impl ShardingPolicy for HostHashShardPolicy {
    fn shard_key(&self, url: &CanonicalUrl) -> ShardKey {
        self.shard_key_from_host(url.host().unwrap_or(""))
    }
    fn shard_key_from_host(&self, host: &str) -> ShardKey {
        let hash = fnv1a_64(host.as_bytes());
        (hash % self.num_shards as u64) as ShardKey
    }
    fn shard_count(&self) -> u32 {
        self.num_shards
    }
}
