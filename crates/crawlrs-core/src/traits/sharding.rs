//! Sharding strategy for distributing URLs across per-shard keyspaces.
//!
//! See [ADR-0006](../../../docs/decisions/0006-sharding-policy-abstraction.md)
//! for the design and [ADR-0010](../../../docs/decisions/0010-default-sharding-policy.md)
//! for the default-policy choice (`HostHashShardPolicy(8)`).
//!
//! Pattern: Strategy. The trait defines the routing decision; concrete
//! impls (single-shard for tests, host-hash for production) plug into
//! every adapter that owns shard-keyed state (frontier + politeness).

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
    fn shard_key(&self, url: &CanonicalUrl) -> ShardKey;

    /// Total number of shards this policy generates. Used by the
    /// frontier impl to size keyspaces and by deployment tools to
    /// validate ownership coverage.
    fn shard_count(&self) -> u32;
}

/// One shard for everyone, Pattern 1 from ADR-0002. Every URL maps to
/// shard `0`. Reserved for tests and one-off ops scripts; production
/// deployments default to [`HostHashShardPolicy`] per ADR-0010.
#[derive(Debug, Clone, Copy, Default)]
pub struct SingleShardPolicy;

impl ShardingPolicy for SingleShardPolicy {
    fn shard_key(&self, _url: &CanonicalUrl) -> ShardKey {
        0
    }
    fn shard_count(&self) -> u32 {
        1
    }
}

/// Host-hashed sharding, Pattern 2 from ADR-0002. Same registrable host
/// always lands on the same shard, so per-host politeness state stays
/// local to one shard.
///
/// Hash function is FNV-1a over the host string. FNV-1a is deterministic
/// (no per-process seed), stable across releases, dependency-free, and
/// adequate for distributing the host space; we are not relying on
/// cryptographic properties.
#[derive(Debug, Clone, Copy)]
pub struct HostHashShardPolicy {
    pub num_shards: u32,
}

impl HostHashShardPolicy {
    pub fn new(num_shards: u32) -> Self {
        assert!(num_shards >= 1, "num_shards must be at least 1");
        Self { num_shards }
    }
}

impl ShardingPolicy for HostHashShardPolicy {
    fn shard_key(&self, url: &CanonicalUrl) -> ShardKey {
        let host = url.host().unwrap_or("");
        let hash = fnv1a_64(host.as_bytes());
        (hash % self.num_shards as u64) as ShardKey
    }
    fn shard_count(&self) -> u32 {
        self.num_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_shard_policy_routes_everything_to_zero() {
        let policy = SingleShardPolicy;
        let a = CanonicalUrl::parse("https://a.test/x").unwrap();
        let b = CanonicalUrl::parse("https://b.test/y").unwrap();
        assert_eq!(policy.shard_key(&a), 0);
        assert_eq!(policy.shard_key(&b), 0);
        assert_eq!(policy.shard_count(), 1);
    }

    #[test]
    fn host_hash_policy_is_deterministic_per_host() {
        let policy = HostHashShardPolicy::new(16);
        let a1 = CanonicalUrl::parse("https://example.com/foo").unwrap();
        let a2 = CanonicalUrl::parse("https://example.com/bar").unwrap();
        // Same host - same shard, regardless of path.
        assert_eq!(policy.shard_key(&a1), policy.shard_key(&a2));
    }

    #[test]
    fn host_hash_policy_distributes_across_shards() {
        let policy = HostHashShardPolicy::new(8);
        let mut shards = std::collections::HashSet::new();
        for host in [
            "alpha.test",
            "bravo.test",
            "charlie.test",
            "delta.test",
            "echo.test",
            "foxtrot.test",
            "golf.test",
            "hotel.test",
            "india.test",
            "juliet.test",
        ] {
            let url = CanonicalUrl::parse(&format!("https://{host}/")).unwrap();
            shards.insert(policy.shard_key(&url));
        }
        // Not asserting all 8 shards hit; that's a property of FNV's
        // distribution on a small sample. But we should hit at least
        // half, otherwise the hash is broken.
        assert!(
            shards.len() >= 4,
            "FNV-1a should spread 10 hosts across at least 4 of 8 shards; got {}",
            shards.len()
        );
    }

    #[test]
    fn host_hash_policy_is_stable_across_calls() {
        // FNV-1a must be deterministic; re-running the same input must
        // give the same output. Locks against accidentally introducing
        // a per-process random seed.
        let policy = HostHashShardPolicy::new(1024);
        let url = CanonicalUrl::parse("https://canary.test/").unwrap();
        let first = policy.shard_key(&url);
        for _ in 0..100 {
            assert_eq!(policy.shard_key(&url), first);
        }
    }

    #[test]
    #[should_panic(expected = "num_shards must be at least 1")]
    fn host_hash_policy_rejects_zero_shards() {
        let _ = HostHashShardPolicy::new(0);
    }
}
