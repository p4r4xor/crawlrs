//! Tests for `ShardingPolicy` and its concrete strategies
//! (`SingleShardPolicy`, `HostHashShardPolicy`).

use crawlrs_core::{
    CanonicalUrl, HostHashShardPolicy, ShardingPolicy, SingleShardPolicy, owned_shards_for_replica,
};

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

#[test]
fn owned_shards_single_replica_owns_everything() {
    assert_eq!(
        owned_shards_for_replica(0, 1, 8),
        vec![0, 1, 2, 3, 4, 5, 6, 7]
    );
}

#[test]
fn owned_shards_replicas_partition_disjointly() {
    // Three replicas over 8 shards: each strided subset, together a
    // disjoint cover of 0..8.
    let r0 = owned_shards_for_replica(0, 3, 8);
    let r1 = owned_shards_for_replica(1, 3, 8);
    let r2 = owned_shards_for_replica(2, 3, 8);
    assert_eq!(r0, vec![0, 3, 6]);
    assert_eq!(r1, vec![1, 4, 7]);
    assert_eq!(r2, vec![2, 5]);
    let mut all: Vec<u32> = r0.into_iter().chain(r1).chain(r2).collect();
    all.sort_unstable();
    assert_eq!(
        all,
        (0..8).collect::<Vec<_>>(),
        "replicas must cover shards disjointly"
    );
}

#[test]
fn owned_shards_replicas_equal_shards_owns_just_ordinal() {
    // The default when CRAWLRS_REPLICAS is unset: replicas == num_shards
    // means each pod owns exactly its own ordinal.
    assert_eq!(owned_shards_for_replica(3, 8, 8), vec![3]);
    assert_eq!(owned_shards_for_replica(0, 8, 8), vec![0]);
}

#[test]
fn owned_shards_ordinal_past_shard_count_owns_nothing() {
    assert!(owned_shards_for_replica(9, 8, 8).is_empty());
}

#[test]
fn owned_shards_zero_replicas_owns_nothing() {
    assert!(owned_shards_for_replica(0, 0, 8).is_empty());
}
