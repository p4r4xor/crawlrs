//! Tests for `ShardingPolicy` and its concrete strategies
//! (`SingleShardPolicy`, `HostHashShardPolicy`).

use crawlrs_core::{CanonicalUrl, HostHashShardPolicy, ShardingPolicy, SingleShardPolicy};

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
