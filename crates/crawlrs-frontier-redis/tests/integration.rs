// Harness-gated: the streams-based test shape cannot be expressed
// against the per-host queue + atomic-Lua + lease ZSET design from
// ADR-0019. Re-enable once the Redis-backed `Frontier` impl lands and
// rewrite the suite around the new surface.
#![cfg(any())]

//! Integration tests for `RedisFrontier` against a real Redis instance.
//!
//! Each test spins up its own Redis container via `testcontainers-rs` and
//! uses a unique `run_id`, so tests are isolated from each other and
//! from any local Redis the developer might be running.
//!
//! Requires Docker on the host. Tests are not gated by a feature flag;
//! if Docker is unreachable, the container startup fails fast with a
//! clear error.

use std::sync::Arc;
use std::time::Duration;

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    CanonicalUrl, Frontier, HostHashShardPolicy, ShardingPolicy, SingleShardPolicy, UrlEntry,
    WorkerIdentity,
};

const TEST_IDENTITY_A: WorkerIdentity = WorkerIdentity::new(0, 0);
const TEST_IDENTITY_B: WorkerIdentity = WorkerIdentity::new(0, 1);
use crawlrs_frontier_redis::RedisFrontier;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

/// Owns the testcontainers Redis container and a bb8 pool wired to it.
/// Tests keep this around for the test's lifetime; dropping it stops
/// the container.
struct RedisFixture {
    _container: ContainerAsync<Redis>,
    pool: Pool<RedisConnectionManager>,
}

async fn fixture() -> RedisFixture {
    // Pin a modern Redis tag so XAUTOCLAIM (introduced in 6.2) is
    // available. testcontainers-modules' default image tag may lag
    // behind what we need.
    let container = Redis::default()
        .with_tag("7.2")
        .start()
        .await
        .expect("docker must be running for integration tests");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let url = format!("redis://{host}:{port}");
    let manager = RedisConnectionManager::new(url).unwrap();
    let pool = Pool::builder().max_size(8).build(manager).await.unwrap();
    RedisFixture {
        _container: container,
        pool,
    }
}

fn run_id() -> String {
    format!("test-{}", cuid2::create_id())
}

fn url(s: &str) -> CanonicalUrl {
    CanonicalUrl::parse(s).unwrap()
}

fn entry(s: &str) -> UrlEntry {
    UrlEntry::seed(url(s))
}

async fn single_shard_frontier(pool: &Pool<RedisConnectionManager>) -> RedisFrontier {
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    RedisFrontier::new(pool.clone(), policy, vec![0], run_id())
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_then_claim_yields_same_url() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;

    let was_new = frontier.submit(entry("https://a.test/")).await.unwrap();
    assert!(was_new, "first submit should be newly enqueued");

    let claimed = frontier
        .claim(&TEST_IDENTITY_A)
        .await
        .unwrap()
        .expect("queue should yield the entry");
    assert_eq!(claimed.entry.url.as_str(), "https://a.test/");
}

#[tokio::test]
async fn duplicate_submit_is_dropped_at_seen_set() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;

    let first = frontier.submit(entry("https://a.test/")).await.unwrap();
    let second = frontier.submit(entry("https://a.test/")).await.unwrap();

    assert!(first, "first submit returns true (newly enqueued)");
    assert!(!second, "second submit returns false (already seen)");

    let depth = frontier.len().await.unwrap();
    assert_eq!(depth, 1, "queue holds exactly one entry");
}

#[tokio::test]
async fn submit_batch_counts_only_new_entries() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;

    // Pre-seed one URL.
    frontier.submit(entry("https://a.test/")).await.unwrap();

    let mixed = vec![
        entry("https://a.test/"), // dupe
        entry("https://b.test/"), // new
        entry("https://c.test/"), // new
        entry("https://b.test/"), // dupe within the same batch
    ];
    let newly = frontier.submit_batch(mixed).await.unwrap();
    assert_eq!(newly, 2, "only b.test and c.test are newly enqueued");

    let depth = frontier.len().await.unwrap();
    assert_eq!(depth, 3, "queue holds a, b, c");
}

#[tokio::test]
async fn ack_after_claim_drops_pending_count() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;

    frontier.submit(entry("https://a.test/")).await.unwrap();
    let claimed = frontier.claim(&TEST_IDENTITY_A).await.unwrap().unwrap();
    assert_eq!(frontier.claim_count(), 1, "in-flight after claim");

    frontier.ack(&claimed.attempt_id).await.unwrap();
    assert_eq!(frontier.claim_count(), 0, "drained after ack");

    // Idempotent: ack again is a no-op, not an error.
    frontier.ack(&claimed.attempt_id).await.unwrap();
}

#[tokio::test]
async fn nack_clears_local_tracking_only() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;

    frontier.submit(entry("https://a.test/")).await.unwrap();
    let claimed = frontier.claim(&TEST_IDENTITY_A).await.unwrap().unwrap();
    assert_eq!(frontier.claim_count(), 1);

    frontier.nack(&claimed.attempt_id).await.unwrap();
    assert_eq!(frontier.claim_count(), 0, "local tracking dropped");
}

#[tokio::test]
async fn nacked_entry_resurfaces_via_tier_1_and_re_counts() {
    // After nack, the entry stays in this consumer's Redis-side PEL.
    // The next claim() surfaces it via tier-1 and the local in-flight
    // tracking must reflect that the worker is once again actively
    // processing the entry. A subsequent ack drains the count cleanly.
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;

    frontier.submit(entry("https://a.test/")).await.unwrap();
    let first = frontier.claim(&TEST_IDENTITY_A).await.unwrap().unwrap();
    assert_eq!(frontier.claim_count(), 1);
    frontier.nack(&first.attempt_id).await.unwrap();
    assert_eq!(frontier.claim_count(), 0, "nack drops local tracking");

    let resurfaced = frontier.claim(&TEST_IDENTITY_A).await.unwrap().unwrap();
    assert_eq!(
        resurfaced.attempt_id, first.attempt_id,
        "tier-1 PEL replay must hand back the same AttemptId",
    );
    assert_eq!(
        frontier.claim_count(),
        1,
        "post-nack reclaim re-counts the entry as in-flight",
    );

    frontier.ack(&resurfaced.attempt_id).await.unwrap();
    assert_eq!(frontier.claim_count(), 0, "ack drains the count");
}

#[tokio::test]
async fn host_hash_policy_routes_same_host_to_same_shard() {
    // Build a fresh frontier per shard, both pointing at the same Redis,
    // with a HostHashShardPolicy of width 4.
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(HostHashShardPolicy::new(4));
    let rid = run_id();

    // Determine which shard reddit.test would land on.
    let target_shard = policy.shard_key(&url("https://reddit.test/"));

    // Owner of that shard.
    let owner = RedisFrontier::new(
        fx.pool.clone(),
        policy.clone(),
        vec![target_shard],
        rid.clone(),
    )
    .await
    .unwrap();

    // Submit two URLs from the same host: both route to `target_shard`,
    // both accepted by the owner.
    assert!(
        owner
            .submit(entry("https://reddit.test/foo"))
            .await
            .unwrap()
    );
    assert!(
        owner
            .submit(entry("https://reddit.test/bar"))
            .await
            .unwrap()
    );

    let depth = owner.len().await.unwrap();
    assert_eq!(depth, 2);
}

#[tokio::test]
async fn frontier_rejects_unowned_shard_submit() {
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(HostHashShardPolicy::new(4));
    let rid = run_id();

    // Pick a shard, then own only the *other* shards.
    let bad_shard = policy.shard_key(&url("https://reddit.test/"));
    let owned: Vec<_> = (0..4u32).filter(|s| *s != bad_shard).collect();

    let frontier = RedisFrontier::new(fx.pool.clone(), policy.clone(), owned, rid)
        .await
        .unwrap();

    // Submitting a URL whose shard we don't own should error.
    let err = frontier.submit(entry("https://reddit.test/page")).await;
    assert!(err.is_err(), "submitting to unowned shard should error");
}

#[tokio::test]
async fn xautoclaim_reclaims_stranded_entries_to_a_second_consumer() {
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let rid = run_id();

    // Worker A submits + claims, never acks.
    let worker_a = RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0], rid.clone())
        .await
        .unwrap()
        .with_autoclaim_idle(Duration::ZERO);
    worker_a.submit(entry("https://a.test/")).await.unwrap();
    let _claim_a = worker_a.claim(&TEST_IDENTITY_A).await.unwrap().unwrap();
    assert_eq!(worker_a.claim_count(), 1);

    // Worker B starts up (different identity) and ticks autoclaim with
    // idle_ms=0, which immediately reclaims A's pending entry.
    let worker_b = RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0], rid)
        .await
        .unwrap()
        .with_autoclaim_idle(Duration::ZERO);
    let reclaimed = worker_b.reclaim_stranded(&TEST_IDENTITY_B).await.unwrap();
    assert_eq!(reclaimed, 1, "worker B should reclaim 1 stranded entry");
    assert_eq!(
        worker_b.claim_count(),
        1,
        "B's in-flight count now reflects the reclaimed entry"
    );

    // B's next claim returns the stranded URL (read from B's PEL).
    let claimed_by_b = worker_b.claim(&TEST_IDENTITY_B).await.unwrap().unwrap();
    assert_eq!(claimed_by_b.entry.url.as_str(), "https://a.test/");

    // B acks; queue is empty.
    worker_b.ack(&claimed_by_b.attempt_id).await.unwrap();
    assert_eq!(worker_b.claim_count(), 0);
}

#[tokio::test]
async fn run_id_isolates_two_concurrent_crawls() {
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);

    let crawl_one = RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0], "crawl-one")
        .await
        .unwrap();
    let crawl_two = RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0], "crawl-two")
        .await
        .unwrap();

    crawl_one.submit(entry("https://a.test/")).await.unwrap();
    crawl_two.submit(entry("https://b.test/")).await.unwrap();

    assert_eq!(crawl_one.len().await.unwrap(), 1);
    assert_eq!(crawl_two.len().await.unwrap(), 1);

    let from_one = crawl_one.claim(&TEST_IDENTITY_A).await.unwrap().unwrap();
    let from_two = crawl_two.claim(&TEST_IDENTITY_A).await.unwrap().unwrap();
    assert_eq!(from_one.entry.url.as_str(), "https://a.test/");
    assert_eq!(from_two.entry.url.as_str(), "https://b.test/");
}

#[tokio::test]
async fn shard_depths_reports_per_shard_xlen() {
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(HostHashShardPolicy::new(4));
    let rid = run_id();

    let frontier = RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0, 1, 2, 3], rid)
        .await
        .unwrap();

    // Submit a handful of distinct hosts; they spread across shards.
    for host in [
        "alpha.test",
        "bravo.test",
        "charlie.test",
        "delta.test",
        "echo.test",
    ] {
        frontier
            .submit(entry(&format!("https://{host}/")))
            .await
            .unwrap();
    }

    let depths = frontier.shard_depths().await.unwrap();
    assert_eq!(depths.len(), 4, "one entry per owned shard");
    assert_eq!(depths.values().sum::<usize>(), 5, "five total submissions");
}

#[tokio::test]
async fn submit_batch_fans_out_one_eval_per_shard() {
    // submit_batch groups by shard and runs one EVAL per shard chunk.
    // Build a batch whose URLs deliberately span all 4 shards, submit
    // it in one call, verify each shard ended up with the right URLs.
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(HostHashShardPolicy::new(4));
    let rid = run_id();

    let frontier = RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0, 1, 2, 3], rid)
        .await
        .unwrap();

    // 12 distinct hosts: FNV-1a should distribute them across all 4
    // shards. (The test asserts the total, not the per-shard split,
    // because the exact distribution is hash-dependent.)
    let hosts = [
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
        "kilo.test",
        "lima.test",
    ];
    let entries: Vec<UrlEntry> = hosts
        .iter()
        .map(|h| entry(&format!("https://{h}/")))
        .collect();

    let newly = frontier.submit_batch(entries).await.unwrap();
    assert_eq!(newly, hosts.len(), "all 12 hosts are new");

    let depths = frontier.shard_depths().await.unwrap();
    assert_eq!(
        depths.values().sum::<usize>(),
        hosts.len(),
        "every entry landed on exactly one shard",
    );
    assert!(
        depths.values().filter(|&&d| d > 0).count() >= 2,
        "FNV-1a should put 12 hosts across at least 2 of 4 shards",
    );
}

#[tokio::test]
async fn max_queue_depth_trims_oldest_entries() {
    // With max_queue_depth set, XADD MAXLEN ~ kicks in; the queue
    // should never grow much beyond the cap. (Approximate trim means
    // depth can transiently exceed the cap; we just assert it stays
    // within a reasonable factor.)
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let frontier = RedisFrontier::new(fx.pool.clone(), policy, vec![0], run_id())
        .await
        .unwrap()
        .with_max_queue_depth(50);

    // Submit 200 distinct URLs in one batch. 4x the cap: easy to detect
    // if the trim isn't happening.
    let entries: Vec<UrlEntry> = (0..200)
        .map(|i| entry(&format!("https://example.test/{i}")))
        .collect();
    let newly = frontier.submit_batch(entries).await.unwrap();
    assert_eq!(newly, 200, "all 200 URLs are new in seen-set");

    // Depth should be capped near 50. Approximate trim allows some
    // overshoot; assert the cap is broadly respected, not exact.
    let depth = frontier.len().await.unwrap();
    assert!(
        depth <= 150,
        "approximate MAXLEN should keep depth below ~3x cap; got {depth}",
    );
}

#[tokio::test]
async fn submit_batch_with_duplicates_only_counts_new() {
    // Within one batch, duplicate URLs must be deduped exactly the same
    // as if submitted singly. Send the same host twice in one batch and
    // verify newly-count is 1 (the SADD inside the Lua loop dedups).
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(HostHashShardPolicy::new(2));
    let rid = run_id();
    let frontier = RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0, 1], rid)
        .await
        .unwrap();

    let entries = vec![
        entry("https://twin.test/foo"),
        entry("https://twin.test/foo"), // duplicate
        entry("https://other.test/bar"),
    ];
    let newly = frontier.submit_batch(entries).await.unwrap();
    assert_eq!(newly, 2, "duplicate within the batch is dropped");
    assert_eq!(frontier.len().await.unwrap(), 2);
}
