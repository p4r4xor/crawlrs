//! Integration tests for `RedisFrontier` against Redis Stack.
//!
//! Each test spins up its own `redis/redis-stack-server` container
//! via `testcontainers-rs`; per-test isolation comes from a fresh
//! `run_id` so the per-run keys don't collide. The seen-set
//! (deployment-wide bloom) is naturally fresh because each container
//! has its own Redis instance.
//!
//! Requires Docker on the host. If Docker isn't available, container
//! startup fails fast with a clear error.

use std::sync::Arc;
use std::time::Duration;

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    CanonicalUrl, ClaimOutcome, Frontier, HostHashShardPolicy, ShardingPolicy, SingleShardPolicy,
    SubmitOutcome, UrlEntry, UrlId, WorkerIdentity,
};
use crawlrs_frontier::{BloomConfig, RedisFrontier};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

const IDENTITY: WorkerIdentity = WorkerIdentity::new(0, 0);

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// Owns the testcontainers Redis Stack container and a bb8 pool wired
/// to it. Dropping the fixture stops the container.
struct RedisFixture {
    _container: ContainerAsync<GenericImage>,
    pool: Pool<RedisConnectionManager>,
}

async fn fixture() -> RedisFixture {
    // Redis Stack ships RedisBloom (and JSON / Search / TimeSeries),
    // which is what the frontier's submit-time bloom requires. A
    // plain `redis:7-alpine` image would reject `BF.RESERVE` with
    // "unknown command".
    let container = GenericImage::new("redis/redis-stack-server", "7.4.0-v0")
        .with_exposed_port(6379.into())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
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

/// Build a `RedisFrontier` bound to `SingleShardPolicy`. Most tests
/// don't care about sharding; the few that do build their own.
async fn single_shard_frontier(pool: &Pool<RedisConnectionManager>) -> RedisFrontier {
    single_shard_frontier_with_run_id(pool, &run_id()).await
}

async fn single_shard_frontier_with_run_id(
    pool: &Pool<RedisConnectionManager>,
    run_id: &str,
) -> RedisFrontier {
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    RedisFrontier::new(
        pool.clone(),
        policy,
        vec![0],
        run_id,
        BloomConfig::default(),
    )
    .await
    .unwrap()
}

/// Helper: unwrap a `Claimed` outcome or panic with the actual variant.
fn unwrap_claimed(outcome: ClaimOutcome) -> (UrlId, UrlEntry, crawlrs_core::AttemptId) {
    match outcome {
        ClaimOutcome::Claimed {
            url_id,
            entry,
            attempt_id,
        } => (url_id, *entry, attempt_id),
        other => panic!("expected Claimed, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Submit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_first_url_returns_queued() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;
    let outcome = frontier.submit(entry("https://a.test/")).await.unwrap();
    assert!(matches!(outcome, SubmitOutcome::Queued));
}

#[tokio::test]
async fn submit_dedups_via_bloom_within_run() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;
    assert!(matches!(
        frontier.submit(entry("https://a.test/")).await.unwrap(),
        SubmitOutcome::Queued
    ));
    assert!(matches!(
        frontier.submit(entry("https://a.test/")).await.unwrap(),
        SubmitOutcome::SkippedDuplicate
    ));
}

/// Sum of all EVAL + EVALSHA calls Redis has processed so far. Used to
/// observe how many Lua-script dispatches a single `submit_batch` call
/// generated, independent of how many URLs were in the batch.
async fn redis_eval_calls(pool: &Pool<RedisConnectionManager>) -> u64 {
    let mut conn = pool.get().await.unwrap();
    let info: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut *conn)
        .await
        .unwrap();
    let mut total: u64 = 0;
    for line in info.lines() {
        for prefix in ["cmdstat_evalsha:", "cmdstat_eval:"] {
            let Some(rest) = line.strip_prefix(prefix) else {
                continue;
            };
            if let Some(calls_seg) = rest.split(',').find(|s| s.starts_with("calls="))
                && let Some(n) = calls_seg
                    .strip_prefix("calls=")
                    .and_then(|s| s.parse::<u64>().ok())
            {
                total += n;
            }
        }
    }
    total
}

#[tokio::test]
async fn submit_batch_collapses_round_trips_per_shard() {
    // Pins the per-shard batching contract: one `submit_batch` call
    // with N URLs spread across S shards must issue ~S Lua calls,
    // not N. The contract is wire-level so the assertion is wire-
    // level: read `INFO commandstats` before and after.
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(HostHashShardPolicy::new(8));
    let frontier = RedisFrontier::new(
        fx.pool.clone(),
        policy.clone(),
        (0..8).collect(),
        run_id(),
        BloomConfig::default(),
    )
    .await
    .unwrap();

    let entries: Vec<UrlEntry> = (0..100)
        .map(|i| entry(&format!("https://host{i}.test/")))
        .collect();
    let distinct_shards: usize = entries
        .iter()
        .map(|e| policy.shard_key(&e.url))
        .collect::<std::collections::HashSet<_>>()
        .len();

    let before = redis_eval_calls(&fx.pool).await;
    let newly = frontier.submit_batch(entries).await.unwrap();
    let after = redis_eval_calls(&fx.pool).await;

    assert_eq!(newly, 100, "every URL is unique so all 100 are Queued");

    // Expected exactly `distinct_shards` calls. We allow 2x headroom
    // because a fresh script may need one SCRIPT LOAD followed by an
    // EVALSHA on cold pool connections, and to absorb any unrelated
    // EVAL traffic from background paths (none today, but the test
    // shouldn't be brittle to a future maintenance script).
    let delta = (after - before) as usize;
    assert!(
        delta >= 1 && delta <= distinct_shards * 2,
        "expected 1..={} EVAL calls for a 100-URL batch over {} shards, \
         got {}",
        distinct_shards * 2,
        distinct_shards,
        delta,
    );
    // And definitely not the per-URL count the old loop produced.
    assert!(
        delta < 50,
        "submit_batch regressed to per-URL Lua calls: delta={delta}",
    );
}

#[tokio::test]
async fn submit_dedups_across_runs() {
    // Cross-run dedup: a URL submitted under one `run_id` is
    // recognised as duplicate under any other `run_id` for the same
    // Redis deployment. This is the whole point of `seen` being
    // scoped per-shard, not per-run.
    let fx = fixture().await;
    let run_a = run_id();
    let run_b = run_id();
    let frontier_a = single_shard_frontier_with_run_id(&fx.pool, &run_a).await;
    let frontier_b = single_shard_frontier_with_run_id(&fx.pool, &run_b).await;
    assert!(matches!(
        frontier_a.submit(entry("https://a.test/")).await.unwrap(),
        SubmitOutcome::Queued
    ));
    assert!(matches!(
        frontier_b.submit(entry("https://a.test/")).await.unwrap(),
        SubmitOutcome::SkippedDuplicate
    ));
}

// ---------------------------------------------------------------------------
// Claim / Promote / Empty
// ---------------------------------------------------------------------------

#[tokio::test]
async fn claim_returns_empty_when_no_urls_submitted() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;
    let outcome = frontier.claim(&IDENTITY).await.unwrap();
    assert!(matches!(outcome, ClaimOutcome::Empty));
}

#[tokio::test]
async fn submit_then_tick_then_claim_yields_url() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;
    frontier.submit(entry("https://a.test/")).await.unwrap();

    // Without a `tick` the host is in `wake` (score = 0) but not yet
    // promoted to `ready`. Claim returns EmptyHint with the
    // soonest-wake score, or Empty if the score was already in the
    // past — either is acceptable behavior before tick.
    frontier.tick().await.unwrap();

    let (_, entry, _) = unwrap_claimed(frontier.claim(&IDENTITY).await.unwrap());
    assert_eq!(entry.url.as_str(), "https://a.test/");
}

#[tokio::test]
async fn claim_returns_empty_hint_when_only_wake_has_entries() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;

    // Submit and claim once to drive the host into `wake` (the
    // claim's safety-margin write).
    frontier.submit(entry("https://a.test/")).await.unwrap();
    frontier.tick().await.unwrap();
    let (_, _, attempt) = unwrap_claimed(frontier.claim(&IDENTITY).await.unwrap());

    // Push the host's wake-time deliberately into the future so a
    // second claim must surface EmptyHint, not Claimed.
    let until = std::time::Instant::now() + Duration::from_secs(30);
    frontier.advance_wake("a.test", until).await.unwrap();
    frontier.ack(&attempt).await.unwrap();

    let outcome = frontier.claim(&IDENTITY).await.unwrap();
    assert!(
        matches!(
            outcome,
            ClaimOutcome::EmptyHint { .. } | ClaimOutcome::Empty
        ),
        "expected EmptyHint (host still in wake) or Empty (host_queue drained); got {outcome:?}",
    );
}

#[tokio::test]
async fn advance_wake_blocks_re_claim_until_promoted() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;

    // Two URLs for the same host so we can claim once, push the host
    // into wake, then assert a re-claim doesn't yield until tick
    // promotes the wake-elapsed host back into ready.
    frontier.submit(entry("https://a.test/1")).await.unwrap();
    frontier.submit(entry("https://a.test/2")).await.unwrap();
    frontier.tick().await.unwrap();

    let (_, _, attempt) = unwrap_claimed(frontier.claim(&IDENTITY).await.unwrap());

    // Set the host's wake to 200ms out, ack the claim.
    let until = std::time::Instant::now() + Duration::from_millis(200);
    frontier.advance_wake("a.test", until).await.unwrap();
    frontier.ack(&attempt).await.unwrap();

    // Immediately, the host is in wake; a tick now does not promote.
    frontier.tick().await.unwrap();
    let outcome_during_wake = frontier.claim(&IDENTITY).await.unwrap();
    assert!(
        !matches!(outcome_during_wake, ClaimOutcome::Claimed { .. }),
        "should not claim during wake window; got {outcome_during_wake:?}",
    );

    // After the wake elapses, tick promotes; next claim succeeds.
    tokio::time::sleep(Duration::from_millis(300)).await;
    frontier.tick().await.unwrap();
    let claimed = frontier.claim(&IDENTITY).await.unwrap();
    let (_, entry, _) = unwrap_claimed(claimed);
    assert_eq!(entry.url.as_str(), "https://a.test/2");
}

// ---------------------------------------------------------------------------
// Lease + reclaim
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expired_lease_is_reclaimed_and_url_re_pushed() {
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let frontier = RedisFrontier::new(
        fx.pool.clone(),
        policy,
        vec![0],
        run_id(),
        BloomConfig::default(),
    )
    .await
    .unwrap()
    .with_lease_timeout(Duration::from_millis(150));

    frontier.submit(entry("https://a.test/")).await.unwrap();
    frontier.tick().await.unwrap();
    let _ = unwrap_claimed(frontier.claim(&IDENTITY).await.unwrap());
    // Deliberately don't ack: simulate worker crash mid-fetch.

    // Wait past the lease + claim-safety wake, then tick to reclaim
    // + promote.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let affected = frontier.tick().await.unwrap();
    assert!(
        affected >= 1,
        "tick should reclaim at least one expired lease; got affected={affected}"
    );

    // One more tick to be sure the now-eligible host gets promoted
    // into ready (reclaim re-stamps wake to now_ms, so the next tick
    // moves it).
    frontier.tick().await.unwrap();

    let (_, entry, _) = unwrap_claimed(frontier.claim(&IDENTITY).await.unwrap());
    assert_eq!(entry.url.as_str(), "https://a.test/");
}

// ---------------------------------------------------------------------------
// AttemptId + ack
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ack_removes_url_from_state() {
    let fx = fixture().await;
    let frontier = single_shard_frontier(&fx.pool).await;
    frontier.submit(entry("https://a.test/")).await.unwrap();
    frontier.tick().await.unwrap();
    let (_, _, attempt) = unwrap_claimed(frontier.claim(&IDENTITY).await.unwrap());
    frontier.ack(&attempt).await.unwrap();
    // Idempotent: a second ack is a no-op.
    frontier.ack(&attempt).await.unwrap();

    // No more URLs queued for this host.
    let outcome = frontier.claim(&IDENTITY).await.unwrap();
    assert!(matches!(
        outcome,
        ClaimOutcome::Empty | ClaimOutcome::EmptyHint { .. }
    ));
    assert_eq!(frontier.len().await.unwrap(), 0);
}

// ---------------------------------------------------------------------------
// Sharding
// ---------------------------------------------------------------------------

#[tokio::test]
async fn host_hash_policy_routes_urls_to_owned_shard() {
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(HostHashShardPolicy::new(4));
    // Compute which shard "example.com" falls into so we can own it.
    let shard = policy.shard_key_from_host("example.com");
    let frontier = RedisFrontier::new(
        fx.pool.clone(),
        policy,
        vec![shard],
        run_id(),
        BloomConfig::default(),
    )
    .await
    .unwrap();

    let outcome = frontier
        .submit(entry("https://example.com/p1"))
        .await
        .unwrap();
    assert!(matches!(outcome, SubmitOutcome::Queued));

    frontier.tick().await.unwrap();
    let (_, entry, _) = unwrap_claimed(frontier.claim(&IDENTITY).await.unwrap());
    assert_eq!(entry.url.as_str(), "https://example.com/p1");
}

#[tokio::test]
async fn submit_rejects_url_for_unowned_shard() {
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(HostHashShardPolicy::new(8));
    // Own only shard 0; submit a URL whose host hashes elsewhere.
    let owned = vec![0];
    let frontier = RedisFrontier::new(
        fx.pool.clone(),
        policy.clone(),
        owned,
        run_id(),
        BloomConfig::default(),
    )
    .await
    .unwrap();

    // Find a host that DOESN'T hash to shard 0.
    let candidates = ["a.test", "b.test", "c.test", "d.test", "e.test", "f.test"];
    let foreign = candidates
        .iter()
        .find(|h| policy.shard_key_from_host(h) != 0)
        .expect("at least one candidate should not hash to shard 0");
    let err = frontier
        .submit(entry(&format!("https://{foreign}/")))
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not owned"),
        "expected ShardNotOwned error; got {msg}",
    );
}
