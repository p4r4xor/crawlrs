//! Tests for `InMemoryFrontier`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crawlrs_core::{
    CanonicalUrl, ClaimOutcome, Frontier, ShardingPolicy, SingleShardPolicy, SubmitOutcome,
    UrlEntry, WorkerIdentity,
};
use crawlrs_fakes::{InMemoryFrontier, ManualClock};

fn url(s: &str) -> CanonicalUrl {
    CanonicalUrl::parse(s).unwrap()
}
fn entry(s: &str) -> UrlEntry {
    UrlEntry::seed(url(s))
}

fn fresh_frontier() -> InMemoryFrontier {
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    InMemoryFrontier::new(policy, vec![0])
}

fn ident() -> WorkerIdentity {
    WorkerIdentity::new(0, 0)
}

#[tokio::test]
async fn submit_then_tick_then_claim_yields_url() {
    let f = fresh_frontier();
    let outcome = f.submit(entry("https://a.test/")).await.unwrap();
    assert!(matches!(outcome, SubmitOutcome::Queued));

    // No tick yet: the host is in `wake` (score=0) but not in
    // `ready`, so claim returns EmptyHint (or Empty if score
    // happens to be in the past, which it is).
    match f.claim(&ident()).await.unwrap() {
        ClaimOutcome::Empty | ClaimOutcome::EmptyHint { .. } => {}
        ClaimOutcome::Claimed { .. } => panic!("should not have a ready host yet"),
    }

    // Promote: tick drains wake -> ready.
    f.tick().await.unwrap();

    let claimed = f.claim(&ident()).await.unwrap();
    match claimed {
        ClaimOutcome::Claimed { entry, .. } => {
            assert_eq!(entry.url.as_str(), "https://a.test/");
        }
        other => panic!("expected Claimed, got {other:?}"),
    }
}

#[tokio::test]
async fn duplicate_submit_is_dropped_at_seen_set() {
    let f = fresh_frontier();
    assert!(matches!(
        f.submit(entry("https://a.test/")).await.unwrap(),
        SubmitOutcome::Queued
    ));
    assert!(matches!(
        f.submit(entry("https://a.test/")).await.unwrap(),
        SubmitOutcome::SkippedDuplicate
    ));
    assert_eq!(f.len().await.unwrap(), 1);
}

#[tokio::test]
async fn ack_removes_url_from_state() {
    let f = fresh_frontier();
    f.submit(entry("https://a.test/")).await.unwrap();
    f.tick().await.unwrap();
    let claimed = f.claim(&ident()).await.unwrap();
    let ClaimOutcome::Claimed { attempt_id, .. } = claimed else {
        panic!("expected Claimed");
    };
    f.ack(&attempt_id).await.unwrap();
    // Idempotent second ack.
    f.ack(&attempt_id).await.unwrap();
    // URL HASH gone, inflight gone, host_queue empty.
    assert_eq!(f.len().await.unwrap(), 0);
}

#[tokio::test]
async fn advance_wake_blocks_re_claim_until_promoted() {
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let clock = Arc::new(ManualClock::new(1_000_000));
    let f = InMemoryFrontier::new(policy, vec![0]).with_clock(clock.clone());

    // Submit two URLs for the same host; promote them.
    f.submit(entry("https://a.test/1")).await.unwrap();
    f.submit(entry("https://a.test/2")).await.unwrap();
    f.tick().await.unwrap();

    // Claim the first; host is now neither in ready nor wake.
    let ClaimOutcome::Claimed { attempt_id, .. } = f.claim(&ident()).await.unwrap() else {
        panic!("expected Claimed for the first URL");
    };

    // Worker reports the host's wake-time 5s out.
    let now = Instant::now();
    f.advance_wake("a.test", now + Duration::from_secs(5))
        .await
        .unwrap();
    f.ack(&attempt_id).await.unwrap();

    // Tick now: host's wake is 5s out, so it does NOT promote.
    f.tick().await.unwrap();
    let again = f.claim(&ident()).await.unwrap();
    assert!(
        matches!(again, ClaimOutcome::EmptyHint { .. } | ClaimOutcome::Empty),
        "second claim should not yield until wake elapses; got {again:?}"
    );

    // Advance the clock past the wake-time; tick promotes.
    clock.advance_ms(6_000);
    f.tick().await.unwrap();
    let third = f.claim(&ident()).await.unwrap();
    assert!(
        matches!(third, ClaimOutcome::Claimed { .. }),
        "after wake elapses, the second URL should claim; got {third:?}"
    );
}

#[tokio::test]
async fn expired_lease_reclaim_re_pushes_url() {
    // The recovery path: a worker holds a lease but never acks.
    // After the lease expires, `tick` re-pushes the URL into its
    // host_queue and the next claim picks it up.
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let clock = Arc::new(ManualClock::new(1_000_000));
    let f = InMemoryFrontier::new(policy, vec![0])
        .with_clock(clock.clone())
        .with_lease_timeout(Duration::from_millis(500));

    f.submit(entry("https://a.test/")).await.unwrap();
    f.tick().await.unwrap();
    let _first = f.claim(&ident()).await.unwrap();
    // Deliberately do not ack. Worker "crashed".

    // Advance past the lease; tick reclaims.
    clock.advance_ms(1_000);
    let affected = f.tick().await.unwrap();
    assert!(affected >= 1, "tick should reclaim the expired lease");

    // Next claim sees the re-pushed URL.
    f.tick().await.unwrap();
    let recovered = f.claim(&ident()).await.unwrap();
    match recovered {
        ClaimOutcome::Claimed { entry, .. } => {
            assert_eq!(entry.url.as_str(), "https://a.test/");
        }
        other => panic!("expected Claimed after reclaim; got {other:?}"),
    }
}
