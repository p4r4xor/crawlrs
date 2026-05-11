//! End-to-end recovery scenarios composed against in-memory test
//! doubles (no Docker, no real wall clock for the frontier work).
//!
//! Architectural invariants asserted here:
//!
//! 1. A claim that goes un-acked (worker crash) is reclaimed once
//!    the lease expires, the URL re-enters its host queue, and the
//!    next claim picks it up. The `AttemptId` is derived from
//!    `(shard, url_id, host)` so it's stable across reclaims of the
//!    same URL - that's load-bearing for the metadata ledger's
//!    `(url, attempt_id)` UNIQUE constraint (Invariant 2 below).
//!
//! 2. The metadata ledger's `(url, attempt_id)` UNIQUE constraint
//!    on `mark_succeeded` dedupes redelivered attempts so the
//!    success history row count stays at exactly one.
//!
//! 3. The outbox `(parent_url_id, parent_attempt_id, url)` UNIQUE
//!    constraint dedupes outbound rows on attempt re-delivery.
//!
//! 4. The outbox publisher drains rows into the Frontier at-least-
//!    once; on its own re-runs the frontier's bloom-fronted submit
//!    absorbs the duplicates so each outbound URL lands once.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::{
    AttemptId, CanonicalUrl, ClaimOutcome, Frontier, MetadataStore, Outbox, ShardingPolicy,
    SingleShardPolicy, SuccessRecord, UrlEntry, WorkerIdentity,
};
use crawlrs_fakes::{InMemoryFrontier, InMemoryMetadataStore};
use crawlrs_fakes::ManualClock;
use crawlrs_runtime::outbox_publisher;
use tokio::sync::watch;

fn url(s: &str) -> CanonicalUrl {
    CanonicalUrl::parse(s).unwrap()
}

const IDENTITY: WorkerIdentity = WorkerIdentity::new(0, 0);

// ---------------------------------------------------------------------------
// Lease-based recovery (Invariant 1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lease_expiry_re_pushes_unacked_url_and_next_claim_yields_same_attempt_id() {
    // Worker A claims, never acks (crash). Lease expires. Tick
    // reclaims; the URL re-enters its host queue. The next claim
    // picks it up. AttemptId is content-addressed by `(shard,
    // url_id, host)` so the redelivered claim carries the same
    // token - that's load-bearing for downstream UNIQUE-constraint
    // dedup at the metadata layer.
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let clock = Arc::new(ManualClock::new(1_000_000));
    let frontier = InMemoryFrontier::new(policy, vec![0])
        .with_clock(clock.clone())
        .with_lease_timeout(Duration::from_millis(500));

    frontier
        .submit(UrlEntry::seed(url("https://a.test/")))
        .await
        .unwrap();
    frontier.tick().await.unwrap();

    let first = match frontier.claim(&IDENTITY).await.unwrap() {
        ClaimOutcome::Claimed { attempt_id, .. } => attempt_id,
        other => panic!("expected first claim to succeed; got {other:?}"),
    };
    // Worker A "crashes" — never calls ack.

    // Advance past the lease; tick reclaims + promotes.
    clock.advance_ms(1_000);
    let affected = frontier.tick().await.unwrap();
    assert!(affected >= 1, "tick should reclaim the expired lease");
    frontier.tick().await.unwrap(); // one more pass for the promote step

    let second = match frontier.claim(&IDENTITY).await.unwrap() {
        ClaimOutcome::Claimed {
            attempt_id, entry, ..
        } => {
            assert_eq!(entry.url.as_str(), "https://a.test/");
            attempt_id
        }
        other => panic!("expected re-claim to succeed after reclaim; got {other:?}"),
    };
    assert_eq!(
        first, second,
        "stable AttemptId across reclaim; downstream `(url, attempt_id)` UNIQUE \
         dedup at the metadata layer relies on this",
    );
}

// ---------------------------------------------------------------------------
// Metadata-side dedup (Invariant 2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duplicate_mark_succeeded_for_same_attempt_appends_one_history_row() {
    // Even if the runtime re-runs the post-fetch pipeline because
    // the reclaim path re-delivered the same URL (and the pipeline
    // happens to use the same attempt_id - rare but possible if the
    // caller re-issues), mark_succeeded twice with the same
    // (url, attempt_id) yields exactly one row in url_history.
    let store = InMemoryMetadataStore::new();
    let target = url("https://a.test/");
    store.mark_attempting(&target, "run-1", 0).await.unwrap();

    let attempt = AttemptId::new("s0|attempt-1|a.test");
    let record = SuccessRecord {
        url: &target,
        attempt_id: &attempt,
        blob_path: "blob://1",
        content_hash: 1,
        outbound: &[],
    };
    store.mark_succeeded(&record).await.unwrap();
    store.mark_succeeded(&record).await.unwrap();

    assert_eq!(
        store.succeeded_history_count(),
        1,
        "the (url, attempt_id) UNIQUE constraint dedupes redelivered attempts",
    );
}

// ---------------------------------------------------------------------------
// Outbox-side dedup (Invariant 3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn outbox_dedupes_outbound_on_attempt_redelivery() {
    // When mark_succeeded is called twice with the same attempt_id
    // and a non-empty outbound list, the outbox table records
    // exactly N rows, not 2N. The InMemoryMetadataStore mirrors the
    // Postgres `(parent_url_id, parent_attempt_id, url)` UNIQUE
    // constraint via its `dedupe` HashSet.
    let metadata = InMemoryMetadataStore::new();
    let parent = url("https://parent.test/");
    let attempt = AttemptId::new("s0|attempt-1|parent.test");
    metadata.mark_attempting(&parent, "run-1", 0).await.unwrap();

    let outbound: Vec<UrlEntry> = ["https://a.test/", "https://b.test/", "https://c.test/"]
        .into_iter()
        .map(|u| UrlEntry::seed(url(u)))
        .collect();

    let record = SuccessRecord {
        url: &parent,
        attempt_id: &attempt,
        blob_path: "blob://1",
        content_hash: 1,
        outbound: &outbound,
    };
    metadata.mark_succeeded(&record).await.unwrap();
    metadata.mark_succeeded(&record).await.unwrap();

    assert_eq!(
        metadata.outbox_row_count(),
        3,
        "the second mark_succeeded for the same (parent, attempt) is absorbed by the \
         outbox UNIQUE constraint; we must not see 6 rows",
    );
}

// ---------------------------------------------------------------------------
// Publisher round-trip (Invariant 4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn outbox_publisher_drains_into_frontier_atleast_once() {
    // After mark_succeeded commits outbound URLs into the outbox,
    // the publisher drains them into the Frontier on its first tick.
    // If the publisher somehow ran twice, the frontier's bloom-
    // fronted submit absorbs the duplicates so each outbound URL
    // lands exactly once.
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let frontier: Arc<dyn Frontier> = Arc::new(InMemoryFrontier::new(policy, vec![0]));
    let metadata = Arc::new(InMemoryMetadataStore::new());
    let outbox: Arc<dyn Outbox> = metadata.clone();

    let parent = url("https://parent.test/");
    metadata.mark_attempting(&parent, "run-1", 0).await.unwrap();
    let outbound: Vec<UrlEntry> = ["https://a.test/", "https://b.test/", "https://c.test/"]
        .into_iter()
        .map(|u| UrlEntry::seed(url(u)))
        .collect();
    let attempt = AttemptId::new("s0|attempt-1|parent.test");
    metadata
        .mark_succeeded(&SuccessRecord {
            url: &parent,
            attempt_id: &attempt,
            blob_path: "blob://1",
            content_hash: 1,
            outbound: &outbound,
        })
        .await
        .unwrap();

    let (tx, rx) = watch::channel(false);
    let publisher = tokio::spawn(outbox_publisher(
        outbox.clone(),
        frontier.clone(),
        rx,
        Duration::from_millis(20),
    ));
    tokio::time::sleep(Duration::from_millis(80)).await;
    tx.send(true).unwrap();
    publisher.await.unwrap();

    assert_eq!(frontier.len().await.unwrap(), 3);
    assert_eq!(
        metadata.unpublished_outbox_count(),
        0,
        "publisher must mark drained rows so they don't reappear",
    );
}
