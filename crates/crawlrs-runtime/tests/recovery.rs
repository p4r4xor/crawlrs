// Harness-gated: these tests assert the tier-1 PEL replay semantic
// from the streams-based frontier (same `AttemptId` on re-delivery to
// the same worker). The new model uses lease-expiry reclaim per
// ADR-0019: the URL is re-pushed and a fresh `AttemptId` is assigned
// on the next claim. Re-enable with a rewrite asserting the
// lease-based recovery shape.
#![cfg(any())]

//! End-to-end recovery scenarios composed against in-memory test
//! doubles (no Docker, no real time).
//!
//! These are the architectural invariants the `WorkerIdentity` +
//! `AttemptId` correlation pair must uphold:
//!
//! 1. A worker that crashes mid-pipeline, when respawned with the
//!    same identity, reclaims its in-flight URLs immediately via
//!    tier-1 PEL replay (no XAUTOCLAIM idle wait).
//!
//! 2. The same `AttemptId` being processed twice (e.g. because tier-1
//!    re-delivered the entry to the resumed worker) does NOT double
//!    the success-side history rows in the metadata ledger.
//!
//! These tests live alongside the runtime so future refactors that
//! drop the load-bearing properties (stable consumer name, attempt-id
//! threading, ON CONFLICT history dedupe) fail loudly here.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::{
    AttemptId, CanonicalUrl, Frontier, MetadataStore, Outbox, ShardingPolicy, SingleShardPolicy,
    SuccessRecord, UrlEntry, WorkerIdentity,
};
use crawlrs_fakes::{InMemoryFrontier, InMemoryMetadataStore};
use crawlrs_runtime::outbox_publisher;
use tokio::sync::watch;

fn url(s: &str) -> CanonicalUrl {
    CanonicalUrl::parse(s).unwrap()
}

#[tokio::test]
async fn restarted_worker_reclaims_in_flight_url_via_tier_1() {
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let frontier = InMemoryFrontier::new(policy, vec![0]);
    let identity = WorkerIdentity::new(0, 0);

    frontier
        .submit(UrlEntry::seed(url("https://a.test/")))
        .await
        .unwrap();
    let mid_flight = frontier.claim(&identity).await.unwrap().unwrap();

    // The "worker process" terminates here without acking. We simulate
    // that by simply not calling ack() and proceeding to a fresh
    // claim.

    let recovered = frontier
        .claim(&identity)
        .await
        .unwrap()
        .expect("tier-1 PEL replay must surface the in-flight URL");
    assert_eq!(
        recovered.attempt_id, mid_flight.attempt_id,
        "the resumed worker's AttemptId is identical to the original delivery's; \
         redelivery does not bump the correlation token, so downstream dedupe at \
         the metadata layer correctly recognises this as the same attempt",
    );
    assert_eq!(recovered.entry.url, mid_flight.entry.url);
}

#[tokio::test]
async fn duplicate_mark_succeeded_for_same_attempt_appends_one_history_row() {
    // The metadata-side mirror of the above: even if the runtime
    // re-runs the post-fetch pipeline because tier-1 re-delivered the
    // same attempt, mark_succeeded twice with the same (url,
    // attempt_id) yields exactly one row in url_history.
    let store = InMemoryMetadataStore::new();
    let url = url("https://a.test/");
    store.mark_attempting(&url, "run-1", 0).await.unwrap();

    let attempt = AttemptId::new("0|1714867200000-0");
    let record = SuccessRecord {
        url: &url,
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
        "the (url, attempt_id) UNIQUE constraint dedupes the redelivered attempt",
    );
}

#[tokio::test]
async fn full_recovery_round_trip_through_pipeline_states() {
    // Stitches the two invariants together: claim, simulate crash
    // mid-pipeline (after store.write but before XACK), restart with
    // the same identity, mark_succeeded a second time for the same
    // attempt, ack. The history must hold exactly one succeeded row.
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let frontier = InMemoryFrontier::new(policy, vec![0]);
    let metadata = InMemoryMetadataStore::new();
    let identity = WorkerIdentity::new(0, 0);
    let target = url("https://a.test/");

    frontier
        .submit(UrlEntry::seed(target.clone()))
        .await
        .unwrap();

    // Original delivery + first half of pipeline.
    let first = frontier.claim(&identity).await.unwrap().unwrap();
    metadata.mark_attempting(&target, "run-1", 0).await.unwrap();
    metadata
        .mark_succeeded(&SuccessRecord {
            url: &target,
            attempt_id: &first.attempt_id,
            blob_path: "blob://v1",
            content_hash: 1,
            outbound: &[],
        })
        .await
        .unwrap();
    // Worker dies here, before frontier.ack(). The PEL still holds the
    // entry on the frontier side.

    // Restart: same identity, tier-1 surfaces the unack'd entry.
    let resumed = frontier.claim(&identity).await.unwrap().unwrap();
    assert_eq!(resumed.attempt_id, first.attempt_id);

    // Second pass through the pipeline (simulating the full re-run).
    metadata
        .mark_succeeded(&SuccessRecord {
            url: &target,
            attempt_id: &resumed.attempt_id,
            blob_path: "blob://v1",
            content_hash: 1,
            outbound: &[],
        })
        .await
        .unwrap();
    frontier.ack(&resumed.attempt_id).await.unwrap();

    // Postcondition: the metadata ledger recorded the success exactly
    // once despite two mark_succeeded calls. The frontier is drained.
    assert_eq!(
        metadata.succeeded_history_count(),
        1,
        "redelivery of the same attempt must not duplicate ledger rows",
    );
    assert_eq!(frontier.len().await.unwrap(), 0);
}

#[tokio::test]
async fn outbox_dedupes_outbound_on_attempt_redelivery() {
    // The outbound side of attempt redelivery: when mark_succeeded is
    // called twice with the same attempt_id and a non-empty outbound
    // list, the outbox table must record exactly N rows, not 2N. The
    // Postgres `(parent_url_id, parent_attempt_id, url)` UNIQUE
    // constraint enforces this; the InMemoryMetadataStore mirrors the
    // same invariant via its `dedupe` HashSet.
    let metadata = InMemoryMetadataStore::new();
    let parent = CanonicalUrl::parse("https://parent.test/").unwrap();
    let attempt = AttemptId::new("0|attempt-1");
    metadata.mark_attempting(&parent, "run-1", 0).await.unwrap();

    let outbound: Vec<UrlEntry> = ["https://a.test/", "https://b.test/", "https://c.test/"]
        .into_iter()
        .map(|u| UrlEntry::seed(CanonicalUrl::parse(u).unwrap()))
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
        "the second mark_succeeded for the same (parent, attempt) is \
         absorbed by the outbox UNIQUE constraint; we must not see 6 rows",
    );
}

#[tokio::test]
async fn outbox_publisher_drains_into_frontier_atleast_once() {
    // End-to-end through the publisher task: after a worker pipeline
    // commits outbound URLs via mark_succeeded, the publisher drains
    // them into the Frontier on its first tick. If the publisher
    // somehow ran twice, the Frontier seen-set would absorb the
    // duplicates so the queue depth is exactly N.
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let frontier: Arc<dyn Frontier> = Arc::new(InMemoryFrontier::new(policy, vec![0]));
    let metadata = Arc::new(InMemoryMetadataStore::new());
    let outbox: Arc<dyn Outbox> = metadata.clone();

    let parent = CanonicalUrl::parse("https://parent.test/").unwrap();
    metadata.mark_attempting(&parent, "run-1", 0).await.unwrap();
    let outbound: Vec<UrlEntry> = ["https://a.test/", "https://b.test/", "https://c.test/"]
        .into_iter()
        .map(|u| UrlEntry::seed(CanonicalUrl::parse(u).unwrap()))
        .collect();
    let attempt = AttemptId::new("0|attempt-1");
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

    // Spawn the publisher with a short interval; let it tick a few
    // times; signal shutdown.
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

    // Frontier holds exactly the three URLs (no duplicates from
    // multiple drain cycles, no orphans from publisher missing rows).
    assert_eq!(frontier.len().await.unwrap(), 3);

    // No unpublished rows remain on the outbox side.
    assert_eq!(
        metadata.unpublished_outbox_count(),
        0,
        "publisher must mark drained rows so they don't reappear",
    );
}
