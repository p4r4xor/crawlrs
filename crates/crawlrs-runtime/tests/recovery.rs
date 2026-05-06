//! End-to-end recovery scenarios composed against in-memory test
//! doubles (no Docker, no real time).
//!
//! These are the architectural invariants captured by the
//! `WorkerIdentity` + `AttemptId` work in ver.11:
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

use crawlrs_core::{
    AttemptId, CanonicalUrl, Frontier, MetadataStore, ShardingPolicy, SingleShardPolicy, UrlEntry,
    WorkerIdentity,
};
use crawlrs_fakes::{InMemoryFrontier, InMemoryMetadataStore};

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
    store
        .mark_succeeded(&url, &attempt, "blob://1", 1)
        .await
        .unwrap();
    store
        .mark_succeeded(&url, &attempt, "blob://1", 1)
        .await
        .unwrap();

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
        .mark_succeeded(&target, &first.attempt_id, "blob://v1", 1)
        .await
        .unwrap();
    // Worker dies here, before frontier.ack(). The PEL still holds the
    // entry on the frontier side.

    // Restart: same identity, tier-1 surfaces the unack'd entry.
    let resumed = frontier.claim(&identity).await.unwrap().unwrap();
    assert_eq!(resumed.attempt_id, first.attempt_id);

    // Second pass through the pipeline (simulating the full re-run).
    metadata
        .mark_succeeded(&target, &resumed.attempt_id, "blob://v1", 1)
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
