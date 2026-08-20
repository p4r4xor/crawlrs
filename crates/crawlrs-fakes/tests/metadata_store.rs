//! Tests for `InMemoryMetadataStore`.

use crawlrs_core::{AttemptId, CanonicalUrl, MetadataStore, RunId, SuccessRecord};
use crawlrs_fakes::InMemoryMetadataStore;

#[tokio::test]
async fn mark_succeeded_dedupes_by_attempt_id() {
    let store = InMemoryMetadataStore::new();
    let url = CanonicalUrl::parse("https://a.test/").unwrap();
    store
        .mark_attempting(&url, &RunId::new("run-1"), 0)
        .await
        .unwrap();

    // First attempt: one history row.
    let first = AttemptId::new("attempt-1");
    store
        .mark_succeeded(&SuccessRecord {
            url: &url,
            attempt_id: &first,
            blob_path: "blob://1",
            content_hash: 1,
            outbound: &[],
        })
        .await
        .unwrap();
    assert_eq!(store.succeeded_history_count(), 1);

    // Same attempt_id (re-delivered after a stall between
    // mark_succeeded and frontier.ack): MUST be idempotent.
    store
        .mark_succeeded(&SuccessRecord {
            url: &url,
            attempt_id: &first,
            blob_path: "blob://1",
            content_hash: 1,
            outbound: &[],
        })
        .await
        .unwrap();
    assert_eq!(
        store.succeeded_history_count(),
        1,
        "redelivery of the same attempt must not duplicate the history row",
    );

    // Different attempt_id (a fresh delivery, e.g. the URL was
    // re-discovered): a new history row IS expected.
    let second = AttemptId::new("attempt-2");
    store
        .mark_succeeded(&SuccessRecord {
            url: &url,
            attempt_id: &second,
            blob_path: "blob://2",
            content_hash: 2,
            outbound: &[],
        })
        .await
        .unwrap();
    assert_eq!(
        store.succeeded_history_count(),
        2,
        "distinct attempt_ids must each produce a history row",
    );
}
