//! Integration tests for `PostgresMetadataStore` against a real
//! Postgres instance via testcontainers-rs. Each test brings up its
//! own container and runs migrations on startup; tests use unique
//! URLs so they could share a container in the future without
//! cross-test contamination.

use std::sync::{Arc, Mutex};

use crawlrs_core::{
    AttemptId, CanonicalUrl, Error, FailureKind, MetadataStore, Outbox, RunId, SuccessRecord,
    UrlStatus,
};
use crawlrs_metadata::PostgresMetadataStore;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

struct Fixture {
    _container: ContainerAsync<Postgres>,
    pool: PgPool,
}

async fn fixture() -> Fixture {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("docker must be running for integration tests");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect to postgres");
    PostgresMetadataStore::migrate(&pool)
        .await
        .expect("apply migrations");
    Fixture {
        _container: container,
        pool,
    }
}

/// Generate a unique URL per test (different host) so concurrent tests
/// against the same Postgres container don't trample each other.
fn unique_url(slug: &str) -> CanonicalUrl {
    let id = cuid2::create_id();
    CanonicalUrl::parse(&format!("https://{slug}-{id}.test/")).unwrap()
}

fn run_id() -> String {
    format!("run-{}", cuid2::create_id())
}

#[tokio::test]
async fn unseen_url_returns_none_from_get() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("unseen");
    assert!(store.get(&url).await.unwrap().is_none());
}

#[tokio::test]
async fn mark_attempting_creates_row_with_in_progress_status() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("attempting");
    let rid = run_id();

    store
        .mark_attempting(&url, &RunId::new(rid.clone()), 0)
        .await
        .unwrap();

    let m = store
        .get(&url)
        .await
        .unwrap()
        .expect("row exists after mark_attempting");
    assert_eq!(m.status, UrlStatus::InProgress);
    assert_eq!(m.retry_count, 0);
    assert_eq!(m.last_run_id, rid);
    assert_eq!(m.depth, 0);
    assert!(m.blob_path.is_none());
    assert!(m.content_hash.is_none());
}

#[tokio::test]
async fn mark_attempting_preserves_discovered_at_on_re_attempt() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("rediscovery");

    store
        .mark_attempting(&url, &RunId::new("run-1"), 0)
        .await
        .unwrap();
    let first = store.get(&url).await.unwrap().unwrap();
    let original_discovered_at = first.discovered_at;

    // Sleep enough for NOW() to advance past the original timestamp's
    // microsecond resolution.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    store
        .mark_attempting(&url, &RunId::new("run-2"), 1)
        .await
        .unwrap();
    let second = store.get(&url).await.unwrap().unwrap();

    assert_eq!(
        second.discovered_at, original_discovered_at,
        "discovered_at must not move"
    );
    assert_eq!(second.last_run_id, "run-2");
    assert_eq!(second.depth, 1);
    assert!(
        second.updated_at >= original_discovered_at,
        "updated_at advances"
    );
}

#[tokio::test]
async fn mark_succeeded_records_blob_and_content_hash() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("succeeded");
    store
        .mark_attempting(&url, &RunId::new("run-1"), 0)
        .await
        .unwrap();

    let attempt = AttemptId::new("attempt-1");
    store
        .mark_succeeded(&SuccessRecord {
            url: &url,
            attempt_id: &attempt,
            blob_path: "s3://bucket/run-1/0.parquet",
            content_hash: 0xabcd_1234_5678_9abc,
            outbound: &[],
        })
        .await
        .unwrap();

    let m = store.get(&url).await.unwrap().unwrap();
    assert_eq!(m.status, UrlStatus::Succeeded);
    assert_eq!(m.retry_count, 0);
    assert_eq!(m.blob_path.as_deref(), Some("s3://bucket/run-1/0.parquet"));
    assert_eq!(m.content_hash, Some(0xabcd_1234_5678_9abc));
}

#[tokio::test]
async fn mark_failed_increments_retry_count() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("failing");
    store
        .mark_attempting(&url, &RunId::new("run-1"), 0)
        .await
        .unwrap();

    let count1 = store
        .mark_failed(&url, FailureKind::TooManyRequests)
        .await
        .unwrap();
    let count2 = store
        .mark_failed(&url, FailureKind::TooManyRequests)
        .await
        .unwrap();
    let count3 = store
        .mark_failed(&url, FailureKind::ServiceUnavailable)
        .await
        .unwrap();

    assert_eq!(count1, 1);
    assert_eq!(count2, 2);
    assert_eq!(count3, 3);

    let m = store.get(&url).await.unwrap().unwrap();
    assert_eq!(m.status, UrlStatus::FailedTransient);
    assert_eq!(m.retry_count, 3);
}

#[tokio::test]
async fn mark_succeeded_resets_retry_count() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("recovers");
    store
        .mark_attempting(&url, &RunId::new("run-1"), 0)
        .await
        .unwrap();
    store.mark_failed(&url, FailureKind::Timeout).await.unwrap();
    store.mark_failed(&url, FailureKind::Timeout).await.unwrap();

    let attempt = AttemptId::new("attempt-1");
    store
        .mark_succeeded(&SuccessRecord {
            url: &url,
            attempt_id: &attempt,
            blob_path: "/data/0.warc",
            content_hash: 1,
            outbound: &[],
        })
        .await
        .unwrap();

    let m = store.get(&url).await.unwrap().unwrap();
    assert_eq!(m.status, UrlStatus::Succeeded);
    assert_eq!(m.retry_count, 0, "successful fetch resets the counter");
}

#[tokio::test]
async fn mark_permanently_failed_lands_in_dlq() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("doomed");
    store
        .mark_attempting(&url, &RunId::new("run-1"), 0)
        .await
        .unwrap();
    store
        .mark_failed(&url, FailureKind::ConnectReset)
        .await
        .unwrap();

    let dlq_before = store.dlq_size().await.unwrap();
    store
        .mark_permanently_failed(&url, "max retries exceeded: ConnectReset")
        .await
        .unwrap();
    let dlq_after = store.dlq_size().await.unwrap();

    assert_eq!(dlq_after, dlq_before + 1);

    let m = store.get(&url).await.unwrap().unwrap();
    assert_eq!(m.status, UrlStatus::PermanentlyFailed);
}

#[tokio::test]
async fn get_returns_state_from_prior_run() {
    // `get(url)` is the operator-facing read of the metadata ledger:
    // "what state is this URL in right now, including last successful
    // run + blob path?" Used by operator forensics and DLQ queries;
    // no longer on the hot path (cross-run dedup moved to the
    // frontier bloom filter).
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("cross-run");

    store
        .mark_attempting(&url, &RunId::new("run-1"), 0)
        .await
        .unwrap();
    let attempt = AttemptId::new("attempt-1");
    store
        .mark_succeeded(&SuccessRecord {
            url: &url,
            attempt_id: &attempt,
            blob_path: "/data/run-1.parquet",
            content_hash: 42,
            outbound: &[],
        })
        .await
        .unwrap();

    let m = store.get(&url).await.unwrap().expect("must see prior run");
    assert_eq!(m.status, UrlStatus::Succeeded);
    assert_eq!(m.last_run_id, "run-1");
    assert_eq!(m.blob_path.as_deref(), Some("/data/run-1.parquet"));
}

#[tokio::test]
async fn history_records_each_transition() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("history");

    store
        .mark_attempting(&url, &RunId::new("run-1"), 0)
        .await
        .unwrap();
    store.mark_failed(&url, FailureKind::Timeout).await.unwrap();
    store.mark_failed(&url, FailureKind::Timeout).await.unwrap();
    let attempt = AttemptId::new("attempt-1");
    store
        .mark_succeeded(&SuccessRecord {
            url: &url,
            attempt_id: &attempt,
            blob_path: "/data/0.warc",
            content_hash: 7,
            outbound: &[],
        })
        .await
        .unwrap();

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM url_history h
            JOIN url_metadata m ON m.id = h.url_id
            WHERE m.url = $1",
    )
    .bind(url.as_str())
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    // attempted + failed + failed + succeeded = 4 events.
    assert_eq!(count, 4);
}

/// One `publish` call that records the row ids it received into a
/// shared collector. Pulled out as a helper because the boxed-closure
/// shape needs each call to own its own clone of the collector.
async fn publish_recording(
    outbox: Arc<dyn Outbox>,
    collected: Arc<Mutex<Vec<u64>>>,
    per_call: usize,
) -> usize {
    outbox
        .publish(
            per_call,
            Box::new(move |entries| {
                Box::pin(async move {
                    let mut g = collected.lock().unwrap();
                    for e in &entries {
                        g.push(e.id.value());
                    }
                    Ok(())
                })
            }),
        )
        .await
        .unwrap()
}

/// Seed `n` outbox rows attached to a single parent URL. Used by the
/// publish tests to fill the table without driving the full
/// `mark_succeeded` path (which would re-test the metadata write,
/// not the outbox lease).
async fn seed_outbox_rows(pool: &PgPool, store: &PostgresMetadataStore, n: usize) -> i64 {
    let parent = unique_url("publish-parent");
    store
        .mark_attempting(&parent, &RunId::new(run_id()), 0)
        .await
        .unwrap();
    let parent_url_id: i64 = sqlx::query_scalar("SELECT id FROM url_metadata WHERE url = $1")
        .bind(parent.as_str())
        .fetch_one(pool)
        .await
        .unwrap();
    for i in 0..n {
        sqlx::query(
            "INSERT INTO frontier_outbox
                (url, depth, discovered_from, parent_url_id, parent_attempt_id)
             VALUES ($1, 1, NULL, $2, 'attempt-seed')",
        )
        .bind(format!("https://child-{}-{i}.test/", cuid2::create_id()))
        .bind(parent_url_id)
        .execute(pool)
        .await
        .unwrap();
    }
    parent_url_id
}

#[tokio::test]
async fn publish_distributes_disjoint_batches_across_concurrent_callers() {
    // Pin the FOR UPDATE SKIP LOCKED contract: N concurrent publish
    // callers each receive a disjoint batch, never the same row twice.
    let fx = fixture().await;
    let store = Arc::new(PostgresMetadataStore::with_pool(fx.pool.clone()));

    let total: usize = 1024;
    let parent_url_id = seed_outbox_rows(&fx.pool, &store, total).await;

    // 4 concurrent publishers, each capped at 256 (sum = 1024).
    // Synchronise their starts via a barrier so SKIP LOCKED is the
    // primitive that splits the work, not arrival timing.
    let publishers = 4;
    let per_call = 256;
    let collected: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(publishers));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..publishers {
        let outbox: Arc<dyn Outbox> = store.clone();
        let collected = collected.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let mut local_total = 0usize;
            // Loop until publish reports nothing left for me to take.
            // A 0 may transiently mean "peers hold the rest under
            // lease right now"; one yield + retry clears that false
            // empty.
            loop {
                let n = publish_recording(outbox.clone(), collected.clone(), per_call).await;
                if n == 0 {
                    tokio::task::yield_now().await;
                    let n2 = publish_recording(outbox.clone(), collected.clone(), per_call).await;
                    if n2 == 0 {
                        break;
                    }
                    local_total += n2;
                } else {
                    local_total += n;
                }
            }
            local_total
        });
    }

    let mut grand_total = 0usize;
    while let Some(r) = tasks.join_next().await {
        grand_total += r.unwrap();
    }
    assert_eq!(grand_total, total, "every row published exactly once");

    let mut all = collected.lock().unwrap().clone();
    all.sort();
    let mut deduped = all.clone();
    deduped.dedup();
    assert_eq!(
        all.len(),
        deduped.len(),
        "no row delivered to more than one publisher"
    );
    assert_eq!(all.len(), total, "every seeded row was delivered");

    let still_unpublished: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM frontier_outbox
         WHERE parent_url_id = $1 AND published_at IS NULL",
    )
    .bind(parent_url_id)
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    assert_eq!(still_unpublished, 0);
}

#[tokio::test]
async fn publish_rolls_back_when_ship_returns_error() {
    // Pin the lease-on-failure contract: if the ship closure errors,
    // the txn rolls back, the lease releases, and the rows reappear
    // for the next caller.
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());

    let total: usize = 64;
    let parent_url_id = seed_outbox_rows(&fx.pool, &store, total).await;

    let result = store
        .publish(
            total,
            Box::new(|_entries| {
                Box::pin(async { Err(Error::Metadata("simulated ship failure".into())) })
            }),
        )
        .await;
    assert!(result.is_err(), "ship error must propagate out of publish");

    // Every row stayed unpublished: the rollback released the lease
    // without marking anything.
    let still_unpublished: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM frontier_outbox
         WHERE parent_url_id = $1 AND published_at IS NULL",
    )
    .bind(parent_url_id)
    .fetch_one(&fx.pool)
    .await
    .unwrap();
    assert_eq!(still_unpublished, total as i64);

    // A subsequent successful publish picks up the same rows.
    let recorded: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded_clone = recorded.clone();
    let n = store
        .publish(
            total,
            Box::new(move |entries| {
                Box::pin(async move {
                    let mut g = recorded_clone.lock().unwrap();
                    for e in entries {
                        g.push(e.id.value());
                    }
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();
    assert_eq!(n, total);
    assert_eq!(recorded.lock().unwrap().len(), total);
}
