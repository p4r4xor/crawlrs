//! Integration tests for `PostgresMetadataStore` against a real
//! Postgres instance via testcontainers-rs. Each test brings up its
//! own container and runs migrations on startup; tests use unique
//! URLs so they could share a container in the future without
//! cross-test contamination.

use crawlrs_core::{AttemptId, CanonicalUrl, FailureKind, MetadataStore, UrlStatus};
use crawlrs_metadata::PostgresMetadataStore;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

    store.mark_attempting(&url, &rid, 0).await.unwrap();

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

    store.mark_attempting(&url, "run-1", 0).await.unwrap();
    let first = store.get(&url).await.unwrap().unwrap();
    let original_discovered_at = first.discovered_at;

    // Sleep enough for NOW() to advance past the original timestamp's
    // microsecond resolution.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    store.mark_attempting(&url, "run-2", 1).await.unwrap();
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
    store.mark_attempting(&url, "run-1", 0).await.unwrap();

    store
        .mark_succeeded(
            &url,
            &AttemptId::new("attempt-1"),
            "s3://bucket/run-1/0.parquet",
            0xabcd_1234_5678_9abc,
            &[],
        )
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
    store.mark_attempting(&url, "run-1", 0).await.unwrap();

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
    store.mark_attempting(&url, "run-1", 0).await.unwrap();
    store.mark_failed(&url, FailureKind::Timeout).await.unwrap();
    store.mark_failed(&url, FailureKind::Timeout).await.unwrap();

    store
        .mark_succeeded(&url, &AttemptId::new("attempt-1"), "/data/0.warc", 1, &[])
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
    store.mark_attempting(&url, "run-1", 0).await.unwrap();
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
async fn cross_run_dedup_lookup_works() {
    let fx = fixture().await;
    let store = PostgresMetadataStore::with_pool(fx.pool.clone());
    let url = unique_url("cross-run");

    // Run 1 finishes.
    store.mark_attempting(&url, "run-1", 0).await.unwrap();
    store
        .mark_succeeded(
            &url,
            &AttemptId::new("attempt-1"),
            "/data/run-1.parquet",
            42,
            &[],
        )
        .await
        .unwrap();

    // Run 2 starts later. Asks: "have we crawled this?"
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

    store.mark_attempting(&url, "run-1", 0).await.unwrap();
    store.mark_failed(&url, FailureKind::Timeout).await.unwrap();
    store.mark_failed(&url, FailureKind::Timeout).await.unwrap();
    store
        .mark_succeeded(&url, &AttemptId::new("attempt-1"), "/data/0.warc", 7, &[])
        .await
        .unwrap();

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM url_history h
            JOIN url_metadata m ON m.id = h.url_id
            WHERE m.url = $1",
    )
    .bind(url.as_str())
    .fetch_one(store.pool())
    .await
    .unwrap();
    // attempted + failed + failed + succeeded = 4 events.
    assert_eq!(count, 4);
}
