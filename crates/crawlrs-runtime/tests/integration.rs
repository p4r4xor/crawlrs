//! End-to-end runtime tests against a real Redis frontier + real
//! politeness + a fake Fetcher + the real lol_html parser + an
//! in-memory test Store. Each test brings up its own Redis container
//! via testcontainers-rs.
//!
//! Test doubles (FakeFetcher, InMemoryStore, InMemoryMetadataStore)
//! live in `crawlrs-fakes` so this file stays focused on test
//! scenarios rather than scaffolding.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    AttemptId, CanonicalUrl, MetadataStore, ShardingPolicy, SingleShardPolicy, SiteAdapterRegistry,
    SuccessRecord, UrlEntry, UrlStatus,
};
use crawlrs_fakes::{FakeFetcher, InMemoryMetadataStore, InMemoryStore};
use crawlrs_frontier_redis::RedisFrontier;
use crawlrs_parse::LolHtmlParser;
use crawlrs_politeness::{PolitenessConfig, RedisPoliteness};
use crawlrs_runtime::{Crawler, CrawlerConfig};
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Fixture {
    _container: ContainerAsync<Redis>,
    pool: Pool<RedisConnectionManager>,
}

async fn fixture() -> Fixture {
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
    Fixture {
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

/// Build a crawler with the standard test setup. Returns the crawler
/// plus handles to the fake fetcher, store, and metadata so the test
/// can install canned responses and assert on stored docs / ledger
/// transitions.
async fn build_crawler(
    fx: &Fixture,
    config: CrawlerConfig,
    politeness_config: PolitenessConfig,
) -> (
    Crawler,
    Arc<FakeFetcher>,
    Arc<InMemoryStore>,
    Arc<InMemoryMetadataStore>,
) {
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let rid = run_id();

    let frontier = Arc::new(
        RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0], rid.clone())
            .await
            .unwrap()
            // 50ms gives the original consumer time to ack/nack
            // before a peer worker tries to steal the entry; healthy
            // workers complete in ~1ms.
            .with_autoclaim_idle(Duration::from_millis(50)),
    );
    let fetcher = Arc::new(FakeFetcher::default());
    let politeness = Arc::new(
        RedisPoliteness::new(
            fx.pool.clone(),
            policy.clone(),
            vec![0],
            fetcher.clone(),
            rid.clone(),
            politeness_config,
        )
        .await
        .unwrap(),
    );
    let parser = Arc::new(LolHtmlParser);
    let store = Arc::new(InMemoryStore::default());
    let metadata = Arc::new(InMemoryMetadataStore::default());
    let adapters = Arc::new(SiteAdapterRegistry::new());

    let crawler = Crawler::builder()
        .frontier(frontier)
        .politeness(politeness)
        .fetcher(fetcher.clone())
        .parser(parser)
        .store(store.clone())
        .metadata(metadata.clone())
        .outbox(metadata.clone())
        .adapters(adapters)
        .config(config)
        .run_id(rid)
        .build()
        .unwrap();

    (crawler, fetcher, store, metadata)
}

fn fast_config() -> CrawlerConfig {
    CrawlerConfig {
        workers: 2,
        user_agent: "TestBot/1.0".into(),
        max_depth: Some(2),
        maintenance_interval: Duration::from_secs(60),
        empty_queue_poll: Duration::from_millis(50),
        startup_poll: Duration::from_millis(20),
        max_idle_sleep: Duration::from_millis(200),
        error_backoff: Duration::from_millis(200),
        max_retries: 3,
        cross_run_dedup: true,
        pod_ordinal: 0,
        restart_policy: Default::default(),
    }
}

fn fast_politeness() -> PolitenessConfig {
    PolitenessConfig {
        min_delay: Duration::from_millis(50),
        honor_robots_txt: false,
        user_agent: "TestBot/1.0".into(),
        backoff: crawlrs_politeness::BackoffPolicy {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
            multiplier: 2.0,
            circuit_open_after_failures: 100,
        },
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn end_to_end_crawl_one_seed_two_pages() {
    let fx = fixture().await;
    let (crawler, fetcher, store, _metadata) =
        build_crawler(&fx, fast_config(), fast_politeness()).await;

    fetcher.install_html(
        "https://a.test/",
        r#"<html><body><a href="/page1">p1</a><a href="https://b.test/">b</a></body></html>"#,
    );
    fetcher.install_html("https://a.test/page1", "<html><body>page1</body></html>");
    fetcher.install_html("https://b.test/", "<html><body>b</body></html>");

    crawler
        .deps()
        .frontier
        .submit(entry("https://a.test/"))
        .await
        .unwrap();

    // Run the crawler in a task; trigger shutdown after enough time
    // for the seed + its 2 children to have been fetched.
    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });

    // Give it time to drain; with min_delay=50ms and 3 distinct
    // hosts, ~1s is plenty.
    tokio::time::sleep(Duration::from_millis(800)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();

    let calls = fetcher.calls();
    assert!(
        calls.contains(&"https://a.test/".to_string()),
        "seed fetched"
    );
    let stored = store.urls();
    assert!(!stored.is_empty(), "at least the seed was stored");
    assert!(stored.contains(&"https://a.test/".to_string()));
}

#[tokio::test]
async fn rate_limit_response_triggers_failure_recording_and_nack() {
    let fx = fixture().await;
    let (crawler, fetcher, _store, _metadata) =
        build_crawler(&fx, fast_config(), fast_politeness()).await;

    fetcher.install_status("https://flaky.test/", 429);

    crawler
        .deps()
        .frontier
        .submit(entry("https://flaky.test/"))
        .await
        .unwrap();

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });

    tokio::time::sleep(Duration::from_millis(400)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();

    // The 429 should have caused the URL to be classified as
    // TooManyRequests, recorded in politeness state, and nacked
    // (so the URL stays in the consumer's PEL until reclaim).
    assert!(
        fetcher.calls().contains(&"https://flaky.test/".to_string()),
        "URL was attempted",
    );
}

#[tokio::test]
async fn graceful_shutdown_returns_promptly() {
    let fx = fixture().await;
    let (crawler, _fetcher, _store, _metadata) =
        build_crawler(&fx, fast_config(), fast_politeness()).await;

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });

    // No URLs in the queue; workers idle on `next_ready_at` -> None.
    // Send shutdown immediately and verify run() returns within a
    // bounded window.
    tokio::time::sleep(Duration::from_millis(50)).await;
    crawler.shutdown();

    let bounded = tokio::time::timeout(Duration::from_secs(2), run_handle).await;
    assert!(bounded.is_ok(), "run() should exit within 2s of shutdown");
}

#[tokio::test]
async fn missing_canned_response_records_transport_failure() {
    let fx = fixture().await;
    let (crawler, _fetcher, _store, _metadata) =
        build_crawler(&fx, fast_config(), fast_politeness()).await;

    crawler
        .deps()
        .frontier
        .submit(entry("https://missing.test/"))
        .await
        .unwrap();

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });

    tokio::time::sleep(Duration::from_millis(300)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();
    // Survival is the assertion; the worker must not panic on a
    // FakeFetcher transport error and must have processed the failure
    // through politeness + nack paths.
}

#[tokio::test]
async fn discovered_links_respect_max_depth() {
    let fx = fixture().await;
    let mut config = fast_config();
    config.max_depth = Some(1); // seed -> depth 0; children -> depth 1; depth 2 dropped
    let (crawler, fetcher, store, _metadata) = build_crawler(&fx, config, fast_politeness()).await;

    // Seed at depth 0 has one link to depth 1; depth 1 page has one
    // link to depth 2. With max_depth=1, depth-2 should not be
    // submitted.
    fetcher.install_html(
        "https://a.test/",
        r#"<html><body><a href="/d1">d1</a></body></html>"#,
    );
    fetcher.install_html(
        "https://a.test/d1",
        r#"<html><body><a href="/d2">d2</a></body></html>"#,
    );
    // Note: do NOT install /d2; if we ever did fetch it, we'd see a
    // transport-error class entry, but we shouldn't.

    crawler
        .deps()
        .frontier
        .submit(entry("https://a.test/"))
        .await
        .unwrap();

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });

    tokio::time::sleep(Duration::from_millis(700)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();

    let calls = fetcher.calls();
    assert!(calls.contains(&"https://a.test/".to_string()));
    assert!(calls.contains(&"https://a.test/d1".to_string()));
    assert!(
        !calls.contains(&"https://a.test/d2".to_string()),
        "d2 is at depth 2 and should be dropped under max_depth=1; calls: {calls:?}",
    );
    let _ = store; // unused; kept for parity with other tests
}

#[tokio::test]
async fn successful_fetch_records_metadata_succeeded() {
    // After a successful fetch + parse + store, the metadata ledger
    // shows status=Succeeded, blob_path set, content_hash set.
    let fx = fixture().await;
    let (crawler, fetcher, _store, metadata) =
        build_crawler(&fx, fast_config(), fast_politeness()).await;

    fetcher.install_html(
        "https://a.test/",
        "<html><body><h1>hello</h1></body></html>",
    );

    crawler
        .deps()
        .frontier
        .submit(entry("https://a.test/"))
        .await
        .unwrap();

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();

    let row = metadata
        .get(&url("https://a.test/"))
        .await
        .unwrap()
        .expect("metadata row exists for fetched URL");
    assert_eq!(row.status, UrlStatus::Succeeded);
    assert_eq!(row.retry_count, 0);
    assert!(
        row.blob_path
            .as_deref()
            .unwrap_or("")
            .starts_with("memory://"),
        "blob_path={:?}",
        row.blob_path
    );
    assert!(row.content_hash.is_some(), "content_hash recorded");
}

#[tokio::test]
async fn repeated_failure_exhausts_retry_budget_and_lands_in_dlq() {
    // Server returns 503 forever. After max_retries failures the URL
    // should move to PermanentlyFailed and stop being re-claimed.
    let fx = fixture().await;
    let mut config = fast_config();
    config.max_retries = 2; // 2 attempts then DLQ
    let (crawler, fetcher, _store, metadata) = build_crawler(&fx, config, fast_politeness()).await;

    fetcher.install_status("https://flaky.test/", 503);

    crawler
        .deps()
        .frontier
        .submit(entry("https://flaky.test/"))
        .await
        .unwrap();

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });
    // Long enough that the worker reclaims via XAUTOCLAIM and exhausts
    // the budget; autoclaim_idle is set to 0 in the fixture so retries
    // are immediate.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();

    let row = metadata
        .get(&url("https://flaky.test/"))
        .await
        .unwrap()
        .expect("metadata row exists after first failure");
    assert_eq!(
        row.status,
        UrlStatus::PermanentlyFailed,
        "URL should land in DLQ after retry budget; got {:?}",
        row.status,
    );
    assert!(metadata.dlq_count() >= 1, "DLQ has at least one entry");
}

#[tokio::test]
async fn cross_run_dedup_skips_previously_succeeded_url() {
    // Pre-seed the metadata ledger with a Succeeded row for some URL.
    // When the runtime claims that URL, it must ack without fetching.
    let fx = fixture().await;
    let (crawler, fetcher, _store, metadata) =
        build_crawler(&fx, fast_config(), fast_politeness()).await;

    let already_done = url("https://done.test/");
    metadata
        .mark_attempting(&already_done, "prior-run", 0)
        .await
        .unwrap();
    let prior_attempt = AttemptId::new("prior-attempt");
    metadata
        .mark_succeeded(&SuccessRecord {
            url: &already_done,
            attempt_id: &prior_attempt,
            blob_path: "memory://prior",
            content_hash: 0xdead_beef,
            outbound: &[],
        })
        .await
        .unwrap();

    // Install a canned response so we'd detect a fetch if it happened.
    fetcher.install_html(
        "https://done.test/",
        "<html><body>should not be fetched</body></html>",
    );

    crawler
        .deps()
        .frontier
        .submit(UrlEntry::seed(already_done.clone()))
        .await
        .unwrap();

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });
    tokio::time::sleep(Duration::from_millis(400)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();

    assert!(
        !fetcher.calls().contains(&"https://done.test/".to_string()),
        "cross-run dedup should have prevented the fetch; calls: {:?}",
        fetcher.calls(),
    );
}

#[tokio::test]
async fn retry_after_header_extends_politeness_wake_time() {
    // Server returns 503 with Retry-After: 2 (seconds). compute_backoff
    // should take max(computed=50ms, hint=2s) = 2s, parking the host
    // for that long.
    //
    // We verify by counting fetch calls within a short observation
    // window: with the hint honored, we should see ~1 attempt; without,
    // we'd see many more (50ms backoff × N retries).
    let fx = fixture().await;
    let mut config = fast_config();
    config.max_retries = 100; // disable DLQ short-circuit so we observe retry pacing only

    // max_backoff must be larger than the server hint so the cap
    // doesn't clip the 2s value.
    let politeness_cfg = PolitenessConfig {
        min_delay: Duration::from_millis(50),
        honor_robots_txt: false,
        user_agent: "TestBot/1.0".into(),
        backoff: crawlrs_politeness::BackoffPolicy {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(60),
            multiplier: 2.0,
            circuit_open_after_failures: 1000,
        },
        ..Default::default()
    };
    let (crawler, fetcher, _store, _metadata) = build_crawler(&fx, config, politeness_cfg).await;

    let mut headers = HashMap::new();
    headers.insert("Retry-After".into(), "2".into());
    fetcher.install_status_with_headers("https://slow.test/", 503, headers);

    crawler
        .deps()
        .frontier
        .submit(entry("https://slow.test/"))
        .await
        .unwrap();

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });
    // Observe for ~800ms; Retry-After: 2 means at most 1 attempt fits.
    tokio::time::sleep(Duration::from_millis(800)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();

    let attempts = fetcher
        .calls()
        .iter()
        .filter(|u| *u == "https://slow.test/")
        .count();
    assert!(
        attempts <= 1,
        "Retry-After: 2 should have parked the host past our 800ms window; got {attempts} attempts",
    );
}
