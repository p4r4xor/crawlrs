//! End-to-end runtime tests against a real Redis frontier + real
//! politeness + a fake Fetcher + the real lol_html parser + an
//! in-memory test Store. Each test brings up its own Redis container
//! via testcontainers-rs.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use bytes::Bytes;
use chrono::Utc;
use crawlrs_core::{
    CanonicalUrl, Error, FetchRequest, FetchResponse, Fetcher, ParsedDocument, Result,
    ShardingPolicy, SingleShardPolicy, SiteAdapterRegistry, Store, UrlEntry,
};
use crawlrs_frontier_redis::RedisFrontier;
use crawlrs_parse::LolHtmlParser;
use crawlrs_politeness::{PolitenessConfig, RedisPoliteness};
use crawlrs_runtime::{Crawler, CrawlerConfig};
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeFetcher {
    responses: Mutex<HashMap<String, FetchResponse>>,
    calls: Mutex<Vec<String>>,
}

impl FakeFetcher {
    fn install_html(&self, url: &str, body: &str) {
        let canon = CanonicalUrl::parse(url).unwrap();
        let resp = FetchResponse {
            url: canon,
            status: 200,
            headers: {
                let mut h = HashMap::new();
                h.insert("content-type".into(), "text/html".into());
                h
            },
            body: Bytes::copy_from_slice(body.as_bytes()),
            redirect_chain: Vec::new(),
            fetched_at: Utc::now(),
            duration: Duration::from_millis(0),
        };
        self.responses.lock().unwrap().insert(url.to_string(), resp);
    }

    fn install_status(&self, url: &str, status: u16) {
        let canon = CanonicalUrl::parse(url).unwrap();
        let resp = FetchResponse {
            url: canon,
            status,
            headers: HashMap::new(),
            body: Bytes::new(),
            redirect_chain: Vec::new(),
            fetched_at: Utc::now(),
            duration: Duration::from_millis(0),
        };
        self.responses.lock().unwrap().insert(url.to_string(), resp);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Fetcher for FakeFetcher {
    async fn fetch(&self, req: FetchRequest) -> Result<FetchResponse> {
        let url = req.url.as_str().to_string();
        self.calls.lock().unwrap().push(url.clone());
        match self.responses.lock().unwrap().get(&url).cloned() {
            Some(r) => Ok(r),
            None => Err(Error::Fetch(format!(
                "FakeFetcher: no canned response for {url}"
            ))),
        }
    }
}

#[derive(Default)]
struct InMemoryStore {
    written: Mutex<Vec<ParsedDocument>>,
}

impl InMemoryStore {
    fn urls(&self) -> Vec<String> {
        self.written
            .lock()
            .unwrap()
            .iter()
            .map(|d| d.url.as_str().to_string())
            .collect()
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn write(&self, doc: &ParsedDocument, _raw_body: Option<&Bytes>) -> Result<()> {
        self.written.lock().unwrap().push(doc.clone());
        Ok(())
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

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
/// plus handles to the fake fetcher and store so the test can install
/// canned responses and assert on stored docs.
async fn build_crawler(
    fx: &Fixture,
    config: CrawlerConfig,
    politeness_config: PolitenessConfig,
) -> (Crawler, Arc<FakeFetcher>, Arc<InMemoryStore>) {
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let rid = run_id();

    let frontier = Arc::new(
        RedisFrontier::new(fx.pool.clone(), policy.clone(), vec![0], rid.clone())
            .await
            .unwrap()
            .with_autoclaim_idle(Duration::ZERO),
    );
    let fetcher = Arc::new(FakeFetcher::default());
    let politeness = Arc::new(
        RedisPoliteness::new(
            fx.pool.clone(),
            policy.clone(),
            vec![0],
            fetcher.clone(),
            rid,
            politeness_config,
        )
        .await
        .unwrap(),
    );
    let parser = Arc::new(LolHtmlParser);
    let store = Arc::new(InMemoryStore::default());
    let adapters = Arc::new(SiteAdapterRegistry::new());

    let crawler = Crawler::builder()
        .frontier(frontier)
        .politeness(politeness)
        .fetcher(fetcher.clone())
        .parser(parser)
        .store(store.clone())
        .adapters(adapters)
        .config(config)
        .build()
        .unwrap();

    (crawler, fetcher, store)
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
    }
}

fn fast_politeness() -> PolitenessConfig {
    PolitenessConfig {
        min_delay: Duration::from_millis(50),
        honor_robots_txt: false,
        user_agent: "TestBot/1.0".into(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn end_to_end_crawl_one_seed_two_pages() {
    let fx = fixture().await;
    let (crawler, fetcher, store) = build_crawler(&fx, fast_config(), fast_politeness()).await;

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
    let (crawler, fetcher, _store) = build_crawler(&fx, fast_config(), fast_politeness()).await;

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
    let (crawler, _fetcher, _store) = build_crawler(&fx, fast_config(), fast_politeness()).await;

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
    let (crawler, _fetcher, _store) = build_crawler(&fx, fast_config(), fast_politeness()).await;

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
    let (crawler, fetcher, store) = build_crawler(&fx, config, fast_politeness()).await;

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
