//! Integration tests for `RedisPoliteness`.
//!
//! Each test brings up its own Redis container via `testcontainers-rs`
//! and uses a unique `run_id` so tests don't collide. A small in-test
//! `FakeFetcher` impl is used wherever robots.txt fetching is exercised;
//! its canned responses let us verify the politeness-side behaviour
//! without a real HTTP server.

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
    CanonicalUrl, Error, FailureKind, FetchRequest, FetchResponse, Fetcher, PoliteDecision,
    Politeness, Result, ShardingPolicy, SingleShardPolicy,
};
use crawlrs_politeness::{BackoffPolicy, PolitenessConfig, PolitenessOverride, RedisPoliteness};
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

// ---------------------------------------------------------------------------
// Fake Fetcher
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeFetcher {
    responses: Mutex<HashMap<String, FetchResponse>>,
    requests: Mutex<Vec<String>>,
}

impl FakeFetcher {
    fn install(&self, url: &str, status: u16, body: &str) {
        let canon = CanonicalUrl::parse(url).unwrap();
        let resp = FetchResponse {
            url: canon,
            status,
            headers: HashMap::new(),
            body: Bytes::copy_from_slice(body.as_bytes()),
            redirect_chain: Vec::new(),
            fetched_at: Utc::now(),
            duration: Duration::from_millis(0),
        };
        self.responses.lock().unwrap().insert(url.to_string(), resp);
    }

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

#[async_trait]
impl Fetcher for FakeFetcher {
    async fn fetch(&self, req: FetchRequest) -> Result<FetchResponse> {
        let url = req.url.as_str().to_string();
        self.requests.lock().unwrap().push(url.clone());
        match self.responses.lock().unwrap().get(&url).cloned() {
            Some(resp) => Ok(resp),
            None => Err(Error::Fetch(format!(
                "FakeFetcher: no canned response for {url}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

struct RedisFixture {
    _container: ContainerAsync<Redis>,
    pool: Pool<RedisConnectionManager>,
}

async fn fixture() -> RedisFixture {
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

fn config_with(host_delay: Duration, robots: bool) -> PolitenessConfig {
    PolitenessConfig {
        host_delay,
        obey_robots_txt: robots,
        user_agent: "TestBot/1.0".into(),
        ..Default::default()
    }
}

async fn build(
    pool: &Pool<RedisConnectionManager>,
    fetcher: Arc<dyn Fetcher>,
    config: PolitenessConfig,
) -> RedisPoliteness {
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    RedisPoliteness::new(pool.clone(), policy, vec![0], fetcher, run_id(), config)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unseen_host_is_allowed() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(1_000), false),
    )
    .await;
    assert_eq!(
        p.check(&url("https://a.test/")).await.unwrap(),
        PoliteDecision::Allow
    );
}

#[tokio::test]
async fn record_fetch_sets_delay_for_same_host() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(5_000), false),
    )
    .await;

    p.record_fetch(&url("https://a.test/")).await.unwrap();
    let decision = p.check(&url("https://a.test/page2")).await.unwrap();
    match decision {
        PoliteDecision::Delay(d) => {
            let ms = d.as_millis() as u64;
            assert!(
                ms > 0 && ms <= 5_000,
                "expected delay in [0, 5000]; got {ms}"
            );
        }
        other => panic!("expected Delay; got {:?}", other),
    }
}

#[tokio::test]
async fn delay_elapses_and_host_is_allowed_again() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(100), false),
    )
    .await;

    p.record_fetch(&url("https://a.test/")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        p.check(&url("https://a.test/")).await.unwrap(),
        PoliteDecision::Allow
    );
}

#[tokio::test]
async fn manual_excludes_returns_disallow() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(1_000), false);
    config.manual_excludes.insert("excluded.test".into());

    let p = build(&fx.pool, fake.clone(), config).await;
    assert_eq!(
        p.check(&url("https://excluded.test/page")).await.unwrap(),
        PoliteDecision::Disallow,
    );
}

#[tokio::test]
async fn per_domain_override_uses_custom_delay() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    config.per_domain.insert(
        "slow.test".into(),
        PolitenessOverride {
            host_delay: Some(Duration::from_millis(5_000)),
            obey_robots_txt: None,
        },
    );

    let p = build(&fx.pool, fake.clone(), config).await;
    p.record_fetch(&url("https://slow.test/")).await.unwrap();

    let decision = p.check(&url("https://slow.test/x")).await.unwrap();
    match decision {
        PoliteDecision::Delay(d) => {
            let ms = d.as_millis() as u64;
            assert!(ms > 1_000, "override should produce a long delay; got {ms}");
        }
        other => panic!("expected long Delay; got {:?}", other),
    }
}

#[tokio::test]
async fn record_failure_applies_backoff() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    // Tighten backoff so the test is fast.
    config.backoff = BackoffPolicy {
        initial_backoff: Duration::from_millis(1_000),
        max_backoff: Duration::from_millis(60_000),
        multiplier: 2.0,
        circuit_open_after_failures: 100,
    };
    let p = build(&fx.pool, fake.clone(), config).await;

    p.record_failure(
        &url("https://flaky.test/"),
        FailureKind::TooManyRequests,
        None,
    )
    .await
    .unwrap();

    let decision = p.check(&url("https://flaky.test/page")).await.unwrap();
    match decision {
        PoliteDecision::Delay(d) => {
            let ms = d.as_millis() as u64;
            assert!(ms > 500, "first 429 should apply ~1s backoff; got {ms}");
        }
        other => panic!("expected Delay after failure; got {:?}", other),
    }
}

#[tokio::test]
async fn consecutive_failures_grow_backoff() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    config.backoff = BackoffPolicy {
        initial_backoff: Duration::from_millis(500),
        max_backoff: Duration::from_millis(60_000),
        multiplier: 2.0,
        circuit_open_after_failures: 100,
    };
    let p = build(&fx.pool, fake.clone(), config).await;

    let u = url("https://flaky.test/");
    p.record_failure(&u, FailureKind::TooManyRequests, None)
        .await
        .unwrap();
    let first = match p.check(&u).await.unwrap() {
        PoliteDecision::Delay(d) => d.as_millis() as u64,
        other => panic!("expected delay after 1 failure; got {:?}", other),
    };

    p.record_failure(&u, FailureKind::TooManyRequests, None)
        .await
        .unwrap();
    let second = match p.check(&u).await.unwrap() {
        PoliteDecision::Delay(d) => d.as_millis() as u64,
        other => panic!("expected delay after 2 failures; got {:?}", other),
    };

    assert!(
        second > first,
        "backoff should grow; first={first} second={second}"
    );
}

#[tokio::test]
async fn record_fetch_resets_failure_state() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    config.backoff.initial_backoff = Duration::from_millis(1_000);
    let p = build(&fx.pool, fake.clone(), config).await;

    let u = url("https://flaky.test/");
    p.record_failure(&u, FailureKind::TooManyRequests, None)
        .await
        .unwrap();
    p.record_fetch(&u).await.unwrap();

    // After record_fetch resets state, the only delay should come from the
    // 100 ms host_delay, not the 1s backoff.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(p.check(&u).await.unwrap(), PoliteDecision::Allow);
}

#[tokio::test]
async fn circuit_opens_after_threshold_consecutive_failures() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    config.backoff = BackoffPolicy {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(100),
        multiplier: 1.0,
        circuit_open_after_failures: 3,
    };
    let p = build(&fx.pool, fake.clone(), config).await;

    let u = url("https://broken.test/");
    for _ in 0..3 {
        p.record_failure(&u, FailureKind::TooManyRequests, None)
            .await
            .unwrap();
    }
    assert_eq!(p.check(&u).await.unwrap(), PoliteDecision::Disallow);
}

#[tokio::test]
async fn next_ready_at_finds_soonest_host() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(60_000), false),
    )
    .await;

    p.record_fetch(&url("https://a.test/")).await.unwrap();
    let ready = p.next_ready_at().await.unwrap();
    assert!(
        ready.is_some(),
        "next_ready_at should find the host we just recorded"
    );

    let when = ready.unwrap();
    let now = std::time::Instant::now();
    let delta = when.saturating_duration_since(now);
    assert!(
        delta <= Duration::from_secs(60),
        "next_ready_at should be within the configured 60s window; got {:?}",
        delta
    );
}

#[tokio::test]
async fn next_ready_at_is_none_when_no_hosts_tracked() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(1_000), false),
    )
    .await;

    assert!(p.next_ready_at().await.unwrap().is_none());
}

#[tokio::test]
async fn robots_txt_blocks_disallowed_path_and_caches_body() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    fake.install(
        "https://blocky.test/robots.txt",
        200,
        "User-agent: *\nDisallow: /private",
    );

    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(100), true),
    )
    .await;

    // Public path: allowed.
    let pub_decision = p.check(&url("https://blocky.test/public")).await.unwrap();
    assert_ne!(pub_decision, PoliteDecision::Disallow);

    // Private path: blocked by robots.
    assert_eq!(
        p.check(&url("https://blocky.test/private/secret"))
            .await
            .unwrap(),
        PoliteDecision::Disallow,
    );

    // Cache check: a third request to the same host shouldn't re-fetch
    // robots.txt.
    let count_before = fake.request_count();
    p.check(&url("https://blocky.test/another")).await.unwrap();
    assert_eq!(
        fake.request_count(),
        count_before,
        "robots.txt should not be re-fetched"
    );
}

#[tokio::test]
async fn robots_txt_404_treated_as_no_rules() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    fake.install("https://norobots.test/robots.txt", 404, "");

    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(100), true),
    )
    .await;
    let decision = p
        .check(&url("https://norobots.test/anything"))
        .await
        .unwrap();
    assert_ne!(decision, PoliteDecision::Disallow);
}

#[tokio::test]
async fn robots_per_domain_override_disables_check() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    // robots.txt that would block everything; without the override
    // we'd get Disallow.
    fake.install(
        "https://staging.test/robots.txt",
        200,
        "User-agent: *\nDisallow: /",
    );

    let mut config = config_with(Duration::from_millis(100), true);
    config.per_domain.insert(
        "staging.test".into(),
        PolitenessOverride {
            host_delay: None,
            obey_robots_txt: Some(false),
        },
    );

    let p = build(&fx.pool, fake.clone(), config).await;
    // Override flips obey_robots_txt off for this host; check passes.
    let decision = p
        .check(&url("https://staging.test/anything"))
        .await
        .unwrap();
    assert_ne!(decision, PoliteDecision::Disallow);
    // Robots.txt is never fetched because the gate is disabled.
    assert_eq!(fake.request_count(), 0);
}

#[tokio::test]
async fn host_count_reflects_record_fetch() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(1_000), false),
    )
    .await;

    assert_eq!(p.host_count().await.unwrap(), 0);
    p.record_fetch(&url("https://a.test/")).await.unwrap();
    p.record_fetch(&url("https://b.test/")).await.unwrap();
    p.record_fetch(&url("https://a.test/page2")).await.unwrap(); // same host
    assert_eq!(p.host_count().await.unwrap(), 2, "two distinct hosts");
}

#[tokio::test]
async fn in_process_robots_lru_populates_after_first_check() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    fake.install(
        "https://lru.test/robots.txt",
        200,
        "User-agent: *\nAllow: /",
    );

    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(100), true),
    )
    .await;

    assert_eq!(p.robots().cache_len(), 0, "LRU empty at startup");

    let _ = p.check(&url("https://lru.test/some/page")).await.unwrap();

    // moka::sync::Cache::entry_count is eventually consistent; it can
    // lag inserts by a tick. Run the cache's pending tasks so the
    // assertion is deterministic.
    p.robots().run_pending_tasks();

    assert_eq!(
        p.robots().cache_len(),
        1,
        "LRU should hold the one host we just checked",
    );

    // Second check on a different path of the same host: still one
    // entry, no eviction, no extra fetch (verified by the existing
    // robots_txt_blocks_disallowed_path_and_caches_body test).
    let _ = p.check(&url("https://lru.test/another")).await.unwrap();
    p.robots().run_pending_tasks();
    assert_eq!(p.robots().cache_len(), 1);
}
