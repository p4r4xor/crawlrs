//! Integration tests for `RedisPoliteness`.
//!
//! Each test brings up its own Redis container via `testcontainers-rs`
//! and uses a unique `run_id` so tests don't collide. A small in-test
//! `FakeFetcher` impl is used wherever robots.txt fetching is
//! exercised; its canned responses let us verify the politeness-side
//! behaviour without a real HTTP server.
//!
//! The politeness layer is policy-only (per ADR-0020). It owns
//! `check` (Allow / Disallow), `record_fetch` and `record_failure`
//! (return `NextWake` plans). Wake-time storage and the
//! ready-host-list / lease ZSET live in the frontier crate; tests
//! for that behaviour live there.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

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

/// Helper: extract `until` as a Duration-from-now from a `NextWake`.
fn delay_from_now(until: Instant) -> Duration {
    until.saturating_duration_since(Instant::now())
}

// ---------------------------------------------------------------------------
// check(): allow / disallow
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
async fn blocklist_returns_disallow() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(1_000), false);
    config.blocklist.insert("excluded.test".into());

    let p = build(&fx.pool, fake.clone(), config).await;
    assert_eq!(
        p.check(&url("https://excluded.test/page")).await.unwrap(),
        PoliteDecision::Disallow,
    );
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
        failure_threshold: 3,
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

// ---------------------------------------------------------------------------
// record_fetch(): NextWake math
// ---------------------------------------------------------------------------

#[tokio::test]
async fn record_fetch_returns_next_wake_at_now_plus_host_delay() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let p = build(
        &fx.pool,
        fake.clone(),
        config_with(Duration::from_millis(5_000), false),
    )
    .await;

    let plan = p.record_fetch(&url("https://a.test/")).await.unwrap();
    assert_eq!(plan.host, "a.test");
    let delay = delay_from_now(plan.until);
    assert!(
        delay >= Duration::from_millis(4_500) && delay <= Duration::from_millis(5_000),
        "expected NextWake ~5s out; got {delay:?}",
    );
}

#[tokio::test]
async fn per_domain_override_uses_custom_host_delay() {
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
    let plan = p.record_fetch(&url("https://slow.test/")).await.unwrap();
    let delay = delay_from_now(plan.until);
    assert!(
        delay >= Duration::from_millis(4_500),
        "override should produce ~5s delay, not the default 100ms; got {delay:?}",
    );
}

#[tokio::test]
async fn record_fetch_resets_circuit_breaker_state() {
    // After a successful fetch, the failure counter is cleared and
    // subsequent `check` calls return Allow even if there were
    // recent failures.
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    config.backoff = BackoffPolicy {
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(1_000),
        multiplier: 2.0,
        failure_threshold: 3,
    };
    let p = build(&fx.pool, fake.clone(), config).await;

    let u = url("https://flaky.test/");
    // Three failures: circuit opens.
    for _ in 0..3 {
        p.record_failure(&u, FailureKind::TooManyRequests, None)
            .await
            .unwrap();
    }
    assert_eq!(p.check(&u).await.unwrap(), PoliteDecision::Disallow);

    // One success: circuit closes.
    p.record_fetch(&u).await.unwrap();
    assert_eq!(p.check(&u).await.unwrap(), PoliteDecision::Allow);
}

// ---------------------------------------------------------------------------
// record_failure(): NextWake math
// ---------------------------------------------------------------------------

#[tokio::test]
async fn record_failure_returns_next_wake_with_backoff() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    config.backoff = BackoffPolicy {
        initial_backoff: Duration::from_millis(1_000),
        max_backoff: Duration::from_millis(60_000),
        multiplier: 2.0,
        failure_threshold: 100,
    };
    let p = build(&fx.pool, fake.clone(), config).await;

    let plan = p
        .record_failure(&url("https://flaky.test/"), FailureKind::TooManyRequests, None)
        .await
        .unwrap();
    let delay = delay_from_now(plan.until);
    assert!(
        delay >= Duration::from_millis(500) && delay <= Duration::from_millis(1_500),
        "first 429 should produce ~1s backoff; got {delay:?}",
    );
}

#[tokio::test]
async fn consecutive_failures_grow_next_wake() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    config.backoff = BackoffPolicy {
        initial_backoff: Duration::from_millis(500),
        max_backoff: Duration::from_millis(60_000),
        multiplier: 2.0,
        failure_threshold: 100,
    };
    let p = build(&fx.pool, fake.clone(), config).await;

    let u = url("https://flaky.test/");
    let first = p
        .record_failure(&u, FailureKind::TooManyRequests, None)
        .await
        .unwrap();
    let second = p
        .record_failure(&u, FailureKind::TooManyRequests, None)
        .await
        .unwrap();
    let first_ms = delay_from_now(first.until).as_millis() as u64;
    let second_ms = delay_from_now(second.until).as_millis() as u64;
    assert!(
        second_ms > first_ms,
        "backoff should grow across consecutive failures; first={first_ms}ms second={second_ms}ms",
    );
}

#[tokio::test]
async fn record_failure_honors_retry_after_as_floor() {
    let fx = fixture().await;
    let fake = Arc::new(FakeFetcher::default());
    let mut config = config_with(Duration::from_millis(100), false);
    // Computed backoff for the first failure would be small (50ms);
    // the server's 10-second Retry-After must dominate.
    config.backoff = BackoffPolicy {
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(120),
        multiplier: 2.0,
        failure_threshold: 100,
    };
    let p = build(&fx.pool, fake.clone(), config).await;

    let plan = p
        .record_failure(
            &url("https://server.test/"),
            FailureKind::ServiceUnavailable,
            Some(Duration::from_secs(10)),
        )
        .await
        .unwrap();
    let delay = delay_from_now(plan.until);
    assert!(
        delay >= Duration::from_secs(9) && delay <= Duration::from_secs(11),
        "server Retry-After 10s should be honored as the floor; got {delay:?}",
    );
}

// ---------------------------------------------------------------------------
// robots.txt
// ---------------------------------------------------------------------------

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
    let decision = p
        .check(&url("https://staging.test/anything"))
        .await
        .unwrap();
    assert_ne!(decision, PoliteDecision::Disallow);
    // Robots.txt is never fetched because the gate is disabled.
    assert_eq!(fake.request_count(), 0);
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
    p.robots().run_pending_tasks();

    assert_eq!(
        p.robots().cache_len(),
        1,
        "LRU should hold the one host we just checked",
    );

    let _ = p.check(&url("https://lru.test/another")).await.unwrap();
    p.robots().run_pending_tasks();
    assert_eq!(p.robots().cache_len(), 1);
}
