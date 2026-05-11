//! End-to-end smoke test that wires the runtime against the real
//! `PostgresMetadataStore` and verifies state transitions land on
//! disk. Exists separately from `tests/integration.rs` so the
//! existing fast test suite doesn't pay for a Postgres container on
//! every run; this file is the "are we wiring the production
//! impl correctly?" backstop.
//!
//! Test doubles come from `crawlrs-fakes`; this file owns the
//! Redis Stack + Postgres testcontainer fixture. Redis Stack is
//! required because the frontier uses RedisBloom.

use std::sync::Arc;
use std::time::Duration;

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    CanonicalUrl, MetadataStore, ShardingPolicy, SingleShardPolicy, SiteAdapterRegistry, UrlEntry,
    UrlStatus,
};
use crawlrs_fakes::{FakeFetcher, InMemoryStore};
use crawlrs_frontier_redis::{BloomConfig, RedisFrontier};
use crawlrs_metadata::PostgresMetadataStore;
use crawlrs_parse::LolHtmlParser;
use crawlrs_politeness::{BackoffPolicy, PolitenessConfig, RedisPoliteness};
use crawlrs_runtime::{Crawler, CrawlerConfig};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

#[allow(dead_code)] // containers must outlive the test; we hold them in fields
struct Fixture {
    redis_container: ContainerAsync<GenericImage>,
    postgres_container: ContainerAsync<Postgres>,
    redis_pool: Pool<RedisConnectionManager>,
    pg_pool: PgPool,
}

async fn fixture() -> Fixture {
    let redis_container = GenericImage::new("redis/redis-stack-server", "7.4.0-v0")
        .with_exposed_port(6379.into())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .expect("docker must be running for integration tests");
    let host = redis_container.get_host().await.unwrap();
    let port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let redis_pool = Pool::builder()
        .max_size(8)
        .build(RedisConnectionManager::new(format!("redis://{host}:{port}")).unwrap())
        .await
        .unwrap();

    let postgres_container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("docker must be running for integration tests");
    let pg_host = postgres_container.get_host().await.unwrap();
    let pg_port = postgres_container.get_host_port_ipv4(5432).await.unwrap();
    let pg_url = format!("postgres://postgres:postgres@{pg_host}:{pg_port}/postgres");
    let pg_pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&pg_url)
        .await
        .expect("connect to postgres");
    PostgresMetadataStore::migrate(&pg_pool)
        .await
        .expect("migrations apply");

    Fixture {
        redis_container,
        postgres_container,
        redis_pool,
        pg_pool,
    }
}

fn run_id() -> String {
    format!("pgwire-{}", cuid2::create_id())
}

fn url(s: &str) -> CanonicalUrl {
    CanonicalUrl::parse(s).unwrap()
}

#[tokio::test]
async fn end_to_end_against_postgres_metadata_store() {
    let fx = fixture().await;
    let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
    let rid = run_id();

    let frontier = Arc::new(
        RedisFrontier::new(
            fx.redis_pool.clone(),
            policy.clone(),
            vec![0],
            rid.clone(),
            BloomConfig::default(),
        )
        .await
        .unwrap()
        .with_lease_timeout(Duration::from_millis(200)),
    );
    let fetcher = Arc::new(FakeFetcher::default());
    fetcher.install_html(
        "https://pgtest.test/",
        "<html><body><h1>hi</h1></body></html>",
    );

    let politeness_cfg = PolitenessConfig {
        host_delay: Duration::from_millis(50),
        obey_robots_txt: false,
        user_agent: "PgWireTest/1.0".into(),
        backoff: BackoffPolicy {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
            multiplier: 2.0,
            failure_threshold: 100,
        },
        ..Default::default()
    };
    let politeness = Arc::new(
        RedisPoliteness::new(
            fx.redis_pool.clone(),
            policy.clone(),
            vec![0],
            fetcher.clone(),
            rid.clone(),
            politeness_cfg,
        )
        .await
        .unwrap(),
    );
    let parser = Arc::new(LolHtmlParser);
    let store = Arc::new(InMemoryStore::new());
    let pg_store = Arc::new(PostgresMetadataStore::with_pool(fx.pg_pool.clone()));
    let metadata: Arc<dyn MetadataStore> = pg_store.clone();
    let outbox: Arc<dyn crawlrs_core::Outbox> = pg_store;
    let adapters = Arc::new(SiteAdapterRegistry::new());

    let config = CrawlerConfig {
        workers: 2,
        user_agent: Some("PgWireTest/1.0".into()),
        max_depth: Some(1),
        maintenance_interval: Duration::from_secs(60),
        promoter_tick: Duration::from_millis(20),
        empty_queue_poll: Duration::from_millis(50),
        max_idle_sleep: Duration::from_millis(200),
        error_backoff: Duration::from_millis(200),
        max_retries: 3,
        pod_ordinal: 0,
        restart_policy: Default::default(),
        link_dispatch: Default::default(),
    };

    let crawler = Crawler::builder()
        .frontier(frontier)
        .politeness(politeness)
        .fetcher(fetcher.clone())
        .parser(parser)
        .store(store)
        .metadata(metadata.clone())
        .outbox(outbox)
        .adapters(adapters)
        .config(config)
        .run_id(rid.clone())
        .build()
        .unwrap();

    crawler
        .deps()
        .frontier
        .submit(UrlEntry::seed(url("https://pgtest.test/")))
        .await
        .unwrap();

    let crawler = Arc::new(crawler);
    let crawler_clone = crawler.clone();
    let run_handle = tokio::spawn(async move { crawler_clone.run().await });
    tokio::time::sleep(Duration::from_millis(800)).await;
    crawler.shutdown();
    run_handle.await.unwrap().unwrap();

    let row = metadata
        .get(&url("https://pgtest.test/"))
        .await
        .unwrap()
        .expect("metadata row exists for fetched URL");
    assert_eq!(row.status, UrlStatus::Succeeded);
    assert_eq!(row.last_run_id, rid);
    assert_eq!(row.depth, 0);
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
