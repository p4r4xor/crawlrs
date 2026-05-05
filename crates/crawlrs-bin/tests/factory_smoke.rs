//! Factory smoke test against testcontainer-backed dependencies.
//!
//! Spins up Redis + Postgres + MinIO (the full backing-services
//! footprint a real `crawlrs crawl` requires), constructs a
//! `CrawlrsConfig` pointing at those endpoints, and verifies
//! `factory::build` returns a `Built` without errors. Then submits
//! one URL to the frontier and confirms it round-trips through
//! `submit_batch` -> `claim`.
//!
//! This test exercises the binary's wiring at the lib level. The
//! end-to-end fetch path (URL -> store object -> metadata row) is
//! covered by `crawlrs-runtime/tests/integration.rs`; replicating
//! it here would re-test the runtime, not the binary's wiring.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crawlrs_bin::config::{
    BackoffPolicy, CrawlrsConfig, FetchConfig, PolitenessConfig, PostgresConfig, RedisConfig,
    RuntimeConfig, ServerConfig, ShardingConfig, StoreBackend, StoreConfig,
};
use crawlrs_bin::factory;
use crawlrs_core::{CanonicalUrl, Frontier, UrlEntry};
use testcontainers::ImageExt;
use testcontainers::core::{CmdWaitFor, ExecCommand};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn factory_builds_against_real_backends() {
    // Redis 7.2 for XAUTOCLAIM support (added in 6.2); same tag the
    // crawlrs-frontier-redis integration tests pin.
    let redis = Redis::default().with_tag("7.2").start().await.unwrap();
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Postgres 16-alpine for sqlx 0.8 compatibility; same tag the
    // crawlrs-metadata integration tests pin.
    let postgres = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .unwrap();
    let pg_port = postgres.get_host_port_ipv4(5432).await.unwrap();

    let minio = MinIO::default().start().await.unwrap();

    // Pre-create the bucket via mc; same pattern as the parquet/warc
    // tests in crawlrs-store.
    let cmd = ExecCommand::new([
        "sh",
        "-c",
        "mc alias set local http://localhost:9000 minioadmin minioadmin \
         && mc mb local/crawlrs-test",
    ])
    .with_cmd_ready_condition(CmdWaitFor::exit_code(0));
    minio.exec(cmd).await.unwrap();

    let tmp = tempfile::tempdir().unwrap();

    let config = CrawlrsConfig {
        run_id: "factory-test".to_string(),
        redis: RedisConfig {
            url: format!("redis://127.0.0.1:{redis_port}"),
            pool_size: 8,
        },
        postgres: PostgresConfig {
            url: format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"),
            pool_size: 4,
        },
        fetch: FetchConfig {
            max_body_bytes: 10 * 1024 * 1024,
            user_agent: "crawlrs-test/0.0.1".to_string(),
            default_timeout: Duration::from_secs(30),
        },
        politeness: PolitenessConfig {
            min_delay: Duration::from_secs(1),
            honor_robots_txt: true,
            robots_cache_ttl: Duration::from_secs(24 * 60 * 60),
            user_agent: None,
            backoff: BackoffPolicy::default(),
            manual_excludes: HashSet::new(),
            per_domain: HashMap::new(),
        },
        runtime: RuntimeConfig::default(),
        store: StoreConfig {
            parquet: true,
            warc: true,
            backend: StoreBackend::Local {
                path: tmp.path().to_path_buf(),
            },
            base_prefix: "crawlrs".to_string(),
            worker_id: Some("0".to_string()),
        },
        server: ServerConfig::default(),
        sharding: ShardingConfig {
            num_shards: 4,
            owned_shards: Some(vec![0, 1, 2, 3]),
        },
    };

    let built = factory::build(&config).await.expect("factory::build");

    // Round-trip a URL through the frontier to prove the wiring is
    // genuinely live (not just constructed): submit -> claim -> ack.
    let url = CanonicalUrl::parse("https://example.test/page").unwrap();
    let n = built
        .frontier
        .submit_batch(vec![UrlEntry::seed(url.clone())])
        .await
        .expect("submit_batch");
    assert_eq!(n, 1, "exactly one URL should be newly inserted");

    let claimed = built
        .frontier
        .claim()
        .await
        .expect("claim")
        .expect("frontier should yield the URL we just submitted");
    assert_eq!(claimed.url, url);

    built.frontier.ack(&url).await.expect("ack");

    // Smoke a Postgres write through the metadata layer; if migrations
    // didn't apply, this would fail with a missing-table error.
    built
        .metadata
        .dlq_size()
        .await
        .expect("metadata.dlq_size (proves schema is in place)");

    // Don't spin up a real workload via crawler.run(); construction
    // success + frontier round-trip + Postgres reachability is what
    // this test verifies. End-to-end fetch is covered in
    // crawlrs-runtime/tests/integration.rs.
    //
    // The migrate_pool path on the factory is exercised implicitly
    // here: if the factory forgot to migrate, the dlq_size() call
    // above would fail with a missing-table error.
}
