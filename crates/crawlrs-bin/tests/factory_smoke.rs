//! Factory smoke test against testcontainer-backed dependencies.
//!
//! Spins up Redis Stack + Postgres (the stateful backing services a
//! real `crawlrs crawl` requires), constructs a `CrawlrsConfig`
//! pointing at those endpoints with a local-FS store backend in a
//! tempdir, and verifies `factory::build` returns a `Built` without
//! errors. Then submits one URL to the frontier and confirms it
//! round-trips through `submit_batch` -> `tick` -> `claim` -> `ack`.
//!
//! This test exercises the binary's wiring at the lib level. The
//! end-to-end fetch path (URL -> store object -> metadata row) is
//! covered by `crawlrs-runtime/tests/integration.rs`; replicating
//! it here would re-test the runtime, not the binary's wiring.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crawlrs_bin::config::{
    BackoffPolicy, CrawlrsConfig, FetchConfig, FrontierConfig, PolitenessConfig, PostgresConfig,
    RedisConfig, RuntimeConfig, ServerConfig, ShardingConfig, StoreBackend, StoreConfig,
};
use crawlrs_bin::factory;
use crawlrs_core::{CanonicalUrl, ClaimOutcome, Frontier, UrlEntry, WorkerIdentity};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn factory_builds_against_real_backends() {
    // Redis Stack: the frontier requires the RedisBloom module for
    // submit-time dedup; stock Redis lacks BF.RESERVE.
    let redis = GenericImage::new("redis/redis-stack-server", "7.4.0-v0")
        .with_exposed_port(6379.into())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .unwrap();
    let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();

    // Postgres 16-alpine for sqlx 0.8 compatibility; same tag the
    // crawlrs-metadata integration tests pin.
    let postgres = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .unwrap();
    let pg_port = postgres.get_host_port_ipv4(5432).await.unwrap();

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
            user_agent: Some("crawlrs-test/0.0.1".to_string()),
            default_timeout: Duration::from_secs(30),
        },
        politeness: PolitenessConfig {
            host_delay: Duration::from_secs(1),
            obey_robots_txt: true,
            robots_ttl: Duration::from_secs(24 * 60 * 60),
            backoff: BackoffPolicy::default(),
            blocklist: HashSet::new(),
            per_domain: HashMap::new(),
            max_depth: None,
            max_urls: None,
        },
        runtime: RuntimeConfig::default(),
        frontier: FrontierConfig::default(),
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
    // genuinely live (not just constructed): submit -> tick -> claim
    // -> ack. The tick is required because `claim` only pops from
    // `ready`, and `submit` only places the host into `wake`; the
    // promoter pass in `tick` moves it.
    let url = CanonicalUrl::parse("https://example.test/page").unwrap();
    let n = built
        .frontier
        .submit_batch(vec![UrlEntry::seed(url.clone())])
        .await
        .expect("submit_batch");
    assert_eq!(n, 1, "exactly one URL should be newly inserted");

    built.frontier.tick().await.expect("tick promotes the host");

    let identity = WorkerIdentity::new(0, 0);
    let outcome = built.frontier.claim(&identity).await.expect("claim");
    match outcome {
        ClaimOutcome::Claimed {
            entry, attempt_id, ..
        } => {
            assert_eq!(entry.url, url);
            built.frontier.ack(&attempt_id).await.expect("ack");
        }
        other => panic!("expected Claimed after submit + tick; got {other:?}"),
    }

    // Smoke a Postgres write through the metadata layer; if migrations
    // didn't apply, this would fail with a missing-table error.
    built
        .metadata
        .dlq_size()
        .await
        .expect("metadata.dlq_size (proves schema is in place)");
}
