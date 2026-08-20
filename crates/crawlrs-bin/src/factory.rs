//! Construct every concrete impl from `CrawlrsConfig`.
//!
//! This is the only place in the binary that knows about specific
//! adapter crates. Output is a fully-wired `Crawler` plus the
//! collaborators the HTTP host needs (frontier, politeness, metadata
//! handles for the maintenance loop).

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    BackoffTracker, Blocklist, CrawlOverride, CrawlScope, HostHashShardPolicy, RobotsChecker,
    RunId, ShardKey, ShardingPolicy, WakePlanner, owned_shards_for_replica,
};
use crawlrs_fetch::{NoProxyResolver, WreqFetcher, WreqFetcherConfig};
use crawlrs_frontier::{RedisFrontier, RedisFrontierParams, validate_pool_size};
use crawlrs_metadata::PostgresMetadataStore;
use crawlrs_parse::LolHtmlParser;
use crawlrs_politeness::{
    CompositePoliteness, NoopBackoffTracker, NoopRobotsChecker, NoopWakePlanner,
    PolitenessConfig as CorePolitenessConfig,
};
use crawlrs_runtime::{Crawler, CrawlerConfig};
use crawlrs_store::{MultiStore, ParquetStore, PathBuilder, RotationPolicy, WarcStore};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use sqlx::postgres::PgPoolOptions;
use tracing::warn;

use crate::config::{CrawlrsConfig, StoreBackend};

/// Wired-up crawler plus inherent handles the maintenance loop needs.
pub struct Built {
    pub crawler: Crawler,
    pub frontier: Arc<RedisFrontier>,
    pub politeness: Arc<CompositePoliteness>,
    pub metadata: Arc<PostgresMetadataStore>,
}

pub async fn build(config: &CrawlrsConfig) -> Result<Built> {
    let sharding_policy: Arc<dyn ShardingPolicy> =
        Arc::new(HostHashShardPolicy::new(config.sharding.num_shards));
    let owned_shards = derive_owned_shards(config)?;

    let redis_pool = build_redis_pool(config).await?;
    let pg_pool = build_postgres_pool(config).await?;

    // Defense-in-depth: an under-sized Redis pool wouldn't fail the
    // build outright, it would just produce connection-acquisition
    // latency at runtime. Fail-fast here instead; diagnosing pool
    // starvation in prod is dramatically more expensive than an
    // honest startup error.
    validate_pool_size(&redis_pool, config.runtime.workers as u32)
        .context("validating Redis pool size against worker count")?;

    let fetcher = build_fetcher(config)?;
    let parser = Arc::new(LolHtmlParser::new());

    PostgresMetadataStore::migrate(&pg_pool)
        .await
        .context("running Postgres migrations")?;
    let metadata = Arc::new(PostgresMetadataStore::with_pool(pg_pool));

    let crawl_scope = build_crawl_scope(config);
    let blocklist = build_blocklist(config);

    let frontier = Arc::new(
        RedisFrontier::new(RedisFrontierParams {
            pool: redis_pool.clone(),
            sharding_policy: sharding_policy.clone(),
            owned_shards: owned_shards.clone(),
            run_id: RunId::new(config.run_id.clone()),
            bloom_config: crawlrs_frontier::BloomConfig {
                capacity: config.frontier.bloom_capacity,
                fpr: config.frontier.bloom_fpr,
            },
            crawl_scope: crawl_scope.clone(),
        })
        .await
        .context("constructing RedisFrontier")?
        .with_lease_timeout(config.frontier.lease_timeout),
    );

    let politeness = build_politeness(
        config,
        &redis_pool,
        &sharding_policy,
        &owned_shards,
        &fetcher,
    )
    .await?;

    let store = build_store(config).await?;

    let runtime_config = build_runtime_config(config);

    let crawler = Crawler::builder()
        .frontier(frontier.clone())
        .politeness(politeness.clone())
        .fetcher(fetcher)
        .parser(parser)
        .store(store)
        .metadata(metadata.clone())
        // PostgresMetadataStore satisfies both MetadataStore (write
        // path) and Outbox (publisher's drain path); the same Arc
        // goes to both setters so the writer and the drain share one
        // connection pool.
        .outbox(metadata.clone())
        .sharding_policy(sharding_policy)
        .config(runtime_config)
        .crawl_scope(crawl_scope)
        .blocklist(blocklist)
        .run_id(config.run_id.as_str())
        .build()
        .map_err(|e| anyhow!("CrawlerBuilder::build: {e}"))?;

    Ok(Built {
        crawler,
        frontier,
        politeness,
        metadata,
    })
}

/// Construct only the RedisFrontier (no Postgres, no store, no parser,
/// no politeness). Used by the `crawlrs seed` subcommand which only
/// needs to push URLs into the Frontier and exit.
///
/// Owns every shard for the seed pass; sharding is a runtime concern
/// (which pod claims which URL), not a load-time one.
pub async fn build_frontier(config: &CrawlrsConfig) -> Result<Arc<RedisFrontier>> {
    let sharding_policy: Arc<dyn ShardingPolicy> =
        Arc::new(HostHashShardPolicy::new(config.sharding.num_shards));
    let owned_shards: Vec<ShardKey> = (0..config.sharding.num_shards).collect();

    let redis_pool = build_redis_pool(config).await?;
    let crawl_scope = build_crawl_scope(config);

    let frontier = Arc::new(
        RedisFrontier::new(RedisFrontierParams {
            pool: redis_pool,
            sharding_policy,
            owned_shards,
            run_id: RunId::new(config.run_id.clone()),
            bloom_config: crawlrs_frontier::BloomConfig {
                capacity: config.frontier.bloom_capacity,
                fpr: config.frontier.bloom_fpr,
            },
            crawl_scope,
        })
        .await
        .context("constructing RedisFrontier")?
        .with_lease_timeout(config.frontier.lease_timeout),
    );
    Ok(frontier)
}

async fn build_redis_pool(config: &CrawlrsConfig) -> Result<bb8::Pool<RedisConnectionManager>> {
    // For v1 we accept the URL as-is; bb8-redis's RedisConnectionManager
    // parses redis:// URLs natively. Sentinel mode (redis-sentinel://)
    // is a v1.x extension; it would require swapping the manager type.
    // The check here surfaces the "not yet implemented" path eagerly
    // rather than silently producing a connection failure.
    if config.redis.url.expose().starts_with("redis-sentinel://") {
        bail!(
            "redis-sentinel:// URLs aren't supported yet; \
             use redis:// against a Sentinel-aware proxy or pin a primary"
        );
    }
    // Don't interpolate the URL into the error: it embeds credentials.
    let manager =
        RedisConnectionManager::new(config.redis.url.expose()).context("parsing Redis URL")?;
    let pool = bb8::Pool::builder()
        .max_size(config.redis.pool_size)
        .build(manager)
        .await
        .context("building Redis bb8 pool")?;
    Ok(pool)
}

async fn build_postgres_pool(config: &CrawlrsConfig) -> Result<sqlx::PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(config.postgres.pool_size)
        .connect(config.postgres.url.expose())
        .await
        // Don't interpolate the URL into the error: it embeds credentials.
        .context("connecting to Postgres")?;
    Ok(pool)
}

fn build_fetcher(config: &CrawlrsConfig) -> Result<Arc<WreqFetcher>> {
    let fetcher_config = WreqFetcherConfig {
        max_body_bytes: config.fetch.max_body_bytes,
        user_agent: config.fetch.user_agent.clone(),
        default_timeout: config.fetch.default_timeout,
        read_timeout: config.fetch.read_timeout,
        proxy: Arc::new(NoProxyResolver),
        ..WreqFetcherConfig::default()
    };
    let fetcher = WreqFetcher::new(fetcher_config).context("constructing WreqFetcher")?;
    Ok(Arc::new(fetcher))
}

fn build_politeness_config(config: &CrawlrsConfig) -> CorePolitenessConfig {
    // One UA: `[fetch].user_agent` identifies us both on the wire and
    // for robots.txt rule matching. The combination "obey_robots_txt
    // = true with no fetch UA" is rejected at config-load time, so by
    // the time we get here either fetch.user_agent is `Some` or robots
    // are being skipped entirely (matcher value never observed).
    let user_agent = config
        .fetch
        .user_agent
        .clone()
        .unwrap_or_else(|| CorePolitenessConfig::default().user_agent);
    CorePolitenessConfig {
        enabled: config.politeness.enabled,
        host_delay: config.politeness.host_delay,
        obey_robots_txt: config.politeness.obey_robots_txt,
        robots_ttl: config.politeness.robots_ttl,
        user_agent,
        backoff: crawlrs_politeness::BackoffPolicy {
            initial_backoff: config.politeness.backoff.initial_backoff,
            max_backoff: config.politeness.backoff.max_backoff,
            multiplier: config.politeness.backoff.multiplier,
            failure_threshold: config.politeness.backoff.failure_threshold,
        },
        per_domain: config
            .politeness
            .per_domain
            .iter()
            .map(|(host, override_)| {
                (
                    host.clone(),
                    crawlrs_politeness::PolitenessOverride {
                        host_delay: override_.host_delay,
                        obey_robots_txt: override_.obey_robots_txt,
                        robots_ttl: override_.robots_ttl,
                    },
                )
            })
            .collect(),
    }
}

fn build_crawl_scope(config: &CrawlrsConfig) -> CrawlScope {
    let per_domain = config
        .crawl
        .per_domain
        .iter()
        .map(|(host, override_)| {
            (
                host.clone(),
                CrawlOverride {
                    max_depth: override_.max_depth,
                    max_urls: override_.max_urls,
                },
            )
        })
        .collect();
    CrawlScope::new(config.crawl.max_depth, config.crawl.max_urls, per_domain)
}

fn build_blocklist(config: &CrawlrsConfig) -> Blocklist {
    Blocklist::new(config.access.blocklist.clone())
}

/// Choose the politeness wiring based on `[politeness].enabled`.
///
/// `true` (default): full Redis-backed `CompositePoliteness` with
/// the three sub-impls (`RedisWakePlanner`, `RedisRobotsChecker`,
/// `RedisBackoffTracker`).
///
/// `false`: the three sub-traits are swapped for noop impls
/// (`NoopWakePlanner` / `NoopRobotsChecker` / `NoopBackoffTracker`),
/// wrapped in the same `CompositePoliteness`. The runtime sees an
/// `Arc<dyn Politeness>` either way; the blocklist (`[access]`)
/// and crawl scope (`[crawl]`) are *independent* concerns owned
/// by the worker and stay active when politeness is disabled.
///
/// When disabled, any `[politeness.per_domain]` overrides are
/// dead config; we log one warning per ignored override so the
/// operator sees them at boot.
async fn build_politeness(
    config: &CrawlrsConfig,
    redis_pool: &bb8::Pool<RedisConnectionManager>,
    sharding_policy: &Arc<dyn ShardingPolicy>,
    owned_shards: &[ShardKey],
    fetcher: &Arc<WreqFetcher>,
) -> Result<Arc<CompositePoliteness>> {
    if config.politeness.enabled {
        return Ok(Arc::new(
            CompositePoliteness::new(
                redis_pool.clone(),
                sharding_policy.clone(),
                owned_shards.to_vec(),
                fetcher.clone(),
                &config.run_id,
                build_politeness_config(config),
            )
            .await
            .context("constructing CompositePoliteness")?,
        ));
    }

    // Master switch off: no Redis touches from politeness. Per-domain
    // overrides become dead config; surface them so the operator
    // doesn't wonder why a per-host override has no effect.
    for host in config.politeness.per_domain.keys() {
        warn!("politeness.enabled=false; ignoring per_domain override for {host:?}",);
    }
    let wake: Arc<dyn WakePlanner> = Arc::new(NoopWakePlanner);
    let robots: Arc<dyn RobotsChecker> = Arc::new(NoopRobotsChecker);
    let backoff: Arc<dyn BackoffTracker> = Arc::new(NoopBackoffTracker);
    Ok(Arc::new(CompositePoliteness::from_parts(
        wake,
        robots,
        backoff,
        build_politeness_config(config),
    )))
}

fn build_runtime_config(config: &CrawlrsConfig) -> CrawlerConfig {
    CrawlerConfig {
        workers: config.runtime.workers,
        max_retries: config.runtime.max_retries,
        pod_ordinal: pod_ordinal_from_env(),
        link_dispatch: config.runtime.link_dispatch,
        promoter_tick: config.frontier.promoter_tick,
        ..CrawlerConfig::default()
    }
}

/// Extract the StatefulSet pod ordinal from `HOSTNAME` (e.g.
/// `crawlrs-2` -> 2). Falls back to 0 outside a StatefulSet (single-pod
/// deployments, local dev, factory smoke tests).
///
/// The ordinal is the load-bearing input to [`crawlrs_core::WorkerIdentity`]
/// at worker spawn time; stable identity is what makes Redis Streams
/// tier-1 PEL replay reattach a restarted worker to its own previously
/// in-flight entries without waiting for `XAUTOCLAIM` idle.
fn pod_ordinal_from_env() -> u32 {
    let hostname = std::env::var("HOSTNAME").unwrap_or_default();
    hostname
        .rsplit_once('-')
        .and_then(|(_, suffix)| suffix.parse().ok())
        .unwrap_or(0)
}

async fn build_store(config: &CrawlrsConfig) -> Result<Arc<dyn crawlrs_core::Store>> {
    if !config.store.parquet && !config.store.warc {
        bail!("at least one of [store].parquet / [store].warc must be true");
    }

    let backend: Arc<dyn ObjectStore> = match &config.store.backend {
        StoreBackend::Local { path } => {
            tokio::fs::create_dir_all(path)
                .await
                .with_context(|| format!("creating local store dir {}", path.display()))?;
            Arc::new(
                LocalFileSystem::new_with_prefix(path)
                    .with_context(|| format!("opening local FS backend at {}", path.display()))?,
            )
        }
        StoreBackend::S3 {
            endpoint,
            bucket,
            region,
            access_key_id,
            secret_access_key,
            allow_http,
            virtual_hosted_style_request,
        } => {
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region)
                .with_allow_http(*allow_http)
                .with_virtual_hosted_style_request(*virtual_hosted_style_request);
            if let Some(ep) = endpoint {
                builder = builder.with_endpoint(ep);
            }
            if let Some(k) = access_key_id {
                builder = builder.with_access_key_id(k.expose());
            }
            if let Some(s) = secret_access_key {
                builder = builder.with_secret_access_key(s.expose());
            }
            Arc::new(builder.build().context("building AmazonS3 backend")?)
        }
    };

    let worker_id = config
        .store
        .worker_id
        .clone()
        .or_else(pod_ordinal_string)
        .unwrap_or_else(|| "0".to_string());

    let paths = PathBuilder::new(
        config.store.base_prefix.clone(),
        config.run_id.clone(),
        worker_id,
    );
    let rotation = RotationPolicy {
        max_bytes: config.store.rotation.max_bytes,
        max_rows: config.store.rotation.max_rows,
        max_duration: config.store.rotation.max_duration,
    };

    let mut stores: Vec<Arc<dyn crawlrs_core::Store>> = Vec::new();
    if config.store.parquet {
        stores.push(Arc::new(ParquetStore::new(
            backend.clone(),
            paths.clone(),
            rotation,
        )));
    }
    if config.store.warc {
        stores.push(Arc::new(WarcStore::new(
            backend.clone(),
            paths.clone(),
            rotation,
            config.run_id.clone(),
        )));
    }

    let multi = MultiStore::new(stores).map_err(|e| anyhow!("MultiStore::new: {e}"))?;
    Ok(Arc::new(multi))
}

/// Derive owned shards from the operator's choice. Order:
///
/// 1. Explicit `[sharding].owned_shards` list in the config wins.
/// 2. Otherwise, `POD_NAME` env var of shape `<name>-<ordinal>`
///    decides: ordinal 0 owns shard 0 (and N, 2N, ... if there are
///    fewer pods than shards), ordinal 1 owns shard 1, etc.
/// 3. If neither is set (single-process / dev path), all shards are
///    owned by this process.
fn derive_owned_shards(config: &CrawlrsConfig) -> Result<Vec<ShardKey>> {
    if let Some(explicit) = &config.sharding.owned_shards {
        for &shard in explicit {
            if shard >= config.sharding.num_shards {
                bail!(
                    "owned_shards entry {shard} >= num_shards {}",
                    config.sharding.num_shards
                );
            }
        }
        return Ok(explicit.clone());
    }

    if let Some(ordinal) = pod_ordinal() {
        // Each pod owns a strided subset of shards keyed on its ordinal
        // and the replica count R (from CRAWLRS_REPLICAS, set by the
        // StatefulSet template). If R is unset, default it to num_shards
        // so this pod owns exactly `[ordinal]` and never overlaps a
        // sibling. (A default of 1 would make pod k claim every shard
        // >= k, i.e. overlapping ownership across pods.) The strided
        // ownership math itself lives in crawlrs-core.
        let replicas: u32 = std::env::var("CRAWLRS_REPLICAS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(config.sharding.num_shards);
        let owned = owned_shards_for_replica(ordinal, replicas, config.sharding.num_shards);
        if owned.is_empty() {
            bail!(
                "POD_NAME ordinal {ordinal} >= num_shards {} with replicas={replicas}; \
                 nothing to own",
                config.sharding.num_shards
            );
        }
        return Ok(owned);
    }

    // No POD_NAME and no explicit list; assume single-process and
    // own every shard.
    Ok((0..config.sharding.num_shards).collect())
}

/// Parse `POD_NAME` (e.g., `crawlrs-3`) into the trailing ordinal.
/// Returns `None` when `POD_NAME` is unset or doesn't end in `-<u32>`.
fn pod_ordinal() -> Option<u32> {
    let name = std::env::var("POD_NAME").ok()?;
    let dash_at = name.rfind('-')?;
    name[dash_at + 1..].parse::<u32>().ok()
}

fn pod_ordinal_string() -> Option<String> {
    pod_ordinal().map(|o| o.to_string())
}
