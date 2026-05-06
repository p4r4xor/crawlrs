//! Construct every concrete impl from `CrawlrsConfig`.
//!
//! This is the only place in the binary that knows about specific
//! adapter crates. Output is a fully-wired `Crawler` plus the
//! collaborators the HTTP host needs (frontier, politeness, metadata
//! handles for the maintenance loop).

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{HostHashShardPolicy, ShardKey, ShardingPolicy};
use crawlrs_fetch::{NoProxyResolver, WreqFetcher, WreqFetcherConfig};
use crawlrs_frontier_redis::RedisFrontier;
use crawlrs_metadata::PostgresMetadataStore;
use crawlrs_parse::LolHtmlParser;
use crawlrs_politeness::{PolitenessConfig as CorePolitenessConfig, RedisPoliteness};
use crawlrs_runtime::{Crawler, CrawlerConfig};
use crawlrs_store::{MultiStore, ParquetStore, PathBuilder, RotationPolicy, WarcStore};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use sqlx::postgres::PgPoolOptions;

use crate::config::{CrawlrsConfig, StoreBackend};

/// Wired-up crawler plus inherent handles the maintenance loop needs.
pub struct Built {
    pub crawler: Crawler,
    pub frontier: Arc<RedisFrontier>,
    pub politeness: Arc<RedisPoliteness>,
    pub metadata: Arc<PostgresMetadataStore>,
}

pub async fn build(config: &CrawlrsConfig) -> Result<Built> {
    let sharding_policy: Arc<dyn ShardingPolicy> =
        Arc::new(HostHashShardPolicy::new(config.sharding.num_shards));
    let owned_shards = derive_owned_shards(config)?;

    let redis_pool = build_redis_pool(config).await?;
    let pg_pool = build_postgres_pool(config).await?;

    let fetcher = build_fetcher(config)?;
    let parser = Arc::new(LolHtmlParser::new());

    PostgresMetadataStore::migrate(&pg_pool)
        .await
        .context("running Postgres migrations")?;
    let metadata = Arc::new(PostgresMetadataStore::with_pool(pg_pool));

    let frontier = Arc::new(
        RedisFrontier::new(
            redis_pool.clone(),
            sharding_policy.clone(),
            owned_shards.clone(),
            &config.run_id,
        )
        .await
        .context("constructing RedisFrontier")?,
    );

    let politeness = Arc::new(
        RedisPoliteness::new(
            redis_pool.clone(),
            sharding_policy.clone(),
            owned_shards.clone(),
            fetcher.clone(),
            &config.run_id,
            build_politeness_config(config),
        )
        .await
        .context("constructing RedisPoliteness")?,
    );

    let store = build_store(config).await?;

    let runtime_config = build_runtime_config(config);

    let crawler = Crawler::builder()
        .frontier(frontier.clone())
        .politeness(politeness.clone())
        .fetcher(fetcher)
        .parser(parser)
        .store(store)
        .metadata(metadata.clone())
        .sharding_policy(sharding_policy)
        .config(runtime_config)
        .run_id(&config.run_id)
        .build()
        .map_err(|e| anyhow!("CrawlerBuilder::build: {e}"))?;

    Ok(Built {
        crawler,
        frontier,
        politeness,
        metadata,
    })
}

async fn build_redis_pool(config: &CrawlrsConfig) -> Result<bb8::Pool<RedisConnectionManager>> {
    // For v1 we accept the URL as-is; bb8-redis's RedisConnectionManager
    // parses redis:// URLs natively. Sentinel mode (redis-sentinel://)
    // is a v1.x extension; it would require swapping the manager type.
    // The check here surfaces the "not yet implemented" path eagerly
    // rather than silently producing a connection failure.
    if config.redis.url.starts_with("redis-sentinel://") {
        bail!(
            "redis-sentinel:// URLs aren't supported yet (Phase 6a v1); \
             use redis:// against a Sentinel-aware proxy or pin a primary"
        );
    }
    let manager = RedisConnectionManager::new(config.redis.url.clone())
        .with_context(|| format!("parsing Redis URL: {}", config.redis.url))?;
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
        .connect(&config.postgres.url)
        .await
        .with_context(|| format!("connecting to Postgres: {}", config.postgres.url))?;
    Ok(pool)
}

fn build_fetcher(config: &CrawlrsConfig) -> Result<Arc<WreqFetcher>> {
    let fetcher_config = WreqFetcherConfig {
        max_body_bytes: config.fetch.max_body_bytes,
        user_agent: Some(config.fetch.user_agent.clone()),
        default_timeout: config.fetch.default_timeout,
        proxy: Arc::new(NoProxyResolver),
        ..WreqFetcherConfig::default()
    };
    let fetcher = WreqFetcher::new(fetcher_config).context("constructing WreqFetcher")?;
    Ok(Arc::new(fetcher))
}

fn build_politeness_config(config: &CrawlrsConfig) -> CorePolitenessConfig {
    let user_agent = config
        .politeness
        .user_agent
        .clone()
        .unwrap_or_else(|| config.fetch.user_agent.clone());
    CorePolitenessConfig {
        min_delay: config.politeness.min_delay,
        honor_robots_txt: config.politeness.honor_robots_txt,
        robots_cache_ttl: config.politeness.robots_cache_ttl,
        user_agent,
        backoff: crawlrs_politeness::BackoffPolicy {
            initial_backoff: config.politeness.backoff.initial_backoff,
            max_backoff: config.politeness.backoff.max_backoff,
            multiplier: config.politeness.backoff.multiplier,
            circuit_open_after_failures: config.politeness.backoff.circuit_open_after_failures,
        },
        manual_excludes: config.politeness.manual_excludes.clone(),
        per_domain: config
            .politeness
            .per_domain
            .iter()
            .map(|(host, override_)| {
                (
                    host.clone(),
                    crawlrs_politeness::PolitenessOverride {
                        min_delay: override_.min_delay,
                        honor_robots_txt: override_.honor_robots_txt,
                    },
                )
            })
            .collect(),
    }
}

fn build_runtime_config(config: &CrawlrsConfig) -> CrawlerConfig {
    CrawlerConfig {
        workers: config.runtime.workers,
        user_agent: config.fetch.user_agent.clone(),
        max_depth: config.runtime.max_depth,
        max_retries: config.runtime.max_retries,
        cross_run_dedup: config.runtime.cross_run_dedup,
        pod_ordinal: pod_ordinal_from_env(),
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
            std::fs::create_dir_all(path)
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
                builder = builder.with_access_key_id(k);
            }
            if let Some(s) = secret_access_key {
                builder = builder.with_secret_access_key(s);
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
    let rotation = RotationPolicy::default();

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
        // Owners are 0..N modulo replica count. We don't know the
        // replica count from inside the pod; for v1 assume each pod
        // owns shards `ordinal, ordinal + R, ordinal + 2R, ...` where
        // R is read from CRAWLRS_REPLICAS env (set by the StatefulSet
        // template). If unset, default to one pod owning all shards
        // mod ordinal (i.e., this pod owns just `ordinal` if ordinal
        // < num_shards, else nothing - which would be a misconfig).
        let replicas: u32 = std::env::var("CRAWLRS_REPLICAS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let mut owned = Vec::new();
        let mut shard = ordinal;
        while shard < config.sharding.num_shards {
            owned.push(shard);
            shard = shard.saturating_add(replicas);
            if replicas == 0 {
                break;
            }
        }
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
