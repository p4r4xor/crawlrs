//! `crawl.toml` schema + parsing.
//!
//! Top-level layout (see `examples/crawl.toml` for a fully-commented
//! template):
//!
//! ```toml
//! run_id = "monthly-2026-05"
//!
//! [redis]
//! url = "redis://localhost:6379"
//! pool_size = 16
//!
//! [postgres]
//! url = "postgres://crawlrs:crawlrs@localhost/crawlrs"
//! pool_size = 8
//!
//! [fetch]
//! max_body_bytes = 10485760
//! user_agent = "crawlrs/0.0.1 (+https://github.com/p4r4xor/crawlrs)"
//!
//! [politeness]
//! host_delay = "1s"
//! obey_robots_txt = true
//! robots_ttl = "24h"
//!
//! [politeness.backoff]
//! initial_backoff = "30s"
//! max_backoff = "10m"
//! multiplier = 2.0
//! failure_threshold = 10
//!
//! [runtime]
//! workers = 4
//! max_retries = 5
//! link_dispatch = "durable_outbox"  # or "direct"
//!
//! [store]
//! parquet = true
//! warc    = true
//!
//! [store.backend]
//! kind = "local"
//! local_path = "/var/lib/crawlrs/data"
//!
//! [server]
//! listen = "0.0.0.0:9090"
//!
//! [sharding]
//! num_shards = 8
//! ```
//!
//! Env-var overlay: a small set of high-impact knobs accept env
//! overrides so an operator can tweak a deployment without rewriting
//! the ConfigMap. Documented inline in `apply_env_overlay`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use crawlrs_core::LinkDispatch;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct CrawlrsConfig {
    pub run_id: String,
    pub redis: RedisConfig,
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub fetch: FetchConfig,
    #[serde(default)]
    pub politeness: PolitenessConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub frontier: FrontierConfig,
    #[serde(default)]
    pub store: StoreConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub sharding: ShardingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    /// `redis://...` or `redis-sentinel://service-name@sentinel-host:port,...`
    pub url: String,
    #[serde(default = "default_redis_pool_size")]
    pub pool_size: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    /// `postgres://user:pass@host/db`
    pub url: String,
    #[serde(default = "default_postgres_pool_size")]
    pub pool_size: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FetchConfig {
    pub max_body_bytes: u64,
    /// Override the User-Agent on outgoing fetches. `None` (the
    /// default) lets the wreq emulation profile drive the UA, which
    /// pairs the wire-format string with the matching TLS / HTTP/2
    /// fingerprint. Set this only when impersonating a specific
    /// crawler identity (e.g. for politeness signalling on a
    /// site-specific allowlist).
    pub user_agent: Option<String>,
    #[serde(with = "humantime_serde")]
    pub default_timeout: Duration,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 10 * 1024 * 1024,
            user_agent: None,
            default_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PolitenessConfig {
    #[serde(with = "humantime_serde")]
    pub host_delay: Duration,
    pub obey_robots_txt: bool,
    #[serde(with = "humantime_serde")]
    pub robots_ttl: Duration,
    pub backoff: BackoffPolicy,
    #[serde(default)]
    pub blocklist: HashSet<String>,
    #[serde(default)]
    pub per_domain: HashMap<String, PerDomainOverride>,
    /// Global default depth cap. `None` (default) = unbounded; per-host
    /// overrides in `per_domain` can still raise or lower an individual
    /// host's cap.
    #[serde(default)]
    pub max_depth: Option<u32>,
    /// Global default cap on successful fetches per host. `None`
    /// (default) = uncapped; per-host overrides in `per_domain` can
    /// still raise or lower an individual host's quota.
    #[serde(default)]
    pub max_urls: Option<u64>,
}

impl Default for PolitenessConfig {
    fn default() -> Self {
        Self {
            host_delay: Duration::from_secs(1),
            obey_robots_txt: true,
            robots_ttl: Duration::from_secs(24 * 60 * 60),
            backoff: BackoffPolicy::default(),
            blocklist: HashSet::new(),
            per_domain: HashMap::new(),
            max_depth: None,
            max_urls: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BackoffPolicy {
    #[serde(with = "humantime_serde")]
    pub initial_backoff: Duration,
    #[serde(with = "humantime_serde")]
    pub max_backoff: Duration,
    pub multiplier: f64,
    pub failure_threshold: u32,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(600),
            multiplier: 2.0,
            failure_threshold: 10,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PerDomainOverride {
    #[serde(with = "humantime_serde::option")]
    pub host_delay: Option<Duration>,
    pub obey_robots_txt: Option<bool>,
    /// Per-host depth cap. Wins over `politeness.max_depth`. `None`
    /// falls back to the global default.
    pub max_depth: Option<u32>,
    /// Per-host URL-count cap. Wins over `politeness.max_urls`.
    /// `None` falls back to the global default (which may itself
    /// be `None` = uncapped).
    pub max_urls: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub workers: usize,
    pub max_retries: u32,
    /// Strategy for moving discovered outbound URLs into the
    /// Frontier. Default `Direct` is the lower-cost path that loses
    /// URLs during transient Frontier errors; `DurableOutbox` is the
    /// opt-in path that commits outbound URLs atomically with
    /// metadata at the cost of ~100x the metadata write rate.
    pub link_dispatch: LinkDispatch,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            max_retries: 5,
            link_dispatch: LinkDispatch::default(),
        }
    }
}

/// Operator-tunable knobs for the Redis-backed frontier.
///
/// Defaults match the per-shard sizing rules from the design ADRs;
/// `bloom_capacity` is the most likely value to tune for production
/// runs (size to expected unique URLs for the run, with ~20%
/// headroom). See `BloomConfig` in `crawlrs-frontier` for the
/// memory / FPR tradeoff.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FrontierConfig {
    /// Lease expiry for an in-flight URL. A worker holding a URL for
    /// longer is presumed dead; the reclaim pass re-pushes the URL.
    /// 60 s comfortably exceeds typical fetch durations.
    #[serde(with = "humantime_serde")]
    pub lease_timeout: Duration,
    /// Cadence at which the runtime drives `Frontier::tick` (which
    /// runs both the promoter wake -> ready pass and the lease
    /// reclaim pass). 50 ms keeps the latency tail collapsed under
    /// sustained load.
    #[serde(with = "humantime_serde")]
    pub promoter_tick: Duration,
    /// Initial capacity of the RedisBloom filter that fronts submit.
    /// Sized once at startup; RedisBloom scales past capacity via
    /// stacked sub-filters but pays a per-op CPU and memory cost per
    /// added layer. Size to expected unique URLs with ~20% headroom.
    pub bloom_capacity: u64,
    /// Target false-positive rate of the RedisBloom filter. Each
    /// false positive silently drops a URL at submit; tune this for
    /// the "missed coverage" budget you can tolerate.
    /// 0.001 (0.1 %) costs ~1.8 bytes per URL.
    pub bloom_fpr: f64,
}

impl Default for FrontierConfig {
    fn default() -> Self {
        Self {
            lease_timeout: Duration::from_secs(60),
            promoter_tick: Duration::from_millis(50),
            bloom_capacity: 1_000_000,
            bloom_fpr: 0.001,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    pub parquet: bool,
    pub warc: bool,
    pub backend: StoreBackend,
    pub base_prefix: String,
    pub worker_id: Option<String>,
    pub rotation: StoreRotationConfig,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            parquet: true,
            warc: true,
            backend: StoreBackend::default(),
            base_prefix: "crawlrs".into(),
            worker_id: None,
            rotation: StoreRotationConfig::default(),
        }
    }
}

/// Rotation thresholds for an output file. The first trigger to fire
/// (rows, bytes, or duration) closes the current file. Lowering these
/// caps cuts the steady-state memory the writer holds: every active
/// shard buffers rows + body bytes in RAM until rotation, so peak
/// resident memory is bounded by (shards * max_bytes) plus the
/// per-row inline overhead.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StoreRotationConfig {
    pub max_bytes: usize,
    pub max_rows: usize,
    #[serde(with = "humantime_serde")]
    pub max_duration: Duration,
}

impl Default for StoreRotationConfig {
    fn default() -> Self {
        Self {
            max_bytes: 128 * 1024 * 1024,
            max_rows: 100_000,
            max_duration: Duration::from_secs(30 * 60),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoreBackend {
    Local {
        path: PathBuf,
    },
    S3 {
        endpoint: Option<String>,
        bucket: String,
        region: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        #[serde(default)]
        allow_http: bool,
        #[serde(default)]
        virtual_hosted_style_request: bool,
    },
}

impl Default for StoreBackend {
    fn default() -> Self {
        Self::Local {
            path: PathBuf::from("/var/lib/crawlrs/data"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:9090".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ShardingConfig {
    pub num_shards: u32,
    /// `None` means "owned shards derived from POD_NAME ordinal";
    /// explicit list overrides the auto-derivation (useful for
    /// single-pod testing).
    pub owned_shards: Option<Vec<u32>>,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            num_shards: 8,
            owned_shards: None,
        }
    }
}

const fn default_redis_pool_size() -> u32 {
    16
}
const fn default_postgres_pool_size() -> u32 {
    8
}

impl CrawlrsConfig {
    /// Parse a TOML config from disk, apply env-var overlay, validate.
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let mut config: Self = toml::from_str(&contents)
            .with_context(|| format!("parsing TOML config {}", path.display()))?;
        config.apply_env_overlay();
        config.validate()?;
        Ok(config)
    }

    /// Reject incoherent configurations before any I/O happens. Currently:
    ///   * `obey_robots_txt = true` requires `[fetch].user_agent` to be
    ///     set. Random emulation rotates the wire User-Agent per request,
    ///     which can't coherently match a robots.txt rule group; honoring
    ///     robots without a stable identity advertises a contract we
    ///     can't actually keep.
    pub fn validate(&self) -> Result<()> {
        if self.politeness.obey_robots_txt && self.fetch.user_agent.is_none() {
            anyhow::bail!(
                "[politeness].obey_robots_txt = true requires [fetch].user_agent \
                 to be set. Random emulation produces a different wire User-Agent \
                 per request, so there is no stable identity for robots.txt to \
                 match against. Either pin [fetch].user_agent to a stable string \
                 (recommended for any crawl that wants to be a good citizen), or \
                 set [politeness].obey_robots_txt = false (stealth mode)."
            );
        }
        Ok(())
    }

    /// Apply env-var overrides for high-impact knobs. Documented here
    /// rather than scattered: any operator setting these expects them
    /// to win over the TOML file.
    fn apply_env_overlay(&mut self) {
        if let Ok(v) = std::env::var("CRAWLRS_RUN_ID") {
            self.run_id = v;
        }
        if let Ok(v) = std::env::var("CRAWLRS_REDIS_URL") {
            self.redis.url = v;
        }
        if let Ok(v) = std::env::var("CRAWLRS_POSTGRES_URL") {
            self.postgres.url = v;
        }
        if let Ok(v) = std::env::var("CRAWLRS_LISTEN") {
            self.server.listen = v;
        }
        if let Ok(v) = std::env::var("CRAWLRS_WORKERS")
            && let Ok(n) = v.parse::<usize>()
        {
            self.runtime.workers = n;
        }
        // S3 backend overlays. Only meaningful when store.backend is
        // `s3`; for `local` the env vars are no-ops.
        if let StoreBackend::S3 {
            endpoint,
            access_key_id,
            secret_access_key,
            ..
        } = &mut self.store.backend
        {
            if let Ok(v) = std::env::var("CRAWLRS_S3_ENDPOINT") {
                *endpoint = Some(v);
            }
            if let Ok(v) = std::env::var("CRAWLRS_S3_ACCESS_KEY_ID") {
                *access_key_id = Some(v);
            }
            if let Ok(v) = std::env::var("CRAWLRS_S3_SECRET_ACCESS_KEY") {
                *secret_access_key = Some(v);
            }
        }
    }

    /// One-line summary used by `crawlrs validate`.
    pub fn summary(&self) -> String {
        format!(
            "run_id={} workers={} shards={} parquet={} warc={} listen={}",
            self.run_id,
            self.runtime.workers,
            self.sharding.num_shards,
            self.store.parquet,
            self.store.warc,
            self.server.listen,
        )
    }
}
