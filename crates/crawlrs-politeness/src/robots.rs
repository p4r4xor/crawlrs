//! Per-host robots.txt cache, backed by Redis.
//!
//! The cache stores the raw body bytes per host (Hash field `body`)
//! alongside fetched-at and expires-at timestamps. Parsing happens
//! per-request with [`texting_robots::Robot`]; we don't cache the
//! parsed form because requesters can ask about different
//! user-agents and parse cost is negligible against the network cost
//! we just avoided.
//!
//! Robots.txt fetching itself bypasses politeness: this layer calls
//! `Fetcher::fetch` directly. Otherwise the politeness gate would
//! block its own dependencies.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use bytes::Bytes;
use crawlrs_core::{CanonicalUrl, FetchRequest, Fetcher, ShardingPolicy};
use redis::AsyncCommands;
use tracing::{info, warn};

use crate::keys::KeyPrefix;
use crate::politeness::RedisPolitenessError;

type LocalResult<T> = std::result::Result<T, RedisPolitenessError>;

const ROBOTS_FIELD_BODY: &str = "body";
const ROBOTS_FIELD_STATUS: &str = "status";
const ROBOTS_STATUS_OK: &str = "ok";
const ROBOTS_STATUS_ABSENT: &str = "absent";

/// TTL jitter half-band, percent. Each host's effective TTL is
/// `base_ttl * (100 + jitter)/100` where jitter is in `[-PCT, +PCT]`,
/// chosen deterministically from the host string so retries don't
/// thrash the cache. Spreads the refresh wave when 1M+ hosts were all
/// cached in the same first-contact burst.
const TTL_JITTER_PCT: i64 = 20;

/// Compute the effective TTL for a host's robots cache entry. The
/// jitter is host-stable so a single host doesn't randomly shorten its
/// own TTL on each rewrite, but is spread across hosts so the
/// expiration moments fan out across `[0.8 * ttl, 1.2 * ttl]`.
fn jittered_ttl(base: Duration, host: &str) -> Duration {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    host.hash(&mut h);
    // Map the 64-bit hash to `[-TTL_JITTER_PCT, +TTL_JITTER_PCT]`
    // (inclusive); 2N+1 buckets total.
    let band = (TTL_JITTER_PCT as u64 * 2) + 1;
    let bucket = (h.finish() % band) as i64;
    let jitter_pct = bucket - TTL_JITTER_PCT;
    let secs = base.as_secs() as i64;
    let scaled = (secs * (100 + jitter_pct)) / 100;
    Duration::from_secs(scaled.max(1) as u64)
}

/// In-process LRU capacity. 1024 hosts at typical robots.txt size
/// (~5 KB each) is ~5 MB, well within budget. If a workload sees
/// substantially more than 1k unique hosts under active rotation,
/// the eviction churn surfaces as Redis re-reads, which is still
/// strictly better than the no-cache-at-all baseline.
const IN_PROCESS_LRU_CAPACITY: u64 = 1024;

/// Per-host cached robots.txt and a parser facade.
///
/// Two-tier cache:
/// - **In-process** (`moka::sync::Cache<host, Bytes>`) - first hit, no
///   network. TTL aligned with the Redis TTL so the two layers expire
///   together. Cuts the typical Redis HGET-per-fetch load to roughly
///   one HGET per host per TTL window per worker process.
/// - **Redis** (Hash keyed on `crawlrs:{run}:s{shard}:robots:{host}`):
///   second hit, shared across pods. Survives worker restarts and
///   amortises fetches across the cluster.
/// - **HTTP fetch** - only on miss in both layers; result populates
///   both.
pub struct RobotsCache {
    pool: Pool<RedisConnectionManager>,
    keys: KeyPrefix,
    sharding_policy: Arc<dyn ShardingPolicy>,
    fetcher: Arc<dyn Fetcher>,
    user_agent: String,
    ttl: Duration,
    fetch_timeout: Duration,
    in_process: moka::sync::Cache<String, Bytes>,
}

impl RobotsCache {
    pub fn new(
        pool: Pool<RedisConnectionManager>,
        keys: KeyPrefix,
        sharding_policy: Arc<dyn ShardingPolicy>,
        fetcher: Arc<dyn Fetcher>,
        user_agent: String,
        ttl: Duration,
    ) -> Self {
        let in_process = moka::sync::Cache::builder()
            .max_capacity(IN_PROCESS_LRU_CAPACITY)
            .time_to_live(ttl)
            .build();
        Self {
            pool,
            keys,
            sharding_policy,
            fetcher,
            user_agent,
            ttl,
            fetch_timeout: Duration::from_secs(15),
            in_process,
        }
    }

    /// Number of hosts currently held in the in-process LRU. Useful as
    /// a gauge metric and for verifying the cache is being populated
    /// in tests.
    pub fn cache_len(&self) -> u64 {
        self.in_process.entry_count()
    }

    /// Force the in-process LRU to flush any pending insertions /
    /// evictions so `cache_len` reflects the current state
    /// exactly. moka batches updates lazily for throughput; tests
    /// that assert on the count call this first.
    pub fn run_pending_tasks(&self) {
        self.in_process.run_pending_tasks();
    }

    /// Override the per-request fetch timeout for robots.txt requests.
    pub fn with_fetch_timeout(mut self, t: Duration) -> Self {
        self.fetch_timeout = t;
        self
    }

    /// Whether the given URL may be fetched, per the host's robots.txt
    /// and the supplied user-agent. Returns `true` if no robots.txt
    /// exists, can't be parsed, or the network fetch fails;
    /// "no rules = everything allowed" is the standard convention,
    /// and we'd rather over-allow than block crawling on a transient
    /// network blip.
    #[tracing::instrument(skip(self), fields(url = %url, user_agent = %user_agent))]
    pub async fn allowed(&self, url: &CanonicalUrl, user_agent: &str) -> LocalResult<bool> {
        let host = match url.host() {
            Some(h) => h,
            None => return Ok(true),
        };
        let body = self.body_for(host, url).await?;
        Ok(evaluate_rules(&body, user_agent, url.as_str()))
    }

    /// Two-tier read for `host`'s robots body:
    /// in-process LRU -> Redis -> network fetch. Empty `Bytes` means
    /// "no robots.txt at this host" (404 or fetch error, cached as
    /// absent). On miss in either lower tier, populates the upper
    /// tier on the way back up.
    async fn body_for(&self, host: &str, source_url: &CanonicalUrl) -> LocalResult<Bytes> {
        // Tier 1: in-process LRU.
        if let Some(cached) = self.in_process.get(host) {
            metrics::counter!(crate::metrics::ROBOTS_CACHE_HITS_TOTAL).increment(1);
            return Ok(cached);
        }
        // Tier 2: Redis.
        if let Some(redis_cached) = self.read_cache(host, source_url).await? {
            self.in_process
                .insert(host.to_string(), redis_cached.clone());
            metrics::counter!(crate::metrics::ROBOTS_CACHE_HITS_TOTAL).increment(1);
            return Ok(redis_cached);
        }
        // Tier 3: network fetch.
        metrics::counter!(crate::metrics::ROBOTS_CACHE_MISSES_TOTAL).increment(1);
        let body = self.fetch_and_cache(host, source_url).await?;
        self.in_process.insert(host.to_string(), body.clone());
        Ok(body)
    }

    async fn read_cache(
        &self,
        host: &str,
        source_url: &CanonicalUrl,
    ) -> LocalResult<Option<Bytes>> {
        let shard = self.sharding_policy.shard_key(source_url);
        let key = self.keys.robots(shard, host);
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| RedisPolitenessError::Pool(format!("{e:?}")))?;

        let status: Option<String> = conn
            .hget::<_, _, Option<String>>(&key, ROBOTS_FIELD_STATUS)
            .await
            .map_err(RedisPolitenessError::from)?;

        match status.as_deref() {
            Some(ROBOTS_STATUS_OK) => {
                let raw: Option<redis::Value> = conn
                    .hget(&key, ROBOTS_FIELD_BODY)
                    .await
                    .map_err(RedisPolitenessError::from)?;
                Ok(raw.map(value_into_bytes).transpose()?)
            }
            Some(ROBOTS_STATUS_ABSENT) => Ok(Some(Bytes::new())),
            Some(other) => {
                warn!(
                    host,
                    status = other,
                    "unknown robots cache status; treating as miss"
                );
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn fetch_and_cache(&self, host: &str, source_url: &CanonicalUrl) -> LocalResult<Bytes> {
        let robots_url_str = format!("{}://{}/robots.txt", source_url.scheme(), host);
        let robots_url = CanonicalUrl::parse(&robots_url_str).map_err(|e| {
            RedisPolitenessError::Robots(format!("invalid robots url {robots_url_str}: {e}"))
        })?;

        let mut req = FetchRequest::new(robots_url.clone());
        req.timeout = self.fetch_timeout;

        let (status, body) = match self.fetcher.fetch(req).await {
            Ok(resp) => (resp.status, resp.body),
            Err(e) => {
                warn!(host, error = %e, "robots.txt fetch failed; caching as absent");
                self.write_cache(host, source_url, None).await?;
                return Ok(Bytes::new());
            }
        };

        match status {
            200 => {
                self.write_cache(host, source_url, Some(&body)).await?;
                info!(host, bytes = body.len(), "robots.txt fetched and cached");
                Ok(body)
            }
            404 | 410 => {
                self.write_cache(host, source_url, None).await?;
                Ok(Bytes::new())
            }
            other => {
                warn!(
                    host,
                    status = other,
                    "robots.txt non-2xx/404; treating as absent"
                );
                self.write_cache(host, source_url, None).await?;
                Ok(Bytes::new())
            }
        }
    }

    async fn write_cache(
        &self,
        host: &str,
        source_url: &CanonicalUrl,
        body: Option<&[u8]>,
    ) -> LocalResult<()> {
        let shard = self.sharding_policy.shard_key(source_url);
        let key = self.keys.robots(shard, host);
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| RedisPolitenessError::Pool(format!("{e:?}")))?;

        let ttl_secs = jittered_ttl(self.ttl, host).as_secs() as i64;
        match body {
            Some(b) => {
                let _: () = redis::pipe()
                    .hset(&key, ROBOTS_FIELD_STATUS, ROBOTS_STATUS_OK)
                    .hset(&key, ROBOTS_FIELD_BODY, b)
                    .expire(&key, ttl_secs)
                    .query_async(&mut *conn)
                    .await
                    .map_err(RedisPolitenessError::from)?;
            }
            None => {
                let _: () = redis::pipe()
                    .hset(&key, ROBOTS_FIELD_STATUS, ROBOTS_STATUS_ABSENT)
                    .hdel(&key, ROBOTS_FIELD_BODY)
                    .expire(&key, ttl_secs)
                    .query_async(&mut *conn)
                    .await
                    .map_err(RedisPolitenessError::from)?;
            }
        }
        Ok(())
    }

    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    /// For tests: bypass the network and fetch via the wrapped Fetcher
    /// instance. Useful for asserting the fake fetcher was called.
    #[doc(hidden)]
    pub async fn force_fetch(&self, url: &CanonicalUrl) -> LocalResult<Bytes> {
        let req = FetchRequest::new(url.clone());
        let resp = self
            .fetcher
            .fetch(req)
            .await
            .map_err(|e| RedisPolitenessError::Robots(format!("force_fetch: {e}")))?;
        Ok(resp.body)
    }
}

/// Pure helper: given the cached robots body, decide whether `url`
/// is allowed for `user_agent`. Empty body means "everything allowed".
pub(crate) fn evaluate_rules(body: &[u8], user_agent: &str, url: &str) -> bool {
    if body.is_empty() {
        return true;
    }
    let body_str = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => return true, // can't parse; fail-open
    };
    match texting_robots::Robot::new(user_agent, body_str.as_bytes()) {
        Ok(robot) => robot.allowed(url),
        Err(_) => true, // unparsable robots.txt; fail-open
    }
}

fn value_into_bytes(v: redis::Value) -> LocalResult<Bytes> {
    match v {
        redis::Value::BulkString(b) => Ok(Bytes::from(b)),
        redis::Value::SimpleString(s) => Ok(Bytes::from(s.into_bytes())),
        redis::Value::Nil => Ok(Bytes::new()),
        other => Err(RedisPolitenessError::Robots(format!(
            "expected BulkString for robots body, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    // Inline because: visibility-forced. `evaluate_rules` is
    // `pub(crate)` (a thin wrapper over `texting_robots`; users
    // who want robots evaluation should reach for `texting_robots`
    // directly, not us), and `jittered_ttl` is fully private (a
    // hash-derived TTL helper that has no standalone purpose
    // outside the cache). Both are correctly invisible from
    // `tests/`; we test them inline.

    use super::*;

    #[test]
    fn empty_body_allows_everything() {
        assert!(evaluate_rules(b"", "any-bot", "/foo"));
    }

    #[test]
    fn jittered_ttl_stays_in_band() {
        let base = Duration::from_secs(3600);
        for h in &["a.com", "b.com", "c.com", "very.long.host.example.test"] {
            let t = jittered_ttl(base, h);
            assert!(t.as_secs() >= 2880, "{h} below band: {t:?}"); // 0.8 * 3600
            assert!(t.as_secs() <= 4320, "{h} above band: {t:?}"); // 1.2 * 3600
        }
    }

    #[test]
    fn jittered_ttl_is_host_stable() {
        let base = Duration::from_secs(600);
        let a = jittered_ttl(base, "stable.test");
        let b = jittered_ttl(base, "stable.test");
        assert_eq!(a, b, "same host must always yield the same TTL");
    }

    #[test]
    fn jittered_ttl_spreads_across_hosts() {
        let base = Duration::from_secs(1000);
        let mut seen = std::collections::HashSet::new();
        for i in 0..50 {
            seen.insert(jittered_ttl(base, &format!("host-{i}.test")).as_secs());
        }
        // 41 distinct buckets are possible; with 50 hosts we should see
        // many of them. Strict-greater-than is safer than equality.
        assert!(
            seen.len() >= 10,
            "low jitter spread: {} unique TTLs",
            seen.len()
        );
    }

    #[test]
    fn disallow_root_blocks_path() {
        let body = b"User-agent: *\nDisallow: /";
        assert!(!evaluate_rules(
            body,
            "test-bot",
            "https://example.com/anything"
        ));
    }

    #[test]
    fn disallow_specific_path_only() {
        let body = b"User-agent: *\nDisallow: /private";
        assert!(evaluate_rules(
            body,
            "test-bot",
            "https://example.com/public"
        ));
        assert!(!evaluate_rules(
            body,
            "test-bot",
            "https://example.com/private/secret"
        ));
    }

    #[test]
    fn ua_specific_rules_apply_to_matching_ua() {
        let body = b"User-agent: bad-bot\nDisallow: /\n\nUser-agent: *\nDisallow: /private";
        assert!(!evaluate_rules(
            body,
            "bad-bot",
            "https://example.com/anything"
        ));
        assert!(evaluate_rules(
            body,
            "good-bot",
            "https://example.com/public"
        ));
    }

    #[test]
    fn invalid_utf8_body_fails_open() {
        let body = &[0xff, 0xfe, 0xfd];
        assert!(evaluate_rules(body, "any-bot", "/foo"));
    }
}
