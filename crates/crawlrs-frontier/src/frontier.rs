//! Redis-backed `Frontier` impl.
//!
//! Orchestrator: holds the bb8 pool, the key prefix, the Lua script
//! cache, the bloom config, and the lease timeout. Delegates the
//! actual Redis work to [`host_queue::HostQueueOps`] (one thin Rust
//! wrapper per Lua script) and to [`promoter::tick_once`] (the
//! `tick` body).
//!
//! The Redis data shape is:
//!
//! - `host_queue:<host>` (LIST<url_id>): per-host FIFO.
//! - `wake` (ZSET<host>): hosts not yet ready; score = next-allowed
//!   wall-clock ms.
//! - `ready` (LIST<host>): hosts whose wake-time has elapsed,
//!   populated by the promoter.
//! - `inflight` (ZSET<"url_id|host">): leases; score = lease
//!   expiry ms.
//! - `urls` (HASH<url_id, payload>): content-addressed payload.
//! - `seen` (RedisBloom): submit-time dedup.
//!
//! All per-shard keys share the same Redis Cluster hash tag so the
//! Lua scripts touch one slot per shard. See [`KeyPrefix`].

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    AttemptId, ClaimOutcome, CrawlScope, Error, Frontier, Result, ShardKey, ShardingPolicy,
    SubmitBatchOutcome, SubmitOutcome, UrlEntry, UrlId, WorkerIdentity,
};
use thiserror::Error as ThisError;
use tracing::{debug, warn};

use crate::bloom::{self, BloomConfig};
use crate::host_queue::{ClaimRaw, HostQueueError, HostQueueOps, Scripts, SubmitItem};
use crate::keys::KeyPrefix;
use crate::metrics as m;
use crate::promoter;

/// Default lease timeout. A worker holding a URL for longer than this
/// is presumed dead; reclaim re-pushes the URL. 60s comfortably
/// exceeds typical fetch durations.
pub const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(60);

/// Default per-tick batch limit for the promoter and reclaim passes.
pub const DEFAULT_TICK_BATCH_LIMIT: u64 = 1_000;

#[derive(Debug, ThisError)]
pub enum RedisFrontierError {
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("connection pool error: {0}")]
    Pool(String),

    #[error("shard {got} is not owned by this frontier instance (owns {owned:?})")]
    ShardNotOwned { got: ShardKey, owned: Vec<ShardKey> },

    #[error("shard {got} is out of range for the policy's shard_count={count}")]
    ShardOutOfRange { got: ShardKey, count: u32 },

    #[error("url has no host: {0}")]
    NoHost(String),

    #[error("bloom: {0}")]
    Bloom(String),

    #[error("host_queue: {0}")]
    HostQueue(#[from] HostQueueError),

    #[error("malformed AttemptId: {0}")]
    MalformedAttempt(String),
}

impl From<RedisFrontierError> for Error {
    fn from(e: RedisFrontierError) -> Self {
        Error::Frontier(e.to_string())
    }
}

type LocalResult<T> = std::result::Result<T, RedisFrontierError>;

/// `Frontier` impl backed by a Redis instance.
pub struct RedisFrontier {
    pool: Pool<RedisConnectionManager>,
    keys: KeyPrefix,
    sharding_policy: Arc<dyn ShardingPolicy>,
    owned_shards: Vec<ShardKey>,
    scripts: Scripts,
    lease_timeout: Duration,
    tick_batch_limit: u64,
    /// Crawl scope: per-host URL caps used by `submit_batch` to
    /// thread `[crawl].max_urls` into the Lua script. Depth caps
    /// are read by the worker (not by the frontier); we hold the
    /// whole scope so future per-batch scope queries don't need a
    /// constructor change.
    crawl_scope: CrawlScope,
    /// Round-robin cursor for `claim` across owned shards.
    claim_cursor: AtomicUsize,
}

impl std::fmt::Debug for RedisFrontier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisFrontier")
            .field("run_id", &self.keys.run_id())
            .field("owned_shards", &self.owned_shards)
            .field("lease_timeout", &self.lease_timeout)
            .finish_non_exhaustive()
    }
}

impl RedisFrontier {
    /// Build a frontier bound to a specific run, sharding policy, and
    /// owned-shard list. Reserves the per-shard bloom filter via
    /// `BF.RESERVE` (idempotent across processes); a missing
    /// RedisBloom module surfaces a clear error here so deployments
    /// fail fast.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        pool: Pool<RedisConnectionManager>,
        sharding_policy: Arc<dyn ShardingPolicy>,
        owned_shards: Vec<ShardKey>,
        run_id: impl Into<String>,
        bloom_config: BloomConfig,
        crawl_scope: CrawlScope,
    ) -> Result<Self> {
        let keys = KeyPrefix::new(run_id);
        let count = sharding_policy.shard_count();
        for &shard in &owned_shards {
            if shard >= count {
                return Err(RedisFrontierError::ShardOutOfRange { got: shard, count }.into());
            }
        }
        for &shard in &owned_shards {
            bloom::reserve(&pool, &keys.seen(shard), bloom_config)
                .await
                .map_err(RedisFrontierError::Bloom)?;
        }
        Ok(Self {
            pool,
            keys,
            sharding_policy,
            owned_shards,
            scripts: Scripts::new(),
            lease_timeout: DEFAULT_LEASE_TIMEOUT,
            tick_batch_limit: DEFAULT_TICK_BATCH_LIMIT,
            crawl_scope,
            claim_cursor: AtomicUsize::new(0),
        })
    }

    pub fn with_lease_timeout(mut self, lease: Duration) -> Self {
        self.lease_timeout = lease;
        self
    }

    pub fn with_tick_batch_limit(mut self, limit: u64) -> Self {
        self.tick_batch_limit = limit;
        self
    }

    pub fn pool_state(&self) -> bb8::State {
        self.pool.state()
    }

    /// Refresh the per-shard `ready` / `inflight` length gauges +
    /// pool-pending gauge. Called by the bin's maintenance loop.
    pub async fn record_pending_metrics(&self) -> Result<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| RedisFrontierError::Pool(format!("{e:?}")))?;
        for &shard in &self.owned_shards {
            let ready_len: i64 = redis::cmd("LLEN")
                .arg(self.keys.ready(shard))
                .query_async(&mut *conn)
                .await
                .map_err(RedisFrontierError::from)?;
            let inflight_len: i64 = redis::cmd("ZCARD")
                .arg(self.keys.inflight(shard))
                .query_async(&mut *conn)
                .await
                .map_err(RedisFrontierError::from)?;
            let shard_label = shard.to_string();
            metrics::gauge!(
                m::FRONTIER_READY_LENGTH,
                "shard" => shard_label.clone(),
            )
            .set(ready_len as f64);
            metrics::gauge!(
                m::FRONTIER_INFLIGHT_LENGTH,
                "shard" => shard_label,
            )
            .set(inflight_len as f64);
        }
        let state = self.pool.state();
        let active = state.connections.saturating_sub(state.idle_connections);
        metrics::gauge!(m::FRONTIER_POOL_PENDING).set(active as f64);
        Ok(())
    }

    // --- internals -----------------------------------------------------

    fn ops(&self) -> HostQueueOps<'_> {
        HostQueueOps {
            pool: &self.pool,
            keys: &self.keys,
            scripts: &self.scripts,
        }
    }

    fn shard_of(&self, entry: &UrlEntry) -> LocalResult<(ShardKey, String, UrlId)> {
        let shard = self.sharding_policy.shard_key(&entry.url);
        if !self.owned_shards.contains(&shard) {
            return Err(RedisFrontierError::ShardNotOwned {
                got: shard,
                owned: self.owned_shards.clone(),
            });
        }
        let host = entry
            .url
            .host()
            .ok_or_else(|| RedisFrontierError::NoHost(entry.url.as_str().into()))?
            .to_string();
        let url_id = UrlId::from_canonical(&entry.url);
        Ok((shard, host, url_id))
    }
}

#[async_trait]
impl Frontier for RedisFrontier {
    #[tracing::instrument(skip(self, entry), fields(url = %entry.url))]
    async fn submit(&self, entry: UrlEntry) -> Result<SubmitOutcome> {
        let started_at = Instant::now();
        let (shard, host, url_id) = self.shard_of(&entry).map_err(Error::from)?;
        let outcome = self
            .ops()
            .submit(shard, url_id, &entry, &host, now_ms())
            .await
            .map_err(RedisFrontierError::from)?;
        record_submit_outcome(outcome);
        metrics::histogram!(
            m::FRONTIER_CALL_SECONDS,
            "op" => m::OP_SUBMIT,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(outcome)
    }

    #[tracing::instrument(skip(self, entries), fields(n = entries.len()))]
    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<SubmitBatchOutcome> {
        let started_at = Instant::now();
        metrics::histogram!(m::FRONTIER_SUBMIT_BATCH_SIZE).record(entries.len() as f64);
        if entries.is_empty() {
            metrics::histogram!(
                m::FRONTIER_CALL_SECONDS,
                "op" => m::OP_SUBMIT_BATCH,
            )
            .record(started_at.elapsed().as_secs_f64());
            return Ok(SubmitBatchOutcome::default());
        }

        // Group entries by shard. Outbound URLs from a single page can
        // span multiple shards under HostHashShardPolicy, but the Lua
        // script touches one cluster slot per call, so we issue one
        // call per shard. In the common case (most outbound links share
        // the source page's host -> same shard) this collapses to one
        // call total.
        // Capacity bounded by min(owned_shards, entries): every entry
        // maps to at most one shard, and we can't have more shards in
        // the map than we own. Pre-sizing skips the 0->4->8 grow chain.
        let mut shard_to_indices: std::collections::HashMap<ShardKey, Vec<usize>> =
            std::collections::HashMap::with_capacity(self.owned_shards.len().min(entries.len()));
        let mut shard_hosts: Vec<String> = Vec::with_capacity(entries.len());
        let mut shard_url_ids: Vec<UrlId> = Vec::with_capacity(entries.len());
        for (i, entry) in entries.iter().enumerate() {
            let (shard, host, url_id) = self.shard_of(entry).map_err(Error::from)?;
            shard_hosts.push(host);
            shard_url_ids.push(url_id);
            shard_to_indices.entry(shard).or_default().push(i);
        }

        let now = now_ms();
        let mut total = SubmitBatchOutcome::default();
        for (shard, indices) in &shard_to_indices {
            let mut items: Vec<SubmitItem<'_>> = Vec::with_capacity(indices.len());
            for &i in indices {
                let host_str = shard_hosts[i].as_str();
                let max_urls = self
                    .crawl_scope
                    .max_urls_for(host_str)
                    .and_then(|cap| i64::try_from(cap).ok())
                    .unwrap_or(-1);
                items.push(SubmitItem {
                    url_id: shard_url_ids[i],
                    entry: &entries[i],
                    host: host_str,
                    max_urls,
                });
            }
            let (queued, rejected) = self
                .ops()
                .submit_batch(*shard, &items, now)
                .await
                .map_err(RedisFrontierError::from)?;
            // Surface per-URL bloom verdicts so the existing
            // `crawlrs_frontier_bloom_total{verdict=...}` panels stay
            // accurate. Quota-rejected URLs are not bloom events;
            // they're a separate operator concern surfaced on the
            // returned outcome (and metricised by the worker).
            for _ in 0..queued {
                record_submit_outcome(SubmitOutcome::Queued);
            }
            let bloom_dupes = items
                .len()
                .saturating_sub(queued)
                .saturating_sub(rejected);
            for _ in 0..bloom_dupes {
                record_submit_outcome(SubmitOutcome::SkippedDuplicate);
            }
            total.queued += queued;
            total.rejected += rejected;
        }

        metrics::histogram!(
            m::FRONTIER_CALL_SECONDS,
            "op" => m::OP_SUBMIT_BATCH,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(total)
    }

    #[tracing::instrument(skip(self), fields(identity = %identity))]
    async fn claim(&self, identity: &WorkerIdentity) -> Result<ClaimOutcome> {
        let started_at = Instant::now();
        let n = self.owned_shards.len();
        if n == 0 {
            return Ok(ClaimOutcome::Empty);
        }
        let start = self.claim_cursor.fetch_add(1, Ordering::Relaxed) % n;
        let lease_ms = self.lease_timeout.as_millis() as i64;

        let mut soonest_hint: Option<u64> = None;
        for offset in 0..n {
            let shard = self.owned_shards[(start + offset) % n];
            match self.ops().claim(shard, now_ms(), lease_ms).await {
                Ok(ClaimRaw::Claimed {
                    url_id,
                    entry,
                    host,
                }) => {
                    metrics::counter!(
                        m::FRONTIER_CLAIM_TOTAL,
                        "outcome" => m::OUTCOME_CLAIMED,
                    )
                    .increment(1);
                    metrics::histogram!(
                        m::FRONTIER_CALL_SECONDS,
                        "op" => m::OP_CLAIM,
                    )
                    .record(started_at.elapsed().as_secs_f64());
                    let attempt_id = encode_attempt(shard, &url_id, &host);
                    let _ = identity; // captured via tracing
                    return Ok(ClaimOutcome::Claimed {
                        url_id,
                        entry,
                        attempt_id,
                    });
                }
                Ok(ClaimRaw::EmptyHint { soonest_ms }) => {
                    soonest_hint = Some(match soonest_hint {
                        Some(prev) => prev.min(soonest_ms),
                        None => soonest_ms,
                    });
                }
                Ok(ClaimRaw::Empty) => {}
                Err(e) => {
                    metrics::counter!(
                        m::FRONTIER_CLAIM_TOTAL,
                        "outcome" => m::OUTCOME_ERROR,
                    )
                    .increment(1);
                    metrics::histogram!(
                        m::FRONTIER_CALL_SECONDS,
                        "op" => m::OP_CLAIM,
                    )
                    .record(started_at.elapsed().as_secs_f64());
                    return Err(RedisFrontierError::from(e).into());
                }
            }
        }
        metrics::histogram!(
            m::FRONTIER_CALL_SECONDS,
            "op" => m::OP_CLAIM,
        )
        .record(started_at.elapsed().as_secs_f64());
        if let Some(ms) = soonest_hint {
            metrics::counter!(
                m::FRONTIER_CLAIM_TOTAL,
                "outcome" => m::OUTCOME_EMPTY_HINT,
            )
            .increment(1);
            return Ok(ClaimOutcome::EmptyHint {
                sleep_until: ms_to_instant(ms),
            });
        }
        metrics::counter!(
            m::FRONTIER_CLAIM_TOTAL,
            "outcome" => m::OUTCOME_EMPTY,
        )
        .increment(1);
        Ok(ClaimOutcome::Empty)
    }

    #[tracing::instrument(skip(self))]
    async fn len(&self) -> Result<usize> {
        // Sum host_queue lengths across owned shards via Redis SCAN +
        // LLEN. Approximate (a SCAN-driven sum can race with concurrent
        // submits) but bounded enough for the queue-depth metric.
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| RedisFrontierError::Pool(format!("{e:?}")))
            .map_err(Error::from)?;
        let mut total = 0usize;
        for &shard in &self.owned_shards {
            let pattern = format!("{}*", self.keys.host_queue_prefix(shard));
            let mut cursor: u64 = 0;
            loop {
                let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(500)
                    .query_async(&mut *conn)
                    .await
                    .map_err(RedisFrontierError::from)
                    .map_err(Error::from)?;
                for key in &batch {
                    let n: i64 = redis::cmd("LLEN")
                        .arg(key)
                        .query_async(&mut *conn)
                        .await
                        .map_err(RedisFrontierError::from)
                        .map_err(Error::from)?;
                    total = total.saturating_add(n as usize);
                }
                if next == 0 {
                    break;
                }
                cursor = next;
            }
        }
        Ok(total)
    }

    #[tracing::instrument(skip(self), fields(host))]
    async fn advance_wake(&self, host: &str, until: Instant) -> Result<()> {
        let started_at = Instant::now();
        // Resolve which shard owns this host. The trait surface
        // doesn't carry a shard parameter; we apply to every shard
        // whose policy hashes this host onto an owned slot. With
        // single-shard scheduling per host, this is exactly one
        // shard.
        let shard = self.sharding_policy.shard_key_from_host(host);
        if !self.owned_shards.contains(&shard) {
            debug!(host, shard, "advance_wake on unowned shard; skipping");
            return Ok(());
        }
        let until_ms = instant_to_ms(until);
        self.ops()
            .advance_wake(shard, host, until_ms)
            .await
            .map_err(RedisFrontierError::from)?;
        metrics::histogram!(
            m::FRONTIER_CALL_SECONDS,
            "op" => m::OP_ADVANCE_WAKE,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(attempt = %attempt))]
    async fn ack(&self, attempt: &AttemptId) -> Result<()> {
        let started_at = Instant::now();
        let (shard, url_id, host) = decode_attempt(attempt).map_err(Error::from)?;
        if !self.owned_shards.contains(&shard) {
            warn!(shard, "ack on unowned shard; ignoring");
            return Ok(());
        }
        self.ops()
            .ack(shard, url_id, &host)
            .await
            .map_err(RedisFrontierError::from)?;
        metrics::histogram!(
            m::FRONTIER_CALL_SECONDS,
            "op" => m::OP_ACK,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn tick(&self) -> Result<usize> {
        let started_at = Instant::now();
        let (promoted, reclaimed) = promoter::tick_once(
            &self.ops(),
            &self.owned_shards,
            now_ms(),
            self.tick_batch_limit,
        )
        .await;
        if promoted > 0 {
            metrics::counter!(m::FRONTIER_PROMOTED_TOTAL).increment(promoted);
        }
        if reclaimed > 0 {
            metrics::counter!(
                m::FRONTIER_LEASE_RECLAIM_TOTAL,
                "reason" => m::RECLAIM_REASON_EXPIRED,
            )
            .increment(reclaimed);
        }
        metrics::histogram!(
            m::FRONTIER_CALL_SECONDS,
            "op" => m::OP_TICK,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok((promoted + reclaimed) as usize)
    }
}

fn record_submit_outcome(outcome: SubmitOutcome) {
    match outcome {
        SubmitOutcome::Queued => {
            metrics::counter!(
                m::FRONTIER_BLOOM_TOTAL,
                "verdict" => m::BLOOM_NEW,
            )
            .increment(1);
        }
        SubmitOutcome::SkippedDuplicate => {
            metrics::counter!(
                m::FRONTIER_BLOOM_TOTAL,
                "verdict" => m::BLOOM_DUPLICATE,
            )
            .increment(1);
        }
    }
}

/// Encode `(shard, url_id, host)` as the opaque [`AttemptId`] the
/// runtime carries through the pipeline. The host is folded in so
/// `ack` can compose the inflight ZSET member without a separate
/// lookup.
fn encode_attempt(shard: ShardKey, url_id: &UrlId, host: &str) -> AttemptId {
    AttemptId::new(format!("s{shard}|{}|{host}", url_id.to_hex()))
}

fn decode_attempt(attempt: &AttemptId) -> LocalResult<(ShardKey, UrlId, String)> {
    let raw = attempt.as_str();
    let mut parts = raw.splitn(3, '|');
    let shard_str = parts
        .next()
        .ok_or_else(|| RedisFrontierError::MalformedAttempt(raw.into()))?;
    let url_id_hex = parts
        .next()
        .ok_or_else(|| RedisFrontierError::MalformedAttempt(raw.into()))?;
    let host = parts
        .next()
        .ok_or_else(|| RedisFrontierError::MalformedAttempt(raw.into()))?;
    let shard: ShardKey = shard_str
        .strip_prefix('s')
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RedisFrontierError::MalformedAttempt(raw.into()))?;
    let url_id = UrlId::from_hex(url_id_hex)
        .ok_or_else(|| RedisFrontierError::MalformedAttempt(raw.into()))?;
    Ok((shard, url_id, host.to_string()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn instant_to_ms(when: Instant) -> i64 {
    let now_inst = Instant::now();
    let now_wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    if when <= now_inst {
        return now_wall;
    }
    let delta_ms = when.duration_since(now_inst).as_millis() as i64;
    now_wall.saturating_add(delta_ms)
}

fn ms_to_instant(target_wall_ms: u64) -> Instant {
    let now_inst = Instant::now();
    let now_wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    if target_wall_ms <= now_wall {
        return now_inst;
    }
    let delta_ms = target_wall_ms - now_wall;
    now_inst + Duration::from_millis(delta_ms)
}

#[cfg(test)]
mod tests {
    // Inline because: visibility-forced. The test exercises the
    // private free functions `encode_attempt` / `decode_attempt`,
    // which are the wire format for the opaque `AttemptId` the
    // runtime carries through the pipeline. `tests/*.rs` (a separate
    // crate) cannot reach them; we keep them private to avoid
    // committing to the encoding in the public API.

    use super::*;
    use crawlrs_core::CanonicalUrl;

    #[test]
    fn attempt_id_round_trips() {
        let url = CanonicalUrl::parse("https://example.com/foo").unwrap();
        let url_id = UrlId::from_canonical(&url);
        let encoded = encode_attempt(0, &url_id, "example.com");
        let (shard, decoded_id, host) = decode_attempt(&encoded).unwrap();
        assert_eq!(shard, 0);
        assert_eq!(decoded_id, url_id);
        assert_eq!(host, "example.com");
    }
}
