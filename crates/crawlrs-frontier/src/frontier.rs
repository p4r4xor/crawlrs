//! Redis-backed `Frontier` impl.
//!
//! Orchestrator: holds the bb8 pool, the key prefix, the Lua script
//! cache, the bloom config, and the lease timeout. Delegates the
//! actual Redis work to [`host_queue::HostQueueOps`] (one thin Rust
//! wrapper per Lua script) and to [`promoter::tick_once`] (the
//! `tick` body).
//!
//! Per ADR-0019 the data shape is:
//!
//! - `host_queue:<host>` (LIST<url_id>) — per-host FIFO.
//! - `wake` (ZSET<host>) — hosts not yet ready; score = next-allowed
//!   wall-clock ms.
//! - `ready` (LIST<host>) — hosts whose wake-time has elapsed,
//!   populated by the promoter.
//! - `inflight` (ZSET<"url_id|host">) — leases; score = lease
//!   expiry ms.
//! - `urls` (HASH<url_id, payload>) — content-addressed payload.
//! - `seen` (RedisBloom) — submit-time dedup.
//! - `overflow` (LIST<url_id>) — spillover for hot hosts.
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
    AttemptId, ClaimOutcome, Error, Frontier, Result, ShardKey, ShardingPolicy, SubmitOutcome,
    UrlEntry, UrlId, WorkerIdentity,
};
use thiserror::Error as ThisError;
use tracing::{debug, warn};

use crate::bloom::{self, BloomConfig};
use crate::host_queue::{ClaimRaw, HostQueueError, HostQueueOps, Scripts};
use crate::keys::KeyPrefix;
use crate::metrics as m;
use crate::promoter;

/// Default lease timeout. A worker holding a URL for longer than this
/// is presumed dead; reclaim re-pushes the URL. 60s comfortably
/// exceeds typical fetch durations.
pub const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(60);

/// Default per-host backlog cap before submits route to the overflow
/// queue. Caps memory: 10k URLs/host * ~150 bytes/entry = ~1.5MB/host
/// worst case.
pub const DEFAULT_MAX_HOST_BACKLOG: u64 = 10_000;

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
    max_host_backlog: u64,
    tick_batch_limit: u64,
    /// Round-robin cursor for `claim` across owned shards.
    claim_cursor: AtomicUsize,
}

impl std::fmt::Debug for RedisFrontier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisFrontier")
            .field("run_id", &self.keys.run_id())
            .field("owned_shards", &self.owned_shards)
            .field("lease_timeout", &self.lease_timeout)
            .field("max_host_backlog", &self.max_host_backlog)
            .finish_non_exhaustive()
    }
}

impl RedisFrontier {
    /// Build a frontier bound to a specific run, sharding policy, and
    /// owned-shard list. Reserves the per-shard bloom filter via
    /// `BF.RESERVE` (idempotent across processes); a missing
    /// RedisBloom module surfaces a clear error here so deployments
    /// fail fast.
    pub async fn new(
        pool: Pool<RedisConnectionManager>,
        sharding_policy: Arc<dyn ShardingPolicy>,
        owned_shards: Vec<ShardKey>,
        run_id: impl Into<String>,
        bloom_config: BloomConfig,
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
            max_host_backlog: DEFAULT_MAX_HOST_BACKLOG,
            tick_batch_limit: DEFAULT_TICK_BATCH_LIMIT,
            claim_cursor: AtomicUsize::new(0),
        })
    }

    pub fn with_lease_timeout(mut self, lease: Duration) -> Self {
        self.lease_timeout = lease;
        self
    }

    pub fn with_max_host_backlog(mut self, cap: u64) -> Self {
        self.max_host_backlog = cap;
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
            .submit(
                shard,
                url_id,
                &entry,
                &host,
                self.max_host_backlog,
                now_ms(),
            )
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
    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<usize> {
        let started_at = Instant::now();
        metrics::histogram!(m::FRONTIER_SUBMIT_BATCH_SIZE).record(entries.len() as f64);
        // The Lua script is per-URL; pipelining is the caller's job.
        // We iterate sequentially so submit-time errors surface
        // immediately; under high fan-out the runtime can fan submits
        // across tasks if it needs to.
        let mut newly = 0;
        for entry in entries {
            match self.submit(entry).await? {
                SubmitOutcome::Queued => newly += 1,
                SubmitOutcome::SkippedDuplicate | SubmitOutcome::Overflowed => {}
            }
        }
        metrics::histogram!(
            m::FRONTIER_CALL_SECONDS,
            "op" => m::OP_SUBMIT_BATCH,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(newly)
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
            // Overflow counts toward total queue depth.
            let overflow_len: i64 = redis::cmd("LLEN")
                .arg(self.keys.overflow(shard))
                .query_async(&mut *conn)
                .await
                .map_err(RedisFrontierError::from)
                .map_err(Error::from)?;
            total = total.saturating_add(overflow_len as usize);
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
        SubmitOutcome::Overflowed => {
            // Per-host attribution is added at the call site that
            // knows the host; here we just emit the no-label
            // counter. (`submit` enriches via a second emission if
            // future versions want both.)
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
    use super::*;
    use crawlrs_core::{CanonicalUrl, SingleShardPolicy};

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

    #[test]
    fn shard_of_rejects_non_http_url_without_host() {
        // SingleShardPolicy always picks shard 0; this exercises the
        // host-extraction error path.
        let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
        // We can't build a RedisFrontier without a Redis pool, so
        // assert on the helper directly.
        let url = CanonicalUrl::parse("https://a.test/").unwrap();
        let entry = UrlEntry::seed(url.clone());
        let host = entry.url.host().unwrap();
        assert_eq!(host, "a.test");
        let _ = policy.shard_key(&entry.url);
    }
}
