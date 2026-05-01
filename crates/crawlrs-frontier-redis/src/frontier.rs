//! Redis-backed `Frontier` impl. See module-level docs in `lib.rs` for the
//! design overview.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{CanonicalUrl, Error, Frontier, Result, ShardKey, ShardingPolicy, UrlEntry};
use redis::AsyncCommands;
use redis::streams::{
    StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadOptions, StreamReadReply,
};
use thiserror::Error as ThisError;
use tracing::{debug, info, warn};

use crate::claims::{ClaimRecord, PendingClaims, StreamEntryId};
use crate::codec::{self, STREAM_FIELD_BODY};
use crate::keys::KeyPrefix;

/// Stranded URLs (claimed but unacked beyond this idle window) become
/// candidates for `XAUTOCLAIM` reclaim by another worker. Five minutes
/// matches the production baseline in ADR-0007.
pub const DEFAULT_AUTOCLAIM_IDLE: Duration = Duration::from_secs(300);

/// Atomic SADD-then-XADD on the shard's seen-set and queue. See
/// `scripts/submit.lua` for semantics.
const SUBMIT_LUA: &str = include_str!("scripts/submit.lua");

/// Internal error type. All variants convert to [`crawlrs_core::Error`]
/// via `From`, so the public trait surface stays in core's error type.
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

    #[error("stream entry missing required field `{0}`")]
    MissingField(&'static str),

    #[error("codec: {0}")]
    Codec(String),
}

impl From<RedisFrontierError> for Error {
    fn from(e: RedisFrontierError) -> Self {
        Error::Frontier(e.to_string())
    }
}

impl<E: std::fmt::Debug> From<bb8::RunError<E>> for RedisFrontierError {
    fn from(e: bb8::RunError<E>) -> Self {
        RedisFrontierError::Pool(format!("{e:?}"))
    }
}

type LocalResult<T> = std::result::Result<T, RedisFrontierError>;

/// A `Frontier` impl backed by a Redis instance, with one keyspace per
/// owned shard and at-least-once delivery via Redis Streams consumer
/// groups.
///
/// Construct via [`RedisFrontier::new`]; build the bb8 pool yourself
/// and hand it in so the caller controls connection topology and
/// timeouts.
pub struct RedisFrontier {
    pool: Pool<RedisConnectionManager>,
    keys: KeyPrefix,
    sharding_policy: Arc<dyn ShardingPolicy>,
    owned_shards: Vec<ShardKey>,
    consumer_id: String,
    claims: Arc<PendingClaims>,

    /// `XAUTOCLAIM` minimum-idle-time. Defaults to
    /// `DEFAULT_AUTOCLAIM_IDLE`; override via builder for tests
    /// that need to trigger reclaim instantly.
    autoclaim_idle: Duration,

    /// Round-robin cursor for `claim` across owned shards.
    claim_cursor: AtomicUsize,
}

impl std::fmt::Debug for RedisFrontier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisFrontier")
            .field("run_id", &self.keys.run_id())
            .field("owned_shards", &self.owned_shards)
            .field("consumer_id", &self.consumer_id)
            .field("autoclaim_idle", &self.autoclaim_idle)
            .field("pending_claims", &self.claims.len())
            .finish()
    }
}

impl RedisFrontier {
    /// Build a frontier bound to a specific run, sharding policy, and
    /// owned-shard list.
    ///
    /// On construction, this:
    /// 1. Validates each owned shard against `sharding_policy.shard_count()`.
    /// 2. Ensures the consumer group exists for each owned shard's
    ///    queue stream (idempotent: `BUSYGROUP` is treated as success).
    /// 3. Generates a unique consumer id for this instance.
    ///
    /// Pool, ack semantics, and key naming are described on the
    /// individual methods.
    pub async fn new(
        pool: Pool<RedisConnectionManager>,
        sharding_policy: Arc<dyn ShardingPolicy>,
        owned_shards: Vec<ShardKey>,
        run_id: impl Into<String>,
    ) -> Result<Self> {
        let keys = KeyPrefix::new(run_id);
        let _span = tracing::info_span!(
            "RedisFrontier::new",
            run_id = keys.run_id(),
            num_shards = sharding_policy.shard_count(),
            owned = ?owned_shards,
        )
        .entered();

        // Validate shard ownership.
        let count = sharding_policy.shard_count();
        for &shard in &owned_shards {
            if shard >= count {
                return Err(RedisFrontierError::ShardOutOfRange { got: shard, count }.into());
            }
        }

        let consumer_id = cuid2::create_id();

        // Ensure consumer group exists for each owned shard.
        ensure_consumer_groups(&pool, &keys, &owned_shards).await?;

        info!(
            run_id = keys.run_id(),
            consumer_id = %consumer_id,
            owned_shards = ?owned_shards,
            "RedisFrontier ready",
        );

        Ok(Self {
            pool,
            keys,
            sharding_policy,
            owned_shards,
            consumer_id,
            claims: Arc::new(PendingClaims::new()),
            autoclaim_idle: DEFAULT_AUTOCLAIM_IDLE,
            claim_cursor: AtomicUsize::new(0),
        })
    }

    /// Override the `XAUTOCLAIM` minimum-idle-time. Tests use this to
    /// reclaim entries immediately rather than waiting 5 minutes.
    pub fn with_autoclaim_idle(mut self, idle: Duration) -> Self {
        self.autoclaim_idle = idle;
        self
    }

    /// Number of URLs currently in-flight on this frontier instance.
    /// Useful as a metric and for shutdown drain checks.
    pub fn pending_claims_count(&self) -> usize {
        self.claims.len()
    }

    /// Snapshot of the bb8 pool's state (size, idle, in-use).
    pub fn pool_state(&self) -> bb8::State {
        self.pool.state()
    }

    /// `XLEN` per owned shard. Approximate; an entry sitting in a
    /// consumer's PEL still counts (which is the right semantic for
    /// "work remaining").
    pub async fn shard_depths(&self) -> Result<HashMap<ShardKey, usize>> {
        let mut depths = HashMap::with_capacity(self.owned_shards.len());
        let mut conn = self.checkout().await?;
        for &shard in &self.owned_shards {
            let key = self.keys.queue(shard);
            let len: usize = conn.xlen(&key).await.map_err(RedisFrontierError::from)?;
            depths.insert(shard, len);
        }
        Ok(depths)
    }

    /// Reassign stranded entries (idle > `autoclaim_idle`) from
    /// other consumers to this one. Returns the total count reclaimed
    /// across all owned shards.
    ///
    /// The runtime should drive this on a regular cadence (e.g. every
    /// 30s) and once during graceful shutdown to drain pending work
    /// from a dying peer. Reclaimed entries are inserted into this
    /// frontier's claims map and become visible on the next `claim`
    /// (which reads the consumer's PEL first).
    #[tracing::instrument(skip(self))]
    pub async fn tick_autoclaim(&self) -> Result<usize> {
        let mut total = 0_usize;
        let group = self.keys.consumer_group();

        for &shard in &self.owned_shards {
            let queue_key = self.keys.queue(shard);
            let mut conn = self.checkout().await?;

            // XAUTOCLAIM <key> <group> <consumer> <min-idle-ms> <start>
            // Start at "0-0" to walk all stranded entries in this call.
            let reply: StreamAutoClaimReply = conn
                .xautoclaim_options(
                    &queue_key,
                    group,
                    &self.consumer_id,
                    self.autoclaim_idle.as_millis() as u64,
                    "0-0",
                    StreamAutoClaimOptions::default(),
                )
                .await
                .map_err(RedisFrontierError::from)?;

            let reclaimed = reply.claimed.len();
            if reclaimed > 0 {
                warn!(
                    shard,
                    reclaimed,
                    deleted = reply.deleted_ids.len(),
                    "xautoclaim reclaimed stranded entries",
                );
                for stream_id in reply.claimed {
                    if let Some(entry) = decode_stream_entry(&stream_id)? {
                        self.claims
                            .record(entry.url.clone(), shard, stream_id.id.clone());
                    }
                }
            }
            total += reclaimed;
        }
        Ok(total)
    }

    // --- internals -----------------------------------------------------

    async fn checkout(&self) -> LocalResult<bb8::PooledConnection<'_, RedisConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| RedisFrontierError::Pool(e.to_string()))
    }

    fn assert_owned(&self, shard: ShardKey) -> LocalResult<()> {
        if !self.owned_shards.contains(&shard) {
            return Err(RedisFrontierError::ShardNotOwned {
                got: shard,
                owned: self.owned_shards.clone(),
            });
        }
        Ok(())
    }

    /// Round-robin order across owned shards, starting from the next
    /// position the cursor points at. Yields each shard once.
    fn shard_visit_order(&self) -> Vec<ShardKey> {
        let n = self.owned_shards.len();
        if n == 0 {
            return Vec::new();
        }
        let start = self.claim_cursor.fetch_add(1, Ordering::Relaxed) % n;
        let mut order = Vec::with_capacity(n);
        for offset in 0..n {
            order.push(self.owned_shards[(start + offset) % n]);
        }
        order
    }

    async fn submit_one(&self, entry: &UrlEntry) -> LocalResult<bool> {
        let shard = self.sharding_policy.shard_key(&entry.url);
        self.assert_owned(shard)?;

        let body = codec::encode(entry).map_err(|e| RedisFrontierError::Codec(e.to_string()))?;
        let seen_key = self.keys.seen(shard);
        let queue_key = self.keys.queue(shard);

        let mut conn = self.checkout().await?;

        // EVAL keeps wire-overhead modest at this script size and
        // sidesteps the NOSCRIPT recovery dance. If we ever measure
        // EVAL as a bottleneck we'll switch to EVALSHA + reload-on-NOSCRIPT.
        let result: i64 = redis::cmd("EVAL")
            .arg(SUBMIT_LUA)
            .arg(2)
            .arg(&seen_key)
            .arg(&queue_key)
            .arg(entry.url.as_str())
            .arg(&body)
            .query_async(&mut *conn)
            .await
            .map_err(RedisFrontierError::from)?;

        debug!(
            shard,
            url = entry.url.as_str(),
            newly = result == 1,
            "submit"
        );
        Ok(result == 1)
    }

    async fn claim_pel_then_new(
        &self,
        shard: ShardKey,
    ) -> LocalResult<Option<(UrlEntry, StreamEntryId)>> {
        let queue_key = self.keys.queue(shard);
        let group = self.keys.consumer_group();
        let mut conn = self.checkout().await?;

        // Step 1: read this consumer's PEL (id "0"). Picks up entries
        // that XAUTOCLAIM moved into our PEL or that we previously
        // claimed but haven't acked (e.g. after a process restart).
        let pel_opts = StreamReadOptions::default()
            .group(group, &self.consumer_id)
            .count(1);
        let pel: StreamReadReply = conn
            .xread_options(&[&queue_key], &["0"], &pel_opts)
            .await
            .map_err(RedisFrontierError::from)?;

        if let Some((entry, id)) = first_entry(&pel)? {
            return Ok(Some((entry, id)));
        }

        // Step 2: PEL empty, read new entries (id ">"). Non-blocking
        // so the caller can round-robin to the next shard.
        let new_opts = StreamReadOptions::default()
            .group(group, &self.consumer_id)
            .count(1);
        let new: StreamReadReply = conn
            .xread_options(&[&queue_key], &[">"], &new_opts)
            .await
            .map_err(RedisFrontierError::from)?;

        first_entry(&new)
    }
}

#[async_trait]
impl Frontier for RedisFrontier {
    #[tracing::instrument(skip(self, entry), fields(url = %entry.url))]
    async fn submit(&self, entry: UrlEntry) -> Result<bool> {
        Ok(self.submit_one(&entry).await?)
    }

    #[tracing::instrument(skip(self, entries), fields(n = entries.len()))]
    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<usize> {
        // v1: loop. Bulk-submit-per-shard via pipelining is a perf
        // optimisation we'll add when measured; correctness-wise this
        // is identical to the pipelined version.
        let mut newly = 0;
        for entry in &entries {
            if self.submit_one(entry).await? {
                newly += 1;
            }
        }
        Ok(newly)
    }

    #[tracing::instrument(skip(self))]
    async fn claim(&self) -> Result<Option<UrlEntry>> {
        for shard in self.shard_visit_order() {
            if let Some((entry, entry_id)) = self.claim_pel_then_new(shard).await? {
                self.claims.record(entry.url.clone(), shard, entry_id);
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    #[tracing::instrument(skip(self), fields(max))]
    async fn claim_batch(&self, max: usize) -> Result<Vec<UrlEntry>> {
        let mut out = Vec::with_capacity(max.min(64));
        // Walk shards once; within each shard, drain until empty or
        // until we hit `max`. This keeps locality (consecutive entries
        // from the same shard come together) without starving other
        // shards within a single call.
        for shard in self.shard_visit_order() {
            while out.len() < max {
                match self.claim_pel_then_new(shard).await? {
                    Some((entry, entry_id)) => {
                        self.claims.record(entry.url.clone(), shard, entry_id);
                        out.push(entry);
                    }
                    None => break,
                }
            }
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }

    #[tracing::instrument(skip(self))]
    async fn len(&self) -> Result<usize> {
        let depths = self.shard_depths().await?;
        Ok(depths.values().sum())
    }

    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn ack(&self, url: &CanonicalUrl) -> Result<()> {
        let Some(ClaimRecord { shard, entry_id }) = self.claims.take(url) else {
            // Not in-flight: idempotent no-op.
            debug!(url = url.as_str(), "ack: url not in claims map; no-op");
            return Ok(());
        };
        let queue_key = self.keys.queue(shard);
        let group = self.keys.consumer_group();
        let mut conn = self.checkout().await.map_err(Error::from)?;
        let _: i64 = redis::cmd("XACK")
            .arg(&queue_key)
            .arg(group)
            .arg(&entry_id)
            .query_async(&mut *conn)
            .await
            .map_err(RedisFrontierError::from)
            .map_err(Error::from)?;
        debug!(shard, url = url.as_str(), entry_id, "ack");
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn nack(&self, url: &CanonicalUrl) -> Result<()> {
        // We drop our local tracking but leave the entry in the
        // consumer PEL on Redis. The natural recovery is via
        // `tick_autoclaim` once the entry is idle past
        // `autoclaim_idle`. Keeping nack synchronous and side-effect-
        // light avoids races where a hot loop re-claims an entry
        // before the original handler has fully released it.
        let _ = self.claims.take(url);
        debug!(url = url.as_str(), "nack");
        Ok(())
    }

    /// Bridges the trait's periodic-maintenance hook to this impl's
    /// `XAUTOCLAIM` reclaim. Returns the count reclaimed across all
    /// owned shards.
    async fn tick(&self) -> Result<usize> {
        self.tick_autoclaim().await
    }
}

// --- module-private helpers --------------------------------------------

async fn ensure_consumer_groups(
    pool: &Pool<RedisConnectionManager>,
    keys: &KeyPrefix,
    owned_shards: &[ShardKey],
) -> Result<()> {
    for &shard in owned_shards {
        let queue_key = keys.queue(shard);
        let group = keys.consumer_group();
        let mut conn = pool
            .get()
            .await
            .map_err(|e| RedisFrontierError::Pool(e.to_string()))?;

        // XGROUP CREATE <key> <group> $ MKSTREAM
        // BUSYGROUP error means the group already exists; treat as success.
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&queue_key)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut *conn)
            .await;

        match result {
            Ok(()) => debug!(shard, "created consumer group"),
            Err(e) if e.code() == Some("BUSYGROUP") => {
                debug!(shard, "consumer group already exists");
            }
            Err(e) => return Err(RedisFrontierError::from(e).into()),
        }
    }
    Ok(())
}

fn first_entry(reply: &StreamReadReply) -> LocalResult<Option<(UrlEntry, StreamEntryId)>> {
    for stream_key in &reply.keys {
        for stream_id in &stream_key.ids {
            if let Some(entry) = decode_stream_entry(stream_id)? {
                return Ok(Some((entry, stream_id.id.clone())));
            }
        }
    }
    Ok(None)
}

fn decode_stream_entry(stream_id: &redis::streams::StreamId) -> LocalResult<Option<UrlEntry>> {
    let raw = stream_id
        .map
        .get(STREAM_FIELD_BODY)
        .ok_or(RedisFrontierError::MissingField(STREAM_FIELD_BODY))?;

    // Stream entry values arrive as bulk strings; pull the bytes out
    // directly rather than via FromRedisValue (whose error type
    // changed shape in redis 1.x and which would otherwise force a
    // clone).
    let bytes: &[u8] = match raw {
        redis::Value::BulkString(b) => b.as_slice(),
        other => {
            return Err(RedisFrontierError::Codec(format!(
                "expected BulkString in stream field `{STREAM_FIELD_BODY}`, got {other:?}"
            )));
        }
    };
    let entry = codec::decode(bytes).map_err(|e| RedisFrontierError::Codec(e.to_string()))?;
    Ok(Some(entry))
}
