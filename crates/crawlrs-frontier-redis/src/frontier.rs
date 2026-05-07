//! Redis-backed `Frontier` impl.
//!
//! # Mental model in 60 seconds
//!
//! The frontier is a **durable URL queue** with at-least-once delivery.
//! It uses three Redis structures per shard, scoped under a `run_id`:
//!
//! - **`crawlrs:{run}:s{N}:queue`** - a Redis Stream. Each entry is one
//!   URL (postcard-encoded). Entries flow in via `XADD` and are
//!   delivered to workers via `XREADGROUP`.
//! - **`crawlrs:{run}:s{N}:seen`** - a Redis Set of URL strings already
//!   submitted. Submit-time dedup; checked atomically with the `XADD`
//!   in one Lua script (`scripts/batch_submit.lua`).
//! - **A consumer group** (`fetchers`) on the queue Stream. Each
//!   *worker* is its own consumer within that group, named after the
//!   worker's stable [`WorkerIdentity`] (`pod-<ordinal>:<index>`). Each
//!   consumer has a private **Pending Entries List (PEL)**: entries it
//!   has been delivered but hasn't acked yet.
//!
//! Crucially, **the consumer name is stable across process restarts**.
//! When a pod restarts with the same StatefulSet ordinal, its workers
//! re-attach to the same consumer names and pick up their own PEL
//! entries via tier-1 (`XREADGROUP id "0"`) immediately, without
//! waiting for the `XAUTOCLAIM` idle threshold. A shared base name
//! across workers in one pod would let a peer worker read another
//! worker's pending entries via id `"0"`, which produces double-claims;
//! per-(pod, worker-index) consumer names give each worker a private
//! PEL while keeping recovery instantaneous.
//!
//! # Production: cold start
//!
//! 1. Process boots. Wiring code constructs `bb8::Pool` against Redis,
//!    then `RedisFrontier::new(pool, HostHashShardPolicy(8),
//!    [0..8], run_id)`.
//! 2. The constructor:
//!    - Validates each owned shard is in range.
//!    - For each owned shard, runs `XGROUP CREATE
//!      crawlrs:{run}:s{N}:queue fetchers $ MKSTREAM`. Creates the
//!      Stream and the consumer group atomically. If the group
//!      already exists (from a prior process for the same `run_id`),
//!      Redis returns BUSYGROUP - we treat that as success.
//! 3. Caller spawns N worker tasks, each holding the same
//!    `Arc<dyn Frontier>` and its own [`WorkerIdentity`]
//!    (`pod-<ordinal>:0` .. `pod-<ordinal>:N-1`). Identity is passed
//!    on every `claim` so each worker acts as its own Redis Streams
//!    consumer.
//! 4. First `claim()` from any worker:
//!    - Walks owned shards in round-robin order.
//!    - For each shard, the three-tier ladder:
//!      1. `XREADGROUP id "0"` - own PEL. Empty: this worker has
//!         never claimed anything.
//!      2. `XREADGROUP id ">"` - new entries. Empty: nothing in the
//!         Stream yet.
//!      3. `reclaim_one(shard)` - `XAUTOCLAIM` with 5-minute
//!         (production) idle threshold. Empty: no stranded entries.
//!    - All shards return `None`. Worker sleeps `empty_queue_poll`,
//!      backing off exponentially up to `max_idle_sleep`.
//! 5. Caller calls `submit(seed_url)`. The Lua script atomically
//!    `SADD`s to the seen-set and `XADD`s to the queue Stream of the
//!    URL's home shard.
//! 6. On the next claim cycle, one worker visits the right shard,
//!    `XREADGROUP id ">"` returns the URL. Atomically: the entry
//!    moves into that worker's PEL. The worker processes the URL
//!    (politeness gate, fetch, parse, store, …). On success, calls
//!    `ack(url)` → `XACK queue group entry-id`. The entry leaves
//!    the PEL.
//!
//! # Production: steady state
//!
//! Imagine 24 workers across 3 processes (8 workers per process, each
//! process owning 8 shards via `HostHashShardPolicy(8)`). The Stream
//! is hot - submits arrive from `submit_discovered` calls in worker
//! pipelines, claims drain entries.
//!
//! - **Each worker's claim path** is the three-tier ladder above:
//!   1. Read its own PEL first (entries it nacked, or that survived
//!      a process restart and were re-delivered to the same task id -
//!      rare, but the read is cheap). Almost always empty in steady
//!      state.
//!   2. Read new entries via `>`. Most claims succeed here. Redis
//!      atomically delivers each entry to exactly one consumer.
//!   3. If `>` is empty too, try `XAUTOCLAIM` to steal one stranded
//!      entry from any peer consumer that's been idle ≥ 5 minutes.
//!      Rare in healthy operation; this is the safety net for dying
//!      workers / partial network failures.
//!
//! - **Acks** flow per-URL: `XACK queue group entry-id` removes the
//!   entry from the PEL. The runtime hits `ack()` after a successful
//!   fetch+store, or `nack()` after a transient failure.
//!
//! - **Nacks are local-only**: we drop the worker's in-memory claim
//!   tracking but leave the entry in its Redis-side PEL. The worker
//!   re-reads it via tier 1 on the next claim cycle. If the worker
//!   dies before it can come back, after 5 minutes any peer worker
//!   will steal it via tier 3.
//!
//! - **Stranded recovery is automatic**. A worker that crashes
//!   mid-process leaves entries in its PEL forever - until a peer's
//!   tier 3 `XAUTOCLAIM` picks them up. No separate maintenance
//!   process needed.
//!
//! - **Submit-time dedup** stays correct across runs of `submit_batch`
//!   because the Lua script `SADD`s and `XADD`s atomically per chunk
//!   (chunks of 1000 URLs, parallel across shards). A URL already in
//!   the seen-set is silently dropped - no duplicate Stream entry.
//!
//! - **Discovery growth** is bounded by `max_queue_depth` (passed to
//!   the Lua script as `XADD MAXLEN ~ N`). Without it, an explosion
//!   of discovered links would blow Redis memory.
//!
//! Operational signals to watch:
//!
//! - `claim_count()` - in-flight URLs in this process. Should hover
//!   near `worker count` in steady state.
//! - `shard_depths()` - `XLEN` per owned shard. Tells you which
//!   shards are hot.
//! - `XPENDING <queue> <group>` (run via `redis-cli`) - total
//!   pending entries across all consumers. Steady state ≈ N workers
//!   in flight; ballooning means workers are stuck.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    AttemptId, ClaimedMessage, Error, Frontier, Result, ShardKey, ShardingPolicy, UrlEntry,
    WorkerIdentity,
};
use redis::AsyncCommands;
use redis::streams::{
    StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadOptions, StreamReadReply,
};
use thiserror::Error as ThisError;
use tracing::{debug, info, warn};

use crate::codec::{self, STREAM_FIELD_BODY};
use crate::keys::KeyPrefix;

type StreamEntryId = String;

/// Encode the (shard, redis-stream-entry-id) pair into the opaque
/// `AttemptId` token that flows through the runtime. Format:
/// `"<shard>|<entry-id>"`. The runtime treats this as opaque; only the
/// Redis impl parses it back to route XACK to the right stream.
fn encode_attempt(shard: ShardKey, entry_id: &str) -> AttemptId {
    AttemptId::new(format!("{shard}|{entry_id}"))
}

/// Inverse of [`encode_attempt`]. Returns the shard plus the
/// stream-entry-id substring (no allocation beyond the caller's owned
/// `AttemptId`).
fn decode_attempt(attempt: &AttemptId) -> LocalResult<(ShardKey, &str)> {
    let raw = attempt.as_str();
    let (shard_str, entry_id) = raw
        .split_once('|')
        .ok_or_else(|| RedisFrontierError::Codec(format!("malformed AttemptId: {raw}")))?;
    let shard: ShardKey = shard_str
        .parse()
        .map_err(|_| RedisFrontierError::Codec(format!("malformed shard in AttemptId: {raw}")))?;
    Ok((shard, entry_id))
}

/// Stranded URLs (claimed but unacked beyond this idle window) become
/// candidates for `XAUTOCLAIM` reclaim by another worker. Five minutes
/// is the production baseline: long enough that a healthy worker on
/// the slow tail of its fetch budget keeps ownership, short enough
/// that a crashed worker's URLs return to circulation within a small
/// number of fetch latencies.
pub const DEFAULT_AUTOCLAIM_IDLE: Duration = Duration::from_secs(300);

/// Approximate per-shard `XADD MAXLEN ~ N` cap. `0` disables trimming.
/// Default is uncapped to preserve current behavior; operators wiring
/// a long-running crawl should set this to bound discovery growth.
pub const DEFAULT_MAX_QUEUE_DEPTH: u64 = 0;

/// Atomic SADD-then-XADD across many URLs on one shard. See
/// `scripts/batch_submit.lua` for semantics. Used by both the
/// singular `submit` path (chunk of 1) and `submit_batch`.
const BATCH_SUBMIT_LUA: &str = include_str!("scripts/batch_submit.lua");

/// Max URLs per `EVAL` of `batch_submit.lua`. Bounded so a single Lua
/// script execution doesn't lock Redis's main thread for too long
/// even on hot shards. At 1000 URLs the script runs ~1-5 ms; safe in
/// shared environments and brings 1M-URL submit from ~17 minutes to
/// ~1 second on localhost.
const SUBMIT_BATCH_CHUNK: usize = 1000;

/// Maximum retry attempts for transient Redis errors (LOADING, BUSY,
/// TRYAGAIN, MASTERDOWN). After this many fails we surface the error
/// to the caller and let the worker's outer error_backoff take over.
const TRANSIENT_RETRY_ATTEMPTS: u32 = 3;

/// Initial delay for the first transient-error retry; doubles each
/// attempt. 50ms is short enough that a brief Redis stall is recovered
/// within a worker iteration, long enough to avoid hammering a Redis
/// that's mid-startup.
const TRANSIENT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(50);

/// Classify a redis-rs error for retry behavior. We use redis-rs's own
/// `retry_method()` so the policy stays aligned with what the upstream
/// crate considers transient.
///
/// - `WaitAndRetry`: Redis says "try later" (LOADING, BUSY, TRYAGAIN,
///   MASTERDOWN). We back off and retry up to `TRANSIENT_RETRY_ATTEMPTS`.
/// - `RetryImmediately`: rare, treated the same as WaitAndRetry but
///   without the initial sleep (delay starts at 0).
/// - Anything else (NoRetry, Reconnect, redirect kinds): not our
///   problem to retry; bubble out so callers handle it.
fn is_transient(err: &redis::RedisError) -> bool {
    matches!(
        err.retry_method(),
        redis::RetryMethod::WaitAndRetry | redis::RetryMethod::RetryImmediately
    )
}

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

    /// `XAUTOCLAIM` minimum-idle-time. Workers self-rebalance by
    /// stealing entries idle for at least this long from peer
    /// consumers; the value is the safety net that prevents healthy
    /// workers' in-flight entries from being stolen mid-process. Five
    /// minutes is the production default; tests use ~50 ms.
    autoclaim_idle: Duration,

    /// `XADD MAXLEN ~ N` cap per shard. `0` disables trimming. When
    /// trimming kicks in, OLDEST stream entries are dropped; the
    /// seen-set still remembers their URLs so they won't be
    /// re-enqueued, meaning dropped URLs are abandoned for this run.
    /// Operator picks the cap balancing memory budget vs. coverage.
    max_queue_depth: u64,

    /// Round-robin cursor for `claim` across owned shards.
    claim_cursor: AtomicUsize,

    /// Best-effort in-flight count for metrics/operability. Increments
    /// on every successful claim; decrements on every ack/nack call.
    /// Authoritative truth lives in Redis (`XPENDING`).
    in_flight: AtomicUsize,
}

impl std::fmt::Debug for RedisFrontier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisFrontier")
            .field("run_id", &self.keys.run_id())
            .field("owned_shards", &self.owned_shards)
            .field("autoclaim_idle", &self.autoclaim_idle)
            .field("in_flight", &self.in_flight.load(Ordering::Relaxed))
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
    ///
    /// Worker identity is supplied per call (`claim`/`claim_batch`),
    /// so a single `RedisFrontier` instance backs every worker in the
    /// pod. Consumer names are derived from the caller's
    /// [`WorkerIdentity`] and are stable across process restarts.
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

        // Ensure consumer group exists for each owned shard.
        ensure_consumer_groups(&pool, &keys, &owned_shards).await?;

        info!(
            run_id = keys.run_id(),
            owned_shards = ?owned_shards,
            "RedisFrontier ready",
        );

        Ok(Self {
            pool,
            keys,
            sharding_policy,
            owned_shards,
            autoclaim_idle: DEFAULT_AUTOCLAIM_IDLE,
            max_queue_depth: DEFAULT_MAX_QUEUE_DEPTH,
            claim_cursor: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
        })
    }

    /// Render `identity` as the Redis Streams consumer name. Stable
    /// across process restarts: the same `(pod_ordinal, worker_index)`
    /// pair always yields the same string, so tier-1 PEL replay
    /// reattaches to the worker's own previous in-flight entries
    /// without waiting for the `XAUTOCLAIM` idle threshold.
    ///
    /// The format is pinned here, in the adapter that owns the wire
    /// contract; do NOT reach for `WorkerIdentity::Display` at call
    /// sites. Display is a domain-level rendering (suitable for logs
    /// and human output); coupling the Redis consumer name to it
    /// would mean a future log-format change silently breaks PEL
    /// replay against an existing Redis dataset.
    fn consumer_name(identity: &WorkerIdentity) -> String {
        format!("pod-{}:{}", identity.pod_ordinal, identity.worker_index)
    }

    /// Override the per-shard `XADD MAXLEN ~ N` cap. Pass `0` to
    /// disable. Bounded queues prevent unbounded discovery growth from
    /// OOMing Redis; the trade-off is that a flood of newly-discovered
    /// URLs can push older ones out before they're claimed.
    pub fn with_max_queue_depth(mut self, depth: u64) -> Self {
        self.max_queue_depth = depth;
        self
    }

    /// Override the `XAUTOCLAIM` minimum-idle-time. Tests use this to
    /// reclaim entries immediately rather than waiting 5 minutes.
    pub fn with_autoclaim_idle(mut self, idle: Duration) -> Self {
        self.autoclaim_idle = idle;
        self
    }

    /// Best-effort count of URLs currently in-flight on this frontier
    /// instance: incremented on every successful claim and decremented
    /// on every ack/nack call. Useful for shutdown drain checks and
    /// local sanity assertions in tests; the authoritative truth lives
    /// in Redis (`XPENDING <queue> <group>`).
    pub fn claim_count(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
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

    /// Refresh the periodic frontier gauges:
    /// `crawlrs_frontier_pending_claims{shard}` (XLEN per shard) and
    /// `crawlrs_frontier_pool_pending` (bb8 pool active count).
    /// Called by the binary's maintenance loop per scrape interval
    /// (Phase 6); not on the hot path because XLEN per shard per
    /// claim would dominate Redis traffic at scale.
    pub async fn record_pending_metrics(&self) -> Result<()> {
        let depths = self.shard_depths().await?;
        for (shard, depth) in depths {
            metrics::gauge!(
                crate::metrics::FRONTIER_PENDING_CLAIMS,
                "shard" => shard.to_string(),
            )
            .set(depth as f64);
        }
        let state = self.pool.state();
        let active = state.connections.saturating_sub(state.idle_connections);
        metrics::gauge!(crate::metrics::FRONTIER_POOL_PENDING).set(active as f64);
        Ok(())
    }

    /// Reassign stranded entries (idle > `autoclaim_idle`) from peer
    /// consumers to `target`. Returns the total count reclaimed across
    /// all owned shards.
    ///
    /// Reclaimed entries land in `target`'s PEL on the Redis side and
    /// surface on `target`'s next `claim()` call via tier-1
    /// (`XREADGROUP id "0"`). Callers commonly invoke this on
    /// graceful-shutdown of a peer pod or on a periodic maintenance
    /// cadence; on the hot path `claim()` already does a single-entry
    /// `XAUTOCLAIM` per shard via tier-3.
    #[tracing::instrument(skip(self), fields(target = %target))]
    pub async fn reclaim_stranded(&self, target: &WorkerIdentity) -> Result<usize> {
        let mut total = 0_usize;
        let group = self.keys.consumer_group();
        let consumer = Self::consumer_name(target);

        for &shard in &self.owned_shards {
            let queue_key = self.keys.queue(shard);
            let mut conn = self.checkout().await?;

            // XAUTOCLAIM <key> <group> <consumer> <min-idle-ms> <start>
            // Start at "0-0" to walk all stranded entries in this call.
            let reply: StreamAutoClaimReply = conn
                .xautoclaim_options(
                    &queue_key,
                    group,
                    &consumer,
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
                self.in_flight.fetch_add(reclaimed, Ordering::Relaxed);
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

    /// Run one `EVAL batch_submit.lua` for `chunk` on the given shard.
    /// `chunk` is bounded by `SUBMIT_BATCH_CHUNK`. Returns the count of
    /// URLs that were newly enqueued (SADD-returned-1) within the chunk.
    /// Retries transient Redis errors (LOADING, BUSY, etc.) up to
    /// `TRANSIENT_RETRY_ATTEMPTS` times with exponential backoff.
    async fn enqueue_chunk(&self, shard: ShardKey, chunk: &[&UrlEntry]) -> LocalResult<usize> {
        debug_assert!(chunk.len() <= SUBMIT_BATCH_CHUNK);
        let seen_key = self.keys.seen(shard);
        let queue_key = self.keys.queue(shard);

        // Build the EVAL command once; reuse across retries since it's
        // immutable after construction. `query_async(&mut *conn)`
        // borrows the cmd by reference, so no clone is needed.
        // ARGV[1] is the queue-depth cap (0 = uncapped); ARGV[2..] is
        // the interleaved (url, body) pairs.
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(BATCH_SUBMIT_LUA)
            .arg(2)
            .arg(&seen_key)
            .arg(&queue_key)
            .arg(self.max_queue_depth);
        for entry in chunk {
            let body =
                codec::encode(entry).map_err(|e| RedisFrontierError::Codec(e.to_string()))?;
            cmd.arg(entry.url.as_str()).arg(body);
        }

        let mut delay = TRANSIENT_RETRY_INITIAL_DELAY;
        for attempt in 1..=TRANSIENT_RETRY_ATTEMPTS {
            let mut conn = self.checkout().await?;
            match cmd.query_async::<i64>(&mut *conn).await {
                Ok(newly) => return Ok(newly as usize),
                Err(err) if is_transient(&err) && attempt < TRANSIENT_RETRY_ATTEMPTS => {
                    warn!(
                        op = "enqueue_chunk",
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        kind = ?err.kind(),
                        "transient redis error; retrying",
                    );
                    drop(conn);
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
                Err(err) => return Err(RedisFrontierError::from(err)),
            }
        }
        unreachable!("loop returns or errors on every iteration")
    }

    /// Group entries by shard, validate ownership, run one chunked
    /// `EVAL batch_submit.lua` per shard. Per-shard work runs
    /// concurrently (each shard takes its own pool connection); chunks
    /// within a shard stay sequential because they all hit the same
    /// Redis-side seen-set and Stream. Returns the total count of
    /// newly-enqueued URLs across all shards.
    async fn enqueue(&self, entries: &[UrlEntry]) -> LocalResult<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        // Group by shard. `&UrlEntry` so we don't clone bodies; the
        // Lua-arg encode happens inside `enqueue_chunk`.
        let mut by_shard: HashMap<ShardKey, Vec<&UrlEntry>> = HashMap::new();
        for entry in entries {
            let shard = self.sharding_policy.shard_key(&entry.url);
            self.assert_owned(shard)?;
            by_shard.entry(shard).or_default().push(entry);
        }

        // One async task per shard; chunks inside the task stay
        // sequential. `try_join_all` short-circuits on the first error.
        let per_shard = by_shard
            .into_iter()
            .map(|(shard, shard_entries)| async move {
                let mut shard_newly = 0usize;
                for chunk in shard_entries.chunks(SUBMIT_BATCH_CHUNK) {
                    let newly = self.enqueue_chunk(shard, chunk).await?;
                    shard_newly += newly;
                    debug!(shard, chunk_size = chunk.len(), newly, "enqueue_chunk");
                }
                Ok::<usize, RedisFrontierError>(shard_newly)
            });

        let totals = futures::future::try_join_all(per_shard).await?;
        Ok(totals.into_iter().sum())
    }

    /// `XREADGROUP` against `id`, retrying transient redis errors.
    /// Both PEL re-read (id `0`) and new-read (id `>`) go through
    /// here; both are safe to retry (no side effect on transient
    /// error since the consumer-group cursor advances only on
    /// successful read).
    async fn read_one(
        &self,
        queue_key: &str,
        group: &str,
        consumer: &str,
        id: &str,
    ) -> LocalResult<StreamReadReply> {
        let opts = StreamReadOptions::default().group(group, consumer).count(1);
        let mut delay = TRANSIENT_RETRY_INITIAL_DELAY;
        for attempt in 1..=TRANSIENT_RETRY_ATTEMPTS {
            let mut conn = self.checkout().await?;
            match conn.xread_options(&[queue_key], &[id], &opts).await {
                Ok(reply) => return Ok(reply),
                Err(err) if is_transient(&err) && attempt < TRANSIENT_RETRY_ATTEMPTS => {
                    warn!(
                        op = "xread_options",
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        kind = ?err.kind(),
                        "transient redis error; retrying",
                    );
                    drop(conn);
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
                Err(err) => return Err(RedisFrontierError::from(err)),
            }
        }
        unreachable!("loop returns or errors on every iteration")
    }

    /// Try to steal one stranded entry from a peer consumer on `shard`
    /// into `identity`. Entries idle for at least `autoclaim_idle` move
    /// into `identity`'s PEL. Returns the first reclaimed entry (if any).
    async fn reclaim_one(
        &self,
        shard: ShardKey,
        identity: &WorkerIdentity,
    ) -> LocalResult<Option<(UrlEntry, StreamEntryId)>> {
        let queue_key = self.keys.queue(shard);
        let group = self.keys.consumer_group();
        let consumer = Self::consumer_name(identity);
        let mut conn = self.checkout().await?;

        let reply: StreamAutoClaimReply = conn
            .xautoclaim_options(
                &queue_key,
                group,
                &consumer,
                self.autoclaim_idle.as_millis() as u64,
                "0-0",
                StreamAutoClaimOptions::default().count(1),
            )
            .await
            .map_err(RedisFrontierError::from)?;

        let Some(stream_id) = reply.claimed.into_iter().next() else {
            return Ok(None);
        };

        let entry = decode_stream_entry(&stream_id)?;
        let entry_id = stream_id.id.clone();
        debug!(shard, url = entry.url.as_str(), "stole stranded entry");
        Ok(Some((entry, entry_id)))
    }

    async fn claim_from(
        &self,
        shard: ShardKey,
        identity: &WorkerIdentity,
    ) -> LocalResult<Option<(UrlEntry, StreamEntryId)>> {
        let queue_key = self.keys.queue(shard);
        let group = self.keys.consumer_group();
        let consumer = Self::consumer_name(identity);

        // Step 1: this worker's own PEL (id "0"). Picks up entries
        // we previously claimed but haven't acked (e.g. after a
        // process restart, or because we already nacked them and
        // are retrying). Stable consumer names mean restart finds the
        // PEL immediately rather than waiting for XAUTOCLAIM idle.
        let pel = self.read_one(&queue_key, group, &consumer, "0").await?;
        if let Some((entry, id)) = first_entry(&pel)? {
            return Ok(Some((entry, id)));
        }

        // Step 2: PEL empty, read new entries (id ">").
        let new = self.read_one(&queue_key, group, &consumer, ">").await?;
        if let Some((entry, id)) = first_entry(&new)? {
            return Ok(Some((entry, id)));
        }

        // Step 3: nothing new and our PEL is clear. Try to steal a
        // stranded entry from a peer consumer (`autoclaim_idle` is
        // the safety threshold; healthy workers ack within ms, so
        // we don't snatch in-flight entries from them in production).
        self.reclaim_one(shard, identity).await
    }

    fn record_claim_outcome(&self, shard: ShardKey, outcome: &'static str) {
        metrics::counter!(
            crate::metrics::FRONTIER_CLAIM_TOTAL,
            "shard" => shard.to_string(),
            "outcome" => outcome,
        )
        .increment(1);
    }
}

#[async_trait]
impl Frontier for RedisFrontier {
    #[tracing::instrument(skip(self, entry), fields(url = %entry.url))]
    async fn submit(&self, entry: UrlEntry) -> Result<bool> {
        // One-element batch: same Lua script, chunk size 1. The
        // overhead is one Lua loop iteration; not worth a separate
        // code path.
        let started_at = std::time::Instant::now();
        let result = self.enqueue(std::slice::from_ref(&entry)).await;
        metrics::histogram!(
            crate::metrics::FRONTIER_CALL_SECONDS,
            "op" => crate::metrics::OP_SUBMIT_BATCH,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(result? == 1)
    }

    #[tracing::instrument(skip(self, entries), fields(n = entries.len()))]
    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<usize> {
        let started_at = std::time::Instant::now();
        metrics::histogram!(crate::metrics::FRONTIER_SUBMIT_BATCH_SIZE)
            .record(entries.len() as f64);
        let result = self.enqueue(&entries).await;
        metrics::histogram!(
            crate::metrics::FRONTIER_CALL_SECONDS,
            "op" => crate::metrics::OP_SUBMIT_BATCH,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(result?)
    }

    #[tracing::instrument(skip(self), fields(identity = %identity))]
    async fn claim(&self, identity: &WorkerIdentity) -> Result<Option<ClaimedMessage>> {
        let n = self.owned_shards.len();
        if n == 0 {
            return Ok(None);
        }
        let started_at = std::time::Instant::now();
        let start = self.claim_cursor.fetch_add(1, Ordering::Relaxed) % n;
        let mut result: Result<Option<ClaimedMessage>> = Ok(None);
        for offset in 0..n {
            let shard = self.owned_shards[(start + offset) % n];
            match self.claim_from(shard, identity).await {
                Ok(Some((entry, entry_id))) => {
                    self.record_claim_outcome(shard, crate::metrics::OUTCOME_CLAIMED);
                    self.in_flight.fetch_add(1, Ordering::Relaxed);
                    let attempt_id = encode_attempt(shard, &entry_id);
                    result = Ok(Some(ClaimedMessage { entry, attempt_id }));
                    break;
                }
                Ok(None) => {
                    self.record_claim_outcome(shard, crate::metrics::OUTCOME_EMPTY);
                }
                Err(e) => {
                    self.record_claim_outcome(shard, crate::metrics::OUTCOME_ERROR);
                    result = Err(e.into());
                    break;
                }
            }
        }
        metrics::histogram!(
            crate::metrics::FRONTIER_CALL_SECONDS,
            "op" => crate::metrics::OP_CLAIM,
        )
        .record(started_at.elapsed().as_secs_f64());
        result
    }

    #[tracing::instrument(skip(self), fields(identity = %identity, max))]
    async fn claim_batch(
        &self,
        identity: &WorkerIdentity,
        max: usize,
    ) -> Result<Vec<ClaimedMessage>> {
        let mut out = Vec::with_capacity(max.min(64));
        let n = self.owned_shards.len();
        if n == 0 || max == 0 {
            return Ok(out);
        }
        // Walk shards once in round-robin order; within each shard,
        // drain until empty or until we hit `max`. This keeps
        // locality (consecutive entries from the same shard come
        // together) without starving other shards within a single
        // call.
        let start = self.claim_cursor.fetch_add(1, Ordering::Relaxed) % n;
        for offset in 0..n {
            let shard = self.owned_shards[(start + offset) % n];
            while out.len() < max {
                match self.claim_from(shard, identity).await? {
                    Some((entry, entry_id)) => {
                        self.in_flight.fetch_add(1, Ordering::Relaxed);
                        let attempt_id = encode_attempt(shard, &entry_id);
                        out.push(ClaimedMessage { entry, attempt_id });
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

    #[tracing::instrument(skip(self), fields(attempt = %attempt))]
    async fn ack(&self, attempt: &AttemptId) -> Result<()> {
        let started_at = std::time::Instant::now();
        let (shard, entry_id) = decode_attempt(attempt).map_err(Error::from)?;
        let queue_key = self.keys.queue(shard);
        let group = self.keys.consumer_group();
        let result: Result<()> = async {
            let mut conn = self.checkout().await.map_err(Error::from)?;
            let _: i64 = redis::cmd("XACK")
                .arg(&queue_key)
                .arg(group)
                .arg(entry_id)
                .query_async(&mut *conn)
                .await
                .map_err(RedisFrontierError::from)
                .map_err(Error::from)?;
            Ok(())
        }
        .await;
        if result.is_ok() {
            debug!(shard, attempt = %attempt, "ack");
            // Saturating decrement: the counter is best-effort metrics,
            // and a duplicate ack on an already-acked attempt should
            // not underflow.
            let prev = self.in_flight.load(Ordering::Relaxed);
            if prev > 0 {
                self.in_flight.fetch_sub(1, Ordering::Relaxed);
            }
        }
        metrics::histogram!(
            crate::metrics::FRONTIER_CALL_SECONDS,
            "op" => crate::metrics::OP_ACK,
        )
        .record(started_at.elapsed().as_secs_f64());
        result
    }

    #[tracing::instrument(skip(self), fields(attempt = %attempt))]
    async fn nack(&self, attempt: &AttemptId) -> Result<()> {
        let started_at = std::time::Instant::now();
        // Leave the entry in this worker's PEL on Redis. On the next
        // claim, tier-1 (`XREADGROUP id "0"`) surfaces it again. Peer
        // workers can steal via `reclaim_one` once idle past
        // `autoclaim_idle`. Nack is local-only on the Redis side; we
        // just drop our in-flight tracking.
        let prev = self.in_flight.load(Ordering::Relaxed);
        if prev > 0 {
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
        debug!(attempt = %attempt, "nack");
        metrics::histogram!(
            crate::metrics::FRONTIER_CALL_SECONDS,
            "op" => crate::metrics::OP_NACK,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(())
    }

    // No `tick()` override: workers self-rebalance via
    // `reclaim_one` in the claim path. The trait
    // default `Ok(0)` is fine. `reclaim_stranded` stays public for
    // ad-hoc drains (e.g. graceful shutdown of an entire pool).
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
        if let Some(stream_id) = stream_key.ids.first() {
            let entry = decode_stream_entry(stream_id)?;
            return Ok(Some((entry, stream_id.id.clone())));
        }
    }
    Ok(None)
}

/// Decode the postcard-encoded `UrlEntry` from a Stream entry's `body`
/// field. Returns `Err` if the field is missing or the bytes don't
/// decode; never `Ok` without a value, so the caller doesn't need to
/// handle a `None` case.
fn decode_stream_entry(stream_id: &redis::streams::StreamId) -> LocalResult<UrlEntry> {
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
    codec::decode(bytes).map_err(|e| RedisFrontierError::Codec(e.to_string()))
}
