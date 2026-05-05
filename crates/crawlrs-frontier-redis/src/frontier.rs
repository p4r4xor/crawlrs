//! Redis-backed `Frontier` impl.
//!
//! # Mental model in 60 seconds
//!
//! The frontier is a **durable URL queue** with at-least-once delivery.
//! It uses three Redis structures per shard, scoped under a `run_id`:
//!
//! - **`crawlrs:{run}:s{N}:queue`** — a Redis Stream. Each entry is one
//!   URL (postcard-encoded). Entries flow in via `XADD` and are
//!   delivered to workers via `XREADGROUP`.
//! - **`crawlrs:{run}:s{N}:seen`** — a Redis Set of URL strings already
//!   submitted. Submit-time dedup; checked atomically with the `XADD`
//!   in one Lua script (`scripts/batch_submit.lua`).
//! - **A consumer group** (`fetchers`) on the queue Stream. Each
//!   *worker task* is its own consumer within that group, named
//!   `<consumer_base>-<tokio::task::id()>`. Each consumer has a
//!   private **Pending Entries List (PEL)**: entries it has been
//!   delivered but hasn't acked yet.
//!
//! Crucially, **one worker = one tokio task = one Redis consumer**.
//! Multiple workers in one process don't share a PEL. See ADR-0012.
//!
//! # Production: cold start
//!
//! 1. Process boots. Wiring code constructs `bb8::Pool` against Redis,
//!    then `RedisFrontier::new(pool, HostHashShardPolicy(8),
//!    [0..8], run_id)`.
//! 2. The constructor:
//!    - Validates each owned shard is in range.
//!    - Generates `consumer_base = cuid2` (unique per `RedisFrontier`
//!      instance — typically one per process).
//!    - For each owned shard, runs `XGROUP CREATE
//!      crawlrs:{run}:s{N}:queue fetchers $ MKSTREAM`. Creates the
//!      Stream and the consumer group atomically. If the group
//!      already exists (from a prior process for the same `run_id`),
//!      Redis returns BUSYGROUP — we treat that as success.
//! 3. Caller spawns N worker tasks, each holding the same
//!    `Arc<dyn Frontier>`. Their consumer names are
//!    `<base>-Id(1)`, `<base>-Id(2)`, … — distinct per task.
//! 4. First `claim()` from any worker:
//!    - Walks owned shards in round-robin order.
//!    - For each shard, the three-tier ladder:
//!      1. `XREADGROUP id "0"` — own PEL. Empty: this worker has
//!         never claimed anything.
//!      2. `XREADGROUP id ">"` — new entries. Empty: nothing in the
//!         Stream yet.
//!      3. `reclaim_one(shard)` — `XAUTOCLAIM` with 5-minute
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
//! is hot — submits arrive from `submit_discovered` calls in worker
//! pipelines, claims drain entries.
//!
//! - **Each worker's claim path** is the three-tier ladder above:
//!   1. Read its own PEL first (entries it nacked, or that survived
//!      a process restart and were re-delivered to the same task id —
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
//!   mid-process leaves entries in its PEL forever — until a peer's
//!   tier 3 `XAUTOCLAIM` picks them up. No separate maintenance
//!   process needed.
//!
//! - **Submit-time dedup** stays correct across runs of `submit_batch`
//!   because the Lua script `SADD`s and `XADD`s atomically per chunk
//!   (chunks of 1000 URLs, parallel across shards). A URL already in
//!   the seen-set is silently dropped — no duplicate Stream entry.
//!
//! - **Discovery growth** is bounded by `max_queue_depth` (passed to
//!   the Lua script as `XADD MAXLEN ~ N`). Without it, an explosion
//!   of discovered links would blow Redis memory.
//!
//! Operational signals to watch:
//!
//! - `claim_count()` — in-flight URLs in this process. Should hover
//!   near `worker count` in steady state.
//! - `shard_depths()` — `XLEN` per owned shard. Tells you which
//!   shards are hot.
//! - `XPENDING <queue> <group>` (run via `redis-cli`) — total
//!   pending entries across all consumers. Steady state ≈ N workers
//!   in flight; ballooning means workers are stuck.

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
    /// Base name; the actual consumer used on every Redis call is
    /// suffixed with the current `tokio::task::id()` so each worker
    /// task acts as its own Redis Streams consumer. See ADR-0012:
    /// shared consumer names cause multiple workers to read each
    /// other's PEL via `XREADGROUP id "0"`, double-claiming entries.
    consumer_base: String,
    claims: Arc<PendingClaims>,

    /// `XAUTOCLAIM` minimum-idle-time. Workers self-rebalance by
    /// stealing entries idle for at least this long from peer
    /// consumers; the value is the safety net that prevents healthy
    /// workers' in-flight entries from being stolen mid-process. Five
    /// minutes is the production default (matches ADR-0007); tests
    /// use ~50 ms.
    autoclaim_idle: Duration,

    /// `XADD MAXLEN ~ N` cap per shard. `0` disables trimming. When
    /// trimming kicks in, OLDEST stream entries are dropped; the
    /// seen-set still remembers their URLs so they won't be
    /// re-enqueued, meaning dropped URLs are abandoned for this run.
    /// Operator picks the cap balancing memory budget vs. coverage.
    max_queue_depth: u64,

    /// Round-robin cursor for `claim` across owned shards.
    claim_cursor: AtomicUsize,
}

impl std::fmt::Debug for RedisFrontier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisFrontier")
            .field("run_id", &self.keys.run_id())
            .field("owned_shards", &self.owned_shards)
            .field("consumer_base", &self.consumer_base)
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

        let consumer_base = cuid2::create_id();

        // Ensure consumer group exists for each owned shard.
        ensure_consumer_groups(&pool, &keys, &owned_shards).await?;

        info!(
            run_id = keys.run_id(),
            consumer_base = %consumer_base,
            owned_shards = ?owned_shards,
            "RedisFrontier ready",
        );

        Ok(Self {
            pool,
            keys,
            sharding_policy,
            owned_shards,
            consumer_base,
            claims: Arc::new(PendingClaims::new()),
            autoclaim_idle: DEFAULT_AUTOCLAIM_IDLE,
            max_queue_depth: DEFAULT_MAX_QUEUE_DEPTH,
            claim_cursor: AtomicUsize::new(0),
        })
    }

    /// Per-task Redis Streams consumer name. Each worker is its own
    /// tokio task; suffixing the task id makes every worker a distinct
    /// consumer with a private PEL. See ADR-0012.
    fn consumer_name(&self) -> String {
        match tokio::task::try_id() {
            Some(id) => format!("{}-{:?}", self.consumer_base, id),
            // Outside a tokio runtime (shouldn't happen on the worker
            // hot path) we fall back to the base name. Better than
            // panicking; the at-least-once-delivery guarantee
            // degrades to the pre-ADR-0012 behavior in this corner.
            None => self.consumer_base.clone(),
        }
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

    /// Number of URLs currently in-flight on this frontier instance.
    /// Useful as a metric and for shutdown drain checks.
    pub fn claim_count(&self) -> usize {
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
    pub async fn reclaim_stranded(&self) -> Result<usize> {
        let mut total = 0_usize;
        let group = self.keys.consumer_group();
        let consumer = self.consumer_name();

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
                for stream_id in reply.claimed {
                    let entry = decode_stream_entry(&stream_id)?;
                    self.claims
                        .record(entry.url.clone(), shard, stream_id.id.clone());
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

    /// Try to steal one stranded entry from a peer consumer on `shard`.
    /// Calls `XAUTOCLAIM` with this worker's consumer name as the
    /// target; entries idle for at least `autoclaim_idle` move into
    /// this worker's PEL. Returns the first reclaimed entry (if any).
    /// See ADR-0012 for the worker-side reclaim model.
    async fn reclaim_one(&self, shard: ShardKey) -> LocalResult<Option<(UrlEntry, StreamEntryId)>> {
        let queue_key = self.keys.queue(shard);
        let group = self.keys.consumer_group();
        let consumer = self.consumer_name();
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

    async fn claim_from(&self, shard: ShardKey) -> LocalResult<Option<(UrlEntry, StreamEntryId)>> {
        let queue_key = self.keys.queue(shard);
        let group = self.keys.consumer_group();
        let consumer = self.consumer_name();

        // Step 1: this worker's own PEL (id "0"). Picks up entries
        // we previously claimed but haven't acked (e.g. after a
        // process restart, or because we already nacked them and
        // are retrying).
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
        self.reclaim_one(shard).await
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

    #[tracing::instrument(skip(self))]
    async fn claim(&self) -> Result<Option<UrlEntry>> {
        let n = self.owned_shards.len();
        if n == 0 {
            return Ok(None);
        }
        let started_at = std::time::Instant::now();
        let start = self.claim_cursor.fetch_add(1, Ordering::Relaxed) % n;
        let mut result: Result<Option<UrlEntry>> = Ok(None);
        for offset in 0..n {
            let shard = self.owned_shards[(start + offset) % n];
            match self.claim_from(shard).await {
                Ok(Some((entry, entry_id))) => {
                    self.record_claim_outcome(shard, crate::metrics::OUTCOME_CLAIMED);
                    self.claims.record(entry.url.clone(), shard, entry_id);
                    result = Ok(Some(entry));
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

    #[tracing::instrument(skip(self), fields(max))]
    async fn claim_batch(&self, max: usize) -> Result<Vec<UrlEntry>> {
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
                match self.claim_from(shard).await? {
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
        let started_at = std::time::Instant::now();
        let result = self.ack_inner(url).await;
        metrics::histogram!(
            crate::metrics::FRONTIER_CALL_SECONDS,
            "op" => crate::metrics::OP_ACK,
        )
        .record(started_at.elapsed().as_secs_f64());
        result
    }

    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn nack(&self, url: &CanonicalUrl) -> Result<()> {
        let started_at = std::time::Instant::now();
        // Drop our local tracking but leave the entry in our PEL on
        // Redis. On the next claim, `read_one(... id="0")` will
        // surface it again to this same worker (PEL re-read). Other
        // workers can steal it via `reclaim_one` once idle past
        // `autoclaim_idle`. Keeping nack synchronous and side-effect-
        // light avoids races where a hot loop re-claims an entry
        // before the original handler has fully released it.
        if let Some(record) = self.claims.take(url) {
            metrics::histogram!(crate::metrics::FRONTIER_IN_FLIGHT_SECONDS)
                .record(record.claimed_at.elapsed().as_secs_f64());
        }
        debug!(url = url.as_str(), "nack");
        metrics::histogram!(
            crate::metrics::FRONTIER_CALL_SECONDS,
            "op" => crate::metrics::OP_NACK,
        )
        .record(started_at.elapsed().as_secs_f64());
        Ok(())
    }

    // No `tick()` override: workers self-rebalance via
    // `reclaim_one` in the claim path (see ADR-0012). The trait
    // default `Ok(0)` is fine. `reclaim_stranded` stays public for
    // ad-hoc drains (e.g. graceful shutdown of an entire pool).
}

impl RedisFrontier {
    /// Inner ack body, wrapped by the trait method with timing.
    async fn ack_inner(&self, url: &CanonicalUrl) -> Result<()> {
        let Some(ClaimRecord {
            shard,
            entry_id,
            claimed_at,
        }) = self.claims.take(url)
        else {
            // Not in-flight: idempotent no-op.
            debug!(url = url.as_str(), "ack: url not in claims map; no-op");
            return Ok(());
        };
        metrics::histogram!(crate::metrics::FRONTIER_IN_FLIGHT_SECONDS)
            .record(claimed_at.elapsed().as_secs_f64());
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
