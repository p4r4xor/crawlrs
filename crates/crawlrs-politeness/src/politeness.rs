//! Redis-backed `Politeness` impl. See module-level docs in `lib.rs`.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    CanonicalUrl, Error, FailureKind, Fetcher, PoliteDecision, Politeness, Result, ShardKey,
    ShardingPolicy,
};
use redis::AsyncCommands;
use thiserror::Error as ThisError;
use tracing::{debug, info};

use crate::config::PolitenessConfig;
use crate::failure::compute_backoff;
use crate::keys::KeyPrefix;
use crate::robots::RobotsCache;

const HOSTSTATE_FIELD_FAILURES: &str = "consecutive_failures";
const HOSTSTATE_FIELD_BACKOFF_UNTIL: &str = "backoff_until_ms";
const HOSTSTATE_FIELD_LAST_KIND: &str = "last_kind";

/// Internal error type for the politeness crate. All variants flow
/// out as [`crawlrs_core::Error::Politeness`] at the trait boundary so
/// callers see a single coarse variant in the public surface.
#[derive(Debug, ThisError)]
pub enum RedisPolitenessError {
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("connection pool error: {0}")]
    Pool(String),

    #[error("shard {got} is not owned by this politeness instance (owns {owned:?})")]
    ShardNotOwned { got: ShardKey, owned: Vec<ShardKey> },

    #[error("shard {got} is out of range for the policy's shard_count={count}")]
    ShardOutOfRange { got: ShardKey, count: u32 },

    #[error("robots: {0}")]
    Robots(String),

    #[error("url has no host: {0}")]
    NoHost(String),
}

impl From<RedisPolitenessError> for Error {
    fn from(e: RedisPolitenessError) -> Self {
        Error::Politeness(e.to_string())
    }
}

type LocalResult<T> = std::result::Result<T, RedisPolitenessError>;

/// Politeness implementation backed by per-shard Redis state.
pub struct RedisPoliteness {
    pool: Pool<RedisConnectionManager>,
    keys: KeyPrefix,
    sharding_policy: Arc<dyn ShardingPolicy>,
    owned_shards: Vec<ShardKey>,
    config: PolitenessConfig,
    robots: RobotsCache,
}

impl std::fmt::Debug for RedisPoliteness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPoliteness")
            .field("run_id", &self.keys.run_id())
            .field("owned_shards", &self.owned_shards)
            .field("min_delay", &self.config.min_delay)
            .field("honor_robots_txt", &self.config.honor_robots_txt)
            .field("manual_excludes", &self.config.manual_excludes.len())
            .field("per_domain_overrides", &self.config.per_domain.len())
            .finish()
    }
}

impl RedisPoliteness {
    pub async fn new(
        pool: Pool<RedisConnectionManager>,
        sharding_policy: Arc<dyn ShardingPolicy>,
        owned_shards: Vec<ShardKey>,
        fetcher: Arc<dyn Fetcher>,
        run_id: impl Into<String>,
        config: PolitenessConfig,
    ) -> Result<Self> {
        let keys = KeyPrefix::new(run_id);
        let count = sharding_policy.shard_count();
        for &shard in &owned_shards {
            if shard >= count {
                return Err(RedisPolitenessError::ShardOutOfRange { got: shard, count }.into());
            }
        }

        let robots = RobotsCache::new(
            pool.clone(),
            keys.clone(),
            sharding_policy.clone(),
            fetcher,
            config.user_agent.clone(),
            config.robots_cache_ttl,
        );

        info!(
            run_id = keys.run_id(),
            owned_shards = ?owned_shards,
            min_delay_ms = config.min_delay.as_millis() as u64,
            honor_robots_txt = config.honor_robots_txt,
            "RedisPoliteness ready",
        );

        Ok(Self {
            pool,
            keys,
            sharding_policy,
            owned_shards,
            config,
            robots,
        })
    }

    pub fn config(&self) -> &PolitenessConfig {
        &self.config
    }

    /// Number of hosts currently tracked in any owned shard's
    /// host-schedule ZSET. Useful as a metric.
    pub async fn host_count(&self) -> Result<usize> {
        let mut total = 0usize;
        let mut conn = self.checkout().await.map_err(Error::from)?;
        for &shard in &self.owned_shards {
            let key = self.keys.hostsched(shard);
            let n: usize = conn
                .zcard(&key)
                .await
                .map_err(RedisPolitenessError::from)
                .map_err(Error::from)?;
            total += n;
        }
        Ok(total)
    }

    /// Snapshot of the bb8 pool's state.
    pub fn pool_state(&self) -> bb8::State {
        self.pool.state()
    }

    /// Refresh the `crawlrs_politeness_pool_pending` gauge from the
    /// bb8 pool's published state. Approximates "outstanding
    /// connections" via `connections - idle_connections`. Called by
    /// the binary's maintenance loop per scrape interval.
    pub fn record_pool_metrics(&self) {
        let state = self.pool.state();
        let active = state.connections.saturating_sub(state.idle_connections);
        metrics::gauge!(crate::metrics::POLITENESS_POOL_PENDING).set(active as f64);
    }

    // --- internals -----------------------------------------------------

    async fn checkout(&self) -> LocalResult<bb8::PooledConnection<'_, RedisConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| RedisPolitenessError::Pool(format!("{e:?}")))
    }

    fn host_of<'u>(&self, url: &'u CanonicalUrl) -> LocalResult<&'u str> {
        url.host()
            .ok_or_else(|| RedisPolitenessError::NoHost(url.as_str().into()))
    }

    fn assert_owned(&self, shard: ShardKey) -> LocalResult<()> {
        if !self.owned_shards.contains(&shard) {
            return Err(RedisPolitenessError::ShardNotOwned {
                got: shard,
                owned: self.owned_shards.clone(),
            });
        }
        Ok(())
    }

    fn effective_min_delay(&self, host: &str) -> std::time::Duration {
        self.config
            .per_domain
            .get(host)
            .and_then(|o| o.min_delay)
            .unwrap_or(self.config.min_delay)
    }

    fn effective_honor_robots(&self, host: &str) -> bool {
        self.config
            .per_domain
            .get(host)
            .and_then(|o| o.honor_robots_txt)
            .unwrap_or(self.config.honor_robots_txt)
    }

    fn is_excluded(&self, host: &str) -> bool {
        self.config.manual_excludes.contains(host)
    }
}

#[async_trait]
impl Politeness for RedisPoliteness {
    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn check(&self, url: &CanonicalUrl) -> Result<PoliteDecision> {
        let host = self.host_of(url).map_err(Error::from)?;
        if self.is_excluded(host) {
            record_check_decision(crate::metrics::DECISION_DISALLOW_EXCLUDED);
            return Ok(PoliteDecision::Disallow);
        }

        let shard = self.sharding_policy.shard_key(url);
        self.assert_owned(shard).map_err(Error::from)?;

        // Robots check (config-gated per-host).
        if self.effective_honor_robots(host) {
            let allowed = self
                .robots
                .allowed(url, &self.config.user_agent)
                .await
                .map_err(Error::from)?;
            if !allowed {
                record_check_decision(crate::metrics::DECISION_DISALLOW_ROBOTS);
                return Ok(PoliteDecision::Disallow);
            }
        }

        // Failure circuit check (open if consecutive_failures has crossed
        // the threshold AND backoff_until is in the future).
        let state_key = self.keys.hoststate(shard, host);
        let mut conn = self.checkout().await.map_err(Error::from)?;
        let failures: Option<u32> = conn
            .hget(&state_key, HOSTSTATE_FIELD_FAILURES)
            .await
            .map_err(RedisPolitenessError::from)
            .map_err(Error::from)?;
        if failures.unwrap_or(0) >= self.config.backoff.circuit_open_after_failures {
            metrics::counter!(crate::metrics::POLITENESS_CIRCUIT_OPEN_TOTAL).increment(1);
            record_check_decision(crate::metrics::DECISION_DISALLOW_CIRCUIT);
            return Ok(PoliteDecision::Disallow);
        }

        // Per-host wake-time.
        let sched_key = self.keys.hostsched(shard);
        let next: Option<f64> = conn
            .zscore(&sched_key, host)
            .await
            .map_err(RedisPolitenessError::from)
            .map_err(Error::from)?;
        let now = SystemTime::now();
        match next {
            None => {
                record_check_decision(crate::metrics::DECISION_ALLOW);
                Ok(PoliteDecision::Allow)
            }
            Some(score) => {
                let when = score_to_wall(score);
                if when <= now {
                    record_check_decision(crate::metrics::DECISION_ALLOW);
                    Ok(PoliteDecision::Allow)
                } else {
                    let delay = when.duration_since(now).unwrap_or_default();
                    record_check_decision(crate::metrics::DECISION_DELAY);
                    Ok(PoliteDecision::Delay(delay))
                }
            }
        }
    }

    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn record_fetch(&self, url: &CanonicalUrl) -> Result<()> {
        let host = self.host_of(url).map_err(Error::from)?;
        let shard = self.sharding_policy.shard_key(url);
        self.assert_owned(shard).map_err(Error::from)?;

        let delay = self.effective_min_delay(host);
        let next_allowed = SystemTime::now() + delay;
        let sched_key = self.keys.hostsched(shard);
        let state_key = self.keys.hoststate(shard, host);

        let mut conn = self.checkout().await.map_err(Error::from)?;
        // Pipeline the schedule update + failure-state reset so the
        // wake-time advance and the backoff clear hit Redis together.
        let _: () = redis::pipe()
            .zadd(&sched_key, host, wall_to_score(next_allowed))
            .hdel(
                &state_key,
                &[
                    HOSTSTATE_FIELD_FAILURES,
                    HOSTSTATE_FIELD_BACKOFF_UNTIL,
                    HOSTSTATE_FIELD_LAST_KIND,
                ],
            )
            .query_async(&mut *conn)
            .await
            .map_err(RedisPolitenessError::from)
            .map_err(Error::from)?;
        debug!(
            shard,
            host,
            delay_ms = delay.as_millis() as u64,
            "record_fetch",
        );
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(url = %url, kind = ?kind))]
    async fn record_failure(
        &self,
        url: &CanonicalUrl,
        kind: FailureKind,
        retry_after: Option<std::time::Duration>,
    ) -> Result<()> {
        let host = self.host_of(url).map_err(Error::from)?;
        let shard = self.sharding_policy.shard_key(url);
        self.assert_owned(shard).map_err(Error::from)?;

        let sched_key = self.keys.hostsched(shard);
        let state_key = self.keys.hoststate(shard, host);

        let mut conn = self.checkout().await.map_err(Error::from)?;

        // Atomically increment the consecutive-failure counter; HINCRBY
        // returns the new value. This avoids the read-modify-write race
        // when two workers see the same failure for the same host.
        let new_failures: u32 = conn
            .hincr(&state_key, HOSTSTATE_FIELD_FAILURES, 1u32)
            .await
            .map_err(RedisPolitenessError::from)
            .map_err(Error::from)?;

        // Take max(server hint, computed backoff). Server's hint is a
        // floor; if our exponential backoff is harsher (e.g. 5th 503
        // in a row), we still apply that. Servers under-estimate
        // recovery time more often than they over-estimate it, but
        // either way max() honors both bounds.
        let backoff = compute_backoff(new_failures, kind, retry_after, &self.config.backoff);
        metrics::histogram!(crate::metrics::POLITENESS_BACKOFF_SECONDS)
            .record(backoff.as_secs_f64());
        // Source attribution: which input dominated the final value?
        // Edge cases (computed == max_backoff exactly, hint == computed
        // exactly) are rare; the classifier picks the dominant cause
        // and is sufficient as an operational counter.
        let source = if backoff >= self.config.backoff.max_backoff {
            crate::metrics::SOURCE_CAPPED
        } else if retry_after.is_some_and(|hint| hint == backoff) {
            crate::metrics::SOURCE_SERVER_HINT
        } else {
            crate::metrics::SOURCE_COMPUTED
        };
        metrics::counter!(
            crate::metrics::POLITENESS_BACKOFF_SOURCE_TOTAL,
            "source" => source,
        )
        .increment(1);
        let until = SystemTime::now() + backoff;
        let until_score = wall_to_score(until);

        let _: () = redis::pipe()
            .hset(
                &state_key,
                HOSTSTATE_FIELD_BACKOFF_UNTIL,
                until_score as u64,
            )
            .hset(&state_key, HOSTSTATE_FIELD_LAST_KIND, format!("{kind:?}"))
            .zadd(&sched_key, host, until_score)
            .query_async(&mut *conn)
            .await
            .map_err(RedisPolitenessError::from)
            .map_err(Error::from)?;
        debug!(
            shard,
            host,
            new_failures,
            backoff_ms = backoff.as_millis() as u64,
            retry_after_ms = retry_after.map(|d| d.as_millis() as u64).unwrap_or(0),
            "record_failure",
        );
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn next_ready_at(&self) -> Result<Option<Instant>> {
        let mut soonest: Option<SystemTime> = None;
        let mut conn = self.checkout().await.map_err(Error::from)?;

        for &shard in &self.owned_shards {
            let key = self.keys.hostsched(shard);
            // Smallest score in the ZSET = earliest next-allowed time.
            let entries: Vec<(String, f64)> = conn
                .zrange_withscores(&key, 0, 0)
                .await
                .map_err(RedisPolitenessError::from)
                .map_err(Error::from)?;
            if let Some((_, score)) = entries.first() {
                let when = score_to_wall(*score);
                soonest = Some(soonest.map_or(when, |s| s.min(when)));
            }
        }

        Ok(soonest.map(wall_to_instant))
    }
}

impl RedisPoliteness {
    /// Borrow the underlying robots cache. Useful for debugging or
    /// fine-grained callers that want the robots-only decision
    /// without going through the full `check`. The trait deliberately
    /// doesn't surface this; the type context on `RobotsCache` carries
    /// the implicit subject for its own methods.
    pub fn robots(&self) -> &RobotsCache {
        &self.robots
    }
}

// --- module-private helpers --------------------------------------------

/// Emit the `crawlrs_politeness_check_total{decision}` counter. The
/// decision label is one of the per-decision constants in the metrics
/// module; bounded set, no cardinality concern.
fn record_check_decision(decision: &'static str) {
    metrics::counter!(
        crate::metrics::POLITENESS_CHECK_TOTAL,
        "decision" => decision,
    )
    .increment(1);
}

/// Encode a wall-clock moment as a Redis ZSET score (millis since
/// the unix epoch). We trust system clocks to be NTP-synced across
/// workers (production assumption); the same epoch on every pod keeps
/// cross-pod ordering consistent.
fn wall_to_score(when: SystemTime) -> f64 {
    when.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

/// Decode a Redis ZSET score back to a wall-clock moment.
fn score_to_wall(score: f64) -> SystemTime {
    let millis = score.max(0.0) as u64;
    UNIX_EPOCH + std::time::Duration::from_millis(millis)
}

/// Convert a wall-clock moment to a monotonic `Instant`, anchored at
/// the current moment. For times in the past, returns `Instant::now()`
/// so the runtime's "sleep until" loop fires immediately.
fn wall_to_instant(when: SystemTime) -> Instant {
    let now_inst = Instant::now();
    let now_wall = SystemTime::now();
    if when <= now_wall {
        now_inst
    } else {
        let ahead = when.duration_since(now_wall).unwrap_or_default();
        now_inst + ahead
    }
}
