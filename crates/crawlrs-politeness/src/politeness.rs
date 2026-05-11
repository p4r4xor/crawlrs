//! Redis-backed `Politeness` impl. See module-level docs in `lib.rs`.
//!
//! Per ADR-0020 the politeness layer is policy-only: it answers
//! `check` (Allow/Disallow) and returns `NextWake` plans from
//! `record_fetch`/`record_failure`. Wake-time persistence moved to
//! the frontier crate. The circuit-breaker state (consecutive
//! failures + last failure kind) still lives here in Redis since
//! it's policy state, not scheduling state.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use crawlrs_core::{
    CanonicalUrl, Error, FailureKind, Fetcher, NextWake, PoliteDecision, Politeness, Result,
    ShardKey, ShardingPolicy,
};
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
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
            .field("host_delay", &self.config.host_delay)
            .field("obey_robots_txt", &self.config.obey_robots_txt)
            .field("blocklist", &self.config.blocklist.len())
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
            config.robots_ttl,
        );

        info!(
            run_id = keys.run_id(),
            owned_shards = ?owned_shards,
            host_delay_ms = config.host_delay.as_millis() as u64,
            obey_robots_txt = config.obey_robots_txt,
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

    fn effective_host_delay(&self, host: &str) -> Duration {
        self.config
            .per_domain
            .get(host)
            .and_then(|o| o.host_delay)
            .unwrap_or(self.config.host_delay)
    }

    fn effective_obey_robots(&self, host: &str) -> bool {
        self.config
            .per_domain
            .get(host)
            .and_then(|o| o.obey_robots_txt)
            .unwrap_or(self.config.obey_robots_txt)
    }

    fn is_blocked(&self, host: &str) -> bool {
        self.config.blocklist.contains(host)
    }

    /// Convert a desired wait `Duration` from now into an `Instant`
    /// suitable for `NextWake.until`. Pure: no Redis call.
    fn next_wake_after(&self, host: &str, delay: Duration) -> NextWake {
        NextWake {
            host: host.to_string(),
            until: Instant::now() + delay,
        }
    }
}

#[async_trait]
impl Politeness for RedisPoliteness {
    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn check(&self, url: &CanonicalUrl) -> Result<PoliteDecision> {
        let host = self.host_of(url).map_err(Error::from)?;
        if self.is_blocked(host) {
            record_check_decision(crate::metrics::DECISION_DISALLOW_BLOCKED);
            return Ok(PoliteDecision::Disallow);
        }

        let shard = self.sharding_policy.shard_key(url);
        self.assert_owned(shard).map_err(Error::from)?;

        // Robots check (config-gated per-host).
        if self.effective_obey_robots(host) {
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

        // Circuit breaker: open if consecutive_failures has crossed
        // the threshold. The wake-time the circuit-breaker would
        // otherwise enforce is now applied by the frontier via the
        // NextWake plan we return from `record_failure`; this branch
        // is purely the policy gate.
        let state_key = self.keys.hoststate(shard, host);
        let mut conn = self.checkout().await.map_err(Error::from)?;
        let failures: Option<u32> = conn
            .hget(&state_key, HOSTSTATE_FIELD_FAILURES)
            .await
            .map_err(RedisPolitenessError::from)
            .map_err(Error::from)?;
        if failures.unwrap_or(0) >= self.config.backoff.failure_threshold {
            metrics::counter!(crate::metrics::POLITENESS_CIRCUIT_OPEN_TOTAL).increment(1);
            record_check_decision(crate::metrics::DECISION_DISALLOW_CIRCUIT);
            return Ok(PoliteDecision::Disallow);
        }

        record_check_decision(crate::metrics::DECISION_ALLOW);
        Ok(PoliteDecision::Allow)
    }

    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn record_fetch(&self, url: &CanonicalUrl) -> Result<NextWake> {
        let host = self.host_of(url).map_err(Error::from)?;
        let shard = self.sharding_policy.shard_key(url);
        self.assert_owned(shard).map_err(Error::from)?;

        let delay = self.effective_host_delay(host);
        let state_key = self.keys.hoststate(shard, host);

        // Reset circuit-breaker state on success. The wake-time write
        // belongs to the frontier now (ADR-0020); we just compute the
        // plan and hand it back.
        let mut conn = self.checkout().await.map_err(Error::from)?;
        let _: () = conn
            .hdel(
                &state_key,
                &[
                    HOSTSTATE_FIELD_FAILURES,
                    HOSTSTATE_FIELD_BACKOFF_UNTIL,
                    HOSTSTATE_FIELD_LAST_KIND,
                ],
            )
            .await
            .map_err(RedisPolitenessError::from)
            .map_err(Error::from)?;

        debug!(
            shard,
            host,
            delay_ms = delay.as_millis() as u64,
            "record_fetch",
        );
        Ok(self.next_wake_after(host, delay))
    }

    #[tracing::instrument(skip(self), fields(url = %url, kind = ?kind))]
    async fn record_failure(
        &self,
        url: &CanonicalUrl,
        kind: FailureKind,
        retry_after: Option<Duration>,
    ) -> Result<NextWake> {
        let host = self.host_of(url).map_err(Error::from)?;
        let shard = self.sharding_policy.shard_key(url);
        self.assert_owned(shard).map_err(Error::from)?;

        let state_key = self.keys.hoststate(shard, host);
        let mut conn = self.checkout().await.map_err(Error::from)?;

        // Atomic increment. Returns the new value, avoiding the
        // read-modify-write race when two workers see the same
        // failure for the same host.
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

        // Persist the backoff-state for the circuit-breaker check.
        // (The wake-time itself is the frontier's responsibility now.)
        let until_score = SystemTime::now()
            .checked_add(backoff)
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let _: () = redis::pipe()
            .hset(&state_key, HOSTSTATE_FIELD_BACKOFF_UNTIL, until_score)
            .hset(&state_key, HOSTSTATE_FIELD_LAST_KIND, format!("{kind:?}"))
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
        Ok(self.next_wake_after(host, backoff))
    }
}

impl RedisPoliteness {
    /// Borrow the underlying robots cache. Useful for debugging or
    /// fine-grained callers that want the robots-only decision
    /// without going through the full `check`.
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
