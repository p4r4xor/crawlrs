//! Redis-backed `BackoffTracker` impl.
//!
//! Owns the per-host `hoststate:{host}` Hash: consecutive-failure
//! count, backoff-until-ms, last failure kind. Drives the
//! exponential-backoff math (via [`crate::failure::compute_backoff`])
//! and the per-host circuit breaker. The breaker opens once the
//! consecutive-failure count crosses the configured threshold and
//! stays open only until the recorded backoff window elapses; after
//! that a half-open probe fetch is allowed, which either clears the
//! breaker on success or re-opens it with a longer window on failure.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    BackoffTracker, CanonicalUrl, Error, FailureKind, NextWake, Result, ShardKey, ShardingPolicy,
};
use redis::AsyncCommands;
use tracing::debug;

use crate::config::BackoffPolicy;
use crate::error::{LocalResult, PolitenessError};
use crate::failure::compute_backoff;
use crate::keys::KeyPrefix;

const HOSTSTATE_FIELD_FAILURES: &str = "consecutive_failures";
const HOSTSTATE_FIELD_BACKOFF_UNTIL: &str = "backoff_until_ms";
const HOSTSTATE_FIELD_LAST_KIND: &str = "last_kind";

pub(crate) struct RedisBackoffTracker {
    pool: Pool<RedisConnectionManager>,
    keys: KeyPrefix,
    sharding_policy: Arc<dyn ShardingPolicy>,
    owned_shards: Vec<ShardKey>,
    backoff: BackoffPolicy,

    /// Hosts this process has observed carrying failure state, so
    /// `reset_on_success` can skip the clearing `HDEL` for the
    /// overwhelmingly common host that never failed. Populated by
    /// `record_failure` and by `is_open` whenever it reads a non-zero
    /// count from Redis (which catches state written before a restart,
    /// since `check` -> `is_open` precedes every `record_fetch`).
    /// A false miss only means one stale key waits for the next probe
    /// to re-observe and clear it, so a bounded cache is safe.
    hosts_with_failure_state: moka::sync::Cache<String, ()>,
}

/// In-process bound on tracked failing hosts. A crawl rarely has more
/// than a few thousand hosts in active backoff at once; past the cap
/// the coldest entries drop and `is_open` re-observes them on demand.
const FAILING_HOST_TRACKER_CAPACITY: u64 = 65_536;

impl RedisBackoffTracker {
    pub(crate) fn new(
        pool: Pool<RedisConnectionManager>,
        keys: KeyPrefix,
        sharding_policy: Arc<dyn ShardingPolicy>,
        owned_shards: Vec<ShardKey>,
        backoff: BackoffPolicy,
    ) -> Self {
        Self {
            pool,
            keys,
            sharding_policy,
            owned_shards,
            backoff,
            hosts_with_failure_state: moka::sync::Cache::builder()
                .max_capacity(FAILING_HOST_TRACKER_CAPACITY)
                .build(),
        }
    }

    async fn checkout(&self) -> LocalResult<bb8::PooledConnection<'_, RedisConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| PolitenessError::Pool(format!("{e:?}")))
    }

    fn assert_owned(&self, shard: ShardKey) -> LocalResult<()> {
        if !self.owned_shards.contains(&shard) {
            return Err(PolitenessError::ShardNotOwned {
                got: shard,
                owned: self.owned_shards.clone(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl BackoffTracker for RedisBackoffTracker {
    async fn is_open(&self, host: &str) -> Result<bool> {
        let shard = self.sharding_policy.shard_key_from_host(host);
        self.assert_owned(shard).map_err(Error::from)?;

        let state_key = self.keys.hoststate(shard, host);
        let mut conn = self.checkout().await.map_err(Error::from)?;
        let (failures, backoff_until_ms): (Option<u32>, Option<u64>) = redis::pipe()
            .hget(&state_key, HOSTSTATE_FIELD_FAILURES)
            .hget(&state_key, HOSTSTATE_FIELD_BACKOFF_UNTIL)
            .query_async(&mut *conn)
            .await
            .map_err(PolitenessError::from)
            .map_err(Error::from)?;

        let observed_failures = failures.unwrap_or(0);
        if observed_failures == 0 {
            self.hosts_with_failure_state.invalidate(host);
        } else {
            // Redis holds failure state for this host; remember it so a
            // later success actually issues the clearing HDEL even if
            // the state predates this process.
            self.hosts_with_failure_state.insert(host.to_string(), ());
        }
        if observed_failures < self.backoff.failure_threshold {
            return Ok(false);
        }
        // Threshold crossed: stay open only until the recorded backoff
        // window elapses, then allow a half-open probe. The probe either
        // clears the breaker (reset_on_success) or re-opens it with a
        // longer window (record_failure), so recovery never depends on
        // an external reset. A missing window (should not happen once
        // failures are recorded) defaults to allowing the probe.
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0);
        Ok(backoff_until_ms.is_some_and(|until| now_ms < until))
    }

    async fn record_failure(
        &self,
        url: &CanonicalUrl,
        kind: FailureKind,
        server_hint: Option<Duration>,
    ) -> Result<NextWake> {
        let host = url
            .host()
            .ok_or_else(|| PolitenessError::NoHost(url.as_str().into()))
            .map_err(Error::from)?;
        let shard = self.sharding_policy.shard_key(url);
        self.assert_owned(shard).map_err(Error::from)?;

        let state_key = self.keys.hoststate(shard, host);
        let mut conn = self.checkout().await.map_err(Error::from)?;

        let new_failures: u32 = conn
            .hincr(&state_key, HOSTSTATE_FIELD_FAILURES, 1u32)
            .await
            .map_err(PolitenessError::from)
            .map_err(Error::from)?;
        self.hosts_with_failure_state.insert(host.to_string(), ());

        let backoff = compute_backoff(new_failures, kind, server_hint, &self.backoff);
        metrics::histogram!(crate::metrics::POLITENESS_BACKOFF_SECONDS)
            .record(backoff.as_secs_f64());
        let source = if backoff >= self.backoff.max_backoff {
            crate::metrics::SOURCE_CAPPED
        } else if server_hint.is_some_and(|hint| hint == backoff) {
            crate::metrics::SOURCE_SERVER_HINT
        } else {
            crate::metrics::SOURCE_COMPUTED
        };
        metrics::counter!(
            crate::metrics::POLITENESS_BACKOFF_SOURCE_TOTAL,
            "source" => source,
        )
        .increment(1);

        let until_score = SystemTime::now()
            .checked_add(backoff)
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let _: () = redis::pipe()
            .hset(&state_key, HOSTSTATE_FIELD_BACKOFF_UNTIL, until_score)
            .hset(&state_key, HOSTSTATE_FIELD_LAST_KIND, kind.as_str())
            .query_async(&mut *conn)
            .await
            .map_err(PolitenessError::from)
            .map_err(Error::from)?;

        debug!(
            shard,
            host,
            new_failures,
            backoff_ms = backoff.as_millis() as u64,
            server_hint_ms = server_hint.map(|d| d.as_millis() as u64).unwrap_or(0),
            "record_failure",
        );
        // Reuse the exact epoch-ms value written to the wake ZSET so
        // the plan the runtime applies matches the stored backoff.
        Ok(NextWake {
            host: host.to_string(),
            until_ms: until_score,
        })
    }

    async fn reset_on_success(&self, host: &str) -> Result<()> {
        // Happy path: a host with no observed failure state has nothing
        // to clear, so skip the dedicated Redis round-trip entirely. The
        // tracker is populated by `record_failure` and by `is_open`
        // (which runs before every fetch), so any host actually carrying
        // state is present here.
        if self.hosts_with_failure_state.get(host).is_none() {
            return Ok(());
        }

        let shard = self.sharding_policy.shard_key_from_host(host);
        self.assert_owned(shard).map_err(Error::from)?;

        let state_key = self.keys.hoststate(shard, host);
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
            .map_err(PolitenessError::from)
            .map_err(Error::from)?;
        self.hosts_with_failure_state.invalidate(host);
        Ok(())
    }
}
