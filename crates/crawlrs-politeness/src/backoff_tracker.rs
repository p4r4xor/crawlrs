//! Redis-backed `BackoffTracker` impl.
//!
//! Owns the per-host `hoststate:{host}` Hash: consecutive-failure
//! count, backoff-until-ms, last failure kind. Drives the
//! exponential-backoff math (via [`crate::failure::compute_backoff`])
//! and the per-host circuit breaker (`is_open` returns true when
//! the failure count crosses the configured threshold).

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

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
}

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
        let failures: Option<u32> = conn
            .hget(&state_key, HOSTSTATE_FIELD_FAILURES)
            .await
            .map_err(PolitenessError::from)
            .map_err(Error::from)?;
        Ok(failures.unwrap_or(0) >= self.backoff.failure_threshold)
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
            .hset(&state_key, HOSTSTATE_FIELD_LAST_KIND, format!("{kind:?}"))
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
        Ok(NextWake {
            host: host.to_string(),
            until: Instant::now() + backoff,
        })
    }

    async fn reset_on_success(&self, host: &str) -> Result<()> {
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
        Ok(())
    }
}
