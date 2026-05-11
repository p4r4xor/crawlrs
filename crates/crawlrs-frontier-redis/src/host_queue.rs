//! Rust wrappers around the per-shard Lua scripts.
//!
//! One [`Scripts`] instance is constructed at frontier startup; the
//! `EVALSHA` cache lives inside `redis::Script` and is shared by
//! every per-shard call. The wrappers translate the opaque Lua
//! return shapes into typed Rust values.

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{SubmitOutcome, UrlEntry, UrlId};
use redis::{FromRedisValue, Script, Value};

use crate::codec;
use crate::keys::KeyPrefix;
use crawlrs_core::ShardKey;

/// One-time-loaded script handles. Cheap to clone (each `Script` is
/// a thin wrapper around the script source string + SHA1; the
/// `EVALSHA` cache lives inside the redis-rs client).
#[derive(Clone)]
pub struct Scripts {
    submit: Script,
    claim: Script,
    advance_wake: Script,
    promote: Script,
    reclaim: Script,
}

impl Default for Scripts {
    fn default() -> Self {
        Self::new()
    }
}

impl Scripts {
    pub fn new() -> Self {
        Self {
            submit: Script::new(include_str!("scripts/submit.lua")),
            claim: Script::new(include_str!("scripts/claim.lua")),
            advance_wake: Script::new(include_str!("scripts/advance_wake.lua")),
            promote: Script::new(include_str!("scripts/promote.lua")),
            reclaim: Script::new(include_str!("scripts/reclaim.lua")),
        }
    }
}

/// Result of `claim.lua`. Matches the three-state contract of the
/// `ClaimOutcome` enum in the core trait; the frontier orchestrator
/// translates this into the public type.
#[derive(Debug)]
pub enum ClaimRaw {
    Claimed {
        url_id: UrlId,
        entry: Box<UrlEntry>,
        host: String,
    },
    EmptyHint {
        soonest_ms: u64,
    },
    Empty,
}

#[derive(Debug, thiserror::Error)]
pub enum HostQueueError {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("redis parse: {0}")]
    Parse(#[from] redis::ParsingError),
    #[error("connection pool: {0}")]
    Pool(String),
    #[error("malformed url_id from claim.lua: {0}")]
    BadUrlId(String),
    #[error("codec: {0}")]
    Codec(String),
    #[error("missing payload in URL HASH for url_id={0}")]
    MissingPayload(String),
    #[error("lua returned an unrecognised tag: {0}")]
    BadTag(String),
}

/// Wrappers in one place so the orchestrator stays thin.
pub struct HostQueueOps<'a> {
    pub pool: &'a Pool<RedisConnectionManager>,
    pub keys: &'a KeyPrefix,
    pub scripts: &'a Scripts,
}

impl<'a> HostQueueOps<'a> {
    /// Submit one URL on `shard`. Returns the script's tri-state
    /// outcome (Queued / SkippedDuplicate / Overflowed).
    pub async fn submit(
        &self,
        shard: ShardKey,
        url_id: UrlId,
        entry: &UrlEntry,
        host: &str,
        max_host_backlog: u64,
        now_ms: i64,
    ) -> Result<SubmitOutcome, HostQueueError> {
        let payload =
            codec::encode(entry).map_err(|e| HostQueueError::Codec(e.to_string()))?;

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| HostQueueError::Pool(format!("{e:?}")))?;

        let outcome: i64 = self
            .scripts
            .submit
            .key(self.keys.seen(shard))
            .key(self.keys.urls(shard))
            .key(self.keys.host_queue(shard, host))
            .key(self.keys.overflow(shard))
            .key(self.keys.wake(shard))
            .arg(url_id.to_hex())
            .arg(host)
            .arg(payload)
            .arg(max_host_backlog)
            .arg(now_ms)
            .invoke_async(&mut *conn)
            .await?;

        Ok(match outcome {
            0 => SubmitOutcome::Queued,
            1 => SubmitOutcome::SkippedDuplicate,
            2 => SubmitOutcome::Overflowed,
            other => {
                return Err(HostQueueError::BadTag(format!(
                    "submit returned unexpected code {other}"
                )));
            }
        })
    }

    /// Claim one URL on `shard`. Returns the script's tri-state
    /// outcome; the orchestrator wraps this into the public
    /// `ClaimOutcome` (adding the `AttemptId` it owns the encoding
    /// of).
    pub async fn claim(
        &self,
        shard: ShardKey,
        now_ms: i64,
        lease_timeout_ms: i64,
    ) -> Result<ClaimRaw, HostQueueError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| HostQueueError::Pool(format!("{e:?}")))?;

        let value: Value = self
            .scripts
            .claim
            .key(self.keys.ready(shard))
            .key(self.keys.wake(shard))
            .key(self.keys.inflight(shard))
            .key(self.keys.urls(shard))
            .arg(self.keys.host_queue_prefix(shard))
            .arg(now_ms)
            .arg(lease_timeout_ms)
            .invoke_async(&mut *conn)
            .await?;

        parse_claim_value(value)
    }

    /// Set a host's wake-time. Idempotent.
    pub async fn advance_wake(
        &self,
        shard: ShardKey,
        host: &str,
        until_ms: i64,
    ) -> Result<(), HostQueueError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| HostQueueError::Pool(format!("{e:?}")))?;

        let _: i64 = self
            .scripts
            .advance_wake
            .key(self.keys.wake(shard))
            .key(self.keys.ready(shard))
            .arg(host)
            .arg(until_ms)
            .invoke_async(&mut *conn)
            .await?;
        Ok(())
    }

    /// Drain `wake` -> `ready` for hosts whose wake-time has elapsed.
    /// Bounded by `batch_limit` so a single pass can't dominate one
    /// Redis tick under heavy backlog.
    pub async fn promote(
        &self,
        shard: ShardKey,
        now_ms: i64,
        batch_limit: u64,
    ) -> Result<u64, HostQueueError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| HostQueueError::Pool(format!("{e:?}")))?;
        let n: i64 = self
            .scripts
            .promote
            .key(self.keys.wake(shard))
            .key(self.keys.ready(shard))
            .arg(now_ms)
            .arg(batch_limit)
            .invoke_async(&mut *conn)
            .await?;
        Ok(n as u64)
    }

    /// Re-push URLs whose lease has expired back onto their host
    /// queue. Bounded by `batch_limit`.
    pub async fn reclaim(
        &self,
        shard: ShardKey,
        now_ms: i64,
        batch_limit: u64,
    ) -> Result<u64, HostQueueError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| HostQueueError::Pool(format!("{e:?}")))?;
        let n: i64 = self
            .scripts
            .reclaim
            .key(self.keys.inflight(shard))
            .key(self.keys.wake(shard))
            .arg(now_ms)
            .arg(batch_limit)
            .arg(self.keys.host_queue_prefix(shard))
            .invoke_async(&mut *conn)
            .await?;
        Ok(n as u64)
    }

    /// Confirm a delivery. Direct Redis calls; no Lua needed
    /// (atomicity between the two ops is not safety-critical: a
    /// crash between ZREM and HDEL leaves an orphan URL HASH entry
    /// that the bloom would still suppress on a future submit).
    pub async fn ack(
        &self,
        shard: ShardKey,
        url_id: UrlId,
        host: &str,
    ) -> Result<(), HostQueueError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| HostQueueError::Pool(format!("{e:?}")))?;
        let member = format!("{}|{}", url_id.to_hex(), host);
        let _: i64 = redis::cmd("ZREM")
            .arg(self.keys.inflight(shard))
            .arg(&member)
            .query_async(&mut *conn)
            .await?;
        let _: i64 = redis::cmd("HDEL")
            .arg(self.keys.urls(shard))
            .arg(url_id.to_hex())
            .query_async(&mut *conn)
            .await?;
        Ok(())
    }
}

fn parse_claim_value(value: Value) -> Result<ClaimRaw, HostQueueError> {
    let parts = <Vec<Value> as FromRedisValue>::from_redis_value(value)?;
    let mut iter = parts.into_iter();
    let tag = <String as FromRedisValue>::from_redis_value(
        iter.next()
            .ok_or_else(|| HostQueueError::BadTag("claim.lua returned empty array".into()))?,
    )?;
    match tag.as_str() {
        "empty" => Ok(ClaimRaw::Empty),
        "empty_hint" => {
            let score_v = iter
                .next()
                .ok_or_else(|| HostQueueError::BadTag("empty_hint missing score".into()))?;
            let soonest_ms = <u64 as FromRedisValue>::from_redis_value(score_v)?;
            Ok(ClaimRaw::EmptyHint { soonest_ms })
        }
        "claimed" => {
            let url_id_v = iter
                .next()
                .ok_or_else(|| HostQueueError::BadTag("claimed missing url_id".into()))?;
            let url_id_hex = <String as FromRedisValue>::from_redis_value(url_id_v)?;
            let host_v = iter
                .next()
                .ok_or_else(|| HostQueueError::BadTag("claimed missing host".into()))?;
            let host = <String as FromRedisValue>::from_redis_value(host_v)?;
            let payload_v = iter
                .next()
                .ok_or_else(|| HostQueueError::MissingPayload(url_id_hex.clone()))?;
            let payload = <Vec<u8> as FromRedisValue>::from_redis_value(payload_v)?;
            let url_id = UrlId::from_hex(&url_id_hex)
                .ok_or_else(|| HostQueueError::BadUrlId(url_id_hex.clone()))?;
            let entry =
                codec::decode(&payload).map_err(|e| HostQueueError::Codec(e.to_string()))?;
            Ok(ClaimRaw::Claimed {
                url_id,
                entry: Box::new(entry),
                host,
            })
        }
        other => Err(HostQueueError::BadTag(other.into())),
    }
}
