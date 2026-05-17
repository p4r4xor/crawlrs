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
pub(crate) struct Scripts {
    submit: Script,
    submit_batch: Script,
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
    pub(crate) fn new() -> Self {
        Self {
            submit: Script::new(include_str!("scripts/submit.lua")),
            submit_batch: Script::new(include_str!("scripts/submit_batch.lua")),
            claim: Script::new(include_str!("scripts/claim.lua")),
            advance_wake: Script::new(include_str!("scripts/advance_wake.lua")),
            promote: Script::new(include_str!("scripts/promote.lua")),
            reclaim: Script::new(include_str!("scripts/reclaim.lua")),
        }
    }
}

/// One URL's input to [`HostQueueOps::submit_batch`].
///
/// Bundled into a named struct so each field's role at the call
/// site is unambiguous: tuples-of-four made the per-URL payload
/// noisy at every line.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SubmitItem<'a> {
    pub url_id: UrlId,
    pub entry: &'a UrlEntry,
    pub host: &'a str,
    /// Effective `[crawl].max_urls` for this host, or `-1` when the
    /// host is uncapped. The Lua script branches on the sign: a
    /// non-negative value gates the `BF.ADD` on a counter check;
    /// `-1` skips both the GET and the INCR entirely.
    pub max_urls: i64,
}

/// Result of `claim.lua`. Matches the three-state contract of the
/// `ClaimOutcome` enum in the core trait; the frontier orchestrator
/// translates this into the public type.
#[derive(Debug)]
pub(crate) enum ClaimRaw {
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
pub(crate) struct HostQueueOps<'a> {
    pub pool: &'a Pool<RedisConnectionManager>,
    pub keys: &'a KeyPrefix,
    pub scripts: &'a Scripts,
}

impl<'a> HostQueueOps<'a> {
    /// Submit one URL on `shard`. Returns the script's two-state
    /// outcome (Queued / SkippedDuplicate).
    pub(crate) async fn submit(
        &self,
        shard: ShardKey,
        url_id: UrlId,
        entry: &UrlEntry,
        host: &str,
        now_ms: i64,
    ) -> Result<SubmitOutcome, HostQueueError> {
        let payload = codec::encode(entry).map_err(|e| HostQueueError::Codec(e.to_string()))?;

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
            .key(self.keys.wake(shard))
            .arg(url_id.to_hex())
            .arg(host)
            .arg(payload)
            .arg(now_ms)
            .invoke_async(&mut *conn)
            .await?;

        Ok(match outcome {
            0 => SubmitOutcome::Queued,
            1 => SubmitOutcome::SkippedDuplicate,
            other => {
                return Err(HostQueueError::BadTag(format!(
                    "submit returned unexpected code {other}"
                )));
            }
        })
    }

    /// Submit many URLs on one `shard` in a single Lua call. Each
    /// [`SubmitItem`] is processed by `submit_batch.lua` with the
    /// counter-first quota check + bloom dedup + enqueue flow; N
    /// submits collapse from N Redis round-trips to 1.
    ///
    /// All URLs in `items` must hash to the same `shard`; cross-shard
    /// batching is the caller's responsibility (a single Lua script
    /// touches a single cluster slot).
    ///
    /// Returns `(queued, rejected)`. Bloom duplicates are the
    /// remainder: `items.len() - queued - rejected`.
    pub(crate) async fn submit_batch(
        &self,
        shard: ShardKey,
        items: &[SubmitItem<'_>],
        now_ms: i64,
    ) -> Result<(usize, usize), HostQueueError> {
        if items.is_empty() {
            return Ok((0, 0));
        }

        // Encode all payloads upfront so a codec failure aborts before
        // we touch Redis. Avoids a partial-batch outcome where Redis
        // sees half the URLs and the caller sees an error.
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(items.len());
        for item in items {
            payloads
                .push(codec::encode(item.entry).map_err(|e| HostQueueError::Codec(e.to_string()))?);
        }

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| HostQueueError::Pool(format!("{e:?}")))?;

        let mut invocation = self.scripts.submit_batch.prepare_invoke();
        invocation.key(self.keys.seen(shard));
        invocation.key(self.keys.urls(shard));
        invocation.key(self.keys.wake(shard));
        for item in items {
            invocation.key(self.keys.host_queue(shard, item.host));
            invocation.key(self.keys.host_count(shard, item.host));
        }
        invocation.arg(items.len() as u64);
        invocation.arg(now_ms);
        for (item, payload) in items.iter().zip(payloads.iter()) {
            invocation.arg(item.url_id.to_hex());
            invocation.arg(item.host);
            invocation.arg(payload.as_slice());
            invocation.arg(item.max_urls);
        }

        let raw: Value = invocation.invoke_async(&mut *conn).await?;
        parse_submit_batch_value(raw)
    }

    /// Claim one URL on `shard`. Returns the script's tri-state
    /// outcome; the orchestrator wraps this into the public
    /// `ClaimOutcome` (adding the `AttemptId` it owns the encoding
    /// of).
    pub(crate) async fn claim(
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
    pub(crate) async fn advance_wake(
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
    pub(crate) async fn promote(
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
    pub(crate) async fn reclaim(
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
    pub(crate) async fn ack(
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

fn parse_submit_batch_value(value: Value) -> Result<(usize, usize), HostQueueError> {
    // submit_batch.lua returns a two-element array {queued, rejected}.
    let parts = <Vec<Value> as FromRedisValue>::from_redis_value(value)?;
    let mut iter = parts.into_iter();
    let queued_v = iter
        .next()
        .ok_or_else(|| HostQueueError::BadTag("submit_batch.lua returned empty array".into()))?;
    let rejected_v = iter
        .next()
        .ok_or_else(|| HostQueueError::BadTag("submit_batch.lua missing rejected".into()))?;
    let queued = <i64 as FromRedisValue>::from_redis_value(queued_v)?;
    let rejected = <i64 as FromRedisValue>::from_redis_value(rejected_v)?;
    Ok((queued as usize, rejected as usize))
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
