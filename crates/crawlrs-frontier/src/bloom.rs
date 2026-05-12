//! RedisBloom thin wrappers.
//!
//! Submit-time dedup uses RedisBloom (`BF.RESERVE`, `BF.ADD`,
//! `BF.EXISTS`). One filter per shard, sized for the expected total
//! URL volume of the run with the configured false-positive rate.
//!
//! BLOOM SEMANTICS THE FRONTIER RELIES ON:
//! - `BF.ADD` is atomic check-and-set: returns 1 when newly added, 0
//!   when already present. Used in `submit.lua` to dedup in one
//!   round-trip.
//! - `BF.RESERVE` is idempotent across processes once the filter
//!   exists (returns an "item exists" error which the constructor
//!   treats as success).
//! - Filters survive RDB snapshots; a restart picks up the same
//!   dedup state.
//!
//! Requires Redis Stack (or stock Redis with the RedisBloom module
//! loaded). The constructor surfaces a clear error if the module is
//! missing so deployments fail fast instead of mis-deduping at
//! runtime.

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
#[cfg(test)]
use redis::AsyncCommands;

/// Configuration for the per-shard bloom filter.
#[derive(Debug, Clone, Copy)]
pub struct BloomConfig {
    /// Expected total URLs the run will submit. Sized once at startup;
    /// RedisBloom grows scalably past the initial capacity (with a
    /// false-positive rate hit on the second-tier filter), but it's
    /// cheaper to size conservatively up front.
    pub capacity: u64,
    /// Target false-positive rate at `capacity`. 0.001 (0.1%) is the
    /// default and uses ~1.8 bytes per URL.
    pub fpr: f64,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self {
            capacity: 1_000_000,
            fpr: 0.001,
        }
    }
}

/// Ensure the bloom filter exists at `key`. Idempotent across
/// processes: a peer racing the `BF.RESERVE` loses with "ERR item
/// exists", which we treat as success.
pub async fn reserve(pool: &Pool<RedisConnectionManager>, key: &str, config: BloomConfig) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("bloom reserve checkout: {e:?}"))?;
    let result: redis::RedisResult<()> = redis::cmd("BF.RESERVE")
        .arg(key)
        .arg(config.fpr)
        .arg(config.capacity)
        .query_async(&mut *conn)
        .await;
    match result {
        Ok(()) => Ok(()),
        Err(e) => {
            // RedisBloom returns "ERR item exists" when the filter
            // already exists; treat as success. Anything else is real.
            let msg = e.to_string();
            if msg.contains("item exists") {
                Ok(())
            } else if msg.contains("unknown command") {
                Err(format!(
                    "RedisBloom not available (BF.RESERVE rejected): {msg}. \
                     The frontier requires Redis Stack or stock Redis with \
                     the redisbloom module loaded."
                ))
            } else {
                Err(format!("BF.RESERVE failed: {msg}"))
            }
        }
    }
}

/// Test-only: check if a URL ID is present in the bloom filter.
/// Always returns `false` if the filter has never been ADD'd to. Not
/// used on the hot path (the `submit.lua` script calls `BF.ADD`
/// directly and inspects the return value); useful for assertions
/// in integration tests.
#[cfg(test)]
pub async fn exists(pool: &Pool<RedisConnectionManager>, key: &str, url_id_hex: &str) -> Result<bool, String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| format!("bloom exists checkout: {e:?}"))?;
    let present: bool = conn
        .exists(key)
        .await
        .map_err(|e: redis::RedisError| format!("EXISTS check: {e}"))?;
    if !present {
        return Ok(false);
    }
    let hit: bool = redis::cmd("BF.EXISTS")
        .arg(key)
        .arg(url_id_hex)
        .query_async(&mut *conn)
        .await
        .map_err(|e: redis::RedisError| format!("BF.EXISTS: {e}"))?;
    Ok(hit)
}
