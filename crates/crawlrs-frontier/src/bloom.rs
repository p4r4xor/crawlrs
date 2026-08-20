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
//! missing so deployments fail fast instead of silently dropping
//! every submit as a duplicate at runtime.

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use thiserror::Error;

/// Failure modes of [`reserve`].
///
/// `AlreadyExists` is not a fault: `BF.RESERVE` is idempotent across
/// processes, so a peer winning the race is the caller's success case.
/// It is surfaced as a distinct variant (rather than folded into `Ok`)
/// so the caller decides how to treat it, and so a real backend error
/// can never be silently reclassified as "already reserved".
#[derive(Debug, Error)]
pub enum BloomError {
    /// The filter already exists; a concurrent peer reserved it first.
    #[error("bloom filter already exists")]
    AlreadyExists,
    /// `BF.RESERVE` was rejected as an unknown command: the RedisBloom
    /// module is not loaded. The frontier requires Redis Stack or stock
    /// Redis with the redisbloom module.
    #[error("RedisBloom module not loaded (BF.RESERVE rejected as unknown command)")]
    ModuleMissing(#[source] redis::RedisError),
    /// A Redis connection could not be checked out of the pool.
    #[error("bloom reserve connection checkout failed")]
    Pool(#[source] bb8::RunError<redis::RedisError>),
    /// `BF.RESERVE` failed for any other reason.
    #[error("BF.RESERVE failed")]
    Backend(#[source] redis::RedisError),
}

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
/// exists", surfaced here as [`BloomError::AlreadyExists`].
///
/// # Errors
///
/// Returns [`BloomError::AlreadyExists`] if the filter is already
/// reserved (the caller treats this as success),
/// [`BloomError::ModuleMissing`] if RedisBloom is not loaded,
/// [`BloomError::Pool`] if a connection cannot be checked out, and
/// [`BloomError::Backend`] for any other `BF.RESERVE` failure.
pub(crate) async fn reserve(
    pool: &Pool<RedisConnectionManager>,
    key: &str,
    config: BloomConfig,
) -> Result<(), BloomError> {
    let mut conn = pool.get().await.map_err(BloomError::Pool)?;
    let result: redis::RedisResult<()> = redis::cmd("BF.RESERVE")
        .arg(key)
        .arg(config.fpr)
        .arg(config.capacity)
        .query_async(&mut *conn)
        .await;
    match result {
        Ok(()) => Ok(()),
        Err(e) => {
            // RedisBloom does not expose distinct error codes for these
            // cases, so the detail string is the only discriminator the
            // module gives us. "item exists" means a peer reserved the
            // filter first; "unknown command" means the module is not
            // loaded. Anything else is a genuine backend failure and
            // keeps the original error as its source.
            let msg = e.to_string();
            if msg.contains("item exists") {
                Err(BloomError::AlreadyExists)
            } else if msg.contains("unknown command") {
                Err(BloomError::ModuleMissing(e))
            } else {
                Err(BloomError::Backend(e))
            }
        }
    }
}
