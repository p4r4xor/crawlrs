//! Pool-size sanity check.
//!
//! Andrew Chan's 10M-URL crawl spent months debugging "why is throughput
//! flat?" before discovering that 500 workers were fighting for 80
//! Postgres connections; P50 acquisition latency was 1.8s. The same
//! foot-gun is one misconfiguration away in any setup that wires a
//! shared `bb8::Pool` to a worker count.
//!
//! [`validate_pool_size`] is a startup-time guard: given the configured
//! pool's `max_size` and the worker count, it returns an error if the
//! pool is sized below `workers + headroom`. Headroom covers the
//! maintenance task and any short-lived ad-hoc connections.
//!
//! Call this from your wiring code before [`crate::RedisFrontier::new`].
//! Failing fast at startup is dramatically cheaper than diagnosing
//! latency saturation in prod.

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use thiserror::Error;

/// Connections reserved beyond worker count: maintenance loop, robots
/// fetch path, ad-hoc admin queries. Two is conservative.
pub const POOL_HEADROOM: u32 = 2;

#[derive(Debug, Error)]
pub enum PoolSizeError {
    #[error(
        "redis pool max_size={pool_max} is below the configured worker count + headroom \
         ({workers} workers + {headroom} headroom = {required}); raise the pool size or \
         lower the worker count"
    )]
    Undersized {
        pool_max: u32,
        workers: u32,
        headroom: u32,
        required: u32,
    },
}

/// Returns Ok if the pool's max_size is at least `workers + POOL_HEADROOM`.
/// Call once at startup, before spawning workers.
pub fn validate_pool_size(
    pool: &Pool<RedisConnectionManager>,
    workers: u32,
) -> Result<(), PoolSizeError> {
    let pool_max = pool.config().max_size;
    let required = workers + POOL_HEADROOM;
    if pool_max < required {
        return Err(PoolSizeError::Undersized {
            pool_max,
            workers,
            headroom: POOL_HEADROOM,
            required,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Small fake of bb8's builder pattern just enough to construct a
    /// pool with a known max_size; we never call `.connect()` on it,
    /// so the manager URL is irrelevant.
    async fn pool_with_max(max_size: u32) -> Pool<RedisConnectionManager> {
        let mgr = RedisConnectionManager::new("redis://127.0.0.1:1/").unwrap();
        Pool::builder()
            .max_size(max_size)
            // Keep the pool from trying to connect during the test.
            .min_idle(Some(0))
            .build_unchecked(mgr)
    }

    #[tokio::test]
    async fn rejects_when_pool_too_small() {
        let pool = pool_with_max(4).await;
        let err = validate_pool_size(&pool, 8).unwrap_err();
        assert!(matches!(
            err,
            PoolSizeError::Undersized {
                pool_max: 4,
                workers: 8,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn accepts_when_pool_meets_headroom() {
        let pool = pool_with_max(10).await;
        validate_pool_size(&pool, 8).unwrap();
    }

    #[tokio::test]
    async fn boundary_is_workers_plus_headroom() {
        let exact = pool_with_max(POOL_HEADROOM + 4).await;
        validate_pool_size(&exact, 4).unwrap();
        let one_short = pool_with_max(POOL_HEADROOM + 4 - 1).await;
        assert!(validate_pool_size(&one_short, 4).is_err());
    }
}
