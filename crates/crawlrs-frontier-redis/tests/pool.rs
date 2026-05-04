//! Tests for `validate_pool_size`: Redis pool max_size vs worker count
//! sanity check.

use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_frontier_redis::pool::POOL_HEADROOM;
use crawlrs_frontier_redis::{PoolSizeError, validate_pool_size};

/// Small fake of bb8's builder pattern just enough to construct a
/// pool with a known max_size; we never call `.connect()` on it, so
/// the manager URL is irrelevant.
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
