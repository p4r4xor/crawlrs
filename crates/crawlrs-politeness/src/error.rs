//! Shared error type for the Redis-backed politeness sub-impls.
//!
//! Each sub-impl (rate limiter, robots checker, backoff tracker)
//! produces this internal error and converts to
//! [`crawlrs_core::Error::Politeness`] at the trait boundary.

use crawlrs_core::{Error, ShardKey};
use thiserror::Error as ThisError;

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

pub type LocalResult<T> = std::result::Result<T, RedisPolitenessError>;
