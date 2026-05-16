//! Internal error type for the politeness crate.
//!
//! Each sub-impl (wake planner, robots checker, backoff tracker,
//! robots cache) produces this error from its Redis paths and
//! converts to [`crawlrs_core::Error::Politeness`] at the trait
//! boundary. The type stays `pub(crate)` because nothing outside
//! the crate constructs or matches on it; the domain `Error` is
//! the public surface.

use crawlrs_core::{Error, ShardKey};
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub(crate) enum PolitenessError {
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

impl From<PolitenessError> for Error {
    fn from(e: PolitenessError) -> Self {
        Error::Politeness(e.to_string())
    }
}

pub(crate) type LocalResult<T> = std::result::Result<T, PolitenessError>;
