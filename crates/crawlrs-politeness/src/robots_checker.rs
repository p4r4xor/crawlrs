//! Redis-backed `RobotsChecker` impl.
//!
//! Thin wrapper over the existing [`RobotsCache`] (in
//! [`crate::robots`]); the cache owns the in-process LRU + Redis
//! hash + on-miss HTTP fetch. This adapter narrows the cache's
//! interface to the trait surface and threads through the
//! configured user-agent product token for RFC 9309 matching.

use std::sync::Arc;

use async_trait::async_trait;
use crawlrs_core::{CanonicalUrl, Error, Result, RobotsChecker};

use crate::robots::RobotsCache;

pub struct RedisRobotsChecker {
    robots: Arc<RobotsCache>,
    user_agent: String,
}

impl RedisRobotsChecker {
    pub fn new(robots: Arc<RobotsCache>, user_agent: String) -> Self {
        Self { robots, user_agent }
    }

    /// Borrow the underlying cache. Used by `CompositePoliteness`
    /// for the introspection accessor that the runtime exposes
    /// for debugging.
    pub fn cache(&self) -> &RobotsCache {
        &self.robots
    }
}

#[async_trait]
impl RobotsChecker for RedisRobotsChecker {
    async fn allowed(&self, url: &CanonicalUrl) -> Result<bool> {
        self.robots
            .allowed(url, &self.user_agent)
            .await
            .map_err(Error::from)
    }
}
