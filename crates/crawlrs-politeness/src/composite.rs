//! `CompositePoliteness`: orchestrator that satisfies
//! [`crawlrs_core::Politeness`] by delegating to three sub-traits
//! ([`WakePlanner`], [`RobotsChecker`], [`BackoffTracker`]).
//!
//! Splitting `Politeness` into sub-traits means a deployment can
//! swap any sub-impl independently: Redis-backed wake planning
//! with no-op robots; in-memory test doubles for unit tests; the
//! whole layer wired to no-ops when `politeness.enabled = false`.
//! The composite is the only `Politeness`-implementing type the
//! runtime sees; it is unaware of which sub-impls back it.
//!
//! Concerns NOT owned here:
//!
//! - Access blocklist (`[access].blocklist`): consulted by the
//!   worker before `politeness.check` is even called. The
//!   composite is purely host-as-guest behavior.
//! - Crawl scope (`[crawl]`): depth caps live on
//!   `WorkerDeps.crawl_scope`; URL-count quotas live on
//!   `RedisFrontier`'s scope and are enforced at submit time.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    BackoffTracker, CanonicalUrl, Error, FailureKind, Fetcher, NextWake, PoliteDecision,
    Politeness, Result, RobotsChecker, ShardKey, ShardingPolicy, WakePlanner,
};
use tracing::info;

use crate::backoff_tracker::RedisBackoffTracker;
use crate::config::PolitenessConfig;
use crate::error::PolitenessError;
use crate::keys::KeyPrefix;
use crate::robots::RobotsCache;
use crate::robots_checker::RedisRobotsChecker;
use crate::wake_planner::RedisWakePlanner;

/// Composed `Politeness` impl. Holds three sub-trait Arcs.
/// Constructed via [`Self::new`] (Redis-backed sub-impls, the
/// default in production) or via [`Self::from_parts`] (any
/// sub-impl mix; used by the factory's noop branch and by tests).
pub struct CompositePoliteness {
    wake: Arc<dyn WakePlanner>,
    robots: Arc<dyn RobotsChecker>,
    backoff: Arc<dyn BackoffTracker>,
    config: PolitenessConfig,

    /// Held purely so the maintenance loop can report the bb8
    /// pool gauge for the politeness layer. `None` when the
    /// composite is built with all-noop sub-impls (no Redis pool
    /// to report on).
    pool: Option<Pool<RedisConnectionManager>>,

    /// Held for the `robots()` introspection accessor used by
    /// debugging callers that want the robots-only decision
    /// without going through the full `check`. `None` when the
    /// composite is built with a non-Redis robots checker.
    robots_cache: Option<Arc<RobotsCache>>,
}

impl std::fmt::Debug for CompositePoliteness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositePoliteness")
            .field("host_delay", &self.config.host_delay)
            .field("obey_robots_txt", &self.config.obey_robots_txt)
            .field("per_domain_overrides", &self.config.per_domain.len())
            .finish()
    }
}

impl CompositePoliteness {
    /// Construct a Redis-backed composite (the production wiring).
    /// Builds [`RedisWakePlanner`], [`RedisRobotsChecker`], and
    /// [`RedisBackoffTracker`] from the shared pool + config and
    /// composes them.
    ///
    /// Access (`[access]`) and crawl scope (`[crawl]`) do not
    /// appear here: the worker consults the blocklist before
    /// calling `politeness.check`, depth caps are read by the
    /// worker directly from `WorkerDeps.crawl_scope`, and URL-count
    /// quotas are enforced atomically at submit time by
    /// `Frontier::submit_batch`. The politeness layer is purely
    /// host-as-guest behavior.
    pub async fn new(
        pool: Pool<RedisConnectionManager>,
        sharding_policy: Arc<dyn ShardingPolicy>,
        owned_shards: Vec<ShardKey>,
        fetcher: Arc<dyn Fetcher>,
        run_id: impl Into<String>,
        config: PolitenessConfig,
    ) -> Result<Self> {
        let keys = KeyPrefix::new(run_id);
        let count = sharding_policy.shard_count();
        for &shard in &owned_shards {
            if shard >= count {
                return Err(PolitenessError::ShardOutOfRange { got: shard, count }.into());
            }
        }

        let robots_cache = Arc::new(RobotsCache::new(
            pool.clone(),
            keys.clone(),
            sharding_policy.clone(),
            fetcher,
            config.user_agent.clone(),
            config.robots_ttl,
        ));

        let wake: Arc<dyn WakePlanner> = Arc::new(RedisWakePlanner::new(
            sharding_policy.clone(),
            owned_shards.clone(),
            config.clone(),
        ));
        let robots: Arc<dyn RobotsChecker> = Arc::new(RedisRobotsChecker::new(
            robots_cache.clone(),
            config.user_agent.clone(),
        ));
        let backoff: Arc<dyn BackoffTracker> = Arc::new(RedisBackoffTracker::new(
            pool.clone(),
            keys,
            sharding_policy,
            owned_shards.clone(),
            config.backoff.clone(),
        ));

        info!(
            owned_shards = ?owned_shards,
            host_delay_ms = config.host_delay.as_millis() as u64,
            obey_robots_txt = config.obey_robots_txt,
            "CompositePoliteness (redis-backed) ready",
        );

        Ok(Self {
            wake,
            robots,
            backoff,
            config,
            pool: Some(pool),
            robots_cache: Some(robots_cache),
        })
    }

    /// Construct from arbitrary sub-trait Arcs. Used by the
    /// factory's `politeness.enabled = false` branch (all-noop)
    /// and by tests that mix-and-match fakes.
    pub fn from_parts(
        wake: Arc<dyn WakePlanner>,
        robots: Arc<dyn RobotsChecker>,
        backoff: Arc<dyn BackoffTracker>,
        config: PolitenessConfig,
    ) -> Self {
        Self {
            wake,
            robots,
            backoff,
            config,
            pool: None,
            robots_cache: None,
        }
    }

    pub fn config(&self) -> &PolitenessConfig {
        &self.config
    }

    /// Snapshot of the bb8 pool state. Returns `None` when the
    /// composite was built with all-noop sub-impls (no Redis).
    pub fn pool_state(&self) -> Option<bb8::State> {
        self.pool.as_ref().map(|p| p.state())
    }

    /// Refresh the `crawlrs_politeness_pool_pending` gauge from
    /// the bb8 pool's published state. No-op when there's no pool
    /// (all-noop wiring). Called by the binary's maintenance loop
    /// per scrape interval.
    pub fn record_pool_metrics(&self) {
        let Some(state) = self.pool_state() else {
            return;
        };
        let active = state.connections.saturating_sub(state.idle_connections);
        metrics::gauge!(crate::metrics::POLITENESS_POOL_PENDING).set(active as f64);
    }

    /// Borrow the underlying robots cache. `None` when the
    /// composite was built with a non-Redis robots checker
    /// (typically: tests or `politeness.enabled = false`).
    pub fn robots(&self) -> Option<&RobotsCache> {
        self.robots_cache.as_deref()
    }

    fn effective_obey_robots(&self, host: &str) -> bool {
        self.config
            .per_domain
            .get(host)
            .and_then(|o| o.obey_robots_txt)
            .unwrap_or(self.config.obey_robots_txt)
    }
}

#[async_trait]
impl Politeness for CompositePoliteness {
    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn check(&self, url: &CanonicalUrl) -> Result<PoliteDecision> {
        let host = url
            .host()
            .ok_or_else(|| PolitenessError::NoHost(url.as_str().into()))
            .map_err(Error::from)?;

        if self.backoff.is_open(host).await? {
            metrics::counter!(crate::metrics::POLITENESS_CIRCUIT_OPEN_TOTAL).increment(1);
            record_check_decision(crate::metrics::DECISION_DISALLOW_CIRCUIT);
            return Ok(PoliteDecision::Disallow);
        }

        if self.effective_obey_robots(host) && !self.robots.allowed(url).await? {
            record_check_decision(crate::metrics::DECISION_DISALLOW_ROBOTS);
            return Ok(PoliteDecision::Disallow);
        }

        // Wake-time pacing is enforced by the frontier (the host's
        // claimable state is gated on the wake ZSET); URL-count
        // quota is enforced at submit time by `Frontier::submit_batch`.
        // No further gate fires in this trait.
        record_check_decision(crate::metrics::DECISION_ALLOW);
        Ok(PoliteDecision::Allow)
    }

    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn record_fetch(&self, url: &CanonicalUrl) -> Result<NextWake> {
        let host = url
            .host()
            .ok_or_else(|| PolitenessError::NoHost(url.as_str().into()))
            .map_err(Error::from)?;

        // A successful fetch clears any per-host failure state.
        // The trait's default `reset_on_success` is `Ok(())`, so
        // stateless impls (noop, fakes) inherit a no-op for free.
        self.backoff.reset_on_success(host).await?;

        self.wake.record_fetch(host).await
    }

    #[tracing::instrument(skip(self), fields(url = %url, kind = ?kind))]
    async fn record_failure(
        &self,
        url: &CanonicalUrl,
        kind: FailureKind,
        retry_after: Option<Duration>,
    ) -> Result<NextWake> {
        self.backoff.record_failure(url, kind, retry_after).await
    }
}

/// Emit the `crawlrs_politeness_check_total{decision}` counter.
/// Bounded label set; no cardinality concern.
fn record_check_decision(decision: &'static str) {
    metrics::counter!(
        crate::metrics::POLITENESS_CHECK_TOTAL,
        "decision" => decision,
    )
    .increment(1);
}
