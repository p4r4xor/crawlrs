//! Redis-backed `RateLimiter` impl.
//!
//! Owns the per-host `fetch_count` counter (used for the
//! `crawl.max_urls` quota) and computes the `NextWake` plan from
//! the configured `host_delay` plus any per-domain override.
//!
//! Wake-time *storage* lives in the frontier; this impl only
//! computes the plan and returns it. The composite passes the
//! plan to the runtime, which writes it via
//! `Frontier::advance_wake`.
//!
//! **Zero-delay short-circuit.** When every effective `host_delay`
//! (global + every `per_domain` override) is `Duration::ZERO`,
//! `record_fetch` returns `NextWake.until = Instant::now()`
//! directly without consulting the per-host override map. The
//! worker's wake-application path uses that signal to skip the
//! `Frontier::advance_wake` round-trip (no point writing a wake
//! that's already in the past). The verdict is cached at
//! construction; changing it requires rebuilding the limiter.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use crawlrs_core::{
    CanonicalUrl, CrawlScope, Error, NextWake, PoliteDecision, RateLimiter, Result, ShardKey,
    ShardingPolicy,
};
use redis::AsyncCommands;
use tracing::warn;

use crate::config::PolitenessConfig;
use crate::error::{LocalResult, RedisPolitenessError};
use crate::keys::KeyPrefix;

/// Per-host pacing backed by Redis. `check` enforces the per-host
/// URL-count quota (when configured); `record_fetch` increments
/// the counter and returns the next-wake plan.
///
/// The quota check + INCR live here until the frontier's submit
/// pipeline takes ownership of per-host counters; at that point
/// the `CrawlScope` argument and the `fetch_count` key go away and
/// this impl becomes purely a wake-time planner.
pub struct RedisRateLimiter {
    pool: Pool<RedisConnectionManager>,
    keys: KeyPrefix,
    sharding_policy: Arc<dyn ShardingPolicy>,
    owned_shards: Vec<ShardKey>,
    config: PolitenessConfig,
    crawl_scope: CrawlScope,
    /// True when there's no per-host pacing to enforce: the
    /// global `host_delay` is zero AND every `per_domain` override
    /// is either unset (inherits the zero global) or explicitly
    /// zero. Cached at construction; see the module-level
    /// "Zero-delay short-circuit" note.
    pacing_disabled: bool,
}

impl RedisRateLimiter {
    pub fn new(
        pool: Pool<RedisConnectionManager>,
        keys: KeyPrefix,
        sharding_policy: Arc<dyn ShardingPolicy>,
        owned_shards: Vec<ShardKey>,
        config: PolitenessConfig,
        crawl_scope: CrawlScope,
    ) -> Self {
        let pacing_disabled = pacing_disabled(&config);
        Self {
            pool,
            keys,
            sharding_policy,
            owned_shards,
            config,
            crawl_scope,
            pacing_disabled,
        }
    }

    async fn checkout(&self) -> LocalResult<bb8::PooledConnection<'_, RedisConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| RedisPolitenessError::Pool(format!("{e:?}")))
    }

    fn assert_owned(&self, shard: ShardKey) -> LocalResult<()> {
        if !self.owned_shards.contains(&shard) {
            return Err(RedisPolitenessError::ShardNotOwned {
                got: shard,
                owned: self.owned_shards.clone(),
            });
        }
        Ok(())
    }

    fn effective_host_delay(&self, host: &str) -> Duration {
        self.config
            .per_domain
            .get(host)
            .and_then(|o| o.host_delay)
            .unwrap_or(self.config.host_delay)
    }
}

#[async_trait]
impl RateLimiter for RedisRateLimiter {
    async fn check(&self, url: &CanonicalUrl) -> Result<PoliteDecision> {
        let host = url
            .host()
            .ok_or_else(|| RedisPolitenessError::NoHost(url.as_str().into()))
            .map_err(Error::from)?;

        let Some(cap) = self.crawl_scope.max_urls_for(host) else {
            return Ok(PoliteDecision::Allow);
        };

        let shard = self.sharding_policy.shard_key(url);
        self.assert_owned(shard).map_err(Error::from)?;

        let mut conn = self.checkout().await.map_err(Error::from)?;
        let count_key = self.keys.fetch_count(shard, host);
        let count: Option<u64> = conn
            .get(&count_key)
            .await
            .map_err(RedisPolitenessError::from)
            .map_err(Error::from)?;
        if count.unwrap_or(0) >= cap {
            return Ok(PoliteDecision::Disallow);
        }
        Ok(PoliteDecision::Allow)
    }

    async fn record_fetch(&self, host: &str) -> Result<NextWake> {
        let shard = self.sharding_policy.shard_key_from_host(host);
        self.assert_owned(shard).map_err(Error::from)?;

        if self.crawl_scope.has_any_quota() {
            let count_key = self.keys.fetch_count(shard, host);
            let mut conn = self.checkout().await.map_err(Error::from)?;
            let incr: redis::RedisResult<u64> = conn.incr(&count_key, 1u64).await;
            if let Err(e) = incr {
                warn!(
                    shard,
                    host,
                    error = %e,
                    "record_fetch: fetch_count incr failed; quota counter drifts",
                );
            }
        }

        let delay = if self.pacing_disabled {
            Duration::ZERO
        } else {
            self.effective_host_delay(host)
        };
        Ok(NextWake {
            host: host.to_string(),
            until: Instant::now() + delay,
        })
    }
}

/// `true` when there's no per-host pacing to enforce: the
/// global `host_delay` is zero AND every `per_domain` override
/// is either unset (inherits the zero global) or explicitly
/// zero. Used once at construction; not on the hot path.
pub(crate) fn pacing_disabled(config: &PolitenessConfig) -> bool {
    if !config.host_delay.is_zero() {
        return false;
    }
    config
        .per_domain
        .values()
        .all(|o| o.host_delay.is_none_or(|d| d.is_zero()))
}

#[cfg(test)]
mod tests {
    // Inline because: visibility-forced. `pacing_disabled` is
    // `pub(crate)` so the predicate stays internal to the
    // politeness crate; a `tests/` file compiles as a separate
    // crate and can't see crate-private items.

    use super::*;
    use crate::PolitenessOverride;
    use crate::config::PolitenessConfig;

    fn config_with(global: Duration) -> PolitenessConfig {
        PolitenessConfig {
            host_delay: global,
            ..Default::default()
        }
    }

    #[test]
    fn pacing_disabled_when_global_is_zero_and_no_overrides() {
        let config = config_with(Duration::ZERO);
        assert!(pacing_disabled(&config));
    }

    #[test]
    fn pacing_enabled_when_global_is_nonzero() {
        let config = config_with(Duration::from_millis(1));
        assert!(!pacing_disabled(&config));
    }

    #[test]
    fn pacing_enabled_when_any_per_domain_override_is_nonzero() {
        let mut config = config_with(Duration::ZERO);
        config.per_domain.insert(
            "slow.test".into(),
            PolitenessOverride {
                host_delay: Some(Duration::from_secs(5)),
                obey_robots_txt: None,
                robots_ttl: None,
            },
        );
        assert!(
            !pacing_disabled(&config),
            "any non-zero per_domain delay re-enables pacing",
        );
    }

    #[test]
    fn pacing_disabled_when_per_domain_override_is_explicit_zero() {
        let mut config = config_with(Duration::ZERO);
        config.per_domain.insert(
            "fast.test".into(),
            PolitenessOverride {
                host_delay: Some(Duration::ZERO),
                obey_robots_txt: None,
                robots_ttl: None,
            },
        );
        assert!(pacing_disabled(&config));
    }

    #[test]
    fn pacing_disabled_when_per_domain_override_inherits_global() {
        // `host_delay: None` on an override means "inherit the
        // global." Global is zero, so the effective delay for
        // this host is zero too.
        let mut config = config_with(Duration::ZERO);
        config.per_domain.insert(
            "inherit.test".into(),
            PolitenessOverride {
                host_delay: None,
                obey_robots_txt: Some(false),
                robots_ttl: None,
            },
        );
        assert!(pacing_disabled(&config));
    }
}
