//! Redis-backed `WakePlanner` impl.
//!
//! Computes the `NextWake` plan from the configured `host_delay`
//! plus any per-domain override. Wake-time *storage* lives in the
//! frontier; this impl only returns the plan.
//!
//! **Zero-delay short-circuit.** When `PolitenessConfig::has_host_delay`
//! is `false` (every effective `host_delay` resolves to zero),
//! `record_fetch` returns `NextWake.until = Instant::now()`
//! without consulting the per-host override map. The worker's
//! wake-application path uses that signal to skip the
//! `Frontier::advance_wake` round-trip (no point writing a wake
//! that's already in the past). The verdict is cached at
//! construction; changing it requires rebuilding the planner.
//!
//! Quota enforcement (`[crawl].max_urls`) lives entirely in the
//! frontier's `submit_batch.lua` (counter-first, atomic). This
//! impl does not touch the per-host fetch counter.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use crawlrs_core::{NextWake, Result, ShardKey, ShardingPolicy, WakePlanner};

use crate::config::PolitenessConfig;
use crate::error::{LocalResult, PolitenessError};

/// Per-host wake-time planner. Today this is pure CPU math
/// against the config; the impl holds `sharding_policy` +
/// `owned_shards` so the shard-ownership guard stays in place,
/// matching the other Redis-backed sub-impls' invariants.
pub(crate) struct RedisWakePlanner {
    sharding_policy: Arc<dyn ShardingPolicy>,
    owned_shards: Vec<ShardKey>,
    config: PolitenessConfig,
    has_host_delay: bool,
}

impl RedisWakePlanner {
    pub(crate) fn new(
        sharding_policy: Arc<dyn ShardingPolicy>,
        owned_shards: Vec<ShardKey>,
        config: PolitenessConfig,
    ) -> Self {
        let has_host_delay = config.has_host_delay();
        Self {
            sharding_policy,
            owned_shards,
            config,
            has_host_delay,
        }
    }

    fn assert_owned(&self, shard: ShardKey) -> LocalResult<()> {
        if !self.owned_shards.contains(&shard) {
            return Err(PolitenessError::ShardNotOwned {
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
impl WakePlanner for RedisWakePlanner {
    async fn record_fetch(&self, host: &str) -> Result<NextWake> {
        let shard = self.sharding_policy.shard_key_from_host(host);
        self.assert_owned(shard)
            .map_err(crawlrs_core::Error::from)?;

        let delay = if self.has_host_delay {
            self.effective_host_delay(host)
        } else {
            Duration::ZERO
        };
        Ok(NextWake {
            host: host.to_string(),
            until: Instant::now() + delay,
        })
    }
}
