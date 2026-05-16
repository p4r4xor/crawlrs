//! Promoter and lease-reclaim driver.
//!
//! The frontier exposes `tick()` as the trait surface; this module
//! defines the work `tick` does. One pass drives:
//!
//!   1. **Promote.** For every owned shard, drain hosts whose wake-
//!      time has elapsed out of the `wake` ZSET and onto the `ready`
//!      LIST. Workers' `claim` calls then `LPOP ready` in O(1)
//!      without scanning ZSET scores on the hot path.
//!
//!   2. **Reclaim.** For every owned shard, scan the `inflight` ZSET
//!      for expired leases and re-push the stranded URLs onto their
//!      host queue. Replaces the streams-based `XAUTOCLAIM` recovery
//!      path with explicit, operator-tunable lease timeouts.
//!
//! Bounded by `batch_limit` so one tick doesn't dominate a Redis tick
//! under heavy backlog. The runtime calls `tick()` on a cadence (the
//! `promoter_tick` config knob; default 50ms).

use crawlrs_core::ShardKey;
use tracing::warn;

use crate::host_queue::HostQueueOps;

/// One pass over the owned shards: promote then reclaim. Returns
/// `(promoted, reclaimed)` so the caller can report metrics.
pub(crate) async fn tick_once(
    ops: &HostQueueOps<'_>,
    owned_shards: &[ShardKey],
    now_ms: i64,
    batch_limit: u64,
) -> (u64, u64) {
    let mut promoted = 0u64;
    let mut reclaimed = 0u64;
    for &shard in owned_shards {
        match ops.promote(shard, now_ms, batch_limit).await {
            Ok(n) => promoted += n,
            Err(e) => warn!(shard, error = %e, "promote failed"),
        }
        match ops.reclaim(shard, now_ms, batch_limit).await {
            Ok(n) => reclaimed += n,
            Err(e) => warn!(shard, error = %e, "reclaim failed"),
        }
    }
    (promoted, reclaimed)
}
