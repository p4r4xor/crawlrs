//! In-flight claims tracking.
//!
//! When a worker calls `RedisFrontier::claim`, the underlying Redis
//! Stream entry is *not* `XACK`ed yet; it sits in the consumer's pending
//! entries list (PEL) until the runtime calls `ack` or `nack` after
//! processing. To translate the runtime's URL-keyed `ack(&url)` into the
//! Redis-required `XACK <stream> <group> <entry-id>`, we track the
//! mapping internally.
//!
//! The map is bounded by the worker's max-in-flight count (typically
//! tens to low hundreds of URLs), so memory is negligible. We also
//! expose the size as a metric (`claim_count`) for runtime
//! observability.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use crawlrs_core::{CanonicalUrl, ShardKey};

/// Redis Stream entry IDs are strings of the form `<ms>-<seq>`.
pub type StreamEntryId = String;

/// Information needed to `XACK` an entry: which shard it came from and
/// its stream entry id. `claimed_at` lets the in-flight latency
/// histogram fire at ack/nack time.
#[derive(Debug, Clone)]
pub struct ClaimRecord {
    pub shard: ShardKey,
    pub entry_id: StreamEntryId,
    pub claimed_at: Instant,
}

/// Concurrent map of URLs currently in-flight on this `RedisFrontier`.
///
/// The mutex here protects a bookkeeping map that is touched only at
/// claim/ack/nack boundaries (not on the hot Redis-IO path), so the
/// lock is held for sub-microsecond windows.
#[derive(Default, Debug)]
pub struct PendingClaims {
    inner: Mutex<HashMap<CanonicalUrl, ClaimRecord>>,
}

impl PendingClaims {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `url` was just claimed and yielded `entry_id` on the
    /// given shard. Replaces any prior record for the same URL (which
    /// can happen after `XAUTOCLAIM` reclaims a stranded entry while
    /// the original consumer is silent).
    pub fn record(&self, url: CanonicalUrl, shard: ShardKey, entry_id: StreamEntryId) {
        let record = ClaimRecord {
            shard,
            entry_id,
            claimed_at: Instant::now(),
        };
        self.inner
            .lock()
            .expect("PendingClaims mutex poisoned")
            .insert(url, record);
    }

    /// Remove and return the claim record for `url`, if any.
    /// `None` means the URL was not in-flight on this frontier instance.
    pub fn take(&self, url: &CanonicalUrl) -> Option<ClaimRecord> {
        self.inner
            .lock()
            .expect("PendingClaims mutex poisoned")
            .remove(url)
    }

    /// Number of URLs currently in-flight. Useful as a metric and for
    /// shutdown checks ("are we drained yet?").
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("PendingClaims mutex poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
