//! `InMemoryFrontier`: a `Frontier` impl that models Redis Streams
//! consumer-group semantics in-process.
//!
//! Pattern: Test Double as Specification. The job of this module is
//! NOT to be a stub that returns canned answers; it's to be a
//! **protocol model** of the Redis Streams behaviour the runtime
//! depends on. Tests written against `RedisFrontier` would also pass
//! against `InMemoryFrontier`. That is what makes simulation testing
//! tractable: the interesting failure modes (PEL accumulation, idle
//! reclaim by a peer, redelivery of the same `AttemptId`, restart
//! recovery via stable `WorkerIdentity`) are all expressible against
//! this model with no Docker.
//!
//! What this models:
//!
//! - **Per-shard streams** keyed by [`ShardKey`]. Each entry has a
//!   monotonic ID `<ms>-<seq>` (millis from the injected [`Clock`],
//!   sequence within the same ms).
//! - **A single consumer group** per stream (the `"fetchers"` name is
//!   implicit since the trait surfaces only one). Consumers are named
//!   by [`WorkerIdentity::to_string`]; the group keeps a per-consumer
//!   PEL.
//! - **Tier-1 PEL replay**: `claim(identity)` first surfaces entries
//!   already in `identity`'s PEL. This is what makes restart recovery
//!   instant when the consumer name is stable across restarts.
//! - **Tier-2 new delivery**: if the PEL is empty, hand out the next
//!   undelivered stream entry to `identity`.
//! - **Tier-3 XAUTOCLAIM**: if both 1 and 2 are empty, transfer one
//!   entry from any peer consumer's PEL whose `claimed_at` is older
//!   than the configured idle threshold into `identity`'s PEL.
//! - **Submit-time dedup**: per-shard "seen" set; a duplicate URL is
//!   silently dropped.
//!
//! What this does NOT model (deliberate scope):
//!
//! - `XADD MAXLEN` trimming.
//! - `XACK`-after-stream-trim corner cases.
//! - Cluster failover / replication lag.
//! - Lua-script atomicity beyond the Mutex granularity.
//!
//! Time control: pass an `Arc<dyn Clock>` (e.g. a `ManualClock`) to
//! [`InMemoryFrontier::with_clock`] so XAUTOCLAIM idle decisions can
//! be exercised in zero wall-time.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use crawlrs_core::{
    AttemptId, ClaimedMessage, Clock, Error, Frontier, Result, ShardKey, ShardingPolicy,
    SystemClock, UrlEntry, WorkerIdentity,
};

/// Default XAUTOCLAIM idle threshold: 5 minutes, mirroring the
/// production Redis adapter. Tests typically override to `Duration::ZERO`
/// so reclaim fires immediately.
const DEFAULT_AUTOCLAIM_IDLE: Duration = Duration::from_secs(300);

pub struct InMemoryFrontier {
    sharding_policy: Arc<dyn ShardingPolicy>,
    owned_shards: Vec<ShardKey>,
    inner: Mutex<Inner>,
    clock: Arc<dyn Clock>,
    autoclaim_idle_ms: u64,
}

impl std::fmt::Debug for InMemoryFrontier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryFrontier")
            .field("owned_shards", &self.owned_shards)
            .field("autoclaim_idle_ms", &self.autoclaim_idle_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct Inner {
    shards: HashMap<ShardKey, ShardState>,
    /// Disambiguates entry IDs that share the same wall-clock millisecond.
    /// Reset on each new ms.
    last_id_ms: u64,
    last_id_seq: u64,
    claim_cursor: usize,
}

#[derive(Debug, Default)]
struct ShardState {
    /// Append-only stream entries, in order of `XADD`.
    stream: Vec<StreamEntry>,
    /// `seen[url]`: dedupes the SADD-then-XADD path.
    seen: HashSet<String>,
    /// Per-consumer PEL: `consumer_name -> [(entry_id, claimed_at_ms)]`.
    pel: HashMap<String, Vec<PelSlot>>,
    /// Index into `stream` of the next entry to deliver via tier-2.
    next_undelivered: usize,
}

#[derive(Debug, Clone)]
struct StreamEntry {
    id: String,
    entry: UrlEntry,
}

#[derive(Debug, Clone)]
struct PelSlot {
    entry_id: String,
    claimed_at_ms: u64,
}

impl InMemoryFrontier {
    pub fn new(sharding_policy: Arc<dyn ShardingPolicy>, owned_shards: Vec<ShardKey>) -> Self {
        let mut inner = Inner::default();
        for shard in &owned_shards {
            inner.shards.insert(*shard, ShardState::default());
        }
        Self {
            sharding_policy,
            owned_shards,
            inner: Mutex::new(inner),
            clock: Arc::new(SystemClock),
            autoclaim_idle_ms: DEFAULT_AUTOCLAIM_IDLE.as_millis() as u64,
        }
    }

    /// Override the wall-clock source. Tests inject a manual clock to
    /// make XAUTOCLAIM idle decisions deterministic.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Override the XAUTOCLAIM idle threshold. Tests set this to zero
    /// to force reclaim on the next `claim()` call.
    pub fn with_autoclaim_idle(mut self, idle: Duration) -> Self {
        self.autoclaim_idle_ms = idle.as_millis() as u64;
        self
    }

    fn next_entry_id(&self, inner: &mut Inner) -> String {
        let now_ms = self.clock.now_ms();
        if now_ms == inner.last_id_ms {
            inner.last_id_seq += 1;
        } else {
            inner.last_id_ms = now_ms;
            inner.last_id_seq = 0;
        }
        format!("{}-{}", now_ms, inner.last_id_seq)
    }

    fn assert_owned(&self, shard: ShardKey) -> Result<()> {
        if !self.owned_shards.contains(&shard) {
            return Err(Error::Frontier(format!(
                "shard {shard} is not owned by this frontier instance (owns {:?})",
                self.owned_shards
            )));
        }
        Ok(())
    }
}

fn encode_attempt(shard: ShardKey, entry_id: &str) -> AttemptId {
    AttemptId::new(format!("{shard}|{entry_id}"))
}

fn decode_attempt(attempt: &AttemptId) -> Result<(ShardKey, String)> {
    let raw = attempt.as_str();
    let (shard_str, entry_id) = raw
        .split_once('|')
        .ok_or_else(|| Error::Frontier(format!("malformed AttemptId: {raw}")))?;
    let shard: ShardKey = shard_str
        .parse()
        .map_err(|_| Error::Frontier(format!("malformed shard in AttemptId: {raw}")))?;
    Ok((shard, entry_id.to_owned()))
}

#[async_trait]
impl Frontier for InMemoryFrontier {
    async fn submit(&self, entry: UrlEntry) -> Result<bool> {
        let shard = self.sharding_policy.shard_key(&entry.url);
        self.assert_owned(shard)?;
        let mut inner = self.inner.lock().unwrap();
        let entry_id = self.next_entry_id(&mut inner);
        let state = inner
            .shards
            .get_mut(&shard)
            .expect("owned shard initialised at construction");
        if !state.seen.insert(entry.url.as_str().to_string()) {
            return Ok(false);
        }
        state.stream.push(StreamEntry {
            id: entry_id,
            entry,
        });
        Ok(true)
    }

    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<usize> {
        let mut newly = 0usize;
        for entry in entries {
            if self.submit(entry).await? {
                newly += 1;
            }
        }
        Ok(newly)
    }

    async fn claim(&self, identity: &WorkerIdentity) -> Result<Option<ClaimedMessage>> {
        let n = self.owned_shards.len();
        if n == 0 {
            return Ok(None);
        }

        let consumer = identity.to_string();
        let mut inner = self.inner.lock().unwrap();
        let start = inner.claim_cursor % n;
        inner.claim_cursor = inner.claim_cursor.wrapping_add(1);

        let now_ms = self.clock.now_ms();
        let idle_threshold = self.autoclaim_idle_ms;

        for offset in 0..n {
            let shard = self.owned_shards[(start + offset) % n];
            if let Some((entry, entry_id)) =
                claim_from_shard(&mut inner, shard, &consumer, now_ms, idle_threshold)
            {
                let attempt_id = encode_attempt(shard, &entry_id);
                return Ok(Some(ClaimedMessage { entry, attempt_id }));
            }
        }
        Ok(None)
    }

    async fn claim_batch(
        &self,
        identity: &WorkerIdentity,
        max: usize,
    ) -> Result<Vec<ClaimedMessage>> {
        let mut out = Vec::with_capacity(max.min(64));
        let n = self.owned_shards.len();
        if n == 0 || max == 0 {
            return Ok(out);
        }

        let consumer = identity.to_string();
        let mut inner = self.inner.lock().unwrap();
        let start = inner.claim_cursor % n;
        inner.claim_cursor = inner.claim_cursor.wrapping_add(1);

        let now_ms = self.clock.now_ms();
        let idle_threshold = self.autoclaim_idle_ms;

        for offset in 0..n {
            let shard = self.owned_shards[(start + offset) % n];
            while out.len() < max {
                match claim_from_shard(&mut inner, shard, &consumer, now_ms, idle_threshold) {
                    Some((entry, entry_id)) => {
                        let attempt_id = encode_attempt(shard, &entry_id);
                        out.push(ClaimedMessage { entry, attempt_id });
                    }
                    None => break,
                }
            }
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }

    async fn len(&self) -> Result<usize> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.shards.values().map(|s| s.stream.len()).sum())
    }

    async fn ack(&self, attempt: &AttemptId) -> Result<()> {
        let (shard, entry_id) = decode_attempt(attempt)?;
        let mut inner = self.inner.lock().unwrap();
        let Some(state) = inner.shards.get_mut(&shard) else {
            return Ok(()); // unknown shard: idempotent no-op
        };
        // Remove from any consumer's PEL (matches XACK semantics: the
        // entry leaves PEL state regardless of which consumer holds
        // it). Also remove from the stream so `len()` reflects "work
        // remaining"; Redis itself doesn't trim on XACK but for the
        // purposes of in-memory tests, "acked == done" is the useful
        // invariant.
        for pel in state.pel.values_mut() {
            pel.retain(|slot| slot.entry_id != entry_id);
        }
        state.stream.retain(|e| e.id != entry_id);
        // Also walk the next_undelivered cursor back if needed.
        if state.next_undelivered > state.stream.len() {
            state.next_undelivered = state.stream.len();
        }
        Ok(())
    }

    async fn nack(&self, _attempt: &AttemptId) -> Result<()> {
        // Local-only no-op (matches the Redis impl's nack semantics):
        // leave the entry in this consumer's PEL; tier-1 will re-read
        // it on the next claim, or a peer's tier-3 will reclaim if
        // it goes idle.
        Ok(())
    }
}

/// Pull the body of `claim` out of the trait method so the borrowing
/// gymnastics stay readable. `inner` is the locked cluster state;
/// the function operates entirely under that single guard.
fn claim_from_shard(
    inner: &mut Inner,
    shard: ShardKey,
    consumer: &str,
    now_ms: u64,
    idle_threshold_ms: u64,
) -> Option<(UrlEntry, String)> {
    let state = inner.shards.get_mut(&shard)?;

    // Tier 1: this consumer's PEL. Entries we previously claimed but
    // haven't acked. Picked up immediately on a restarted consumer with
    // the same identity, which is the load-bearing property.
    if let Some(pel) = state.pel.get(consumer)
        && let Some(slot) = pel.first()
        && let Some(stream_entry) = state.stream.iter().find(|e| e.id == slot.entry_id)
    {
        return Some((stream_entry.entry.clone(), slot.entry_id.clone()));
    }

    // Tier 2: a fresh entry never delivered to anyone.
    if state.next_undelivered < state.stream.len() {
        let stream_entry = state.stream[state.next_undelivered].clone();
        state.next_undelivered += 1;
        state
            .pel
            .entry(consumer.to_string())
            .or_default()
            .push(PelSlot {
                entry_id: stream_entry.id.clone(),
                claimed_at_ms: now_ms,
            });
        return Some((stream_entry.entry, stream_entry.id));
    }

    // Tier 3: XAUTOCLAIM. Find any peer's PEL slot older than the idle
    // threshold and transfer it to `consumer`'s PEL.
    let mut victim: Option<(String, usize)> = None; // (peer_consumer, slot_index)
    for (peer, pel) in state.pel.iter() {
        if peer == consumer {
            continue;
        }
        for (i, slot) in pel.iter().enumerate() {
            if now_ms.saturating_sub(slot.claimed_at_ms) >= idle_threshold_ms {
                victim = Some((peer.clone(), i));
                break;
            }
        }
        if victim.is_some() {
            break;
        }
    }
    if let Some((peer, i)) = victim {
        let pel = state.pel.get_mut(&peer).expect("found above");
        let mut slot = pel.remove(i);
        slot.claimed_at_ms = now_ms;
        let entry_id = slot.entry_id.clone();
        let stream_entry = state.stream.iter().find(|e| e.id == entry_id).cloned();
        state
            .pel
            .entry(consumer.to_string())
            .or_default()
            .push(slot);
        if let Some(stream_entry) = stream_entry {
            return Some((stream_entry.entry, entry_id));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crawlrs_core::{CanonicalUrl, SingleShardPolicy, UrlEntry};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Test-local manual clock. Lives here rather than in a top-level
    /// module so the in-memory frontier's tests don't pull in
    /// dependencies on a separate clock crate.
    #[derive(Debug)]
    struct ManualClock {
        ms: AtomicU64,
    }
    impl ManualClock {
        fn new(start_ms: u64) -> Self {
            Self {
                ms: AtomicU64::new(start_ms),
            }
        }
        fn advance_ms(&self, delta: u64) {
            self.ms.fetch_add(delta, Ordering::Relaxed);
        }
    }
    impl Clock for ManualClock {
        fn now_ms(&self) -> u64 {
            self.ms.load(Ordering::Relaxed)
        }
    }

    fn url(s: &str) -> CanonicalUrl {
        CanonicalUrl::parse(s).unwrap()
    }
    fn entry(s: &str) -> UrlEntry {
        UrlEntry::seed(url(s))
    }

    fn fresh_frontier() -> InMemoryFrontier {
        let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
        InMemoryFrontier::new(policy, vec![0])
    }

    #[tokio::test]
    async fn submit_then_claim_yields_same_url() {
        let f = fresh_frontier();
        let id = WorkerIdentity::new(0, 0);
        f.submit(entry("https://a.test/")).await.unwrap();
        let claimed = f.claim(&id).await.unwrap().expect("claim should yield");
        assert_eq!(claimed.entry.url.as_str(), "https://a.test/");
    }

    #[tokio::test]
    async fn duplicate_submit_is_dropped_at_seen_set() {
        let f = fresh_frontier();
        assert!(f.submit(entry("https://a.test/")).await.unwrap());
        assert!(!f.submit(entry("https://a.test/")).await.unwrap());
        assert_eq!(f.len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn ack_removes_from_pel_and_stream() {
        let f = fresh_frontier();
        let id = WorkerIdentity::new(0, 0);
        f.submit(entry("https://a.test/")).await.unwrap();
        let claimed = f.claim(&id).await.unwrap().unwrap();
        assert_eq!(f.len().await.unwrap(), 1);
        f.ack(&claimed.attempt_id).await.unwrap();
        assert_eq!(f.len().await.unwrap(), 0);
        // Idempotent: a second ack on the same attempt is a no-op.
        f.ack(&claimed.attempt_id).await.unwrap();
    }

    #[tokio::test]
    async fn stable_identity_recovers_pel_immediately() {
        // The architectural invariant being asserted: when a worker
        // dies mid-flight, a freshly-spawned worker with the SAME
        // WorkerIdentity reattaches to the dead one's PEL via tier-1
        // (no XAUTOCLAIM idle wait). This is the recovery path that
        // makes pod-restart cheap.
        let f = fresh_frontier();
        let identity = WorkerIdentity::new(0, 0);

        f.submit(entry("https://a.test/")).await.unwrap();
        let first_claim = f.claim(&identity).await.unwrap().unwrap();
        assert_eq!(first_claim.entry.url.as_str(), "https://a.test/");
        // We deliberately do NOT ack: simulates the worker crashing
        // between claim and finalize.

        // A new "worker process" with the same identity calls claim:
        // tier-1 must surface the PEL'd entry without waiting for the
        // 5-minute autoclaim idle.
        let recovered = f.claim(&identity).await.unwrap().expect("tier-1 replay");
        assert_eq!(
            recovered.entry.url.as_str(),
            "https://a.test/",
            "same identity must see the same in-flight URL on tier-1 read",
        );
        assert_eq!(
            recovered.attempt_id, first_claim.attempt_id,
            "redelivery via tier-1 carries the same AttemptId",
        );
    }

    #[tokio::test]
    async fn xautoclaim_transfers_idle_pel_entry_to_peer() {
        // The recovery path for the case where the original worker
        // doesn't come back: a peer's tier-3 reclaims after the
        // configured idle threshold. With ManualClock + idle=1000ms,
        // we control wall-time deterministically.
        let policy: Arc<dyn ShardingPolicy> = Arc::new(SingleShardPolicy);
        let clock = Arc::new(ManualClock::new(1_000_000));
        let f = InMemoryFrontier::new(policy, vec![0])
            .with_clock(clock.clone())
            .with_autoclaim_idle(Duration::from_millis(1000));

        let dead = WorkerIdentity::new(0, 0);
        let alive = WorkerIdentity::new(0, 1);

        f.submit(entry("https://a.test/")).await.unwrap();
        let dead_claim = f.claim(&dead).await.unwrap().unwrap();

        // Immediate peer claim sees nothing: idle threshold not met.
        // Stream is empty (already delivered to `dead`), and the entry
        // has been claimed for 0ms.
        assert!(f.claim(&alive).await.unwrap().is_none());

        // Advance past the idle threshold; peer's tier-3 must reclaim.
        clock.advance_ms(2000);
        let reclaimed = f.claim(&alive).await.unwrap().expect("xautoclaim");
        assert_eq!(reclaimed.entry.url.as_str(), "https://a.test/");
        assert_eq!(
            reclaimed.attempt_id, dead_claim.attempt_id,
            "the redelivered AttemptId is the original entry's id",
        );
    }
}
