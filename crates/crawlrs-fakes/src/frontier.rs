//! `InMemoryFrontier`: a `Frontier` impl that models the per-host
//! queue + wake ZSET + ready LIST + lease ZSET shape from ADR-0019
//! entirely in-process.
//!
//! Pattern: Test Double as Specification. The job of this module is
//! NOT to be a stub that returns canned answers; it's to be a
//! **protocol model** of the Redis frontier behavior the runtime
//! depends on. Tests written against `RedisFrontier` would also pass
//! against `InMemoryFrontier`. The interesting failure modes (lease
//! expiry + reclaim, bloom dedup at submit, ready-host promotion)
//! are all expressible against this model with no Docker.
//!
//! What this models, per ADR-0019:
//!
//! - **Per-shard state.** Each owned shard has its own host queues,
//!   URL HASH, seen-set, wake/ready/inflight bookkeeping.
//! - **Per-host FIFO queue** keyed by `host`. Submitted URLs are
//!   pushed to the back; claims pop from the front.
//! - **Wake ZSET** keyed by host, score = next-allowed-fetch wall-
//!   clock millis. The frontier owns this state outright now (was
//!   the politeness layer's responsibility pre-ADR-0020).
//! - **Ready LIST** keyed by shard: hosts whose wake-time has elapsed.
//!   Populated by `tick` (the promoter analogue) and drained by
//!   `claim`. Pre-computing readiness keeps claim O(1).
//! - **Lease ZSET** keyed by url_id, score = lease-expiry millis.
//!   `tick` reclaims expired leases and re-pushes the URL to its
//!   host queue for re-delivery.
//! - **Bloom-style seen-set** (a `HashSet<UrlId>` here; real impl
//!   uses RedisBloom). Submit drops a URL if its id is already in
//!   the set.
//! - **URL HASH** keyed by url_id; payload is the `UrlEntry`. Claim
//!   materialises this; ack deletes it.
//!
//! What this does NOT model (deliberate scope):
//!
//! - Bloom false-positive math (we use a `HashSet`; never false-
//!   positive, never false-negative).
//! - Lua-script atomicity beyond the Mutex granularity.
//! - Cluster failover / replication lag.
//!
//! Time control: pass an `Arc<dyn Clock>` (e.g. a `ManualClock`) to
//! [`InMemoryFrontier::with_clock`] so lease-expiry and wake-time
//! decisions can be exercised in zero wall-time.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use crawlrs_core::{
    AttemptId, ClaimOutcome, Clock, Error, Frontier, Result, ShardKey, ShardingPolicy,
    SubmitOutcome, SystemClock, UrlEntry, UrlId, WorkerIdentity,
};

/// Default lease timeout. A worker that crashes mid-fetch holds its
/// URL for this long before the reclaim path re-pushes it. 60s
/// matches the chart's `frontier.leaseTimeout` and is comfortably
/// above the typical fetch duration.
const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(60);

pub struct InMemoryFrontier {
    sharding_policy: Arc<dyn ShardingPolicy>,
    owned_shards: Vec<ShardKey>,
    state: Mutex<FrontierState>,
    clock: Arc<dyn Clock>,
    lease_timeout_ms: u64,
    /// Reference wall-clock instant. The Frontier trait carries wake-
    /// times as `Instant` but `Clock` only exposes millis since
    /// process start; we capture an anchor here so we can convert
    /// in both directions consistently.
    anchor_instant: Instant,
    anchor_clock_ms: u64,
}

impl std::fmt::Debug for InMemoryFrontier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryFrontier")
            .field("owned_shards", &self.owned_shards)
            .field("lease_timeout_ms", &self.lease_timeout_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct FrontierState {
    shards: HashMap<ShardKey, ShardState>,
    /// Round-robin cursor across owned shards on claim.
    claim_cursor: usize,
}

#[derive(Debug, Default)]
struct ShardState {
    /// Per-host FIFO of URL IDs.
    host_queue: HashMap<String, VecDeque<UrlId>>,
    /// Content-addressed URL records. Claim materialises from here;
    /// ack removes.
    urls: HashMap<UrlId, UrlEntry>,
    /// Submit-time dedup. Mirrors RedisBloom's `seen:s{shard}`.
    seen: HashSet<UrlId>,
    /// Host -> next-allowed-fetch-millis. Score-sorted (we walk the
    /// values on `tick`; explicit sort isn't worth maintaining at this
    /// scale).
    wake: HashMap<String, u64>,
    /// Hosts whose wake-time has elapsed, awaiting `claim`. Maintained
    /// by `tick`.
    ready: VecDeque<String>,
    /// url_id -> InflightSlot. The lease ZSET analogue.
    inflight: HashMap<UrlId, InflightSlot>,
}

#[derive(Debug, Clone)]
struct InflightSlot {
    host: String,
    lease_expiry_ms: u64,
}

impl InMemoryFrontier {
    pub fn new(sharding_policy: Arc<dyn ShardingPolicy>, owned_shards: Vec<ShardKey>) -> Self {
        let mut state = FrontierState::default();
        for shard in &owned_shards {
            state.shards.insert(*shard, ShardState::default());
        }
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        Self {
            sharding_policy,
            owned_shards,
            state: Mutex::new(state),
            anchor_clock_ms: clock.now_ms(),
            anchor_instant: Instant::now(),
            clock,
            lease_timeout_ms: DEFAULT_LEASE_TIMEOUT.as_millis() as u64,
        }
    }

    /// Override the wall-clock source. Tests inject a manual clock to
    /// make lease-expiry and wake-time decisions deterministic.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.anchor_clock_ms = clock.now_ms();
        self.anchor_instant = Instant::now();
        self.clock = clock;
        self
    }

    /// Override the lease timeout. Tests pass a short timeout (e.g.
    /// 100ms) so reclaim fires on the next `tick` after a manual
    /// clock advance.
    pub fn with_lease_timeout(mut self, t: Duration) -> Self {
        self.lease_timeout_ms = t.as_millis() as u64;
        self
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

    /// Convert an `Instant` to clock-millis using the captured anchor.
    /// Instants in the past relative to the anchor saturate at the
    /// anchor's millis value (the worker is asking for "now" or
    /// earlier, which means "ready immediately").
    fn instant_to_clock_ms(&self, when: Instant) -> u64 {
        if when <= self.anchor_instant {
            return self.anchor_clock_ms;
        }
        let delta = when.duration_since(self.anchor_instant).as_millis() as u64;
        self.anchor_clock_ms.saturating_add(delta)
    }

    /// Inverse of `instant_to_clock_ms`. Used to surface wake-time as
    /// an `Instant` in `ClaimOutcome::EmptyHint`.
    fn clock_ms_to_instant(&self, ms: u64) -> Instant {
        let delta = ms.saturating_sub(self.anchor_clock_ms);
        self.anchor_instant + Duration::from_millis(delta)
    }
}

fn encode_attempt(shard: ShardKey, url_id: &UrlId) -> AttemptId {
    AttemptId::new(format!("{shard}|{}", url_id.to_hex()))
}

fn decode_attempt(attempt: &AttemptId) -> Result<(ShardKey, UrlId)> {
    let raw = attempt.as_str();
    let (shard_str, id_hex) = raw
        .split_once('|')
        .ok_or_else(|| Error::Frontier(format!("malformed AttemptId: {raw}")))?;
    let shard: ShardKey = shard_str
        .parse()
        .map_err(|_| Error::Frontier(format!("malformed shard in AttemptId: {raw}")))?;
    let url_id = UrlId::from_hex(id_hex)
        .ok_or_else(|| Error::Frontier(format!("malformed url_id in AttemptId: {raw}")))?;
    Ok((shard, url_id))
}

#[async_trait]
impl Frontier for InMemoryFrontier {
    async fn submit(&self, entry: UrlEntry) -> Result<SubmitOutcome> {
        let shard = self.sharding_policy.shard_key(&entry.url);
        self.assert_owned(shard)?;
        let url_id = UrlId::from_canonical(&entry.url);
        // The runtime only enqueues HTTP-shape URLs (the parser drops
        // mailto:/tel:/javascript: at link-collection time), so a URL
        // arriving here without a host is a programming error.
        let host = entry
            .url
            .host()
            .ok_or_else(|| Error::Frontier(format!("URL has no host: {}", entry.url)))?
            .to_string();
        let now_ms = self.clock.now_ms();

        let mut state = self.state.lock().unwrap();
        let shard_state = state
            .shards
            .get_mut(&shard)
            .expect("owned shard initialised at construction");

        if !shard_state.seen.insert(url_id) {
            return Ok(SubmitOutcome::SkippedDuplicate);
        }
        shard_state.urls.insert(url_id, entry);

        let host_queue = shard_state.host_queue.entry(host.clone()).or_default();
        host_queue.push_back(url_id);

        // New host? Wake at 0 (immediately ready). Promoter (tick) will
        // pick this up on the next tick. We don't add to `ready`
        // directly because that would race with concurrent claims on
        // the same shard.
        shard_state.wake.entry(host).or_insert(now_ms);
        Ok(SubmitOutcome::Queued)
    }

    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<usize> {
        let mut newly = 0usize;
        for entry in entries {
            if matches!(self.submit(entry).await?, SubmitOutcome::Queued) {
                newly += 1;
            }
        }
        Ok(newly)
    }

    async fn claim(&self, _identity: &WorkerIdentity) -> Result<ClaimOutcome> {
        let n = self.owned_shards.len();
        if n == 0 {
            return Ok(ClaimOutcome::Empty);
        }
        let now_ms = self.clock.now_ms();
        let lease_timeout = self.lease_timeout_ms;

        let mut state = self.state.lock().unwrap();
        let start = state.claim_cursor % n;
        state.claim_cursor = state.claim_cursor.wrapping_add(1);

        // Walk owned shards round-robin. The first shard whose `ready`
        // list is non-empty wins; we serve at most one URL per claim.
        let mut soonest_wake: Option<u64> = None;
        for offset in 0..n {
            let shard = self.owned_shards[(start + offset) % n];
            let Some(shard_state) = state.shards.get_mut(&shard) else {
                continue;
            };

            if let Some((url_id, entry, host)) = pop_one_ready(shard_state, now_ms, lease_timeout) {
                let attempt_id = encode_attempt(shard, &url_id);
                let _ = host; // captured into InflightSlot already
                return Ok(ClaimOutcome::Claimed {
                    url_id,
                    entry: Box::new(entry),
                    attempt_id,
                });
            }

            // No ready host on this shard. Track the earliest wake-time
            // across all shards so we can return EmptyHint.
            if let Some(min) = shard_state.wake.values().min().copied()
                && (soonest_wake.is_none() || soonest_wake.is_some_and(|s| min < s))
            {
                soonest_wake = Some(min);
            }
        }

        match soonest_wake {
            Some(ms) if ms > now_ms => Ok(ClaimOutcome::EmptyHint {
                sleep_until: self.clock_ms_to_instant(ms),
            }),
            // Either the wake ZSET is empty (truly idle), or hosts are
            // already eligible but the promoter hasn't ticked them
            // into `ready` yet. Both cases: return Empty and let the
            // worker apply its idle floor.
            _ => Ok(ClaimOutcome::Empty),
        }
    }

    async fn len(&self) -> Result<usize> {
        let state = self.state.lock().unwrap();
        Ok(state
            .shards
            .values()
            .map(|s| s.host_queue.values().map(|q| q.len()).sum::<usize>())
            .sum())
    }

    async fn advance_wake(&self, host: &str, until: Instant) -> Result<()> {
        let until_ms = self.instant_to_clock_ms(until);
        let mut state = self.state.lock().unwrap();
        // Apply to every owned shard that has the host. In production
        // the sharding policy resolves the host to one shard, but the
        // trait surface doesn't carry a shard parameter; we mirror the
        // Redis impl's per-shard write by writing wherever the host
        // is known.
        for shard_state in state.shards.values_mut() {
            if shard_state.host_queue.contains_key(host) || shard_state.wake.contains_key(host) {
                shard_state.wake.insert(host.to_string(), until_ms);
                // If the host was sitting in `ready`, remove it; the
                // next tick will re-promote when the new wake-time
                // elapses.
                shard_state.ready.retain(|h| h != host);
            }
        }
        Ok(())
    }

    async fn ack(&self, attempt: &AttemptId) -> Result<()> {
        let (shard, url_id) = decode_attempt(attempt)?;
        let mut state = self.state.lock().unwrap();
        let Some(shard_state) = state.shards.get_mut(&shard) else {
            return Ok(()); // unknown shard: idempotent no-op
        };
        shard_state.inflight.remove(&url_id);
        shard_state.urls.remove(&url_id);
        Ok(())
    }

    async fn tick(&self) -> Result<usize> {
        let now_ms = self.clock.now_ms();
        let mut affected = 0usize;
        let mut state = self.state.lock().unwrap();

        for shard_state in state.shards.values_mut() {
            // Promote: drain hosts from `wake` whose score <= now into
            // `ready`. Skip hosts already in `ready` (idempotent).
            let mut promoted: Vec<String> = Vec::new();
            for (host, score) in shard_state.wake.iter() {
                if *score <= now_ms && !shard_state.ready.contains(host) {
                    promoted.push(host.clone());
                }
            }
            for host in &promoted {
                shard_state.wake.remove(host);
                // Only promote hosts that have URLs queued.
                if shard_state
                    .host_queue
                    .get(host)
                    .is_some_and(|q| !q.is_empty())
                {
                    shard_state.ready.push_back(host.clone());
                    affected += 1;
                }
            }

            // Reclaim: scan inflight for lease_expiry <= now, re-push
            // the URL to its host queue, ZADD wake (now) so the host
            // gets promoted next tick.
            let expired: Vec<UrlId> = shard_state
                .inflight
                .iter()
                .filter(|(_, slot)| slot.lease_expiry_ms <= now_ms)
                .map(|(id, _)| *id)
                .collect();
            for url_id in expired {
                let slot = shard_state.inflight.remove(&url_id).unwrap();
                shard_state
                    .host_queue
                    .entry(slot.host.clone())
                    .or_default()
                    .push_back(url_id);
                shard_state.wake.entry(slot.host).or_insert(now_ms);
                affected += 1;
            }
        }
        Ok(affected)
    }
}

/// Pop one URL from a `ready` host, lease it, and return the
/// materialised payload. Returns `None` if `ready` is empty or every
/// ready host's `host_queue` is somehow empty (stale `ready` entries).
///
/// On a successful pop, the host is *not* re-added to `ready` even if
/// it has more URLs queued: the worker is expected to call
/// `advance_wake(host, ..)` after the fetch, which writes the host's
/// next-allowed time into `wake`. Until then, the host is in neither
/// `ready` nor `wake` — it's owned by the in-flight worker. The lease
/// timeout (set on `inflight`) is the safety net if the worker
/// crashes before calling `advance_wake`.
fn pop_one_ready(
    shard: &mut ShardState,
    now_ms: u64,
    lease_timeout_ms: u64,
) -> Option<(UrlId, UrlEntry, String)> {
    while let Some(host) = shard.ready.pop_front() {
        let Some(q) = shard.host_queue.get_mut(&host) else {
            continue;
        };
        let Some(url_id) = q.pop_front() else {
            continue;
        };
        let entry = shard.urls.get(&url_id).cloned()?;
        shard.inflight.insert(
            url_id,
            InflightSlot {
                host: host.clone(),
                lease_expiry_ms: now_ms.saturating_add(lease_timeout_ms),
            },
        );
        return Some((url_id, entry, host));
    }
    None
}
