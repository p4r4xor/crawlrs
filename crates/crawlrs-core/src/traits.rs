//! Trait interfaces that the runtime composes.
//!
//! Implementations live in sibling crates: `crawlrs-fetch`, `crawlrs-parse`,
//! `crawlrs-store`, `crawlrs-frontier`, `crawlrs-politeness`. The runtime
//! crate only knows about these traits; swapping (e.g.) the in-memory
//! frontier for a Redis-backed one is a matter of changing which impl is
//! constructed.
//!
//! All trait methods are `async` and use `#[async_trait]` so they're
//! object-safe (we want `Box<dyn Fetcher>` for runtime composition).

use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;
use crate::types::{FetchRequest, FetchResponse, ParsedDocument, UrlEntry};
use crate::url::CanonicalUrl;

#[async_trait]
pub trait Fetcher: Send + Sync {
    async fn fetch(&self, req: FetchRequest) -> Result<FetchResponse>;
}

#[async_trait]
pub trait Parser: Send + Sync {
    async fn parse(&self, resp: &FetchResponse) -> Result<ParsedDocument>;
}

#[async_trait]
pub trait Store: Send + Sync {
    /// Persist one parsed document, optionally with the raw response body.
    async fn write(&self, doc: &ParsedDocument, raw_body: Option<&Bytes>) -> Result<()>;

    /// Flush any buffered writes to durable storage. Implementations that
    /// write synchronously may make this a no-op.
    async fn flush(&self) -> Result<()>;
}

#[async_trait]
pub trait Frontier: Send + Sync {
    /// Add one URL to the queue.
    ///
    /// Returns `true` if the URL was newly enqueued, `false` if the
    /// implementation determined it was already known and dropped the entry.
    async fn submit(&self, entry: UrlEntry) -> Result<bool>;

    /// Add many URLs at once.
    ///
    /// Returns the count of entries that were newly enqueued (i.e. not
    /// already known to the frontier). Implementations should prefer a
    /// single round-trip to the underlying store over N calls to `submit`.
    async fn submit_batch(&self, entries: Vec<UrlEntry>) -> Result<usize>;

    /// Pop the next URL to fetch, or `None` if the queue is empty.
    async fn claim(&self) -> Result<Option<UrlEntry>>;

    /// Pop up to `max` URLs in a single call. May return fewer (including
    /// zero) than `max` if the queue is shallow. Implementations should
    /// prefer one round-trip over `max` calls to `claim`.
    async fn claim_batch(&self, max: usize) -> Result<Vec<UrlEntry>>;

    /// Approximate queue depth, for metrics and shutdown checks.
    async fn len(&self) -> Result<usize>;
}

// ---------------------------------------------------------------------------
// Sharding (per ADR-0006)
// ---------------------------------------------------------------------------

/// Identifier of a shard within a sharded `Frontier`. `u32` is plenty:
/// the practical upper bound on shards is the worker pod count times a
/// small fan-out factor, nowhere near 4 billion.
pub type ShardKey = u32;

/// Routes URLs to shards. Pattern: Strategy.
///
/// `SingleShardPolicy` = Pattern 1 (one shard, every worker consumes it);
/// `HostHashShardPolicy` = Pattern 2 (host-sharded). Same `Frontier` impl
/// in either case; the impl reads the policy at construction.
///
/// See [ADR-0006](../../../docs/decisions/0006-sharding-policy-abstraction.md).
pub trait ShardingPolicy: Send + Sync {
    /// Map a URL to the shard that owns its queue, seen-set, and
    /// politeness state.
    fn shard_key(&self, url: &CanonicalUrl) -> ShardKey;

    /// Total number of shards this policy generates. Used by the
    /// frontier impl to size keyspaces and by deployment tools to
    /// validate ownership coverage.
    fn shard_count(&self) -> u32;
}

/// One shard for everyone, Pattern 1 from ADR-0002. Every URL maps to
/// shard `0`. Default for dev runs and small single-process crawls.
#[derive(Debug, Clone, Copy, Default)]
pub struct SingleShardPolicy;

impl ShardingPolicy for SingleShardPolicy {
    fn shard_key(&self, _url: &CanonicalUrl) -> ShardKey {
        0
    }
    fn shard_count(&self) -> u32 {
        1
    }
}

/// Host-hashed sharding, Pattern 2 from ADR-0002. Same registrable host
/// always lands on the same shard, so per-host politeness state stays
/// local to one shard.
///
/// Hash function is FNV-1a over the host string. FNV-1a is deterministic
/// (no per-process seed), stable across releases, dependency-free, and
/// adequate for distributing the host space; we are not relying on
/// cryptographic properties.
#[derive(Debug, Clone, Copy)]
pub struct HostHashShardPolicy {
    pub num_shards: u32,
}

impl HostHashShardPolicy {
    pub fn new(num_shards: u32) -> Self {
        assert!(num_shards >= 1, "num_shards must be at least 1");
        Self { num_shards }
    }
}

impl ShardingPolicy for HostHashShardPolicy {
    fn shard_key(&self, url: &CanonicalUrl) -> ShardKey {
        let host = url.host().unwrap_or("");
        let hash = fnv1a_64(host.as_bytes());
        (hash % self.num_shards as u64) as ShardKey
    }
    fn shard_count(&self) -> u32 {
        self.num_shards
    }
}

/// FNV-1a 64-bit hash. Deterministic across processes and crate versions,
/// no allocation, no dependency. Used for shard routing where a stable
/// non-cryptographic hash is sufficient.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Politeness
// ---------------------------------------------------------------------------

/// Politeness gate decision for a single URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoliteDecision {
    /// Safe to fetch right now.
    Allow,
    /// Wait at least this many milliseconds before fetching.
    DelayMs(u64),
    /// Disallowed (robots.txt, host on a deny-list, etc.).
    Disallow,
}

/// Why a fetch failed. The politeness layer cares about the *category*
/// of failure to decide backoff strategy, not the underlying error.
///
/// Mapped from HTTP status codes and transport errors at the boundary
/// between `Fetcher` and `Politeness::record_failure`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// HTTP 429. Server is explicitly rate-limiting; honor `Retry-After`
    /// if present, otherwise apply exponential backoff per host.
    TooManyRequests,
    /// HTTP 503. Server is overloaded or down for maintenance; same
    /// category as 429 for backoff, sometimes with a `Retry-After`.
    ServiceUnavailable,
    /// Transport-level reset (TCP RST, broken pipe). Often indicates
    /// the server is dropping us; back off conservatively.
    ConnectReset,
    /// We gave up waiting on the server. May indicate overload, or just
    /// network slowness; backoff is appropriate but milder than 429.
    Timeout,
    /// Anything else (DNS failure, TLS error, malformed response). Logged
    /// but does not necessarily trigger per-host backoff on its own.
    Other,
}

#[async_trait]
pub trait Politeness: Send + Sync {
    /// May this URL be fetched right now? Honors per-host wake-time,
    /// 429/503 backoff, robots.txt, and any per-domain overrides.
    async fn check(&self, url: &CanonicalUrl) -> PoliteDecision;

    /// A successful fetch just completed. Implementations use this to
    /// update per-host last-fetched timestamps so the next `check` for
    /// this host applies the configured delay.
    async fn record_fetch(&self, url: &CanonicalUrl);

    /// A fetch failed. Implementations use this to apply per-host
    /// exponential backoff on rate-limit categories (429/503) and to
    /// open per-host circuits after repeated transport failures.
    /// Without this, the policy can't distinguish "host is fine, just
    /// slow" from "host is rate-limiting us."
    async fn record_failure(&self, url: &CanonicalUrl, kind: FailureKind);

    /// Soonest moment any host this instance tracks becomes claimable.
    /// Lets the runtime sleep precisely until then instead of
    /// busy-polling every host on every tick.
    ///
    /// Returns `None` if the politeness layer has no scheduled work
    /// (every host is currently free, or no hosts are tracked yet).
    /// Implementations typically back this with a time-ordered
    /// structure keyed on host (sorted set, delay-queue, etc.) so the
    /// answer is O(log N) or better.
    async fn next_ready_at(&self) -> Option<Instant>;

    /// Whether robots.txt for this URL's host allows the given user
    /// agent to fetch the URL. Implementations own the robots cache
    /// internally; there is no separate `RobotsCache` trait.
    async fn robots_allows(&self, url: &CanonicalUrl, user_agent: &str) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_shard_policy_routes_everything_to_zero() {
        let policy = SingleShardPolicy;
        let a = CanonicalUrl::parse("https://a.test/x").unwrap();
        let b = CanonicalUrl::parse("https://b.test/y").unwrap();
        assert_eq!(policy.shard_key(&a), 0);
        assert_eq!(policy.shard_key(&b), 0);
        assert_eq!(policy.shard_count(), 1);
    }

    #[test]
    fn host_hash_policy_is_deterministic_per_host() {
        let policy = HostHashShardPolicy::new(16);
        let a1 = CanonicalUrl::parse("https://example.com/foo").unwrap();
        let a2 = CanonicalUrl::parse("https://example.com/bar").unwrap();
        // Same host → same shard, regardless of path.
        assert_eq!(policy.shard_key(&a1), policy.shard_key(&a2));
    }

    #[test]
    fn host_hash_policy_distributes_across_shards() {
        let policy = HostHashShardPolicy::new(8);
        let mut shards = std::collections::HashSet::new();
        for host in [
            "alpha.test",
            "bravo.test",
            "charlie.test",
            "delta.test",
            "echo.test",
            "foxtrot.test",
            "golf.test",
            "hotel.test",
            "india.test",
            "juliet.test",
        ] {
            let url = CanonicalUrl::parse(&format!("https://{host}/")).unwrap();
            shards.insert(policy.shard_key(&url));
        }
        // Not asserting all 8 shards hit; that's a property of FNV's
        // distribution on a small sample. But we should hit at least
        // half, otherwise the hash is broken.
        assert!(
            shards.len() >= 4,
            "FNV-1a should spread 10 hosts across at least 4 of 8 shards; got {}",
            shards.len()
        );
    }

    #[test]
    fn host_hash_policy_is_stable_across_calls() {
        // FNV-1a must be deterministic; re-running the same input must
        // give the same output. Locks against accidentally introducing
        // a per-process random seed.
        let policy = HostHashShardPolicy::new(1024);
        let url = CanonicalUrl::parse("https://canary.test/").unwrap();
        let first = policy.shard_key(&url);
        for _ in 0..100 {
            assert_eq!(policy.shard_key(&url), first);
        }
    }

    #[test]
    fn fnv1a_known_vectors() {
        // FNV-1a 64-bit reference values from
        // http://www.isthe.com/chongo/tech/comp/fnv/index.html
        // Locking these prevents silent algorithm drift.
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    #[should_panic(expected = "num_shards must be at least 1")]
    fn host_hash_policy_rejects_zero_shards() {
        let _ = HostHashShardPolicy::new(0);
    }
}
