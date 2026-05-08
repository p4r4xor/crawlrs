//! Data structs that flow between pipeline stages.
//!
//! - `UrlEntry`: a frontier item (a URL the crawler intends to fetch).
//! - `FetchRequest`: fetcher input (the URL plus per-request overrides).
//! - `FetchResponse`: fetcher output (status, headers, body, timing).
//! - `ParsedDocument`: parser output (text, links, metadata).
//! - `UrlMetadata` / `UrlStatus`: per-URL ledger entry (cross-run state).
//! - `WorkerIdentity`: stable identity for one worker across restarts.
//! - `AttemptId`: opaque correlation token for one delivery of a URL.
//!
//! `ClaimedMessage` (the `UrlEntry` + `AttemptId` pair returned by
//! `Frontier::claim`) lives next to the trait it serves, in
//! [`crate::traits::frontier`].

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::url::CanonicalUrl;

/// Stable identity for one worker.
///
/// Pattern: Identity Object. The contract is that **the same logical
/// worker carries the same `WorkerIdentity` across process restarts**;
/// the rendered string is therefore safe to use as a Redis Streams
/// consumer name, a logging tag, or any other identifier whose stability
/// is load-bearing for recovery (e.g. tier-1 PEL replay on restart).
///
/// `pod_ordinal` is the StatefulSet ordinal extracted from the pod's
/// hostname (`crawlrs-2` -> 2). `worker_index` is the per-pod task
/// index assigned at spawn time (0 .. workers_per_pod). Together they
/// uniquely identify a worker in the cluster.
///
/// The `Display` rendering (`pod-{ordinal}:{index}`) is the canonical
/// stringification used at adapter boundaries. Don't construct the
/// string by hand at call sites; let the type render itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerIdentity {
    pub pod_ordinal: u32,
    pub worker_index: u32,
}

impl WorkerIdentity {
    pub const fn new(pod_ordinal: u32, worker_index: u32) -> Self {
        Self {
            pod_ordinal,
            worker_index,
        }
    }
}

impl fmt::Display for WorkerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pod-{}:{}", self.pod_ordinal, self.worker_index)
    }
}

/// Opaque correlation token for one delivery of a URL.
///
/// Pattern: Correlation Identifier. The runtime carries an `AttemptId`
/// from `Frontier::claim` through the pipeline to `MetadataStore`
/// writes and `Frontier::ack`/`nack`, so every layer agrees on which
/// *attempt* a side-effect belongs to. Two redeliveries of the same URL
/// (e.g. via XAUTOCLAIM after a stall) carry **different** AttemptIds,
/// so downstream stores can dedupe per-attempt without conflating
/// retries.
///
/// The string contents are opaque to the runtime; each `Frontier`
/// impl owns its own encoding and treats the token as private state.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AttemptId(String);

impl AttemptId {
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One item in the frontier: "this URL is queued to be fetched."
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlEntry {
    pub url: CanonicalUrl,
    pub depth: u32,
    /// The page that linked us to this URL, if any. `None` for seeds.
    pub discovered_from: Option<CanonicalUrl>,
}

impl UrlEntry {
    pub fn seed(url: CanonicalUrl) -> Self {
        Self {
            url,
            depth: 0,
            discovered_from: None,
        }
    }
}

/// Input to `Fetcher::fetch`.
///
/// Headers and timeout here override any defaults baked into the fetcher
/// implementation (e.g. the default User-Agent or per-request deadline).
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub url: CanonicalUrl,
    pub headers: HashMap<String, String>,
    pub timeout: Duration,
}

impl FetchRequest {
    pub fn new(url: CanonicalUrl) -> Self {
        Self {
            url,
            headers: HashMap::new(),
            timeout: Duration::from_secs(30),
        }
    }
}

/// One hop in a redirect chain.
///
/// A redirect from `https://a.test/` (status 301) to `https://b.test/` is
/// represented as `RedirectHop { from: a, to: b, status: 301 }`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedirectHop {
    pub from: CanonicalUrl,
    pub to: CanonicalUrl,
    pub status: u16,
}

/// Output of `Fetcher::fetch`.
///
/// `url` here is the *final* URL after redirects; it may differ from
/// `FetchRequest::url`. Body is held as `Bytes` so cloning is cheap (refcount).
/// `redirect_chain` is empty when no redirect was followed. Otherwise it holds
/// each hop in order, ending at `url`.
#[derive(Debug, Clone)]
pub struct FetchResponse {
    pub url: CanonicalUrl,
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Bytes,
    pub redirect_chain: Vec<RedirectHop>,
    pub fetched_at: DateTime<Utc>,
    pub duration: Duration,
}

/// Output of `Parser::parse`.
///
/// `text` is the extracted readable text (LanceDB-bound). `outbound_links`
/// are already-canonicalized URLs ready to feed back into the frontier.
#[derive(Debug, Clone)]
pub struct ParsedDocument {
    pub url: CanonicalUrl,
    pub status: u16,
    pub title: Option<String>,
    pub text: Option<String>,
    pub outbound_links: Vec<CanonicalUrl>,
    pub fetched_at: DateTime<Utc>,
}

/// Input to `Store::write`. Bundles fetch + parse output + per-run
/// context into one parameter object so the trait surface is one
/// argument and impls can populate any column they need.
///
/// `FetchResponse` carries the wire-level fields (status, headers,
/// body, redirect chain, fetched_at, duration); `ParsedDocument`
/// carries the parsed-content fields (title, text, outbound_links);
/// the runtime supplies the run/shard/depth context plus the
/// content_hash already computed at this point in the pipeline.
///
/// Pattern: Introduce Parameter Object. Grouping these fields keeps
/// the `Store::write` signature one argument and lets impls pull just
/// the columns they need without overloading the trait method as the
/// stored shape evolves.
#[derive(Debug, Clone, Copy)]
pub struct StoreRecord<'a> {
    pub doc: &'a ParsedDocument,
    pub resp: &'a FetchResponse,
    pub run_id: &'a str,
    pub shard: u32,
    pub depth: u32,
    pub content_hash: u64,
}

/// Lifecycle status of a URL in the metadata ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UrlStatus {
    /// First-seen but not yet attempted.
    Pending,
    /// A worker has claimed this URL and is processing it.
    InProgress,
    /// Fetched and stored successfully.
    Succeeded,
    /// A retryable failure (429, 503, transport reset, etc.). The
    /// `retry_count` on `UrlMetadata` carries how many failures so
    /// far.
    FailedTransient,
    /// Retry budget exhausted, or the failure is non-retryable.
    /// Forms the dead-letter view: rows where `status =
    /// 'permanently_failed'` are the DLQ; ops queries against
    /// `url_metadata` and `url_history` answer "what broke?".
    PermanentlyFailed,
    /// Skipped without an attempt (e.g. robots.txt disallowed,
    /// manual exclude, depth limit, content-hash dupe).
    Skipped,
}

/// Per-URL ledger entry. The `MetadataStore` trait stores one of
/// these per URL across all crawl runs; concrete impls back this with
/// whatever's appropriate (Redis Hash, Postgres row, etc.).
///
/// All time fields are `SystemTime` in the API surface; storage layers
/// encode them as wall-clock millis at the wire boundary, the same
/// convention used by politeness state.
#[derive(Debug, Clone)]
pub struct UrlMetadata {
    pub url: CanonicalUrl,
    pub status: UrlStatus,
    pub retry_count: u32,
    /// Where the body lives in the configured `Store` impl. `None`
    /// until the URL has been successfully fetched + persisted.
    pub blob_path: Option<String>,
    /// `fnv1a_64` of the response body (see [`crate::content_hash`]),
    /// recorded at storage time. Used for content-level dedup (v2)
    /// and change detection.
    pub content_hash: Option<u64>,
    /// Hop distance from the seed that introduced this URL.
    pub depth: u32,
    /// `run_id` of the run that most recently touched this row.
    pub last_run_id: String,
    /// When this URL was first added to the metadata ledger.
    pub discovered_at: SystemTime,
    /// Last modification of any field. On a fresh insert this equals
    /// `discovered_at`.
    pub updated_at: SystemTime,
}

/// Strategy for moving a successful fetch's outbound URLs into the
/// Frontier. Selected at runtime composition time.
///
/// `Direct` (default) skips the outbox: the worker calls
/// `Frontier::submit_batch` directly after the metadata commit. A
/// `submit_batch` failure or a worker crash mid-call drops those
/// outbound URLs. Best-effort delivery; ~50x lower Postgres write
/// volume than `DurableOutbox` because outbound URLs never become
/// rows in the outbox table.
///
/// `DurableOutbox` commits outbound URLs atomically with the metadata
/// write into a Postgres outbox table; a separate publisher drains
/// the table into the Frontier with at-least-once delivery. Survives
/// any single component crashing at the cost of ~100x the metadata
/// write rate. Opt-in for system-of-record runs where every
/// discovered URL must reach the Frontier.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkDispatch {
    /// Fire-and-forget direct enqueue from the worker. Loses URLs
    /// during transient Frontier errors. Default.
    #[default]
    Direct,
    /// Atomic with metadata, drained asynchronously by the outbox
    /// publisher. Survives any single component crash.
    DurableOutbox,
}
