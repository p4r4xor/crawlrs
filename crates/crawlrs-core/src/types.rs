//! Data structs that flow between pipeline stages.
//!
//! - `UrlEntry`: a frontier item (a URL the crawler intends to fetch).
//! - `FetchRequest`: fetcher input (the URL plus per-request overrides).
//! - `FetchResponse`: fetcher output (status, headers, body, timing).
//! - `ParsedDocument`: parser output (text, links, metadata).
//! - `UrlMetadata` / `UrlStatus`: per-URL ledger entry (cross-run state).

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::url::CanonicalUrl;

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

/// Lifecycle status of a URL in the metadata ledger. See ADR-0009.
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
    /// `url_metadata` and `url_history` answer "what broke?" (per
    /// ADR-0011).
    PermanentlyFailed,
    /// Skipped without an attempt (e.g. robots.txt disallowed,
    /// manual exclude, depth limit, content-hash dupe).
    Skipped,
}

/// Per-URL ledger entry. The `MetadataStore` trait stores one of
/// these per URL across all crawl runs (cross-run shape per ADR-0009);
/// concrete impls back this with whatever's appropriate (Redis Hash,
/// Postgres row, etc.).
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
