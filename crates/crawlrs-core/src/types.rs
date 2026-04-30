//! Data structs that flow between pipeline stages.
//!
//! - `UrlEntry`: a frontier item (a URL the crawler intends to fetch).
//! - `FetchRequest`: fetcher input (the URL plus per-request overrides).
//! - `FetchResponse`: fetcher output (status, headers, body, timing).
//! - `ParsedDocument`: parser output (text, links, metadata).

use std::collections::HashMap;
use std::time::Duration;

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
        Self { url, depth: 0, discovered_from: None }
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
