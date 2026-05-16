//! `FakeFetcher`: an in-memory `Fetcher` driven by canned responses.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use crawlrs_core::{CanonicalUrl, Error, FetchRequest, FetchResponse, Fetcher, Result};
use smallvec::SmallVec;

/// Routes URLs to canned responses installed before the test runs.
///
/// `install_*` methods register what `fetch(url)` should return; the
/// `calls()` method returns the URLs that were actually fetched, in
/// order. Unmatched URLs return `Error::Fetch("FakeFetcher: no canned
/// response for ...")` so tests fail fast on missing setup rather
/// than silently returning empty bodies.
#[derive(Default)]
pub struct FakeFetcher {
    responses: Mutex<HashMap<String, FetchResponse>>,
    calls: Mutex<Vec<String>>,
}

impl FakeFetcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a 200-OK HTML response for `url`. The body is copied
    /// into a `Bytes` buffer; `content-type: text/html` is set
    /// automatically so the parser path treats the response as HTML.
    pub fn install_html(&self, url: &str, body: &str) {
        let canon = CanonicalUrl::parse(url).expect("FakeFetcher: install_html with valid URL");
        let resp = FetchResponse {
            url: canon,
            status: 200,
            headers: Box::new(HashMap::from([("content-type".into(), "text/html".into())])),
            body: Bytes::copy_from_slice(body.as_bytes()),
            redirect_chain: SmallVec::new(),
            fetched_at: Utc::now(),
            duration: Duration::from_millis(0),
        };
        self.responses.lock().unwrap().insert(url.to_string(), resp);
    }

    /// Install a status-only response (empty body, no headers). Used
    /// to exercise rate-limit / failure paths.
    pub fn install_status(&self, url: &str, status: u16) {
        self.install_status_with_headers(url, status, HashMap::new());
    }

    /// Install a response with arbitrary status and body, no headers.
    /// Used by callers (e.g. robots.txt fixtures) that want a body but
    /// don't want the `text/html` content-type that `install_html`
    /// sets.
    pub fn install_response(&self, url: &str, status: u16, body: &str) {
        let canon = CanonicalUrl::parse(url).expect("FakeFetcher: install_response with valid URL");
        let resp = FetchResponse {
            url: canon,
            status,
            headers: Box::new(HashMap::new()),
            body: Bytes::copy_from_slice(body.as_bytes()),
            redirect_chain: SmallVec::new(),
            fetched_at: Utc::now(),
            duration: Duration::from_millis(0),
        };
        self.responses.lock().unwrap().insert(url.to_string(), resp);
    }

    /// Install a status response with headers. Used to exercise the
    /// `Retry-After` parsing path and similar header-driven behaviors.
    pub fn install_status_with_headers(
        &self,
        url: &str,
        status: u16,
        headers: HashMap<String, String>,
    ) {
        let canon = CanonicalUrl::parse(url)
            .expect("FakeFetcher: install_status_with_headers with valid URL");
        let resp = FetchResponse {
            url: canon,
            status,
            headers: Box::new(headers),
            body: Bytes::new(),
            redirect_chain: SmallVec::new(),
            fetched_at: Utc::now(),
            duration: Duration::from_millis(0),
        };
        self.responses.lock().unwrap().insert(url.to_string(), resp);
    }

    /// URLs the fetcher has been asked about, in call order. Tests
    /// assert on this to verify that (e.g.) cross-run dedup actually
    /// prevented a fetch.
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Fetcher for FakeFetcher {
    async fn fetch(&self, req: FetchRequest) -> Result<FetchResponse> {
        let url = req.url.as_str().to_string();
        self.calls.lock().unwrap().push(url.clone());
        match self.responses.lock().unwrap().get(&url).cloned() {
            Some(resp) => Ok(resp),
            None => Err(Error::Fetch(format!(
                "FakeFetcher: no canned response for {url}"
            ))),
        }
    }
}
