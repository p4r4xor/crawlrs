//! Per-domain code hooks via the `SiteAdapter` trait.
//!
//! `SiteAdapter` is how a library user plugs domain-specific extraction
//! logic into the generic pipeline. Two layers, both Strategy pattern:
//!
//! - `SiteAdapter::matches(url)`: pure predicate. Does this adapter
//!   apply to this URL? Match on registrable domain, host, or path
//!   prefix. Cheap, called per URL.
//! - `SiteAdapter::extract(resp)`: the actual custom extraction.
//!   Returns `Some(doc)` when the adapter handled it, `None` to fall
//!   through to the generic `Parser`.
//!
//! `SiteAdapterRegistry` is a simple ordered list with first-match-wins
//! lookup (Composite + Chain of Responsibility). The runtime consults
//! the registry per response; if no adapter matches, the generic
//! `Parser` handles the page.
//!
//! A future scripted variant (a `LuaSiteAdapter` or `WasmSiteAdapter`
//! that loads scripts at runtime) fits behind this same trait without
//! changing it. The trait surface stays code-agnostic.
//!
//! See ARCHITECTURE.md §5 for the per-domain customization layering
//! (declarative config vs code hooks vs scripting).

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{FetchResponse, ParsedDocument};
use crate::url::CanonicalUrl;

#[async_trait]
pub trait SiteAdapter: Send + Sync {
    /// Pure predicate: does this adapter apply to this URL? Implementations
    /// should match on the registrable domain or finer (path prefix). Must
    /// be cheap; this is called for every URL the runtime processes.
    fn matches(&self, url: &CanonicalUrl) -> bool;

    /// Custom extraction. Return:
    ///
    /// - `Ok(Some(doc))` if this adapter fully handled the response.
    /// - `Ok(None)` if the adapter matches the domain but this specific
    ///   URL is not its shape (e.g. a `GitHubAdapter` returns `None` for
    ///   the GitHub homepage and only handles `/user/repo/blob/...`).
    ///   The runtime falls through to the generic `Parser`.
    /// - `Err(_)` if the adapter encountered an unrecoverable error
    ///   processing what it claimed to handle.
    async fn extract(&self, resp: &FetchResponse) -> Result<Option<ParsedDocument>>;
}

/// Ordered registry of site adapters. First match wins.
///
/// Built once at startup by the binary (or library user) from a list of
/// `Arc<dyn SiteAdapter>`. The runtime consults the registry at parse
/// time. Adapter ordering matters: register more specific adapters
/// first.
#[derive(Default)]
pub struct SiteAdapterRegistry {
    adapters: Vec<Arc<dyn SiteAdapter>>,
}

impl SiteAdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an adapter to the registry. Earlier-registered adapters
    /// take precedence on URLs that match multiple adapters.
    pub fn register(&mut self, adapter: Arc<dyn SiteAdapter>) {
        self.adapters.push(adapter);
    }

    /// First adapter (in registration order) that matches the URL, or
    /// `None` if no adapter applies. Returns an `Arc` so the caller can
    /// hold the adapter across an `await` without keeping the registry
    /// borrowed.
    pub fn find_for(&self, url: &CanonicalUrl) -> Option<Arc<dyn SiteAdapter>> {
        self.adapters
            .iter()
            .find(|adapter| adapter.matches(url))
            .cloned()
    }

    pub fn len(&self) -> usize {
        self.adapters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use bytes::Bytes;
    use chrono::Utc;

    use super::*;

    struct GitHubFake;

    #[async_trait]
    impl SiteAdapter for GitHubFake {
        fn matches(&self, url: &CanonicalUrl) -> bool {
            url.host() == Some("github.com")
        }
        async fn extract(&self, resp: &FetchResponse) -> Result<Option<ParsedDocument>> {
            Ok(Some(ParsedDocument {
                url: resp.url.clone(),
                status: resp.status,
                title: Some("github-fake".to_string()),
                text: None,
                outbound_links: Vec::new(),
                fetched_at: resp.fetched_at,
            }))
        }
    }

    fn fake_response(url: &str) -> FetchResponse {
        let url = CanonicalUrl::parse(url).unwrap();
        FetchResponse {
            url,
            status: 200,
            headers: HashMap::new(),
            body: Bytes::new(),
            redirect_chain: Vec::new(),
            fetched_at: Utc::now(),
            duration: Duration::from_millis(0),
        }
    }

    #[tokio::test]
    async fn registry_picks_first_matching_adapter() {
        let mut registry = SiteAdapterRegistry::new();
        registry.register(Arc::new(GitHubFake));

        let github_url = CanonicalUrl::parse("https://github.com/foo").unwrap();
        let other_url = CanonicalUrl::parse("https://example.com/").unwrap();

        assert!(registry.find_for(&github_url).is_some());
        assert!(registry.find_for(&other_url).is_none());

        let adapter = registry.find_for(&github_url).unwrap();
        let resp = fake_response("https://github.com/foo");
        let doc = adapter.extract(&resp).await.unwrap().unwrap();
        assert_eq!(doc.title.as_deref(), Some("github-fake"));
    }

    #[test]
    fn registry_starts_empty() {
        let registry = SiteAdapterRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }
}
