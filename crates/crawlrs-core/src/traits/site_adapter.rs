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
