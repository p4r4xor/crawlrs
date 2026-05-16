//! Tests for the `SiteAdapter` trait + `SiteAdapterRegistry` registry.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use crawlrs_core::{
    CanonicalUrl, FetchResponse, ParsedDocument, Result, SiteAdapter, SiteAdapterRegistry,
};

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
            outbound_links: Box::new(Vec::new()),
            fetched_at: resp.fetched_at,
        }))
    }
}

fn fake_response(url: &str) -> FetchResponse {
    let url = CanonicalUrl::parse(url).unwrap();
    FetchResponse {
        url,
        status: 200,
        headers: Box::new(HashMap::new()),
        body: Bytes::new(),
        redirect_chain: Vec::new().into(),
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
