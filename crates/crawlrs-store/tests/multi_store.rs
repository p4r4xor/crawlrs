//! `MultiStore` fan-out: one `write()` call lands in every inner
//! store; the returned blob_path is the primary's.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use crawlrs_core::{CanonicalUrl, FetchResponse, ParsedDocument, Store, StoreRecord, content_hash};
use crawlrs_fakes::InMemoryStore;
use crawlrs_store::MultiStore;

#[tokio::test]
async fn fans_out_to_all_inner_stores_and_returns_primary_path() {
    let primary = Arc::new(InMemoryStore::new());
    let secondary = Arc::new(InMemoryStore::new());
    let multi = MultiStore::new(vec![
        primary.clone() as Arc<dyn Store>,
        secondary.clone() as Arc<dyn Store>,
    ])
    .unwrap();

    let (doc, resp) = fixture("https://example.com/x", b"<html>x</html>");
    let record = StoreRecord {
        doc: &doc,
        resp: &resp,
        run_id: "multi-run",
        shard: 0,
        depth: 0,
        content_hash: content_hash(&resp.body),
    };

    let returned_path = multi.write(&record).await.unwrap();

    assert_eq!(primary.len(), 1, "primary should have received the record");
    assert_eq!(
        secondary.len(),
        1,
        "secondary should have received the record"
    );
    assert_eq!(
        primary.urls(),
        vec!["https://example.com/x".to_string()],
        "primary urls"
    );
    assert_eq!(
        secondary.urls(),
        vec!["https://example.com/x".to_string()],
        "secondary urls"
    );

    // Returned path must be the primary's, since the metadata ledger
    // only carries one blob_path and ADR-0013 specifies the
    // first-configured store as canonical.
    assert!(
        returned_path.starts_with("memory://"),
        "returned: {returned_path}"
    );
    assert_eq!(returned_path, "memory://https://example.com/x");
}

#[tokio::test]
async fn rejects_empty_store_list() {
    let result = MultiStore::new(Vec::new());
    assert!(result.is_err());
}

#[tokio::test]
async fn flush_propagates_to_all_inner_stores() {
    let primary = Arc::new(InMemoryStore::new());
    let secondary = Arc::new(InMemoryStore::new());
    let multi =
        MultiStore::new(vec![primary as Arc<dyn Store>, secondary as Arc<dyn Store>]).unwrap();
    // InMemoryStore's flush is a no-op but the test verifies the
    // MultiStore call returns Ok rather than panicking on an empty
    // chain.
    multi.flush().await.unwrap();
}

fn fixture(url_str: &str, body: &[u8]) -> (ParsedDocument, FetchResponse) {
    let url = CanonicalUrl::parse(url_str).unwrap();
    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), "text/html".to_string());
    let resp = FetchResponse {
        url: url.clone(),
        status: 200,
        headers,
        body: Bytes::copy_from_slice(body),
        redirect_chain: Vec::new(),
        fetched_at: Utc::now(),
        duration: Duration::from_millis(11),
    };
    let doc = ParsedDocument {
        url,
        status: 200,
        title: None,
        text: None,
        outbound_links: Vec::new(),
        fetched_at: resp.fetched_at,
    };
    (doc, resp)
}
