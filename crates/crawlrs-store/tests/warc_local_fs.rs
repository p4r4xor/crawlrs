//! WarcStore roundtrip against local filesystem.
//!
//! Writes a few records, flushes, decompresses the resulting
//! concatenated-gzip `.warc.gz` file, and asserts on the WARC framing
//! (warcinfo opener present, one response record per fetch, target
//! URIs and body bytes preserved). v1 verification is grep-shaped on
//! the decoded text, not full WARC parsing; sufficient to catch
//! regressions in the encoder or path layout.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use crawlrs_core::{CanonicalUrl, FetchResponse, ParsedDocument, Store, StoreRecord, content_hash};
use crawlrs_store::{PathBuilder, RotationPolicy, WarcStore};
use flate2::read::MultiGzDecoder;
use object_store::local::LocalFileSystem;

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
        duration: Duration::from_millis(7),
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

fn record<'a>(
    doc: &'a ParsedDocument,
    resp: &'a FetchResponse,
    run_id: &'a str,
    shard: u32,
) -> StoreRecord<'a> {
    StoreRecord {
        doc,
        resp,
        run_id,
        shard,
        depth: 0,
        content_hash: content_hash(&resp.body),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warc_roundtrip_one_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let paths = PathBuilder::new("crawlrs", "warc-run", "0");
    let store = WarcStore::new(backend, paths, RotationPolicy::default(), "warc-run");

    let (doc1, resp1) = fixture("https://example.com/alpha", b"<html>alpha</html>");
    let (doc2, resp2) = fixture("https://example.com/beta", b"<html>beta</html>");

    store
        .write(&record(&doc1, &resp1, "warc-run", 2))
        .await
        .unwrap();
    store
        .write(&record(&doc2, &resp2, "warc-run", 2))
        .await
        .unwrap();
    store.flush().await.unwrap();

    let warc_files = walk(tmp.path(), "warc.gz");
    assert_eq!(warc_files.len(), 1, "got: {warc_files:?}");

    let path = &warc_files[0];
    let path_str = path.to_string_lossy();
    assert!(path_str.contains("run=warc-run"));
    assert!(path_str.contains("shard=2"));
    assert!(path_str.contains("worker=0"));
    assert!(path_str.contains("/warc/"));
    assert!(path_str.ends_with(".warc.gz"));

    // Decompress concatenated gzip streams and grep for expected WARC records.
    let raw = std::fs::read(path).unwrap();
    let mut decoded = String::new();
    MultiGzDecoder::new(raw.as_slice())
        .read_to_string(&mut decoded)
        .unwrap();

    assert!(
        decoded.contains("WARC-Type: warcinfo"),
        "missing warcinfo opener:\n{decoded}"
    );
    assert!(decoded.contains("software: crawlrs/0.0.1"));
    assert!(decoded.contains("run-id: warc-run"));

    let response_records = decoded.matches("WARC-Type: response").count();
    assert_eq!(
        response_records, 2,
        "expected 2 response records; decoded:\n{decoded}"
    );

    assert!(decoded.contains("WARC-Target-URI: https://example.com/alpha"));
    assert!(decoded.contains("WARC-Target-URI: https://example.com/beta"));
    assert!(decoded.contains("HTTP/1.1 200 OK"));
    assert!(decoded.contains("<html>alpha</html>"));
    assert!(decoded.contains("<html>beta</html>"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warc_two_shards_two_files() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let paths = PathBuilder::new("crawlrs", "warc-run", "0");
    let store = WarcStore::new(backend, paths, RotationPolicy::default(), "warc-run");

    let (doc_a, resp_a) = fixture("https://a.example/", b"<html>a</html>");
    let (doc_b, resp_b) = fixture("https://b.example/", b"<html>b</html>");
    store
        .write(&record(&doc_a, &resp_a, "warc-run", 0))
        .await
        .unwrap();
    store
        .write(&record(&doc_b, &resp_b, "warc-run", 7))
        .await
        .unwrap();
    store.flush().await.unwrap();

    let warc_files = walk(tmp.path(), "warc.gz");
    assert_eq!(warc_files.len(), 2);
    let paths: Vec<String> = warc_files
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    assert!(paths.iter().any(|p| p.contains("shard=0")));
    assert!(paths.iter().any(|p| p.contains("shard=7")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warc_rotates_on_row_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let paths = PathBuilder::new("crawlrs", "warc-run", "0");
    let rotation = RotationPolicy {
        max_rows: 2,
        max_bytes: usize::MAX,
        max_duration: Duration::from_secs(60 * 60),
    };
    let store = WarcStore::new(backend, paths, rotation, "warc-run");

    let (doc, resp) = fixture("https://example.com/p", b"<html>p</html>");
    let r = record(&doc, &resp, "warc-run", 0);
    store.write(&r).await.unwrap();
    store.write(&r).await.unwrap();

    let warc_files = walk(tmp.path(), "warc.gz");
    assert_eq!(warc_files.len(), 1, "rotation should have flushed");
}

fn walk(root: &std::path::Path, ext: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    fn rec(dir: &std::path::Path, ext: &str, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                rec(&path, ext, out);
            } else if path.to_string_lossy().ends_with(ext) {
                out.push(path);
            }
        }
    }
    rec(root, ext, &mut out);
    out.sort();
    out
}
