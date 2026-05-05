//! ParquetStore against an S3-compatible backend (MinIO via
//! testcontainers).
//!
//! Exercises the `object_store::aws::AmazonS3` write path end to end:
//! bucket creation via `mc` exec inside the container, multipart-aware
//! PUT through object_store, list/iterate over the bucket to verify
//! the resulting object's path layout.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use crawlrs_core::{CanonicalUrl, FetchResponse, ParsedDocument, Store, StoreRecord, content_hash};
use crawlrs_store::{ParquetStore, PathBuilder, RotationPolicy};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use testcontainers::core::{CmdWaitFor, ExecCommand};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

const BUCKET: &str = "crawlrs-test";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_store_writes_to_minio() {
    let container = MinIO::default().start().await.unwrap();

    // Create the bucket via the container's bundled `mc` client. The
    // testcontainers Image waits for "API:" before returning, so the
    // server is reachable at this point; mc just needs to register the
    // alias and run mb.
    let cmd = ExecCommand::new([
        "sh",
        "-c",
        "mc alias set local http://localhost:9000 minioadmin minioadmin \
         && mc mb local/crawlrs-test",
    ])
    .with_cmd_ready_condition(CmdWaitFor::exit_code(0));
    container.exec(cmd).await.unwrap();

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let endpoint = format!("http://{host}:{port}");

    let backend: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_region("us-east-1")
            .with_bucket_name(BUCKET)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap(),
    );

    let paths = PathBuilder::new("crawlrs", "run-minio", "0");
    let store = ParquetStore::new(backend.clone(), paths, RotationPolicy::default());

    let (doc1, resp1) = fixture("https://example.com/a", b"<html>a</html>", "Page A");
    let (doc2, resp2) = fixture("https://example.com/b", b"<html>b</html>", "Page B");

    store
        .write(&record(&doc1, &resp1, "run-minio", 5, 0))
        .await
        .unwrap();
    store
        .write(&record(&doc2, &resp2, "run-minio", 5, 1))
        .await
        .unwrap();
    store.flush().await.unwrap();

    // List objects under our prefix and verify exactly one Parquet
    // file landed at the expected shape.
    let prefix = object_store::path::Path::from("crawlrs/run=run-minio");
    let mut stream = backend.list(Some(&prefix));
    let mut parquet_keys: Vec<String> = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.expect("list error");
        let key = meta.location.to_string();
        if key.ends_with(".parquet") {
            parquet_keys.push(key);
        }
    }
    assert_eq!(
        parquet_keys.len(),
        1,
        "expected exactly one parquet file under the prefix; got {parquet_keys:?}"
    );
    let key = &parquet_keys[0];
    assert!(key.contains("run=run-minio"), "key: {key}");
    assert!(key.contains("shard=5"), "key: {key}");
    assert!(key.contains("worker=0"), "key: {key}");
    assert!(key.contains("/parquet/"), "key: {key}");
}

fn fixture(url_str: &str, body: &[u8], title: &str) -> (ParsedDocument, FetchResponse) {
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
        duration: Duration::from_millis(42),
    };
    let doc = ParsedDocument {
        url,
        status: 200,
        title: Some(title.to_string()),
        text: Some(format!("text of {title}")),
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
    depth: u32,
) -> StoreRecord<'a> {
    StoreRecord {
        doc,
        resp,
        run_id,
        shard,
        depth,
        content_hash: content_hash(&resp.body),
    }
}
