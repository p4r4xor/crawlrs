//! ParquetStore roundtrip against the local filesystem backend.
//!
//! Writes a few records, flushes, walks the resulting directory tree,
//! and reads each Parquet file back via the parquet crate's row-group
//! reader to verify schema, row count, and a handful of column values.
//! S3 wire-path coverage is owned by the upstream `object_store`
//! crate's own test suite; we test the Parquet writer + path layout
//! here against a tempdir.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BinaryArray, Int32Array, Int64Array, StringArray};
use bytes::Bytes;
use chrono::Utc;
use crawlrs_core::{CanonicalUrl, FetchResponse, ParsedDocument, Store, StoreRecord, content_hash};
use crawlrs_store::{ParquetStore, PathBuilder, RotationPolicy};
use object_store::local::LocalFileSystem;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn store_record<'a>(
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

fn fixture(url_str: &str, body: &[u8], title: &str) -> (ParsedDocument, FetchResponse) {
    let url = CanonicalUrl::parse(url_str).unwrap();
    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), "text/html".to_string());
    headers.insert("server".to_string(), "test/1.0".to_string());

    let resp = FetchResponse {
        url: url.clone(),
        status: 200,
        headers: Box::new(headers),
        body: Bytes::copy_from_slice(body),
        redirect_chain: Vec::new(),
        fetched_at: Utc::now(),
        duration: Duration::from_millis(123),
    };
    let doc = ParsedDocument {
        url,
        status: 200,
        title: Some(title.to_string()),
        text: Some(Box::new("body text".to_string())),
        outbound_links: Box::new(vec![
            CanonicalUrl::parse("https://example.com/other").unwrap(),
        ]),
        fetched_at: resp.fetched_at,
    };
    (doc, resp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_then_read_back_one_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let paths = PathBuilder::new("crawlrs", "run-test", "0");
    let store = ParquetStore::new(backend.clone(), paths, RotationPolicy::default());

    let (doc1, resp1) = fixture("https://example.com/a", b"<html>a</html>", "Page A");
    let (doc2, resp2) = fixture("https://example.com/b", b"<html>b</html>", "Page B");

    store
        .write(&store_record(&doc1, &resp1, "run-test", 3, 0))
        .await
        .unwrap();
    store
        .write(&store_record(&doc2, &resp2, "run-test", 3, 1))
        .await
        .unwrap();
    store.flush().await.unwrap();

    // Walk the tempdir and find the one Parquet file we expect.
    let parquet_files = walk_parquet_files(tmp.path());
    assert_eq!(
        parquet_files.len(),
        1,
        "expected 1 parquet file under shard=3 dir, got: {:?}",
        parquet_files
    );

    let file_path = &parquet_files[0];
    let path_str = file_path.to_string_lossy();
    assert!(path_str.contains("run=run-test"), "path: {path_str}");
    assert!(path_str.contains("shard=3"), "path: {path_str}");
    assert!(path_str.contains("worker=0"), "path: {path_str}");
    assert!(path_str.ends_with(".parquet"), "path: {path_str}");

    // Read back via the parquet crate.
    let bytes = std::fs::read(file_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "expected 2 rows, got {total_rows}");

    let batch = &batches[0];
    let url = batch
        .column_by_name("url")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(url.value(0), "https://example.com/a");
    assert_eq!(url.value(1), "https://example.com/b");

    let body = batch
        .column_by_name("body")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(body.value(0), b"<html>a</html>");
    assert_eq!(body.value(1), b"<html>b</html>");

    let shard = batch
        .column_by_name("shard")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(shard.value(0), 3);
    assert_eq!(shard.value(1), 3);

    let fetch_duration_ms = batch
        .column_by_name("fetch_duration_ms")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(fetch_duration_ms.value(0), 123);

    let headers_json = batch
        .column_by_name("headers_json")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let parsed: HashMap<String, String> = serde_json::from_str(headers_json.value(0)).unwrap();
    assert_eq!(
        parsed.get("content-type").map(String::as_str),
        Some("text/html")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_to_two_shards_produces_two_files() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let paths = PathBuilder::new("crawlrs", "run-test", "0");
    let store = ParquetStore::new(backend.clone(), paths, RotationPolicy::default());

    let (doc_a, resp_a) = fixture("https://a.example/page", b"<html>a</html>", "A");
    let (doc_b, resp_b) = fixture("https://b.example/page", b"<html>b</html>", "B");
    store
        .write(&store_record(&doc_a, &resp_a, "run-test", 0, 0))
        .await
        .unwrap();
    store
        .write(&store_record(&doc_b, &resp_b, "run-test", 7, 0))
        .await
        .unwrap();
    store.flush().await.unwrap();

    let parquet_files = walk_parquet_files(tmp.path());
    assert_eq!(parquet_files.len(), 2, "expected one file per shard");
    let paths: Vec<String> = parquet_files
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    assert!(paths.iter().any(|p| p.contains("shard=0")));
    assert!(paths.iter().any(|p| p.contains("shard=7")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_by_row_count_closes_file_eagerly() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let paths = PathBuilder::new("crawlrs", "run-test", "0");
    let rotation = RotationPolicy {
        max_rows: 2,
        max_bytes: usize::MAX,
        max_duration: Duration::from_secs(60 * 60),
    };
    let store = ParquetStore::new(backend.clone(), paths, rotation);

    let (doc, resp) = fixture("https://example.com/p", b"<html>p</html>", "P");
    let record = store_record(&doc, &resp, "run-test", 0, 0);

    // Two writes hit the row cap; the second triggers rotation and uploads.
    store.write(&record).await.unwrap();
    store.write(&record).await.unwrap();
    // No flush() yet; the rotation should already have produced a file.
    let parquet_files = walk_parquet_files(tmp.path());
    assert_eq!(
        parquet_files.len(),
        1,
        "rotation should have flushed by now"
    );
}

fn walk_parquet_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    walk(root, &mut out);
    out.sort();
    out
}

fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
}
