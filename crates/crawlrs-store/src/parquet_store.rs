//! `ParquetStore`: per-fetch records → buffered owned rows → Arrow
//! RecordBatch → Parquet file (zstd, default row groups) → object_store
//! backend.
//!
//! Concurrency model: one `Mutex<HashMap<shard, ActiveFile>>`. Writes
//! buffer rows under the lock (cheap), then check rotation triggers.
//! When rotation fires, the active file is removed from the map, the
//! lock is released, and the heavy work (build columnar arrays, encode
//! Parquet, upload to object_store) happens off-lock. Per-shard state
//! means the path layout's `shard=` partition stays coherent.
//!
//! Schema is built once at construction and reused. Headers are
//! encoded as a JSON string column for v1 simplicity (DuckDB's
//! json_extract / json_each handles this fine downstream); revisiting
//! to native Arrow Map type is a follow-up if a query pattern requires
//! it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use arrow::array::{
    ArrayRef, BinaryBuilder, Int32Array, Int64Array, ListBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use bytes::Bytes;
use crawlrs_core::{Result, Store, StoreRecord};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

use crate::error::StoreError;
use crate::path::PathBuilder;
use crate::rotation::RotationPolicy;

pub struct ParquetStore {
    backend: Arc<dyn ObjectStore>,
    paths: PathBuilder,
    rotation: RotationPolicy,
    schema: Arc<Schema>,
    writer_props: WriterProperties,
    state: Mutex<HashMap<u32, ActiveFile>>,
}

/// In-memory buffer for one shard's in-progress file. Rows are stored
/// pre-encoded (as `OwnedRow`) so the lock-held path is O(push); the
/// expensive Arrow / Parquet work happens after the lock is released.
struct ActiveFile {
    seq: u32,
    window_start_ms: u64,
    raw_bytes: usize,
    opened_at: Instant,
    rows: Vec<OwnedRow>,
}

struct OwnedRow {
    url: String,
    final_url: String,
    fetched_at_micros: i64,
    status: i32,
    content_type: Option<String>,
    content_hash: i64,
    body: Bytes,
    text: Option<String>,
    title: Option<String>,
    discovered_links: Vec<String>,
    headers_json: String,
    fetch_duration_ms: i64,
    run_id: String,
    shard: i32,
    depth: i32,
}

impl ParquetStore {
    pub fn new(
        backend: Arc<dyn ObjectStore>,
        paths: PathBuilder,
        rotation: RotationPolicy,
    ) -> Self {
        let schema = build_schema();
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        Self {
            backend,
            paths,
            rotation,
            schema,
            writer_props,
            state: Mutex::new(HashMap::new()),
        }
    }

    async fn flush_one(&self, shard: u32, file: ActiveFile) -> Result<Path> {
        let path = self.paths.parquet(shard, file.window_start_ms, file.seq);
        debug!(
            target = %path,
            rows = file.rows.len(),
            raw_bytes = file.raw_bytes,
            "ParquetStore: flushing file"
        );

        let bytes = encode(&file.rows, &self.schema, &self.writer_props)?;
        self.backend
            .put(&path, bytes.into())
            .await
            .map_err(StoreError::from)?;
        Ok(path)
    }
}

#[async_trait]
impl Store for ParquetStore {
    #[instrument(skip(self, record), fields(shard = record.shard, url = %record.doc.url))]
    async fn write(&self, record: &StoreRecord<'_>) -> Result<String> {
        let started_at = Instant::now();
        let row = OwnedRow::from_record(record);
        let raw_bytes = row.body.len();

        // Lock state, push the row, decide rotation.
        let mut state = self.state.lock().await;
        let entry = state.entry(record.shard).or_insert_with(|| ActiveFile {
            seq: 0,
            window_start_ms: chrono::Utc::now().timestamp_millis() as u64,
            raw_bytes: 0,
            opened_at: Instant::now(),
            rows: Vec::new(),
        });
        entry.rows.push(row);
        entry.raw_bytes += raw_bytes;

        let planned_path = self
            .paths
            .parquet(record.shard, entry.window_start_ms, entry.seq);
        let should_rotate =
            self.rotation
                .should_rotate(entry.rows.len(), entry.raw_bytes, entry.opened_at);
        let buffer_bytes_now = entry.raw_bytes;
        let shard_label = record.shard.to_string();

        let result = if should_rotate {
            let active = state.remove(&record.shard).expect("just inserted");
            drop(state);
            metrics::counter!(
                crate::metrics::STORE_ROTATION_TOTAL,
                "format" => crate::metrics::FORMAT_PARQUET,
                "shard" => shard_label.clone(),
            )
            .increment(1);
            metrics::gauge!(
                crate::metrics::STORE_BUFFER_BYTES,
                "format" => crate::metrics::FORMAT_PARQUET,
                "shard" => shard_label.clone(),
            )
            .set(0.0);
            self.flush_one(record.shard, active).await
        } else {
            drop(state);
            metrics::gauge!(
                crate::metrics::STORE_BUFFER_BYTES,
                "format" => crate::metrics::FORMAT_PARQUET,
                "shard" => shard_label.clone(),
            )
            .set(buffer_bytes_now as f64);
            Ok(planned_path.clone())
        };

        metrics::histogram!(
            crate::metrics::STORE_WRITE_SECONDS,
            "format" => crate::metrics::FORMAT_PARQUET,
        )
        .record(started_at.elapsed().as_secs_f64());

        result?;
        Ok(planned_path.to_string())
    }

    async fn flush(&self) -> Result<()> {
        // Drain all active files atomically, then upload after releasing the lock.
        let actives: Vec<(u32, ActiveFile)> = {
            let mut state = self.state.lock().await;
            state.drain().collect()
        };
        for (shard, file) in actives {
            self.flush_one(shard, file).await?;
        }
        Ok(())
    }
}

impl OwnedRow {
    fn from_record(record: &StoreRecord<'_>) -> Self {
        let content_type = record
            .resp
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            .map(|(_, v)| v.clone());
        let headers_json =
            serde_json::to_string(&record.resp.headers).unwrap_or_else(|_| "{}".to_string());
        Self {
            url: record.doc.url.as_str().to_string(),
            final_url: record.resp.url.as_str().to_string(),
            fetched_at_micros: record.resp.fetched_at.timestamp_micros(),
            status: record.resp.status as i32,
            content_type,
            content_hash: record.content_hash as i64,
            body: record.resp.body.clone(),
            text: record.doc.text.clone(),
            title: record.doc.title.clone(),
            discovered_links: record
                .doc
                .outbound_links
                .iter()
                .map(|u| u.as_str().to_string())
                .collect(),
            headers_json,
            fetch_duration_ms: record.resp.duration.as_millis() as i64,
            run_id: record.run_id.to_string(),
            shard: record.shard as i32,
            depth: record.depth as i32,
        }
    }
}

fn build_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, false),
        Field::new("final_url", DataType::Utf8, false),
        Field::new(
            "fetched_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("status", DataType::Int32, false),
        Field::new("content_type", DataType::Utf8, true),
        Field::new("content_hash", DataType::Int64, false),
        Field::new("body", DataType::Binary, false),
        Field::new("text", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        // The item field is `nullable: true` to match Arrow's
        // ListBuilder<StringBuilder> default field metadata. The
        // *values* we write are always present (we collect from a
        // Vec<String>), but the schema declaration has to agree with
        // the builder's emitted field shape.
        Field::new(
            "discovered_links",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("headers_json", DataType::Utf8, false),
        Field::new("fetch_duration_ms", DataType::Int64, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("shard", DataType::Int32, false),
        Field::new("depth", DataType::Int32, false),
    ]))
}

fn encode(rows: &[OwnedRow], schema: &Arc<Schema>, props: &WriterProperties) -> Result<Vec<u8>> {
    let batch = build_batch(rows, schema).map_err(StoreError::from)?;

    let buffer = SharedBuffer::default();
    let writer_buffer = buffer.clone();
    let mut writer = ArrowWriter::try_new(writer_buffer, schema.clone(), Some(props.clone()))
        .map_err(StoreError::from)?;
    writer.write(&batch).map_err(StoreError::from)?;
    writer.close().map_err(StoreError::from)?;

    let bytes = buffer.take_bytes();
    Ok(bytes)
}

fn build_batch(
    rows: &[OwnedRow],
    schema: &Arc<Schema>,
) -> std::result::Result<RecordBatch, arrow::error::ArrowError> {
    let url: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.url)));
    let final_url: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| &r.final_url),
    ));
    let fetched_at: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.fetched_at_micros))
            .with_timezone("UTC"),
    );
    let status: ArrayRef = Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.status)));
    let content_type: ArrayRef = Arc::new(StringArray::from_iter(
        rows.iter().map(|r| r.content_type.clone()),
    ));
    let content_hash: ArrayRef = Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|r| r.content_hash),
    ));

    let mut body_builder = BinaryBuilder::new();
    for r in rows {
        body_builder.append_value(r.body.as_ref());
    }
    let body: ArrayRef = Arc::new(body_builder.finish());

    let text: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|r| r.text.clone())));
    let title: ArrayRef = Arc::new(StringArray::from_iter(rows.iter().map(|r| r.title.clone())));

    let mut links_builder = ListBuilder::new(StringBuilder::new());
    for r in rows {
        for link in &r.discovered_links {
            links_builder.values().append_value(link);
        }
        links_builder.append(true);
    }
    let discovered_links: ArrayRef = Arc::new(links_builder.finish());

    let headers_json: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| &r.headers_json),
    ));
    let fetch_duration_ms: ArrayRef = Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|r| r.fetch_duration_ms),
    ));
    let run_id: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| &r.run_id),
    ));
    let shard: ArrayRef = Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.shard)));
    let depth: ArrayRef = Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.depth)));

    RecordBatch::try_new(
        schema.clone(),
        vec![
            url,
            final_url,
            fetched_at,
            status,
            content_type,
            content_hash,
            body,
            text,
            title,
            discovered_links,
            headers_json,
            fetch_duration_ms,
            run_id,
            shard,
            depth,
        ],
    )
}

/// Shared `Vec<u8>` adapter so an `ArrowWriter` can stream into a buffer
/// the caller holds an Arc to. After `writer.close()` the bytes are
/// retrievable via `take_bytes` (consuming the last Arc clone).
#[derive(Clone, Default)]
struct SharedBuffer(Arc<StdMutex<Vec<u8>>>);

impl SharedBuffer {
    fn take_bytes(self) -> Vec<u8> {
        // Try to unwrap; if the Arc is still shared (writer didn't drop yet),
        // fall back to cloning the contents. The fallback path goes
        // through std `Mutex::into_inner` to drain the cloned bytes.
        match Arc::try_unwrap(self.0) {
            Ok(mutex) => mutex.into_inner().expect("SharedBuffer mutex was poisoned"),
            Err(arc) => arc.lock().expect("SharedBuffer mutex was poisoned").clone(),
        }
    }
}

impl std::io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
