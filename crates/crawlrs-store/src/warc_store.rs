//! `WarcStore`: archival mirror of every fetch as ISO 28500 records.
//!
//! Per ADR-0013, this is the v1 archival path running alongside
//! `ParquetStore`. Each successful fetch becomes one gzip-framed
//! `WARC-Type: response` record; per-shard files open with one
//! `warcinfo` opener record. Concatenation of independently-gzipped
//! records is the WARC spec shape: tools like `warcio` / `pywb` read
//! `.warc.gz` files as a sequence of gzip streams.
//!
//! Concurrency mirrors `ParquetStore`: per-shard active file under
//! `Mutex<HashMap<u32, ActiveFile>>`. Encoding (CRLF framing + gzip)
//! happens off-lock; only the byte append happens under the lock.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use crawlrs_core::{Result, Store, StoreRecord};
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

use crate::error::StoreError;
use crate::path::PathBuilder;
use crate::rotation::RotationPolicy;
use crate::warc_record::{encode_response, encode_warcinfo};

pub struct WarcStore {
    backend: Arc<dyn ObjectStore>,
    paths: PathBuilder,
    rotation: RotationPolicy,
    run_id: String,
    state: Mutex<HashMap<u32, ActiveFile>>,
}

struct ActiveFile {
    seq: u32,
    window_start_ms: u64,
    raw_bytes: usize,
    rows: usize,
    opened_at: Instant,
    /// Concatenation of per-record gzip streams. Already includes the
    /// `warcinfo` opener record (written when the file is created).
    gzipped: Vec<u8>,
}

impl WarcStore {
    pub fn new(
        backend: Arc<dyn ObjectStore>,
        paths: PathBuilder,
        rotation: RotationPolicy,
        run_id: impl Into<String>,
    ) -> Self {
        Self {
            backend,
            paths,
            rotation,
            run_id: run_id.into(),
            state: Mutex::new(HashMap::new()),
        }
    }

    fn open_file(&self) -> ActiveFile {
        ActiveFile {
            seq: 0,
            window_start_ms: chrono::Utc::now().timestamp_millis() as u64,
            raw_bytes: 0,
            rows: 0,
            opened_at: Instant::now(),
            gzipped: encode_warcinfo(&self.run_id),
        }
    }

    async fn flush_one(&self, shard: u32, file: ActiveFile) -> Result<Path> {
        let path = self.paths.warc(shard, file.window_start_ms, file.seq);
        debug!(
            target = %path,
            rows = file.rows,
            raw_bytes = file.raw_bytes,
            gz_bytes = file.gzipped.len(),
            "WarcStore: flushing file"
        );
        self.backend
            .put(&path, file.gzipped.into())
            .await
            .map_err(StoreError::from)?;
        Ok(path)
    }
}

#[async_trait]
impl Store for WarcStore {
    #[instrument(skip(self, record), fields(shard = record.shard, url = %record.doc.url))]
    async fn write(&self, record: &StoreRecord<'_>) -> Result<String> {
        let started_at = std::time::Instant::now();
        // Encode off-lock; the result is independent of shared state.
        let response_bytes = encode_response(record);
        let raw_bytes = record.resp.body.len();

        let mut state = self.state.lock().await;
        let active = state
            .entry(record.shard)
            .or_insert_with(|| self.open_file());
        active.gzipped.extend_from_slice(&response_bytes);
        active.rows += 1;
        active.raw_bytes += raw_bytes;

        let planned_path = self
            .paths
            .warc(record.shard, active.window_start_ms, active.seq);
        let should_rotate =
            self.rotation
                .should_rotate(active.rows, active.raw_bytes, active.opened_at);
        let gz_bytes_now = active.gzipped.len();
        let shard_label = record.shard.to_string();

        let flush_result = if should_rotate {
            let active = state.remove(&record.shard).expect("just inserted");
            drop(state);
            metrics::counter!(
                crate::metrics::STORE_ROTATION_TOTAL,
                "format" => crate::metrics::FORMAT_WARC,
                "shard" => shard_label.clone(),
            )
            .increment(1);
            metrics::gauge!(
                crate::metrics::STORE_BUFFER_BYTES,
                "format" => crate::metrics::FORMAT_WARC,
                "shard" => shard_label.clone(),
            )
            .set(0.0);
            self.flush_one(record.shard, active).await
        } else {
            drop(state);
            metrics::gauge!(
                crate::metrics::STORE_BUFFER_BYTES,
                "format" => crate::metrics::FORMAT_WARC,
                "shard" => shard_label.clone(),
            )
            .set(gz_bytes_now as f64);
            Ok(planned_path.clone())
        };

        metrics::histogram!(
            crate::metrics::STORE_WRITE_SECONDS,
            "format" => crate::metrics::FORMAT_WARC,
        )
        .record(started_at.elapsed().as_secs_f64());

        flush_result?;
        Ok(planned_path.to_string())
    }

    async fn flush(&self) -> Result<()> {
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
