//! Path-layout helper shared by `ParquetStore` and `WarcStore`.
//!
//! The convention:
//! ```text
//! <base>/run=<run_id>/shard=<shard>/worker=<worker_id>/<format>/part-<startts_ms>.<ext>
//! ```
//! Hive-style `key=value` directory components so DuckDB / Spark
//! auto-detect them as filterable partitions.

use object_store::path::Path;

#[derive(Debug, Clone)]
pub struct PathBuilder {
    base: String,
    run_id: String,
    worker_id: String,
}

impl PathBuilder {
    pub fn new(
        base: impl Into<String>,
        run_id: impl Into<String>,
        worker_id: impl Into<String>,
    ) -> Self {
        Self {
            base: base.into(),
            run_id: run_id.into(),
            worker_id: worker_id.into(),
        }
    }

    pub fn parquet(&self, shard: u32, startts_ms: u64) -> Path {
        self.compose("parquet", "parquet", shard, startts_ms)
    }

    pub fn warc(&self, shard: u32, startts_ms: u64) -> Path {
        self.compose("warc", "warc.gz", shard, startts_ms)
    }

    fn compose(&self, format: &str, ext: &str, shard: u32, startts_ms: u64) -> Path {
        Path::from(format!(
            "{}/run={}/shard={}/worker={}/{}/part-{:013}.{}",
            self.base.trim_end_matches('/'),
            self.run_id,
            shard,
            self.worker_id,
            format,
            startts_ms,
            ext,
        ))
    }
}
