//! Store-layer metric names + descriptors. Per ADR-0014.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub const STORE_WRITE_SECONDS: &str = "crawlrs_store_write_seconds";
pub const STORE_ROTATION_TOTAL: &str = "crawlrs_store_rotation_total";
pub const STORE_BUFFER_BYTES: &str = "crawlrs_store_buffer_bytes";

pub const FORMAT_PARQUET: &str = "parquet";
pub const FORMAT_WARC: &str = "warc";

pub fn register() {
    describe_histogram!(
        STORE_WRITE_SECONDS,
        Unit::Seconds,
        "Wall-clock duration of one Store::write call, by format."
    );
    describe_counter!(
        STORE_ROTATION_TOTAL,
        "File rotations triggered (size / row / time cap), by format and shard."
    );
    describe_gauge!(
        STORE_BUFFER_BYTES,
        Unit::Bytes,
        "Per-shard in-memory buffer size held by the active file, by format."
    );
}
