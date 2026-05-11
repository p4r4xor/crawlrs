//! Fetch-layer metric names + descriptors.
//!
//! Just the wire-level identifiers (metric names + `register()`); the
//! categorical vocabulary that fills `kind`, `stage`, `content_type`,
//! and `status_class` lives in `crate::classify` because those values
//! are domain classifications that happen to be used as labels, not
//! the other way around.

use metrics::{Unit, describe_counter, describe_histogram};

pub const FETCH_SECONDS: &str = "crawlrs_fetch_seconds";
pub const FETCH_STAGE_SECONDS: &str = "crawlrs_fetch_stage_seconds";
pub const FETCH_RESPONSE_TOTAL: &str = "crawlrs_fetch_response_total";
pub const FETCH_BODY_BYTES: &str = "crawlrs_fetch_body_bytes";

pub fn register() {
    describe_histogram!(
        FETCH_SECONDS,
        Unit::Seconds,
        "Wall-clock duration of one Fetcher::fetch call, by kind \
         (page vs robots.txt)."
    );
    describe_histogram!(
        FETCH_STAGE_SECONDS,
        Unit::Seconds,
        "Per-stage duration of a fetch, labelled by `kind` (page / \
         robots) and `stage` (request = wreq send-to-headers, \
         body = streaming body read, total = full elapsed). The split \
         localizes latency to the request setup vs body transfer."
    );
    describe_counter!(
        FETCH_RESPONSE_TOTAL,
        "HTTP responses received, bucketed by status class (2xx/3xx/4xx/5xx)."
    );
    describe_histogram!(
        FETCH_BODY_BYTES,
        Unit::Bytes,
        "Response body size in bytes on successful fetch. Labelled by \
         `kind` (page / robots) and `content_type` (html / non_html / \
         unknown). content_type is derived from the Content-Type \
         header; unknown means missing or unparseable."
    );
}
