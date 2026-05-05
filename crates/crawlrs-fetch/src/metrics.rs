//! Fetch-layer metric names + descriptors.
//!
//! Per ADR-0014. The `status_class` label aggregates raw HTTP status
//! codes into 4 buckets (`2xx`/`3xx`/`4xx`/`5xx`) so the counter
//! cardinality stays at 4 instead of ~50 (any-RFC-9110-status).

use crawlrs_core::CanonicalUrl;
use metrics::{Unit, describe_counter, describe_histogram};

pub const FETCH_SECONDS: &str = "crawlrs_fetch_seconds";
pub const FETCH_RESPONSE_TOTAL: &str = "crawlrs_fetch_response_total";
pub const FETCH_BODY_BYTES: &str = "crawlrs_fetch_body_bytes";

pub const KIND_PAGE: &str = "page";
pub const KIND_ROBOTS: &str = "robots";

pub fn register() {
    describe_histogram!(
        FETCH_SECONDS,
        Unit::Seconds,
        "Wall-clock duration of one Fetcher::fetch call, by kind \
         (page vs robots.txt)."
    );
    describe_counter!(
        FETCH_RESPONSE_TOTAL,
        "HTTP responses received, bucketed by status class (2xx/3xx/4xx/5xx)."
    );
    describe_histogram!(
        FETCH_BODY_BYTES,
        Unit::Bytes,
        "Response body size in bytes on successful fetch."
    );
}

/// Bucket an HTTP status code into the 4-element label set for the
/// `status_class` label. Anything outside 100..=599 maps to `"other"`.
pub fn status_class_label(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

/// Classify a URL as a robots.txt fetch or a regular page fetch. The
/// detection is path-based: any URL whose path is exactly
/// `/robots.txt` is a robots fetch. Edge cases (a path called
/// `/robots.txt` that's actually a regular page) are vanishingly rare;
/// the politeness layer is the only thing that fetches /robots.txt
/// internally.
pub fn fetch_kind_label(url: &CanonicalUrl) -> &'static str {
    if url.as_url().path() == "/robots.txt" {
        KIND_ROBOTS
    } else {
        KIND_PAGE
    }
}
