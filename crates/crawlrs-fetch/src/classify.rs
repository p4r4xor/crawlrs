//! Pure classifiers for fetch-layer domain inputs.
//!
//! Each function maps a domain value (URL, HTTP status, Content-Type
//! header) into a small, named categorical bucket. The output strings
//! double as Prometheus label values, but classification is the
//! primary concern; metric emission is downstream. Callers needing
//! the same bucketing for logging, retry policy, or routing decisions
//! import from here, not from `metrics`.
//!
//! Cardinality discipline: every classifier resolves to one of a
//! fixed set of `&'static str` so the metric series count stays
//! bounded regardless of crawl volume. Adding a new bucket means
//! adding a `pub const` here so dashboards have a stable handle.

use crawlrs_core::CanonicalUrl;

pub const KIND_PAGE: &str = "page";
pub const KIND_ROBOTS: &str = "robots";

pub const STAGE_REQUEST: &str = "request";
pub const STAGE_BODY: &str = "body";
pub const STAGE_TOTAL: &str = "total";

pub const CONTENT_TYPE_HTML: &str = "html";
pub const CONTENT_TYPE_NON_HTML: &str = "non_html";
pub const CONTENT_TYPE_UNKNOWN: &str = "unknown";

/// Classify a URL as a robots.txt fetch or a regular page fetch. The
/// detection is path-based: any URL whose path is exactly
/// `/robots.txt` is a robots fetch. Edge cases (a path called
/// `/robots.txt` that's actually a regular page) are vanishingly rare;
/// the politeness layer is the only thing that fetches /robots.txt
/// internally.
#[must_use]
pub fn fetch_kind(url: &CanonicalUrl) -> &'static str {
    if url.as_url().path() == "/robots.txt" {
        KIND_ROBOTS
    } else {
        KIND_PAGE
    }
}

/// Bucket an HTTP status code into a 5-element categorical set
/// (`2xx` / `3xx` / `4xx` / `5xx` / `other`). Anything outside
/// 100..=599 falls into `other`.
#[must_use]
pub fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

/// Bucket a Content-Type header value into a 3-element categorical
/// set (`html` / `non_html` / `unknown`). Missing or unparsable
/// headers map to `unknown` so absence is distinguishable from an
/// explicit non-html type.
#[must_use]
pub fn content_type(content_type_header: Option<&str>) -> &'static str {
    let Some(value) = content_type_header else {
        return CONTENT_TYPE_UNKNOWN;
    };
    let main_type = value.split(';').next().unwrap_or("").trim();
    if main_type.is_empty() {
        CONTENT_TYPE_UNKNOWN
    } else if main_type.eq_ignore_ascii_case("text/html")
        || main_type.eq_ignore_ascii_case("application/xhtml+xml")
    {
        CONTENT_TYPE_HTML
    } else {
        CONTENT_TYPE_NON_HTML
    }
}
