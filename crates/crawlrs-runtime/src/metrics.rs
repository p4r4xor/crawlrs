//! Runtime-layer metric names + descriptors.
//!
//! Metric names are an external operational contract: dashboards and
//! runbooks reference these strings, so they must be stable across
//! crawlrs versions. Defining them as `pub const` here puts them all
//! in one place and protects against typos in the emission sites (the
//! constants are the single source of truth).
//!
//! `register()` is the one-shot description hook: callers (the binary
//! or a test harness) install a `metrics` recorder, then call
//! `register()` to attach human-readable help text + units to each
//! metric. Without registration the metrics still emit, just without
//! the descriptions a Prometheus scraper would surface.

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

pub const URLS_FETCHED_TOTAL: &str = "crawlrs_urls_fetched_total";
pub const URLS_FAILED_TOTAL: &str = "crawlrs_urls_failed_total";
pub const URLS_SKIPPED_TOTAL: &str = "crawlrs_urls_skipped_total";
pub const WORKERS_ACTIVE: &str = "crawlrs_workers_active";
pub const PIPELINE_SECONDS: &str = "crawlrs_pipeline_seconds";

pub const SKIP_ALREADY_SUCCEEDED: &str = "already_succeeded";
pub const SKIP_ALREADY_DLQ: &str = "already_dlq";
pub const SKIP_POLITENESS_DISALLOWED: &str = "politeness_disallowed";
pub const SKIP_DEPTH_LIMIT: &str = "depth_limit";

/// Attach descriptions (help text + units) to the runtime-layer
/// metrics. Idempotent; safe to call multiple times in tests.
pub fn register() {
    describe_counter!(
        URLS_FETCHED_TOTAL,
        "Total URLs fetched and successfully stored."
    );
    describe_counter!(
        URLS_FAILED_TOTAL,
        "Total URLs whose fetch or parse failed, by FailureKind."
    );
    describe_counter!(
        URLS_SKIPPED_TOTAL,
        "Total URLs that were not fetched, by reason. Covers cross-run \
         dedup hits, politeness disallows, and depth-limit drops."
    );
    describe_gauge!(
        WORKERS_ACTIVE,
        Unit::Count,
        "Currently in-flight URLs across the worker pool."
    );
    describe_histogram!(
        PIPELINE_SECONDS,
        Unit::Seconds,
        "End-to-end per-URL pipeline duration: claim through ack."
    );
}

/// Bucket a `FailureKind` enum variant into a stable string label
/// value. Keeps the `kind` label cardinality bounded to the variant
/// set, per the cardinality discipline of the metric-name contract.
pub fn failure_kind_label(kind: crawlrs_core::FailureKind) -> &'static str {
    use crawlrs_core::FailureKind::*;
    match kind {
        Timeout => "timeout",
        ConnectReset => "connect_reset",
        TooManyRequests => "too_many_requests",
        ServiceUnavailable => "service_unavailable",
        Other => "other",
    }
}
