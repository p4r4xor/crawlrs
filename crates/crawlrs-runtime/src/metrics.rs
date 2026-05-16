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
pub const URLS_REJECTED_TOTAL: &str = "crawlrs_urls_rejected_total";
pub const WORKERS_ACTIVE: &str = "crawlrs_workers_active";
pub const PIPELINE_SECONDS: &str = "crawlrs_pipeline_seconds";
pub const PIPELINE_PHASE_SECONDS: &str = "crawlrs_pipeline_phase_seconds";
pub const WORKER_RESTARTS_TOTAL: &str = "crawlrs_worker_restarts_total";
pub const OUTBOX_PUBLISHED_TOTAL: &str = "crawlrs_outbox_published_total";
pub const DIRECT_DISPATCH_LOST_TOTAL: &str = "crawlrs_direct_dispatch_lost_total";

// Process-health gauges; emitted by the maintenance heartbeat
// alongside the structured log line so the same fields are both
// searchable in logs and graphable in Grafana.
pub const PROCESS_RSS_BYTES: &str = "crawlrs_process_rss_bytes";
pub const PROCESS_HWM_BYTES: &str = "crawlrs_process_hwm_bytes";
pub const PROCESS_VSIZE_BYTES: &str = "crawlrs_process_vsize_bytes";
pub const PROCESS_PEAK_BYTES: &str = "crawlrs_process_peak_bytes";
pub const PROCESS_DATA_BYTES: &str = "crawlrs_process_data_bytes";
pub const PROCESS_THREADS: &str = "crawlrs_process_threads";
pub const PROCESS_FDS: &str = "crawlrs_process_fds";
pub const TOKIO_ALIVE_TASKS: &str = "crawlrs_tokio_alive_tasks";
pub const TOKIO_WORKERS: &str = "crawlrs_tokio_workers";

pub const SKIP_POLITENESS_DISALLOWED: &str = "politeness_disallowed";
pub const SKIP_DEPTH_LIMIT: &str = "depth_limit";

/// Label value for `URLS_REJECTED_TOTAL`. The frontier's atomic
/// per-host counter (set by `[crawl].max_urls`) rejected the URL
/// at submit time before it ever reached a worker. Bounded label
/// set; the family is reserved for future submit-time rejection
/// reasons.
pub const REJECTED_REASON_QUOTA: &str = "quota";

/// Phase labels for `PIPELINE_PHASE_SECONDS`. Six fixed values
/// covering the per-URL pipeline in `worker.rs::UrlPipeline`. Sum
/// across phases approximates `PIPELINE_SECONDS` (small gap covered
/// by accounting between phases).
pub const PHASE_POLITENESS: &str = "politeness";
pub const PHASE_MARK: &str = "mark";
pub const PHASE_FETCH: &str = "fetch";
pub const PHASE_PARSE: &str = "parse";
pub const PHASE_STORE: &str = "store";
pub const PHASE_COMMIT: &str = "commit";

/// Label values for `OUTBOX_PUBLISHED_TOTAL`. The two variants have
/// different units: `success` counts rows shipped to the Frontier,
/// `error` counts publish-error events observed in the drain loop
/// (one increment per failure regardless of batch size). Operators
/// alert on the error rate; throughput dashboards filter to
/// `result=success`. Don't sum across labels.
pub const OUTBOX_RESULT_SUCCESS: &str = "success";
pub const OUTBOX_RESULT_ERROR: &str = "error";

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
    describe_counter!(
        URLS_REJECTED_TOTAL,
        "URLs rejected at submit time before reaching a worker. \
         Labelled by `reason`: today only `quota`, meaning the \
         frontier's per-host `[crawl].max_urls` counter was already \
         at the host's cap. Distinct from `URLS_SKIPPED_TOTAL` \
         (which counts post-claim drops) and `URLS_FAILED_TOTAL` \
         (post-fetch failures)."
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
    describe_histogram!(
        PIPELINE_PHASE_SECONDS,
        Unit::Seconds,
        "Per-phase wall-clock duration within one URL pipeline. \
         Labelled by `phase`: politeness, attempting, fetch, extract, \
         store, commit. Stack the rate-of-sum / rate-of-count for a \
         'where does pipeline time go' breakdown; heatmap each phase \
         for tail-latency drift."
    );
    describe_counter!(
        WORKER_RESTARTS_TOTAL,
        "Worker tasks respawned by the supervisor. Labelled by reason \
         (panic / cancelled / exit_unexpected / join_error). Spike \
         indicates a poison-URL crash loop or transient infra fault."
    );
    describe_counter!(
        OUTBOX_PUBLISHED_TOTAL,
        "Outbox publisher activity, labelled by `result`. \
         `result=success` counts rows successfully shipped from the \
         metadata store into the Frontier (sums to outbound \
         throughput). `result=error` counts publish-error events \
         observed in the drain loop (any failure inside `Outbox::publish`, \
         including the leased SELECT, the ship closure, and the \
         mark-published UPDATE); each event is one increment \
         regardless of how many rows were involved. Don't sum across \
         labels - the units differ."
    );
    describe_counter!(
        DIRECT_DISPATCH_LOST_TOTAL,
        "Outbound URLs dropped under `LinkDispatch::Direct` because \
         the worker's post-commit `Frontier::submit_batch` call \
         errored. One increment per URL lost. Always zero under \
         `LinkDispatch::DurableOutbox`. Operators running Direct \
         mode alert on a non-trivial rate; sustained loss is the \
         signal to flip back to DurableOutbox."
    );
    describe_gauge!(
        PROCESS_RSS_BYTES,
        Unit::Bytes,
        "Resident set size: physical RAM in use right now."
    );
    describe_gauge!(
        PROCESS_HWM_BYTES,
        Unit::Bytes,
        "RSS high-water mark since process start. Useful for spotting \
         transient spikes that don't show in instantaneous rss."
    );
    describe_gauge!(
        PROCESS_VSIZE_BYTES,
        Unit::Bytes,
        "Virtual address space (mapped, not necessarily resident)."
    );
    describe_gauge!(
        PROCESS_PEAK_BYTES,
        Unit::Bytes,
        "VmPeak: virtual-address-space high-water mark since process \
         start. Together with vsize this shows whether the allocator \
         has ever reached above the current footprint."
    );
    describe_gauge!(
        PROCESS_DATA_BYTES,
        Unit::Bytes,
        "VmData: data + stack + heap. Gap with RSS is the canonical \
         allocator-fragmentation signal."
    );
    describe_gauge!(
        PROCESS_THREADS,
        Unit::Count,
        "Process thread count from /proc/self/status."
    );
    describe_gauge!(
        PROCESS_FDS,
        Unit::Count,
        "Open file descriptor count from /proc/self/fd. Includes \
         sockets, files, pipes, anon-inode fds."
    );
    describe_gauge!(
        TOKIO_ALIVE_TASKS,
        Unit::Count,
        "Tokio runtime: total alive tasks across all workers. Climbs \
         past the steady state when a tokio task leaks."
    );
    describe_gauge!(
        TOKIO_WORKERS,
        Unit::Count,
        "Tokio runtime: configured worker thread count. Sanity check \
         against config."
    );
}
