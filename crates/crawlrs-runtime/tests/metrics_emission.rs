//! Metric-name contract: every metric pinned by the contract must be
//! observable under a `DebuggingRecorder`. This is the lowest-cost
//! defense against typos in the emission sites: a mismatch between a
//! constant and the string the recorder sees would silently pass
//! `cargo build` but get caught here.
//!
//! Each metric is "emitted" by directly calling its facade with the
//! `pub const` name from the owning crate. We're not trying to
//! exercise every business code path; we're verifying that the
//! contract symbols exist and that the names they resolve to are the
//! ones the contract commits to.

use std::collections::HashSet;

use metrics_util::debugging::DebuggingRecorder;

#[test]
fn metric_name_contract_holds() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // Tests run in one process; only one global recorder install.
    // If another test in the same binary installed first, ours fails;
    // putting this test in its own integration-test file isolates it.
    recorder.install().expect("install DebuggingRecorder");

    // ---- runtime layer ----
    let w = crawlrs_runtime::metrics::LABEL_WORKER;
    metrics::counter!(crawlrs_runtime::metrics::URLS_FETCHED_TOTAL, w => "pod-0:0").increment(1);
    metrics::counter!(crawlrs_runtime::metrics::URLS_FAILED_TOTAL, "kind" => "other", w => "pod-0:0").increment(1);
    metrics::counter!(
        crawlrs_runtime::metrics::URLS_SKIPPED_TOTAL,
        "reason" => crawlrs_runtime::metrics::SKIP_POLITENESS_DISALLOWED,
        w => "pod-0:0",
    )
    .increment(1);
    metrics::counter!(
        crawlrs_runtime::metrics::URLS_REJECTED_TOTAL,
        "reason" => crawlrs_runtime::metrics::REJECTED_REASON_QUOTA,
        w => "pod-0:0",
    )
    .increment(1);
    metrics::gauge!(crawlrs_runtime::metrics::WORKERS_ACTIVE, w => "pod-0:0").set(0.0);
    metrics::histogram!(crawlrs_runtime::metrics::PIPELINE_SECONDS, w => "pod-0:0").record(0.1);

    // ---- frontier layer ----
    metrics::counter!(
        crawlrs_frontier::metrics::FRONTIER_CLAIM_TOTAL,
        "shard" => "0",
        "outcome" => crawlrs_frontier::metrics::OUTCOME_CLAIMED,
    )
    .increment(1);
    metrics::gauge!(
        crawlrs_frontier::metrics::FRONTIER_READY_LENGTH,
        "shard" => "0",
    )
    .set(0.0);
    metrics::gauge!(
        crawlrs_frontier::metrics::FRONTIER_INFLIGHT_LENGTH,
        "shard" => "0",
    )
    .set(0.0);
    metrics::histogram!(
        crawlrs_frontier::metrics::FRONTIER_CALL_SECONDS,
        "op" => crawlrs_frontier::metrics::OP_CLAIM,
    )
    .record(0.001);
    metrics::histogram!(crawlrs_frontier::metrics::FRONTIER_SUBMIT_BATCH_SIZE).record(100.0);
    metrics::gauge!(crawlrs_frontier::metrics::FRONTIER_POOL_PENDING).set(0.0);
    metrics::counter!(
        crawlrs_frontier::metrics::FRONTIER_BLOOM_TOTAL,
        "verdict" => crawlrs_frontier::metrics::BLOOM_NEW,
    )
    .increment(1);
    metrics::counter!(
        crawlrs_frontier::metrics::FRONTIER_LEASE_RECLAIM_TOTAL,
        "reason" => crawlrs_frontier::metrics::RECLAIM_REASON_EXPIRED,
    )
    .increment(0);
    metrics::counter!(crawlrs_frontier::metrics::FRONTIER_PROMOTED_TOTAL).increment(0);

    // ---- politeness layer ----
    metrics::counter!(crawlrs_politeness::metrics::ROBOTS_CACHE_HITS_TOTAL).increment(1);
    metrics::counter!(crawlrs_politeness::metrics::ROBOTS_CACHE_MISSES_TOTAL).increment(1);
    metrics::histogram!(crawlrs_politeness::metrics::POLITENESS_BACKOFF_SECONDS).record(30.0);
    metrics::counter!(
        crawlrs_politeness::metrics::POLITENESS_BACKOFF_SOURCE_TOTAL,
        "source" => crawlrs_politeness::metrics::SOURCE_COMPUTED,
    )
    .increment(1);
    metrics::counter!(crawlrs_politeness::metrics::POLITENESS_CIRCUIT_OPEN_TOTAL).increment(1);
    metrics::counter!(
        crawlrs_politeness::metrics::POLITENESS_CHECK_TOTAL,
        "decision" => crawlrs_politeness::metrics::DECISION_ALLOW,
    )
    .increment(1);
    metrics::gauge!(crawlrs_politeness::metrics::POLITENESS_POOL_PENDING).set(0.0);

    // ---- fetch layer ----
    metrics::histogram!(
        crawlrs_fetch::metrics::FETCH_SECONDS,
        "kind" => crawlrs_fetch::classify::KIND_PAGE,
    )
    .record(0.5);
    metrics::counter!(
        crawlrs_fetch::metrics::FETCH_RESPONSE_TOTAL,
        "status_class" => "2xx",
    )
    .increment(1);
    metrics::histogram!(
        crawlrs_fetch::metrics::FETCH_BODY_BYTES,
        "kind" => "page",
        "content_type" => "html",
    )
    .record(1024.0);
    metrics::histogram!(
        crawlrs_fetch::metrics::FETCH_STAGE_SECONDS,
        "kind" => "page",
        "stage" => "total",
    )
    .record(0.5);

    // ---- parse layer ----
    metrics::histogram!(crawlrs_parse::metrics::PARSE_SECONDS).record(0.005);
    metrics::histogram!(crawlrs_parse::metrics::PARSE_LINKS_DISCOVERED).record(20.0);

    // ---- metadata layer ----
    metrics::histogram!(
        crawlrs_metadata::metrics::METADATA_QUERY_SECONDS,
        "op" => crawlrs_metadata::metrics::OP_GET,
    )
    .record(0.001);
    metrics::gauge!(crawlrs_metadata::metrics::METADATA_POOL_PENDING).set(0.0);
    metrics::gauge!(crawlrs_metadata::metrics::DLQ_SIZE).set(0.0);

    // ---- store layer ----
    metrics::histogram!(
        crawlrs_store::metrics::STORE_WRITE_SECONDS,
        "format" => crawlrs_store::metrics::FORMAT_PARQUET,
    )
    .record(0.01);
    metrics::counter!(
        crawlrs_store::metrics::STORE_ROTATION_TOTAL,
        "format" => crawlrs_store::metrics::FORMAT_PARQUET,
        "shard" => "0",
    )
    .increment(1);
    metrics::gauge!(
        crawlrs_store::metrics::STORE_BUFFER_BYTES,
        "format" => crawlrs_store::metrics::FORMAT_PARQUET,
        "shard" => "0",
    )
    .set(0.0);

    // ---- snapshot + assert ----
    let snapshot = snapshotter.snapshot();
    let captured: HashSet<String> = snapshot
        .into_vec()
        .into_iter()
        .map(|(key, _, _, _)| key.key().name().to_string())
        .collect();

    let expected: Vec<&str> = vec![
        crawlrs_runtime::metrics::URLS_FETCHED_TOTAL,
        crawlrs_runtime::metrics::URLS_FAILED_TOTAL,
        crawlrs_runtime::metrics::URLS_SKIPPED_TOTAL,
        crawlrs_runtime::metrics::URLS_REJECTED_TOTAL,
        crawlrs_runtime::metrics::WORKERS_ACTIVE,
        crawlrs_runtime::metrics::PIPELINE_SECONDS,
        crawlrs_frontier::metrics::FRONTIER_CLAIM_TOTAL,
        crawlrs_frontier::metrics::FRONTIER_CALL_SECONDS,
        crawlrs_frontier::metrics::FRONTIER_SUBMIT_BATCH_SIZE,
        crawlrs_frontier::metrics::FRONTIER_POOL_PENDING,
        crawlrs_frontier::metrics::FRONTIER_BLOOM_TOTAL,
        crawlrs_frontier::metrics::FRONTIER_LEASE_RECLAIM_TOTAL,
        crawlrs_frontier::metrics::FRONTIER_PROMOTED_TOTAL,
        crawlrs_frontier::metrics::FRONTIER_READY_LENGTH,
        crawlrs_frontier::metrics::FRONTIER_INFLIGHT_LENGTH,
        crawlrs_politeness::metrics::ROBOTS_CACHE_HITS_TOTAL,
        crawlrs_politeness::metrics::ROBOTS_CACHE_MISSES_TOTAL,
        crawlrs_politeness::metrics::POLITENESS_BACKOFF_SECONDS,
        crawlrs_politeness::metrics::POLITENESS_BACKOFF_SOURCE_TOTAL,
        crawlrs_politeness::metrics::POLITENESS_CIRCUIT_OPEN_TOTAL,
        crawlrs_politeness::metrics::POLITENESS_CHECK_TOTAL,
        crawlrs_politeness::metrics::POLITENESS_POOL_PENDING,
        crawlrs_fetch::metrics::FETCH_SECONDS,
        crawlrs_fetch::metrics::FETCH_RESPONSE_TOTAL,
        crawlrs_fetch::metrics::FETCH_BODY_BYTES,
        crawlrs_parse::metrics::PARSE_SECONDS,
        crawlrs_parse::metrics::PARSE_LINKS_DISCOVERED,
        crawlrs_metadata::metrics::METADATA_QUERY_SECONDS,
        crawlrs_metadata::metrics::METADATA_POOL_PENDING,
        crawlrs_metadata::metrics::DLQ_SIZE,
        crawlrs_store::metrics::STORE_WRITE_SECONDS,
        crawlrs_store::metrics::STORE_ROTATION_TOTAL,
        crawlrs_store::metrics::STORE_BUFFER_BYTES,
    ];

    assert_eq!(
        expected.len(),
        33,
        "expected 33 distinct metric names per the metric-name contract; \
         if you intentionally added or removed one, update both this \
         assertion and the contract"
    );

    for name in &expected {
        assert!(
            captured.contains(*name),
            "metric `{name}` not captured by the recorder; \
             check the emission site uses the `pub const` from its crate's `metrics` module"
        );
    }

    // Also verify each name matches the exact string the contract
    // commits to. Catches accidental constant rename that drifts from
    // the contract.
    let exact_names: &[&str] = &[
        "crawlrs_urls_fetched_total",
        "crawlrs_urls_failed_total",
        "crawlrs_urls_skipped_total",
        "crawlrs_urls_rejected_total",
        "crawlrs_workers_active",
        "crawlrs_pipeline_seconds",
        "crawlrs_frontier_claim_total",
        "crawlrs_frontier_call_seconds",
        "crawlrs_frontier_submit_batch_size",
        "crawlrs_frontier_pool_pending",
        "crawlrs_frontier_bloom_total",
        "crawlrs_frontier_lease_reclaim_total",
        "crawlrs_frontier_promoted_total",
        "crawlrs_frontier_ready_length",
        "crawlrs_frontier_inflight_length",
        "crawlrs_robots_cache_hits_total",
        "crawlrs_robots_cache_misses_total",
        "crawlrs_politeness_backoff_seconds",
        "crawlrs_politeness_backoff_source_total",
        "crawlrs_politeness_circuit_open_total",
        "crawlrs_politeness_check_total",
        "crawlrs_politeness_pool_pending",
        "crawlrs_fetch_seconds",
        "crawlrs_fetch_response_total",
        "crawlrs_fetch_body_bytes",
        "crawlrs_parse_seconds",
        "crawlrs_parse_links_discovered",
        "crawlrs_metadata_query_seconds",
        "crawlrs_metadata_pool_pending",
        "crawlrs_dlq_size",
        "crawlrs_store_write_seconds",
        "crawlrs_store_rotation_total",
        "crawlrs_store_buffer_bytes",
    ];
    for name in exact_names {
        assert!(
            captured.contains(*name),
            "exact name `{name}` from the metric-name contract not captured; \
             a constant somewhere drifted from the contract"
        );
    }
}
