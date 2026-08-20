//! `crawlrs crawl` orchestration. Wires config, factory, HTTP host,
//! maintenance loop, shutdown signal, and the runtime worker pool
//! into a single tokio process.

use std::sync::Arc;

use anyhow::{Context, Result};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tokio::sync::watch;
use tracing::info;

use crate::cli::CrawlArgs;
use crate::config::CrawlrsConfig;
use crate::factory;
use crate::http::{self, ProbeState};
use crate::maintenance;
use crate::shutdown;

/// Latency buckets: 1ms..30s, geometric. Covers the realistic span of
/// per-stage timings (fetch, parse, store write, Postgres query) and
/// gives `histogram_quantile()` enough resolution to compute p50/p95/p99
/// without empty cells in the tail.
const SECONDS_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Body-size buckets: 1KiB..16MiB, geometric. Web pages cluster around
/// 10-200KiB; the tail captures large blobs that drive memory + parse
/// pressure. Upper bound is one factor above `max_body_bytes = 5MiB`.
const BYTES_BUCKETS: &[f64] = &[
    1024.0,
    4096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
];

/// Count buckets for cardinality-style histograms (submit batch size,
/// outbound links per page): 1..5000.
const COUNT_BUCKETS: &[f64] = &[
    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0,
];

pub async fn crawl(args: CrawlArgs) -> Result<()> {
    let config = CrawlrsConfig::load(&args.config)
        .with_context(|| format!("loading config {}", args.config.display()))?;
    info!(summary = %config.summary(), "config loaded");

    install_metrics_descriptions();

    // Install the Prometheus recorder *before* any subsystem emits.
    // Otherwise early emissions (during construction) are lost.
    //
    // Without explicit buckets, histograms only emit `_sum`/`_count`
    // (Summary-shape), which makes `histogram_quantile()` return NaN in
    // dashboards. We register three bucket families by name suffix /
    // exact match so every histogram exposes a `_bucket` series:
    //
    //   * `_seconds` -> latency buckets covering 1ms..30s.
    //   * `fetch_body_bytes` -> 1KiB..16MiB body sizes.
    //   * `*_batch_size` / `*_links_discovered` -> 1..5000 cardinality.
    let prom_handle = PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), SECONDS_BUCKETS)
        .context("set seconds buckets")?
        .set_buckets_for_metric(
            Matcher::Full("crawlrs_fetch_body_bytes".into()),
            BYTES_BUCKETS,
        )
        .context("set body bytes buckets")?
        .set_buckets_for_metric(
            Matcher::Full("crawlrs_frontier_submit_batch_size".into()),
            COUNT_BUCKETS,
        )
        .context("set submit batch buckets")?
        .set_buckets_for_metric(
            Matcher::Full("crawlrs_parse_links_discovered".into()),
            COUNT_BUCKETS,
        )
        .context("set links discovered buckets")?
        .install_recorder()
        .context("installing PrometheusBuilder recorder")?;
    let prom_handle = Arc::new(prom_handle);

    let probes = Arc::new(ProbeState::new_ready());

    let built = factory::build(&config).await.context("factory::build")?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Separate signal for retiring the HTTP listener. It fires only
    // after the drain window (see the bridge task below) so the health
    // probes keep answering during shutdown instead of the listener
    // tearing down the instant SIGTERM lands.
    let (http_shutdown_tx, http_shutdown_rx) = watch::channel(false);

    let http_handle = tokio::spawn(http::serve(
        config.server.listen.clone(),
        prom_handle,
        probes.clone(),
        http_shutdown_rx,
    ));

    let maintenance_handle = tokio::spawn(maintenance::run(
        built.frontier.clone(),
        built.politeness.clone(),
        built.metadata.clone(),
        shutdown_rx.clone(),
    ));

    let signal_handle = tokio::spawn(shutdown::wait_for_signal(shutdown_tx));

    // Worker pool - runs until the shared CrawlerBuilder shutdown
    // flag flips. We bridge our shutdown_rx to that flag below.
    let crawler = Arc::new(built.crawler);
    let probes_for_shutdown = probes.clone();
    let crawler_for_signal = crawler.clone();
    let mut shutdown_rx_for_bridge = shutdown_rx.clone();
    let bridge_handle = tokio::spawn(async move {
        let _ = shutdown_rx_for_bridge.changed().await;
        // Mark not-ready *before* draining: lets scrapers and load
        // balancers observe /readyz = 503 and stop hitting this pod
        // while in-flight URLs finish. The HTTP listener deliberately
        // stays up through the drain window so those probes still get
        // an answer.
        probes_for_shutdown.mark_not_ready();
        // 5s drain delay before signaling the worker pool to stop.
        // Lets in-flight scrapes / probe checks land.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        crawler_for_signal.shutdown();
        let _ = http_shutdown_tx.send(true);
    });

    info!("worker pool starting");
    crawler.run().await.context("Crawler::run")?;

    // Joinset cleanup. We don't propagate panics here; the worker pool
    // has already returned by this point. Best-effort awaits.
    let _ = http_handle.await;
    let _ = maintenance_handle.await;
    let _ = signal_handle.await;
    let _ = bridge_handle.await;

    info!("crawlrs exited cleanly");
    Ok(())
}

/// Attach Prometheus help text + units to every metric in the
/// metric contract. Idempotent; safe to call multiple times.
fn install_metrics_descriptions() {
    crawlrs_runtime::metrics::register();
    crawlrs_frontier::metrics::register();
    crawlrs_politeness::metrics::register();
    crawlrs_fetch::metrics::register();
    crawlrs_parse::metrics::register();
    crawlrs_metadata::metrics::register();
    crawlrs_store::metrics::register();
}
