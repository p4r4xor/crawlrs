//! `crawlrs crawl` orchestration. Wires config, factory, HTTP host,
//! maintenance loop, shutdown signal, and the runtime worker pool
//! into a single tokio process.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use crawlrs_core::{CanonicalUrl, Frontier, UrlEntry};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::cli::CrawlArgs;
use crate::config::CrawlrsConfig;
use crate::factory;
use crate::http::{self, ProbeState};
use crate::maintenance;
use crate::shutdown;

pub async fn crawl(args: CrawlArgs) -> Result<()> {
    let config = CrawlrsConfig::load(&args.config)
        .with_context(|| format!("loading config {}", args.config.display()))?;
    info!(summary = %config.summary(), "config loaded");

    install_metrics_descriptions();

    // Install the Prometheus recorder *before* any subsystem emits.
    // Otherwise early emissions (during construction) are lost.
    let prom_handle = PrometheusBuilder::new()
        .install_recorder()
        .context("installing PrometheusBuilder recorder")?;
    let prom_handle = Arc::new(prom_handle);

    let probes = Arc::new(ProbeState::new_ready());

    let built = factory::build(&config).await.context("factory::build")?;

    if let Some(seeds_path) = &args.seeds {
        load_seeds(seeds_path, built.frontier.as_ref()).await?;
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // HTTP host (axum) - separate task. Stops cleanly on shutdown.
    let http_handle = tokio::spawn(http::serve(
        config.server.listen.clone(),
        prom_handle,
        probes.clone(),
        shutdown_rx.clone(),
    ));

    // Maintenance loop - periodic gauge refresh.
    let maintenance_handle = tokio::spawn(maintenance::run(
        built.frontier.clone(),
        built.politeness.clone(),
        built.metadata.clone(),
        shutdown_rx.clone(),
    ));

    // Signal handler - flips the shutdown watch on SIGTERM/SIGINT.
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
        // balancers stop hitting this pod while in-flight URLs finish.
        probes_for_shutdown.mark_not_ready();
        // 5s drain delay before signaling the worker pool to stop.
        // Lets in-flight scrapes / probe checks land.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        crawler_for_signal.shutdown();
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

/// Read a seeds file (one URL per line; blank and `#`-prefixed lines
/// ignored) and submit all URLs to the frontier in one batch.
async fn load_seeds(path: &Path, frontier: &dyn Frontier) -> Result<()> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("reading seeds file {}", path.display()))?;
    let mut entries = Vec::new();
    for (line_no, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match CanonicalUrl::parse(line) {
            Ok(url) => entries.push(UrlEntry::seed(url)),
            Err(e) => warn!(
                line = line_no + 1,
                value = line,
                error = %e,
                "skipping malformed seed URL"
            ),
        }
    }
    if entries.is_empty() {
        info!(path = %path.display(), "seeds file is empty; nothing to submit");
        return Ok(());
    }
    let count = entries.len();
    let submitted = frontier
        .submit_batch(entries)
        .await
        .context("frontier.submit_batch (seeds)")?;
    info!(
        path = %path.display(),
        seeds = count,
        newly_inserted = submitted,
        "seeds submitted"
    );
    Ok(())
}

/// Attach Prometheus help text + units to every metric in the
/// 29-metric contract. Idempotent; safe to call multiple times.
fn install_metrics_descriptions() {
    crawlrs_runtime::metrics::register();
    crawlrs_frontier::metrics::register();
    crawlrs_politeness::metrics::register();
    crawlrs_fetch::metrics::register();
    crawlrs_parse::metrics::register();
    crawlrs_metadata::metrics::register();
    crawlrs_store::metrics::register();
}
