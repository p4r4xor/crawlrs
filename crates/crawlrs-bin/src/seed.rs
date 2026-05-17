//! `crawlrs seed` orchestration. One-shot bootstrap that loads URLs
//! from a file into the Frontier and exits.
//!
//! Runs from a Helm post-install Job so pod restarts never re-seed.
//! Tolerant of per-batch failures: a Redis OOM or transient error
//! during one batch logs a warning and the loader continues with the
//! next batch. The process exits non-zero only if *no* batch
//! succeeded; otherwise it summarises what was loaded vs dropped.

use std::path::Path;

use anyhow::{Context, Result};
use crawlrs_core::{CanonicalUrl, Frontier, UrlEntry};
use tracing::{info, warn};

use crate::cli::SeedArgs;
use crate::config::CrawlrsConfig;
use crate::factory;

pub async fn seed(args: SeedArgs) -> Result<()> {
    let config = CrawlrsConfig::load(&args.config)
        .with_context(|| format!("loading config {}", args.config.display()))?;
    info!(summary = %config.summary(), "config loaded");

    let frontier = factory::build_frontier(&config)
        .await
        .context("factory::build_frontier")?;

    let entries = read_seeds(&args.path).await?;
    if entries.is_empty() {
        info!(path = %args.path.display(), "seeds file is empty; nothing to do");
        return Ok(());
    }

    submit_in_batches(frontier.as_ref(), entries, args.batch_size, &args.path).await
}

/// Read a seeds file. One URL per line; blank and `#`-prefixed lines
/// ignored. Lines that fail `CanonicalUrl::parse` are logged and
/// skipped (seed quality is a pre-flight concern, not a runtime fault).
async fn read_seeds(path: &Path) -> Result<Vec<UrlEntry>> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("reading seeds file {}", path.display()))?;
    let mut entries = Vec::new();
    let mut skipped = 0u64;
    for (line_no, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match CanonicalUrl::parse(line) {
            Ok(url) => entries.push(UrlEntry::seed(url)),
            Err(e) => {
                skipped += 1;
                warn!(
                    line = line_no + 1,
                    value = line,
                    error = %e,
                    "skipping malformed seed URL"
                );
            }
        }
    }
    info!(valid = entries.len(), skipped, "seeds file parsed");
    Ok(entries)
}

/// Submit `entries` to the Frontier in chunks of `batch_size`. Logs a
/// warning per failed batch and continues; returns Ok if any batch
/// succeeded, Err only if every batch failed.
async fn submit_in_batches(
    frontier: &dyn Frontier,
    entries: Vec<UrlEntry>,
    batch_size: usize,
    seeds_path: &Path,
) -> Result<()> {
    let total = entries.len();
    let mut newly_inserted: u64 = 0;
    let mut rejected_quota: u64 = 0;
    let mut failed: u64 = 0;
    let mut batches_attempted: u64 = 0;
    let mut batches_succeeded: u64 = 0;

    for (idx, chunk) in entries.chunks(batch_size).enumerate() {
        batches_attempted += 1;
        let batch_num = idx + 1;
        match frontier.submit_batch(chunk.to_vec()).await {
            Ok(outcome) => {
                batches_succeeded += 1;
                newly_inserted += outcome.newly as u64;
                rejected_quota += outcome.rejected_quota as u64;
                info!(
                    batch = batch_num,
                    size = chunk.len(),
                    newly = outcome.newly,
                    rejected_quota = outcome.rejected_quota,
                    "batch submitted"
                );
            }
            Err(e) => {
                failed += chunk.len() as u64;
                warn!(
                    batch = batch_num,
                    size = chunk.len(),
                    error = %e,
                    "batch failed; continuing with next batch"
                );
            }
        }
    }

    let bloom_duplicates = (total as u64).saturating_sub(newly_inserted + rejected_quota + failed);
    info!(
        path = %seeds_path.display(),
        total,
        batches_attempted,
        batches_succeeded,
        newly_inserted,
        rejected_quota,
        bloom_duplicates,
        failed,
        "seed bootstrap complete"
    );

    if batches_succeeded == 0 {
        anyhow::bail!(
            "all {} seed batch(es) failed; nothing was loaded",
            batches_attempted
        );
    }
    Ok(())
}
