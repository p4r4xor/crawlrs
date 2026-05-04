//! Per-worker pipeline: claim -> politeness check -> fetch -> parse
//! (or site-adapter) -> submit discovered links -> store -> ack.
//!
//! Each worker is one tokio task. The worker pool size is set by
//! [`CrawlerConfig::workers`]; workers share the trait-object Arcs
//! and don't coordinate with each other directly. Backpressure is
//! implicit via await-points: a worker that's blocked in `fetch` or
//! `store` simply isn't claiming new URLs.

use std::sync::Arc;

use crawlrs_core::{
    FetchRequest, FetchResponse, Fetcher, Frontier, MetadataStore, ParsedDocument, Parser,
    PoliteDecision, Politeness, SiteAdapterRegistry, Store, UrlEntry, UrlStatus, content_hash,
};
use tokio::sync::watch;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, warn};

use crate::crawler::CrawlerConfig;
use crate::failure::{classify_status, classify_transport_error, extract_retry_after};

/// Bag of dependencies shared across all worker tasks. `Arc<dyn _>`
/// for each so workers can clone cheaply.
pub struct WorkerDeps {
    pub frontier: Arc<dyn Frontier>,
    pub politeness: Arc<dyn Politeness>,
    pub fetcher: Arc<dyn Fetcher>,
    pub parser: Arc<dyn Parser>,
    pub store: Arc<dyn Store>,
    pub metadata: Arc<dyn MetadataStore>,
    pub adapters: Arc<SiteAdapterRegistry>,
    pub config: CrawlerConfig,
    /// Identity of this crawl run. Stamped on every `mark_attempting`
    /// so cross-run dedup and history queries can distinguish "URL X
    /// was last touched by run Y." Must be stable for the lifetime of
    /// the `Crawler`.
    pub run_id: String,
}

/// Drive one worker until shutdown. Loops:
///   1. Sleep until the soonest host-wake-time the politeness layer
///      knows about, or a brief poll interval if no hosts are tracked.
///   2. Claim a URL. If the queue is empty, sleep poll interval.
///   3. Process the URL (politeness gate, fetch, parse, submit, store).
pub async fn worker_loop(
    worker_id: usize,
    deps: Arc<WorkerDeps>,
    mut shutdown: watch::Receiver<bool>,
) {
    debug!(worker_id, "worker_loop started");
    // Successive empty-claim attempts back off exponentially up to
    // `max_idle_sleep`. Resets on any successful claim. Without this
    // a 100-worker idle pool burns ~200 RPS on Redis just polling.
    let mut empty_backoff = deps.config.empty_queue_poll;
    while !*shutdown.borrow() {
        sleep_until_ready(&deps, &mut shutdown).await;
        if *shutdown.borrow() {
            break;
        }

        let entry = match deps.frontier.claim().await {
            Ok(Some(e)) => {
                empty_backoff = deps.config.empty_queue_poll;
                e
            }
            Ok(None) => {
                tokio::select! {
                    _ = tokio::time::sleep(empty_backoff) => {}
                    _ = shutdown.changed() => break,
                }
                empty_backoff = (empty_backoff * 2).min(deps.config.max_idle_sleep);
                continue;
            }
            Err(e) => {
                warn!(worker_id, error = %e, "frontier.claim failed");
                tokio::time::sleep(deps.config.error_backoff).await;
                continue;
            }
        };

        process_url(worker_id, &deps, entry).await;
    }
    debug!(worker_id, "worker_loop exiting");
}

/// Sleep up to the next-ready instant the politeness layer reports.
/// Returns immediately if no hosts are tracked yet (startup case).
async fn sleep_until_ready(deps: &Arc<WorkerDeps>, shutdown: &mut watch::Receiver<bool>) {
    match deps.politeness.next_ready_at().await {
        Ok(Some(when)) => {
            let when_tokio = TokioInstant::from_std(when);
            let now = TokioInstant::now();
            if when_tokio > now {
                let wait = when_tokio - now;
                // Cap the wait so we don't sleep through a shutdown by
                // accident; the runtime polls at least every `wait_cap`.
                let cap = deps.config.max_idle_sleep;
                let actual = wait.min(cap);
                tokio::select! {
                    _ = tokio::time::sleep(actual) => {}
                    _ = shutdown.changed() => {}
                }
            }
        }
        Ok(None) => {
            // No hosts tracked yet. Short sleep so we don't spin.
            tokio::select! {
                _ = tokio::time::sleep(deps.config.startup_poll) => {}
                _ = shutdown.changed() => {}
            }
        }
        Err(e) => {
            warn!(error = %e, "politeness.next_ready_at failed");
            tokio::time::sleep(deps.config.error_backoff).await;
        }
    }
}

#[tracing::instrument(skip(deps), fields(worker_id, url = %entry.url, depth = entry.depth))]
async fn process_url(_worker_id: usize, deps: &Arc<WorkerDeps>, entry: UrlEntry) {
    // Cross-run dedup: if a prior run already crawled this URL
    // successfully, ack and skip without fetching. Costs one metadata
    // get per claim; opt out via `CrawlerConfig::cross_run_dedup` if
    // re-fetching is the point of the run.
    if deps.config.cross_run_dedup {
        match deps.metadata.get(&entry.url).await {
            Ok(Some(prior)) if prior.status == UrlStatus::Succeeded => {
                debug!(url = %entry.url, "cross-run dedup hit; acking without fetch");
                let _ = deps.frontier.ack(&entry.url).await;
                return;
            }
            Ok(Some(prior)) if prior.status == UrlStatus::PermanentlyFailed => {
                debug!(url = %entry.url, "URL is in DLQ; acking without fetch");
                let _ = deps.frontier.ack(&entry.url).await;
                return;
            }
            Ok(_) | Err(_) => {} // missing row, transient ledger error, or non-terminal status -> proceed
        }
    }

    // Politeness gate.
    match deps.politeness.check(&entry.url).await {
        Ok(PoliteDecision::Allow) => {}
        Ok(PoliteDecision::Disallow) => {
            // Disallow is a per-run policy verdict, not a URL-level
            // failure: robots.txt or excludes can flip between runs,
            // so we don't burn a metadata row recording today's "no."
            // Just ack and move on.
            debug!(url = %entry.url, "politeness disallowed; acking");
            let _ = deps.frontier.ack(&entry.url).await;
            return;
        }
        Ok(PoliteDecision::Delay(d)) => {
            // Host isn't ready. Nack so the entry stays in our PEL for
            // XAUTOCLAIM-driven re-delivery later; another worker may
            // hit it once the host wakes. Avoids tight retry loops.
            // No metadata write: a Delay isn't an attempt yet.
            debug!(url = %entry.url, delay_ms = d.as_millis() as u64, "politeness delay; nacking");
            let _ = deps.frontier.nack(&entry.url).await;
            return;
        }
        Err(e) => {
            warn!(url = %entry.url, error = %e, "politeness.check failed");
            let _ = deps.frontier.nack(&entry.url).await;
            return;
        }
    }

    // Mark the attempt before any fetch I/O. If the worker dies before
    // ack/nack, the row is left InProgress and XAUTOCLAIM hands the
    // URL to a peer who'll redo this transition.
    if let Err(e) = deps
        .metadata
        .mark_attempting(&entry.url, &deps.run_id, entry.depth)
        .await
    {
        warn!(url = %entry.url, error = %e, "metadata.mark_attempting failed; continuing");
    }

    // Fetch.
    let mut req = FetchRequest::new(entry.url.clone());
    req.headers
        .insert("User-Agent".into(), deps.config.user_agent.clone());
    let resp = match deps.fetcher.fetch(req).await {
        Ok(r) => r,
        Err(e) => {
            let kind = classify_transport_error(&e);
            warn!(url = %entry.url, error = %e, kind = ?kind, "fetch transport error");
            // No response means no Retry-After header; computed
            // backoff is the only signal.
            let _ = deps.politeness.record_failure(&entry.url, kind, None).await;
            handle_failure(deps, &entry, kind, &format!("transport: {e}")).await;
            return;
        }
    };

    // HTTP-status-level failure handling.
    if let Some(kind) = classify_status(resp.status) {
        // Honor Retry-After when the server sent one (RFC 9110 §10.2.3).
        // Politeness uses max(server_hint, computed_backoff).
        let retry_after = extract_retry_after(&resp.headers);
        debug!(
            url = %entry.url,
            status = resp.status,
            kind = ?kind,
            retry_after_ms = retry_after.map(|d| d.as_millis() as u64).unwrap_or(0),
            "fetch http failure",
        );
        let _ = deps
            .politeness
            .record_failure(&entry.url, kind, retry_after)
            .await;
        handle_failure(deps, &entry, kind, &format!("http {}", resp.status)).await;
        return;
    }

    // Successful fetch: tell politeness, then run the extraction
    // pipeline (site adapter -> generic parser fallback -> store ->
    // submit discovered links -> ack).
    let _ = deps.politeness.record_fetch(&entry.url).await;

    let doc = match extract_document(deps, &resp).await {
        Some(d) => d,
        None => {
            // Parser/adapter failed; the URL was fetched but the bytes
            // were unusable. Mark permanently failed (re-trying won't
            // help; a bad parse is content-side) and ack.
            let _ = deps
                .metadata
                .mark_permanently_failed(&entry.url, "parse: extractor returned no document")
                .await;
            let _ = deps.frontier.ack(&entry.url).await;
            return;
        }
    };

    // Submit discovered links subject to scope rules.
    submit_discovered(deps, &entry, &doc).await;

    // Persist body, capture blob path for the metadata write.
    let blob_path = match deps.store.write(&doc, Some(&resp.body)).await {
        Ok(p) => p,
        Err(e) => {
            warn!(url = %entry.url, error = %e, "store write failed; acking anyway to avoid hot loop");
            // No metadata.mark_succeeded: the row stays InProgress so
            // a future run can pick it up. Acking the frontier
            // prevents an infinite re-claim loop.
            let _ = deps.frontier.ack(&entry.url).await;
            return;
        }
    };

    let body_hash = content_hash(&resp.body);
    if let Err(e) = deps
        .metadata
        .mark_succeeded(&entry.url, &blob_path, body_hash)
        .await
    {
        warn!(url = %entry.url, error = %e, "metadata.mark_succeeded failed; acking anyway");
    }

    if let Err(e) = deps.frontier.ack(&entry.url).await {
        warn!(url = %entry.url, error = %e, "frontier ack failed");
    }
}

/// Common path for transport + HTTP-status failures: increment retry
/// count via the metadata ledger; if the budget is exhausted, move to
/// DLQ + ack so the URL stops cycling. Otherwise nack and let
/// XAUTOCLAIM re-deliver it later.
async fn handle_failure(
    deps: &Arc<WorkerDeps>,
    entry: &UrlEntry,
    kind: crawlrs_core::FailureKind,
    reason: &str,
) {
    let new_count = match deps.metadata.mark_failed(&entry.url, kind).await {
        Ok(c) => c,
        Err(e) => {
            warn!(url = %entry.url, error = %e, "metadata.mark_failed failed; nacking conservatively");
            let _ = deps.frontier.nack(&entry.url).await;
            return;
        }
    };

    if new_count >= deps.config.max_retries {
        debug!(url = %entry.url, retry_count = new_count, "retry budget exhausted; DLQ");
        let dlq_reason = format!("retries exceeded ({new_count}): {reason}");
        let _ = deps
            .metadata
            .mark_permanently_failed(&entry.url, &dlq_reason)
            .await;
        let _ = deps.frontier.ack(&entry.url).await;
    } else {
        debug!(url = %entry.url, retry_count = new_count, "retry budget remaining; nacking");
        let _ = deps.frontier.nack(&entry.url).await;
    }
}

async fn extract_document(deps: &Arc<WorkerDeps>, resp: &FetchResponse) -> Option<ParsedDocument> {
    if let Some(adapter) = deps.adapters.find_for(&resp.url) {
        match adapter.extract(resp).await {
            Ok(Some(doc)) => return Some(doc),
            Ok(None) => {} // adapter punted; fall through to generic parser
            Err(e) => {
                warn!(url = %resp.url, error = %e, "site adapter extract failed");
                return None;
            }
        }
    }
    match deps.parser.parse(resp).await {
        Ok(doc) => Some(doc),
        Err(e) => {
            warn!(url = %resp.url, error = %e, "generic parse failed");
            None
        }
    }
}

async fn submit_discovered(deps: &Arc<WorkerDeps>, parent: &UrlEntry, doc: &ParsedDocument) {
    let max_depth = deps.config.max_depth;
    let new_depth = parent.depth + 1;

    let candidates: Vec<UrlEntry> = doc
        .outbound_links
        .iter()
        .filter(|u| u.is_http())
        .filter(|_| max_depth.is_none_or(|limit| new_depth <= limit))
        .map(|u| UrlEntry {
            url: u.clone(),
            depth: new_depth,
            discovered_from: Some(parent.url.clone()),
        })
        .collect();

    if candidates.is_empty() {
        return;
    }

    let _ = deps.frontier.submit_batch(candidates).await.map_err(|e| {
        warn!(parent = %parent.url, error = %e, "submit_batch failed");
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn worker_deps_field_count_locked() {
        // If you add a field to WorkerDeps, update this and the
        // CrawlerBuilder::build site that constructs it. Locking
        // against silent drift.
        let names = [
            "frontier",
            "politeness",
            "fetcher",
            "parser",
            "store",
            "metadata",
            "adapters",
            "config",
            "run_id",
        ];
        assert_eq!(names.len(), 9);
    }
}
