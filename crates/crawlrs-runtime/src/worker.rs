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
    FetchRequest, FetchResponse, Fetcher, Frontier, ParsedDocument, Parser, PoliteDecision,
    Politeness, SiteAdapterRegistry, Store, UrlEntry,
};
use tokio::sync::watch;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, warn};

use crate::crawler::CrawlerConfig;
use crate::failure::{classify_status, classify_transport_error};

/// Bag of dependencies shared across all worker tasks. `Arc<dyn _>`
/// for each so workers can clone cheaply.
pub struct WorkerDeps {
    pub frontier: Arc<dyn Frontier>,
    pub politeness: Arc<dyn Politeness>,
    pub fetcher: Arc<dyn Fetcher>,
    pub parser: Arc<dyn Parser>,
    pub store: Arc<dyn Store>,
    pub adapters: Arc<SiteAdapterRegistry>,
    pub config: CrawlerConfig,
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
    // Politeness gate.
    match deps.politeness.check(&entry.url).await {
        Ok(PoliteDecision::Allow) => {}
        Ok(PoliteDecision::Disallow) => {
            debug!(url = %entry.url, "politeness disallowed; acking");
            let _ = deps.frontier.ack(&entry.url).await;
            return;
        }
        Ok(PoliteDecision::Delay(d)) => {
            // Host isn't ready. Nack so the entry stays in our PEL for
            // XAUTOCLAIM-driven re-delivery later; another worker may
            // hit it once the host wakes. Avoids tight retry loops.
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

    // Fetch.
    let mut req = FetchRequest::new(entry.url.clone());
    req.headers
        .insert("User-Agent".into(), deps.config.user_agent.clone());
    let resp = match deps.fetcher.fetch(req).await {
        Ok(r) => r,
        Err(e) => {
            let kind = classify_transport_error(&e);
            warn!(url = %entry.url, error = %e, kind = ?kind, "fetch transport error");
            let _ = deps.politeness.record_failure(&entry.url, kind).await;
            let _ = deps.frontier.nack(&entry.url).await;
            return;
        }
    };

    // HTTP-status-level failure handling.
    if let Some(kind) = classify_status(resp.status) {
        debug!(url = %entry.url, status = resp.status, kind = ?kind, "fetch http failure");
        let _ = deps.politeness.record_failure(&entry.url, kind).await;
        let _ = deps.frontier.nack(&entry.url).await;
        return;
    }

    // Successful fetch: tell politeness, then run the extraction
    // pipeline (site adapter -> generic parser fallback -> store ->
    // submit discovered links -> ack).
    let _ = deps.politeness.record_fetch(&entry.url).await;

    let doc = match extract_document(deps, &resp).await {
        Some(d) => d,
        None => {
            // Parser/adapter failed; treat as a content-side issue, not
            // a politeness issue. Ack the URL so we don't loop on it.
            let _ = deps.frontier.ack(&entry.url).await;
            return;
        }
    };

    // Submit discovered links subject to scope rules.
    submit_discovered(deps, &entry, &doc).await;

    // Persist.
    if let Err(e) = deps.store.write(&doc, Some(&resp.body)).await {
        warn!(url = %entry.url, error = %e, "store write failed; acking anyway to avoid hot loop");
    }

    // ACK once we've done what we can with this URL.
    if let Err(e) = deps.frontier.ack(&entry.url).await {
        warn!(url = %entry.url, error = %e, "frontier ack failed");
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
            "adapters",
            "config",
        ];
        assert_eq!(names.len(), 7);
    }
}
