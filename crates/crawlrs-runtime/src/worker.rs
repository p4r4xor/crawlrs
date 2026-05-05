//! Per-worker pipeline: claim -> politeness check -> fetch -> parse
//! (or site-adapter) -> submit discovered links -> store -> ack.
//!
//! Each worker is one tokio task. The worker pool size is set by
//! [`CrawlerConfig::workers`]; workers share the trait-object Arcs
//! and don't coordinate with each other directly. Backpressure is
//! implicit via await-points: a worker that's blocked in `fetch` or
//! `store` simply isn't claiming new URLs.
//!
//! The per-URL pipeline is encapsulated in [`UrlPipeline`]: a struct
//! that owns one URL's working set (deps + entry) and exposes one
//! short async method per phase. The top-level [`UrlPipeline::run`]
//! reads as a checklist; each phase fits well under the 60-line
//! readability budget.

use std::sync::Arc;

use crawlrs_core::{
    CanonicalUrl, FailureKind, FetchRequest, FetchResponse, Fetcher, Frontier, MetadataStore,
    ParsedDocument, Parser, PoliteDecision, Politeness, ShardingPolicy, SiteAdapterRegistry, Store,
    StoreRecord, UrlEntry, UrlStatus, content_hash,
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
    /// Sharding policy used to derive a URL's shard at storage time.
    /// The blob store's path layout includes the shard component for
    /// Hive-partitioned downstream pruning. Must agree with whatever
    /// the frontier impl is using internally; the runtime defaults to
    /// `HostHashShardPolicy::new(8)` unless the operator overrides
    /// via the builder.
    pub sharding_policy: Arc<dyn ShardingPolicy>,
}

/// Drive one worker until shutdown. Loops:
///   1. Sleep until the soonest host-wake-time the politeness layer
///      knows about, or a brief poll interval if no hosts are tracked.
///   2. Claim a URL. If the queue is empty, sleep poll interval.
///   3. Process the URL via [`UrlPipeline`].
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

/// Trace boundary for one URL's pipeline. Wraps [`UrlPipeline::run`]
/// so the worker_id is in scope for every span the pipeline emits,
/// and so the per-URL metric envelope (workers_active gauge +
/// pipeline_seconds histogram) lives in one place.
#[tracing::instrument(skip(deps), fields(worker_id, url = %entry.url, depth = entry.depth))]
async fn process_url(_worker_id: usize, deps: &Arc<WorkerDeps>, entry: UrlEntry) {
    let started_at = tokio::time::Instant::now();
    metrics::gauge!(crate::metrics::WORKERS_ACTIVE).increment(1.0);
    UrlPipeline::new(Arc::clone(deps), entry).run().await;
    metrics::gauge!(crate::metrics::WORKERS_ACTIVE).decrement(1.0);
    metrics::histogram!(crate::metrics::PIPELINE_SECONDS)
        .record(started_at.elapsed().as_secs_f64());
}

// ---------------------------------------------------------------------------
// Per-URL pipeline
// ---------------------------------------------------------------------------

/// One URL's worth of work, encapsulated as a struct.
///
/// Owns the dependencies (cheap `Arc` clone) and the entry being
/// processed for the lifetime of one `run()` call. Each phase is its
/// own short async method; [`Self::run`] is the orchestration.
///
/// The contract for each phase method is uniform: perform the phase,
/// and *if it terminally handled the URL* (acked or nacked the
/// frontier, wrote any required metadata transition), signal that to
/// the caller via the return type so `run` can short-circuit. None of
/// the phase methods bubble errors; politeness/metadata/frontier
/// errors are warn-and-move-on so one failed RPC doesn't poison the
/// whole pipeline.
struct UrlPipeline {
    deps: Arc<WorkerDeps>,
    entry: UrlEntry,
}

impl UrlPipeline {
    fn new(deps: Arc<WorkerDeps>, entry: UrlEntry) -> Self {
        Self { deps, entry }
    }

    fn url(&self) -> &CanonicalUrl {
        &self.entry.url
    }

    /// Top-level orchestration. Reads as a checklist: each step
    /// either proceeds or short-circuits the run. The methods below
    /// follow a uniform contract - they perform their phase, and if
    /// they terminally handle the URL (acking/nacking the frontier
    /// and writing any metadata transition), they signal that to
    /// `run` via the return type so we exit early.
    async fn run(self) {
        if self.is_already_done().await {
            return;
        }
        if !self.politeness_allows().await {
            return;
        }
        self.mark_attempting().await;
        let Some(resp) = self.fetch().await else {
            return;
        };
        let Some(doc) = self.extract(&resp).await else {
            return;
        };
        self.submit_discovered(&doc).await;
        self.finalize(&resp, &doc).await;
    }

    /// Cross-run dedup. Returns `true` iff a prior run already
    /// terminally handled this URL (Succeeded or in DLQ); the caller
    /// (in this case `run()`) treats `true` as "ack and skip." Costs
    /// one metadata `get` per claim; opt out via
    /// `CrawlerConfig::cross_run_dedup`.
    async fn is_already_done(&self) -> bool {
        if !self.deps.config.cross_run_dedup {
            return false;
        }
        let prior = match self.deps.metadata.get(self.url()).await {
            Ok(Some(p)) => p,
            Ok(None) | Err(_) => return false,
        };
        match prior.status {
            UrlStatus::Succeeded => {
                debug!(url = %self.url(), "cross-run dedup hit; acking without fetch");
                metrics::counter!(
                    crate::metrics::URLS_SKIPPED_TOTAL,
                    "reason" => crate::metrics::SKIP_ALREADY_SUCCEEDED,
                )
                .increment(1);
                let _ = self.deps.frontier.ack(self.url()).await;
                true
            }
            UrlStatus::PermanentlyFailed => {
                debug!(url = %self.url(), "URL is in DLQ; acking without fetch");
                metrics::counter!(
                    crate::metrics::URLS_SKIPPED_TOTAL,
                    "reason" => crate::metrics::SKIP_ALREADY_DLQ,
                )
                .increment(1);
                let _ = self.deps.frontier.ack(self.url()).await;
                true
            }
            _ => false,
        }
    }

    /// Returns `true` iff politeness allows the fetch. `false` means
    /// the pipeline is already finalized (acked on Disallow, nacked on
    /// Delay or check-error). No metadata write on Disallow: the
    /// verdict is per-run policy, not a URL-level failure.
    async fn politeness_allows(&self) -> bool {
        match self.deps.politeness.check(self.url()).await {
            Ok(PoliteDecision::Allow) => true,
            Ok(PoliteDecision::Disallow) => {
                debug!(url = %self.url(), "politeness disallowed; acking");
                metrics::counter!(
                    crate::metrics::URLS_SKIPPED_TOTAL,
                    "reason" => crate::metrics::SKIP_POLITENESS_DISALLOWED,
                )
                .increment(1);
                let _ = self.deps.frontier.ack(self.url()).await;
                false
            }
            Ok(PoliteDecision::Delay(d)) => {
                debug!(url = %self.url(), delay_ms = d.as_millis() as u64, "politeness delay; nacking");
                let _ = self.deps.frontier.nack(self.url()).await;
                false
            }
            Err(e) => {
                warn!(url = %self.url(), error = %e, "politeness.check failed");
                let _ = self.deps.frontier.nack(self.url()).await;
                false
            }
        }
    }

    /// Best-effort: stamp the metadata ledger before any fetch I/O.
    /// If the worker dies before ack/nack the row is left InProgress
    /// and `XAUTOCLAIM` hands the URL to a peer who'll redo this
    /// transition.
    async fn mark_attempting(&self) {
        let result = self
            .deps
            .metadata
            .mark_attempting(self.url(), &self.deps.run_id, self.entry.depth)
            .await;
        if let Err(e) = result {
            warn!(url = %self.url(), error = %e, "metadata.mark_attempting failed; continuing");
        }
    }

    /// Fetch + classification + politeness recording. Returns
    /// `Some(resp)` only on a clean status; `None` means the failure
    /// path already finalized the URL (handled retry budget + ack/nack).
    async fn fetch(&self) -> Option<FetchResponse> {
        let mut req = FetchRequest::new(self.url().clone());
        req.headers
            .insert("User-Agent".into(), self.deps.config.user_agent.clone());

        let resp = match self.deps.fetcher.fetch(req).await {
            Ok(r) => r,
            Err(e) => {
                let kind = classify_transport_error(&e);
                warn!(url = %self.url(), error = %e, kind = ?kind, "fetch transport error");
                let _ = self
                    .deps
                    .politeness
                    .record_failure(self.url(), kind, None)
                    .await;
                self.handle_failure(kind, &format!("transport: {e}")).await;
                return None;
            }
        };

        if let Some(kind) = classify_status(resp.status) {
            let retry_after = extract_retry_after(&resp.headers);
            debug!(
                url = %self.url(),
                status = resp.status,
                kind = ?kind,
                retry_after_ms = retry_after.map(|d| d.as_millis() as u64).unwrap_or(0),
                "fetch http failure",
            );
            let _ = self
                .deps
                .politeness
                .record_failure(self.url(), kind, retry_after)
                .await;
            self.handle_failure(kind, &format!("http {}", resp.status))
                .await;
            return None;
        }

        let _ = self.deps.politeness.record_fetch(self.url()).await;
        Some(resp)
    }

    /// Site-adapter first, generic parser fallback. Returns `None` if
    /// the bytes were unusable; in that case we mark the URL
    /// permanently failed (re-trying won't help; bad parse is a
    /// content-side problem) and ack.
    async fn extract(&self, resp: &FetchResponse) -> Option<ParsedDocument> {
        if let Some(adapter) = self.deps.adapters.find_for(&resp.url) {
            match adapter.extract(resp).await {
                Ok(Some(doc)) => return Some(doc),
                Ok(None) => {} // adapter punted; fall through to generic parser
                Err(e) => {
                    warn!(url = %resp.url, error = %e, "site adapter extract failed");
                    self.fail_parse(&format!("adapter: {e}")).await;
                    return None;
                }
            }
        }
        match self.deps.parser.parse(resp).await {
            Ok(doc) => Some(doc),
            Err(e) => {
                warn!(url = %resp.url, error = %e, "generic parse failed");
                self.fail_parse(&format!("parser: {e}")).await;
                None
            }
        }
    }

    /// Mark permanently failed for a parse-side failure and ack. Helper
    /// for the two parse paths (site adapter + generic).
    async fn fail_parse(&self, reason: &str) {
        let dlq_reason = format!("parse: {reason}");
        let _ = self
            .deps
            .metadata
            .mark_permanently_failed(self.url(), &dlq_reason)
            .await;
        let _ = self.deps.frontier.ack(self.url()).await;
    }

    /// Filter outbound links by scheme + max-depth, then submit the
    /// batch to the frontier. Errors are logged; submit failure
    /// doesn't abort the URL we're processing.
    async fn submit_discovered(&self, doc: &ParsedDocument) {
        let max_depth = self.deps.config.max_depth;
        let new_depth = self.entry.depth + 1;

        // Count depth-limit drops: HTTP-shape outlinks that were
        // filtered out because their depth exceeds the configured cap.
        // Non-HTTP links (mailto:, tel:, javascript:) are silently
        // dropped without a metric since they're scheme-mismatch, not
        // a crawler decision worth surfacing.
        if max_depth.is_some_and(|limit| new_depth > limit) {
            let dropped = doc.outbound_links.iter().filter(|u| u.is_http()).count();
            if dropped > 0 {
                metrics::counter!(
                    crate::metrics::URLS_SKIPPED_TOTAL,
                    "reason" => crate::metrics::SKIP_DEPTH_LIMIT,
                )
                .increment(dropped as u64);
            }
        }

        let candidates: Vec<UrlEntry> = doc
            .outbound_links
            .iter()
            .filter(|u| u.is_http())
            .filter(|_| max_depth.is_none_or(|limit| new_depth <= limit))
            .map(|u| UrlEntry {
                url: u.clone(),
                depth: new_depth,
                discovered_from: Some(self.entry.url.clone()),
            })
            .collect();

        if candidates.is_empty() {
            return;
        }

        let _ = self
            .deps
            .frontier
            .submit_batch(candidates)
            .await
            .map_err(|e| warn!(parent = %self.url(), error = %e, "submit_batch failed"));
    }

    /// Persist the body via the store, record the blob path + content
    /// hash on the metadata ledger, then ack the frontier. Each step's
    /// failure path acks anyway to avoid hot-looping; the URL stays
    /// `InProgress` in the ledger so a future run can pick it up.
    async fn finalize(&self, resp: &FetchResponse, doc: &ParsedDocument) {
        let body_hash = content_hash(&resp.body);
        let record = StoreRecord {
            doc,
            resp,
            run_id: &self.deps.run_id,
            shard: self.deps.sharding_policy.shard_key(self.url()),
            depth: self.entry.depth,
            content_hash: body_hash,
        };
        let blob_path = match self.deps.store.write(&record).await {
            Ok(p) => p,
            Err(e) => {
                warn!(url = %self.url(), error = %e, "store write failed; acking anyway to avoid hot loop");
                let _ = self.deps.frontier.ack(self.url()).await;
                return;
            }
        };

        if let Err(e) = self
            .deps
            .metadata
            .mark_succeeded(self.url(), &blob_path, body_hash)
            .await
        {
            warn!(url = %self.url(), error = %e, "metadata.mark_succeeded failed; acking anyway");
        }

        if let Err(e) = self.deps.frontier.ack(self.url()).await {
            warn!(url = %self.url(), error = %e, "frontier ack failed");
        }

        metrics::counter!(crate::metrics::URLS_FETCHED_TOTAL).increment(1);
    }

    /// Common path for transport + HTTP-status failures: increment
    /// retry count via the metadata ledger; if the budget is
    /// exhausted, move to DLQ + ack so the URL stops cycling.
    /// Otherwise nack and let `XAUTOCLAIM` re-deliver later.
    async fn handle_failure(&self, kind: FailureKind, reason: &str) {
        metrics::counter!(
            crate::metrics::URLS_FAILED_TOTAL,
            "kind" => crate::metrics::failure_kind_label(kind),
        )
        .increment(1);
        let new_count = match self.deps.metadata.mark_failed(self.url(), kind).await {
            Ok(c) => c,
            Err(e) => {
                warn!(url = %self.url(), error = %e, "metadata.mark_failed failed; nacking conservatively");
                let _ = self.deps.frontier.nack(self.url()).await;
                return;
            }
        };

        if new_count >= self.deps.config.max_retries {
            debug!(url = %self.url(), retry_count = new_count, "retry budget exhausted; DLQ");
            let dlq_reason = format!("retries exceeded ({new_count}): {reason}");
            let _ = self
                .deps
                .metadata
                .mark_permanently_failed(self.url(), &dlq_reason)
                .await;
            let _ = self.deps.frontier.ack(self.url()).await;
        } else {
            debug!(url = %self.url(), retry_count = new_count, "retry budget remaining; nacking");
            let _ = self.deps.frontier.nack(self.url()).await;
        }
    }
}

// Inline because: this is a *locality guard*, not a visibility-forced
// test. Its job is to remind whoever edits `WorkerDeps` (a few
// hundred lines above) to update the array; that reminder only
// works if the test sits in the same file as the struct. Moving it
// to `tests/` would make the array a number floating in space, no
// longer tied to the thing it guards.
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
            "sharding_policy",
        ];
        assert_eq!(names.len(), 10);
    }
}
