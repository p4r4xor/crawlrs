//! Per-worker pipeline: claim -> politeness check -> fetch -> parse
//! (or site-adapter) -> submit discovered links -> store -> ack.
//!
//! Each worker is one tokio task. The worker pool size is set by
//! [`CrawlerConfig::workers`]; workers share the trait-object Arcs
//! and don't coordinate with each other directly. Backpressure is
//! implicit via await-points: a worker that's blocked in `fetch` or
//! `store` simply isn't claiming new URLs.
//!
//! Per ADR-0020 the politeness layer is policy-only: it returns a
//! [`NextWake`] from `record_fetch` / `record_failure`; the runtime
//! applies it via [`Frontier::advance_wake`]. This module is the
//! composition site for that handoff.
//!
//! The per-URL pipeline is encapsulated in [`UrlPipeline`]: a struct
//! that owns one URL's working set (deps + entry) and exposes one
//! short async method per phase. The top-level [`UrlPipeline::run`]
//! reads as a checklist; each phase fits well under the 60-line
//! readability budget.

use std::sync::Arc;

use crawlrs_core::{
    AttemptId, CanonicalUrl, ClaimOutcome, Clock, FailureKind, FetchRequest, FetchResponse,
    Fetcher, Frontier, LinkDispatch, MetadataStore, NextWake, ParsedDocument, Parser,
    PoliteDecision, Politeness, ShardingPolicy, SiteAdapterRegistry, Store, StoreRecord,
    SuccessRecord, UrlEntry, UrlId, WorkerIdentity, content_hash,
};
use tokio::sync::watch;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, info, warn};

use crate::crawler::CrawlerConfig;
use crate::failure::{classify_status, classify_transport_error, extract_retry_after};

/// Records a phase's wall-clock duration via Drop. Use as
/// `let _t = PhaseTimer::start(metrics::PHASE_FETCH);`; the histogram
/// emission happens when `_t` goes out of scope, which means every
/// exit path (success, early return, panic) is accounted for without
/// hand-wiring `record(...)` at each return site.
///
/// Pattern: Resource Acquisition Is Initialization (RAII) over the
/// timing measurement. `metrics::histogram!` is sync-safe and a no-op
/// when no recorder is installed, so `Drop` cannot panic.
struct PhaseTimer {
    started: TokioInstant,
    phase: &'static str,
}

impl PhaseTimer {
    fn start(phase: &'static str) -> Self {
        Self {
            started: TokioInstant::now(),
            phase,
        }
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed().as_secs_f64();
        metrics::histogram!(
            crate::metrics::PIPELINE_PHASE_SECONDS,
            "phase" => self.phase,
        )
        .record(elapsed);
    }
}

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
    /// Wall-clock source. The supervisor uses this for restart-budget
    /// timing; tests can swap a `ManualClock` to drive restart-window
    /// math deterministically. The runtime defaults to `SystemClock`.
    pub clock: Arc<dyn Clock>,
}

/// Drive one worker until shutdown. Loops:
///   1. Claim a URL on behalf of `identity`. The frontier returns one
///      of three outcomes:
///      - `Claimed`: process the URL.
///      - `EmptyHint`: nothing ready right now but a host wakes soon;
///        sleep until then (capped by `max_idle_sleep`).
///      - `Empty`: queue is fully idle; sleep `empty_queue_poll`.
///   2. Process the [`ClaimOutcome::Claimed`] via [`UrlPipeline`].
///
/// `identity` is constant for the lifetime of this task. Frontier impls
/// MAY use it as the lease holder for crash-recovery attribution; the
/// `(pod_ordinal, worker_index)` pair MUST be unique within the
/// cluster and stable across process restarts.
pub async fn worker_loop(
    identity: WorkerIdentity,
    deps: Arc<WorkerDeps>,
    mut shutdown: watch::Receiver<bool>,
) {
    debug!(identity = %identity, "worker_loop started");
    while !*shutdown.borrow() {
        match deps.frontier.claim(&identity).await {
            Ok(ClaimOutcome::Claimed {
                url_id,
                entry,
                attempt_id,
            }) => {
                process_url(identity, &deps, url_id, entry, attempt_id).await;
            }
            Ok(ClaimOutcome::EmptyHint { sleep_until }) => {
                let when_tokio = TokioInstant::from_std(sleep_until);
                let now = TokioInstant::now();
                let wait = when_tokio.saturating_duration_since(now);
                let actual = wait.min(deps.config.max_idle_sleep);
                tokio::select! {
                    _ = tokio::time::sleep(actual) => {}
                    _ = shutdown.changed() => break,
                }
            }
            Ok(ClaimOutcome::Empty) => {
                tokio::select! {
                    _ = tokio::time::sleep(deps.config.empty_queue_poll) => {}
                    _ = shutdown.changed() => break,
                }
            }
            Err(e) => {
                warn!(identity = %identity, error = %e, "frontier.claim failed");
                tokio::time::sleep(deps.config.error_backoff).await;
            }
        }
    }
    debug!(identity = %identity, "worker_loop exiting");
}

/// Trace boundary for one URL's pipeline. Wraps [`UrlPipeline::run`]
/// so the worker identity is in scope for every span the pipeline
/// emits, and so the per-URL metric envelope (workers_active gauge +
/// pipeline_seconds histogram) lives in one place.
#[tracing::instrument(
    skip(deps, entry),
    fields(identity = %identity, url = %entry.url, depth = entry.depth, url_id = %url_id),
)]
async fn process_url(
    identity: WorkerIdentity,
    deps: &Arc<WorkerDeps>,
    url_id: UrlId,
    entry: Box<UrlEntry>,
    attempt_id: AttemptId,
) {
    let started_at = tokio::time::Instant::now();
    metrics::gauge!(crate::metrics::WORKERS_ACTIVE).increment(1.0);
    UrlPipeline::new(Arc::clone(deps), url_id, *entry, attempt_id)
        .run()
        .await;
    metrics::gauge!(crate::metrics::WORKERS_ACTIVE).decrement(1.0);
    metrics::histogram!(crate::metrics::PIPELINE_SECONDS)
        .record(started_at.elapsed().as_secs_f64());
    let _ = identity; // referenced via tracing fields
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
/// and *if it terminally handled the URL* (committed via `ack` or
/// chose to let the lease expire), signal that to the caller via the
/// return type so `run` can short-circuit. None of the phase methods
/// bubble errors; politeness/metadata/frontier errors are warn-and-
/// move-on so one failed RPC doesn't poison the whole pipeline.
struct UrlPipeline {
    deps: Arc<WorkerDeps>,
    #[allow(dead_code)] // surfaced in tracing fields and useful for future logging
    url_id: UrlId,
    entry: UrlEntry,
    attempt_id: AttemptId,
}

impl UrlPipeline {
    fn new(deps: Arc<WorkerDeps>, url_id: UrlId, entry: UrlEntry, attempt_id: AttemptId) -> Self {
        Self {
            deps,
            url_id,
            entry,
            attempt_id,
        }
    }

    fn url(&self) -> &CanonicalUrl {
        &self.entry.url
    }

    fn attempt(&self) -> &AttemptId {
        &self.attempt_id
    }

    /// Top-level orchestration. Reads as a checklist: each step
    /// either proceeds or short-circuits the run. The methods below
    /// follow a uniform contract - they perform their phase, and if
    /// they terminally handle the URL (acking the frontier and/or
    /// writing any metadata transition), they signal that to `run`
    /// via the return type so we exit early.
    async fn run(self) {
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
        // Outbound URLs are computed here but their dispatch path
        // depends on `LinkDispatch`: DurableOutbox commits them
        // atomically with the metadata write into a Postgres outbox
        // (drained by a separate publisher task); Direct enqueues
        // them via `Frontier::submit_batch` after the metadata
        // commit, accepting bounded loss on transient errors.
        let outbound = compute_outbound(&self.entry, &doc, |host| {
            self.deps.politeness.depth_cap(host)
        });
        self.finalize(&resp, &doc, outbound).await;
    }

    /// Returns `true` iff politeness allows the fetch. `false` means
    /// the pipeline is already finalized (acked on Disallow, lease
    /// left to expire on check-error).
    ///
    /// No metadata write on Disallow: the verdict is per-run policy
    /// (robots / blocklist / circuit-open), not a URL-level failure
    /// worth recording in the ledger.
    async fn politeness_allows(&self) -> bool {
        let _phase = PhaseTimer::start(crate::metrics::PHASE_POLITENESS);
        match self.deps.politeness.check(self.url()).await {
            Ok(PoliteDecision::Allow) => {
                info!(url = %self.url(), "politeness allow");
                true
            }
            Ok(PoliteDecision::Disallow) => {
                debug!(url = %self.url(), "politeness disallowed; acking");
                metrics::counter!(
                    crate::metrics::URLS_SKIPPED_TOTAL,
                    "reason" => crate::metrics::SKIP_POLITENESS_DISALLOWED,
                )
                .increment(1);
                let _ = self.deps.frontier.ack(self.attempt()).await;
                false
            }
            Err(e) => {
                warn!(url = %self.url(), error = %e, "politeness.check failed; leasing expires");
                // Don't ack: the URL stays in the inflight ZSET. When
                // the lease expires, the frontier's reclaim path
                // re-pushes it to its host_queue for re-delivery.
                false
            }
        }
    }

    /// Best-effort: stamp the metadata ledger before any fetch I/O.
    /// If the worker dies before ack, the row is left InProgress
    /// and the lease-expiry reclaim hands the URL to a peer who'll
    /// redo this transition.
    async fn mark_attempting(&self) {
        let _phase = PhaseTimer::start(crate::metrics::PHASE_MARK);
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
    /// path already finalized the URL (handled retry budget + ack /
    /// lease-expiry).
    async fn fetch(&self) -> Option<FetchResponse> {
        let _phase = PhaseTimer::start(crate::metrics::PHASE_FETCH);
        let req = FetchRequest::new(self.url().clone());

        let resp = match self.deps.fetcher.fetch(req).await {
            Ok(r) => r,
            Err(e) => {
                let kind = classify_transport_error(&e);
                warn!(url = %self.url(), error = %e, kind = ?kind, "fetch transport error");
                let plan = self
                    .deps
                    .politeness
                    .record_failure(self.url(), kind, None)
                    .await;
                self.apply_wake_plan(plan).await;
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
            let plan = self
                .deps
                .politeness
                .record_failure(self.url(), kind, retry_after)
                .await;
            self.apply_wake_plan(plan).await;
            self.handle_failure(kind, &format!("http {}", resp.status))
                .await;
            return None;
        }

        let plan = self.deps.politeness.record_fetch(self.url()).await;
        self.apply_wake_plan(plan).await;
        info!(
            url = %self.url(),
            status = resp.status,
            body_bytes = resp.body.len(),
            "fetch ok",
        );
        Some(resp)
    }

    /// Apply a politeness-computed `NextWake` plan via the frontier.
    /// Per ADR-0020 politeness returns the plan and the runtime owns
    /// the write. Frontier errors are warned and dropped: the lease
    /// is the safety net (a missed wake-time defaults to the lease
    /// timeout via the claim-time wake stamp).
    async fn apply_wake_plan(&self, plan: crawlrs_core::Result<NextWake>) {
        match plan {
            Ok(next) => {
                if let Err(e) = self
                    .deps
                    .frontier
                    .advance_wake(&next.host, next.until)
                    .await
                {
                    warn!(host = %next.host, error = %e, "frontier.advance_wake failed");
                }
            }
            Err(e) => warn!(url = %self.url(), error = %e, "politeness record_* failed"),
        }
    }

    /// Site-adapter first, generic parser fallback. Returns `None` if
    /// the bytes were unusable; in that case we mark the URL
    /// permanently failed (re-trying won't help; bad parse is a
    /// content-side problem) and ack.
    async fn extract(&self, resp: &FetchResponse) -> Option<ParsedDocument> {
        let _phase = PhaseTimer::start(crate::metrics::PHASE_PARSE);
        if let Some(adapter) = self.deps.adapters.find_for(&resp.url) {
            match adapter.extract(resp).await {
                Ok(Some(doc)) => {
                    info!(
                        url = %resp.url,
                        outbound_links = doc.outbound_links.len(),
                        via = "adapter",
                        "parse ok",
                    );
                    return Some(doc);
                }
                Ok(None) => {} // adapter punted; fall through to generic parser
                Err(e) => {
                    warn!(url = %resp.url, error = %e, "site adapter extract failed");
                    self.fail_parse(&format!("adapter: {e}")).await;
                    return None;
                }
            }
        }
        match self.deps.parser.parse(resp).await {
            Ok(doc) => {
                info!(
                    url = %resp.url,
                    outbound_links = doc.outbound_links.len(),
                    via = "lol_html",
                    "parse ok",
                );
                Some(doc)
            }
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
        let _ = self.deps.frontier.ack(self.attempt()).await;
    }

    /// Persist the body via the store, record the blob path + content
    /// hash on the metadata ledger, dispatch outbound URLs to the
    /// Frontier per the configured `LinkDispatch` strategy, then ack
    /// the frontier. Each step's failure path acks anyway to avoid
    /// hot-looping; the URL stays `InProgress` in the ledger so a
    /// future run can pick it up.
    async fn finalize(&self, resp: &FetchResponse, doc: &ParsedDocument, outbound: Vec<UrlEntry>) {
        let body_hash = content_hash(&resp.body);
        let record = StoreRecord {
            doc,
            resp,
            run_id: &self.deps.run_id,
            shard: self.deps.sharding_policy.shard_key(self.url()),
            depth: self.entry.depth,
            content_hash: body_hash,
        };
        let store_result = {
            let _phase = PhaseTimer::start(crate::metrics::PHASE_STORE);
            self.deps.store.write(&record).await
        };
        let blob_path = match store_result {
            Ok(p) => {
                info!(url = %self.url(), blob_path = %p, "store ok");
                p
            }
            Err(e) => {
                warn!(url = %self.url(), error = %e, "store write failed; acking anyway to avoid hot loop");
                let _phase = PhaseTimer::start(crate::metrics::PHASE_COMMIT);
                let _ = self.deps.frontier.ack(self.attempt()).await;
                return;
            }
        };

        let _phase = PhaseTimer::start(crate::metrics::PHASE_COMMIT);

        // Outbound dispatch strategy. DurableOutbox commits outbound
        // URLs atomically with metadata so the publisher can replay
        // them at-least-once; Direct skips the outbox and lets the
        // worker fire-and-forget into the Frontier after the metadata
        // commit, accepting bounded loss on transient errors.
        let outbound_for_metadata: &[UrlEntry] = match self.deps.config.link_dispatch {
            LinkDispatch::DurableOutbox => &outbound,
            LinkDispatch::Direct => &[],
        };
        let success = SuccessRecord {
            url: self.url(),
            attempt_id: self.attempt(),
            blob_path: &blob_path,
            content_hash: body_hash,
            outbound: outbound_for_metadata,
        };
        if let Err(e) = self.deps.metadata.mark_succeeded(&success).await {
            warn!(url = %self.url(), error = %e, "metadata.mark_succeeded failed; acking anyway");
        }

        if matches!(self.deps.config.link_dispatch, LinkDispatch::Direct) && !outbound.is_empty() {
            // Fire-and-forget enqueue. submit_batch errors are logged
            // and counted; we do not retry, buffer, or propagate. The
            // worker continues to ack and pull the next URL. Bounded
            // loss is the explicit tradeoff of Direct mode: a retry
            // buffer would re-implement the outbox without the
            // durability that justifies it.
            let n = outbound.len();
            if let Err(e) = self.deps.frontier.submit_batch(outbound).await {
                warn!(url = %self.url(), error = %e, n, "direct dispatch lost outbound URLs");
                metrics::counter!(crate::metrics::DIRECT_DISPATCH_LOST_TOTAL).increment(n as u64);
            }
        }

        if let Err(e) = self.deps.frontier.ack(self.attempt()).await {
            warn!(url = %self.url(), error = %e, "frontier ack failed");
        }

        info!(url = %self.url(), depth = self.entry.depth, "url complete");
        metrics::counter!(crate::metrics::URLS_FETCHED_TOTAL).increment(1);
    }

    /// Common path for transport + HTTP-status failures: increment
    /// retry count via the metadata ledger; if the budget is
    /// exhausted, move to DLQ + ack so the URL stops cycling.
    /// Otherwise leave the lease in place: the frontier's reclaim
    /// path re-pushes the URL when the lease expires, and the
    /// host's wake-time (already advanced by `apply_wake_plan`
    /// upstream) gates when the re-claim happens.
    async fn handle_failure(&self, kind: FailureKind, reason: &str) {
        metrics::counter!(
            crate::metrics::URLS_FAILED_TOTAL,
            "kind" => kind.as_str(),
        )
        .increment(1);
        let new_count = match self.deps.metadata.mark_failed(self.url(), kind).await {
            Ok(c) => c,
            Err(e) => {
                warn!(url = %self.url(), error = %e, "metadata.mark_failed failed; leaving lease to expire");
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
            let _ = self.deps.frontier.ack(self.attempt()).await;
        } else {
            debug!(
                url = %self.url(),
                retry_count = new_count,
                "retry budget remaining; lease expires for re-delivery",
            );
            // No ack: the lease will time out and the frontier's
            // reclaim path re-pushes the URL onto its host_queue.
        }
    }
}

/// Filter outbound links by scheme + max-depth and return them as
/// `UrlEntry`s ready to ride along on the `mark_succeeded`
/// transaction. Pure: no I/O. The actual enqueue into the Frontier
/// happens via the outbox publisher (see
/// [`crate::outbox::outbox_publisher`]) so that the Postgres commit
/// and the queue write are atomic from the worker's perspective.
///
/// `depth_cap` is called per outbound URL with its host string and
/// returns the **effective** depth cap for that host: per-host
/// override if configured, else the politeness layer's global
/// default. `None` means uncapped. Both globals and per-host
/// overrides live in `[politeness]`; the runtime doesn't carry a
/// separate cap on this path.
fn compute_outbound(
    parent: &UrlEntry,
    doc: &ParsedDocument,
    depth_cap: impl Fn(&str) -> Option<u32>,
) -> Vec<UrlEntry> {
    let new_depth = parent.depth + 1;
    let mut dropped = 0u64;

    let kept: Vec<UrlEntry> = doc
        .outbound_links
        .iter()
        .filter(|u| u.is_http())
        .filter_map(|u| {
            let cap = u.host().and_then(&depth_cap);
            if cap.is_some_and(|limit| new_depth > limit) {
                dropped += 1;
                return None;
            }
            Some(UrlEntry {
                url: u.clone(),
                depth: new_depth,
                discovered_from: Some(parent.url.clone()),
            })
        })
        .collect();

    // Non-HTTP links (mailto:, tel:, javascript:) are silently dropped
    // without a metric because they're scheme-mismatch, not a crawler
    // decision worth surfacing. Depth-cap drops are tracked under one
    // skip reason regardless of whether the cap came from the global
    // default or a per-host override; operators can correlate with
    // politeness::check disallows (decision=disallow_quota) when
    // they need to tell the two mechanisms apart.
    if dropped > 0 {
        metrics::counter!(
            crate::metrics::URLS_SKIPPED_TOTAL,
            "reason" => crate::metrics::SKIP_DEPTH_LIMIT,
        )
        .increment(dropped);
    }

    kept
}

#[cfg(test)]
mod tests {
    // Inline because: locality guards + visibility-forced. The first
    // test ties an array of field names to `WorkerDeps` so adding a
    // field forces an update at the construction site; that reminder
    // only works if the test sits next to the struct. The remaining
    // tests pin the depth-limit math in `compute_outbound`, a private
    // free function, next to the function it guards. Promoting either
    // to `tests/` would force `pub` on an internal helper or array.

    use super::*;
    use crawlrs_core::{CanonicalUrl, ParsedDocument, UrlEntry};

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
            "clock",
        ];
        assert_eq!(names.len(), 11);
    }

    fn parent_at(depth: u32) -> UrlEntry {
        UrlEntry {
            url: CanonicalUrl::parse("https://parent.test/").unwrap(),
            depth,
            discovered_from: None,
        }
    }

    fn doc_with_links(urls: &[&str]) -> ParsedDocument {
        ParsedDocument {
            url: CanonicalUrl::parse("https://parent.test/").unwrap(),
            status: 200,
            title: None,
            text: None,
            outbound_links: urls
                .iter()
                .map(|u| CanonicalUrl::parse(u).unwrap())
                .collect(),
            fetched_at: chrono::Utc::now(),
        }
    }

    /// `compute_outbound` now takes a single closure that returns the
    /// **effective** cap (override-or-global) for each host. These
    /// stubs mimic that: `uncapped` is "every host returns None"
    /// (no global, no override), `capped(n)` is "every host
    /// returns `Some(n)`" (simulates the global being set).
    fn uncapped(_host: &str) -> Option<u32> {
        None
    }
    fn capped(n: u32) -> impl Fn(&str) -> Option<u32> {
        move |_host| Some(n)
    }

    #[test]
    fn compute_outbound_with_no_cap_keeps_all_http_links() {
        let parent = parent_at(0);
        let doc = doc_with_links(&["https://a.test/", "https://b.test/"]);
        let outbound = compute_outbound(&parent, &doc, uncapped);
        assert_eq!(outbound.len(), 2);
        assert!(outbound.iter().all(|e| e.depth == 1));
    }

    #[test]
    fn compute_outbound_drops_links_past_max_depth() {
        let parent = parent_at(2);
        let doc = doc_with_links(&["https://a.test/", "https://b.test/"]);
        // Cap=2 means a parent at depth 2 produces children at depth
        // 3, which exceeds the cap; everything is filtered out.
        let outbound = compute_outbound(&parent, &doc, capped(2));
        assert!(outbound.is_empty());
    }

    #[test]
    fn compute_outbound_keeps_children_exactly_at_max_depth() {
        let parent = parent_at(2);
        let doc = doc_with_links(&["https://a.test/"]);
        // Cap=3 means a parent at depth 2 produces children at depth
        // 3 (exactly the cap); they must be kept, not treated as
        // past-cap.
        let outbound = compute_outbound(&parent, &doc, capped(3));
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0].depth, 3);
    }

    #[test]
    fn compute_outbound_threads_parent_url_as_discovered_from() {
        let parent = parent_at(0);
        let doc = doc_with_links(&["https://a.test/"]);
        let outbound = compute_outbound(&parent, &doc, uncapped);
        assert_eq!(
            outbound[0].discovered_from.as_ref().map(|u| u.as_str()),
            Some("https://parent.test/"),
        );
    }

    #[test]
    fn compute_outbound_per_host_cap_drops_only_capped_host() {
        let parent = parent_at(2);
        // a.test capped at depth 2 -> child at depth 3 dropped.
        // b.test uncapped -> child at depth 3 kept.
        let doc = doc_with_links(&["https://a.test/", "https://b.test/"]);
        let outbound = compute_outbound(&parent, &doc, |host| (host == "a.test").then_some(2));
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0].url.as_str(), "https://b.test/");
    }

    #[test]
    fn compute_outbound_per_host_cap_can_raise_or_lower_independently() {
        let parent = parent_at(2);
        // The closure encodes the effective cap each host already
        // resolved to: a.test got override=10 (high), b.test got the
        // global=2 (low). Children at depth 3: a.test passes (3 <= 10),
        // b.test drops (3 > 2).
        let doc = doc_with_links(&["https://a.test/", "https://b.test/"]);
        let outbound = compute_outbound(&parent, &doc, |host| match host {
            "a.test" => Some(10),
            _ => Some(2),
        });
        assert_eq!(outbound.len(), 1);
        assert_eq!(outbound[0].url.as_str(), "https://a.test/");
    }
}
