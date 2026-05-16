//! Top-level [`Crawler`] handle: builder, config, run/shutdown.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::{
    Blocklist, Clock, CrawlScope, Fetcher, Frontier, HostHashShardPolicy, LinkDispatch,
    MetadataStore, Outbox, Parser, Politeness, ShardingPolicy, SiteAdapterRegistry, Store,
    SystemClock,
};
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::maintenance::maintenance_loop;
use crate::outbox::outbox_publisher;
use crate::supervisor::supervise_worker;
use crate::worker::WorkerDeps;

/// Periodic driver for `Frontier::tick`. Combines the promoter
/// (wake -> ready) and lease-reclaim passes; both run inside the
/// `tick` implementation per ADR-0019 (one background task, not two).
///
/// Lives in the runtime per the project's architecture rule: tokio
/// composition belongs here, not inside the frontier crate.
async fn frontier_tick_loop(
    frontier: Arc<dyn Frontier>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // skip immediate first fire
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(e) = frontier.tick().await {
                    warn!(error = %e, "frontier.tick failed");
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum CrawlerError {
    #[error("missing dependency: {0}")]
    MissingDep(&'static str),
}

/// Knobs for the runtime. Defaults are sensible for a small dev
/// crawl; tighten for production via the builder.
#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    /// Number of concurrent worker tasks. Each owns one in-flight URL
    /// at a time.
    pub workers: usize,

    /// How often the maintenance task emits the process-health
    /// heartbeat. 30s catches slow leaks without dominating log
    /// volume.
    pub maintenance_interval: Duration,

    /// How often the frontier-tick task drives `Frontier::tick` —
    /// the per-shard promoter (wake -> ready) and lease-reclaim
    /// pass. 50ms keeps the latency-tail collapsed under sustained
    /// load while staying out of Redis CPU's way on idle clusters.
    /// See ADR-0019.
    pub promoter_tick: Duration,

    /// Sleep duration when `frontier.claim()` returns `Empty` (queue
    /// is idle, no wake-time in the near future).
    pub empty_queue_poll: Duration,

    /// Cap on how long a worker waits when `claim()` returns
    /// `EmptyHint` with a far-future wake-time. Bounded so shutdown
    /// signals propagate quickly.
    pub max_idle_sleep: Duration,

    /// Sleep duration after an unexpected backend error before
    /// retrying. Throttles error spam without giving up.
    pub error_backoff: Duration,

    /// Per-URL retry budget. After this many `mark_failed` calls the
    /// runtime moves the URL to the dead-letter queue
    /// (`mark_permanently_failed` + frontier `ack` so it stops cycling
    /// via `XAUTOCLAIM`). 5 covers typical 429/503 transient bursts
    /// without re-attempting genuinely broken hosts indefinitely; tune
    /// down on hostile networks, up on tolerant corpora.
    pub max_retries: u32,

    /// StatefulSet pod ordinal for this process. Combined with each
    /// worker's task index it forms the [`crawlrs_core::WorkerIdentity`]
    /// used by the frontier for lease attribution. Stable across
    /// process restarts so a restarting worker can recover its own
    /// previously-leased URLs without waiting for the reclaim timeout.
    ///
    /// In a Kubernetes StatefulSet deployment this is the integer
    /// suffix on `HOSTNAME` (`crawlrs-2` -> 2); single-process or test
    /// deployments use 0.
    pub pod_ordinal: u32,

    /// Per-worker restart policy applied by the supervisor task. When
    /// a worker panics or exits unexpectedly the supervisor respawns
    /// it under this budget; once exhausted it gives up on that worker
    /// and the pool runs degraded. Tune `max_restarts` up for hostile
    /// corpora that crash parsers, down to limit blast radius from a
    /// genuine crash-loop bug.
    pub restart_policy: crate::supervisor::RestartPolicy,

    /// Strategy for moving discovered outbound URLs into the
    /// Frontier. `Direct` (default) bypasses the outbox and lets the
    /// worker submit URLs fire-and-forget after the metadata commit;
    /// trades durability for ~50x lower Postgres write rate.
    /// `DurableOutbox` commits outbound URLs atomically with the
    /// metadata write and lets the publisher drain them
    /// asynchronously, surviving any single component crash.
    pub link_dispatch: LinkDispatch,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            maintenance_interval: Duration::from_secs(30),
            promoter_tick: Duration::from_millis(50),
            empty_queue_poll: Duration::from_millis(500),
            max_idle_sleep: Duration::from_secs(5),
            error_backoff: Duration::from_secs(1),
            max_retries: 5,
            pod_ordinal: 0,
            restart_policy: crate::supervisor::RestartPolicy::default(),
            link_dispatch: LinkDispatch::default(),
        }
    }
}

/// Top-level handle. Build via [`CrawlerBuilder`]; drive via
/// [`Crawler::run`]; flip the shutdown flag via [`Crawler::shutdown`]
/// from a signal handler or admin command.
pub struct Crawler {
    deps: Arc<WorkerDeps>,
    /// Reader side of the transactional outbox. The atomic write
    /// path runs through `MetadataStore::mark_succeeded`; this
    /// handle drives the publisher task that drains the table into
    /// the Frontier. Typically the same `Arc` as `deps.metadata`,
    /// since the Postgres impl satisfies both traits.
    outbox: Arc<dyn Outbox>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl std::fmt::Debug for Crawler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crawler")
            .field("workers", &self.deps.config.workers)
            .field(
                "maintenance_interval",
                &self.deps.config.maintenance_interval,
            )
            .finish()
    }
}

impl Crawler {
    pub fn builder() -> CrawlerBuilder {
        CrawlerBuilder::default()
    }

    /// Spawn the worker pool + maintenance task and wait for them
    /// all to exit. Exit happens on `shutdown` flip or when all
    /// workers naturally complete (which only happens if the queue
    /// drains *and* shutdown was requested; without shutdown,
    /// workers loop forever waiting for new URLs).
    pub async fn run(&self) -> Result<(), CrawlerError> {
        info!(workers = self.deps.config.workers, "Crawler starting",);

        let mut tasks = JoinSet::new();

        // Heartbeat task. Process-health snapshot only; frontier
        // bookkeeping is driven by `frontier_tick_loop` below.
        let interval = self.deps.config.maintenance_interval;
        let m_shutdown = self.shutdown_rx.clone();
        tasks.spawn(maintenance_loop(interval, m_shutdown));

        // Frontier promoter + lease-reclaim. Ticks every
        // `promoter_tick`; the frontier's `tick` impl does both
        // promotion (wake -> ready) and lease-expiry reclaim
        // (inflight -> host_queue) in one call.
        let f_shutdown = self.shutdown_rx.clone();
        let frontier = self.deps.frontier.clone();
        let promoter_tick = self.deps.config.promoter_tick;
        tasks.spawn(frontier_tick_loop(frontier, promoter_tick, f_shutdown));

        // Outbox publisher: drains frontier_outbox rows written
        // transactionally by the worker pipeline's mark_succeeded
        // call into the Frontier. Only relevant under
        // `LinkDispatch::DurableOutbox`; in `Direct` mode the worker
        // submits outbound URLs to the Frontier itself and the
        // outbox table sits empty, so spawning the publisher would
        // be a no-op tight loop.
        if matches!(self.deps.config.link_dispatch, LinkDispatch::DurableOutbox) {
            let outbox = self.outbox.clone();
            let frontier = self.deps.frontier.clone();
            let p_shutdown = self.shutdown_rx.clone();
            tasks.spawn(outbox_publisher(
                outbox,
                frontier,
                p_shutdown,
                crate::outbox::DEFAULT_PUBLISH_INTERVAL,
            ));
        }

        // Worker pool. Each worker gets a stable WorkerIdentity built
        // from the configured pod_ordinal + the per-pod worker index.
        // The identity flows into Frontier::claim as the Redis Streams
        // consumer name; stability across restarts is what makes
        // tier-1 PEL replay work without waiting for XAUTOCLAIM.
        //
        // We spawn one *supervisor* task per worker, not the worker
        // directly. The supervisor owns the worker's lifecycle: a
        // panic in the worker is caught by the supervisor and the
        // worker is respawned under `restart_policy` instead of
        // permanently shrinking the pool to W-1.
        let pod_ordinal = self.deps.config.pod_ordinal;
        let restart_policy = self.deps.config.restart_policy.clone();
        for worker_index in 0..self.deps.config.workers {
            let identity = crawlrs_core::WorkerIdentity::new(pod_ordinal, worker_index as u32);
            let deps = self.deps.clone();
            let w_shutdown = self.shutdown_rx.clone();
            let policy = restart_policy.clone();
            tasks.spawn(supervise_worker(identity, deps, w_shutdown, policy));
        }

        // Drain.
        while let Some(joined) = tasks.join_next().await {
            if let Err(e) = joined {
                warn!(error = %e, "worker / maintenance task panicked");
            }
        }
        info!("Crawler stopped");
        Ok(())
    }

    /// Signal graceful shutdown. Workers finish their in-flight URL
    /// (with ack/nack) and exit; the maintenance task ticks once more
    /// to drain pending reclaim before exiting.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Borrow the dep bag. Useful for tests that want to drive the
    /// frontier directly (e.g. seed URLs before calling `run`).
    pub fn deps(&self) -> &WorkerDeps {
        &self.deps
    }
}

/// Builder. Each setter consumes self for ergonomic chaining.
#[derive(Default)]
pub struct CrawlerBuilder {
    frontier: Option<Arc<dyn Frontier>>,
    politeness: Option<Arc<dyn Politeness>>,
    fetcher: Option<Arc<dyn Fetcher>>,
    parser: Option<Arc<dyn Parser>>,
    store: Option<Arc<dyn Store>>,
    metadata: Option<Arc<dyn MetadataStore>>,
    /// Outbox reader. Optional in the builder surface because the
    /// production Postgres impl satisfies both `MetadataStore` and
    /// `Outbox` and callers typically pass the same `Arc` to
    /// both setters; if omitted at build time, we default-fall-back
    /// to none and the publisher is spawned only when supplied.
    outbox: Option<Arc<dyn Outbox>>,
    adapters: Option<Arc<SiteAdapterRegistry>>,
    sharding_policy: Option<Arc<dyn ShardingPolicy>>,
    clock: Option<Arc<dyn Clock>>,
    config: Option<CrawlerConfig>,
    crawl_scope: Option<CrawlScope>,
    blocklist: Option<Blocklist>,
    run_id: Option<String>,
}

impl CrawlerBuilder {
    pub fn frontier(mut self, f: Arc<dyn Frontier>) -> Self {
        self.frontier = Some(f);
        self
    }
    pub fn politeness(mut self, p: Arc<dyn Politeness>) -> Self {
        self.politeness = Some(p);
        self
    }
    pub fn fetcher(mut self, f: Arc<dyn Fetcher>) -> Self {
        self.fetcher = Some(f);
        self
    }
    pub fn parser(mut self, p: Arc<dyn Parser>) -> Self {
        self.parser = Some(p);
        self
    }
    pub fn store(mut self, s: Arc<dyn Store>) -> Self {
        self.store = Some(s);
        self
    }
    pub fn metadata(mut self, m: Arc<dyn MetadataStore>) -> Self {
        self.metadata = Some(m);
        self
    }
    /// Set the outbox reader. Production wiring usually passes the
    /// same `Arc` here as in `metadata()` (PostgresMetadataStore
    /// implements both traits).
    pub fn outbox(mut self, o: Arc<dyn Outbox>) -> Self {
        self.outbox = Some(o);
        self
    }
    pub fn adapters(mut self, a: Arc<SiteAdapterRegistry>) -> Self {
        self.adapters = Some(a);
        self
    }
    pub fn sharding_policy(mut self, p: Arc<dyn ShardingPolicy>) -> Self {
        self.sharding_policy = Some(p);
        self
    }
    /// Inject a custom [`Clock`]. Defaults to [`SystemClock`]. Tests
    /// pass a `ManualClock` to drive supervisor restart-window math
    /// deterministically.
    pub fn clock(mut self, c: Arc<dyn Clock>) -> Self {
        self.clock = Some(c);
        self
    }
    pub fn config(mut self, c: CrawlerConfig) -> Self {
        self.config = Some(c);
        self
    }
    /// Inject the `[crawl]` config table (per-host depth + URL
    /// caps). Defaults to an unbounded scope when omitted, which
    /// is appropriate for tests that don't exercise scope
    /// controls.
    pub fn crawl_scope(mut self, s: CrawlScope) -> Self {
        self.crawl_scope = Some(s);
        self
    }
    /// Inject the `[access]` blocklist. Defaults to an empty
    /// blocklist when omitted. Checked by the worker's
    /// `politeness_allows` before delegating to `Politeness::check`.
    pub fn blocklist(mut self, b: Blocklist) -> Self {
        self.blocklist = Some(b);
        self
    }
    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    pub fn build(self) -> Result<Crawler, CrawlerError> {
        let frontier = self.frontier.ok_or(CrawlerError::MissingDep("frontier"))?;
        let politeness = self
            .politeness
            .ok_or(CrawlerError::MissingDep("politeness"))?;
        let fetcher = self.fetcher.ok_or(CrawlerError::MissingDep("fetcher"))?;
        let parser = self.parser.ok_or(CrawlerError::MissingDep("parser"))?;
        let store = self.store.ok_or(CrawlerError::MissingDep("store"))?;
        let metadata = self.metadata.ok_or(CrawlerError::MissingDep("metadata"))?;
        let outbox = self.outbox.ok_or(CrawlerError::MissingDep("outbox"))?;
        let run_id = self.run_id.ok_or(CrawlerError::MissingDep("run_id"))?;
        let adapters = self
            .adapters
            .unwrap_or_else(|| Arc::new(SiteAdapterRegistry::new()));
        // 8 shards by default: bounds hot-domain head-of-line blocking
        // to one shard's worker capacity while staying small enough that
        // one Redis instance can hold all per-shard state. Operators
        // running SingleShard (typical for tests) override via the builder
        // and must pass the same policy they wired into the frontier.
        let sharding_policy = self
            .sharding_policy
            .unwrap_or_else(|| Arc::new(HostHashShardPolicy::new(8)));
        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        let config = self.config.unwrap_or_default();
        let crawl_scope = self.crawl_scope.unwrap_or_default();
        let blocklist = self.blocklist.unwrap_or_default();

        let deps = Arc::new(WorkerDeps {
            frontier,
            politeness,
            fetcher,
            parser,
            store,
            metadata,
            adapters,
            config,
            crawl_scope,
            blocklist,
            run_id,
            sharding_policy,
            clock,
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Ok(Crawler {
            deps,
            outbox,
            shutdown_tx,
            shutdown_rx,
        })
    }
}
