//! Top-level [`Crawler`] handle: builder, config, run/shutdown.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::{Fetcher, Frontier, Parser, Politeness, SiteAdapterRegistry, Store};
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::maintenance::maintenance_loop;
use crate::worker::{worker_loop, WorkerDeps};

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

    /// User-agent header sent on every fetch. Should match the
    /// politeness layer's `user_agent` so robots.txt rules apply
    /// consistently.
    pub user_agent: String,

    /// Maximum link depth from any seed. `None` means unbounded.
    /// Discovered links beyond this depth are dropped at submit time.
    pub max_depth: Option<u32>,

    /// How often the maintenance task drives `Frontier::tick`.
    /// 30s mirrors the typical XAUTOCLAIM cadence at which stranded
    /// entries are reclaimed.
    pub maintenance_interval: Duration,

    /// Sleep duration when `frontier.claim()` returns no entry.
    pub empty_queue_poll: Duration,

    /// Sleep duration when `politeness.next_ready_at()` returns None
    /// (no hosts tracked yet, e.g. at startup).
    pub startup_poll: Duration,

    /// Cap on how long a worker waits even when politeness reports a
    /// far-future wake-time. Bounded so shutdown signals propagate
    /// quickly.
    pub max_idle_sleep: Duration,

    /// Sleep duration after an unexpected backend error before
    /// retrying. Throttles error spam without giving up.
    pub error_backoff: Duration,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            user_agent: "crawlrs/0.0.1 (+https://github.com/p4r4xor/crawlrs)".into(),
            max_depth: Some(5),
            maintenance_interval: Duration::from_secs(30),
            empty_queue_poll: Duration::from_millis(500),
            startup_poll: Duration::from_millis(100),
            max_idle_sleep: Duration::from_secs(5),
            error_backoff: Duration::from_secs(1),
        }
    }
}

/// Top-level handle. Build via [`CrawlerBuilder`]; drive via
/// [`Crawler::run`]; flip the shutdown flag via [`Crawler::shutdown`]
/// from a signal handler or admin command.
pub struct Crawler {
    deps: Arc<WorkerDeps>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl std::fmt::Debug for Crawler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crawler")
            .field("workers", &self.deps.config.workers)
            .field("max_depth", &self.deps.config.max_depth)
            .field("maintenance_interval", &self.deps.config.maintenance_interval)
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
        info!(
            workers = self.deps.config.workers,
            max_depth = ?self.deps.config.max_depth,
            user_agent = %self.deps.config.user_agent,
            "Crawler starting",
        );

        let mut tasks = JoinSet::new();

        // Maintenance task.
        let frontier = self.deps.frontier.clone();
        let interval = self.deps.config.maintenance_interval;
        let m_shutdown = self.shutdown_rx.clone();
        tasks.spawn(maintenance_loop(frontier, interval, m_shutdown));

        // Worker pool.
        for worker_id in 0..self.deps.config.workers {
            let deps = self.deps.clone();
            let w_shutdown = self.shutdown_rx.clone();
            tasks.spawn(worker_loop(worker_id, deps, w_shutdown));
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
    adapters: Option<Arc<SiteAdapterRegistry>>,
    config: Option<CrawlerConfig>,
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
    pub fn adapters(mut self, a: Arc<SiteAdapterRegistry>) -> Self {
        self.adapters = Some(a);
        self
    }
    pub fn config(mut self, c: CrawlerConfig) -> Self {
        self.config = Some(c);
        self
    }

    pub fn build(self) -> Result<Crawler, CrawlerError> {
        let frontier = self.frontier.ok_or(CrawlerError::MissingDep("frontier"))?;
        let politeness = self.politeness.ok_or(CrawlerError::MissingDep("politeness"))?;
        let fetcher = self.fetcher.ok_or(CrawlerError::MissingDep("fetcher"))?;
        let parser = self.parser.ok_or(CrawlerError::MissingDep("parser"))?;
        let store = self.store.ok_or(CrawlerError::MissingDep("store"))?;
        let adapters = self
            .adapters
            .unwrap_or_else(|| Arc::new(SiteAdapterRegistry::new()));
        let config = self.config.unwrap_or_default();

        let deps = Arc::new(WorkerDeps {
            frontier,
            politeness,
            fetcher,
            parser,
            store,
            adapters,
            config,
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Ok(Crawler { deps, shutdown_tx, shutdown_rx })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_deps_fail_to_build() {
        let err = Crawler::builder().build().unwrap_err();
        assert!(matches!(err, CrawlerError::MissingDep("frontier")));
    }

    #[test]
    fn defaults_are_sensible() {
        let c = CrawlerConfig::default();
        assert!(c.workers >= 1);
        assert!(c.maintenance_interval >= Duration::from_secs(1));
        assert!(c.empty_queue_poll < c.maintenance_interval);
    }
}
