//! Periodic maintenance task that drives [`Frontier::tick`] on a
//! configurable cadence and once during graceful shutdown. Also emits
//! a process-health heartbeat (RSS, open FD count) so a running crawl
//! has the breadcrumbs Andrew Chan's debugging sessions wished they
//! had: an FD leak from 1500 to 4000 over 11 hours is invisible
//! without periodic snapshots.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::Frontier;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Run a maintenance loop until `shutdown` flips. On each interval
/// tick, call `frontier.tick()` (which for `RedisFrontier` reclaims
/// stranded entries via `XAUTOCLAIM`) and log a process-health
/// snapshot. On shutdown, call `tick` once more to drain pending work
/// for a dying peer.
pub async fn maintenance_loop(
    frontier: Arc<dyn Frontier>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    // Skip the immediate first tick that `interval` fires; we want our
    // first maintenance to happen after `interval` elapses, not at t=0.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;

    loop {
        tokio::select! {
            _ = tick.tick() => {
                run_one(&*frontier).await;
                heartbeat();
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    debug!("maintenance_loop: shutdown signalled, draining once");
                    run_one(&*frontier).await;
                    break;
                }
            }
        }
    }
}

#[tracing::instrument(skip(frontier))]
async fn run_one(frontier: &dyn Frontier) {
    match frontier.tick().await {
        Ok(0) => {}
        Ok(n) => debug!(reclaimed = n, "maintenance tick"),
        Err(e) => warn!(error = %e, "maintenance tick failed"),
    }
}

/// Log RSS and open-FD count. Best-effort: if `/proc/self/*` is
/// unavailable (non-Linux, restricted container) we skip the field
/// rather than fail the loop. This is intentionally minimal; full
/// metrics (Prometheus, OTLP) land with Phase 5d.
fn heartbeat() {
    let rss_kb = read_rss_kb();
    let fd_count = read_fd_count();
    info!(
        rss_kb = rss_kb.map(|n| n as i64).unwrap_or(-1),
        fd_count = fd_count.map(|n| n as i64).unwrap_or(-1),
        "process heartbeat",
    );
}

/// Parse `VmRSS` out of `/proc/self/status`. Linux-only; returns None
/// on other platforms or any read/parse failure.
fn read_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb_str = rest.split_whitespace().next()?;
            return kb_str.parse().ok();
        }
    }
    None
}

/// Count entries in `/proc/self/fd`. Linux-only. Includes sockets,
/// regular files, anon-inode fds (epoll, eventfd, etc.) — exactly what
/// the kernel's per-process FD limit constrains.
fn read_fd_count() -> Option<usize> {
    Some(std::fs::read_dir("/proc/self/fd").ok()?.count())
}
