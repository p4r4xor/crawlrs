//! Periodic maintenance task: process-health heartbeat (RSS, open
//! FD count) on a configurable cadence.
//!
//! Frontier maintenance (`XAUTOCLAIM` reclaim of stranded entries) is
//! driven by the workers themselves now; this loop's only job is
//! observability. Periodic snapshots make slow leaks (e.g. an FD
//! count creeping from 1500 to 4000 over an 11-hour run) visible
//! when point-in-time inspection would miss them.

use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info};

/// Run a maintenance loop until `shutdown` flips. On each interval
/// tick, log a process-health snapshot. The loop currently does not
/// drive any frontier-side work; the heartbeat exists so long-running
/// crawls leave a per-minute audit trail in the log.
pub async fn maintenance_loop(interval: Duration, mut shutdown: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(interval);
    // Skip the immediate first tick that `interval` fires; we want our
    // first heartbeat to happen after `interval` elapses, not at t=0.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;

    loop {
        tokio::select! {
            _ = tick.tick() => {
                heartbeat().await;
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    debug!("maintenance_loop: shutdown signalled, exiting");
                    break;
                }
            }
        }
    }
}

/// Log RSS and open-FD count. Best-effort: if `/proc/self/*` is
/// unavailable (non-Linux, restricted container) we skip the field
/// rather than fail the loop. This is intentionally minimal; full
/// metrics (Prometheus, OTLP) land with Phase 5d.
async fn heartbeat() {
    let rss_kb = read_rss_kb().await;
    let fd_count = read_fd_count().await;
    info!(
        rss_kb = rss_kb.map(|n| n as i64).unwrap_or(-1),
        fd_count = fd_count.map(|n| n as i64).unwrap_or(-1),
        "process heartbeat",
    );
}

/// Parse `VmRSS` out of `/proc/self/status`. Linux-only; returns None
/// on other platforms or any read/parse failure. Uses `tokio::fs` to
/// avoid blocking the executor (Rule 16 - `/proc` reads are nominally
/// instant, but the precedent matters).
async fn read_rss_kb() -> Option<u64> {
    let status = tokio::fs::read_to_string("/proc/self/status").await.ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb_str = rest.split_whitespace().next()?;
            return kb_str.parse().ok();
        }
    }
    None
}

/// Count entries in `/proc/self/fd`. Linux-only. Includes sockets,
/// regular files, anon-inode fds (epoll, eventfd, etc.) - exactly what
/// the kernel's per-process FD limit constrains.
async fn read_fd_count() -> Option<usize> {
    let mut dir = tokio::fs::read_dir("/proc/self/fd").await.ok()?;
    let mut count = 0usize;
    while dir.next_entry().await.ok().flatten().is_some() {
        count += 1;
    }
    Some(count)
}
