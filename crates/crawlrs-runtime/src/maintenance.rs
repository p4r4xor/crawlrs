//! Periodic maintenance task: process-health heartbeat on a
//! configurable cadence.
//!
//! Frontier maintenance (`XAUTOCLAIM` reclaim of stranded entries) is
//! driven by the workers themselves now; this loop's only job is
//! observability. Periodic snapshots make slow leaks visible (e.g.
//! RSS climbing 750 MB in 60s, FD count creeping from 1500 to 4000)
//! when point-in-time inspection would miss them.
//!
//! The heartbeat carries enough fields to localize a leak across
//! three layers without external profiling tooling:
//!
//! - **OS / cgroup**: `rss_kb`, `peak_kb`, `vsize_kb`, `data_kb`,
//!   `hwm_kb`, `threads`, `fd_count`. Sourced from `/proc/self/status`
//!   and `/proc/self/fd`.
//! - **Tokio runtime**: `tokio_alive_tasks`, `tokio_workers`. Sourced
//!   from `tokio::runtime::Handle::current().metrics()`.
//! - **Application**: emitted as separate metric counters elsewhere
//!   (URLs fetched, frontier claims, etc.); not duplicated here.

use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info};

/// Run a maintenance loop until `shutdown` flips. On each interval
/// tick, log a process-health snapshot.
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

/// Snapshot of memory + thread state from `/proc/self/status`.
/// All fields are best-effort: `None` means the line was missing or
/// failed to parse, which we surface as -1 in the log so a missing
/// field is distinguishable from a zero value.
#[derive(Default)]
struct ProcStatus {
    /// Resident set size: physical RAM the process is using right now.
    rss_kb: Option<u64>,
    /// High-water mark of resident set size since process start.
    /// Useful for spotting transient memory spikes that don't show
    /// up in instantaneous `rss_kb`.
    hwm_kb: Option<u64>,
    /// Total virtual address space, including memory-mapped files,
    /// shared libs, etc. Climbs faster than rss when the allocator
    /// reserves heap regions it hasn't touched yet.
    vsize_kb: Option<u64>,
    /// High-water mark of virtual size since process start.
    peak_kb: Option<u64>,
    /// Data + stack + heap. Closer to "what the program owns"
    /// than vsize. The gap between data_kb and rss_kb is the canonical
    /// allocator-fragmentation signal.
    data_kb: Option<u64>,
    /// Thread count. A leak here typically means a tokio task that
    /// spawned a blocking thread and never released it.
    threads: Option<u32>,
}

/// Log a snapshot covering OS state + tokio runtime state. Best-effort:
/// missing fields render as -1 in the log; the loop never fails the
/// process.
async fn heartbeat() {
    let proc = read_proc_status().await;
    let fd_count = read_fd_count().await;
    let rt = tokio::runtime::Handle::current().metrics();
    let alive_tasks = rt.num_alive_tasks() as i64;
    let workers = rt.num_workers() as i64;

    // Emit as Prometheus gauges (graphable) alongside the log line
    // (searchable). KB -> bytes for the OS fields so Grafana's `bytes`
    // unit picks them up cleanly.
    if let Some(kb) = proc.rss_kb {
        metrics::gauge!(crate::metrics::PROCESS_RSS_BYTES).set((kb * 1024) as f64);
    }
    if let Some(kb) = proc.hwm_kb {
        metrics::gauge!(crate::metrics::PROCESS_HWM_BYTES).set((kb * 1024) as f64);
    }
    if let Some(kb) = proc.vsize_kb {
        metrics::gauge!(crate::metrics::PROCESS_VSIZE_BYTES).set((kb * 1024) as f64);
    }
    if let Some(kb) = proc.peak_kb {
        metrics::gauge!(crate::metrics::PROCESS_PEAK_BYTES).set((kb * 1024) as f64);
    }
    if let Some(kb) = proc.data_kb {
        metrics::gauge!(crate::metrics::PROCESS_DATA_BYTES).set((kb * 1024) as f64);
    }
    if let Some(t) = proc.threads {
        metrics::gauge!(crate::metrics::PROCESS_THREADS).set(t as f64);
    }
    if let Some(n) = fd_count {
        metrics::gauge!(crate::metrics::PROCESS_FDS).set(n as f64);
    }
    metrics::gauge!(crate::metrics::TOKIO_ALIVE_TASKS).set(alive_tasks as f64);
    metrics::gauge!(crate::metrics::TOKIO_WORKERS).set(workers as f64);

    info!(
        // OS / cgroup
        rss_kb = proc.rss_kb.map(|n| n as i64).unwrap_or(-1),
        hwm_kb = proc.hwm_kb.map(|n| n as i64).unwrap_or(-1),
        vsize_kb = proc.vsize_kb.map(|n| n as i64).unwrap_or(-1),
        peak_kb = proc.peak_kb.map(|n| n as i64).unwrap_or(-1),
        data_kb = proc.data_kb.map(|n| n as i64).unwrap_or(-1),
        threads = proc.threads.map(|n| n as i64).unwrap_or(-1),
        fd_count = fd_count.map(|n| n as i64).unwrap_or(-1),
        // Tokio runtime
        tokio_alive_tasks = alive_tasks,
        tokio_workers = workers,
        "process heartbeat",
    );
}

/// Parse `/proc/self/status` for every memory + thread field we
/// care about in one read. Linux-only; returns an empty `ProcStatus`
/// (all `None`) on other platforms or any read/parse failure.
async fn read_proc_status() -> ProcStatus {
    let Ok(status) = tokio::fs::read_to_string("/proc/self/status").await else {
        return ProcStatus::default();
    };
    let mut out = ProcStatus::default();
    for line in status.lines() {
        let Some((label, rest)) = line.split_once(':') else {
            continue;
        };
        let value_str = rest.split_whitespace().next();
        match (label, value_str) {
            ("VmRSS", Some(v)) => out.rss_kb = v.parse().ok(),
            ("VmHWM", Some(v)) => out.hwm_kb = v.parse().ok(),
            ("VmSize", Some(v)) => out.vsize_kb = v.parse().ok(),
            ("VmPeak", Some(v)) => out.peak_kb = v.parse().ok(),
            ("VmData", Some(v)) => out.data_kb = v.parse().ok(),
            ("Threads", Some(v)) => out.threads = v.parse().ok(),
            _ => {}
        }
    }
    out
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
