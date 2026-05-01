//! Periodic maintenance task that drives [`Frontier::tick`] on a
//! configurable cadence and once during graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::Frontier;
use tokio::sync::watch;
use tracing::{debug, warn};

/// Run a maintenance loop until `shutdown` flips. On each interval
/// tick, call `frontier.tick()` (which for `RedisFrontier` reclaims
/// stranded entries via `XAUTOCLAIM`). On shutdown, call `tick` once
/// more to drain pending work for a dying peer.
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
