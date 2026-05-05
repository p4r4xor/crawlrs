//! SIGTERM / Ctrl-C handler.
//!
//! Waits for either signal, then sets the shared `watch::Sender<bool>`
//! to `true`. Every other long-lived task (worker pool, HTTP server,
//! maintenance loop) listens to the matching `Receiver` and exits.

use anyhow::Result;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::info;

pub async fn wait_for_signal(tx: watch::Sender<bool>) -> Result<()> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = sigterm.recv() => info!("SIGTERM received"),
        _ = sigint.recv() => info!("SIGINT received"),
    }
    let _ = tx.send(true);
    Ok(())
}
