//! axum HTTP host. Mounts `/metrics` (Prometheus exposition) plus
//! the three health probes `/healthz`, `/livez`, `/readyz` on a
//! single port.
//!
//! Health-probe semantics:
//!
//! - **`/healthz`**: process is up. Always 200 if the binary is
//!   running. The probe of last resort.
//! - **`/livez`**: internal liveness. 200 unless the worker pool has
//!   deadlocked (`ProbeState::live` is false). Triggers a pod restart
//!   on failure.
//! - **`/readyz`**: ready to serve scrapes. 200 only when
//!   `ProbeState::ready` is true. Marked `false` during startup
//!   (before adapters connected) and during shutdown (so scrapers
//!   stop hitting the pod before workers drain).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tracing::info;

/// Internal probe state shared between the axum handlers and the
/// rest of the binary. `ready` flips false during shutdown; `live`
/// is reserved for future deadlock-detection logic (currently always
/// true).
#[derive(Debug, Default)]
pub struct ProbeState {
    pub ready: AtomicBool,
    pub live: AtomicBool,
}

impl ProbeState {
    pub fn new_ready() -> Self {
        Self {
            ready: AtomicBool::new(true),
            live: AtomicBool::new(true),
        }
    }

    pub fn mark_not_ready(&self) {
        self.ready.store(false, Ordering::Release);
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
struct AppState {
    metrics: Arc<PrometheusHandle>,
    probes: Arc<ProbeState>,
}

pub fn router(metrics: Arc<PrometheusHandle>, probes: Arc<ProbeState>) -> Router {
    let state = AppState { metrics, probes };
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .with_state(state)
}

/// Serve the router on `listen` until `shutdown.changed()` fires.
pub async fn serve(
    listen: String,
    metrics: Arc<PrometheusHandle>,
    probes: Arc<ProbeState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let listener = TcpListener::bind(&listen).await?;
    info!(addr = %listen, "HTTP server listening");
    let app = router(metrics, probes);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await?;
    Ok(())
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn livez(State(state): State<AppState>) -> impl IntoResponse {
    if state.probes.is_live() {
        (StatusCode::OK, "live")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not live")
    }
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if state.probes.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}
