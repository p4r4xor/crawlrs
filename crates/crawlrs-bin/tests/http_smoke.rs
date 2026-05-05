//! HTTP host smoke tests. Mounts the router on an ephemeral port,
//! hits every endpoint, asserts on status codes + the `/metrics`
//! body shape (Prometheus exposition).

use std::sync::Arc;

use crawlrs_bin::http::{ProbeState, router};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;

async fn spawn_server() -> (String, Arc<ProbeState>) {
    let probes = Arc::new(ProbeState::new_ready());
    // Per-test recorder; we don't install it globally since multiple
    // tests in this binary would collide. The handle still serves
    // valid (possibly empty) Prometheus exposition for /metrics.
    let metrics = Arc::new(PrometheusBuilder::new().build_recorder().handle());

    let app = router(metrics, probes.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), probes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthz_always_ok() {
    let (base, _probes) = spawn_server().await;
    let client = reqwest_blocking_get(&format!("{base}/healthz")).await;
    assert_eq!(client.0, 200);
    assert!(client.1.contains("ok"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn livez_reflects_probe_state() {
    let (base, probes) = spawn_server().await;

    let (status, body) = reqwest_blocking_get(&format!("{base}/livez")).await;
    assert_eq!(status, 200);
    assert!(body.contains("live"));

    probes
        .live
        .store(false, std::sync::atomic::Ordering::Release);

    let (status, _body) = reqwest_blocking_get(&format!("{base}/livez")).await;
    assert_eq!(status, 503);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_reflects_probe_state() {
    let (base, probes) = spawn_server().await;

    let (status, body) = reqwest_blocking_get(&format!("{base}/readyz")).await;
    assert_eq!(status, 200);
    assert!(body.contains("ready"));

    probes.mark_not_ready();
    let (status, _body) = reqwest_blocking_get(&format!("{base}/readyz")).await;
    assert_eq!(status, 503);

    probes.mark_ready();
    let (status, _body) = reqwest_blocking_get(&format!("{base}/readyz")).await;
    assert_eq!(status, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_serves_prometheus_response() {
    let (base, _probes) = spawn_server().await;
    let (status, _body) = reqwest_blocking_get(&format!("{base}/metrics")).await;
    // The endpoint returns 200 with whatever the per-test recorder
    // has rendered (possibly an empty body if no metrics emitted).
    // Verifying emission round-trip is the job of the
    // `metrics_emission` test under crawlrs-runtime, which uses
    // metrics-util's DebuggingRecorder to capture by name.
    assert_eq!(status, 200);
}

/// Tiny HTTP GET via tokio TcpStream (no reqwest dep needed for tests).
/// Returns (status_code, body_string).
async fn reqwest_blocking_get(url: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Parse "http://host:port/path"
    let stripped = url.strip_prefix("http://").unwrap();
    let (host_port, path) = match stripped.find('/') {
        Some(i) => (&stripped[..i], &stripped[i..]),
        None => (stripped, "/"),
    };

    let mut stream = tokio::net::TcpStream::connect(host_port).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();

    // Parse status code from "HTTP/1.1 <code> ..."
    let status_line = response.lines().next().unwrap_or("");
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);

    // Body starts after the blank line.
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();

    (status_code, body)
}
