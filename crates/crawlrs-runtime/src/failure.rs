//! Mapping from concrete fetch outcomes to [`FailureKind`] categories,
//! and parsing the `Retry-After` response header into a [`Duration`].
//!
//! The runtime calls these at the boundary between `Fetcher::fetch`
//! returning and `Politeness::record_failure` being invoked, so the
//! politeness layer sees the *category* of failure (and applies the
//! right backoff), not the underlying error message.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crawlrs_core::{Error, FailureKind};

/// Map an HTTP status code to a `FailureKind`, or `None` if the
/// status code represents a successful fetch (the politeness layer
/// shouldn't be told about successes).
///
/// 2xx and 3xx are success-shaped. 404 / 410 surface as `NotFound`
/// so dashboards can see how much of the failure tail is dead links
/// (usually a frontier or seed problem rather than politeness).
/// 429 / 503 get the dedicated rate-limit variants. Other 4xx fall
/// to `ClientError` (auth / policy refusal), other 5xx to
/// `ServerError` (origin glitch). Anything outside 100..=599 is
/// `Other`.
pub fn classify_status(status: u16) -> Option<FailureKind> {
    match status {
        200..=399 => None,
        404 | 410 => Some(FailureKind::NotFound),
        429 => Some(FailureKind::TooManyRequests),
        400..=499 => Some(FailureKind::ClientError),
        503 => Some(FailureKind::ServiceUnavailable),
        500..=599 => Some(FailureKind::ServerError),
        _ => Some(FailureKind::Other),
    }
}

/// Map a transport-level fetch error to a `FailureKind`.
///
/// String-matching on the error's `Display` form is rough, but our
/// `crawlrs_core::Error` deliberately wraps each crate's error in a
/// string at the boundary; the alternative is a richer error enum,
/// which is a future refactor. Until then, this keeps the
/// classification logic in one place.
///
/// Match order matters: specific local-side / protocol failures are
/// checked before generic network-shaped ones, so e.g. a TLS error
/// whose Display string also contains "timeout" (rare but possible)
/// lands in `TlsError` and not `Timeout`.
pub fn classify_transport_error(err: &Error) -> FailureKind {
    // Hyper / wreq wrap connection-establishment failures in opaque
    // `client error (Connect)`-style strings that hide the actual
    // syscall failure. Walk the source chain via `Error::source()` and
    // concatenate every layer's Display string so the substring matchers
    // below can catch the inner cause (e.g. "connection refused"
    // sitting two layers below a "client error (Connect)" wrapper).
    let mut text = err.to_string().to_ascii_lowercase();
    let mut current: &dyn std::error::Error = err;
    while let Some(source) = current.source() {
        text.push(' ');
        text.push_str(&source.to_string().to_ascii_lowercase());
        current = source;
    }

    // Local resource exhaustion is "our fault" and very specific.
    if text.contains("too many open files") || text.contains("emfile") {
        return FailureKind::ResourceExhausted;
    }
    // TLS handshake failures: rustls / BoringSSL surface as "tls",
    // "ssl", "handshake", or "certificate" depending on the failure.
    if text.contains("tls")
        || text.contains("ssl")
        || text.contains("handshake")
        || text.contains("certificate")
    {
        return FailureKind::TlsError;
    }
    // DNS resolution: hickory wraps as "dns error -> ..." and glibc's
    // resolver surfaces as "failed to lookup address" / "name resolution".
    if text.contains("dns error")
        || text.contains("name resolution")
        || text.contains("failed to lookup")
    {
        return FailureKind::DnsFailure;
    }
    // Routable-but-no-route shapes (ENETUNREACH / EHOSTUNREACH). Multiple
    // Display variants exist depending on whether the error came from
    // an io::ErrorKind variant ("network unreachable") or libc strerror
    // ("network is unreachable"); cover both, plus the host-level forms.
    if text.contains("network is unreachable")
        || text.contains("network unreachable")
        || text.contains("no route to host")
        || text.contains("host is unreachable")
        || text.contains("host unreachable")
    {
        return FailureKind::Unreachable;
    }
    if text.contains("timeout") || text.contains("timed out") {
        return FailureKind::Timeout;
    }
    if text.contains("connection reset")
        || text.contains("connection aborted")
        || text.contains("broken pipe")
        || text.contains("connection refused")
    {
        return FailureKind::ConnectReset;
    }
    // Local source-port pool exhausted. Common at high concurrency with
    // `pool_max_idle_per_host = 0` against many hosts: the kernel runs
    // out of ephemeral ports for new outbound sockets.
    if text.contains("address not available")
        || text.contains("cannot assign requested address")
        || text.contains("eaddrnotavail")
    {
        return FailureKind::ResourceExhausted;
    }
    FailureKind::Other
}

/// Parse the value of the HTTP `Retry-After` response header (RFC 9110
/// §10.2.3). The header takes one of two forms:
///
/// - **delta-seconds** (e.g. `Retry-After: 120`): integer seconds the
///   client should wait before the next request.
/// - **HTTP-date** (e.g. `Retry-After: Wed, 21 Oct 2026 07:28:00 GMT`):
///   absolute moment after which the client may retry. Converted to a
///   duration relative to *now*; if the date is in the past we return
///   `Some(Duration::ZERO)` so the caller can treat it as "no delay
///   required" rather than ignoring the hint.
///
/// Returns `None` if the value is missing, malformed, or represents
/// a non-positive delta. Whitespace around the value is tolerated;
/// case-insensitive month / weekday names per the RFC.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(secs) = trimmed.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    // HTTP-date form. RFC 9110 §5.6.7 defines IMF-fixdate as the
    // preferred form; we also accept RFC 850 + asctime() since RFC
    // 9110 still requires recipients to parse them.
    let when = httpdate::parse_http_date(trimmed).ok()?;
    let now = SystemTime::now();
    Some(when.duration_since(now).unwrap_or(Duration::ZERO))
}

/// Convenience: pull the `Retry-After` header out of a response's
/// header map and parse it. Header names are matched case-insensitively
/// because the wire form varies. Returns `None` if absent or unparsable.
pub fn extract_retry_after(headers: &HashMap<String, String>) -> Option<Duration> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
        .and_then(|(_, value)| parse_retry_after(value))
}
