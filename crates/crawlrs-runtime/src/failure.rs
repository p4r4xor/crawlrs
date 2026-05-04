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
/// 2xx and 3xx are success-shaped. 404 / 410 are missing-resource
/// shapes that are usually a frontier or seed problem rather than
/// a politeness one; we surface them as `Other` so they're logged
/// but don't trigger per-host backoff on their own. 429 / 503 get
/// the dedicated rate-limit backoff. 5xx (other than 503) are
/// `Other` until we have a use case to distinguish.
pub fn classify_status(status: u16) -> Option<FailureKind> {
    match status {
        200..=399 => None,
        429 => Some(FailureKind::TooManyRequests),
        503 => Some(FailureKind::ServiceUnavailable),
        404 | 410 => Some(FailureKind::Other),
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
pub fn classify_transport_error(err: &Error) -> FailureKind {
    let text = err.to_string().to_ascii_lowercase();
    if text.contains("timeout") || text.contains("timed out") {
        FailureKind::Timeout
    } else if text.contains("connection reset")
        || text.contains("broken pipe")
        || text.contains("connection refused")
    {
        FailureKind::ConnectReset
    } else {
        FailureKind::Other
    }
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
/// because the wire form varies. Returns `None` if absent or unparseable.
pub fn extract_retry_after(headers: &HashMap<String, String>) -> Option<Duration> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
        .and_then(|(_, value)| parse_retry_after(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_codes_are_not_failures() {
        for code in [200, 201, 204, 301, 302, 304] {
            assert_eq!(
                classify_status(code),
                None,
                "status {code} should not be a failure"
            );
        }
    }

    #[test]
    fn rate_limit_codes_map_explicitly() {
        assert_eq!(classify_status(429), Some(FailureKind::TooManyRequests));
        assert_eq!(classify_status(503), Some(FailureKind::ServiceUnavailable));
    }

    #[test]
    fn other_codes_map_to_other_kind() {
        for code in [404, 410, 500, 502] {
            assert_eq!(classify_status(code), Some(FailureKind::Other));
        }
    }

    #[test]
    fn transport_errors_match_on_keywords() {
        assert_eq!(
            classify_transport_error(&Error::Fetch("request timed out".into())),
            FailureKind::Timeout,
        );
        assert_eq!(
            classify_transport_error(&Error::Fetch("connection reset by peer".into())),
            FailureKind::ConnectReset,
        );
        assert_eq!(
            classify_transport_error(&Error::Fetch("certificate verification failed".into())),
            FailureKind::Other,
        );
    }

    #[test]
    fn retry_after_parses_delta_seconds() {
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after("0"), Some(Duration::ZERO));
        assert_eq!(parse_retry_after("  30  "), Some(Duration::from_secs(30)));
    }

    #[test]
    fn retry_after_parses_imf_fixdate() {
        // A date in the future: parsed and converted to a positive
        // duration. The exact magnitude depends on wall-clock; assert
        // we at least got Some.
        let date = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(60));
        let parsed = parse_retry_after(&date).expect("imf-fixdate parses");
        assert!(
            parsed.as_secs() > 50 && parsed.as_secs() < 70,
            "got {parsed:?}"
        );
    }

    #[test]
    fn retry_after_past_date_returns_zero() {
        let date = httpdate::fmt_http_date(SystemTime::now() - Duration::from_secs(3600));
        assert_eq!(parse_retry_after(&date), Some(Duration::ZERO));
    }

    #[test]
    fn retry_after_rejects_garbage() {
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("not a date"), None);
        assert_eq!(parse_retry_after("-5"), None); // negative parses as i64 not u64
    }

    #[test]
    fn extract_retry_after_is_case_insensitive() {
        let mut h = HashMap::new();
        h.insert("Retry-After".into(), "60".into());
        assert_eq!(extract_retry_after(&h), Some(Duration::from_secs(60)));

        let mut h = HashMap::new();
        h.insert("retry-after".into(), "60".into());
        assert_eq!(extract_retry_after(&h), Some(Duration::from_secs(60)));

        let mut h = HashMap::new();
        h.insert("RETRY-AFTER".into(), "60".into());
        assert_eq!(extract_retry_after(&h), Some(Duration::from_secs(60)));
    }

    #[test]
    fn extract_retry_after_returns_none_when_absent() {
        let h = HashMap::new();
        assert_eq!(extract_retry_after(&h), None);
    }
}
