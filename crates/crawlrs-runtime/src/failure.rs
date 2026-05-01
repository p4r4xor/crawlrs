//! Mapping from concrete fetch outcomes to [`FailureKind`] categories.
//!
//! The runtime calls these at the boundary between `Fetcher::fetch`
//! returning and `Politeness::record_failure` being invoked, so the
//! politeness layer sees the *category* of failure (and applies the
//! right backoff), not the underlying error message.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_codes_are_not_failures() {
        for code in [200, 201, 204, 301, 302, 304] {
            assert_eq!(classify_status(code), None, "status {code} should not be a failure");
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
}
