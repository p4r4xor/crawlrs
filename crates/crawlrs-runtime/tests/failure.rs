//! Tests for `classify_status`, `classify_transport_error`,
//! `parse_retry_after`, and `extract_retry_after` - the boundary
//! helpers that turn HTTP responses into `FailureKind` + Retry-After
//! durations.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crawlrs_core::{Error, FailureKind};
use crawlrs_runtime::{
    classify_status, classify_transport_error, extract_retry_after, parse_retry_after,
};

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
fn missing_resource_codes_map_to_not_found() {
    for code in [404, 410] {
        assert_eq!(classify_status(code), Some(FailureKind::NotFound));
    }
}

#[test]
fn other_4xx_codes_map_to_client_error() {
    for code in [400, 401, 403, 451] {
        assert_eq!(classify_status(code), Some(FailureKind::ClientError));
    }
}

#[test]
fn other_5xx_codes_map_to_server_error() {
    for code in [500, 502, 504] {
        assert_eq!(classify_status(code), Some(FailureKind::ServerError));
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
        FailureKind::TlsError,
    );
    assert_eq!(
        classify_transport_error(&Error::Fetch(
            "dns error -> Invalid argument (os error 22)".into()
        )),
        FailureKind::DnsFailure,
    );
    assert_eq!(
        classify_transport_error(&Error::Fetch(
            "dns error -> proto error: io error: Too many open files (os error 24)".into()
        )),
        // ResourceExhausted wins over DnsFailure because the local-side
        // cap is the actionable cause; the DNS wrapper is incidental.
        FailureKind::ResourceExhausted,
    );
    assert_eq!(
        classify_transport_error(&Error::Fetch(
            "tcp connect error -> Network is unreachable (os error 101)".into()
        )),
        FailureKind::Unreachable,
    );
    assert_eq!(
        classify_transport_error(&Error::Fetch("something completely unknown".into())),
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
    // duration. The exact magnitude depends on wall-clock; assert we
    // at least got Some.
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
