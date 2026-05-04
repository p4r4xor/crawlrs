//! Tests for `CanonicalUrl` parsing, canonicalization, and length cap.

use std::collections::HashSet;

use crawlrs_core::CanonicalUrl;
use crawlrs_core::url::{MAX_URL_LEN, UrlError};

#[test]
fn parses_simple_url() {
    let url = CanonicalUrl::parse("https://example.com/foo").unwrap();
    assert_eq!(url.host(), Some("example.com"));
    assert_eq!(url.scheme(), "https");
}

#[test]
fn rejects_garbage() {
    assert!(CanonicalUrl::parse("not a url").is_err());
}

#[test]
fn rejects_overlong_url() {
    let long_path = "a".repeat(MAX_URL_LEN);
    let url = format!("https://example.com/{long_path}");
    let err = CanonicalUrl::parse(&url).unwrap_err();
    assert!(matches!(err, UrlError::TooLong { .. }), "got {err:?}");
}

#[test]
fn rejects_overlong_relative_resolution() {
    let base = CanonicalUrl::parse("https://example.com/").unwrap();
    let long_href = "a".repeat(MAX_URL_LEN);
    let err = CanonicalUrl::parse_relative(&base, &long_href).unwrap_err();
    assert!(matches!(err, UrlError::TooLong { .. }), "got {err:?}");
}

#[test]
fn equal_urls_hash_equal() {
    let mut set = HashSet::new();
    set.insert(CanonicalUrl::parse("https://example.com/").unwrap());
    assert!(set.contains(&CanonicalUrl::parse("https://example.com/").unwrap()));
}

#[test]
fn host_is_lowercased() {
    let url = CanonicalUrl::parse("https://Example.COM/page").unwrap();
    assert_eq!(url.host(), Some("example.com"));
}

#[test]
fn fragment_is_dropped() {
    let url = CanonicalUrl::parse("https://example.com/page#section").unwrap();
    assert_eq!(url.as_str(), "https://example.com/page");
}

#[test]
fn utm_params_stripped() {
    let url =
        CanonicalUrl::parse("https://example.com/?utm_source=foo&utm_medium=bar&kept=yes").unwrap();
    assert_eq!(url.as_str(), "https://example.com/?kept=yes");
}

#[test]
fn other_tracking_params_stripped() {
    let url = CanonicalUrl::parse("https://example.com/?gclid=x&fbclid=y&ref=z&ref_src=w&kept=yes")
        .unwrap();
    assert_eq!(url.as_str(), "https://example.com/?kept=yes");
}

#[test]
fn all_tracking_strips_to_no_query() {
    let url = CanonicalUrl::parse("https://example.com/?utm_source=foo&gclid=x").unwrap();
    assert_eq!(url.as_str(), "https://example.com/");
}

#[test]
fn query_params_sorted() {
    let a = CanonicalUrl::parse("https://example.com/?b=2&a=1").unwrap();
    let b = CanonicalUrl::parse("https://example.com/?a=1&b=2").unwrap();
    assert_eq!(a, b);
    assert_eq!(a.as_str(), "https://example.com/?a=1&b=2");
}

#[test]
fn parse_relative_resolves_path() {
    let base = CanonicalUrl::parse("https://example.com/blog/post").unwrap();
    let resolved = CanonicalUrl::parse_relative(&base, "/about").unwrap();
    assert_eq!(resolved.as_str(), "https://example.com/about");
}

#[test]
fn parse_relative_resolves_relative_path() {
    let base = CanonicalUrl::parse("https://example.com/blog/").unwrap();
    let resolved = CanonicalUrl::parse_relative(&base, "post-1").unwrap();
    assert_eq!(resolved.as_str(), "https://example.com/blog/post-1");
}

#[test]
fn parse_relative_passes_through_absolute() {
    let base = CanonicalUrl::parse("https://a.com/").unwrap();
    let resolved = CanonicalUrl::parse_relative(&base, "https://b.com/page").unwrap();
    assert_eq!(resolved.as_str(), "https://b.com/page");
}

#[test]
fn parse_relative_canonicalizes_result() {
    let base = CanonicalUrl::parse("https://example.com/").unwrap();
    let resolved = CanonicalUrl::parse_relative(&base, "/page?utm_source=x&kept=1#frag").unwrap();
    assert_eq!(resolved.as_str(), "https://example.com/page?kept=1");
}

#[test]
fn is_http_accepts_http_and_https() {
    assert!(CanonicalUrl::parse("http://example.com").unwrap().is_http());
    assert!(
        CanonicalUrl::parse("https://example.com")
            .unwrap()
            .is_http()
    );
}

#[test]
fn is_http_rejects_other_schemes() {
    assert!(!CanonicalUrl::parse("mailto:foo@bar.com").unwrap().is_http());
    assert!(
        !CanonicalUrl::parse("javascript:alert(1)")
            .unwrap()
            .is_http()
    );
    assert!(
        !CanonicalUrl::parse("ftp://example.com/file")
            .unwrap()
            .is_http()
    );
    assert!(!CanonicalUrl::parse("tel:+15551234567").unwrap().is_http());
}

#[test]
fn fragment_only_difference_collapses() {
    let a = CanonicalUrl::parse("https://example.com/page#a").unwrap();
    let b = CanonicalUrl::parse("https://example.com/page#b").unwrap();
    assert_eq!(a, b);
}
