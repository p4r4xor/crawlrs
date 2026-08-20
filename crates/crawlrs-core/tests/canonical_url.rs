//! Tests for `CanonicalUrl` parsing, canonicalization, and length cap.

use std::collections::HashSet;

use crawlrs_core::CanonicalUrl;
use crawlrs_core::url::{MAX_URL_LEN, UrlError};

/// Helper used by the bulk-canonicalization tests below. Existing
/// tests in this file use `CanonicalUrl::parse(...).as_str()` inline;
/// `canon()` is the short form for the dense rule-by-rule cases.
fn canon(s: &str) -> String {
    CanonicalUrl::parse(s).unwrap().as_str().to_owned()
}

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

// Rules we own (canonicalization layered on top of the url crate).

#[test]
fn strips_utm_and_other_tracking_params() {
    assert_eq!(
        canon("http://e.test/?utm_source=x&q=hi&gclid=abc&fbclid=y&ref=foo"),
        "http://e.test/?q=hi",
    );
}

#[test]
fn preserves_trailing_slash_on_non_root_path() {
    // Trailing slashes on directory-style paths are kept so
    // `parse_relative` resolves correctly: a base of `/blog/`
    // with href `post-1` must yield `/blog/post-1`, not `/post-1`.
    // Servers also routinely distinguish `/page` from `/page/`
    // (mod_dir 301, REST list-vs-collection routes); collapsing
    // them in our canonical form would silently drop coverage.
    assert_eq!(canon("http://e.test/page/"), "http://e.test/page/");
}

#[test]
fn preserves_root_slash() {
    assert_eq!(canon("http://e.test/"), "http://e.test/");
}

#[test]
fn strips_trailing_dot_on_hostname() {
    assert_eq!(canon("http://e.test./page"), "http://e.test/page");
}

#[test]
fn trailing_dot_strip_preserves_port() {
    assert_eq!(canon("http://e.test.:8080/x"), "http://e.test:8080/x");
}

#[test]
fn collapses_duplicate_slashes_in_path() {
    assert_eq!(canon("http://e.test/a//b///c"), "http://e.test/a/b/c");
}

#[test]
fn collapses_leading_double_slash_in_path() {
    assert_eq!(canon("http://e.test//x"), "http://e.test/x");
}

#[test]
fn preserves_embedded_scheme_double_slash_in_path() {
    // Proxy-style URLs that carry another URL in the path keep
    // their `://`. Without this carveout we'd corrupt the embedded
    // URL into a single slash.
    assert_eq!(
        canon("http://e.test/proxy/https://other.test/x"),
        "http://e.test/proxy/https://other.test/x",
    );
}

#[test]
fn strips_userinfo_username_and_password() {
    assert_eq!(canon("http://user:pass@e.test/page"), "http://e.test/page");
}

#[test]
fn strips_userinfo_username_only() {
    assert_eq!(canon("http://user@e.test/page"), "http://e.test/page");
}

// Behaviors the `url` crate handles for us; locked here as
// regression tests so an upstream change surfaces immediately.

#[test]
fn url_crate_lowercases_scheme() {
    assert_eq!(canon("HTTPS://e.test/x"), "https://e.test/x");
}

#[test]
fn url_crate_normalizes_empty_path_to_root_slash() {
    assert_eq!(canon("http://e.test"), "http://e.test/");
}

#[test]
fn url_crate_strips_default_port_http() {
    assert_eq!(canon("http://e.test:80/x"), "http://e.test/x");
}

#[test]
fn url_crate_strips_default_port_https() {
    assert_eq!(canon("https://e.test:443/x"), "https://e.test/x");
}

#[test]
fn url_crate_keeps_non_default_port() {
    assert_eq!(canon("http://e.test:8080/x"), "http://e.test:8080/x");
}

#[test]
fn url_crate_treats_backslash_as_forward_slash_in_path() {
    // WHATWG URL spec. With our `//` collapse running after, the
    // resulting double slash gets collapsed too.
    assert_eq!(canon("http://e.test/a\\b"), "http://e.test/a/b");
}

#[test]
fn decodes_unreserved_percent_encoded_octets_in_path() {
    // RFC 3986 section 6.2.2.2 mandates this for normalization;
    // the upstream `url` crate doesn't do it, so we do. `%41` is
    // `A` (unreserved per RFC 3986 section 2.3).
    assert_eq!(canon("http://e.test/%41"), "http://e.test/A");
}

#[test]
fn decodes_unreserved_lowercase_hex_too() {
    // Hex digits are case-insensitive in `%XX`.
    assert_eq!(canon("http://e.test/%66%6f%6f"), "http://e.test/foo");
}

#[test]
fn preserves_reserved_percent_encoded_octets_in_path() {
    // `%2F` is `/`, a reserved character. Decoding it would
    // change path-segment semantics, so the encoded form stays.
    assert_eq!(canon("http://e.test/a%2Fb"), "http://e.test/a%2Fb");
}

#[test]
fn passes_through_invalid_percent_escapes_in_path() {
    // `%ZZ` is not valid hex; leave alone. (The `url` crate
    // percent-encodes the `%` itself, so we observe `%25ZZ` here.)
    let canonical = canon("http://e.test/%ZZ");
    assert!(
        canonical.contains("ZZ"),
        "invalid escape should be preserved verbatim somewhere; got {canonical}",
    );
}

#[test]
fn all_rules_compose() {
    // Trailing slash on `/Sub/` is preserved (see
    // `preserves_trailing_slash_on_non_root_path` for why);
    // every other rule still applies.
    assert_eq!(
        canon("HTTPS://USER:PASS@E.test.:443//Path///Sub/?utm_source=x&b=2&a=1#frag"),
        "https://e.test/Path/Sub/?a=1&b=2",
    );
}
