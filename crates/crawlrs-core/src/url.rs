//! URL types.
//!
//! [`CanonicalUrl`] is a newtype wrapper around [`url::Url`] that has been
//! through the crate's canonicalization rules. Two URLs that *resolve to the
//! same resource* compare equal; that's what makes the type useful as a key
//! in dedup sets, frontier hash tables, and bloom filters.
//!
//! Canonicalization rules (applied on every construction). The first
//! group is handled for us by the upstream [`url`] crate's WHATWG /
//! RFC 3986 normalization; we lock those behaviors with regression
//! tests rather than re-implementing them:
//!
//! - Scheme is lowercased (`HTTPS://` -> `https://`).
//! - Host is lowercased for ASCII; IDN labels are punycode-encoded.
//! - Backslashes in the path are coerced to forward slashes
//!   (WHATWG URL spec).
//! - Default ports are stripped on serialization (`:443` on `https`,
//!   `:80` on `http`).
//! - Unreserved percent-encoded octets in the path are decoded to
//!   their literal form (`/%41` -> `/A`).
//! - An empty path is normalized to `/` so that `http://e.test` and
//!   `http://e.test/` produce the same serialized form.
//!
//! On top of those we layer the rules this crate cares about:
//!
//! - Fragment is dropped (`#section` is in-page, not a separate
//!   crawl target).
//! - User-info (`user:pass@`) is stripped. Auth-in-URL shouldn't be
//!   a crawl target; if it appears in HTML it's almost certainly
//!   accidental.
//! - Trailing dot on the hostname is stripped (`e.test.` -> `e.test`);
//!   FQDN form resolves DNS-equivalently to the bare form.
//! - Runs of slashes in the path are collapsed (`/a//b` -> `/a/b`),
//!   preserving any embedded scheme (a path like `/proxy/https://other.test/`
//!   keeps its `://` intact).
//! - Trailing slash on non-root paths is dropped (`/page/` -> `/page`,
//!   but `/` stays).
//! - Tracking query parameters are stripped (`utm_*`, `gclid`,
//!   `fbclid`, `ref`, `ref_src`); they identify the *referrer*, not
//!   the resource.
//! - Remaining query parameters are sorted alphabetically so that
//!   `?a=1&b=2` and `?b=2&a=1` hash identically.
//!
//! [`is_http`](Self::is_http) is a separate predicate; `mailto:` and
//! `javascript:` parse fine as URLs but aren't crawlable. Callers (the
//! parser, the frontier) gate on `is_http()`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum accepted URL length, in bytes. Defends against adversarial
/// inputs that would otherwise inflate the seen-set, the queue body
/// payload, and per-row metadata storage. 2 KiB matches industry
/// practice (Heritrix, common search engines) and is comfortably above
/// the 99.99th percentile of URLs in the wild.
pub const MAX_URL_LEN: usize = 2048;

/// Error type for [`CanonicalUrl::parse`] / [`CanonicalUrl::parse_relative`].
/// Wraps the underlying `url::ParseError` and adds a length-rejected
/// variant that the upstream crate doesn't model.
#[derive(Debug, Error)]
pub enum UrlError {
    #[error("url length {len} exceeds cap of {cap} bytes")]
    TooLong { len: usize, cap: usize },

    #[error("invalid url: {0}")]
    Parse(#[from] ::url::ParseError),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalUrl(::url::Url);

impl CanonicalUrl {
    /// Parse an absolute URL string and apply canonicalization. Rejects
    /// inputs longer than [`MAX_URL_LEN`] before parsing.
    pub fn parse(input: &str) -> Result<Self, UrlError> {
        if input.len() > MAX_URL_LEN {
            return Err(UrlError::TooLong {
                len: input.len(),
                cap: MAX_URL_LEN,
            });
        }
        let parsed = ::url::Url::parse(input)?;
        Ok(Self(canonicalize(parsed)))
    }

    /// Resolve `href` (which may be relative) against `base`, then
    /// canonicalize. This is what the parser uses on `<a href>` values.
    /// The resolved absolute URL is checked against [`MAX_URL_LEN`]
    /// post-resolution, since `href` alone is often shorter than the
    /// final absolute URL.
    pub fn parse_relative(base: &Self, href: &str) -> Result<Self, UrlError> {
        let resolved = base.0.join(href)?;
        let canonicalized = canonicalize(resolved);
        let serialized_len = canonicalized.as_str().len();
        if serialized_len > MAX_URL_LEN {
            return Err(UrlError::TooLong {
                len: serialized_len,
                cap: MAX_URL_LEN,
            });
        }
        Ok(Self(canonicalized))
    }

    /// True if the scheme is `http` or `https`, i.e. crawlable.
    /// Returns false for `mailto:`, `javascript:`, `tel:`, `data:`, `ftp:`, etc.
    pub fn is_http(&self) -> bool {
        matches!(self.0.scheme(), "http" | "https")
    }

    pub fn host(&self) -> Option<&str> {
        self.0.host_str()
    }

    pub fn scheme(&self) -> &str {
        self.0.scheme()
    }

    pub fn as_url(&self) -> &::url::Url {
        &self.0
    }

    pub fn into_url(self) -> ::url::Url {
        self.0
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Display for CanonicalUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// Apply the canonicalization rules documented at module level. The
/// upstream [`url`] crate handles scheme / host / port / WHATWG
/// percent-encoding normalization on parse; this function only
/// applies the dedup-oriented rules on top of that.
fn canonicalize(mut url: ::url::Url) -> ::url::Url {
    url.set_fragment(None);

    // `set_username` / `set_password` return Err only for cannot-be-
    // a-base URLs (mailto, etc.); we don't care since those aren't
    // crawlable anyway.
    let _ = url.set_username("");
    let _ = url.set_password(None);

    // Trailing-dot FQDN form resolves DNS-equivalently. Skip if the
    // host is just `.` (won't happen with a valid URL, but defensive).
    if let Some(host) = url.host_str()
        && host.ends_with('.')
        && host.len() > 1
    {
        let trimmed = host.trim_end_matches('.').to_owned();
        let _ = url.set_host(Some(&trimmed));
    }

    // Collapse `//`+ runs in the path, preserving any embedded scheme
    // (a path like `/proxy/https://other.test/x` keeps its `://`).
    let collapsed = collapse_path_slashes(url.path());
    if collapsed.as_str() != url.path() {
        url.set_path(&collapsed);
    }

    // Decode `%XX` sequences whose byte is an RFC 3986 *unreserved*
    // character (ALPHA / DIGIT / `-` / `.` / `_` / `~`). Reserved
    // characters (like `%2F` for `/`) MUST stay encoded because
    // decoding them would change path-segment semantics.
    let decoded = decode_unreserved_percent_octets(url.path());
    if decoded.as_str() != url.path() {
        url.set_path(&decoded);
    }

    // Strip trailing slash on non-root paths. Root `/` stays so
    // `http://e.test/` doesn't degenerate to `http://e.test`.
    {
        let path = url.path();
        if path.len() > 1 && path.ends_with('/') {
            let trimmed = path.trim_end_matches('/').to_owned();
            url.set_path(&trimmed);
        }
    }

    if url.query().is_some() {
        let mut kept_pairs: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(name, _)| !is_tracking_param(name))
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect();

        kept_pairs.sort();

        if kept_pairs.is_empty() {
            url.set_query(None);
        } else {
            let mut serializer = ::url::form_urlencoded::Serializer::new(String::new());
            for (name, value) in &kept_pairs {
                serializer.append_pair(name, value);
            }
            url.set_query(Some(&serializer.finish()));
        }
    }

    url
}

/// Collapse runs of `/` in `path` while leaving any `://` substring
/// intact. The latter matters for URLs whose path embeds another URL
/// (proxy-style: `/proxy/https://other.test/x`); naive deduplication
/// would corrupt those.
fn collapse_path_slashes(path: &str) -> String {
    if !path.contains("//") {
        return path.to_owned();
    }
    let mut out = String::with_capacity(path.len());
    let mut chunks = path.split("://").peekable();
    while let Some(chunk) = chunks.next() {
        let mut prev_slash = false;
        for c in chunk.chars() {
            if c == '/' {
                if !prev_slash {
                    out.push(c);
                }
                prev_slash = true;
            } else {
                out.push(c);
                prev_slash = false;
            }
        }
        if chunks.peek().is_some() {
            out.push_str("://");
        }
    }
    out
}

/// Tracking query parameters that identify the *referrer*, not the
/// resource itself. Stripped during canonicalization so URLs that
/// differ only in tracking attribution dedup as one.
fn is_tracking_param(name: &str) -> bool {
    if name.starts_with("utm_") {
        return true;
    }
    matches!(name, "gclid" | "fbclid" | "ref" | "ref_src")
}

/// Replace every `%XX` whose byte is an RFC 3986 unreserved character
/// with the character itself. Invalid escapes (non-hex digits, or `%`
/// at the end of the string) are passed through unchanged.
///
/// The `url` crate canonicalises percent-encoding for *reserved*
/// characters but does not decode unreserved ones; this closes that
/// gap so `/foo` and `/%66%6f%6f` dedup as one.
fn decode_unreserved_percent_octets(s: &str) -> String {
    if !s.contains('%') {
        return s.to_owned();
    }
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2]))
        {
            let byte = (hi << 4) | lo;
            if is_rfc3986_unreserved(byte) {
                out.push(byte as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn is_rfc3986_unreserved(byte: u8) -> bool {
    matches!(byte,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canon(s: &str) -> String {
        CanonicalUrl::parse(s).unwrap().as_str().to_owned()
    }

    // -------------------------------------------------------------
    // Rules we own
    // -------------------------------------------------------------

    #[test]
    fn drops_fragment() {
        assert_eq!(canon("http://e.test/x#frag"), "http://e.test/x");
    }

    #[test]
    fn sorts_query_pairs() {
        assert_eq!(canon("http://e.test/?b=2&a=1"), "http://e.test/?a=1&b=2");
    }

    #[test]
    fn strips_utm_and_other_tracking_params() {
        assert_eq!(
            canon("http://e.test/?utm_source=x&q=hi&gclid=abc&fbclid=y&ref=foo"),
            "http://e.test/?q=hi",
        );
    }

    #[test]
    fn empties_query_string_when_all_params_were_tracking() {
        // Should not leave a dangling `?`.
        assert_eq!(canon("http://e.test/x?utm_a=1&utm_b=2"), "http://e.test/x");
    }

    #[test]
    fn strips_trailing_slash_on_non_root_path() {
        assert_eq!(canon("http://e.test/page/"), "http://e.test/page");
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

    // -------------------------------------------------------------
    // Behaviors the `url` crate handles for us; locked here as
    // regression tests so an upstream change surfaces immediately.
    // -------------------------------------------------------------

    #[test]
    fn url_crate_lowercases_scheme() {
        assert_eq!(canon("HTTPS://e.test/x"), "https://e.test/x");
    }

    #[test]
    fn url_crate_lowercases_ascii_host() {
        assert_eq!(canon("http://E.Test/x"), "http://e.test/x");
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

    // -------------------------------------------------------------
    // Compose
    // -------------------------------------------------------------

    #[test]
    fn all_rules_compose() {
        assert_eq!(
            canon("HTTPS://USER:PASS@E.test.:443//Path///Sub/?utm_source=x&b=2&a=1#frag"),
            "https://e.test/Path/Sub?a=1&b=2",
        );
    }

    #[test]
    fn collapse_path_slashes_handles_simple_runs() {
        assert_eq!(collapse_path_slashes("/a//b"), "/a/b");
        assert_eq!(collapse_path_slashes("/a///b////c"), "/a/b/c");
    }

    #[test]
    fn collapse_path_slashes_short_circuits_on_clean_input() {
        // The fast path matters: most paths have no `//` runs.
        assert_eq!(collapse_path_slashes("/a/b/c"), "/a/b/c");
        assert_eq!(collapse_path_slashes("/"), "/");
    }

    #[test]
    fn collapse_path_slashes_preserves_embedded_scheme() {
        assert_eq!(
            collapse_path_slashes("/proxy/https://other.test/x"),
            "/proxy/https://other.test/x",
        );
    }
}
