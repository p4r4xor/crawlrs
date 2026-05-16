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
/// payload, and per-row metadata storage. 2 KiB is comfortably above
/// the 99.99th percentile of URLs in the wild and matches common
/// industry practice for crawlers.
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
    // Inline because: visibility-forced. These tests exercise
    // `collapse_path_slashes`, a private free function that
    // `tests/*.rs` (compiled as a separate crate) cannot reach.
    // The public-API canonicalization tests live in
    // `tests/canonical_url.rs`.

    use super::*;

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
