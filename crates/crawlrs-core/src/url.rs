//! URL types.
//!
//! [`CanonicalUrl`] is a newtype wrapper around [`url::Url`] that has been
//! through the crate's canonicalization rules. Two URLs that *resolve to the
//! same resource* compare equal; that's what makes the type useful as a key
//! in dedup sets, frontier hash tables, and bloom filters.
//!
//! Canonicalization rules (applied on every construction):
//!
//! - Host is lowercased (the [`url`] crate does this for ASCII; IDN is
//!   punycode-encoded, which is also normalized).
//! - Default ports are stripped (`url` crate handles this on serialization).
//! - The fragment is dropped (`#section` is in-page, not a different
//!   crawl target).
//! - Tracking query parameters are stripped (`utm_*`, `gclid`, `fbclid`,
//!   `ref`, `ref_src`), since they identify the *referrer*, not the resource.
//! - Remaining query parameters are sorted alphabetically so that
//!   `?a=1&b=2` and `?b=2&a=1` hash identically.
//!
//! [`is_http`](Self::is_http) is a separate predicate; `mailto:` and
//! `javascript:` parse fine as URLs but aren't crawlable. Callers (the
//! parser, the frontier) gate on `is_http()`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalUrl(::url::Url);

impl CanonicalUrl {
    /// Parse an absolute URL string and apply canonicalization.
    pub fn parse(input: &str) -> Result<Self, ::url::ParseError> {
        let parsed = ::url::Url::parse(input)?;
        Ok(Self(canonicalize(parsed)))
    }

    /// Resolve `href` (which may be relative) against `base`, then
    /// canonicalize. This is what the parser uses on `<a href>` values.
    pub fn parse_relative(base: &Self, href: &str) -> Result<Self, ::url::ParseError> {
        let resolved = base.0.join(href)?;
        Ok(Self(canonicalize(resolved)))
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

/// Drop the fragment, strip tracking params, sort the remaining query.
///
/// Host casing is already handled by [`url::Url::parse`] (ASCII hosts are
/// lowercased; IDN is punycode-encoded). Default ports are dropped on
/// serialization. So we only need to fix what the `url` crate doesn't.
fn canonicalize(mut url: ::url::Url) -> ::url::Url {
    url.set_fragment(None);

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

/// Tracking query parameters that identify the *referrer*, not the
/// resource itself. Stripped during canonicalization so URLs that
/// differ only in tracking attribution dedup as one.
fn is_tracking_param(name: &str) -> bool {
    if name.starts_with("utm_") {
        return true;
    }
    matches!(name, "gclid" | "fbclid" | "ref" | "ref_src")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

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
        let url = CanonicalUrl::parse(
            "https://example.com/?utm_source=foo&utm_medium=bar&kept=yes",
        )
        .unwrap();
        assert_eq!(url.as_str(), "https://example.com/?kept=yes");
    }

    #[test]
    fn other_tracking_params_stripped() {
        let url = CanonicalUrl::parse(
            "https://example.com/?gclid=x&fbclid=y&ref=z&ref_src=w&kept=yes",
        )
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
        let resolved =
            CanonicalUrl::parse_relative(&base, "/page?utm_source=x&kept=1#frag").unwrap();
        assert_eq!(resolved.as_str(), "https://example.com/page?kept=1");
    }

    #[test]
    fn is_http_accepts_http_and_https() {
        assert!(CanonicalUrl::parse("http://example.com").unwrap().is_http());
        assert!(CanonicalUrl::parse("https://example.com").unwrap().is_http());
    }

    #[test]
    fn is_http_rejects_other_schemes() {
        assert!(!CanonicalUrl::parse("mailto:foo@bar.com").unwrap().is_http());
        assert!(!CanonicalUrl::parse("javascript:alert(1)").unwrap().is_http());
        assert!(!CanonicalUrl::parse("ftp://example.com/file").unwrap().is_http());
        assert!(!CanonicalUrl::parse("tel:+15551234567").unwrap().is_http());
    }

    #[test]
    fn fragment_only_difference_collapses() {
        let a = CanonicalUrl::parse("https://example.com/page#a").unwrap();
        let b = CanonicalUrl::parse("https://example.com/page#b").unwrap();
        assert_eq!(a, b);
    }
}
