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
