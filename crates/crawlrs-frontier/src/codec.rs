//! Binary serialisation for `UrlEntry` payloads on the wire.
//!
//! `postcard` is the default binary encoding for crawlrs. Smaller than
//! JSON, faster to (de)serialise, varint-packed for compactness on small
//! integers (depth, etc.). Spec is stable.
//!
//! The frontier stores each encoded `UrlEntry` as the value of the
//! per-shard URL HASH (`urls:s<N>`) keyed by `url_id`. Claim
//! materialises it; ack deletes it.

use crawlrs_core::{Error, Result, UrlEntry};

pub fn encode(entry: &UrlEntry) -> Result<Vec<u8>> {
    postcard::to_allocvec(entry).map_err(|e| Error::Frontier(format!("postcard encode: {e}")))
}

pub fn decode(bytes: &[u8]) -> Result<UrlEntry> {
    postcard::from_bytes(bytes).map_err(|e| Error::Frontier(format!("postcard decode: {e}")))
}

// Inline because: `encode` and `decode` are intentionally `pub(crate)`
// - they're the postcard wire format for `UrlEntry`, an
// implementation detail of how this crate stores items in Redis
// Streams. Promoting them to `pub` to satisfy a `tests/` integration
// crate would commit the public API to postcard forever; we'd rather
// keep the option to swap encodings in a future ADR.
#[cfg(test)]
mod tests {
    use super::*;
    use crawlrs_core::CanonicalUrl;

    fn entry() -> UrlEntry {
        UrlEntry::seed(CanonicalUrl::parse("https://example.test/page").unwrap())
    }

    #[test]
    fn roundtrip() {
        let encoded = encode(&entry()).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.url.as_str(), "https://example.test/page");
        assert_eq!(decoded.depth, 0);
    }

    #[test]
    fn encoded_is_compact() {
        // A simple seed URL should fit in well under 100 bytes after
        // postcard encoding. Locks against accidental encoding-format
        // regressions (e.g. someone swaps in JSON).
        let bytes = encode(&entry()).unwrap();
        assert!(
            bytes.len() < 100,
            "expected compact encoding; got {} bytes",
            bytes.len()
        );
    }
}
