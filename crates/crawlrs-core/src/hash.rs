//! Stable, dependency-free non-cryptographic hashing.
//!
//! FNV-1a 64-bit. Used for shard routing (`HostHashShardPolicy`) and
//! content fingerprinting ([`content_hash`]). Stability matters more
//! than collision resistance: two runs of the crawler against the
//! same input must produce the same value, across crate versions and
//! across processes.

/// FNV-1a 64-bit hash. Deterministic, no per-process seed, no
/// allocation, no dependency.
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

/// Compute the content hash recorded in
/// [`UrlMetadata::content_hash`](crate::types::UrlMetadata::content_hash).
/// Wraps [`fnv1a_64`] so callers don't need to know the underlying
/// hash function.
pub fn content_hash(body: &[u8]) -> u64 {
    fnv1a_64(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_known_vectors() {
        // FNV-1a 64-bit reference values from
        // http://www.isthe.com/chongo/tech/comp/fnv/index.html
        // Locking these prevents silent algorithm drift.
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn content_hash_is_stable() {
        // Same body must produce the same value across calls.
        let body = b"the quick brown fox jumps over the lazy dog";
        let first = content_hash(body);
        for _ in 0..100 {
            assert_eq!(content_hash(body), first);
        }
    }
}
