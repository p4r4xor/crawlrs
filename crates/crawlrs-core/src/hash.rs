//! Stable, dependency-free non-cryptographic hashing.
//!
//! FNV-1a 64-bit. Used for shard routing (`HostHashShardPolicy`) and
//! content fingerprinting ([`content_hash`]). Stability matters more
//! than collision resistance: two runs of the crawler against the
//! same input must produce the same value, across crate versions and
//! across processes.

/// FNV-1a 64-bit hash. Deterministic, no per-process seed, no
/// allocation, no dependency.
#[must_use]
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
#[must_use]
pub fn content_hash(body: &[u8]) -> u64 {
    fnv1a_64(body)
}
