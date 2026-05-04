//! Tests for the public hash helpers (`fnv1a_64`, `content_hash`).

use crawlrs_core::{content_hash, fnv1a_64};

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
