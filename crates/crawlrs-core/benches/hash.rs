//! Hashing microbench.
//!
//! Two functions, two regimes:
//!
//! 1. `fnv1a_64` over short inputs (host strings ~5-30 bytes). Hot
//!    in the frontier's shard-key derivation; called once per URL
//!    submit and once per claim. Pure CPU, no allocation.
//!
//! 2. `content_hash` over body-shaped inputs (1 KB / 16 KB / 256 KB).
//!    Called once per stored document on the store-write path. Backed
//!    by blake3; allocation pattern matters less than steady-state
//!    throughput per byte.
//!
//! Throughput reported per byte for `content_hash` (so different
//! input sizes are comparable), per element for `fnv1a_64`.

use std::hint::black_box;

use crawlrs_core::{content_hash, fnv1a_64};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const DOMAIN_CORPUS: &str = include_str!("fixtures/seeds.txt");

fn bench_fnv1a_64(c: &mut Criterion) {
    let hosts: Vec<&str> = DOMAIN_CORPUS.lines().filter(|d| !d.is_empty()).collect();
    let mut group = c.benchmark_group("fnv1a_64");
    group.throughput(Throughput::Elements(hosts.len() as u64));
    group.bench_function("hosts", |b| {
        b.iter(|| {
            for host in &hosts {
                let _ = black_box(fnv1a_64(black_box(host.as_bytes())));
            }
        });
    });
    group.finish();
}

fn bench_content_hash(c: &mut Criterion) {
    // Three sizes spanning the body-size distribution we see in the
    // wild: small JSON / API responses, mid-size HTML, large HTML
    // (long-form articles, search-result pages).
    let sizes = [("1KB", 1024), ("16KB", 16 * 1024), ("256KB", 256 * 1024)];
    let mut group = c.benchmark_group("content_hash");
    for (label, size) in sizes {
        // Pseudorandom-ish bytes via a simple LCG so the input is
        // reproducible and not all-zeros (avoids unrealistic blake3
        // shortcuts on uniform inputs).
        let mut buf = Vec::with_capacity(size);
        let mut state: u32 = 0x12345678;
        for _ in 0..size {
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            buf.push((state >> 16) as u8);
        }
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(label, |b| {
            b.iter(|| {
                let _ = black_box(content_hash(black_box(&buf)));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_fnv1a_64, bench_content_hash);
criterion_main!(benches);
