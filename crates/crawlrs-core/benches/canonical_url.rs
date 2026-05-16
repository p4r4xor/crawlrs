//! URL canonicalization microbench.
//!
//! Measures `CanonicalUrl::parse` throughput against a representative
//! domain corpus (every 1000th domain from Cloudflare Radar's top-1M
//! list, sampled deterministically so re-runs produce identical
//! results across machines).
//!
//! For each domain we synthesize a small mix of URL shapes that
//! exercise the canonicalizer's hot paths:
//!
//! - `https://<domain>/` (root, no path work)
//! - `https://<domain>/path/with/segments/` (path normalization)
//! - `https://<domain>/page?utm_source=x&utm_medium=y&id=42` (query
//!   filtering: tracking-param strip + sort)
//! - `https://<domain>/p%61th` (percent-octet decoding of unreserved
//!   bytes)
//!
//! Throughput is reported in URLs/sec via Criterion's element-count
//! mode. Compare across Cargo profile knobs by running:
//!
//!     cargo bench --bench canonical_url -- --save-baseline before
//!     # change profile
//!     cargo bench --bench canonical_url -- --baseline before
//!
//! The HTML report at `target/criterion/report/index.html` shows the
//! deltas with statistical-significance markers.

use std::hint::black_box;

use crawlrs_core::CanonicalUrl;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const DOMAIN_CORPUS: &str = include_str!("fixtures/seeds.txt");

/// Expand each domain into 4 URL shapes; returns a flat Vec of fully
/// rendered URL strings ready for `CanonicalUrl::parse`. Run once
/// outside the timed loop.
fn build_corpus() -> Vec<String> {
    let domains: Vec<&str> = DOMAIN_CORPUS.lines().filter(|d| !d.is_empty()).collect();
    let mut urls = Vec::with_capacity(domains.len() * 4);
    for domain in &domains {
        urls.push(format!("https://{domain}/"));
        urls.push(format!("https://{domain}/path/with/segments/"));
        urls.push(format!(
            "https://{domain}/page?utm_source=x&utm_medium=y&id=42"
        ));
        urls.push(format!("https://{domain}/p%61th"));
    }
    urls
}

fn bench_parse(c: &mut Criterion) {
    let corpus = build_corpus();
    let mut group = c.benchmark_group("CanonicalUrl::parse");
    group.throughput(Throughput::Elements(corpus.len() as u64));
    group.bench_function("mixed_shapes", |b| {
        b.iter(|| {
            for url in &corpus {
                let _ = black_box(CanonicalUrl::parse(black_box(url)));
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
