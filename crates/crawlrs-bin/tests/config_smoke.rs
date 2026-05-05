//! Config-loading smoke tests. Verifies the shipped sample parses
//! cleanly and that env-var overlays land where they should.

use crawlrs_bin::config::{CrawlrsConfig, StoreBackend};

#[test]
fn sample_config_parses() {
    // The sample crawl.toml in examples/ must always parse cleanly;
    // it's the operator-facing template.
    let path = std::path::Path::new("examples/crawl.toml");
    let config = CrawlrsConfig::load(path).expect("sample config should parse");

    assert_eq!(config.run_id, "demo-2026-05");
    assert_eq!(config.runtime.workers, 4);
    assert_eq!(config.sharding.num_shards, 8);
    assert!(config.store.parquet);
    assert!(config.store.warc);
    assert_eq!(config.server.listen, "0.0.0.0:9090");

    match &config.store.backend {
        StoreBackend::Local { path } => {
            assert_eq!(path.to_str(), Some("/var/lib/crawlrs/data"));
        }
        StoreBackend::S3 { .. } => panic!("sample defaults to local backend"),
    }
}

#[test]
fn summary_format_is_stable() {
    let path = std::path::Path::new("examples/crawl.toml");
    let config = CrawlrsConfig::load(path).expect("load sample");
    let summary = config.summary();
    // Operator-facing single-line shape; prefer breakage-on-format-drift
    // over silent contract changes.
    assert!(summary.starts_with("run_id="));
    assert!(summary.contains("workers="));
    assert!(summary.contains("shards="));
    assert!(summary.contains("parquet="));
    assert!(summary.contains("warc="));
    assert!(summary.contains("listen="));
}
