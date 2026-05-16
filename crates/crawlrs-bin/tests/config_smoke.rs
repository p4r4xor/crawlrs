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

// ---------------------------------------------------------------------------
// Hard-break migration: keys that previously lived under `[politeness]` now
// live under `[crawl]` (max_depth, max_urls) or `[access]` (blocklist). The
// slimmed `PolitenessConfig` carries `deny_unknown_fields`, so any legacy
// TOML produces a parse-time error that names the offending key.
// ---------------------------------------------------------------------------

fn parse_inline(toml_str: &str) -> anyhow::Result<CrawlrsConfig> {
    // Build a complete config by overlaying `toml_str` on top of the
    // mandatory `run_id` + `[redis]` + `[postgres]` shape. Anything
    // not provided here uses serde's `default`.
    let preamble = r#"
run_id = "legacy-test"

[redis]
url = "redis://localhost:6379"

[postgres]
url = "postgres://crawlrs:crawlrs@localhost/crawlrs"
"#;
    let combined = format!("{preamble}\n{toml_str}\n");
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), combined)?;
    CrawlrsConfig::load(tmp.path())
}

#[test]
fn legacy_politeness_max_depth_rejected() {
    let err = parse_inline(
        r#"
[politeness]
max_depth = 5
"#,
    )
    .expect_err("legacy politeness.max_depth must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("max_depth"),
        "error must name the offending key; got: {msg}",
    );
}

#[test]
fn legacy_politeness_max_urls_rejected() {
    let err = parse_inline(
        r#"
[politeness]
max_urls = 100
"#,
    )
    .expect_err("legacy politeness.max_urls must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("max_urls"),
        "error must name the offending key; got: {msg}",
    );
}

#[test]
fn legacy_politeness_blocklist_rejected() {
    let err = parse_inline(
        r#"
[politeness]
blocklist = ["example.com"]
"#,
    )
    .expect_err("legacy politeness.blocklist must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("blocklist"),
        "error must name the offending key; got: {msg}",
    );
}

#[test]
fn legacy_per_domain_max_depth_rejected() {
    let err = parse_inline(
        r#"
[politeness.per_domain."python.org"]
max_depth = 3
"#,
    )
    .expect_err("legacy politeness.per_domain.<host>.max_depth must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("max_depth"),
        "error must name the offending key; got: {msg}",
    );
}

#[test]
fn unknown_typo_in_politeness_is_also_rejected() {
    // Hard-break has no special-casing: any unknown key in a slimmed
    // struct fails the same way as a legacy one. Operator gets the
    // same serde error message; the rewrite map in this ADR's
    // migration section is the source of truth for which keys moved.
    let err = parse_inline(
        r#"
[politeness]
host_dely = "1s"
"#,
    )
    .expect_err("unknown politeness key must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("host_dely"),
        "error must name the offending key; got: {msg}",
    );
}

#[test]
fn new_shape_with_crawl_and_access_parses() {
    let cfg = parse_inline(
        r#"
[crawl]
max_depth = 2
max_urls  = 100

[crawl.per_domain."python.org"]
max_depth = 1

[access]
blocklist = ["example.com"]

[fetch]
user_agent = "crawlrs-test/0.0.1"

[politeness]
enabled = true
"#,
    )
    .expect("new-shape config should parse");
    assert_eq!(cfg.crawl.max_depth, Some(2));
    assert_eq!(cfg.crawl.max_urls, Some(100));
    assert_eq!(
        cfg.crawl
            .per_domain
            .get("python.org")
            .and_then(|o| o.max_depth),
        Some(1),
    );
    assert!(cfg.access.blocklist.contains("example.com"));
    assert!(cfg.politeness.enabled);
}
