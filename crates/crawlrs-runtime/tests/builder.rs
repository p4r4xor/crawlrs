//! Tests for `CrawlerBuilder` + `CrawlerConfig` defaults.
//!
//! These are unit-shape tests of the public builder/config API. Heavy
//! end-to-end tests live in `tests/integration.rs` and
//! `tests/postgres_metadata.rs`; this file is the lightweight,
//! no-testcontainer path.

use std::time::Duration;

use crawlrs_runtime::{Crawler, CrawlerConfig, CrawlerError};

#[test]
fn missing_deps_fail_to_build() {
    let err = Crawler::builder().build().unwrap_err();
    assert!(matches!(err, CrawlerError::MissingDep("frontier")));
}

#[test]
fn defaults_are_sensible() {
    let c = CrawlerConfig::default();
    assert!(c.workers >= 1);
    assert!(c.maintenance_interval >= Duration::from_secs(1));
    assert!(c.empty_queue_poll < c.maintenance_interval);
}
