//! Tests for the public config types: `PolitenessConfig`,
//! `PolitenessOverride`, `BackoffPolicy`. Default-value sanity only;
//! the values themselves are operator-tunable in `crawl.toml`.

use std::time::Duration;

use crawlrs_politeness::{BackoffPolicy, PolitenessConfig, PolitenessOverride};

#[test]
fn defaults_are_sensible() {
    let c = PolitenessConfig::default();
    assert_eq!(c.min_delay, Duration::from_secs(1));
    assert!(c.honor_robots_txt);
    assert!(c.manual_excludes.is_empty());
    assert!(c.per_domain.is_empty());
}

#[test]
fn override_default_inherits_everything() {
    let o = PolitenessOverride::default();
    assert_eq!(o.min_delay, None);
    assert_eq!(o.honor_robots_txt, None);
}

#[test]
fn backoff_default_caps_at_ten_minutes() {
    let p = BackoffPolicy::default();
    assert_eq!(p.max_backoff, Duration::from_secs(600));
    assert!(p.multiplier > 1.0);
}
