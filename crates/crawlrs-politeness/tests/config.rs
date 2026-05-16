//! Tests for the public config types: `PolitenessConfig`,
//! `PolitenessOverride`, `BackoffPolicy`. Default-value sanity only;
//! the values themselves are operator-tunable in `crawl.toml`.

use std::time::Duration;

use crawlrs_politeness::{BackoffPolicy, PolitenessConfig, PolitenessOverride};

#[test]
fn defaults_are_sensible() {
    let c = PolitenessConfig::default();
    assert!(c.enabled);
    assert_eq!(c.host_delay, Duration::from_secs(1));
    assert!(c.obey_robots_txt);
    assert!(c.per_domain.is_empty());
}

#[test]
fn override_default_inherits_everything() {
    let o = PolitenessOverride::default();
    assert_eq!(o.host_delay, None);
    assert_eq!(o.obey_robots_txt, None);
}

#[test]
fn backoff_default_caps_at_ten_minutes() {
    let p = BackoffPolicy::default();
    assert_eq!(p.max_backoff, Duration::from_secs(600));
    assert!(p.multiplier > 1.0);
}

// ---------------------------------------------------------------------------
// has_host_delay: the verdict the wake planner caches at
// construction. When `false`, the planner short-circuits the
// per-host lookup and the worker skips the
// `Frontier::advance_wake` write per fetch.
// ---------------------------------------------------------------------------

fn config_with(global: Duration) -> PolitenessConfig {
    PolitenessConfig {
        host_delay: global,
        ..Default::default()
    }
}

#[test]
fn no_host_delay_when_global_is_zero_and_no_overrides() {
    let config = config_with(Duration::ZERO);
    assert!(!config.has_host_delay());
}

#[test]
fn has_host_delay_when_global_is_nonzero() {
    let config = config_with(Duration::from_millis(1));
    assert!(config.has_host_delay());
}

#[test]
fn has_host_delay_when_any_per_domain_override_is_nonzero() {
    let mut config = config_with(Duration::ZERO);
    config.per_domain.insert(
        "slow.test".into(),
        PolitenessOverride {
            host_delay: Some(Duration::from_secs(5)),
            obey_robots_txt: None,
            robots_ttl: None,
        },
    );
    assert!(
        config.has_host_delay(),
        "any non-zero per_domain delay re-enables the pacing path",
    );
}

#[test]
fn no_host_delay_when_per_domain_override_is_explicit_zero() {
    let mut config = config_with(Duration::ZERO);
    config.per_domain.insert(
        "fast.test".into(),
        PolitenessOverride {
            host_delay: Some(Duration::ZERO),
            obey_robots_txt: None,
            robots_ttl: None,
        },
    );
    assert!(!config.has_host_delay());
}

#[test]
fn no_host_delay_when_per_domain_override_inherits_global() {
    // `host_delay: None` on an override means "inherit the
    // global." Global is zero, so the effective delay for
    // this host is zero too.
    let mut config = config_with(Duration::ZERO);
    config.per_domain.insert(
        "inherit.test".into(),
        PolitenessOverride {
            host_delay: None,
            obey_robots_txt: Some(false),
            robots_ttl: None,
        },
    );
    assert!(!config.has_host_delay());
}
