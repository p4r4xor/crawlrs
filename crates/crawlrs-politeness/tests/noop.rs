//! Tests for the all-noop politeness wiring (master switch off).
//!
//! When `politeness.enabled = false`, the factory composes
//! `NoopWakePlanner` + `NoopRobotsChecker` + `NoopBackoffTracker`
//! through `CompositePoliteness::from_parts`. The composite is
//! still the only `Politeness` impl the runtime sees; its
//! observable contract collapses to "allow everything, no I/O,
//! no failure tracking." These tests pin that.

use std::sync::Arc;
use std::time::Duration;

use crawlrs_core::{
    BackoffTracker, Blocklist, CanonicalUrl, FailureKind, PoliteDecision, Politeness,
    RobotsChecker, WakePlanner,
};
use crawlrs_politeness::{
    BackoffPolicy, CompositePoliteness, NoopBackoffTracker, NoopRobotsChecker, NoopWakePlanner,
    PolitenessConfig, PolitenessOverride,
};

fn url(s: &str) -> CanonicalUrl {
    CanonicalUrl::parse(s).unwrap()
}

fn noop_composite_with_config(config: PolitenessConfig) -> CompositePoliteness {
    CompositePoliteness::from_parts(
        Arc::new(NoopWakePlanner),
        Arc::new(NoopRobotsChecker),
        Arc::new(NoopBackoffTracker),
        Blocklist::default(),
        config,
    )
}

fn noop_composite() -> CompositePoliteness {
    noop_composite_with_config(PolitenessConfig::default())
}

#[tokio::test]
async fn noop_composite_allows_every_url() {
    let composite = noop_composite();
    let decision = composite.check(&url("https://example.test/page")).await;
    assert_eq!(decision.unwrap(), PoliteDecision::Allow);
}

#[tokio::test]
async fn noop_composite_allows_even_when_obey_robots_is_true() {
    // The composite branches on `effective_obey_robots`; with noop
    // sub-traits the branch hits `NoopRobotsChecker::allowed`
    // which is `Ok(true)` for any URL. So the gate stays Allow
    // regardless of `obey_robots_txt`.
    let config = PolitenessConfig {
        obey_robots_txt: true,
        ..Default::default()
    };
    let composite = noop_composite_with_config(config);
    assert_eq!(
        composite
            .check(&url("https://example.test/private"))
            .await
            .unwrap(),
        PoliteDecision::Allow,
    );
}

#[tokio::test]
async fn noop_composite_record_fetch_returns_immediate_wake() {
    let composite = noop_composite();
    let plan = composite.record_fetch(&url("https://example.test/")).await;
    let plan = plan.unwrap();
    assert_eq!(plan.host, "example.test");
    // Immediate next-wake: the noop rate limiter returns now() so
    // the duration-from-now should be effectively zero.
    let delta = plan
        .until
        .saturating_duration_since(std::time::Instant::now());
    assert!(
        delta <= Duration::from_millis(100),
        "noop should produce ~0 delay; got {delta:?}",
    );
}

#[tokio::test]
async fn noop_composite_record_failure_returns_immediate_wake() {
    let composite = noop_composite();
    let plan = composite
        .record_failure(
            &url("https://flaky.test/"),
            FailureKind::TooManyRequests,
            Some(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    assert_eq!(plan.host, "flaky.test");
    // Noop backoff tracker ignores the server hint; the composite
    // is "off," so the operator does not pay backoff. The plan
    // is immediate-wake.
    let delta = plan
        .until
        .saturating_duration_since(std::time::Instant::now());
    assert!(
        delta <= Duration::from_millis(100),
        "noop backoff should ignore server hint and return ~0; got {delta:?}",
    );
}

#[tokio::test]
async fn noop_composite_honors_blocklist() {
    // The blocklist is owned by `[access]`, not by `[politeness]`,
    // so it stays active even when the master switch is off. The
    // composite checks blocklist BEFORE delegating to backoff /
    // robots / rate, so an `[access].blocklist` hit returns
    // Disallow even with all-noop sub-traits.
    let composite = CompositePoliteness::from_parts(
        Arc::new(NoopWakePlanner),
        Arc::new(NoopRobotsChecker),
        Arc::new(NoopBackoffTracker),
        Blocklist::new(["excluded.test".to_string()].into_iter().collect()),
        PolitenessConfig::default(),
    );
    assert_eq!(
        composite
            .check(&url("https://excluded.test/page"))
            .await
            .unwrap(),
        PoliteDecision::Disallow,
        "blocklist is access-layer config and outlives the politeness master switch",
    );
}

#[tokio::test]
async fn noop_composite_ignores_per_domain_overrides() {
    // The factory logs a warning per ignored override; the
    // composite itself treats per_domain entries as dead config
    // because the sub-trait impls are noop. Verify the behavior
    // here: with a per_domain override set, the URL still passes
    // (no rate gate fires).
    let per_domain = [(
        "slow-host.example".to_string(),
        PolitenessOverride {
            host_delay: Some(Duration::from_secs(60)),
            obey_robots_txt: Some(true),
            robots_ttl: None,
        },
    )]
    .into_iter()
    .collect();
    let config = PolitenessConfig {
        per_domain,
        ..Default::default()
    };
    let composite = noop_composite_with_config(config);
    let plan = composite
        .record_fetch(&url("https://slow-host.example/"))
        .await
        .unwrap();
    let delta = plan
        .until
        .saturating_duration_since(std::time::Instant::now());
    assert!(
        delta <= Duration::from_millis(100),
        "per_domain override is dead config under noop; got {delta:?}",
    );
}

// Sanity: the noop sub-traits themselves return the documented
// "allow / true / false / now" answers. These don't go through
// the composite, so they pin the trait-level contract directly.

#[tokio::test]
async fn noop_wake_planner_returns_immediate_wake() {
    let r = NoopWakePlanner;
    let plan = r.record_fetch("anywhere.test").await.unwrap();
    assert_eq!(plan.host, "anywhere.test");
    let delta = plan
        .until
        .saturating_duration_since(std::time::Instant::now());
    assert!(delta <= Duration::from_millis(100));
}

#[tokio::test]
async fn noop_robots_checker_is_allow_for_anything() {
    let r = NoopRobotsChecker;
    assert!(
        r.allowed(&url("https://anywhere.test/disallow"))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn noop_backoff_tracker_is_never_open() {
    let b = NoopBackoffTracker;
    assert!(!b.is_open("flaky.test").await.unwrap());
}

#[tokio::test]
async fn backoff_policy_unused_under_noop() {
    // The composite holds a `PolitenessConfig` for `effective_obey_robots`
    // resolution but the noop backoff tracker ignores `config.backoff`
    // entirely. Lock that: a `BackoffPolicy` with a tiny threshold
    // never opens the circuit under noop, even after many recorded
    // failures.
    let config = PolitenessConfig {
        backoff: BackoffPolicy {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            multiplier: 2.0,
            failure_threshold: 1,
        },
        ..Default::default()
    };
    let composite = noop_composite_with_config(config);
    for _ in 0..5 {
        let _ = composite
            .record_failure(&url("https://x.test/"), FailureKind::Timeout, None)
            .await
            .unwrap();
    }
    assert_eq!(
        composite.check(&url("https://x.test/")).await.unwrap(),
        PoliteDecision::Allow,
        "noop backoff tracker should never open the circuit",
    );
}
