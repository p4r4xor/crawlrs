//! Resolution-rule tests for `CrawlScope`.
//!
//! The contract for both `depth_cap` and `max_urls_for`:
//!
//!   per-domain value wins IF SET; else the global; else `None`.
//!
//! "Set" means the override holds `Some(_)`. A per-domain entry
//! with `None` on a field inherits the global. These tests pin
//! the four-cell matrix per method, plus the partial-override
//! case where one field overrides and the other inherits.

use std::collections::HashMap;

use crawlrs_core::{CrawlOverride, CrawlScope};

fn scope(max_depth: Option<u32>, max_urls: Option<u64>) -> CrawlScope {
    CrawlScope::new(max_depth, max_urls, HashMap::new())
}

fn scope_with_override(
    global_max_depth: Option<u32>,
    global_max_urls: Option<u64>,
    host: &str,
    override_: CrawlOverride,
) -> CrawlScope {
    let mut per_domain = HashMap::new();
    per_domain.insert(host.to_string(), override_);
    CrawlScope::new(global_max_depth, global_max_urls, per_domain)
}

// ---------------------------------------------------------------------------
// depth_cap: per-domain wins; else global; else None
// ---------------------------------------------------------------------------

#[test]
fn depth_cap_returns_none_when_neither_global_nor_per_domain_set() {
    let s = scope(None, None);
    assert_eq!(s.depth_cap("any.test"), None);
}

#[test]
fn depth_cap_returns_global_when_only_global_set() {
    let s = scope(Some(5), None);
    assert_eq!(s.depth_cap("a.test"), Some(5));
    assert_eq!(s.depth_cap("b.test"), Some(5));
}

#[test]
fn depth_cap_per_domain_overrides_global() {
    let s = scope_with_override(
        Some(5),
        None,
        "deep.test",
        CrawlOverride {
            max_depth: Some(2),
            max_urls: None,
        },
    );
    assert_eq!(s.depth_cap("deep.test"), Some(2), "override wins");
    assert_eq!(
        s.depth_cap("other.test"),
        Some(5),
        "other hosts fall back to global",
    );
}

#[test]
fn depth_cap_returns_per_domain_when_no_global() {
    let s = scope_with_override(
        None,
        None,
        "deep.test",
        CrawlOverride {
            max_depth: Some(2),
            max_urls: None,
        },
    );
    assert_eq!(s.depth_cap("deep.test"), Some(2));
    assert_eq!(
        s.depth_cap("other.test"),
        None,
        "other hosts stay unbounded",
    );
}

#[test]
fn depth_cap_none_override_inherits_global() {
    // An override entry exists for the host but its `max_depth`
    // is `None`. The contract says inherit, not "explicit None
    // overrides global to None."
    let s = scope_with_override(
        Some(5),
        None,
        "partial.test",
        CrawlOverride {
            max_depth: None,
            max_urls: Some(100),
        },
    );
    assert_eq!(
        s.depth_cap("partial.test"),
        Some(5),
        "None on the override field inherits the global",
    );
}

// ---------------------------------------------------------------------------
// max_urls_for: same resolution rule
// ---------------------------------------------------------------------------

#[test]
fn max_urls_for_returns_none_when_neither_global_nor_per_domain_set() {
    let s = scope(None, None);
    assert_eq!(s.max_urls_for("any.test"), None);
}

#[test]
fn max_urls_for_returns_global_when_only_global_set() {
    let s = scope(None, Some(1000));
    assert_eq!(s.max_urls_for("a.test"), Some(1000));
}

#[test]
fn max_urls_for_per_domain_overrides_global() {
    let s = scope_with_override(
        None,
        Some(1000),
        "capped.test",
        CrawlOverride {
            max_depth: None,
            max_urls: Some(50),
        },
    );
    assert_eq!(s.max_urls_for("capped.test"), Some(50));
    assert_eq!(s.max_urls_for("other.test"), Some(1000));
}

#[test]
fn max_urls_for_returns_per_domain_when_no_global() {
    let s = scope_with_override(
        None,
        None,
        "capped.test",
        CrawlOverride {
            max_depth: None,
            max_urls: Some(50),
        },
    );
    assert_eq!(s.max_urls_for("capped.test"), Some(50));
    assert_eq!(s.max_urls_for("other.test"), None);
}

#[test]
fn max_urls_for_none_override_inherits_global() {
    let s = scope_with_override(
        None,
        Some(1000),
        "partial.test",
        CrawlOverride {
            max_depth: Some(2),
            max_urls: None,
        },
    );
    assert_eq!(s.max_urls_for("partial.test"), Some(1000));
}

// ---------------------------------------------------------------------------
// Partial-override: each field resolves independently
// ---------------------------------------------------------------------------

#[test]
fn partial_override_resolves_each_field_independently() {
    // Global has both; override has only one. The set field wins
    // for that host; the unset field falls back to global.
    let s = scope_with_override(
        Some(5),
        Some(1000),
        "linkedin.test",
        CrawlOverride {
            max_depth: Some(2),
            max_urls: None,
        },
    );
    assert_eq!(s.depth_cap("linkedin.test"), Some(2), "override wins");
    assert_eq!(
        s.max_urls_for("linkedin.test"),
        Some(1000),
        "unset field inherits global",
    );
}
