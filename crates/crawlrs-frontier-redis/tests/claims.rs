//! Tests for `PendingClaims`: record/take roundtrip, overwrite semantics,
//! length tracking.

use crawlrs_core::CanonicalUrl;
use crawlrs_frontier_redis::PendingClaims;

fn url(s: &str) -> CanonicalUrl {
    CanonicalUrl::parse(s).unwrap()
}

#[test]
fn record_and_take_roundtrip() {
    let claims = PendingClaims::new();
    let u = url("https://a.test/");
    claims.record(u.clone(), 0, "1700000000000-0".into());
    let rec = claims.take(&u).unwrap();
    assert_eq!(rec.shard, 0);
    assert_eq!(rec.entry_id, "1700000000000-0");
    assert!(claims.take(&u).is_none(), "second take should miss");
}

#[test]
fn record_overwrites_prior_entry_id() {
    let claims = PendingClaims::new();
    let u = url("https://a.test/");
    claims.record(u.clone(), 0, "1-0".into());
    claims.record(u.clone(), 0, "2-0".into());
    let rec = claims.take(&u).unwrap();
    assert_eq!(
        rec.entry_id, "2-0",
        "later record should overwrite earlier one"
    );
}

#[test]
fn len_tracks_outstanding() {
    let claims = PendingClaims::new();
    assert!(claims.is_empty());
    claims.record(url("https://a.test/"), 0, "1-0".into());
    claims.record(url("https://b.test/"), 0, "2-0".into());
    assert_eq!(claims.len(), 2);
    claims.take(&url("https://a.test/"));
    assert_eq!(claims.len(), 1);
}
