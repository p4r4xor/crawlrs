//! Tests for the public identity + correlation types
//! (`WorkerIdentity`, `AttemptId`).
//!
//! Both types are load-bearing for recovery: a stable
//! `WorkerIdentity` is what makes Redis Streams tier-1 PEL replay
//! reattach a restarted worker to its own in-flight entries; an
//! `AttemptId` is what lets `MetadataStore::mark_succeeded` dedupe a
//! redelivered attempt without conflating it with a fresh retry.
//! These tests pin the contracts that downstream layers rely on.

use std::collections::HashSet;

use crawlrs_core::{AttemptId, WorkerIdentity};

#[test]
fn worker_identity_renders_stable_string() {
    let id = WorkerIdentity::new(2, 3);
    assert_eq!(id.to_string(), "pod-2:3");

    // Re-rendering the same identity must produce a byte-identical
    // string. The Frontier impl uses this string as the Redis
    // consumer name; if it varied, tier-1 PEL replay after a
    // process restart would attach to a different consumer and
    // miss in-flight entries.
    let twin = WorkerIdentity::new(2, 3);
    assert_eq!(id.to_string(), twin.to_string());
    assert_eq!(id, twin);
}

#[test]
fn worker_identity_distinguishes_pods_and_indices() {
    let a = WorkerIdentity::new(0, 0);
    let b = WorkerIdentity::new(0, 1);
    let c = WorkerIdentity::new(1, 0);
    assert_ne!(a, b, "different worker_index");
    assert_ne!(a, c, "different pod_ordinal");
    assert_ne!(b, c);
    assert_ne!(a.to_string(), b.to_string());
    assert_ne!(a.to_string(), c.to_string());
}

#[test]
fn attempt_id_round_trips_string() {
    let raw = "1714867200000-0";
    let attempt = AttemptId::new(raw);
    assert_eq!(attempt.as_str(), raw);
    assert_eq!(attempt.to_string(), raw);
}

#[test]
fn attempt_id_supports_eq_and_hash() {
    let a = AttemptId::new("X");
    let b = AttemptId::new("X");
    let c = AttemptId::new("Y");
    assert_eq!(a, b);
    assert_ne!(a, c);

    let mut seen: HashSet<AttemptId> = HashSet::new();
    seen.insert(a.clone());
    assert!(!seen.insert(b), "same token must collide in a HashSet");
    assert!(seen.insert(c), "different token must not collide");
}
