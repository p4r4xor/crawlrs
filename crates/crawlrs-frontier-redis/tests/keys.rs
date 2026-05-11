//! Tests for `KeyPrefix`: run-id + shard scoping of Redis keys.

use crawlrs_frontier_redis::KeyPrefix;

#[test]
fn per_run_keys_are_run_scoped_and_shard_scoped() {
    let keys = KeyPrefix::new("run-x");
    assert_eq!(keys.wake(0), "crawlrs:{run-x_s0}:wake");
    assert_eq!(keys.wake(7), "crawlrs:{run-x_s7}:wake");
}

#[test]
fn seen_is_deployment_wide_for_cross_run_dedup() {
    // The seen bloom is the dedup substrate shared across all runs
    // for the same Redis deployment. Two different run_ids must
    // produce identical seen keys per shard.
    let a = KeyPrefix::new("run-a");
    let b = KeyPrefix::new("run-b");
    assert_eq!(a.seen(0), "crawlrs:{s0}:seen");
    assert_eq!(a.seen(0), b.seen(0));
}

#[test]
fn different_runs_dont_collide_on_per_run_keys() {
    let a = KeyPrefix::new("run-a");
    let b = KeyPrefix::new("run-b");
    assert_ne!(a.wake(0), b.wake(0));
    assert_ne!(a.host_queue(0, "example.com"), b.host_queue(0, "example.com"));
}
