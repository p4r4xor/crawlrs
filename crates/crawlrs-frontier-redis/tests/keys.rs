//! Tests for `KeyPrefix`: run-id + shard scoping of Redis keys.

use crawlrs_frontier_redis::KeyPrefix;

#[test]
fn keys_are_run_scoped_and_shard_scoped() {
    let keys = KeyPrefix::new("run-x");
    assert_eq!(keys.wake(0), "crawlrs:{run-x_s0}:wake");
    assert_eq!(keys.wake(7), "crawlrs:{run-x_s7}:wake");
    assert_eq!(keys.seen(0), "crawlrs:{run-x_s0}:seen");
}

#[test]
fn different_runs_dont_collide() {
    let a = KeyPrefix::new("run-a");
    let b = KeyPrefix::new("run-b");
    assert_ne!(a.wake(0), b.wake(0));
    assert_ne!(a.host_queue(0, "example.com"), b.host_queue(0, "example.com"));
}
