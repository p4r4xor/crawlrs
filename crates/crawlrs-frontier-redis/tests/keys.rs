//! Tests for `KeyPrefix`: run-id + shard scoping of Redis keys.

use crawlrs_frontier_redis::KeyPrefix;

#[test]
fn keys_are_run_scoped_and_shard_scoped() {
    let keys = KeyPrefix::new("run-x");
    assert_eq!(keys.queue(0), "crawlrs:run-x:s0:queue");
    assert_eq!(keys.queue(7), "crawlrs:run-x:s7:queue");
    assert_eq!(keys.seen(0), "crawlrs:run-x:s0:seen");
}

#[test]
fn different_runs_dont_collide() {
    let a = KeyPrefix::new("run-a");
    let b = KeyPrefix::new("run-b");
    assert_ne!(a.queue(0), b.queue(0));
}

#[test]
fn consumer_group_is_stable() {
    let a = KeyPrefix::new("run-a");
    let b = KeyPrefix::new("run-b");
    assert_eq!(a.consumer_group(), b.consumer_group());
    assert_eq!(a.consumer_group(), "fetchers");
}
