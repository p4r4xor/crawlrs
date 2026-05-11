//! Redis key naming: run/shard scoping.

use crawlrs_politeness::KeyPrefix;

#[test]
fn keys_are_run_and_shard_scoped() {
    let prefix = KeyPrefix::new("run-x");
    assert_eq!(
        prefix.hoststate(2, "example.com"),
        "crawlrs:run-x:s2:hoststate:example.com"
    );
    assert_eq!(
        prefix.robots(0, "example.com"),
        "crawlrs:run-x:s0:robots:example.com"
    );
}
