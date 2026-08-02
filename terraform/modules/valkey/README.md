# Valkey

Provisions the ElastiCache Valkey cluster holding the crawler's frontier and politeness state, plus the parameter group and security group it needs. Everything you need to wire it up is on this page.

## 1. What this holds

**Frontier:** the URL queue (Redis Streams plus consumer groups) and the Bloom filter that dedups URLs at submit time.

**Politeness:** the per-host wake-time sorted set and the robots.txt cache.

None of it is a cache in the disposable sense. An evicted key is a URL the crawler believes is queued and will never claim again, or a Bloom filter that has forgotten what it already crawled.

## 2. Settings you cannot change

**`maxmemory-policy noeviction`:** pinned through the module's own parameter group. ElastiCache defaults to an LRU policy, which drops queued URLs under memory pressure and tells nobody. With `noeviction` a write past the memory ceiling returns an OOM error the crawler surfaces and retries.

**Snapshots stay on:** `snapshot_retention_limit` rejects 0. Bloom filter state has no other source. Replace a node without a snapshot and the crawler re-fetches every URL it had already deduped.

**At-rest encryption stays on:** not configurable, no reason to turn it off.

## 3. Engine version

9.1 by default, 8.1 the hard floor enforced by a validation.

The frontier issues `BF.RESERVE` and `BF.ADD` on every submit, and ElastiCache exposes the Bloom filter data type from Valkey 8.1 onward. Below that the cluster accepts your connection and rejects every Bloom command.

The parameter group family tracks the major version, so a move to a new major means changing `parameter_group_family` alongside `engine_version`. Confirm the family string for a version with:

```bash
aws elasticache describe-cache-engine-versions --engine valkey
```

## 4. Transit encryption

On by default. The crawler's `[redis].url` must then use the `rediss://` scheme. The module's `url` output already picks the right scheme for the setting, so pass that through rather than assembling the URL yourself.

Turning it off saves roughly a millisecond per round trip and is only defensible inside a VPC you trust.

## 5. Network access

Ingress is granted to security group IDs, never CIDRs. Pass the EKS node security group in `allowed_security_group_ids` and the rule stays correct when node subnets change.

## 6. Failover

At `num_cache_clusters = 1` there is no replica, so automatic failover and multi-AZ are both off and a node replacement is a full outage of the queue. At 2 or more the module turns both on for you.

## 7. Edgecases

### "Every submit fails but the connection is healthy"

Engine version below 8.1. See section 3.

### "The crawler cannot connect at all and the URL looks right"

Scheme mismatch. With `transit_encryption_enabled = true` the URL must be `rediss://`, not `redis://`. Use the `url` output.

### "Writes started returning OOM errors"

Working set outgrew the node. That is the designed behaviour, not a fault: `noeviction` surfaces the pressure instead of silently dropping queued URLs. Move to a larger `node_type`.

### "A `tofu apply` wants to replace the parameter group"

You changed `parameter_group_family`, which forces replacement, and the replication group references it. Change the family and the engine version in the same apply.

## 8. Limits and numbers

| Thing | Value |
|---|---|
| Default engine version | 9.1 |
| Minimum engine version | 8.1 |
| Default node type | `cache.r7g.large` |
| Port | 6379 |
| Eviction policy | `noeviction`, not configurable |
| Snapshot retention | 1 day minimum, cannot be 0 |
| Automatic failover | on at 2 or more nodes |
| Ingress | security group IDs only |
