-- submit_batch.lua
--
-- Atomic batched submit for N URLs on one shard. Semantically a loop
-- of submit.lua, run inside Redis so the N submits collapse into one
-- round-trip from the client and one EVAL dispatch on the server.
--
-- KEYS:
--   [1]       seen           -- RedisBloom filter for this shard
--   [2]       urls           -- URL HASH (id -> payload)
--   [3]       wake           -- per-shard wake ZSET
--   [4..N+3]  host_queue_i   -- per-URL host_queue LIST. URLs in a
--                               single batch may have different hosts,
--                               so each URL gets its own KEYS slot.
-- ARGV:
--   [1]                N                -- batch size
--   [2]                now_ms
--   [3..3*N+2]         (url_id, host, payload) triples, packed
--
-- Returns: integer count of URLs newly queued (bloom-NEW). The rest
-- were SkippedDuplicate.
--
-- All KEYS share the per-shard hash tag so Redis Cluster routes the
-- script to one slot. Cross-shard batching is the caller's job;
-- mixing shards in one call would split the keyspace and break the
-- single-slot guarantee.

local n      = tonumber(ARGV[1])
local now_ms = tonumber(ARGV[2])
local newly  = 0

for i = 0, n - 1 do
  local base   = 3 + i * 3
  local url_id  = ARGV[base]
  local host    = ARGV[base + 1]
  local payload = ARGV[base + 2]
  -- BF.ADD: 1 if newly added, 0 if already present. Atomic check-and-set.
  if redis.call('BF.ADD', KEYS[1], url_id) == 1 then
    redis.call('HSET',  KEYS[2], url_id, payload)
    redis.call('RPUSH', KEYS[4 + i], url_id)
    -- NX so we don't stomp a wake-time already set by a prior claim
    -- or advance_wake against the same host.
    redis.call('ZADD',  KEYS[3], 'NX', now_ms, host)
    newly = newly + 1
  end
end

return newly
