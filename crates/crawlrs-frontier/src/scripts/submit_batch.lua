-- submit_batch.lua
--
-- Atomic batched submit for N URLs on one shard. Semantically a loop
-- of submit.lua, run inside Redis so the N submits collapse into one
-- round-trip from the client and one EVAL dispatch on the server.
--
-- Extends the per-URL flow with a `[crawl].max_urls` quota check:
-- counter-first (GET host_count), then bloom (BF.ADD), then enqueue.
-- URLs rejected by the counter are NOT marked in the bloom so they
-- remain eligible for a future run (where the per-host counter
-- resets).
--
-- KEYS:
--   [1]                   seen           -- RedisBloom filter for this shard
--   [2]                   urls           -- URL HASH (id -> payload)
--   [3]                   wake           -- per-shard wake ZSET
--   [4 + i*2]             host_queue_i   -- per-URL host_queue LIST
--   [5 + i*2]             host_count_i   -- per-URL host counter (INCR target)
-- ARGV:
--   [1]                   N                  -- batch size
--   [2]                   now_ms
--   [3 + i*4 ... +3]      (url_id, host, payload, max_urls_or_neg1)
--                                            packed per URL. max_urls = -1
--                                            means "no cap for this host."
--
-- Returns: a Redis array {queued, rejected}. The rest of the
-- batch (N - queued - rejected) was a bloom duplicate.
--
-- All KEYS share the per-shard hash tag so Redis Cluster routes the
-- script to one slot. Cross-shard batching is the caller's job;
-- mixing shards in one call would split the keyspace and break the
-- single-slot guarantee.

local n         = tonumber(ARGV[1])
local now_ms    = tonumber(ARGV[2])
local queued    = 0
local rejected  = 0

for i = 0, n - 1 do
  local base    = 3 + i * 4
  local url_id  = ARGV[base]
  local host    = ARGV[base + 1]
  local payload = ARGV[base + 2]
  local cap     = tonumber(ARGV[base + 3])

  local host_queue_key = KEYS[4 + i * 2]
  local host_count_key = KEYS[5 + i * 2]

  -- Counter-first: cheap GET. URLs already at/over quota are
  -- rejected before we mark the bloom, so they're still eligible
  -- for a future run when the counter resets. Downstream tradeoff:
  -- a URL that would have been a bloom-duplicate is instead counted
  -- as rejected. Acceptable because quota and dedup are both "drop
  -- this URL" outcomes; the metric attribution is the only difference.
  --
  -- The flag-then-branch shape (rather than `goto`) is required
  -- because Redis EVAL embeds Lua 5.1, which predates `goto`/labels.
  local quota_exceeded = false
  if cap >= 0 then
    local cur = tonumber(redis.call('GET', host_count_key)) or 0
    if cur >= cap then
      rejected = rejected + 1
      quota_exceeded = true
    end
  end

  if not quota_exceeded then
    -- BF.ADD: 1 if newly added, 0 if already present. Atomic check-and-set.
    if redis.call('BF.ADD', KEYS[1], url_id) == 1 then
      -- Bloom accepted as new: bump the per-host counter, then
      -- enqueue.
      if cap >= 0 then
        redis.call('INCR', host_count_key)
      end
      redis.call('HSET',  KEYS[2], url_id, payload)
      redis.call('RPUSH', host_queue_key, url_id)
      -- NX so we don't stomp a wake-time already set by a prior claim
      -- or advance_wake against the same host.
      redis.call('ZADD',  KEYS[3], 'NX', now_ms, host)
      queued = queued + 1
    end
  end
end

return {queued, rejected}
