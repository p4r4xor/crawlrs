-- submit.lua
--
-- Atomic submit of one URL: bloom dedup -> URL HASH insert ->
-- per-host queue push (or overflow if backlog full) -> wake-NX so the
-- promoter picks the host up if it's new.
--
-- KEYS:
--   [1] seen        -- RedisBloom filter
--   [2] urls        -- URL HASH (id -> payload)
--   [3] host_queue      -- per-host LIST for this URL's host
--   [4] overflow    -- per-shard overflow LIST
--   [5] wake        -- per-shard wake ZSET
-- ARGV:
--   [1] url_id_hex
--   [2] host
--   [3] payload  (postcard-encoded UrlEntry)
--   [4] max_host_backlog
--   [5] now_ms
--
-- Returns: 0=Queued, 1=SkippedDuplicate, 2=Overflowed

local url_id  = ARGV[1]
local host    = ARGV[2]
local payload = ARGV[3]
local cap     = tonumber(ARGV[4])
local now_ms  = tonumber(ARGV[5])

-- BF.ADD returns 1 if newly added, 0 if already present. Atomic
-- check-and-set; eliminates the EXISTS+ADD race seen with explicit
-- two-step dedup.
if redis.call('BF.ADD', KEYS[1], url_id) == 0 then
    return 1  -- SkippedDuplicate
end

-- Stash the payload. HSET is unconditional; duplicate entries would
-- be caught by the bloom above so we don't bother with HSETNX.
redis.call('HSET', KEYS[2], url_id, payload)

-- Backlog cap: if the host queue is at capacity, divert to overflow.
-- The operator-visible metric on overflow length is the signal that
-- the host needs a blocklist entry or a larger cap.
local depth = redis.call('LLEN', KEYS[3])
if cap > 0 and depth >= cap then
    redis.call('RPUSH', KEYS[4], url_id)
    return 2  -- Overflowed
end

redis.call('RPUSH', KEYS[3], url_id)

-- First sighting of this host? Mark it ready right now so the
-- promoter picks it up on its next tick. NX prevents stomping on an
-- existing wake-time set by a prior claim or advance_wake.
redis.call('ZADD', KEYS[5], 'NX', now_ms, host)
return 0  -- Queued
