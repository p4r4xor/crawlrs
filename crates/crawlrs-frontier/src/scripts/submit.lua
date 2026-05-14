-- submit.lua
--
-- Atomic submit of one URL: bloom dedup -> URL HASH insert ->
-- per-host queue push -> wake-NX so the promoter picks the host up
-- if it's new.
--
-- KEYS:
--   [1] seen        -- RedisBloom filter
--   [2] urls        -- URL HASH (id -> payload)
--   [3] host_queue  -- per-host LIST for this URL's host
--   [4] wake        -- per-shard wake ZSET
-- ARGV:
--   [1] url_id_hex
--   [2] host
--   [3] payload  (postcard-encoded UrlEntry)
--   [4] now_ms
--
-- Returns: 0=Queued, 1=SkippedDuplicate
--
-- Per-host queues are intentionally unbounded. Politeness rate-limits
-- per host (one fetch per host_delay), so a deep queue is the work
-- waiting in line for its host's wake-time slots, not a defect.
-- Memory protection lives one level up: Redis itself runs with
-- `--maxmemory <cap> --maxmemory-policy noeviction`, returning OOM to
-- writers if the heap hits the ceiling. The crawler treats a failed
-- submit as a soft drop (the URL is unmarked in the bloom, so a
-- future submit can re-try it).

local url_id  = ARGV[1]
local host    = ARGV[2]
local payload = ARGV[3]
local now_ms  = tonumber(ARGV[4])

-- BF.ADD returns 1 if newly added, 0 if already present. Atomic
-- check-and-set; eliminates the EXISTS+ADD race seen with explicit
-- two-step dedup.
if redis.call('BF.ADD', KEYS[1], url_id) == 0 then
    return 1  -- SkippedDuplicate
end

-- Stash the payload. HSET is unconditional; duplicate entries would
-- be caught by the bloom above so we don't bother with HSETNX.
redis.call('HSET', KEYS[2], url_id, payload)
redis.call('RPUSH', KEYS[3], url_id)

-- First sighting of this host? Mark it ready right now so the
-- promoter picks it up on its next tick. NX prevents stomping on an
-- existing wake-time set by a prior claim or advance_wake.
redis.call('ZADD', KEYS[4], 'NX', now_ms, host)
return 0  -- Queued
