-- reclaim.lua
--
-- Scan the `inflight` ZSET for leases whose expiry has elapsed and
-- re-push their URL IDs onto the appropriate host queue. Bounded by
-- ARGV[2] so reclaim sweeps don't dominate a single Redis tick.
--
-- Each inflight member is `<url_id_hex>|<host>`; we split on the
-- separator, ZREM the member, and RPUSH the URL ID onto
-- `<host_queue_prefix><host>`. The host's wake-time gets re-stamped at
-- `now_ms` so the promoter picks it up on the next pass (in case the
-- host had drifted out of `wake` while the lease was active).
--
-- KEYS:
--   [1] inflight
--   [2] wake
-- ARGV:
--   [1] now_ms
--   [2] batch_limit
--   [3] host_queue_prefix
--
-- Returns: count of leases reclaimed

local now_ms        = tonumber(ARGV[1])
local limit         = tonumber(ARGV[2])
local host_queue_prefix = ARGV[3]

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], 0, now_ms, 'LIMIT', 0, limit)
local reclaimed = 0
for _, member in ipairs(expired) do
    local sep = string.find(member, '|', 1, true)
    if sep then
        local url_id = string.sub(member, 1, sep - 1)
        local host   = string.sub(member, sep + 1)
        redis.call('ZREM',  KEYS[1], member)
        redis.call('RPUSH', host_queue_prefix .. host, url_id)
        redis.call('ZADD',  KEYS[2], now_ms, host)
        reclaimed = reclaimed + 1
    end
end
return reclaimed
