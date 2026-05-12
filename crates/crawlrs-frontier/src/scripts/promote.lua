-- promote.lua
--
-- Drain hosts whose wake-time has elapsed out of the `wake` ZSET and
-- push them onto the `ready` LIST. Bounded by ARGV[2] so a single
-- tick can't drag on under heavy backlog; the caller loops the
-- promoter task to drain the rest.
--
-- KEYS:
--   [1] wake
--   [2] ready
-- ARGV:
--   [1] now_ms
--   [2] batch_limit
--
-- Returns: count of hosts promoted

local now_ms = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])

local hosts = redis.call('ZRANGEBYSCORE', KEYS[1], 0, now_ms, 'LIMIT', 0, limit)
local promoted = 0
for _, host in ipairs(hosts) do
    redis.call('ZREM',  KEYS[1], host)
    redis.call('RPUSH', KEYS[2], host)
    promoted = promoted + 1
end
return promoted
