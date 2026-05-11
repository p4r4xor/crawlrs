-- reclaim.lua
--
-- Scan the `inflight` ZSET for leases whose expiry has elapsed and
-- re-push their URL IDs onto the appropriate host queue. Bounded by
-- ARGV[2] so reclaim sweeps don't dominate a single Redis tick.
--
-- Each inflight member is `<url_id_hex>|<host>`; we split on the
-- separator, ZREM the member, and RPUSH the URL ID onto
-- `<host_queue_prefix><host>`.
--
-- IMPORTANT: reclaim uses `ZADD wake host NX now_ms` (insert only if
-- absent). Three states to consider:
--   (a) Worker called advance_wake with a future politeness backoff
--       (Retry-After, exponential backoff) BEFORE the lease expired.
--       wake[host] holds that future score. NX leaves it alone -
--       overwriting would clobber the backoff and let the host
--       re-fetch immediately.
--   (b) Worker called advance_wake, the promoter promoted the host
--       (score elapsed), and the host got popped off `ready` against
--       an empty `host_queue` (the URL was in `inflight` at the
--       time). The host is now in NEITHER wake nor ready. We need
--       to re-add it so the next promoter tick can pick it up.
--   (c) Worker crashed before advance_wake. wake[host] still carries
--       the claim-time safety score (claim_time + lease_timeout),
--       which equals "now" at reclaim time. NX is a no-op; the
--       existing-now score is correct.
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

local now_ms            = tonumber(ARGV[1])
local limit             = tonumber(ARGV[2])
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
        redis.call('ZADD',  KEYS[2], 'NX', now_ms, host)
        reclaimed = reclaimed + 1
    end
end
return reclaimed
