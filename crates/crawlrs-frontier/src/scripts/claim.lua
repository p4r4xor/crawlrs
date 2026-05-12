-- claim.lua
--
-- Atomic claim of one URL: pop a ready host, pop its next queued
-- URL ID, materialise the payload, stamp the lease, set a safety
-- wake-time so no other worker hits the host until advance_wake is
-- called from the post-fetch path.
--
-- KEYS:
--   [1] ready      -- per-shard LIST of ready hosts
--   [2] wake       -- per-shard wake ZSET
--   [3] inflight   -- per-shard lease ZSET; member = "url_id|host"
--   [4] urls       -- URL HASH
-- ARGV:
--   [1] host_queue_prefix    -- e.g. "crawlrs:{run_s0}:host_queue:"
--   [2] now_ms
--   [3] lease_timeout_ms
--
-- Returns one of:
--   {"claimed",    url_id, host, payload}
--   {"empty_hint", soonest_ms}
--   {"empty"}
--
-- If a popped host has an empty host_queue (race with reclaim that
-- removed the host's last URL), we loop and pop the next ready host
-- rather than failing the call.

local host_queue_prefix    = ARGV[1]
local now_ms           = tonumber(ARGV[2])
local lease_timeout_ms = tonumber(ARGV[3])

while true do
    local host = redis.call('LPOP', KEYS[1])
    if not host then break end

    local url_id = redis.call('LPOP', host_queue_prefix .. host)
    if url_id then
        local expiry_ms = now_ms + lease_timeout_ms
        -- Safety wake-time: ensures the host stays out of `ready`
        -- until advance_wake overwrites this with the politeness
        -- value. If the worker crashes, the promoter will re-promote
        -- the host once `expiry_ms` elapses, which is also when the
        -- reclaim pass picks up the stranded URL.
        redis.call('ZADD', KEYS[2], expiry_ms, host)
        -- Lease entry. Member encodes the host so reclaim can re-push
        -- without re-decoding the URL HASH payload.
        redis.call('ZADD', KEYS[3], expiry_ms, url_id .. '|' .. host)
        local payload = redis.call('HGET', KEYS[4], url_id)
        return {'claimed', url_id, host, payload}
    end
    -- Stale `ready` entry. Continue to the next host.
end

-- `ready` was empty. Surface the soonest scheduled wake-time, if any.
local soonest = redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if #soonest == 2 then
    return {'empty_hint', soonest[2]}
end
return {'empty'}
