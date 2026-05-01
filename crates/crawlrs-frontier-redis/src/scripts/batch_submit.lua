-- batch_submit.lua
--
-- Atomic per-URL SADD-then-XADD for many entries on one shard.
-- One EVAL handles the whole chunk so we pay one network round-trip
-- regardless of chunk size; the seen-set check + enqueue stays atomic
-- per URL the same way the singular path was.
--
-- KEYS[1] = seen-set key                     (e.g. "crawlrs:run:s0:seen")
-- KEYS[2] = queue stream key                 (e.g. "crawlrs:run:s0:queue")
-- ARGV[1] = max queue depth                  (0 = uncapped; otherwise XADD MAXLEN ~ N)
-- ARGV[2..] = interleaved pairs:             url1, body1, url2, body2, ...
--
-- Returns: count of newly-enqueued entries (the SADD-returned-1 cases).
--
-- The MAXLEN cap uses the `~` (approximate) form so Redis can trim in
-- O(log N) bursts rather than per-add. Trimming drops the OLDEST
-- entries; with the seen-set still holding their URL strings, dropped
-- URLs won't be re-enqueued by a future submit, so the trim is a hard
-- loss for those URLs. Operator chooses the cap balancing memory vs.
-- coverage. Pass 0 to opt out entirely.

local seen_key  = KEYS[1]
local queue_key = KEYS[2]
local max_len   = tonumber(ARGV[1])
local newly = 0
local i = 2
while i <= #ARGV do
    local url  = ARGV[i]
    local body = ARGV[i + 1]
    if redis.call('SADD', seen_key, url) == 1 then
        if max_len and max_len > 0 then
            redis.call('XADD', queue_key, 'MAXLEN', '~', max_len, '*', 'body', body)
        else
            redis.call('XADD', queue_key, '*', 'body', body)
        end
        newly = newly + 1
    end
    i = i + 2
end
return newly
