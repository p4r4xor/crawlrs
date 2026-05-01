-- submit.lua
--
-- Atomic SADD-then-XADD: dedup on the seen-set first, only enqueue if new.
-- Eliminates the race where a non-atomic SADD-then-XADD could leave the
-- seen-set populated while the queue lacks the entry (or vice versa).
--
-- KEYS[1] = seen-set key                 (e.g. "crawlrs:run:s0:seen")
-- KEYS[2] = queue stream key             (e.g. "crawlrs:run:s0:queue")
-- ARGV[1] = url hash                     (string used as the seen-set member)
-- ARGV[2] = postcard-encoded UrlEntry    (binary blob for the stream)
--
-- Returns: 1 if newly enqueued, 0 if the URL was already known.

local newly_added = redis.call('SADD', KEYS[1], ARGV[1])
if newly_added == 1 then
    redis.call('XADD', KEYS[2], '*', 'body', ARGV[2])
    return 1
end
return 0
