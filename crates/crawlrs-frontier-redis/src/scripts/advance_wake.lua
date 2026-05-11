-- advance_wake.lua
--
-- Set a host's next-allowed-fetch time. Two operations as one round-
-- trip:
--   1. ZADD wake host until_ms      (unconditional; politeness owns
--                                    the value, claim only sets a
--                                    safety floor)
--   2. LREM ready 0 host            (idempotent: removes host from
--                                    the ready list if for any reason
--                                    a peer's reclaim re-promoted it
--                                    between our claim and now)
--
-- KEYS:
--   [1] wake
--   [2] ready
-- ARGV:
--   [1] host
--   [2] until_ms
--
-- Returns: 1

redis.call('ZADD',  KEYS[1], ARGV[2], ARGV[1])
redis.call('LREM',  KEYS[2], 0, ARGV[1])
return 1
