-- Add attempt_id correlation token to url_history so re-delivery of the
-- same Frontier attempt (e.g. via XAUTOCLAIM after a stall between
-- mark_succeeded and frontier.ack) cannot duplicate a history row.
--
-- attempt_id is opaque from the database's perspective: the runtime
-- supplies it from the AttemptId carried by each ClaimedMessage. The
-- Redis frontier impl encodes "<shard>|<stream-entry-id>"; an in-memory
-- frontier impl can use any unique-per-delivery token.
--
-- The column is NULLable for backfill compatibility with rows written
-- by the v1 codepath. Postgres treats multiple NULLs as distinct under
-- a UNIQUE constraint, so existing rows keep their independence.

ALTER TABLE url_history
    ADD COLUMN attempt_id TEXT;

ALTER TABLE url_history
    ADD CONSTRAINT url_history_attempt_unique
        UNIQUE (url_id, attempt_id);
