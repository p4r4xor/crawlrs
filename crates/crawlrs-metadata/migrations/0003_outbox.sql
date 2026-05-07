-- Transactional outbox for outbound URL enqueues. Co-located with
-- the URL ledger so a successful fetch's metadata update and its
-- discovered-link writes happen in one Postgres transaction. A
-- separate publisher task drains unpublished rows and pushes the
-- entries into the Frontier; per-URL dedupe at the Frontier side
-- absorbs the at-least-once delivery semantics.
--
-- Per-attempt dedupe: a redelivered (url, attempt_id) pair (i.e. the
-- same XAUTOCLAIM-resurrected attempt) MUST NOT duplicate outbox
-- rows. The unique constraint enforces this at the schema level so
-- the runtime's idempotency contract isn't an unwritten promise.

CREATE TABLE frontier_outbox (
    id                BIGSERIAL    PRIMARY KEY,
    url               TEXT         NOT NULL,
    depth             INT          NOT NULL,
    discovered_from   TEXT,
    parent_url_id     BIGINT       NOT NULL REFERENCES url_metadata(id) ON DELETE CASCADE,
    parent_attempt_id TEXT         NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at      TIMESTAMPTZ
);

-- One outbox row per (parent fetch attempt, discovered URL). A
-- redelivered attempt re-runs the post-fetch path; the second pass's
-- INSERT statements collapse to no-ops via ON CONFLICT DO NOTHING in
-- the application layer, which lets the publisher drain a
-- deterministic set without worrying about double-publishes.
ALTER TABLE frontier_outbox
    ADD CONSTRAINT frontier_outbox_unique_per_attempt
        UNIQUE (parent_url_id, parent_attempt_id, url);

-- Partial index so the publisher's "drain unpublished" query stays
-- fast as the table grows. Once a row is published, it leaves the
-- index entirely.
CREATE INDEX frontier_outbox_unpublished_idx
    ON frontier_outbox (id)
    WHERE published_at IS NULL;
