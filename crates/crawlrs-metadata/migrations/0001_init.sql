-- crawlrs metadata schema.
--
-- Three tables:
--   url_metadata    one row per URL; current ledger snapshot. Mutable.
--   url_history     append-only log of state transitions. Bounded
--                   cardinality per URL by retry_count + lifecycle
--                   (a URL with N transient failures emits ~N+2 rows).
--   frontier_outbox transactional outbox for outbound URL enqueues;
--                   atomic with the ledger update on a successful
--                   fetch, drained by a separate publisher.
--
-- The runtime path is URL-keyed (`WHERE url = $1`); BIGSERIAL `id` is
-- the FK target for url_history + frontier_outbox so dependent rows
-- reference 8 bytes per row instead of duplicating the URL string.
--
-- The `status` CHECK uses split skip values (skipped_blocklist,
-- skipped_robots, skipped_depth, skipped_max_urls) rather than a
-- single 'skipped' value so dashboards can facet on the rejection
-- reason without joining url_history. The Rust side splits this
-- across two enums (UrlStatus + SkipReason) and serialises the pair
-- to one of the four strings at the SQL boundary.

CREATE TABLE url_metadata (
    id              BIGSERIAL   PRIMARY KEY,
    url             TEXT        NOT NULL UNIQUE,
    host            TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN (
        'pending',
        'in_progress',
        'succeeded',
        'failed_transient',
        'permanently_failed',
        'skipped_blocklist',
        'skipped_robots',
        'skipped_depth',
        'skipped_max_urls'
    )),
    retry_count     INT         NOT NULL DEFAULT 0,
    blob_path       TEXT,
    content_hash    BIGINT,
    depth           INT         NOT NULL DEFAULT 0,
    last_run_id     TEXT        NOT NULL,
    -- Parent URL that introduced this one. NULL for seed URLs and
    -- URLs first attempted before this column existed.
    discovered_from TEXT,
    discovered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-host slice queries; e.g. "show me everything we know about
-- reddit.com that's still in_progress."
CREATE INDEX url_metadata_host_status_idx
    ON url_metadata (host, status);

CREATE TABLE url_history (
    id           BIGSERIAL   PRIMARY KEY,
    url_id       BIGINT      NOT NULL REFERENCES url_metadata(id) ON DELETE CASCADE,
    run_id       TEXT        NOT NULL,
    event        TEXT        NOT NULL CHECK (event IN (
        'attempted',
        'succeeded',
        'failed',
        'permanently_failed'
    )),
    detail       JSONB,
    -- Correlation token from the Frontier ClaimedMessage that drove
    -- this attempt. Set on the `succeeded` event so a redelivered
    -- attempt's idempotent re-insert (caught by the UNIQUE constraint
    -- below) is the same dedupe boundary we enforce in the outbox.
    -- NULL on `attempted` rows; `mark_attempting` is called before
    -- the worker has emitted the success row.
    attempt_id   TEXT,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Postgres treats NULLs as distinct in UNIQUE constraints, so
    -- multiple `attempted` rows per url_id (with attempt_id IS NULL)
    -- coexist; the constraint only fires for the success/failure rows
    -- that carry a real correlation token.
    CONSTRAINT url_history_attempt_unique UNIQUE (url_id, attempt_id)
);

-- "Show me the last N events for this URL" (timeline view).
CREATE INDEX url_history_url_id_occurred_at_idx
    ON url_history (url_id, occurred_at DESC);

-- "Show me the most recent dead-letters across all URLs" (DLQ inspection).
CREATE INDEX url_history_event_occurred_at_idx
    ON url_history (event, occurred_at DESC);

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
    published_at      TIMESTAMPTZ,
    CONSTRAINT frontier_outbox_unique_per_attempt
        UNIQUE (parent_url_id, parent_attempt_id, url)
);

-- Partial index so the publisher's "drain unpublished" query stays
-- fast as the table grows. Once a row is published, it leaves the
-- index entirely.
CREATE INDEX frontier_outbox_unpublished_idx
    ON frontier_outbox (id)
    WHERE published_at IS NULL;
