-- crawlrs metadata schema.
--
-- Two tables:
--   url_metadata  one row per URL; current ledger snapshot. Mutable.
--   url_history   append-only log of state transitions. Bounded
--                 cardinality per URL by retry_count + lifecycle
--                 (a URL with N transient failures emits ~N+2 rows).
--
-- The runtime path is URL-keyed (`WHERE url = $1`); BIGSERIAL `id` is
-- the FK target for url_history so the history rows reference 8 bytes
-- per row instead of duplicating the URL string.

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
        'skipped'
    )),
    retry_count     INT         NOT NULL DEFAULT 0,
    blob_path       TEXT,
    content_hash    BIGINT,
    depth           INT         NOT NULL DEFAULT 0,
    last_run_id     TEXT        NOT NULL,
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
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "Show me the last N events for this URL" (timeline view).
CREATE INDEX url_history_url_id_occurred_at_idx
    ON url_history (url_id, occurred_at DESC);

-- "Show me the most recent dead-letters across all URLs" (DLQ inspection).
CREATE INDEX url_history_event_occurred_at_idx
    ON url_history (event, occurred_at DESC);
