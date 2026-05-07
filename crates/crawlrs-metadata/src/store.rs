//! `PostgresMetadataStore`: implements [`MetadataStore`] over Postgres.
//!
//! Schema (see `migrations/0001_init.sql`): one mutable row per URL
//! in `url_metadata`, plus an append-only event row in `url_history`
//! per state transition. The two writes happen inside a transaction
//! so the ledger and the history can never disagree.
//!
//! Time fields cross the API/DB boundary as `chrono::DateTime<Utc>`
//! and are converted to `SystemTime` for the `UrlMetadata` struct
//! that the trait returns. `content_hash` is `u64` in Rust and
//! `BIGINT` (i64) in Postgres; the bit-cast at the boundary is
//! lossless because the value is opaque.

use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crawlrs_core::{
    CanonicalUrl, Error, FailureKind, MetadataStore, OutboxEntry, OutboxReader, OutboxRowId,
    Result, SuccessRecord, UrlEntry, UrlMetadata, UrlStatus,
};
use serde_json::json;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Transaction};
use thiserror::Error as ThisError;
use tracing::{debug, info};

const STATUS_PENDING: &str = "pending";
const STATUS_IN_PROGRESS: &str = "in_progress";
const STATUS_SUCCEEDED: &str = "succeeded";
const STATUS_FAILED_TRANSIENT: &str = "failed_transient";
const STATUS_PERMANENTLY_FAILED: &str = "permanently_failed";
const STATUS_SKIPPED: &str = "skipped";

const EVENT_ATTEMPTED: &str = "attempted";
const EVENT_SUCCEEDED: &str = "succeeded";
const EVENT_FAILED: &str = "failed";
const EVENT_PERMANENTLY_FAILED: &str = "permanently_failed";

/// Errors specific to the Postgres-backed metadata layer. Funnels into
/// [`Error::Metadata`] at the trait boundary so callers see a single
/// coarse variant in the public surface.
#[derive(Debug, ThisError)]
pub enum PostgresMetadataError {
    #[error("postgres error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("metadata: {0}")]
    Decode(String),

    #[error("metadata: row not found for url {0}")]
    Missing(String),
}

impl From<PostgresMetadataError> for Error {
    fn from(e: PostgresMetadataError) -> Self {
        Error::Metadata(e.to_string())
    }
}

type LocalResult<T> = std::result::Result<T, PostgresMetadataError>;

/// Postgres-backed [`MetadataStore`]. Writes go through a transaction
/// so the `url_metadata` row and its `url_history` event commit
/// atomically.
///
/// Construct via [`PostgresMetadataStore::connect`] (which also
/// applies migrations) or [`PostgresMetadataStore::with_pool`] when
/// the caller already manages a pool.
pub struct PostgresMetadataStore {
    pool: PgPool,
}

impl std::fmt::Debug for PostgresMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresMetadataStore").finish()
    }
}

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

impl PostgresMetadataStore {
    /// Open a pool against `database_url`, run pending migrations,
    /// return a ready store. Use this in production wiring.
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .map_err(PostgresMetadataError::from)?;
        Self::migrate(&pool).await?;
        info!(max_connections, "PostgresMetadataStore ready");
        Ok(Self { pool })
    }

    /// Wrap an externally-managed pool. Migrations are NOT applied;
    /// the caller is responsible for schema parity. Useful for
    /// integration tests that own the container lifecycle.
    pub fn with_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Apply pending migrations against `pool`. Idempotent.
    pub async fn migrate(pool: &PgPool) -> Result<()> {
        MIGRATOR
            .run(pool)
            .await
            .map_err(PostgresMetadataError::from)?;
        Ok(())
    }

    /// Number of URLs currently in the dead-letter state. Used as a
    /// metric and for ops dashboards. Counts metadata rows, not
    /// history events; a URL that ends up `permanently_failed` is
    /// counted once even if it was retried multiple times before.
    pub async fn dlq_size(&self) -> Result<u64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM url_metadata WHERE status = $1")
            .bind(STATUS_PERMANENTLY_FAILED)
            .fetch_one(&self.pool)
            .await
            .map_err(PostgresMetadataError::from)?;
        metrics::gauge!(crate::metrics::DLQ_SIZE).set(count as f64);
        Ok(count as u64)
    }

    /// Refresh the `crawlrs_metadata_pool_pending` gauge from the
    /// sqlx pool's published stats. Sqlx doesn't expose the
    /// "currently-blocked acquire futures" count directly; we
    /// approximate via `size - num_idle`, which is "currently
    /// outstanding connections" and serves as a saturation indicator.
    /// Called by the binary's maintenance loop per scrape interval.
    pub fn record_pool_metrics(&self) {
        let active = self.pool.size().saturating_sub(self.pool.num_idle() as u32);
        metrics::gauge!(crate::metrics::METADATA_POOL_PENDING).set(active as f64);
    }

    /// Borrow the underlying pool. Exposed so the runtime can share
    /// the same pool across collaborators (e.g. metrics queries).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl MetadataStore for PostgresMetadataStore {
    #[tracing::instrument(skip(self), fields(url = %url))]
    async fn get(&self, url: &CanonicalUrl) -> Result<Option<UrlMetadata>> {
        let _timer = crate::metrics::QueryTimer::new(crate::metrics::OP_GET);
        let row: Option<UrlRow> = sqlx::query_as::<_, UrlRow>(
            "SELECT url, status, retry_count, blob_path, content_hash, depth,
                    last_run_id, discovered_at, updated_at
             FROM url_metadata WHERE url = $1",
        )
        .bind(url.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresMetadataError::from)?;
        row.map(|r| r.into_metadata(url))
            .transpose()
            .map_err(Error::from)
    }

    #[tracing::instrument(skip(self), fields(url = %url, run_id = %run_id, depth))]
    async fn mark_attempting(&self, url: &CanonicalUrl, run_id: &str, depth: u32) -> Result<()> {
        let _timer = crate::metrics::QueryTimer::new(crate::metrics::OP_MARK_ATTEMPTING);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PostgresMetadataError::from)?;
        let id = upsert_attempting(&mut tx, url, run_id, depth as i32).await?;
        insert_history(&mut tx, id, run_id, EVENT_ATTEMPTED, None, None).await?;
        tx.commit().await.map_err(PostgresMetadataError::from)?;
        debug!(url = url.as_str(), "mark_attempting");
        Ok(())
    }

    #[tracing::instrument(skip(self, record), fields(url = %record.url, attempt = %record.attempt_id, blob_path = %record.blob_path, outbound_n = record.outbound.len()))]
    async fn mark_succeeded(&self, record: &SuccessRecord<'_>) -> Result<()> {
        let _timer = crate::metrics::QueryTimer::new(crate::metrics::OP_MARK_SUCCEEDED);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PostgresMetadataError::from)?;

        // Three-step orchestration, all in one transaction so the
        // ledger update, the history append, and the outbox writes
        // commit atomically.
        let (id, last_run_id) =
            update_url_to_succeeded(&mut tx, record.url, record.blob_path, record.content_hash)
                .await?;
        let detail = json!({ "blob_path": record.blob_path });
        insert_history(
            &mut tx,
            id,
            &last_run_id,
            EVENT_SUCCEEDED,
            Some(detail),
            Some(record.attempt_id.as_str()),
        )
        .await?;
        for child in record.outbound {
            insert_outbox_row(&mut tx, id, record.attempt_id.as_str(), child).await?;
        }

        tx.commit().await.map_err(PostgresMetadataError::from)?;
        debug!(
            url = record.url.as_str(),
            outbound_n = record.outbound.len(),
            "mark_succeeded"
        );
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(url = %url, kind = ?kind))]
    async fn mark_failed(&self, url: &CanonicalUrl, kind: FailureKind) -> Result<u32> {
        let _timer = crate::metrics::QueryTimer::new(crate::metrics::OP_MARK_FAILED);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PostgresMetadataError::from)?;
        let row: Option<(i64, String, i32)> = sqlx::query_as(
            "UPDATE url_metadata
                SET status = $1,
                    retry_count = retry_count + 1,
                    updated_at = NOW()
              WHERE url = $2
              RETURNING id, last_run_id, retry_count",
        )
        .bind(STATUS_FAILED_TRANSIENT)
        .bind(url.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(PostgresMetadataError::from)?;

        let (id, last_run_id, retry_count) =
            row.ok_or_else(|| PostgresMetadataError::Missing(url.as_str().to_string()))?;
        let detail = json!({ "kind": failure_kind_str(kind) });
        insert_history(&mut tx, id, &last_run_id, EVENT_FAILED, Some(detail), None).await?;
        tx.commit().await.map_err(PostgresMetadataError::from)?;
        debug!(url = url.as_str(), retry_count, "mark_failed");
        Ok(retry_count as u32)
    }

    #[tracing::instrument(skip(self), fields(url = %url, reason = %reason))]
    async fn mark_permanently_failed(&self, url: &CanonicalUrl, reason: &str) -> Result<()> {
        let _timer = crate::metrics::QueryTimer::new(crate::metrics::OP_MARK_PERMANENTLY_FAILED);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PostgresMetadataError::from)?;
        let row: Option<(i64, String)> = sqlx::query_as(
            "UPDATE url_metadata
                SET status = $1,
                    updated_at = NOW()
              WHERE url = $2
              RETURNING id, last_run_id",
        )
        .bind(STATUS_PERMANENTLY_FAILED)
        .bind(url.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(PostgresMetadataError::from)?;

        let (id, last_run_id) =
            row.ok_or_else(|| PostgresMetadataError::Missing(url.as_str().to_string()))?;
        let detail = json!({ "reason": reason });
        insert_history(
            &mut tx,
            id,
            &last_run_id,
            EVENT_PERMANENTLY_FAILED,
            Some(detail),
            None,
        )
        .await?;
        tx.commit().await.map_err(PostgresMetadataError::from)?;
        debug!(url = url.as_str(), "mark_permanently_failed");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

async fn update_url_to_succeeded(
    tx: &mut Transaction<'_, Postgres>,
    url: &CanonicalUrl,
    blob_path: &str,
    content_hash: u64,
) -> LocalResult<(i64, String)> {
    let row: Option<(i64, String)> = sqlx::query_as(
        "UPDATE url_metadata
            SET status = $1,
                retry_count = 0,
                blob_path = $2,
                content_hash = $3,
                updated_at = NOW()
          WHERE url = $4
          RETURNING id, last_run_id",
    )
    .bind(STATUS_SUCCEEDED)
    .bind(blob_path)
    .bind(content_hash as i64)
    .bind(url.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    row.ok_or_else(|| PostgresMetadataError::Missing(url.as_str().to_string()))
}

async fn upsert_attempting(
    tx: &mut Transaction<'_, Postgres>,
    url: &CanonicalUrl,
    run_id: &str,
    depth: i32,
) -> LocalResult<i64> {
    let host = url.host().unwrap_or("_unknown_");
    // ON CONFLICT preserves discovered_at and retry_count; only the
    // mutable lifecycle fields move on a re-attempt.
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO url_metadata (url, host, status, last_run_id, depth)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (url) DO UPDATE SET
             status      = EXCLUDED.status,
             last_run_id = EXCLUDED.last_run_id,
             depth       = EXCLUDED.depth,
             updated_at  = NOW()
         RETURNING id",
    )
    .bind(url.as_str())
    .bind(host)
    .bind(STATUS_IN_PROGRESS)
    .bind(run_id)
    .bind(depth)
    .fetch_one(&mut **tx)
    .await?;
    Ok(id)
}

async fn insert_history(
    tx: &mut Transaction<'_, Postgres>,
    url_id: i64,
    run_id: &str,
    event: &str,
    detail: Option<serde_json::Value>,
    attempt_id: Option<&str>,
) -> LocalResult<()> {
    // ON CONFLICT (url_id, attempt_id) DO NOTHING: redelivery of the
    // same Frontier attempt (same stream entry id) is idempotent. The
    // unique constraint allows multiple NULL attempt_id rows so legacy
    // events without correlation tokens (e.g. the `attempted` event)
    // continue to work.
    sqlx::query(
        "INSERT INTO url_history (url_id, run_id, event, detail, attempt_id)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (url_id, attempt_id) DO NOTHING",
    )
    .bind(url_id)
    .bind(run_id)
    .bind(event)
    .bind(detail)
    .bind(attempt_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_outbox_row(
    tx: &mut Transaction<'_, Postgres>,
    parent_url_id: i64,
    parent_attempt_id: &str,
    child: &UrlEntry,
) -> LocalResult<()> {
    // The unique (parent_url_id, parent_attempt_id, url) constraint
    // catches a redelivered attempt's second pass: the second insert
    // collapses to a no-op so the publisher drains a deterministic
    // set of rows.
    sqlx::query(
        "INSERT INTO frontier_outbox
            (url, depth, discovered_from, parent_url_id, parent_attempt_id)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (parent_url_id, parent_attempt_id, url) DO NOTHING",
    )
    .bind(child.url.as_str())
    .bind(child.depth as i32)
    .bind(child.discovered_from.as_ref().map(|u| u.as_str()))
    .bind(parent_url_id)
    .bind(parent_attempt_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[async_trait]
impl OutboxReader for PostgresMetadataStore {
    #[tracing::instrument(skip(self), fields(max))]
    async fn fetch_unpublished(&self, max: usize) -> Result<Vec<OutboxEntry>> {
        let rows: Vec<OutboxRow> = sqlx::query_as::<_, OutboxRow>(
            "SELECT id, url, depth, discovered_from
             FROM frontier_outbox
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT $1",
        )
        .bind(max as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(PostgresMetadataError::from)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            // A malformed URL in the outbox means a corrupted parent
            // write; we surface the parse error so the operator
            // notices, rather than silently dropping the row.
            let url = CanonicalUrl::parse(&row.url).map_err(|e| {
                Error::Metadata(format!(
                    "outbox row {} carries unparsable url {}: {e}",
                    row.id, row.url
                ))
            })?;
            let discovered_from = row
                .discovered_from
                .as_deref()
                .map(CanonicalUrl::parse)
                .transpose()
                .map_err(|e| {
                    Error::Metadata(format!(
                        "outbox row {} carries unparsable discovered_from: {e}",
                        row.id
                    ))
                })?;
            out.push(OutboxEntry {
                // BIGSERIAL is non-negative in practice; the cast
                // narrows to our domain newtype without risk of
                // wraparound for any id within Postgres BIGSERIAL
                // range.
                id: OutboxRowId::new(row.id as u64),
                entry: UrlEntry {
                    url,
                    depth: row.depth as u32,
                    discovered_from,
                },
            });
        }
        Ok(out)
    }

    #[tracing::instrument(skip(self, ids), fields(n = ids.len()))]
    async fn mark_published(&self, ids: &[OutboxRowId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        // Convert the domain newtype to BIGINT for sqlx. The cast is
        // safe within Postgres BIGSERIAL range (always non-negative).
        let pg_ids: Vec<i64> = ids.iter().map(|id| id.value() as i64).collect();
        // Idempotent: a row already marked published gets its
        // published_at left alone (the WHERE clause filters it).
        sqlx::query(
            "UPDATE frontier_outbox
                SET published_at = NOW()
              WHERE id = ANY($1)
                AND published_at IS NULL",
        )
        .bind(&pg_ids)
        .execute(&self.pool)
        .await
        .map_err(PostgresMetadataError::from)?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct OutboxRow {
    id: i64,
    url: String,
    depth: i32,
    discovered_from: Option<String>,
}

fn failure_kind_str(kind: FailureKind) -> &'static str {
    match kind {
        FailureKind::TooManyRequests => "too_many_requests",
        FailureKind::ServiceUnavailable => "service_unavailable",
        FailureKind::ConnectReset => "connect_reset",
        FailureKind::Timeout => "timeout",
        FailureKind::Other => "other",
    }
}

fn parse_status(s: &str) -> LocalResult<UrlStatus> {
    Ok(match s {
        STATUS_PENDING => UrlStatus::Pending,
        STATUS_IN_PROGRESS => UrlStatus::InProgress,
        STATUS_SUCCEEDED => UrlStatus::Succeeded,
        STATUS_FAILED_TRANSIENT => UrlStatus::FailedTransient,
        STATUS_PERMANENTLY_FAILED => UrlStatus::PermanentlyFailed,
        STATUS_SKIPPED => UrlStatus::Skipped,
        other => {
            return Err(PostgresMetadataError::Decode(format!(
                "unknown status string: {other}"
            )));
        }
    })
}

#[derive(sqlx::FromRow)]
struct UrlRow {
    #[allow(dead_code)] // url is supplied by the caller; kept for future use
    url: String,
    status: String,
    retry_count: i32,
    blob_path: Option<String>,
    content_hash: Option<i64>,
    depth: i32,
    last_run_id: String,
    discovered_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl UrlRow {
    fn into_metadata(self, url: &CanonicalUrl) -> LocalResult<UrlMetadata> {
        Ok(UrlMetadata {
            url: url.clone(),
            status: parse_status(&self.status)?,
            retry_count: self.retry_count.max(0) as u32,
            blob_path: self.blob_path,
            content_hash: self.content_hash.map(|v| v as u64),
            depth: self.depth.max(0) as u32,
            last_run_id: self.last_run_id,
            discovered_at: datetime_to_system_time(self.discovered_at),
            updated_at: datetime_to_system_time(self.updated_at),
        })
    }
}

fn datetime_to_system_time(dt: DateTime<Utc>) -> SystemTime {
    let secs = dt.timestamp();
    let nanos = dt.timestamp_subsec_nanos();
    if secs >= 0 {
        SystemTime::UNIX_EPOCH + std::time::Duration::new(secs as u64, nanos)
    } else {
        // Pre-epoch timestamps cannot occur in our schema (NOW() at
        // insert), but encode defensively.
        let abs_secs = (-secs) as u64;
        SystemTime::UNIX_EPOCH - std::time::Duration::new(abs_secs, 0)
            + std::time::Duration::new(0, nanos)
    }
}

// Inline because: `parse_status`, `failure_kind_str`, and
// `datetime_to_system_time` are all private boundary functions that
// translate Postgres-vocabulary (TEXT statuses, BIGINT timestamps,
// chrono::DateTime) into our domain types. Making them `pub` would
// commit the public API to a specific DB schema and break a future
// swap to DynamoDB / Scylla. They stay private; their tests stay
// here.
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn status_round_trips_through_string() {
        for (s, expected) in [
            (STATUS_PENDING, UrlStatus::Pending),
            (STATUS_IN_PROGRESS, UrlStatus::InProgress),
            (STATUS_SUCCEEDED, UrlStatus::Succeeded),
            (STATUS_FAILED_TRANSIENT, UrlStatus::FailedTransient),
            (STATUS_PERMANENTLY_FAILED, UrlStatus::PermanentlyFailed),
            (STATUS_SKIPPED, UrlStatus::Skipped),
        ] {
            assert_eq!(parse_status(s).unwrap(), expected);
        }
    }

    #[test]
    fn unknown_status_string_fails_to_decode() {
        assert!(parse_status("nonsense").is_err());
    }

    #[test]
    fn failure_kind_str_is_stable() {
        assert_eq!(
            failure_kind_str(FailureKind::TooManyRequests),
            "too_many_requests"
        );
        assert_eq!(failure_kind_str(FailureKind::Other), "other");
    }

    #[test]
    fn datetime_round_trips_through_system_time() {
        let now = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
        let st = datetime_to_system_time(now);
        let back: DateTime<Utc> = st.into();
        assert_eq!(back.timestamp(), now.timestamp());
    }
}
