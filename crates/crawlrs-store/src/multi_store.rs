//! `MultiStore`: Composite-over-Strategy fan-out of one `Store::write`
//! call to N inner stores. The runtime sees one `Store` trait object
//! even when the operator has configured Parquet + WARC.
//!
//! Convention: the first-configured store is the *primary*; its
//! returned blob_path is the canonical pointer recorded in the
//! metadata ledger. Every other store's path is logged at debug level
//! but otherwise dropped, since the metadata schema only carries one
//! `blob_path: Option<String>`.
//!
//! `write` failure semantics are fail-fast: any inner write error
//! returns immediately and the runtime treats the call as a write
//! failure. Partial state (one store wrote, another didn't) is
//! acceptable for v1; the runtime's per-URL retry budget covers
//! eventual re-attempt and the at-least-once delivery posture is
//! unchanged.
//!
//! `flush` failure semantics are best-effort: every store is flushed
//! even if an earlier one errors, so one store's flush failure cannot
//! strand the buffered records of the others on the shutdown drain
//! path. All errors are aggregated into a single returned error.

use std::sync::Arc;

use async_trait::async_trait;
use crawlrs_core::{Error, Result, Store, StoreRecord};
use tracing::debug;

pub struct MultiStore {
    stores: Vec<Arc<dyn Store>>,
}

impl MultiStore {
    /// Construct from an ordered list. The first entry is the primary
    /// (its blob_path is what `write()` returns). At least one store
    /// is required; an empty `Vec` returns `Err` so a misconfiguration
    /// surfaces at startup rather than at first write.
    pub fn new(stores: Vec<Arc<dyn Store>>) -> Result<Self> {
        if stores.is_empty() {
            return Err(Error::Store(
                "MultiStore requires at least one inner store".into(),
            ));
        }
        Ok(Self { stores })
    }

    pub fn len(&self) -> usize {
        self.stores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }
}

#[async_trait]
impl Store for MultiStore {
    #[tracing::instrument(
        skip(self, record),
        fields(
            url = %record.doc.url,
            depth = record.depth,
            fanout = self.stores.len(),
        )
    )]
    async fn write(&self, record: &StoreRecord<'_>) -> Result<String> {
        let mut paths = Vec::with_capacity(self.stores.len());
        for store in &self.stores {
            paths.push(store.write(record).await?);
        }
        for (i, path) in paths.iter().enumerate().skip(1) {
            debug!(secondary_index = i, secondary_path = %path, "MultiStore secondary write");
        }
        // SAFETY: `new()` rejects empty Vec, so paths[0] always exists.
        Ok(paths.into_iter().next().expect("non-empty by construction"))
    }

    async fn flush(&self) -> Result<()> {
        // Best-effort: attempt every child even if one fails, so a
        // single store's flush error can't strand the others' buffered
        // records on shutdown. Aggregate and surface afterward.
        let mut errors = Vec::new();
        for store in &self.stores {
            if let Err(flush_error) = store.flush().await {
                errors.push(flush_error.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Store(format!(
                "{} of {} stores failed to flush: {}",
                errors.len(),
                self.stores.len(),
                errors.join("; "),
            )))
        }
    }
}
