//! `InMemoryStore`: a `Store` impl that holds parsed documents in a Vec.

use std::sync::Mutex;

use async_trait::async_trait;
use crawlrs_core::{ParsedDocument, Result, Store, StoreRecord};

/// Records every `ParsedDocument` written to it. `urls()` returns the
/// list of URLs whose docs landed; `documents()` returns clones of the
/// docs themselves. `write` returns `memory://<url>` as the blob path
/// so the metadata layer's mark_succeeded path is exercised end-to-end.
#[derive(Default)]
pub struct InMemoryStore {
    written: Mutex<Vec<ParsedDocument>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// URLs of every doc written so far, in write order.
    pub fn urls(&self) -> Vec<String> {
        self.written
            .lock()
            .unwrap()
            .iter()
            .map(|d| d.url.as_str().to_string())
            .collect()
    }

    /// Clones of every doc written so far.
    pub fn documents(&self) -> Vec<ParsedDocument> {
        self.written.lock().unwrap().clone()
    }

    /// Number of docs written. Cheaper than `urls().len()` when callers
    /// only need a count.
    pub fn len(&self) -> usize {
        self.written.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn write(&self, record: &StoreRecord<'_>) -> Result<String> {
        let blob_path = format!("memory://{}", record.doc.url.as_str());
        self.written.lock().unwrap().push(record.doc.clone());
        Ok(blob_path)
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
