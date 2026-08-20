//! `InMemoryStore`: a `Store` impl that holds parsed documents in a Vec.

use std::sync::Mutex;

use async_trait::async_trait;
use crawlrs_core::{Error, ParsedDocument, Result, Store, StoreRecord};

/// Records every `ParsedDocument` written to it. `urls()` returns the
/// list of URLs whose docs landed; `documents()` returns clones of the
/// docs themselves. `write` returns `memory://<url>` as the blob path
/// so the metadata layer's mark_succeeded path is exercised end-to-end.
#[derive(Default)]
pub struct InMemoryStore {
    written: Mutex<Vec<ParsedDocument>>,
    /// When set, `write` returns an error instead of recording. Lets
    /// tests reach the runtime's degraded "store write failed" branch
    /// that canned-response fakes otherwise can't exercise.
    fail_writes: Mutex<bool>,
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

    /// Number of docs written. Cheaper than `urls().len()` when callers
    /// only need a count.
    pub fn len(&self) -> usize {
        self.written.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Make `write` fail (`true`) or succeed (`false`). A failing store
    /// drives the worker's "store write failed; acking anyway" path.
    pub fn set_write_failure(&self, failing: bool) {
        *self.fail_writes.lock().unwrap() = failing;
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn write(&self, record: &StoreRecord<'_>) -> Result<String> {
        if *self.fail_writes.lock().unwrap() {
            return Err(Error::Store("injected write failure".into()));
        }
        let blob_path = format!("memory://{}", record.doc.url.as_str());
        self.written.lock().unwrap().push(record.doc.clone());
        Ok(blob_path)
    }
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
