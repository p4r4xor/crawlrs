//! `PipelineContext`: the value that flows fetch -> parse -> store.
//!
//! Every URL gets one. Each stage updates its own outcome field as it
//! processes the URL, so by the time the pipeline finishes we have a complete
//! per-stage record without callers having to thread state manually.

use serde::{Deserialize, Serialize};

use crate::outcome::{CrawlOutcome, FetchOutcome, ParseOutcome, StoreOutcome};
use crate::url::CanonicalUrl;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineContext {
    pub url: CanonicalUrl,
    pub depth: u32,
    pub fetch_outcome: FetchOutcome,
    pub parse_outcome: ParseOutcome,
    pub store_outcome: StoreOutcome,
    pub error: Option<String>,
}

impl PipelineContext {
    pub fn new(url: CanonicalUrl, depth: u32) -> Self {
        Self {
            url,
            depth,
            fetch_outcome: FetchOutcome::Pending,
            parse_outcome: ParseOutcome::Pending,
            store_outcome: StoreOutcome::Pending,
            error: None,
        }
    }

    /// Collapse the per-stage outcomes into a single CrawlOutcome.
    ///
    /// Order of precedence: a successful store wins; otherwise the first
    /// definitive failure decides; otherwise we report Skipped.
    pub fn final_outcome(&self) -> CrawlOutcome {
        if self.store_outcome == StoreOutcome::Success {
            return CrawlOutcome::Success;
        }
        if self.fetch_outcome == FetchOutcome::NotFound {
            return CrawlOutcome::NotFound;
        }
        if matches!(
            self.fetch_outcome,
            FetchOutcome::PermanentFailure | FetchOutcome::BlockedByRobots
        ) {
            return CrawlOutcome::PermanentFailure;
        }
        if self.fetch_outcome == FetchOutcome::TransientFailure
            || self.parse_outcome == ParseOutcome::Failed
            || self.store_outcome == StoreOutcome::Failed
        {
            return CrawlOutcome::TransientFailure;
        }
        CrawlOutcome::Skipped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> PipelineContext {
        PipelineContext::new(CanonicalUrl::parse("https://x.test/").unwrap(), 0)
    }

    #[test]
    fn pending_pipeline_is_skipped() {
        assert_eq!(ctx().final_outcome(), CrawlOutcome::Skipped);
    }

    #[test]
    fn store_success_wins() {
        let mut c = ctx();
        c.fetch_outcome = FetchOutcome::Success;
        c.parse_outcome = ParseOutcome::Success;
        c.store_outcome = StoreOutcome::Success;
        assert_eq!(c.final_outcome(), CrawlOutcome::Success);
    }

    #[test]
    fn fetch_404_reports_not_found() {
        let mut c = ctx();
        c.fetch_outcome = FetchOutcome::NotFound;
        assert_eq!(c.final_outcome(), CrawlOutcome::NotFound);
    }

    #[test]
    fn transient_fetch_is_retryable() {
        let mut c = ctx();
        c.fetch_outcome = FetchOutcome::TransientFailure;
        assert_eq!(c.final_outcome(), CrawlOutcome::TransientFailure);
    }

    #[test]
    fn robots_block_is_permanent() {
        let mut c = ctx();
        c.fetch_outcome = FetchOutcome::BlockedByRobots;
        assert_eq!(c.final_outcome(), CrawlOutcome::PermanentFailure);
    }
}
