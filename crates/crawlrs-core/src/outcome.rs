//! Per-stage outcome enums.
//!
//! Modeled after `data_lab.interfaces.dto`'s {Scraping,Parsing,Saving}Outcome
//! split. Each stage records its own outcome on `PipelineContext`; observers
//! (metrics, retry policy, audit log) inspect the enum variant rather than
//! parsing free-form error strings.

use serde::{Deserialize, Serialize};

/// Outcome of the fetch stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchOutcome {
    Pending,
    /// 2xx response received and body read.
    Success,
    /// 5xx, timeout, connection reset: retryable.
    TransientFailure,
    /// DNS NXDOMAIN, refused connection, bad TLS: not worth retrying.
    PermanentFailure,
    /// 404 specifically, treated separately because it's neither
    /// "the network broke" nor "we got data".
    NotFound,
    /// robots.txt disallowed before we sent the request.
    BlockedByRobots,
    /// URL or content already seen, no fetch was performed.
    SkippedDuplicate,
}

/// Outcome of the parse stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseOutcome {
    Pending,
    Success,
    Failed,
    /// Non-HTML content (PDF, image, binary), oversized body, or otherwise
    /// uninteresting to the v1 parser.
    Skipped,
}

/// Outcome of the store stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreOutcome {
    Pending,
    Success,
    Failed,
    /// Nothing to store (e.g. parse was skipped).
    Skipped,
}

/// Aggregated final outcome of one URL's pipeline run.
///
/// Used by retry/backoff logic to pick a policy. Mirrors the role of
/// `OrchestrationOutcome` in Crustdata's data_lab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrawlOutcome {
    Success,
    TransientFailure,
    PermanentFailure,
    NotFound,
    Skipped,
}
