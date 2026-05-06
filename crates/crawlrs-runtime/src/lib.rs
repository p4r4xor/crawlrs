//! Pipeline orchestration: composes the trait impls in `crawlrs-core`
//! into a working tokio worker pool.
//!
//! [`Crawler`] is the top-level handle. Build it via [`CrawlerBuilder`],
//! call [`Crawler::run`] to drive the loop, [`Crawler::shutdown`] to
//! signal graceful exit. Each worker runs the full per-URL pipeline
//! (claim -> politeness check -> fetch -> parse -> submit links ->
//! store -> ack) as a single future; backpressure flows naturally via
//! await-points rather than via explicit channels.
//!
//! A separate maintenance task drives [`Frontier::tick`] on a fixed
//! cadence and once during shutdown, so stranded URLs from crashed
//! peers get reclaimed without the workers having to coordinate it.

pub mod crawler;
pub mod failure;
pub mod maintenance;
pub mod metrics;
pub mod supervisor;
pub mod worker;

pub use crawler::{Crawler, CrawlerBuilder, CrawlerConfig, CrawlerError};
pub use failure::{
    classify_status, classify_transport_error, extract_retry_after, parse_retry_after,
};
pub use supervisor::{RestartPolicy, supervise_worker};
