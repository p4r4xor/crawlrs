//! Core types, errors, and traits for the crawlrs web crawler.
//!
//! This crate is dependency-light by design: it defines the *shapes*
//! (data structs and traits) that every other crate compiles against.
//! No I/O, no networking, no storage backends live here.
//!
//! Layout:
//!
//! - [`traits`] - every public trait (port) the crate publishes. One
//!   file per trait. New abstractions go here.
//! - [`types`] - value types that flow between traits (`UrlEntry`,
//!   `FetchRequest`, `FetchResponse`, `ParsedDocument`, `UrlMetadata`,
//!   `UrlStatus`, `RedirectHop`).
//! - [`url`] - the `CanonicalUrl` newtype + canonicalization rules.
//! - [`error`] - the crate-wide `Error` enum.
//! - [`hash`] - pure helper functions for FNV-1a + content hashing.

pub mod error;
pub mod hash;
pub mod traits;
pub mod types;
pub mod url;

pub use error::{Error, Result};
pub use hash::{content_hash, fnv1a_64};
pub use traits::clock::{Clock, SystemClock, system_clock};
pub use traits::fetcher::Fetcher;
pub use traits::frontier::Frontier;
pub use traits::metadata::MetadataStore;
pub use traits::outbox::{OutboxEntry, OutboxReader};
pub use traits::parser::Parser;
pub use traits::politeness::{FailureKind, PoliteDecision, Politeness};
pub use traits::proxy::{ProxyOutcome, ProxyResolver, ProxySelection};
pub use traits::sharding::{HostHashShardPolicy, ShardKey, ShardingPolicy, SingleShardPolicy};
pub use traits::site_adapter::{SiteAdapter, SiteAdapterRegistry};
pub use traits::store::Store;
pub use types::{
    AttemptId, ClaimedMessage, FetchRequest, FetchResponse, ParsedDocument, RedirectHop,
    StoreRecord, UrlEntry, UrlMetadata, UrlStatus, WorkerIdentity,
};
pub use url::CanonicalUrl;
