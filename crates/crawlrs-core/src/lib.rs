//! Core types, errors, and traits for the crawlrs web crawler.
//!
//! This crate is dependency-light by design: it defines the *shapes* (data
//! structs and traits) that every other crate compiles against. No I/O, no
//! networking, no storage backends live here.

pub mod adapter;
pub mod config;
pub mod context;
pub mod error;
pub mod outcome;
pub mod proxy;
pub mod traits;
pub mod types;
pub mod url;

pub use adapter::{SiteAdapter, SiteAdapterRegistry};
pub use config::CrawlConfig;
pub use context::PipelineContext;
pub use error::{Error, Result};
pub use outcome::{CrawlOutcome, FetchOutcome, ParseOutcome, StoreOutcome};
pub use proxy::{ProxyOutcome, ProxyResolver, ProxySelection};
pub use traits::{
    FailureKind, Fetcher, Frontier, HostHashShardPolicy, MetadataStore, Parser, PoliteDecision,
    Politeness, ShardKey, ShardingPolicy, SingleShardPolicy, Store,
};
pub use types::{
    FetchRequest, FetchResponse, ParsedDocument, RedirectHop, UrlEntry, UrlMetadata, UrlStatus,
};
pub use url::CanonicalUrl;
