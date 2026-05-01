//! Crate-wide error type.
//!
//! Variants are intentionally coarse: `Fetch`, `Parse`, `Store`, `Frontier`
//! each wrap a string. Implementations attach details via `.to_string()` so
//! that the trait surface stays free of implementation-specific error types
//! (e.g. `wreq::Error`, `parquet::errors::ParquetError`).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid url: {0}")]
    InvalidUrl(#[from] ::url::ParseError),

    #[error("fetch error: {0}")]
    Fetch(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("frontier error: {0}")]
    Frontier(String),

    #[error("metadata error: {0}")]
    Metadata(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
