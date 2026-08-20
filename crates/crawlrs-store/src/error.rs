//! Internal error type. Wraps backend / encoder errors and converts
//! to the domain `crawlrs_core::Error::Store(String)` at the trait
//! boundary so the public surface stays in core's error type.

use crawlrs_core::Error;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum StoreError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Error::Store(e.to_string())
    }
}
