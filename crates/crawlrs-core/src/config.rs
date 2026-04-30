//! Top-level crawl configuration.
//!
//! Built to be loadable from a TOML file (`crawl.toml`) or constructed in
//! code. Sub-crates layer their own config sections on top via composition.
//! This struct stays minimal.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CrawlConfig {
    /// Initial URLs to crawl from.
    pub seeds: Vec<String>,

    /// Hard cap on total pages fetched. `None` means unbounded.
    pub max_pages: Option<u64>,

    /// Maximum link depth from any seed. `None` means unbounded.
    pub max_depth: Option<u32>,

    /// Number of concurrent worker tasks.
    pub concurrency: usize,

    /// User-Agent header for outgoing requests.
    pub user_agent: String,

    /// Minimum delay between requests to the same host, in milliseconds.
    pub per_host_delay_ms: u64,

    /// If true, skip URLs disallowed by robots.txt.
    pub respect_robots_txt: bool,

    /// Where to write crawl output.
    pub output_path: PathBuf,
}

impl Default for CrawlConfig {
    fn default() -> Self {
        Self {
            seeds: Vec::new(),
            max_pages: Some(100),
            max_depth: Some(3),
            concurrency: 4,
            user_agent: "crawlrs/0.0.1 (+https://github.com/p4r4xor/crawlrs)".to_string(),
            per_host_delay_ms: 1000,
            respect_robots_txt: true,
            output_path: PathBuf::from("./crawlrs-output.parquet"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_sane() {
        let config = CrawlConfig::default();
        assert!(config.respect_robots_txt);
        assert!(config.concurrency >= 1);
        assert!(config.per_host_delay_ms > 0);
    }
}
