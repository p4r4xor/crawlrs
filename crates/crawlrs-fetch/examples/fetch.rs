//! Manual end-to-end smoke test for [`WreqFetcher`].
//!
//! Usage: `cargo run --example fetch -- https://example.com`
//!
//! Optional env vars:
//! - `HTTPS_PROXY` / `HTTP_PROXY`: picked up automatically when
//!   `CRAWLRS_PROXY=env` is set.
//! - `CRAWLRS_PROXY=env|none`: pick a built-in resolver.

use std::sync::Arc;

use crawlrs_core::{CanonicalUrl, FetchRequest, Fetcher};
use crawlrs_fetch::{EnvProxyResolver, NoProxyResolver, WreqFetcher, WreqFetcherConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url_arg = std::env::args()
        .nth(1)
        .ok_or("usage: fetch_one <url>")?;

    let proxy = match std::env::var("CRAWLRS_PROXY").as_deref() {
        Ok("env") => Arc::new(EnvProxyResolver::new()) as Arc<_>,
        _ => Arc::new(NoProxyResolver) as Arc<_>,
    };

    let fetcher = WreqFetcher::new(WreqFetcherConfig {
        proxy,
        ..Default::default()
    })?;

    let url = CanonicalUrl::parse(&url_arg)?;
    let resp = fetcher.fetch(FetchRequest::new(url)).await?;

    println!("status:    {}", resp.status);
    println!("final url: {}", resp.url);
    println!("duration:  {:.2?}", resp.duration);
    println!("body len:  {} bytes", resp.body.len());
    if !resp.redirect_chain.is_empty() {
        println!("redirects:");
        for hop in &resp.redirect_chain {
            println!("  {} -> {} ({})", hop.from, hop.to, hop.status);
        }
    }
    Ok(())
}
