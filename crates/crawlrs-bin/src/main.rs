//! Entry point. Thin: parse CLI args, install tracing, dispatch to
//! the matching command handler. Everything load-bearing lives in
//! the lib.

use anyhow::{Context, Result};
use clap::Parser;
use crawlrs_bin::cli::{Cli, Command};
use crawlrs_bin::config::CrawlrsConfig;
use crawlrs_bin::run;

#[tokio::main]
async fn main() -> Result<()> {
    install_tracing();

    let cli = Cli::parse();
    match cli.command {
        Command::Crawl(args) => run::crawl(args).await,
        Command::Validate(args) => {
            let config = CrawlrsConfig::load(&args.config)
                .with_context(|| format!("loading config {}", args.config.display()))?;
            println!("ok: {}", config.summary());
            Ok(())
        }
        Command::Version => {
            println!("crawlrs {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

fn install_tracing() {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // `RUST_LOG` controls verbosity (per crate); `CRAWLRS_LOG_FORMAT`
    // toggles between text (default; dev-friendly) and JSON
    // (production / log-aggregator-friendly).
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let format = std::env::var("CRAWLRS_LOG_FORMAT").unwrap_or_else(|_| "text".to_string());
    match format.as_str() {
        "json" => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
        _ => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .init();
        }
    }
}
