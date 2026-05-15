//! Entry point. Thin: parse CLI args, install tracing, dispatch to
//! the matching command handler. Everything load-bearing lives in
//! the lib.

// Replace glibc malloc with jemalloc on linux. Two effects: (1)
// dramatically better page-return-to-OS behaviour (the canonical
// fragmentation cliff we see in `VmData - RSS` on glibc shrinks
// substantially with jemalloc's `dirty_decay_ms` / `muzzy_decay_ms`
// purging defaults); (2) when `MALLOC_CONF=prof:true,prof_active:true`
// is set in the environment, jemalloc samples allocations and dumps
// heap profiles parseable by `jeprof`, letting us see which call
// sites are holding memory at any instant. Linux-only because the
// crate doesn't build on Windows; on other Unixes the default
// allocator stays in place.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
