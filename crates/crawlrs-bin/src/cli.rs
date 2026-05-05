//! clap-derive CLI surface.
//!
//! Three subcommands:
//!
//! - `crawlrs crawl --config <path> [--seeds <path>]` - start the crawler.
//! - `crawlrs validate --config <path>` - parse the config and emit a
//!   one-line summary; exits non-zero on parse / structural errors.
//! - `crawlrs version` - print the binary version.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "crawlrs", version, about = "Distributed web crawler.")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the crawler. Loads the config, builds the runtime, mounts
    /// the HTTP host on the configured port, and runs until SIGTERM.
    Crawl(CrawlArgs),
    /// Parse the config file and print a one-line summary. Exits
    /// non-zero on parse error or schema violation.
    Validate(ValidateArgs),
    /// Print the binary version (matches the workspace package version).
    Version,
}

#[derive(Debug, clap::Args)]
pub struct CrawlArgs {
    /// Path to the `crawl.toml` config file.
    #[arg(short, long, env = "CRAWLRS_CONFIG", default_value = "crawl.toml")]
    pub config: PathBuf,

    /// Optional seeds file: one URL per line, blank/`#`-prefixed lines
    /// ignored. URLs are submitted to the frontier before the worker
    /// pool starts.
    #[arg(long, env = "CRAWLRS_SEEDS")]
    pub seeds: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct ValidateArgs {
    /// Path to the `crawl.toml` config file.
    #[arg(short, long, env = "CRAWLRS_CONFIG", default_value = "crawl.toml")]
    pub config: PathBuf,
}
