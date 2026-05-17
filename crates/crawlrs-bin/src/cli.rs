//! clap-derive CLI surface.
//!
//! Subcommands:
//!
//! - `crawlrs crawl --config <path>` - run the worker pool.
//! - `crawlrs seed --config <path> --path <seeds>` - one-shot bootstrap
//!   that loads URLs into the Frontier. Intended to run from a
//!   post-install Helm hook (Job) so pod restarts never re-seed.
//! - `crawlrs validate --config <path>` - parse + summarise the config.
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
    /// Start the worker pool. Loads the config, builds the runtime,
    /// mounts the HTTP host on the configured port, and runs until
    /// SIGTERM. Does not load seeds; seeding is a separate concern
    /// owned by `crawlrs seed` (run once on chart install).
    Crawl(CrawlArgs),
    /// One-shot: load URLs from a file into the Frontier and exit.
    /// Per-batch failures (Redis OOM, transient errors) are logged
    /// and the loader continues; the process exits non-zero only if
    /// no batch succeeded. Intended for a Helm post-install Job.
    Seed(SeedArgs),
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
}

#[derive(Debug, clap::Args)]
pub struct SeedArgs {
    /// Path to the `crawl.toml` config file (used for Redis URL,
    /// sharding policy, bloom filter sizing, per-host quotas).
    #[arg(short, long, env = "CRAWLRS_CONFIG", default_value = "crawl.toml")]
    pub config: PathBuf,

    /// Path to the seeds file. One URL per line; blank lines and
    /// `#`-prefixed lines are ignored.
    #[arg(short = 'p', long, env = "CRAWLRS_SEEDS")]
    pub path: PathBuf,

    /// How many URLs to submit per `Frontier::submit_batch` call.
    /// Smaller batches mean more round-trips but bound the loss on
    /// any single Redis error to one batch.
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,
}

#[derive(Debug, clap::Args)]
pub struct ValidateArgs {
    /// Path to the `crawl.toml` config file.
    #[arg(short, long, env = "CRAWLRS_CONFIG", default_value = "crawl.toml")]
    pub config: PathBuf,
}
