//! `crawlrs-bin` library surface.
//!
//! The binary's entry point lives in `main.rs` and is intentionally
//! thin: it parses CLI args, dispatches to one of the command
//! handlers in this lib, and exits. Everything else (config loading,
//! adapter construction, HTTP server, maintenance loop, shutdown) is
//! a public module so integration tests can compose pieces without
//! shelling out to the binary.

pub mod cli;
pub mod config;
pub mod factory;
pub mod http;
pub mod maintenance;
pub mod run;
pub mod shutdown;
