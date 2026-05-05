//! `lol_html`-backed implementation of [`crawlrs_core::Parser`].
//!
//! [`LolHtmlParser`] takes a [`FetchResponse`](crawlrs_core::FetchResponse)
//! body, runs it through `lol_html`'s streaming rewriter, and produces a
//! [`ParsedDocument`](crawlrs_core::ParsedDocument) with title, visible
//! text, and canonicalized outbound links.

pub mod metrics;
pub mod parser;

pub use parser::LolHtmlParser;
