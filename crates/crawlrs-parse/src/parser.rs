//! [`Parser`] impl backed by `lol_html`.
//!
//! `lol_html` is a streaming HTML rewriter; it processes bytes as they
//! arrive without building a DOM. We use it as an extractor by registering
//! handlers that *accumulate* into owned locals borrowed by their one
//! handler, and discarding the rewritten output. The lone exception is
//! the `<script>`/`<style>` exclusion-depth counter, shared via
//! `Rc<Cell<u32>>` because the exit handler must be `'static`.
//!
//! The handlers we register:
//!
//! - `<title>` text → captured into the document's title.
//! - `<base href>` → recorded as the base for relative URL resolution.
//! - `<a href>` → raw href values pushed for later canonicalization.
//! - `<script>` / `<style>` → enter/exit handlers maintain a depth
//!   counter so their text content is excluded from the visible-text buffer.
//! - text inside `<body>` → appended to the visible-text buffer when
//!   `excluded_depth == 0`.
//!
//! After streaming, hrefs are resolved against the effective base
//! (`<base href>` if present, otherwise `response.url`) via
//! [`CanonicalUrl::parse_relative`], filtered to http(s) only,
//! filtered through [`extensions::denies`] to drop
//! known non-HTML targets (images, video, archives, office docs,
//! scripts), and deduped.

use std::cell::Cell;
use std::collections::HashSet;
use std::rc::Rc;

use async_trait::async_trait;
use crawlrs_core::{CanonicalUrl, Error, FetchResponse, ParsedDocument, Parser, Result};
use lol_html::{EndTagHandler, HtmlRewriter, Settings, element, text};

use crate::extensions;

#[derive(Debug, Default, Clone, Copy)]
pub struct LolHtmlParser;

impl LolHtmlParser {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Parser for LolHtmlParser {
    #[tracing::instrument(skip(self, response), fields(url = %response.url))]
    async fn parse(&self, response: &FetchResponse) -> Result<ParsedDocument> {
        let started_at = std::time::Instant::now();

        if let Some(reason) = skip_reason(response) {
            metrics::counter!(
                crate::metrics::PARSE_SKIPPED_TOTAL,
                "reason" => reason,
            )
            .increment(1);
            metrics::histogram!(crate::metrics::PARSE_SECONDS)
                .record(started_at.elapsed().as_secs_f64());
            return Ok(ParsedDocument {
                url: response.url.clone(),
                status: response.status,
                title: None,
                text: None,
                outbound_links: Box::new(Vec::new()),
                fetched_at: response.fetched_at,
            });
        }

        let extracted = extract_html(&response.body)?;

        let effective_base = match &extracted.base_href {
            Some(href) => match CanonicalUrl::parse_relative(&response.url, href) {
                Ok(base) => base,
                Err(_) => {
                    metrics::counter!(crate::metrics::PARSE_BASE_HREF_INVALID_TOTAL).increment(1);
                    response.url.clone()
                }
            },
            None => response.url.clone(),
        };

        let (outbound_links, extension_denied) =
            resolve_links(&extracted.raw_links, &effective_base);
        if extension_denied > 0 {
            metrics::counter!(crate::metrics::PARSE_LINKS_EXTENSION_DENIED_TOTAL)
                .increment(extension_denied as u64);
        }

        let title = extracted.title.and_then(|mut raw| {
            raw.truncate(raw.trim_end().len());
            let leading = raw.len() - raw.trim_start().len();
            raw.drain(..leading);
            if raw.is_empty() { None } else { Some(raw) }
        });

        let text = if extracted.collapsed_text.is_empty() {
            None
        } else {
            Some(extracted.collapsed_text)
        };

        metrics::histogram!(crate::metrics::PARSE_SECONDS)
            .record(started_at.elapsed().as_secs_f64());
        metrics::histogram!(crate::metrics::PARSE_LINKS_DISCOVERED)
            .record(outbound_links.len() as f64);

        Ok(ParsedDocument {
            url: response.url.clone(),
            status: response.status,
            title,
            text: text.map(Box::new),
            outbound_links: Box::new(outbound_links),
            fetched_at: response.fetched_at,
        })
    }
}

#[derive(Debug, Default)]
struct Extracted {
    title: Option<String>,
    base_href: Option<String>,
    raw_links: Vec<String>,
    collapsed_text: String,
}

fn extract_html(body: &[u8]) -> Result<Extracted> {
    // Pre-size the workhorse buffers to typical magnitudes so we
    // skip the doubling-growth chain on the parser's hot path.
    // - `raw_links`: most HTML pages have ~50-200 hrefs; 128 is a
    //   median that avoids realloc for ~80% of pages.
    // - `collapsed_text`: assume ~1/3 of body bytes survive as visible
    //   text (markup stripped). Clamped at the body length so we
    //   never over-reserve on tiny pages.
    let text_capacity = body.len() / 3;
    let mut title: Option<String> = None;
    let mut base_href: Option<String> = None;
    let mut raw_links: Vec<String> = Vec::with_capacity(128);
    let mut collapsed_text = String::with_capacity(text_capacity);
    let excluded_depth: Rc<Cell<u32>> = Rc::new(Cell::new(0));

    let depth_for_enter = Rc::clone(&excluded_depth);
    let depth_for_text = Rc::clone(&excluded_depth);
    let text_buffer = &mut collapsed_text;
    // Whitespace is collapsed as chunks stream in, so the accumulator
    // never holds the raw (uncollapsed) body and no second pass is
    // needed. `last_was_whitespace` starts true to drop leading space.
    let mut last_was_whitespace = true;

    {
        let mut rewriter = HtmlRewriter::new(
            Settings {
                element_content_handlers: vec![
                    text!("title", |chunk| {
                        let chunk_text = chunk.as_str();
                        match title.as_mut() {
                            Some(existing) => existing.push_str(chunk_text),
                            None => title = Some(chunk_text.to_string()),
                        }
                        Ok(())
                    }),
                    element!("base[href]", |el| {
                        if let Some(href) = el.get_attribute("href") {
                            base_href = Some(href);
                        }
                        Ok(())
                    }),
                    element!("a[href]", |el| {
                        if let Some(href) = el.get_attribute("href") {
                            raw_links.push(href);
                        }
                        Ok(())
                    }),
                    element!("script, style", move |el| {
                        depth_for_enter.set(depth_for_enter.get() + 1);
                        let depth_for_exit = Rc::clone(&depth_for_enter);
                        let exit_handler: EndTagHandler<'static> = Box::new(move |_end| {
                            depth_for_exit.set(depth_for_exit.get().saturating_sub(1));
                            Ok(())
                        });
                        el.on_end_tag(exit_handler)?;
                        Ok(())
                    }),
                    text!("body", move |chunk| {
                        if depth_for_text.get() == 0 {
                            for ch in chunk.as_str().chars() {
                                if ch.is_whitespace() {
                                    if !last_was_whitespace {
                                        text_buffer.push(' ');
                                        last_was_whitespace = true;
                                    }
                                } else {
                                    text_buffer.push(ch);
                                    last_was_whitespace = false;
                                }
                            }
                        }
                        Ok(())
                    }),
                ],
                ..Settings::new()
            },
            |_rewritten: &[u8]| {},
        );

        rewriter
            .write(body)
            .map_err(|err| Error::Parse(format!("lol_html write failed: {err}")))?;
        rewriter
            .end()
            .map_err(|err| Error::Parse(format!("lol_html end failed: {err}")))?;
    }

    if collapsed_text.ends_with(' ') {
        collapsed_text.pop();
    }

    Ok(Extracted {
        title,
        base_href,
        raw_links,
        collapsed_text,
    })
}

/// Resolve raw hrefs to canonical absolute URLs and filter.
///
/// Returns `(kept_urls, extension_denied_count)`. The denied-count is
/// surfaced separately so the caller can emit it as a counter; we
/// don't allocate a parallel `Vec` for dropped URLs because the
/// per-fetch metric only needs the cardinality, not the values.
fn resolve_links(raw: &[String], base: &CanonicalUrl) -> (Vec<CanonicalUrl>, usize) {
    let mut seen = HashSet::with_capacity(raw.len());
    // Pre-size: upper bound is `raw.len()` (some hrefs drop out for
    // empty / anchor-only / parse-failure / extension-deny reasons,
    // but allocating once up-front beats the doubling chain on link-
    // heavy pages where `raw.len()` is in the hundreds).
    let mut out = Vec::with_capacity(raw.len());
    let mut extension_denied = 0usize;
    for href in raw {
        let trimmed = href.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Ok(resolved) = CanonicalUrl::parse_relative(base, trimmed) else {
            continue;
        };
        if !resolved.is_http() {
            continue;
        }
        if extensions::denies(&resolved) {
            extension_denied += 1;
            continue;
        }
        if !seen.contains(&resolved) {
            seen.insert(resolved.clone());
            out.push(resolved);
        }
    }
    (out, extension_denied)
}

/// Two-gate filter before lol_html. Returns `Some(label)` if the response
/// should bypass parsing entirely; `None` if the body is parseable HTML.
///
/// Gate 1 (cheap): `Content-Type` header check. Catches polite servers
/// that label their binary content honestly. The non-HTML set covers
/// images, fonts, archives, PDFs, audio, video, and generic
/// `application/octet-stream`.
///
/// Gate 2 (slightly more expensive): SIMD UTF-8 validation on the body.
/// Catches binary content served with a `Content-Type: text/html` lie.
/// Falls through to lol_html only when the body is valid UTF-8.
fn skip_reason(response: &FetchResponse) -> Option<&'static str> {
    let content_type = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.as_str())
        .unwrap_or("");
    if is_binary_content_type(content_type) {
        return Some("binary_content_type");
    }
    if !response.body.is_empty() && simdutf8::basic::from_utf8(&response.body).is_err() {
        return Some("invalid_utf8");
    }
    None
}

const BINARY_CONTENT_TYPES: &[&str] = &[
    "application/pdf",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/x-tar",
    "application/x-7z-compressed",
    "application/x-rar-compressed",
    "application/x-bzip2",
    "application/octet-stream",
    "application/msword",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
];

const BINARY_CONTENT_TYPE_PREFIXES: &[&str] = &["image/", "audio/", "video/", "font/"];

fn is_binary_content_type(ct: &str) -> bool {
    let trimmed = ct.split(';').next().unwrap_or("").trim();
    if trimmed.is_empty() {
        return false;
    }
    if BINARY_CONTENT_TYPES
        .iter()
        .any(|candidate| trimmed.eq_ignore_ascii_case(candidate))
    {
        return true;
    }
    let bytes = trimmed.as_bytes();
    BINARY_CONTENT_TYPE_PREFIXES.iter().any(|prefix| {
        bytes.len() >= prefix.len() && bytes[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
    })
}
