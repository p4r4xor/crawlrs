//! [`Parser`] impl backed by `lol_html`.
//!
//! `lol_html` is a streaming HTML rewriter; it processes bytes as they
//! arrive without building a DOM. We use it as an extractor by registering
//! handlers that *accumulate* into shared cells (`Rc<RefCell<...>>`) and
//! discarding the rewritten output.
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
//! [`CanonicalUrl::parse_relative`], filtered to http(s) only, and deduped.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;

use async_trait::async_trait;
use crawlrs_core::{CanonicalUrl, Error, FetchResponse, ParsedDocument, Parser, Result};
use lol_html::{EndTagHandler, HtmlRewriter, Settings, element, text};

#[derive(Debug, Default, Clone, Copy)]
pub struct LolHtmlParser;

impl LolHtmlParser {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Parser for LolHtmlParser {
    async fn parse(&self, response: &FetchResponse) -> Result<ParsedDocument> {
        let extracted = extract_html(&response.body)?;

        let effective_base = match &extracted.base_href {
            Some(href) => CanonicalUrl::parse_relative(&response.url, href)
                .unwrap_or_else(|_| response.url.clone()),
            None => response.url.clone(),
        };

        let outbound_links = resolve_links(&extracted.raw_links, &effective_base);

        let title = extracted
            .title
            .map(|raw| raw.trim().to_string())
            .filter(|cleaned| !cleaned.is_empty());

        let text = {
            let collapsed = collapse_whitespace(&extracted.visible_text);
            if collapsed.is_empty() {
                None
            } else {
                Some(collapsed)
            }
        };

        Ok(ParsedDocument {
            url: response.url.clone(),
            status: response.status,
            title,
            text,
            outbound_links,
            fetched_at: response.fetched_at,
        })
    }
}

#[derive(Debug, Default)]
struct Extracted {
    title: Option<String>,
    base_href: Option<String>,
    raw_links: Vec<String>,
    visible_text: String,
}

fn extract_html(body: &[u8]) -> Result<Extracted> {
    let title: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let base_href: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let raw_links: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let visible_text: Rc<RefCell<String>> = Rc::new(RefCell::new(String::new()));
    let excluded_depth: Rc<Cell<u32>> = Rc::new(Cell::new(0));

    let title_for_text = Rc::clone(&title);
    let base_for_handler = Rc::clone(&base_href);
    let links_for_handler = Rc::clone(&raw_links);
    let depth_for_enter = Rc::clone(&excluded_depth);
    let text_buffer = Rc::clone(&visible_text);
    let depth_for_text = Rc::clone(&excluded_depth);

    {
        let mut rewriter = HtmlRewriter::new(
            Settings {
                element_content_handlers: vec![
                    text!("title", move |chunk| {
                        let chunk_text = chunk.as_str();
                        let mut current = title_for_text.borrow_mut();
                        match current.as_mut() {
                            Some(existing) => existing.push_str(chunk_text),
                            None => *current = Some(chunk_text.to_string()),
                        }
                        Ok(())
                    }),
                    element!("base[href]", move |el| {
                        if let Some(href) = el.get_attribute("href") {
                            *base_for_handler.borrow_mut() = Some(href);
                        }
                        Ok(())
                    }),
                    element!("a[href]", move |el| {
                        if let Some(href) = el.get_attribute("href") {
                            links_for_handler.borrow_mut().push(href);
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
                            text_buffer.borrow_mut().push_str(chunk.as_str());
                        }
                        Ok(())
                    }),
                ],
                ..Settings::new()
            },
            |_rewritten: &[u8]| {
                // We don't care about the rewritten output; only the data
                // accumulated by the handlers above.
            },
        );

        rewriter
            .write(body)
            .map_err(|err| Error::Parse(format!("lol_html write failed: {err}")))?;
        rewriter
            .end()
            .map_err(|err| Error::Parse(format!("lol_html end failed: {err}")))?;
    }

    // Closures are dropped here, so the only remaining Rc clones are ours.
    Ok(Extracted {
        title: Rc::try_unwrap(title)
            .ok()
            .and_then(|cell| cell.into_inner()),
        base_href: Rc::try_unwrap(base_href)
            .ok()
            .and_then(|cell| cell.into_inner()),
        raw_links: Rc::try_unwrap(raw_links)
            .map(|cell| cell.into_inner())
            .unwrap_or_default(),
        visible_text: Rc::try_unwrap(visible_text)
            .map(|cell| cell.into_inner())
            .unwrap_or_default(),
    })
}

fn resolve_links(raw: &[String], base: &CanonicalUrl) -> Vec<CanonicalUrl> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
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
        if seen.insert(resolved.clone()) {
            out.push(resolved);
        }
    }
    out
}

/// Collapse runs of whitespace into a single space and trim.
///
/// HTML text content has a lot of incidental whitespace (newlines after
/// tags, indentation). We don't preserve it because the downstream LanceDB
/// embedding case wants a clean prose-like string.
fn collapse_whitespace(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut last_was_whitespace = true; // skip leading whitespace
    for ch in input.chars() {
        if ch.is_whitespace() {
            if !last_was_whitespace {
                out.push(' ');
                last_was_whitespace = true;
            }
        } else {
            out.push(ch);
            last_was_whitespace = false;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::time::Duration;

    fn resp(url: &str, body: &str) -> FetchResponse {
        FetchResponse {
            url: CanonicalUrl::parse(url).unwrap(),
            status: 200,
            headers: HashMap::new(),
            body: Bytes::copy_from_slice(body.as_bytes()),
            redirect_chain: Vec::new(),
            fetched_at: chrono::Utc::now(),
            duration: Duration::from_millis(1),
        }
    }

    #[tokio::test]
    async fn extracts_title() {
        let p = LolHtmlParser::new();
        let r = resp(
            "https://example.com/",
            "<html><head><title>Hello World</title></head><body></body></html>",
        );
        let doc = p.parse(&r).await.unwrap();
        assert_eq!(doc.title.as_deref(), Some("Hello World"));
    }

    #[tokio::test]
    async fn extracts_outbound_links() {
        let p = LolHtmlParser::new();
        let r = resp(
            "https://example.com/page",
            r#"<html><body>
                <a href="/about">About</a>
                <a href="https://other.com/">External</a>
                <a href="contact">Contact</a>
            </body></html>"#,
        );
        let doc = p.parse(&r).await.unwrap();
        let urls: Vec<&str> = doc.outbound_links.iter().map(|u| u.as_str()).collect();
        assert!(urls.contains(&"https://example.com/about"));
        assert!(urls.contains(&"https://other.com/"));
        assert!(urls.contains(&"https://example.com/contact"));
    }

    #[tokio::test]
    async fn honors_base_href() {
        let p = LolHtmlParser::new();
        let r = resp(
            "https://example.com/page",
            r#"<html><head><base href="https://other.com/blog/"></head>
               <body><a href="post-1">Post</a></body></html>"#,
        );
        let doc = p.parse(&r).await.unwrap();
        let urls: Vec<&str> = doc.outbound_links.iter().map(|u| u.as_str()).collect();
        assert_eq!(urls, vec!["https://other.com/blog/post-1"]);
    }

    #[tokio::test]
    async fn rejects_non_http_schemes() {
        let p = LolHtmlParser::new();
        let r = resp(
            "https://example.com/",
            r#"<a href="mailto:foo@bar.com">m</a>
               <a href="javascript:alert(1)">js</a>
               <a href="tel:+15551234567">t</a>
               <a href="/keep">good</a>"#,
        );
        let doc = p.parse(&r).await.unwrap();
        let urls: Vec<&str> = doc.outbound_links.iter().map(|u| u.as_str()).collect();
        assert_eq!(urls, vec!["https://example.com/keep"]);
    }

    #[tokio::test]
    async fn rejects_fragment_only_links() {
        let p = LolHtmlParser::new();
        let r = resp(
            "https://example.com/page",
            r##"<a href="#section">jump</a><a href="/real">go</a>"##,
        );
        let doc = p.parse(&r).await.unwrap();
        let urls: Vec<&str> = doc.outbound_links.iter().map(|u| u.as_str()).collect();
        assert_eq!(urls, vec!["https://example.com/real"]);
    }

    #[tokio::test]
    async fn dedupes_via_canonicalization() {
        let p = LolHtmlParser::new();
        let r = resp(
            "https://example.com/",
            r##"<a href="/page?utm_source=x">a</a>
                <a href="/page?utm_medium=y">b</a>
                <a href="/page">c</a>
                <a href="/page#anchor">d</a>"##,
        );
        let doc = p.parse(&r).await.unwrap();
        // All four collapse to https://example.com/page after canonicalization.
        let urls: Vec<&str> = doc.outbound_links.iter().map(|u| u.as_str()).collect();
        assert_eq!(urls, vec!["https://example.com/page"]);
    }

    #[tokio::test]
    async fn extracts_visible_text_excluding_script_and_style() {
        let p = LolHtmlParser::new();
        let r = resp(
            "https://example.com/",
            r#"<html><body>
                <p>Hello world.</p>
                <script>var secret = 42;</script>
                <style>.x { color: red; }</style>
                <p>Goodbye.</p>
            </body></html>"#,
        );
        let doc = p.parse(&r).await.unwrap();
        let text = doc.text.unwrap();
        assert!(text.contains("Hello world."));
        assert!(text.contains("Goodbye."));
        assert!(!text.contains("secret"));
        assert!(!text.contains("color: red"));
    }

    #[tokio::test]
    async fn handles_empty_body() {
        let p = LolHtmlParser::new();
        let r = resp("https://example.com/", "");
        let doc = p.parse(&r).await.unwrap();
        assert_eq!(doc.title, None);
        assert_eq!(doc.outbound_links.len(), 0);
        assert_eq!(doc.text, None);
    }

    #[tokio::test]
    async fn handles_malformed_html() {
        let p = LolHtmlParser::new();
        let r = resp(
            "https://example.com/",
            r#"<html><body><a href="/foo"<broken><p>txt"#,
        );
        // Parser shouldn't error on imperfect HTML; lol_html is lenient.
        let _doc = p.parse(&r).await.unwrap();
    }
}
