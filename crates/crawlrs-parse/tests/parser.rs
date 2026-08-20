//! Tests for `LolHtmlParser`: title, link, text, and edge-case extraction.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use crawlrs_core::{CanonicalUrl, FetchResponse, Parser};
use crawlrs_parse::LolHtmlParser;

fn resp(url: &str, body: &str) -> FetchResponse {
    FetchResponse {
        url: CanonicalUrl::parse(url).unwrap(),
        status: 200,
        headers: Box::new(HashMap::new()),
        body: Bytes::copy_from_slice(body.as_bytes()),
        redirect_chain: Vec::new().into(),
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
