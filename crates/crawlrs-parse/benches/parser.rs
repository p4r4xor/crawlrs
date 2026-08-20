//! HTML parser microbench.
//!
//! Exercises `LolHtmlParser::parse` end-to-end (HTML in, structured
//! `ParsedDocument` out) across three input shapes:
//!
//! - **small**: a ~1 KB blog-post fixture; typical "page after
//!   title-and-snippet was prefetched" shape.
//! - **synthetic_links_heavy**: 4 KB with 200 outbound anchors but
//!   little body text; stresses link extraction + canonicalization.
//! - **synthetic_text_heavy**: 256 KB of paragraphs with few links;
//!   stresses the visible-text accumulator. 256 KB is a deliberately
//!   large body: big enough to make the accumulator's allocation and
//!   whitespace-collapse passes dominate the sample.
//!
//! Throughput is reported per byte (so different sizes are
//! comparable). Run with:
//!
//!     cargo bench --bench parser -- --save-baseline before
//!     # change profile / dep version
//!     cargo bench --bench parser -- --baseline before

use std::hint::black_box;

use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use crawlrs_core::{CanonicalUrl, FetchResponse, Parser, RedirectHop};
use crawlrs_parse::LolHtmlParser;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use smallvec::SmallVec;
use tokio::runtime::Builder as RuntimeBuilder;

const SMALL_HTML: &str = include_str!("fixtures/small_blog_post.html");

fn synth_links_heavy() -> String {
    let mut html = String::from(
        "<!DOCTYPE html><html><body>\
         <h1>Links-heavy synthetic fixture</h1><ul>",
    );
    for i in 0..200 {
        html.push_str(&format!(
            "<li><a href=\"https://example-{i}.test/page/{i}?ref=src\">link {i}</a></li>"
        ));
    }
    html.push_str("</ul></body></html>");
    html
}

fn synth_text_heavy() -> String {
    let para = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. \
                Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. \
                Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris \
                nisi ut aliquip ex ea commodo consequat. ";
    let target = 256 * 1024;
    let mut html = String::from("<!DOCTYPE html><html><body><h1>Text-heavy synthetic fixture</h1>");
    while html.len() < target {
        html.push_str("<p>");
        html.push_str(para);
        html.push_str("</p>");
    }
    html.push_str("<p>See <a href=\"https://example.test/more\">more</a>.</p>");
    html.push_str("</body></html>");
    html
}

fn response_with_body(body: &str, url: &str) -> FetchResponse {
    FetchResponse {
        url: CanonicalUrl::parse(url).unwrap(),
        status: 200,
        headers: Box::new(std::collections::HashMap::from([(
            "content-type".to_string(),
            "text/html; charset=utf-8".to_string(),
        )])),
        body: Bytes::copy_from_slice(body.as_bytes()),
        redirect_chain: SmallVec::<[RedirectHop; 4]>::new(),
        fetched_at: Utc::now(),
        duration: Duration::from_millis(0),
    }
}

fn bench_parse(c: &mut Criterion) {
    // The Parser trait is async (via `#[async_trait]`); without
    // driving the returned future to completion we'd only be timing
    // future-object construction, not parsing work. A single-threaded
    // runtime keeps the per-iteration overhead minimal and removes
    // executor variance from the measurement.
    let runtime = RuntimeBuilder::new_current_thread().build().unwrap();
    let parser = LolHtmlParser;

    let small = response_with_body(SMALL_HTML, "https://example.test/blog/post-1");
    let links_html = synth_links_heavy();
    let links = response_with_body(&links_html, "https://example.test/links");
    let text_html = synth_text_heavy();
    let text = response_with_body(&text_html, "https://example.test/text");

    let mut group = c.benchmark_group("LolHtmlParser::parse");

    group.throughput(Throughput::Bytes(small.body.len() as u64));
    group.bench_function("small_blog_post", |b| {
        b.iter(|| {
            let _ = black_box(runtime.block_on(parser.parse(black_box(&small))));
        });
    });

    group.throughput(Throughput::Bytes(links.body.len() as u64));
    group.bench_function("synthetic_links_heavy", |b| {
        b.iter(|| {
            let _ = black_box(runtime.block_on(parser.parse(black_box(&links))));
        });
    });

    group.throughput(Throughput::Bytes(text.body.len() as u64));
    group.bench_function("synthetic_text_heavy", |b| {
        b.iter(|| {
            let _ = black_box(runtime.block_on(parser.parse(black_box(&text))));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
