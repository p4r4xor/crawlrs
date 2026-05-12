//! Tests for `extensions::denies` URL filter.

use crawlrs_core::CanonicalUrl;
use crawlrs_parse::extensions;

fn url(s: &str) -> CanonicalUrl {
    CanonicalUrl::parse(s).expect("test URL must parse")
}

#[test]
fn matches_common_binary_extensions() {
    assert!(extensions::denies(&url("https://example.com/movie.mp4")));
    assert!(extensions::denies(&url(
        "https://example.com/dir/manual.pdf"
    )));
    assert!(extensions::denies(&url(
        "https://example.com/dl/archive.zip"
    )));
    assert!(extensions::denies(&url("https://example.com/style.css")));
}

#[test]
fn case_insensitive() {
    assert!(extensions::denies(&url("https://example.com/Movie.MP4")));
    assert!(extensions::denies(&url("https://example.com/IMG.JPG")));
}

#[test]
fn matches_final_extension_of_compound() {
    assert!(extensions::denies(&url("https://example.com/file.tar.gz")));
}

#[test]
fn rejects_no_extension_paths() {
    assert!(!extensions::denies(&url("https://example.com/page")));
    assert!(!extensions::denies(&url("https://example.com/")));
    assert!(!extensions::denies(&url("https://example.com")));
}

#[test]
fn rejects_extension_on_dir_not_file() {
    assert!(!extensions::denies(&url("https://example.com/foo.jpg/bar")));
}

#[test]
fn query_string_does_not_mask_extension() {
    assert!(extensions::denies(&url("https://example.com/file.pdf?v=1")));
    assert!(extensions::denies(&url(
        "https://example.com/file.pdf#section"
    )));
}

#[test]
fn html_targets_pass() {
    // Pages we want to keep crawling: explicit .html, .htm, no ext.
    assert!(!extensions::denies(&url("https://example.com/page.html")));
    assert!(!extensions::denies(&url("https://example.com/page.htm")));
    assert!(!extensions::denies(&url("https://example.com/about/")));
}

#[test]
fn extension_only_path_segment_does_not_panic() {
    // Hidden-file convention: `.htaccess`. ext = "htaccess", not in list.
    assert!(!extensions::denies(&url("https://example.com/.htaccess")));
}
