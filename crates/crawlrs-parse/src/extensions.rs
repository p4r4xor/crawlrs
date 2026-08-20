//! URL-extension deny list for link extraction.
//!
//! When a page links to `/movie.mp4`, `/manual.pdf`, or `/style.css`,
//! enqueuing it costs a worker a fetch + a parse pass that produces
//! no useful outlinks and stores a body the downstream content
//! pipeline can't use as HTML. Filtering at link-extraction time
//! keeps these URLs out of the frontier entirely.
//!
//! Detection rule: look at the URL's last path segment (skipping
//! trailing empty segments from a `/`-suffixed path), take the
//! suffix after the final `.`, lowercase it, check the set.
//! Query string and fragment are already excluded by
//! `url::Url::path()`.
//!
//! Single-segment extensions only: `tar.gz` is intentionally absent
//! because the suffix match runs on the final segment alone, and the
//! algorithm already catches `gz`.

use std::collections::HashSet;
use std::sync::LazyLock;

use crawlrs_core::CanonicalUrl;

/// Extensions for URLs we should not enqueue. Each entry is the
/// lowercase suffix following the final `.` in the last path
/// segment.
pub(crate) const DENY_EXTENSIONS: &[&str] = &[
    // archives
    "7z", "7zip", "bz2", "gz", "rar", "tar", "xz", "zip", // images
    "ai", "bmp", "cdr", "drw", "dxf", "eps", "gif", "ico", "jpeg", "jpg", "mng", "pct", "png",
    "ps", "psp", "pst", "svg", "tif", "tiff", "webp", // audio
    "aac", "aiff", "au", "mid", "mp3", "ogg", "ra", "wav", "wma", // video
    "3gp", "asf", "asx", "avi", "flv", "m4a", "m4v", "mov", "mp4", "mpg", "qt", "rm", "swf",
    "webm", "wmv", // office suites
    "doc", "docb", "docm", "docx", "dotm", "dotx", "odg", "odp", "ods", "odt", "potm", "potx",
    "pps", "ppt", "pptm", "pptx", "xls", "xlsm", "xltm", "xltx", "xlsx",
    // other binaries / packagings / scripts / known-non-HTML text
    "apk", "bat", "bin", "cpl", "css", "dmg", "exe", "hta", "iso", "jar", "js", "msi", "msp", "pdf",
    "py", "rb", "rss", "sh",
];

static DENY_SET: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| DENY_EXTENSIONS.iter().copied().collect());

/// Returns true if the URL's last non-empty path segment ends with a
/// denied extension. Examples:
///
/// - `/movie.mp4`            -> true
/// - `/Movie.MP4`            -> true (case-insensitive)
/// - `/file.tar.gz`          -> true (matches `gz`)
/// - `/page`                 -> false
/// - `/foo.jpg/bar`          -> false (extension is on a dir, not the file)
/// - `/`                     -> false
/// - `/file.pdf?v=1`         -> true (`path()` excludes query)
pub fn denies(url: &CanonicalUrl) -> bool {
    let path = url.as_url().path();
    let Some(last_segment) = path.rsplit('/').find(|seg| !seg.is_empty()) else {
        return false;
    };
    let Some(dot_idx) = last_segment.rfind('.') else {
        return false;
    };
    let ext = &last_segment[dot_idx + 1..];
    if ext.is_empty() {
        return false;
    }
    // Real-world extensions are almost always already lowercase; only
    // allocate a lowercased copy when an uppercase byte is present.
    if ext.bytes().any(|b| b.is_ascii_uppercase()) {
        DENY_SET.contains(ext.to_ascii_lowercase().as_str())
    } else {
        DENY_SET.contains(ext)
    }
}
