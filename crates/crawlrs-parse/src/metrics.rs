//! Parse-layer metric names + descriptors.

use metrics::{Unit, describe_counter, describe_histogram};

pub const PARSE_SECONDS: &str = "crawlrs_parse_seconds";
pub const PARSE_LINKS_DISCOVERED: &str = "crawlrs_parse_links_discovered";
pub const PARSE_LINKS_EXTENSION_DENIED_TOTAL: &str = "crawlrs_parse_links_extension_denied_total";
pub const PARSE_BASE_HREF_INVALID_TOTAL: &str = "crawlrs_parse_base_href_invalid_total";
pub const PARSE_SKIPPED_TOTAL: &str = "crawlrs_parse_skipped_total";

pub fn register() {
    describe_histogram!(
        PARSE_SECONDS,
        Unit::Seconds,
        "Wall-clock duration of one Parser::parse call. Surfaces \
         parser regressions hidden inside pipeline_seconds."
    );
    describe_histogram!(
        PARSE_LINKS_DISCOVERED,
        Unit::Count,
        "Outlinks per parsed page; capacity-planning signal for the \
         frontier (submit_batch volume scales with this)."
    );
    describe_counter!(
        PARSE_LINKS_EXTENSION_DENIED_TOTAL,
        "Outlinks dropped by the parser because their URL ends in a \
         non-HTML extension (images, video, archives, office docs, \
         scripts). Pre-frontier filter that prevents pointless fetch + \
         parse work and keeps the frontier focused on HTML."
    );
    describe_counter!(
        PARSE_BASE_HREF_INVALID_TOTAL,
        "Documents whose `<base href>` failed to parse, so link \
         resolution fell back to the response URL. A nonzero rate \
         signals malformed base tags in the crawled corpus."
    );
    describe_counter!(
        PARSE_SKIPPED_TOTAL,
        "Responses for which lol_html parsing was bypassed entirely. \
         `reason` label: `binary_content_type` (Content-Type indicated \
         non-text), `invalid_utf8` (body failed simdutf8 validation; \
         binary content with a lying Content-Type header)."
    );
}
