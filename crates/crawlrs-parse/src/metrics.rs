//! Parse-layer metric names + descriptors.

use metrics::{Unit, describe_histogram};

pub const PARSE_SECONDS: &str = "crawlrs_parse_seconds";
pub const PARSE_LINKS_DISCOVERED: &str = "crawlrs_parse_links_discovered";

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
}
