//! WARC/1.1 record encoding (ISO 28500). Pure functions, no I/O.
//!
//! Each call returns a gzip-framed byte vector ready to concatenate
//! into a `.warc.gz` file. The WARC spec mandates that a `.warc.gz`
//! is a sequence of independently-gzipped records, so concatenation
//! is the right shape (vs whole-file gzip).
//!
//! v1 emits two record types:
//!
//! - `warcinfo` once per file (file header).
//! - `response` per fetched URL: a synthetic HTTP-response payload
//!   (status line + headers + body) wrapped in the WARC framing.
//!
//! Deferred to v2:
//!
//! - `request` records (would require preserving FetchRequest into
//!   StoreRecord; lossy without it).
//! - `revisit` records for body deduplication on recrawl
//!   (`WARC-Type: revisit` + `WARC-Refers-To` pointing at a prior
//!   record). Mirrors the deferred Parquet `body_revisit_of` column.
//! - `WARC-Payload-Digest` / `WARC-Block-Digest` SHA-1 fields.

use std::io::Write;

use chrono::{DateTime, Utc};
use crawlrs_core::StoreRecord;
use flate2::Compression;
use flate2::write::GzEncoder;
use http::StatusCode;

/// `WARC-Date` field format: ISO 8601 with `Z` suffix, second precision.
const WARC_DATE_FMT: &str = "%Y-%m-%dT%H:%M:%SZ";

/// One gzipped WARC record, ready to append to a `.warc.gz` file.
pub fn encode_warcinfo(run_id: &str) -> Vec<u8> {
    let record_id = new_record_id();
    let now = Utc::now();

    let body = format!(
        "software: crawlrs/0.0.1\r\n\
         format: WARC File Format 1.1\r\n\
         run-id: {run_id}\r\n",
    );
    let header = format!(
        "WARC/1.1\r\n\
         WARC-Type: warcinfo\r\n\
         WARC-Record-ID: {record_id}\r\n\
         WARC-Date: {date}\r\n\
         Content-Type: application/warc-fields\r\n\
         Content-Length: {len}\r\n\
         \r\n",
        date = now.format(WARC_DATE_FMT),
        len = body.len(),
    );
    framed(&header, body.as_bytes())
}

pub fn encode_response(record: &StoreRecord<'_>) -> Vec<u8> {
    let record_id = new_record_id();
    let payload = build_http_response_payload(record);
    let header = format!(
        "WARC/1.1\r\n\
         WARC-Type: response\r\n\
         WARC-Target-URI: {uri}\r\n\
         WARC-Record-ID: {record_id}\r\n\
         WARC-Date: {date}\r\n\
         Content-Type: application/http;msgtype=response\r\n\
         Content-Length: {len}\r\n\
         \r\n",
        uri = record.resp.url.as_str(),
        date = record.resp.fetched_at.format(WARC_DATE_FMT),
        len = payload.len(),
    );
    framed(&header, &payload)
}

/// Build the HTTP-response payload: status line + headers + CRLF + body.
/// HTTP version is always emitted as `HTTP/1.1` because `FetchResponse`
/// doesn't preserve the wire-level protocol version (a v1 lossiness).
fn build_http_response_payload(record: &StoreRecord<'_>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(record.resp.body.len() + 512);
    let reason = StatusCode::from_u16(record.resp.status)
        .ok()
        .and_then(|c| c.canonical_reason())
        .unwrap_or("");
    let status_line = format!("HTTP/1.1 {} {}\r\n", record.resp.status, reason);
    buf.extend_from_slice(status_line.as_bytes());
    for (name, value) in record.resp.headers.iter() {
        buf.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(&record.resp.body);
    buf
}

fn framed(header: &str, body: &[u8]) -> Vec<u8> {
    // WARC record terminator is two CRLFs after the body.
    let mut record = Vec::with_capacity(header.len() + body.len() + 4);
    record.extend_from_slice(header.as_bytes());
    record.extend_from_slice(body);
    record.extend_from_slice(b"\r\n\r\n");
    gzip(&record)
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(bytes.len() / 2), Compression::default());
    encoder.write_all(bytes).expect("gzip write to Vec");
    encoder.finish().expect("gzip finish on Vec")
}

/// `WARC-Record-ID` must be a URI per ISO 28500 §6.2; the spec
/// recommends `<urn:uuid:...>` but any URI shape is valid. We use a
/// project-namespaced URN backed by cuid2 (already a workspace dep,
/// shorter than a UUID, monotonic-ish, collision-resistant). Strict
/// UUID-format validators are rare in WARC tooling; the format remains
/// fully spec-conformant.
fn new_record_id() -> String {
    format!("<urn:crawlrs:{}>", cuid2::create_id())
}

/// Unused outside tests today; exposed so a future revisit-record
/// builder has the same date format helper.
#[allow(dead_code)]
pub fn warc_date(ts: DateTime<Utc>) -> String {
    ts.format(WARC_DATE_FMT).to_string()
}
