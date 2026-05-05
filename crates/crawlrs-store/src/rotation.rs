//! Rotation policy: when to close the current file and open a new one.
//!
//! Three triggers, OR-combined: byte cap (raw input bytes consumed),
//! row count, and elapsed time since the file opened. The first one
//! to fire causes a rotation. Defaults match ADR-0013: 128 MB raw,
//! 100 K rows, 30 minutes.

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct RotationPolicy {
    pub max_bytes: usize,
    pub max_rows: usize,
    pub max_duration: Duration,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            max_bytes: 128 * 1024 * 1024,
            max_rows: 100_000,
            max_duration: Duration::from_secs(30 * 60),
        }
    }
}

impl RotationPolicy {
    pub fn should_rotate(&self, rows: usize, bytes: usize, opened_at: Instant) -> bool {
        rows >= self.max_rows || bytes >= self.max_bytes || opened_at.elapsed() >= self.max_duration
    }
}
