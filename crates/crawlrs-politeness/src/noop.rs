//! No-op sub-trait implementations.
//!
//! Wired by the factory when `politeness.enabled = false`. Each
//! method returns the permissive answer (immediate next-wake /
//! true / false) with no I/O. `CompositePoliteness` wraps the
//! trio to produce a `Politeness` that disables the politeness
//! layer end-to-end without any other code knowing.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use crawlrs_core::{
    BackoffTracker, CanonicalUrl, FailureKind, NextWake, Result, RobotsChecker, WakePlanner,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopWakePlanner;

#[async_trait]
impl WakePlanner for NoopWakePlanner {
    async fn record_fetch(&self, host: &str) -> Result<NextWake> {
        Ok(NextWake {
            host: host.to_string(),
            until: Instant::now(),
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopRobotsChecker;

#[async_trait]
impl RobotsChecker for NoopRobotsChecker {
    async fn allowed(&self, _url: &CanonicalUrl) -> Result<bool> {
        Ok(true)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopBackoffTracker;

#[async_trait]
impl BackoffTracker for NoopBackoffTracker {
    async fn is_open(&self, _host: &str) -> Result<bool> {
        Ok(false)
    }

    async fn record_failure(
        &self,
        url: &CanonicalUrl,
        _kind: FailureKind,
        _server_hint: Option<Duration>,
    ) -> Result<NextWake> {
        Ok(NextWake {
            host: url.host().unwrap_or("").to_string(),
            until: Instant::now(),
        })
    }
}
