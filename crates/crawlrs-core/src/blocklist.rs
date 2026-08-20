//! Access control: hosts the operator refuses to crawl.
//!
//! In-memory set, checked sync as the first gate in the politeness
//! pipeline. Distinct from `RobotsChecker` (which honors the
//! *host's* refusal) and from `BackoffTracker` (which reacts to
//! per-host failures).

use std::collections::HashSet;

/// Operator-mandated host blocklist. Construct once from the
/// parsed `[access].blocklist` list and pass by reference or by
/// clone into the runtime.
#[derive(Debug, Clone, Default)]
pub struct Blocklist {
    hosts: HashSet<String>,
}

impl Blocklist {
    pub fn new(hosts: HashSet<String>) -> Self {
        Self { hosts }
    }

    /// `true` when the host is on the blocklist. Match is exact
    /// on the canonicalized host string; subdomain matching is
    /// not performed here (a future `[access]` pattern field
    /// would be a separate concern).
    #[must_use]
    pub fn is_blocked(&self, host: &str) -> bool {
        self.hosts.contains(host)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.hosts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hosts.is_empty()
    }
}
