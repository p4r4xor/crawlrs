//! Identity of a single crawl run.

use std::fmt;

/// Identity of one crawl run. Stamped on metadata writes and used to
/// scope per-run Redis keys, so a URL's ledger history can distinguish
/// "which run last touched this row" and operators can introspect one
/// run's state in isolation.
///
/// Pattern: Value Object. The string contents are an operator-supplied
/// label (e.g. `monthly-2026-05`); the newtype keeps run identity from
/// being confused with any other string that flows through the same
/// signatures (a host, a blob path, a URL).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunId(String);

impl RunId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for RunId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for RunId {
    fn from(id: &str) -> Self {
        Self(id.to_string())
    }
}
