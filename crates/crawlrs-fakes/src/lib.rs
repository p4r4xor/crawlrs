//! Test doubles for the trait surfaces in `crawlrs-core`.
//!
//! Use this crate as a `dev-dependency` from any crate whose tests
//! need to compose the runtime without real I/O. Each double is the
//! minimum-viable impl of one core trait, with inherent helpers for
//! installing canned data and inspecting calls.
//!
//! These are *test doubles*, not production code: they hold state in
//! `Mutex<HashMap>` and accept everything the trait permits. Real
//! adapters (`WreqFetcher`, `RedisFrontier`, `PostgresMetadataStore`)
//! enforce more invariants and live in their own adapter crates.

pub mod clock;
pub mod fetcher;
pub mod frontier;
pub mod metadata_store;
pub(crate) mod outbox;
pub mod store;

pub use clock::ManualClock;
pub use fetcher::FakeFetcher;
pub use frontier::InMemoryFrontier;
pub use metadata_store::InMemoryMetadataStore;
pub use store::InMemoryStore;
