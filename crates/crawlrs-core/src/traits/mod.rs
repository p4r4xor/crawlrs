//! All abstract contracts (ports) the crate publishes.
//!
//! **The rule**: every public trait this crate defines lives in a file
//! under this directory, named `<trait_name_in_snake_case>.rs`. The
//! file holds the trait declaration and any types tightly coupled to
//! it (return enums, parameter types, simple impl structs).
//!
//! Modules at the top level of `src/` are guaranteed *not* to declare
//! traits - they hold value types ([`crate::types`]), errors
//! ([`crate::error`]), helpers ([`crate::hash`]), or canonicalised
//! values ([`crate::url`]).
//!
//! New abstractions go here. New value types go in [`crate::types`]
//! (or their own file at top level if substantial). The split is
//! mechanical: open `src/traits/` to see what the crate's surface is,
//! open `src/` to see what flows between abstractions.

pub mod clock;
pub mod fetcher;
pub mod frontier;
pub mod metadata;
pub mod outbox;
pub mod parser;
pub mod politeness;
pub mod proxy;
pub mod sharding;
pub mod site_adapter;
pub mod store;
