//! `allins3` — a multi-driver S3 server.
//!
//! The crate serves S3 over one or more storage drivers: it parses and
//! authenticates HTTP/S3 requests, models objects and multipart uploads in the
//! service layer, and publishes through a driver's staged write. Production
//! backends are not selected yet — the drivers in this build are the in-memory
//! test tools — so persistence and durability claims are out of scope here
//! (`INV-10`). See `docs/implementation-spec.md` and
//! `docs/implementation-plan.md`.
//!
//! The crate is deliberately a single crate with module boundaries until the
//! interfaces stabilise; it is not yet split into many crates.

pub mod auth;
pub mod catalog;
pub mod config;
pub mod domain;
pub mod driver;
pub mod integrity;
pub mod protocol;
pub mod server;
pub mod service;
pub mod timeouts;

pub(crate) mod compose;
pub(crate) mod sync;

/// Test-support drivers, fault injection and the reusable contract-test
/// harness. Enabled with the `testing` feature.
#[cfg(feature = "testing")]
pub mod testing;
