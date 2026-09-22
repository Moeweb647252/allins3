//! `allins3` — a multi-driver S3 server.
//!
//! This crate is currently an **interface engineering preview**: it defines the
//! domain model, the driver interfaces and the capability model. It does not
//! yet serve HTTP, verify signatures or select a production backend. See
//! `docs/implementation-spec.md` and `docs/implementation-plan.md`.
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

/// Test-support drivers, fault injection and the reusable contract-test
/// harness. Enabled with the `testing` feature.
#[cfg(feature = "testing")]
pub mod testing;
