//! Test-support drivers, fault injection and the reusable contract harness.
//!
//! Everything here is a test tool. None of it represents a production backend
//! choice, and none of it may be used to satisfy the persistence (`INV-10`)
//! requirement.
//!
//! Enabled with the `testing` feature.

pub mod contract;
pub mod counting;
pub mod fault;
pub mod fixture;
pub mod reference;
pub mod second;
pub mod support;
