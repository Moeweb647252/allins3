//! Concrete storage backends.
//!
//! Each backend implements the interfaces in [`crate::driver`] and is registered
//! in the build's kind table (`crate::driver::builtins`). Backends live here
//! rather than under `crate::testing` because they are real, not test doubles:
//! the local filesystem backend is the first production driver
//! (`docs/adr/0015-local-filesystem-production-driver.md`).
//!
//! A backend depends on `domain` and `driver` only — never on `protocol` or
//! `service` (`INV-01`).

pub mod fs;

// The read-only 139 cloud-drive backend, and with it the only outbound HTTPS
// client in the build. Behind a feature so the default build stays free of the
// extra dependencies (ADR-0017).
#[cfg(feature = "yun139")]
pub mod yun139;
