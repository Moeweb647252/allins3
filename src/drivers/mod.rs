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
