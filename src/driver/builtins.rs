//! The driver kinds this build can construct.
//!
//! This is the one place a build declares which kinds it registers, and the one
//! place that knows which of them are test drivers. Adding a driver is adding an
//! entry here, not editing a `match` in the composition root, and no arm can be
//! dead code depending on the feature set: the table is data, so a build with no
//! drivers simply has an empty table.
//!
//! A production backend is not registered yet (see `docs/implementation-plan.md`
//! P6); the test drivers are registered only under the `testing` feature. The
//! `test` flag — not a `test-` name prefix — is what configuration uses to refuse
//! a test driver for durable writes, so a real backend called `test-*` is not
//! mistaken for a test double and a test double named otherwise is not let
//! through.

use std::sync::Arc;

use super::DriverFactory;

/// Constructs a factory for one kind, given the kind name from configuration.
///
/// The name is passed so a kind can be registered under the exact string the
/// config used, rather than a second hard-coded copy of it.
pub(crate) type FactoryCtor = fn(kind: &str) -> Arc<dyn DriverFactory>;

/// One registered kind.
pub(crate) struct Builtin {
    /// The kind name configuration names.
    pub(crate) name: &'static str,
    /// Whether this is a test-only driver, inadmissible for production writes.
    pub(crate) test: bool,
    /// Constructs the factory for this kind.
    pub(crate) ctor: FactoryCtor,
}

/// Every kind this build registers, in registration order.
pub(crate) fn builtins() -> Vec<Builtin> {
    // With no features enabled the table stays empty; the `allow` keeps that
    // from reading as a mistake.
    #[allow(unused_mut)]
    let mut table: Vec<Builtin> = Vec::new();

    #[cfg(feature = "testing")]
    {
        table.push(Builtin {
            name: "test-reference",
            test: true,
            ctor: |kind| Arc::new(crate::testing::reference::ReferenceDriverFactory::new(kind)),
        });
        table.push(Builtin {
            name: "test-vec",
            test: true,
            ctor: |kind| Arc::new(crate::testing::second::VecDriverFactory::new(kind)),
        });
        // The reference driver with native multipart declared, so the native
        // route can be exercised end to end (including over HTTP) rather than
        // only through the coordinator.
        table.push(Builtin {
            name: "test-native",
            test: true,
            ctor: |kind| {
                Arc::new(crate::testing::reference::ReferenceDriverFactory::native_multipart(kind))
            },
        });
    }

    table
}

/// The registered kind names, for an error that tells the operator what a build
/// *can* run instead of only what it cannot.
pub(crate) fn registered_kinds() -> Vec<&'static str> {
    let mut kinds: Vec<&'static str> = builtins().into_iter().map(|builtin| builtin.name).collect();
    kinds.sort_unstable();
    kinds
}

/// Whether `kind` is a registered test driver.
///
/// An unregistered kind is not a test kind: it is rejected as unregistered by
/// the composition root, so answering `false` here keeps the two refusals from
/// overlapping.
pub(crate) fn is_test_kind(kind: &str) -> bool {
    builtins()
        .into_iter()
        .find(|builtin| builtin.name == kind)
        .is_some_and(|builtin| builtin.test)
}
