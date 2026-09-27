//! Driver factories and secret resolution.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::{DriverContext, DriverError, DriverInstanceId};

use super::blocking::BlockingExecutor;
use super::descriptor::DriverKind;
use super::traits::StorageDriver;

/// A secret value whose `Debug` output is always redacted.
///
/// Secret values are read from the environment or a secret reference, are
/// never written into samples or logs, and are never passed to a driver.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a resolved secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the secret value to code that genuinely needs it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// Resolves secret references such as `env:NAME`.
pub trait SecretResolver: Send + Sync {
    /// Resolves a reference to its value.
    fn resolve(&self, reference: &str) -> Result<Secret, DriverError>;

    /// The scheme portion of a reference, e.g. `env` for `env:NAME`.
    fn scheme(reference: &str) -> Option<&str>
    where
        Self: Sized,
    {
        reference.split_once(':').map(|(scheme, _)| scheme)
    }
}

/// Per-instance driver configuration.
///
/// The core layer understands only the public fields: which instance is being
/// configured, the opaque `options` table and the secret references the driver
/// may resolve. The concrete private schema of `options` is the driver's own —
/// the core never parses it (spec §3.2.5: *"the factory validates the driver's
/// private configuration"*). `secret_refs` are validated as resolvable before
/// startup, but which reference maps to which option is again the driver's
/// decision.
///
/// The blocking executor travels with the config because it is owned by the
/// composition root, not by each driver: a synchronous backend runs its work on
/// this shared, bounded executor rather than parking a runtime thread.
#[derive(Debug, Clone)]
pub struct FactoryConfig {
    /// The instance being configured.
    pub instance_id: DriverInstanceId,
    /// Driver-private options, opaque to the core.
    pub options: toml::Table,
    /// Secret references this instance may resolve, e.g. `env:NAME`.
    pub secret_refs: Vec<String>,
    /// The shared, bounded executor for blocking backend work.
    pub blocking: BlockingExecutor,
}

impl FactoryConfig {
    /// Builds a config for one instance.
    pub fn new(
        instance_id: DriverInstanceId,
        options: toml::Table,
        secret_refs: Vec<String>,
        blocking: BlockingExecutor,
    ) -> Self {
        Self {
            instance_id,
            options,
            secret_refs,
            blocking,
        }
    }

    /// A config with no options, no secrets and an unbounded executor.
    ///
    /// Used by test harnesses that build an instance directly from a factory
    /// and have nothing private to configure.
    pub fn minimal(instance_id: DriverInstanceId) -> Self {
        Self::new(
            instance_id,
            toml::Table::new(),
            Vec::new(),
            BlockingExecutor::unbounded(),
        )
    }
}

/// Creates and validates driver instances.
///
/// `validate` must not write user data, and on failure it must release any
/// connections it allocated. Both methods may consult the secret resolver.
#[async_trait]
pub trait DriverFactory: Send + Sync {
    /// The kind this factory registers.
    fn kind(&self) -> DriverKind;

    /// Validates private configuration without touching user data.
    async fn validate(
        &self,
        ctx: &DriverContext,
        config: &FactoryConfig,
        secrets: &dyn SecretResolver,
    ) -> Result<(), DriverError>;

    /// Creates a configured driver instance.
    async fn create(
        &self,
        ctx: &DriverContext,
        config: &FactoryConfig,
        secrets: &dyn SecretResolver,
    ) -> Result<Arc<dyn StorageDriver>, DriverError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_is_redacted() {
        let s = Secret::new("super-secret-value");
        let rendered = format!("{s:?}");
        assert!(!rendered.contains("super-secret-value"));
        assert_eq!(s.expose(), "super-secret-value");
    }

    #[test]
    fn scheme_parsing() {
        assert_eq!(
            <DummyResolver as SecretResolver>::scheme("env:ALLINS3_SECRET"),
            Some("env")
        );
        assert_eq!(<DummyResolver as SecretResolver>::scheme("plain"), None);
    }

    struct DummyResolver;
    impl SecretResolver for DummyResolver {
        fn resolve(&self, _reference: &str) -> Result<Secret, DriverError> {
            Err(DriverError::not_found("no secret"))
        }
    }
}
