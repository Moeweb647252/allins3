//! Driver factories and secret resolution.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::{DriverContext, DriverError, DriverInstanceId};

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
/// The concrete private schema is intentionally not fixed here; the core layer
/// only understands the public fields, the kind, the instance id and the
/// capabilities. Configuration parsing lands in P2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactoryConfig {
    /// The instance being configured.
    pub instance_id: DriverInstanceId,
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
        secrets: &dyn SecretResolver,
    ) -> Result<(), DriverError>;

    /// Creates a configured driver instance.
    async fn create(
        &self,
        ctx: &DriverContext,
        instance_id: &DriverInstanceId,
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
