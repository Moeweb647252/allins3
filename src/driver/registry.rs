//! Driver factory registration and instance initialization.
//!
//! The registry maps a [`DriverKind`] to a factory, creates instances and
//! tracks their health. It performs **no** S3 authorization: that belongs to
//! the auth layer (spec §2).

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

use crate::domain::{DriverContext, DriverError, DriverInstanceId};

use super::descriptor::{DriverDescriptor, DriverHealth, DriverKind};
use super::factory::{DriverFactory, FactoryConfig, SecretResolver};
use super::traits::StorageDriver;

/// Registration and creation failures.
#[derive(Debug, Error)]
pub enum RegistryError {
    /// No factory is registered for the requested kind.
    #[error("no driver factory registered for kind `{0}`")]
    UnknownKind(DriverKind),
    /// A factory for this kind is already registered.
    #[error("a driver factory is already registered for kind `{0}`")]
    DuplicateKind(DriverKind),
    /// The instance id is already in use.
    #[error("a driver instance is already registered for id `{0}`")]
    DuplicateInstance(DriverInstanceId),
    /// The factory rejected the configuration or failed to create.
    #[error("driver factory failed: {0}")]
    Factory(#[from] DriverError),
}

/// A registry of driver factories and live instances.
#[derive(Default)]
pub struct DriverRegistry {
    factories: HashMap<DriverKind, Arc<dyn DriverFactory>>,
    instances: HashMap<DriverInstanceId, Arc<dyn StorageDriver>>,
}

impl std::fmt::Debug for DriverRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverRegistry")
            .field("kinds", &self.factories.keys().collect::<Vec<_>>())
            .field("instances", &self.instances.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl DriverRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a factory, rejecting duplicate kinds. Within a kind,
    /// registration order does not matter.
    pub fn register(&mut self, factory: Arc<dyn DriverFactory>) -> Result<(), RegistryError> {
        let kind = factory.kind();
        if self.factories.contains_key(&kind) {
            return Err(RegistryError::DuplicateKind(kind));
        }
        self.factories.insert(kind, factory);
        Ok(())
    }

    /// Whether a kind is registered.
    pub fn has_kind(&self, kind: &DriverKind) -> bool {
        self.factories.contains_key(kind)
    }

    /// Creates and initializes a driver instance for a registered kind.
    ///
    /// The factory's `validate` runs first so a bad configuration fails before
    /// resources are committed, and it receives the same [`FactoryConfig`] the
    /// create call does — so a factory cannot validate one configuration and be
    /// created with another.
    pub async fn create_instance(
        &mut self,
        ctx: &DriverContext,
        kind: &DriverKind,
        config: &FactoryConfig,
        secrets: &dyn SecretResolver,
    ) -> Result<Arc<dyn StorageDriver>, RegistryError> {
        let instance_id = &config.instance_id;
        if self.instances.contains_key(instance_id) {
            return Err(RegistryError::DuplicateInstance(instance_id.clone()));
        }
        let factory = self
            .factories
            .get(kind)
            .cloned()
            .ok_or_else(|| RegistryError::UnknownKind(kind.clone()))?;

        factory.validate(ctx, config, secrets).await?;
        let driver = factory.create(ctx, config, secrets).await?;
        self.instances.insert(instance_id.clone(), driver.clone());
        Ok(driver)
    }

    /// Runs a factory's `validate` without creating anything.
    ///
    /// `check-config` uses this so a private option the driver rejects fails
    /// the preflight, while nothing is allocated and no directory is created
    /// as a side effect of checking a file.
    pub async fn validate_instance(
        &self,
        ctx: &DriverContext,
        kind: &DriverKind,
        config: &FactoryConfig,
        secrets: &dyn SecretResolver,
    ) -> Result<(), RegistryError> {
        let factory = self
            .factories
            .get(kind)
            .cloned()
            .ok_or_else(|| RegistryError::UnknownKind(kind.clone()))?;
        factory.validate(ctx, config, secrets).await?;
        Ok(())
    }

    /// Fetches a live instance.
    pub fn instance(&self, id: &DriverInstanceId) -> Option<Arc<dyn StorageDriver>> {
        self.instances.get(id).cloned()
    }

    /// The descriptor of every live instance, sorted by instance id.
    ///
    /// This is the operator-visible identity of the deployment: which kind and
    /// version is behind each instance. Drivers have reported it since the
    /// interface existed; this is what finally reads it.
    pub fn descriptors(&self) -> Vec<DriverDescriptor> {
        let mut descriptors: Vec<DriverDescriptor> = self
            .instances
            .values()
            .map(|driver| driver.descriptor())
            .collect();
        descriptors.sort_by(|a, b| a.instance_id.as_str().cmp(b.instance_id.as_str()));
        descriptors
    }

    /// The number of live instances.
    pub fn instance_count(&self) -> usize {
        self.instances.len()
    }

    /// Probes every instance. One unhealthy instance does not stop the others.
    pub async fn health(&self, ctx: &DriverContext) -> HashMap<DriverInstanceId, DriverHealth> {
        let mut out = HashMap::with_capacity(self.instances.len());
        for (id, driver) in &self.instances {
            let health = driver
                .health(ctx)
                .await
                .unwrap_or_else(|e| DriverHealth::Unhealthy {
                    reason: e.to_string(),
                });
            out.insert(id.clone(), health);
        }
        out
    }

    /// Stops every instance, returning the ones whose shutdown failed.
    ///
    /// A failing instance must not stop the others from closing, so failures
    /// are collected rather than propagated — the same rule [`Self::health`]
    /// follows for an unreachable instance. The caller owns the deadline: an
    /// instance that will not close must not hold the process open.
    pub async fn shutdown_all(&self, ctx: &DriverContext) -> Vec<(DriverInstanceId, DriverError)> {
        let mut failures = Vec::new();
        for (id, driver) in &self.instances {
            if let Err(error) = driver.shutdown(ctx).await {
                failures.push((id.clone(), error));
            }
        }
        failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;

    use crate::domain::NamespaceId;
    use crate::driver::capabilities::NamespaceCapabilities;
    use crate::driver::descriptor::DriverDescriptor;
    use crate::driver::factory::FactoryConfig;
    use crate::driver::traits::NamespaceHandle;

    struct NoopDriver {
        kind: DriverKind,
        instance_id: DriverInstanceId,
    }

    #[async_trait]
    impl StorageDriver for NoopDriver {
        fn descriptor(&self) -> DriverDescriptor {
            DriverDescriptor {
                kind: self.kind.clone(),
                instance_id: self.instance_id.clone(),
                display_name: "noop".into(),
                version: None,
            }
        }
        async fn health(&self, _ctx: &DriverContext) -> Result<DriverHealth, DriverError> {
            Ok(DriverHealth::Healthy)
        }
        async fn shutdown(&self, _ctx: &DriverContext) -> Result<(), DriverError> {
            Ok(())
        }
        async fn open_namespace(
            &self,
            _ctx: &DriverContext,
            _namespace: &NamespaceId,
        ) -> Result<NamespaceHandle, DriverError> {
            Err(DriverError::not_supported("noop"))
        }
    }

    struct NoopFactory {
        kind: DriverKind,
        fail_create: Arc<AtomicBool>,
    }

    #[async_trait]
    impl DriverFactory for NoopFactory {
        fn kind(&self) -> DriverKind {
            self.kind.clone()
        }
        async fn validate(
            &self,
            _ctx: &DriverContext,
            _config: &FactoryConfig,
            _secrets: &dyn SecretResolver,
        ) -> Result<(), DriverError> {
            Ok(())
        }
        async fn create(
            &self,
            _ctx: &DriverContext,
            config: &FactoryConfig,
            _secrets: &dyn SecretResolver,
        ) -> Result<Arc<dyn StorageDriver>, DriverError> {
            if self.fail_create.load(Ordering::Relaxed) {
                return Err(DriverError::internal("create failed"));
            }
            Ok(Arc::new(NoopDriver {
                kind: self.kind.clone(),
                instance_id: config.instance_id.clone(),
            }))
        }
    }

    struct EmptyResolver;
    impl SecretResolver for EmptyResolver {
        fn resolve(&self, _reference: &str) -> Result<crate::driver::factory::Secret, DriverError> {
            Err(DriverError::not_found("none"))
        }
    }

    fn ctx() -> DriverContext {
        DriverContext::new(crate::domain::RequestId::new("t"))
    }

    fn config(id: &str) -> FactoryConfig {
        FactoryConfig::minimal(DriverInstanceId::parse(id).expect("id"))
    }

    fn factory(kind: &str, fail: bool) -> Arc<dyn DriverFactory> {
        Arc::new(NoopFactory {
            kind: DriverKind::new(kind),
            fail_create: Arc::new(AtomicBool::new(fail)),
        })
    }

    #[tokio::test]
    async fn rejects_duplicate_kind() {
        let mut reg = DriverRegistry::new();
        reg.register(factory("a", false)).unwrap();
        assert!(matches!(
            reg.register(factory("a", false)),
            Err(RegistryError::DuplicateKind(_))
        ));
    }

    #[tokio::test]
    async fn rejects_unknown_kind() {
        let mut reg = DriverRegistry::new();
        let err = reg
            .create_instance(
                &ctx(),
                &DriverKind::new("missing"),
                &config("i1"),
                &EmptyResolver,
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(err, RegistryError::UnknownKind(_)));
    }

    #[tokio::test]
    async fn rejects_duplicate_instance() {
        let mut reg = DriverRegistry::new();
        reg.register(factory("a", false)).unwrap();
        reg.create_instance(&ctx(), &DriverKind::new("a"), &config("i1"), &EmptyResolver)
            .await
            .unwrap();
        let err = reg
            .create_instance(&ctx(), &DriverKind::new("a"), &config("i1"), &EmptyResolver)
            .await
            .err()
            .unwrap();
        assert!(matches!(err, RegistryError::DuplicateInstance(_)));
    }

    #[tokio::test]
    async fn failed_create_does_not_register_instance() {
        let mut reg = DriverRegistry::new();
        reg.register(factory("a", true)).unwrap();
        let id = DriverInstanceId::parse("i1").unwrap();
        assert!(
            reg.create_instance(&ctx(), &DriverKind::new("a"), &config("i1"), &EmptyResolver)
                .await
                .is_err()
        );
        assert_eq!(reg.instance_count(), 0);
        assert!(reg.instance(&id).is_none());
    }

    #[tokio::test]
    async fn multiple_instances_of_one_kind() {
        let mut reg = DriverRegistry::new();
        reg.register(factory("a", false)).unwrap();
        for name in ["a-1", "a-2"] {
            reg.create_instance(&ctx(), &DriverKind::new("a"), &config(name), &EmptyResolver)
                .await
                .unwrap();
        }
        assert_eq!(reg.instance_count(), 2);
    }

    #[tokio::test]
    async fn descriptors_expose_each_instance_identity() {
        let mut reg = DriverRegistry::new();
        reg.register(factory("a", false)).unwrap();
        for name in ["a-2", "a-1"] {
            reg.create_instance(&ctx(), &DriverKind::new("a"), &config(name), &EmptyResolver)
                .await
                .unwrap();
        }
        let descriptors = reg.descriptors();
        assert_eq!(descriptors.len(), 2);
        // Sorted by instance id, so the log order is deterministic.
        assert_eq!(descriptors[0].instance_id.as_str(), "a-1");
        assert_eq!(descriptors[0].kind.as_str(), "a");
        assert_eq!(descriptors[0].display_name, "noop");
    }

    /// A factory that rejects any configuration without a private `root`
    /// option, so the test can prove `validate` sees the configuration.
    struct ConfigCheckingFactory;

    #[async_trait]
    impl DriverFactory for ConfigCheckingFactory {
        fn kind(&self) -> DriverKind {
            DriverKind::new("configured")
        }

        async fn validate(
            &self,
            _ctx: &DriverContext,
            config: &FactoryConfig,
            _secrets: &dyn SecretResolver,
        ) -> Result<(), DriverError> {
            if config.options.contains_key("root") {
                Ok(())
            } else {
                Err(DriverError::invalid_argument("missing `root` option"))
            }
        }

        async fn create(
            &self,
            _ctx: &DriverContext,
            config: &FactoryConfig,
            _secrets: &dyn SecretResolver,
        ) -> Result<Arc<dyn StorageDriver>, DriverError> {
            Ok(Arc::new(NoopDriver {
                kind: self.kind(),
                instance_id: config.instance_id.clone(),
            }))
        }
    }

    #[tokio::test]
    async fn validate_receives_the_same_configuration_create_would() {
        let mut reg = DriverRegistry::new();
        reg.register(Arc::new(ConfigCheckingFactory)).unwrap();
        let kind = DriverKind::new("configured");
        let id = DriverInstanceId::parse("i1").unwrap();

        // The private option the factory requires is missing: validation fails
        // and no instance is registered.
        assert!(
            reg.create_instance(
                &ctx(),
                &kind,
                &FactoryConfig::minimal(id.clone()),
                &EmptyResolver
            )
            .await
            .is_err()
        );
        assert_eq!(reg.instance_count(), 0);

        // With the option present, the same factory validates and builds.
        let mut options = toml::Table::new();
        options.insert("root".into(), toml::Value::String("/tmp/allins3".into()));
        let configured = FactoryConfig::new(
            id.clone(),
            options,
            Vec::new(),
            crate::driver::BlockingExecutor::unbounded(),
        );
        reg.create_instance(&ctx(), &kind, &configured, &EmptyResolver)
            .await
            .expect("valid configuration creates");
        assert_eq!(reg.instance_count(), 1);
    }

    // Silence unused-import warning for capability type used nowhere else here.
    #[allow(dead_code)]
    fn _cap(_: NamespaceCapabilities) {}
}
