//! Composition root: configuration in, wired service out.
//!
//! The startup sequence is stated once here instead of being re-derived by each
//! caller: register the driver kinds this build supports → create the instances
//! the config names → build the object service over those instances and
//! capability-check every namespace before readiness → wrap the S3 protocol
//! adapter around it. `server::run` and the test harnesses each used to keep
//! their own copy of that sequence, which is how the adapter flags verified by
//! one path could drift from the flags served by another.
//!
//! This module sits above every other module: it depends on `config`, `driver`,
//! `service` and `protocol`, and none of them may depend on it. It stays
//! `pub(crate)` on purpose — the public entries for embedders remain
//! `server::run` (production) and `testing::fixture::service` /
//! `testing::protocol::ProtocolHarness` (tests), so no wiring shape is frozen
//! as a contract before a second consumer proves it (`ADR-0008` §2).

use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::{AccessPolicy, CredentialStore};
use crate::catalog::BucketCatalog;
use crate::config::{AppConfig, EnvSecretResolver, Limits};
use crate::domain::{DriverContext, DriverInstanceId, RequestId};
use crate::driver::{DriverFactory, DriverKind, DriverRegistry, SecretResolver, StorageDriver};
use crate::protocol::s3::{PrincipalLimiter, S3Handler, S3Service, adapter_config, build_service};
use crate::service::{ObjectService, ServiceError};

/// A driver context for startup work, before any request exists.
///
/// Every driver call needs one; startup has no request to borrow.
fn startup_ctx() -> DriverContext {
    DriverContext::new(RequestId::new("startup"))
}

/// Registers the driver kinds this build supports.
pub fn build_registry(config: &AppConfig) -> Result<DriverRegistry, String> {
    let mut registry = DriverRegistry::new();
    let kinds: Vec<String> = {
        let mut kinds: Vec<String> = config.drivers.iter().map(|d| d.kind.clone()).collect();
        kinds.sort();
        kinds.dedup();
        kinds
    };

    for kind in kinds {
        let factory: Arc<dyn DriverFactory> = match kind.as_str() {
            #[cfg(feature = "testing")]
            "test-reference" => Arc::new(crate::testing::reference::ReferenceDriverFactory::new(
                kind.clone(),
            )),
            #[cfg(feature = "testing")]
            "test-vec" => Arc::new(crate::testing::second::VecDriverFactory::new(kind.clone())),
            other => {
                return Err(format!(
                    "driver kind `{other}` is not registered in this build"
                ));
            }
        };
        registry
            .register(factory)
            .map_err(|e| format!("failed to register driver kind `{kind}`: {e}"))?;
    }
    Ok(registry)
}

/// Creates every driver instance the config names, keyed by instance id.
///
/// Config validation already guaranteed the ids are unique and the kinds
/// registered, so a failure here is an instance-level fault (bad secret,
/// unwritable backend), not a configuration mistake.
pub async fn instances_from_config(
    registry: &mut DriverRegistry,
    config: &AppConfig,
) -> Result<HashMap<String, Arc<dyn StorageDriver>>, String> {
    let ctx = startup_ctx();
    let mut instances: HashMap<String, Arc<dyn StorageDriver>> = HashMap::new();
    for driver in &config.drivers {
        let kind = DriverKind::new(driver.kind.clone());
        let instance_id = DriverInstanceId::parse(driver.id.clone())
            .map_err(|e| format!("invalid driver id `{}`: {e}", driver.id))?;
        let instance = registry
            .create_instance(&ctx, &kind, &instance_id, &EnvSecretResolver)
            .await
            .map_err(|e| format!("failed to create driver `{}`: {e}", driver.id))?;
        instances.insert(driver.id.clone(), instance);
    }
    Ok(instances)
}

/// Creates one driver instance from a factory the caller already holds.
///
/// The factory is validated before it is created, the same order
/// [`DriverRegistry::create_instance`] enforces, so a test cannot build an
/// instance a production path would have rejected. Each call registers the
/// factory in its own registry, because tests legitimately hand in two
/// factories of the same kind (two instances of one backend) — something a
/// single registry rejects as a duplicate kind.
pub async fn instance_from_factory(
    factory: Arc<dyn DriverFactory>,
    id: &str,
    ctx: &DriverContext,
    secrets: &dyn SecretResolver,
) -> Result<Arc<dyn StorageDriver>, String> {
    let mut registry = DriverRegistry::new();
    registry
        .register(factory.clone())
        .map_err(|e| format!("failed to register driver instance `{id}`: {e}"))?;
    let instance_id =
        DriverInstanceId::parse(id).map_err(|e| format!("invalid driver id `{id}`: {e}"))?;
    registry
        .create_instance(ctx, &factory.kind(), &instance_id, secrets)
        .await
        .map_err(|e| format!("failed to create driver `{id}`: {e}"))
}

/// Builds the object service over prepared instances and capability-checks
/// every namespace before it can serve.
///
/// Readiness is part of composition, not an optional follow-up: a service that
/// has not been checked must not be handed to a listener, so the check happens
/// here rather than at each call site.
pub async fn object_service(
    catalog: BucketCatalog,
    instances: HashMap<String, Arc<dyn StorageDriver>>,
    limits: Limits,
    access: AccessPolicy,
) -> Result<ObjectService, ServiceError> {
    let service = ObjectService::new(catalog, instances, limits, access);
    service.warmup(&startup_ctx()).await?;
    Ok(service)
}

/// Wraps an object service in the S3 protocol adapter.
///
/// The adapter configuration is derived from the same resolved limits the
/// service enforces, so the protocol layer and the service cannot disagree
/// about, say, the largest single write they accept (`ADR-0007` §3).
pub fn s3_service(
    service: ObjectService,
    credentials: Arc<CredentialStore>,
    policy: Arc<AccessPolicy>,
    limits: &Limits,
) -> S3Service {
    let limiter = PrincipalLimiter::new(limits.max_concurrent_requests_per_principal);
    let handler = S3Handler::new(service, credentials.clone(), limiter);
    build_service(handler, credentials, policy, adapter_config(limits))
}
