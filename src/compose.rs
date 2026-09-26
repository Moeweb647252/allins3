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
use crate::config::{AppConfig, Limits};
use crate::domain::{DriverContext, DriverInstanceId, RequestId};
#[cfg(feature = "testing")]
use crate::driver::DriverFactory;
use crate::driver::{DriverKind, DriverRegistry, SecretResolver, StorageDriver};
use crate::protocol::s3::{PrincipalLimiter, S3Handler, S3Service, adapter_config, build_service};
use crate::service::{ObjectService, ServiceError};

/// A driver context for startup work, before any request exists.
///
/// Every driver call needs one; startup has no request to borrow.
fn startup_ctx() -> DriverContext {
    DriverContext::new(RequestId::new("startup"))
}

/// Registers the driver kinds this build supports.
///
/// The kinds a build can construct are data ([`crate::driver::builtins`]), not a
/// `match` arm per kind: this function only looks each configured kind up in
/// that table, so adding a driver never touches the composition root and no
/// build carries an arm it cannot reach.
pub fn build_registry(config: &AppConfig) -> Result<DriverRegistry, String> {
    let builtins = crate::driver::builtins::builtins();
    let kinds: Vec<&str> = {
        let mut kinds: Vec<&str> = config.drivers.iter().map(|d| d.kind.as_str()).collect();
        kinds.sort_unstable();
        kinds.dedup();
        kinds
    };

    let mut registry = DriverRegistry::new();
    for kind in kinds {
        let builtin = builtins
            .iter()
            .find(|builtin| builtin.name == kind)
            .ok_or_else(|| unregistered_kind(kind))?;
        registry
            .register((builtin.ctor)(kind))
            .map_err(|e| format!("failed to register driver kind `{kind}`: {e}"))?;
    }
    Ok(registry)
}

/// The error for a kind no registered factory can construct.
///
/// It names the kinds the build *does* have, so the common cause — a test kind
/// in a build without the `testing` feature — is visible without reading the
/// source.
fn unregistered_kind(kind: &str) -> String {
    let available = crate::driver::builtins::registered_kinds();
    if available.is_empty() {
        format!(
            "driver kind `{kind}` is not registered in this build (this build registers no driver kinds)"
        )
    } else {
        format!(
            "driver kind `{kind}` is not registered in this build (available: {})",
            available.join(", ")
        )
    }
}

/// Creates every driver instance the config names, keyed by instance id.
///
/// The secret resolver is a parameter, not a hard-wired environment read: the
/// production entry passes [`crate::config::EnvSecretResolver`], and a test can
/// pass a resolver it controls. Config validation already guaranteed the ids
/// are unique and the kinds registered, so a failure here is an instance-level
/// fault (bad secret, unwritable backend), not a configuration mistake.
pub async fn instances_from_config(
    registry: &mut DriverRegistry,
    config: &AppConfig,
    secrets: &dyn SecretResolver,
) -> Result<HashMap<String, Arc<dyn StorageDriver>>, String> {
    let ctx = startup_ctx();
    let mut instances: HashMap<String, Arc<dyn StorageDriver>> = HashMap::new();
    for driver in &config.drivers {
        let kind = DriverKind::new(driver.kind.clone());
        let instance_id = DriverInstanceId::parse(driver.id.clone())
            .map_err(|e| format!("invalid driver id `{}`: {e}", driver.id))?;
        let instance = registry
            .create_instance(&ctx, &kind, &instance_id, secrets)
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
#[cfg(feature = "testing")]
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
/// here rather than at each call site. `production` comes from the resolved
/// configuration and tightens that check for writable bindings, so the caller
/// cannot forget to pass the deployment mode along with the limits.
pub async fn object_service(
    catalog: BucketCatalog,
    instances: HashMap<String, Arc<dyn StorageDriver>>,
    limits: Limits,
    access: AccessPolicy,
    production: bool,
) -> Result<ObjectService, ServiceError> {
    let service = ObjectService::new(catalog, instances, limits, access, production);
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
