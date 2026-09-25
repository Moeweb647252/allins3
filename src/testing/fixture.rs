//! Service-level test fixtures.
//!
//! Service tests need the same wiring as `server::run` — a registry, driver
//! instances, a bucket catalog, limits and an access policy — without binding a
//! listener. Keeping that in one place stops every test file from growing its
//! own slightly different copy.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{AccessPolicy, Action, Grant};
use crate::catalog::{BucketBinding, BucketCatalog};
use crate::config::Limits;
use crate::domain::{
    BucketId, DriverContext, DriverError, DriverInstanceId, NamespaceId, RequestId,
};
use crate::driver::{
    DriverFactory, DriverKind, DriverRegistry, Profile, Secret, SecretResolver, StorageDriver,
};
use crate::service::ObjectService;

/// A resolver that knows no secrets, for drivers that take none.
pub struct NoSecrets;

impl SecretResolver for NoSecrets {
    fn resolve(&self, reference: &str) -> Result<Secret, DriverError> {
        Err(DriverError::not_found(format!("no secret: {reference}")))
    }
}

/// A request context for tests.
pub fn ctx(tag: &str) -> DriverContext {
    DriverContext::new(RequestId::new(format!("test-{tag}")))
}

/// Limits with room for multi-megabyte test bodies.
pub fn default_limits() -> Limits {
    Limits {
        max_xml_bytes: 1024 * 1024,
        max_object_size: None,
        max_single_write_size: 32 * 1024 * 1024,
        max_list_keys: 1000,
        max_concurrent_requests: 64,
        max_concurrent_requests_per_principal: 16,
        max_concurrent_requests_per_driver: 64,
        header_timeout: Duration::from_secs(30),
        body_idle_timeout: Duration::from_secs(60),
        request_timeout: None,
        staging_quota_bytes: 256 * 1024 * 1024,
        multipart_lease: Duration::from_secs(3600),
        multipart_sweep_interval: None,
        list_token_ttl: Duration::from_secs(3600),
    }
}

/// An access policy granting `tester` every action on every bucket.
pub fn full_access() -> AccessPolicy {
    AccessPolicy::new(vec![Grant {
        principal: "tester".into(),
        bucket: "*".into(),
        actions: vec![Action::List, Action::Read, Action::Write, Action::Delete],
        prefix: String::new(),
    }])
}

/// One bucket bound to one driver instance.
pub struct BucketSpec<'a> {
    /// Logical bucket name.
    pub bucket: &'a str,
    /// Driver instance id, also the key in the service's instance map.
    pub driver: &'a str,
    /// Namespace inside that instance.
    pub namespace: &'a str,
    /// Compatibility tier.
    pub profile: Profile,
}

/// Builds an `ObjectService` over driver instances created from `factories`.
///
/// `factories` maps a driver instance id to the factory that builds it, so a
/// test can mix drivers (including fault-wrapped ones) in one service.
pub async fn service(
    factories: Vec<(&str, Arc<dyn DriverFactory>)>,
    buckets: Vec<BucketSpec<'_>>,
    limits: Limits,
    access: AccessPolicy,
) -> ObjectService {
    let mut drivers: HashMap<String, Arc<dyn StorageDriver>> = HashMap::new();
    for (id, factory) in factories {
        let mut registry = DriverRegistry::new();
        registry.register(factory.clone()).expect("register");
        let kind = factory.kind();
        let instance = registry
            .create_instance(
                &ctx("fixture"),
                &DriverKind::new(kind.as_str()),
                &DriverInstanceId::parse(id).expect("instance id"),
                &NoSecrets,
            )
            .await
            .expect("create instance");
        drivers.insert(id.to_string(), instance);
    }

    let bindings = buckets
        .into_iter()
        .map(|spec| BucketBinding {
            bucket: BucketId::parse(spec.bucket).expect("bucket id"),
            driver: DriverInstanceId::parse(spec.driver).expect("instance id"),
            namespace: NamespaceId::parse(spec.namespace).expect("namespace"),
            generation: 1,
            profile: spec.profile,
            readonly: false,
            region: "us-east-1".into(),
        })
        .collect::<Vec<_>>();

    let catalog = BucketCatalog::from_bindings(bindings).expect("catalog");
    let service = ObjectService::new(catalog, drivers, limits, access);
    service.warmup(&ctx("warmup")).await.expect("warmup");
    service
}
