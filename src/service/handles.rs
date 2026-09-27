//! Namespace handles: from a bucket binding to a usable driver namespace.
//!
//! This module owns the driver instance table and the handle cache, and nothing
//! else does. A caller hands it a [`BucketBinding`] and gets back a handle that
//! is already open and already capability-checked for that binding's profile;
//! it never learns that a driver table or a cache key exists.
//!
//! Two rules from ADR-0004 are load-bearing here:
//!
//! - the handle is pinned to the binding's **generation** (ADR-0004 §5), so a
//!   generation is never silently swapped under a request, a page token or an
//!   upload. The cache key is `bucket#generation` for exactly that reason;
//! - authorization is *not* this module's business (ADR-0004 §8): a handle says
//!   which namespace a binding reaches, never who may reach it.
//!
//! A binding whose profile the namespace cannot serve fails closed here — it is
//! a configuration fault, not a client error — so a capability mismatch is
//! reported as `Internal` rather than served as a broken success. The same
//! rule applies in production: a writable binding is admitted only when its
//! namespace is `is_production_write_capable`.
//!
//! Opening a namespace also derives its [`CapabilityPlan`], and that plan — not
//! the raw flags — is what routing consults (spec §3.3: 可选功能必须通过能力规划
//! 选择原生、补齐或拒绝).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::catalog::BucketBinding;
use crate::domain::DriverContext;
use crate::driver::{
    CapabilityPlan, Consistency, FeatureSupport, MultipartNamespace, NamespaceCapabilities,
    NamespaceHandle, NativeCopy, Persistence, PlannedFeature, Profile, StorageDriver,
    WriteNamespace, features_for_profile, plan, validate_profile,
};
use crate::sync;

use super::ServiceError;

/// An opened namespace together with the plan derived from its declarations.
///
/// Routing reads the plan, never the raw capability flags: `support`,
/// [`Namespace::copy_route`], [`Namespace::native_multipart`] and
/// [`Namespace::writer`] are the only places a Native/Emulated choice is made,
/// so a namespace cannot be planned one way and routed another. It derefs to
/// the driver handle, so callers that only need the interfaces are unchanged.
#[derive(Debug)]
pub(crate) struct Namespace {
    handle: NamespaceHandle,
    plan: CapabilityPlan,
}

impl std::ops::Deref for Namespace {
    type Target = NamespaceHandle;

    fn deref(&self) -> &NamespaceHandle {
        &self.handle
    }
}

/// How a copy is provided for one namespace.
pub(crate) enum CopyRoute<'a> {
    /// The driver copies within its own domain.
    Native(&'a Arc<dyn NativeCopy>),
    /// The service reads the source and stages a destination write.
    Emulated,
}

impl Namespace {
    /// How the service will provide `feature` here.
    pub(crate) fn support(&self, feature: PlannedFeature) -> FeatureSupport {
        self.plan.support(feature)
    }

    /// The copy route the plan selects.
    ///
    /// A `Native` plan implies the interface is present: the plan is derived
    /// from the driver's declared capabilities, and [`NamespaceHandles::get`]
    /// refuses a handle whose declarations and interfaces disagree
    /// (`is_self_consistent`), so the handle lookup cannot silently downgrade a
    /// planned native copy. The fallback arm therefore only carries the
    /// `Emulated` (and unreachable `Unsupported`) cases.
    pub(crate) fn copy_route(&self) -> CopyRoute<'_> {
        match (self.support(PlannedFeature::Copy), &self.handle.native_copy) {
            (FeatureSupport::Native, Some(native)) => CopyRoute::Native(native),
            _ => CopyRoute::Emulated,
        }
    }

    /// The native multipart interface, when the plan selects the native route.
    pub(crate) fn native_multipart(&self) -> Option<&Arc<dyn MultipartNamespace>> {
        match (
            self.support(PlannedFeature::Multipart),
            &self.handle.multipart,
        ) {
            (FeatureSupport::Native, Some(multipart)) => Some(multipart),
            _ => None,
        }
    }

    /// The staged-write interface, when the plan makes this namespace writable.
    ///
    /// `Write` has no emulated form: a namespace that cannot stage and commit a
    /// write is not writable, and the plan says so. Gating on the plan rather
    /// than on the raw handle keeps the answer identical to every other route —
    /// a `CoreRead` binding plans no `Write` at all, so it stays unwritable even
    /// if its driver happens to expose a writer.
    pub(crate) fn writer(&self) -> Option<&Arc<dyn WriteNamespace>> {
        match (self.support(PlannedFeature::Write), &self.handle.write) {
            (FeatureSupport::Native, Some(write)) => Some(write),
            _ => None,
        }
    }
}

/// The driver instances of one service and the namespace handles opened from
/// them.
pub(crate) struct NamespaceHandles {
    drivers: HashMap<String, Arc<dyn StorageDriver>>,
    /// Whether this service is running in production mode.
    ///
    /// Production is a stronger bar than a satisfiable profile: a namespace
    /// that declares volatile storage or a non-exclusive writer still passes
    /// `validate_profile` for `CoreWrite` but cannot back a persistent write.
    /// `is_production_write_capable` is the single predicate that says so, and
    /// it is evaluated here so a disqualified binding fails at startup rather
    /// than silently serving writes that a restart erases (ADR-0006 §4).
    production: bool,
    cache: Mutex<HashMap<String, Arc<Namespace>>>,
}

impl NamespaceHandles {
    /// A table over the resolved driver instances, keyed by instance id.
    pub(crate) fn new(drivers: HashMap<String, Arc<dyn StorageDriver>>, production: bool) -> Self {
        Self {
            drivers,
            production,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// The open, capability-checked handle for `binding`.
    ///
    /// Opening is done once per binding generation and then cached; a caller
    /// that asks again gets the same handle rather than a second backend
    /// connection.
    pub(crate) async fn get(
        &self,
        binding: &BucketBinding,
        ctx: &DriverContext,
    ) -> Result<Arc<Namespace>, ServiceError> {
        let cache_key = format!("{}#{}", binding.bucket, binding.generation);
        if let Some(namespace) = sync::lock(&self.cache).get(&cache_key).cloned() {
            return Ok(namespace);
        }

        let driver = self
            .drivers
            .get(binding.driver.as_str())
            .cloned()
            .ok_or_else(|| {
                ServiceError::Internal(format!("driver `{}` is not registered", binding.driver))
            })?;
        let handle = driver
            .open_namespace(ctx, &binding.namespace)
            .await
            .map_err(|e| ServiceError::from_driver(&e, false))?;

        // The interfaces a driver exposes and the capabilities it declares must
        // agree; a declared capability with no handle (or vice versa) is a
        // driver bug, and routing reads the plan — which is derived from the
        // declarations — so an inconsistent handle would silently route a
        // native choice at an interface that is not there. This is the one
        // place a handle enters the service, so the check lives here rather
        // than in the test harness alone (the harness asserts the same thing).
        if !handle.is_self_consistent() {
            return Err(ServiceError::Internal(format!(
                "driver `{}` opened namespace `{}` with capabilities that disagree with its interfaces",
                binding.driver, binding.namespace
            )));
        }

        validate_profile(binding.profile, &handle.capabilities).map_err(|e| {
            ServiceError::Internal(format!(
                "bucket `{}` capability mismatch: {e}",
                binding.bucket
            ))
        })?;

        // A read-only profile writes nothing, so production admission does not
        // apply to it; every writable profile must clear the stronger bar.
        if self.production && !matches!(binding.profile, Profile::CoreRead) {
            let caps = &handle.capabilities;
            if !caps.is_production_write_capable() {
                return Err(ServiceError::Internal(format!(
                    "bucket `{}` is not production-write-capable: {}",
                    binding.bucket,
                    production_write_shortfall(caps)
                )));
            }
        }

        // The plan is derived once, here, from the declarations just read. A
        // feature the profile needs but the namespace cannot provide (natively
        // or through an adapter) fails startup instead of surfacing as a broken
        // operation, and every later routing decision reads this plan.
        let plan =
            plan(features_for_profile(binding.profile), &handle.capabilities).map_err(|e| {
                ServiceError::Internal(format!(
                    "bucket `{}` capability mismatch: {e}",
                    binding.bucket
                ))
            })?;

        let namespace = Arc::new(Namespace { handle, plan });
        sync::lock(&self.cache).insert(cache_key, namespace.clone());
        Ok(namespace)
    }
}

/// Names the declarations that keep a namespace from production writes.
///
/// The predicate and this list must agree; the message exists so a refused
/// startup says which capability to fix instead of only that one is missing.
fn production_write_shortfall(caps: &NamespaceCapabilities) -> String {
    let mut reasons = Vec::new();
    if !caps.write {
        reasons.push("atomic staged write is not supported");
    }
    if caps.persistence == Persistence::Volatile {
        reasons.push("persistence is volatile");
    }
    if caps.consistency == Consistency::Eventual {
        reasons.push("consistency is eventual");
    }
    if !caps.exclusive_writer {
        reasons.push("the adapter does not guarantee exclusive writes");
    }
    reasons.join("; ")
}

#[cfg(all(test, feature = "testing"))]
mod production_admission_tests {
    use super::*;
    use crate::domain::{BucketId, DriverError, DriverInstanceId, NamespaceId, RequestId};
    use crate::driver::{DriverFactory, Persistence, Secret, SecretResolver};

    struct NoSecrets;

    impl SecretResolver for NoSecrets {
        fn resolve(&self, _reference: &str) -> Result<Secret, DriverError> {
            Err(DriverError::not_found("none"))
        }
    }

    fn ctx() -> DriverContext {
        DriverContext::new(RequestId::new("handles-test"))
    }

    async fn handles(production: bool, caps: NamespaceCapabilities) -> NamespaceHandles {
        let factory: Arc<dyn DriverFactory> = Arc::new(
            crate::testing::reference::ReferenceDriverFactory::with_capabilities("reference", caps),
        );
        let driver = crate::compose::instance_from_factory(
            factory,
            "ref-a",
            toml::Table::new(),
            &ctx(),
            &NoSecrets,
        )
        .await
        .expect("instance");
        let mut drivers = HashMap::new();
        drivers.insert("ref-a".to_string(), driver);
        NamespaceHandles::new(drivers, production)
    }

    fn binding(profile: Profile) -> BucketBinding {
        BucketBinding {
            bucket: BucketId::parse("b1").expect("bucket"),
            driver: DriverInstanceId::parse("ref-a").expect("driver"),
            namespace: NamespaceId::parse("ns").expect("namespace"),
            generation: 1,
            profile,
            readonly: false,
            region: "us-east-1".into(),
        }
    }

    #[tokio::test]
    async fn production_refuses_a_volatile_writable_binding() {
        let caps = NamespaceCapabilities::full_memory();
        let err = handles(true, caps)
            .await
            .get(&binding(Profile::CoreWrite), &ctx())
            .await
            .expect_err("must refuse");
        let ServiceError::Internal(message) = err else {
            panic!("expected an internal configuration fault");
        };
        assert!(message.contains("production-write-capable"), "{message}");
        assert!(message.contains("volatile"), "{message}");
    }

    #[tokio::test]
    async fn development_allows_a_volatile_writable_binding() {
        let caps = NamespaceCapabilities::full_memory();
        assert!(
            handles(false, caps)
                .await
                .get(&binding(Profile::CoreWrite), &ctx())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn production_allows_a_durable_writable_binding() {
        let mut caps = NamespaceCapabilities::full_memory();
        caps.persistence = Persistence::Durable;
        assert!(
            handles(true, caps)
                .await
                .get(&binding(Profile::CoreWrite), &ctx())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn production_admission_does_not_apply_to_read_only() {
        let caps = NamespaceCapabilities::read_only();
        assert!(
            handles(true, caps)
                .await
                .get(&binding(Profile::CoreRead), &ctx())
                .await
                .is_ok()
        );
    }
}
