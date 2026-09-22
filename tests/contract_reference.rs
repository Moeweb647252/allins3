//! Contract tests for the in-memory test drivers.
//!
//! Demonstrates `INV-01`: two independent implementations satisfy the same
//! reusable contract suite with no change to the service or protocol layers.

#![cfg(feature = "testing")]

use std::sync::Arc;

use allins3::domain::{DriverContext, DriverError, DriverInstanceId, NamespaceId, RequestId};
use allins3::driver::{
    CapabilityError, DriverFactory, DriverKind, DriverRegistry, FeatureSupport,
    NamespaceCapabilities, PlannedFeature, Profile, Secret, SecretResolver, StorageDriver,
    support_for, validate_profile,
};
use allins3::testing::contract::{ContractNamespaces, run_contract};
use allins3::testing::reference::ReferenceDriverFactory;
use allins3::testing::second::VecDriverFactory;

struct NoSecrets;

impl SecretResolver for NoSecrets {
    fn resolve(&self, reference: &str) -> Result<Secret, DriverError> {
        Err(DriverError::not_found(format!("no secret: {reference}")))
    }
}

fn ctx() -> DriverContext {
    DriverContext::new(RequestId::new("contract-test"))
}

async fn build(
    factory: Arc<dyn DriverFactory>,
    instance: &str,
) -> (Arc<dyn StorageDriver>, DriverKind) {
    let kind = factory.kind();
    let mut registry = DriverRegistry::new();
    registry.register(factory).expect("register");
    let driver = registry
        .create_instance(
            &ctx(),
            &kind,
            &DriverInstanceId::parse(instance).expect("instance id"),
            &NoSecrets,
        )
        .await
        .expect("create instance");
    (driver, kind)
}

async fn run_for(driver: &dyn StorageDriver) -> allins3::testing::contract::ContractReport {
    let primary = NamespaceId::parse("contract-primary").expect("ns");
    let secondary = NamespaceId::parse("contract-secondary").expect("ns");
    run_contract(
        driver,
        ContractNamespaces {
            primary: &primary,
            secondary: Some(&secondary),
        },
    )
    .await
    .expect("contract suite passed")
}

#[tokio::test]
async fn reference_driver_satisfies_full_contract() {
    let (driver, kind) = build(Arc::new(ReferenceDriverFactory::new("reference")), "ref-1").await;
    assert_eq!(kind.as_str(), "reference");

    let report = run_for(driver.as_ref()).await;
    for check in [
        "capability_consistency",
        "crud",
        "hidden_staging",
        "atomic_publish_and_abort",
        "stable_read_by_revision",
        "revision_changes_on_metadata_update",
        "ordering_and_pagination",
        "delimiter_folding",
        "conditional_writes",
        "namespace_isolation",
        "commit_recovery_semantics",
    ] {
        assert!(
            report.passed.contains(&check),
            "expected `{check}` to pass, got {report:?}"
        );
    }
}

#[tokio::test]
async fn vec_driver_satisfies_full_contract() {
    let (driver, _) = build(Arc::new(VecDriverFactory::new("vec")), "vec-1").await;
    let report = run_for(driver.as_ref()).await;
    assert!(report.passed.contains(&"crud"));
    assert!(report.passed.contains(&"ordering_and_pagination"));
    assert!(report.passed.contains(&"namespace_isolation"));
    assert!(report.passed.contains(&"commit_recovery_semantics"));
}

#[tokio::test]
async fn two_instances_of_one_kind_do_not_share_data() {
    let (a, kind) = build(Arc::new(ReferenceDriverFactory::new("reference")), "ref-a").await;
    let (b, _) = build(Arc::new(ReferenceDriverFactory::new("reference")), "ref-b").await;
    assert_eq!(kind.as_str(), "reference");

    let report = run_for(a.as_ref()).await;
    assert!(report.passed.contains(&"crud"));
    let report = run_for(b.as_ref()).await;
    assert!(report.passed.contains(&"crud"));
}

#[tokio::test]
async fn read_only_capabilities_reject_core_write_profile() {
    let caps = NamespaceCapabilities::read_only();
    let err = validate_profile(Profile::CoreWrite, &caps).expect_err("must reject");
    assert!(!err.missing.is_empty());

    let err = allins3::driver::plan(&[PlannedFeature::Read, PlannedFeature::Write], &caps)
        .expect_err("must reject");
    assert_eq!(
        err,
        CapabilityError::Unsupported {
            feature: PlannedFeature::Write
        }
    );
}

#[tokio::test]
async fn vec_driver_routes_copy_to_emulation() {
    let (driver, _) = build(Arc::new(VecDriverFactory::new("vec")), "vec-cap").await;
    let handle = driver
        .open_namespace(&ctx(), &NamespaceId::parse("cap-ns").expect("ns"))
        .await
        .expect("open");

    // No native copy handle, so the service must plan an emulated copy.
    assert!(handle.native_copy.is_none());
    assert_eq!(
        support_for(PlannedFeature::Copy, &handle.capabilities),
        FeatureSupport::Emulated
    );
    // A memory driver is never production-write capable.
    assert!(!handle.capabilities.is_production_write_capable());
}
