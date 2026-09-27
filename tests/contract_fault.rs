//! Fault-injection tests for the commit-outcome model (`INV-09`).
//!
//! These cover the two orderings the spec calls out: a commit that published
//! but whose acknowledgement was lost, and a commit that was fenced before
//! publishing. Recovery must distinguish them, and only the fenced attempt may
//! be replaced.

#![cfg(feature = "testing")]

use std::sync::Arc;

use allins3::domain::{
    BeginWrite, CommitState, ContentProperties, DriverContext, DriverError, DriverInstanceId,
    NamespaceId, ObjectKey, Preconditions, RequestId, UserMetadata, WriteId,
};
use allins3::driver::{
    DriverFactory, DriverKind, DriverRegistry, FactoryConfig, NamespaceHandle, Secret,
    SecretResolver, StorageDriver,
};
use allins3::testing::contract::{ContractNamespaces, run_contract};
use allins3::testing::fault::{FaultDriverFactory, FaultPlan};
use allins3::testing::reference::ReferenceDriverFactory;

struct NoSecrets;

impl SecretResolver for NoSecrets {
    fn resolve(&self, reference: &str) -> Result<Secret, DriverError> {
        Err(DriverError::not_found(format!("no secret: {reference}")))
    }
}

fn ctx() -> DriverContext {
    DriverContext::new(RequestId::new("fault-test"))
}

async fn build(plan: FaultPlan, instance: &str) -> Arc<dyn StorageDriver> {
    let inner: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    let factory: Arc<dyn DriverFactory> = Arc::new(FaultDriverFactory::new(inner, plan));
    let kind: DriverKind = factory.kind();
    let mut registry = DriverRegistry::new();
    registry.register(factory).expect("register");
    let config = FactoryConfig::minimal(DriverInstanceId::parse(instance).expect("id"));
    registry
        .create_instance(&ctx(), &kind, &config, &NoSecrets)
        .await
        .expect("create")
}

async fn open(driver: &dyn StorageDriver, name: &str) -> NamespaceHandle {
    driver
        .open_namespace(&ctx(), &NamespaceId::parse(name).expect("ns"))
        .await
        .expect("open namespace")
}

async fn begin(
    handle: &NamespaceHandle,
    key: &str,
    write_id: WriteId,
) -> Box<dyn allins3::driver::StagedWrite> {
    use bytes::Bytes;
    let mut staged = handle
        .write
        .as_ref()
        .expect("writable")
        .begin_write(
            &ctx(),
            BeginWrite {
                key: ObjectKey::parse(key).expect("key"),
                expected_length: Some(4),
                preconditions: Preconditions::none(),
                write_id,
                content: ContentProperties::default(),
                user_metadata: UserMetadata::new(),
                checksum_algorithm: None,
            },
        )
        .await
        .expect("begin write");
    staged
        .write_chunk(&ctx(), Bytes::from_static(b"data"))
        .await
        .expect("write chunk");
    staged
}

#[tokio::test]
async fn wrapper_with_no_faults_is_transparent() {
    let driver = build(FaultPlan::none(), "fault-none").await;
    let primary = NamespaceId::parse("fault-none-ns").expect("ns");
    let report = run_contract(
        driver.as_ref(),
        ContractNamespaces {
            primary: &primary,
            secondary: None,
        },
    )
    .await
    .expect("contract passes through a transparent wrapper");
    assert!(report.passed.contains(&"crud"));
    assert!(report.passed.contains(&"commit_recovery_semantics"));
}

#[tokio::test]
async fn lost_acknowledgement_is_committed_and_not_replaceable() {
    let driver = build(FaultPlan::lost_ack(), "fault-lost").await;
    let handle = open(driver.as_ref(), "fault-lost-ns").await;

    let key = ObjectKey::parse("lost-ack").expect("key");
    let write_id = WriteId::generate();
    let staged = begin(&handle, "lost-ack", write_id.clone()).await;

    let err = staged
        .commit(
            &ctx(),
            allins3::domain::CommitAttributes {
                length: 4,
                etag: None,
                checksums: Vec::new(),
            },
        )
        .await
        .expect_err("acknowledgement must be reported lost");

    assert_eq!(err.commit_state(), Some(CommitState::Unknown));

    // Recovery reveals the attempt actually published.
    let recovery = handle.recovery.as_ref().expect("recovery");
    let state = recovery.query(&ctx(), &write_id).await.expect("query");
    assert_eq!(state, CommitState::Committed);
    assert!(!state.permits_replacement_attempt());
    assert!(state.is_recoverable_success());

    // The object is present despite the lost acknowledgement.
    let stat = handle.read.stat(&ctx(), &key).await;
    assert!(stat.is_ok(), "published object missing: {stat:?}");
}

#[tokio::test]
async fn fenced_attempt_is_finally_not_committed_and_replaceable() {
    let driver = build(FaultPlan::fenced(), "fault-fenced").await;
    let handle = open(driver.as_ref(), "fault-fenced-ns").await;

    let key = ObjectKey::parse("fenced").expect("key");
    let write_id = WriteId::generate();
    let staged = begin(&handle, "fenced", write_id.clone()).await;

    let err = staged
        .commit(
            &ctx(),
            allins3::domain::CommitAttributes {
                length: 4,
                etag: None,
                checksums: Vec::new(),
            },
        )
        .await
        .expect_err("must report an unknown outcome");

    assert_eq!(err.commit_state(), Some(CommitState::Unknown));

    let recovery = handle.recovery.as_ref().expect("recovery");
    let state = recovery.query(&ctx(), &write_id).await.expect("query");
    assert_eq!(state, CommitState::NotCommittedFinal);
    assert!(state.permits_replacement_attempt());

    // Fenced means nothing was published, so a replacement may proceed.
    let stat = handle.read.stat(&ctx(), &key).await;
    assert!(
        matches!(stat, Err(ref e) if e.kind() == allins3::domain::DriverErrorKind::NotFound),
        "fenced attempt left an object behind: {stat:?}"
    );

    let replacement = begin(&handle, "fenced", WriteId::generate()).await;
    replacement
        .commit(
            &ctx(),
            allins3::domain::CommitAttributes {
                length: 4,
                etag: None,
                checksums: Vec::new(),
            },
        )
        .await
        .expect("replacement commit after fence");
    assert!(handle.read.stat(&ctx(), &key).await.is_ok());
}

#[tokio::test]
async fn injected_read_failure_does_not_change_capabilities() {
    let driver = build(
        FaultPlan {
            fail_stat: true,
            ..FaultPlan::none()
        },
        "fault-stat",
    )
    .await;
    let handle = open(driver.as_ref(), "fault-stat-ns").await;

    let err = handle
        .read
        .stat(&ctx(), &ObjectKey::parse("anything").expect("key"))
        .await
        .expect_err("stat must fail");
    assert_eq!(
        err.kind(),
        allins3::domain::DriverErrorKind::TemporarilyUnavailable
    );
    // Availability is not a capability: the declaration is unchanged.
    assert!(handle.capabilities.write);
    assert_eq!(
        allins3::driver::support_for(allins3::driver::PlannedFeature::Write, &handle.capabilities),
        allins3::driver::FeatureSupport::Native
    );
}
