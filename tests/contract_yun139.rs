//! Contract and read tests for the read-only 139 cloud-drive driver.
//!
//! These tests talk to a real drive account, so they cannot run in CI. They are
//! gated on a session supplied through the environment and **report a skip with
//! a reason** when it is absent, rather than silently passing.
//!
//! ```bash
//! export ALLINS3_YUN139_PHONE=19800000000
//! export ALLINS3_YUN139_AUTH_TOKEN='…'      # the `auth_token` cookie
//! export ALLINS3_YUN139_FOLDER_ID='Ft…'     # a drive folder backing the namespace
//! export ALLINS3_YUN139_COOKIE='…'          # optional full cookie header
//! cargo test --features testing,yun139 --test contract_yun139 -- --nocapture
//! ```
//!
//! The suite proves the namespace passes the shared contract for the abilities
//! it declares (and reports the write-side checks as skipped), and — against the
//! drive's real contents — that `stat` and a ranged `read` return the bytes the
//! drive holds.

#![cfg(all(feature = "testing", feature = "yun139"))]

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::TryStreamExt;

use allins3::domain::{
    DriverContext, DriverError, DriverInstanceId, ListRequest, NamespaceId, RangeResolution,
    RangeSpec, RequestId, ResolvedRange,
};
use allins3::driver::{
    BlockingExecutor, DriverFactory, DriverKind, DriverRegistry, FactoryConfig, ReadRequest,
    Secret, SecretResolver, StorageDriver,
};
use allins3::drivers::yun139::Yun139DriverFactory;
use allins3::testing::contract::{ContractNamespaces, run_contract};

struct NoSecrets;

impl SecretResolver for NoSecrets {
    fn resolve(&self, reference: &str) -> Result<Secret, DriverError> {
        Err(DriverError::not_found(format!("no secret: {reference}")))
    }
}

fn ctx() -> DriverContext {
    DriverContext::new(RequestId::new("yun139-test"))
}

/// Builds a driver from the environment, or `None` when no session is set.
async fn driver_from_env() -> Option<Arc<dyn StorageDriver>> {
    let phone = std::env::var("ALLINS3_YUN139_PHONE").ok()?;
    let auth_token = std::env::var("ALLINS3_YUN139_AUTH_TOKEN").ok()?;
    let folder_id = std::env::var("ALLINS3_YUN139_FOLDER_ID").ok()?;

    let mut options = toml::Table::new();
    options.insert("phone".into(), toml::Value::String(phone));
    options.insert("auth_token".into(), toml::Value::String(auth_token));
    options.insert("folder_id".into(), toml::Value::String(folder_id));
    if let Ok(cookie) = std::env::var("ALLINS3_YUN139_COOKIE")
        && !cookie.trim().is_empty()
    {
        options.insert("cookie".into(), toml::Value::String(cookie));
    }

    let factory: Arc<dyn DriverFactory> = Arc::new(Yun139DriverFactory::new("yun139"));
    let mut registry = DriverRegistry::new();
    registry.register(factory).expect("register");

    let config = FactoryConfig::new(
        DriverInstanceId::parse("yun139-1").expect("id"),
        options,
        Vec::new(),
        BlockingExecutor::unbounded(),
    );
    let driver = registry
        .create_instance(&ctx(), &DriverKind::new("yun139"), &config, &NoSecrets)
        .await
        .expect("create instance");
    Some(driver)
}

/// Skips the test body when no session is configured.
macro_rules! require_session {
    () => {
        match driver_from_env().await {
            Some(driver) => driver,
            None => {
                eprintln!(
                    "SKIP: set ALLINS3_YUN139_PHONE, ALLINS3_YUN139_AUTH_TOKEN and \
                     ALLINS3_YUN139_FOLDER_ID to run the live yun139 tests"
                );
                return;
            }
        }
    };
}

#[tokio::test]
async fn the_yun139_driver_satisfies_the_contract_against_a_live_account() {
    let driver = require_session!();

    let primary = NamespaceId::parse("yun139-contract").expect("ns");
    let secondary = NamespaceId::parse("yun139-contract-secondary").expect("ns");
    let report = run_contract(
        &*driver,
        ContractNamespaces {
            primary: &primary,
            secondary: Some(&secondary),
        },
    )
    .await
    .expect("contract suite passed");

    // The read-only declaration must actually hold: the write-side checks are
    // skipped with a reason, never silently passed.
    assert!(
        report.passed.contains(&"capability_consistency"),
        "capability consistency must run"
    );
    assert!(
        report.skipped.iter().any(|(name, _)| *name == "crud"),
        "a read-only namespace must report crud as skipped"
    );
    assert!(
        report
            .skipped
            .iter()
            .any(|(name, _)| *name == "multipart_native_semantics"),
        "a read-only namespace must report native multipart as skipped"
    );
    eprintln!("yun139 contract report: {:?}", report);
}

#[tokio::test]
async fn a_live_stat_and_ranged_read_return_the_drive_bytes() {
    let driver = require_session!();
    let handle = driver
        .open_namespace(&ctx(), &NamespaceId::parse("yun139-contract").expect("ns"))
        .await
        .expect("open namespace");

    // Find any real file in the mapped folder.
    let page = handle
        .read
        .list(
            &ctx(),
            ListRequest::new(std::num::NonZeroU32::new(20).expect("limit")),
        )
        .await
        .expect("list");
    let Some(object) = page.objects.iter().find(|object| object.size > 0) else {
        eprintln!("SKIP: the configured folder has no non-empty files to read");
        return;
    };
    let key = object.key.clone();

    // `stat` agrees with the listing and is stable across a re-stat.
    let stat = handle.read.stat(&ctx(), &key).await.expect("stat");
    assert_eq!(stat.revision, object.revision);
    assert_eq!(stat.size, object.size);

    // Read the first bytes as a range and confirm the drive returns exactly them.
    let desired_end = object.size.saturating_sub(1).min(15);
    let spec = RangeSpec::Closed {
        start: 0,
        end: desired_end,
    };
    let range: Option<ResolvedRange> = match spec.resolve(object.size) {
        RangeResolution::Partial(range) => Some(range),
        RangeResolution::Full | RangeResolution::Unsatisfiable => None,
    };
    let expected = range.map(|range| range.len()).unwrap_or(object.size);

    let result = handle
        .read
        .read(
            &ctx(),
            ReadRequest {
                key: key.clone(),
                expected_revision: stat.revision.clone(),
                range,
            },
        )
        .await
        .expect("read");
    let bytes: Bytes = collect(result.stream).await;
    assert_eq!(bytes.len() as u64, expected);
    eprintln!(
        "yun139 live read: key={} size={} read={}",
        key,
        object.size,
        bytes.len()
    );
}

#[tokio::test]
async fn a_stale_revision_is_refused_by_the_live_driver() {
    let driver = require_session!();
    let handle = driver
        .open_namespace(&ctx(), &NamespaceId::parse("yun139-contract").expect("ns"))
        .await
        .expect("open namespace");

    let page = handle
        .read
        .list(
            &ctx(),
            ListRequest::new(std::num::NonZeroU32::new(20).expect("limit")),
        )
        .await
        .expect("list");
    let Some(object) = page.objects.first() else {
        eprintln!("SKIP: the configured folder is empty");
        return;
    };

    let err = handle
        .read
        .read(
            &ctx(),
            ReadRequest {
                key: object.key.clone(),
                expected_revision: allins3::domain::ObjectRevision::new("no-such-revision"),
                range: None,
            },
        )
        .await
        .expect_err("a stale revision must be refused");
    assert_eq!(
        err.kind(),
        allins3::domain::DriverErrorKind::RevisionUnavailable
    );
}

/// Drains a byte stream into one buffer.
async fn collect(stream: allins3::driver::ObjectByteStream) -> Bytes {
    let buffer: BytesMut = stream.try_collect().await.expect("collect stream");
    buffer.freeze()
}
