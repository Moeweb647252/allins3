//! Contract and durability tests for the local filesystem driver.
//!
//! Two properties matter here that the in-memory drivers cannot show:
//!
//! 1. the backend passes the same contract suite as every other driver
//!    (`INV-01`), even though it declares no native copy and no native
//!    multipart;
//! 2. what survives a restart survives a *process*, not just a handle — the
//!    durability tests build a second driver over the same directory with no
//!    shared memory and read the object back.

#![cfg(feature = "testing")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::TryStreamExt;

use allins3::domain::{
    BeginWrite, CommitAttributes, CommitState, ContentProperties, DriverContext, DriverError,
    DriverInstanceId, EntityTag, NamespaceId, ObjectKey, Preconditions, RequestId, UserMetadata,
    WriteId,
};
use allins3::driver::{
    BlockingExecutor, DriverFactory, DriverKind, DriverRegistry, FactoryConfig, ReadRequest,
    Secret, SecretResolver, StorageDriver,
};
use allins3::drivers::fs::FsDriverFactory;
use allins3::testing::contract::{ContractNamespaces, run_contract};

struct NoSecrets;

impl SecretResolver for NoSecrets {
    fn resolve(&self, reference: &str) -> Result<Secret, DriverError> {
        Err(DriverError::not_found(format!("no secret: {reference}")))
    }
}

fn ctx() -> DriverContext {
    DriverContext::new(RequestId::new("fs-test"))
}

/// Drains a byte stream into one buffer.
async fn collect(stream: allins3::driver::ObjectByteStream) -> Bytes {
    let buffer: BytesMut = stream.try_collect().await.expect("collect stream");
    buffer.freeze()
}

/// A unique scratch directory for one test, removed by [`Scratch`] on drop.
struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path =
            std::env::temp_dir().join(format!("allins3-fs-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).expect("scratch dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Builds a fresh driver over `root` — nothing is shared with any other
/// instance, so this is as close to a process restart as an in-process test
/// can get.
async fn driver(root: &Path) -> Arc<dyn StorageDriver> {
    let factory: Arc<dyn DriverFactory> = Arc::new(FsDriverFactory::new("fs"));
    let mut registry = DriverRegistry::new();
    registry.register(factory).expect("register");

    let mut options = toml::Table::new();
    options.insert(
        "root".into(),
        toml::Value::String(root.display().to_string()),
    );
    let config = FactoryConfig::new(
        DriverInstanceId::parse("fs-1").expect("id"),
        options,
        Vec::new(),
        BlockingExecutor::unbounded(),
    );
    registry
        .create_instance(&ctx(), &DriverKind::new("fs"), &config, &NoSecrets)
        .await
        .expect("create instance")
}

async fn open(driver: &Arc<dyn StorageDriver>) -> allins3::driver::NamespaceHandle {
    driver
        .open_namespace(&ctx(), &NamespaceId::parse("fs-contract").expect("ns"))
        .await
        .expect("open namespace")
}

async fn put(
    driver: &Arc<dyn StorageDriver>,
    key: &str,
    bytes: &[u8],
    preconditions: Preconditions,
) -> allins3::domain::ObjectMetadata {
    let handle = open(driver).await;
    let write_id = WriteId::generate();
    let mut staged = handle
        .write
        .as_ref()
        .expect("writable")
        .begin_write(
            &ctx(),
            BeginWrite {
                key: ObjectKey::parse(key).expect("key"),
                expected_length: Some(bytes.len() as u64),
                preconditions,
                write_id,
                content: ContentProperties {
                    content_type: Some("text/plain".into()),
                    ..ContentProperties::default()
                },
                user_metadata: UserMetadata::new(),
                checksum_algorithm: None,
            },
        )
        .await
        .expect("begin write");
    staged
        .write_chunk(&ctx(), Bytes::copy_from_slice(bytes))
        .await
        .expect("write chunk");
    staged
        .commit(
            &ctx(),
            CommitAttributes {
                length: bytes.len() as u64,
                etag: None,
                checksums: Vec::new(),
            },
        )
        .await
        .expect("commit")
}

#[tokio::test]
async fn the_filesystem_driver_satisfies_the_contract() {
    let scratch = Scratch::new("contract");
    let driver = driver(scratch.path()).await;
    let primary = NamespaceId::parse("fs-contract").expect("ns");
    let secondary = NamespaceId::parse("fs-contract-secondary").expect("ns");
    run_contract(
        &*driver,
        ContractNamespaces {
            primary: &primary,
            secondary: Some(&secondary),
        },
    )
    .await
    .expect("contract suite passed");
}

#[tokio::test]
async fn an_object_survives_a_new_driver_over_the_same_directory() {
    let scratch = Scratch::new("durable");

    let first = driver(scratch.path()).await;
    let written = put(&first, "dir/hello", b"survive me", Preconditions::none()).await;

    // A second driver with no shared state: the bytes can only come from disk.
    let second = driver(scratch.path()).await;
    let handle = open(&second).await;
    let stat = handle
        .read
        .stat(&ctx(), &ObjectKey::parse("dir/hello").expect("key"))
        .await
        .expect("stat after restart");
    assert_eq!(stat, written);

    let result = handle
        .read
        .read(
            &ctx(),
            ReadRequest {
                key: ObjectKey::parse("dir/hello").expect("key"),
                expected_revision: written.revision.clone(),
                range: None,
            },
        )
        .await
        .expect("read after restart");
    let bytes: Bytes = collect(result.stream).await;
    assert_eq!(&bytes[..], b"survive me");
}

#[tokio::test]
async fn a_stale_revision_is_refused_rather_than_served() {
    let scratch = Scratch::new("revision");
    let driver = driver(scratch.path()).await;
    let handle = open(&driver).await;

    let first = put(&driver, "k", b"one", Preconditions::none()).await;
    let _second = put(&driver, "k", b"two", Preconditions::none()).await;

    // The revision captured by the first read is no longer current.
    let err = handle
        .read
        .read(
            &ctx(),
            ReadRequest {
                key: ObjectKey::parse("k").expect("key"),
                expected_revision: first.revision.clone(),
                range: None,
            },
        )
        .await
        .expect_err("stale revision must be refused");
    assert_eq!(
        err.kind(),
        allins3::domain::DriverErrorKind::RevisionUnavailable
    );
}

/// Stages a write for `key` with the given attempt id.
async fn stage(
    handle: &allins3::driver::NamespaceHandle,
    key: &str,
    write_id: WriteId,
) -> Box<dyn allins3::driver::StagedWrite> {
    handle
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
        .expect("begin write")
}

#[tokio::test]
async fn an_aborted_attempt_cannot_publish_and_leaves_nothing() {
    let scratch = Scratch::new("abort");
    let driver = driver(scratch.path()).await;
    let handle = open(&driver).await;
    let write_id = WriteId::generate();

    // Two handles of one attempt, as a crash-then-retry ordering produces: the
    // second is aborted while the first is still able to call `commit`.
    let mut stale = stage(&handle, "never", write_id.clone()).await;
    let live = stage(&handle, "never", write_id.clone()).await;
    live.abort(&ctx()).await.expect("abort");

    stale
        .write_chunk(&ctx(), Bytes::from_static(b"data"))
        .await
        .expect("chunk");
    let err = stale
        .commit(
            &ctx(),
            CommitAttributes {
                length: 4,
                etag: None,
                checksums: Vec::new(),
            },
        )
        .await
        .expect_err("a fenced attempt must not publish");
    assert_eq!(err.commit_state(), Some(CommitState::NotCommittedFinal));

    // Nothing was published.
    assert!(
        handle
            .read
            .stat(&ctx(), &ObjectKey::parse("never").expect("key"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_fresh_attempt_after_an_abort_can_still_publish() {
    // Aborting one attempt must not poison the key for the retry the service
    // makes with a fresh `WriteId` (the P4-05 lesson).
    let scratch = Scratch::new("retry");
    let driver = driver(scratch.path()).await;
    let handle = open(&driver).await;

    let first = stage(&handle, "retryable", WriteId::generate()).await;
    first.abort(&ctx()).await.expect("abort");

    let mut second = stage(&handle, "retryable", WriteId::generate()).await;
    second
        .write_chunk(&ctx(), Bytes::from_static(b"data"))
        .await
        .expect("chunk");
    second
        .commit(
            &ctx(),
            CommitAttributes {
                length: 4,
                etag: None,
                checksums: Vec::new(),
            },
        )
        .await
        .expect("a fresh attempt publishes");
}

#[tokio::test]
async fn a_conditional_commit_is_rejected_and_leaves_no_object() {
    let scratch = Scratch::new("conditional");
    let driver = driver(scratch.path()).await;
    put(&driver, "present", b"original", Preconditions::none()).await;

    let handle = open(&driver).await;
    let err = {
        let mut staged = handle
            .write
            .as_ref()
            .expect("writable")
            .begin_write(
                &ctx(),
                BeginWrite {
                    key: ObjectKey::parse("present").expect("key"),
                    expected_length: Some(3),
                    preconditions: Preconditions::if_match(
                        EntityTag::new("not-the-current-etag").expect("tag"),
                    ),
                    write_id: WriteId::generate(),
                    content: ContentProperties::default(),
                    user_metadata: UserMetadata::new(),
                    checksum_algorithm: None,
                },
            )
            .await
            .expect("begin");
        staged
            .write_chunk(&ctx(), Bytes::from_static(b"new"))
            .await
            .expect("chunk");
        staged
            .commit(
                &ctx(),
                CommitAttributes {
                    length: 3,
                    etag: None,
                    checksums: Vec::new(),
                },
            )
            .await
            .expect_err("If-Match must fail")
    };
    assert_eq!(err.commit_state(), Some(CommitState::NotCommittedFinal));

    // The original object is untouched.
    let result = handle
        .read
        .read(
            &ctx(),
            ReadRequest {
                key: ObjectKey::parse("present").expect("key"),
                expected_revision: handle
                    .read
                    .stat(&ctx(), &ObjectKey::parse("present").expect("key"))
                    .await
                    .expect("stat")
                    .revision,
                range: None,
            },
        )
        .await
        .expect("read");
    let bytes: Bytes = collect(result.stream).await;
    assert_eq!(&bytes[..], b"original");
}

#[tokio::test]
async fn the_recovery_record_survives_a_restart() {
    let scratch = Scratch::new("recovery");

    let first = driver(scratch.path()).await;
    let handle = open(&first).await;
    let write_id = WriteId::generate();
    let mut staged = handle
        .write
        .as_ref()
        .expect("writable")
        .begin_write(
            &ctx(),
            BeginWrite {
                key: ObjectKey::parse("k").expect("key"),
                expected_length: Some(2),
                preconditions: Preconditions::none(),
                write_id: write_id.clone(),
                content: ContentProperties::default(),
                user_metadata: UserMetadata::new(),
                checksum_algorithm: None,
            },
        )
        .await
        .expect("begin");
    staged
        .write_chunk(&ctx(), Bytes::from_static(b"hi"))
        .await
        .expect("chunk");
    staged
        .commit(
            &ctx(),
            CommitAttributes {
                length: 2,
                etag: None,
                checksums: Vec::new(),
            },
        )
        .await
        .expect("commit");

    let second = driver(scratch.path()).await;
    let handle = open(&second).await;
    let recovery = handle.recovery.as_ref().expect("recovery declared");
    assert_eq!(
        recovery.query(&ctx(), &write_id).await.expect("query"),
        CommitState::Committed
    );

    // A write id the driver has never seen is `Unknown`, never a final failure.
    assert_eq!(
        recovery
            .query(&ctx(), &WriteId::generate())
            .await
            .expect("query"),
        CommitState::Unknown
    );
}
