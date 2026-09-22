//! The reusable driver contract harness.
//!
//! Any driver — reference, fault-wrapped or a future real adapter — can be
//! checked by passing its factory-created instance and a set of namespaces to
//! [`run_contract`]. The harness branches on the namespace's declared
//! capabilities: checks whose preconditions are not met are *skipped and
//! reported*, never silently passed.
//!
//! The harness exercises only the public domain/driver interfaces. Adding a new
//! driver implementation requires no change here or in the service layer
//! (`INV-01`).

use std::num::NonZeroU32;

use bytes::Bytes;

use crate::domain::{
    BeginWrite, CommitState, ContentProperties, DriverContext, DriverError, DriverErrorKind,
    ListCursor, ListRequest, NamespaceId, ObjectKey, Preconditions, RequestId, UserMetadata,
    WriteId,
};
use crate::driver::{
    CommitRecovery, NamespaceHandle, PlannedFeature, Profile, StorageDriver, support_for,
    validate_profile,
};

use super::support::{new_write_id, put_object, put_object_with_metadata, read_to_end};

/// Namespaces the harness may use for isolation checks.
pub struct ContractNamespaces<'a> {
    /// The namespace under test.
    pub primary: &'a NamespaceId,
    /// An optional second namespace used to prove isolation.
    pub secondary: Option<&'a NamespaceId>,
}

/// The outcome of a contract run.
#[derive(Debug, Default, Clone)]
pub struct ContractReport {
    /// Checks that ran and passed.
    pub passed: Vec<&'static str>,
    /// Checks that were skipped and why.
    pub skipped: Vec<(&'static str, &'static str)>,
}

/// A contract check failed, with the check name and a detail message.
#[derive(Debug, Clone)]
pub struct ContractFailure {
    /// The failing check.
    pub check: &'static str,
    /// What went wrong.
    pub detail: String,
}

impl std::fmt::Display for ContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "contract check `{}` failed: {}", self.check, self.detail)
    }
}

impl std::error::Error for ContractFailure {}

fn ctx(tag: &str) -> DriverContext {
    DriverContext::new(RequestId::new(format!("contract-{tag}")))
}

fn err_detail(e: &DriverError) -> String {
    format!("{}: {}", e.kind().as_str(), e.message())
}

fn require(cond: bool, msg: impl Into<String>) -> Result<(), String> {
    if cond { Ok(()) } else { Err(msg.into()) }
}

fn limit(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).expect("non-zero page limit")
}

fn key(s: &str) -> ObjectKey {
    ObjectKey::parse(s).expect("valid test key")
}

/// Runs the full contract suite for one driver and namespace set.
pub async fn run_contract(
    driver: &dyn StorageDriver,
    namespaces: ContractNamespaces<'_>,
) -> Result<ContractReport, ContractFailure> {
    let ctx = ctx("main");
    let handle = driver
        .open_namespace(&ctx, namespaces.primary)
        .await
        .map_err(|e| ContractFailure {
            check: "open_namespace",
            detail: err_detail(&e),
        })?;
    let caps = handle.capabilities.clone();

    let mut report = ContractReport::default();

    run(
        "capability_consistency",
        require(
            handle.is_self_consistent(),
            "declared capabilities and optional handles disagree",
        ),
        &mut report,
    )?;

    run(
        "profile_validation",
        validate_profile(Profile::CoreRead, &caps)
            .map_err(|e| e.to_string())
            .and_then(|_| {
                if caps.write {
                    validate_profile(Profile::CoreWrite, &caps).map_err(|e| e.to_string())
                } else {
                    Ok(())
                }
            }),
        &mut report,
    )?;

    // Skip checks that need primitives the namespace does not declare.
    if !caps.read || !caps.write {
        report
            .skipped
            .push(("crud", "requires read + write capabilities"));
    } else {
        run("crud", check_crud(&handle, &ctx).await, &mut report)?;
        run(
            "hidden_staging",
            check_hidden_staging(&handle, &ctx).await,
            &mut report,
        )?;
        run(
            "atomic_publish_and_abort",
            check_atomic_publish_and_abort(&handle, &ctx).await,
            &mut report,
        )?;
        run(
            "stable_read_by_revision",
            check_stable_read_by_revision(&handle, &ctx).await,
            &mut report,
        )?;
        run(
            "revision_changes_on_metadata_update",
            check_revision_changes_on_metadata_update(&handle, &ctx).await,
            &mut report,
        )?;
        run(
            "ordering_and_pagination",
            check_ordering_and_pagination(&handle, &ctx).await,
            &mut report,
        )?;
        run(
            "delimiter_folding",
            check_delimiter_folding(&handle, &ctx).await,
            &mut report,
        )?;
    }

    if !caps.read || !caps.list {
        report
            .skipped
            .push(("list_absent_key", "requires read + list"));
    } else {
        run(
            "list_absent_key",
            check_list_absent_key(&handle, &ctx).await,
            &mut report,
        )?;
    }

    let conditional = matches!(
        support_for(PlannedFeature::Conditional, &caps),
        crate::driver::FeatureSupport::Native
    );
    if !caps.write || !conditional {
        report.skipped.push((
            "conditional_writes",
            "requires atomic conditional write support",
        ));
    } else {
        run(
            "conditional_writes",
            check_conditional_writes(&handle, &ctx).await,
            &mut report,
        )?;
    }

    if let Some(secondary) = namespaces.secondary {
        if !caps.read || !caps.write {
            report
                .skipped
                .push(("namespace_isolation", "requires read + write"));
        } else {
            run(
                "namespace_isolation",
                check_namespace_isolation(driver, namespaces.primary, secondary).await,
                &mut report,
            )?;
        }
    }

    if caps.recovery {
        run(
            "commit_recovery_semantics",
            check_commit_recovery(&handle).await,
            &mut report,
        )?;
    } else {
        report.skipped.push((
            "commit_recovery_semantics",
            "namespace does not declare commit recovery",
        ));
    }

    Ok(report)
}

fn run(
    name: &'static str,
    result: Result<(), String>,
    report: &mut ContractReport,
) -> Result<(), ContractFailure> {
    match result {
        Ok(()) => {
            report.passed.push(name);
            Ok(())
        }
        Err(detail) => Err(ContractFailure {
            check: name,
            detail,
        }),
    }
}

async fn check_crud(handle: &NamespaceHandle, ctx: &DriverContext) -> Result<(), String> {
    let k = key("crud/hello.txt");
    let data = b"hello world".to_vec();
    let meta = put_object(handle, ctx, k.clone(), &data, Preconditions::none())
        .await
        .map_err(|e| err_detail(&e))?;
    require(meta.size == data.len() as u64, "size mismatch after commit")?;

    let stat = handle
        .read
        .stat(ctx, &k)
        .await
        .map_err(|e| err_detail(&e))?;
    require(
        stat.revision == meta.revision,
        "stat revision differs from commit",
    )?;
    require(stat.etag.is_some(), "expected an ETag from the driver")?;

    let read = handle
        .read
        .read(
            ctx,
            crate::driver::ReadRequest {
                key: k.clone(),
                expected_revision: stat.revision.clone(),
                range: None,
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    let bytes = read_to_end(read.stream).await.map_err(|e| err_detail(&e))?;
    require(bytes == data, "read returned different bytes")?;

    let deleted = handle
        .write
        .as_ref()
        .expect("writable")
        .delete(
            ctx,
            crate::domain::DeleteRequest {
                key: k.clone(),
                preconditions: Preconditions::none(),
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    require(deleted.deleted, "delete reported nothing removed")?;

    let missing = handle.read.stat(ctx, &k).await;
    require(
        matches!(missing, Err(ref e) if e.kind() == DriverErrorKind::NotFound),
        "deleted key still readable",
    )?;

    // Deleting an absent key is a success with deleted = false.
    let again = handle
        .write
        .as_ref()
        .expect("writable")
        .delete(
            ctx,
            crate::domain::DeleteRequest {
                key: k,
                preconditions: Preconditions::none(),
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    require(
        !again.deleted,
        "second delete of an absent key reported a removal",
    )?;
    Ok(())
}

async fn check_hidden_staging(handle: &NamespaceHandle, ctx: &DriverContext) -> Result<(), String> {
    let k = key("staging/secret.bin");
    let mut staged = handle
        .write
        .as_ref()
        .expect("writable")
        .begin_write(
            ctx,
            BeginWrite {
                key: k.clone(),
                expected_length: Some(5),
                preconditions: Preconditions::none(),
                write_id: new_write_id(),
                content: ContentProperties::default(),
                user_metadata: UserMetadata::new(),
                checksum_algorithm: None,
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    staged
        .write_chunk(ctx, Bytes::from_static(b"aaaaa"))
        .await
        .map_err(|e| err_detail(&e))?;

    let stat = handle.read.stat(ctx, &k).await;
    require(
        matches!(stat, Err(ref e) if e.kind() == DriverErrorKind::NotFound),
        "staged (uncommitted) object is visible to stat",
    )?;

    let page = list_page(handle, ctx, &k, None).await?;
    require(
        !page.objects.iter().any(|o| o.key == k),
        "staged (uncommitted) object is visible to list",
    )?;

    staged.abort(ctx).await.map_err(|e| err_detail(&e))?;
    let after = handle.read.stat(ctx, &k).await;
    require(
        matches!(after, Err(ref e) if e.kind() == DriverErrorKind::NotFound),
        "aborted staged object became visible",
    )?;
    Ok(())
}

async fn check_atomic_publish_and_abort(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
) -> Result<(), String> {
    let k = key("atomic/value");
    put_object(handle, ctx, k.clone(), b"old", Preconditions::none())
        .await
        .map_err(|e| err_detail(&e))?;

    // A failed/aborted commit must leave the old object untouched.
    let mut staged = handle
        .write
        .as_ref()
        .expect("writable")
        .begin_write(
            ctx,
            BeginWrite {
                key: k.clone(),
                expected_length: Some(3),
                preconditions: Preconditions::none(),
                write_id: new_write_id(),
                content: ContentProperties::default(),
                user_metadata: UserMetadata::new(),
                checksum_algorithm: None,
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    staged
        .write_chunk(ctx, Bytes::from_static(b"new"))
        .await
        .map_err(|e| err_detail(&e))?;
    staged.abort(ctx).await.map_err(|e| err_detail(&e))?;

    let stat = handle
        .read
        .stat(ctx, &k)
        .await
        .map_err(|e| err_detail(&e))?;
    let read = handle
        .read
        .read(
            ctx,
            crate::driver::ReadRequest {
                key: k.clone(),
                expected_revision: stat.revision.clone(),
                range: None,
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    let bytes = read_to_end(read.stream).await.map_err(|e| err_detail(&e))?;
    require(bytes == b"old", "aborted write replaced the old object")?;

    // A committed write publishes the whole new value.
    put_object(handle, ctx, k.clone(), b"new!", Preconditions::none())
        .await
        .map_err(|e| err_detail(&e))?;
    let stat = handle
        .read
        .stat(ctx, &k)
        .await
        .map_err(|e| err_detail(&e))?;
    let read = handle
        .read
        .read(
            ctx,
            crate::driver::ReadRequest {
                key: k,
                expected_revision: stat.revision.clone(),
                range: None,
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    let bytes = read_to_end(read.stream).await.map_err(|e| err_detail(&e))?;
    require(bytes == b"new!", "committed value not visible")?;
    Ok(())
}

async fn check_stable_read_by_revision(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
) -> Result<(), String> {
    let k = key("stable/object");
    let first = put_object(handle, ctx, k.clone(), b"v1", Preconditions::none())
        .await
        .map_err(|e| err_detail(&e))?;
    let second = put_object(handle, ctx, k.clone(), b"v2", Preconditions::none())
        .await
        .map_err(|e| err_detail(&e))?;
    require(
        first.revision != second.revision,
        "overwrite did not change the revision",
    )?;

    // Reading the superseded revision must fail with RevisionUnavailable; it
    // must never silently return the current object.
    let result = handle
        .read
        .read(
            ctx,
            crate::driver::ReadRequest {
                key: k.clone(),
                expected_revision: first.revision.clone(),
                range: None,
            },
        )
        .await;
    require(
        matches!(result, Err(ref e) if e.kind() == DriverErrorKind::RevisionUnavailable),
        "stale revision did not report RevisionUnavailable",
    )?;

    // The current revision reads back v2.
    let read = handle
        .read
        .read(
            ctx,
            crate::driver::ReadRequest {
                key: k,
                expected_revision: second.revision.clone(),
                range: None,
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    let bytes = read_to_end(read.stream).await.map_err(|e| err_detail(&e))?;
    require(bytes == b"v2", "current revision read wrong bytes")?;
    Ok(())
}

async fn check_revision_changes_on_metadata_update(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
) -> Result<(), String> {
    let k = key("meta/revision");
    let mut first_meta = UserMetadata::new();
    first_meta.insert("tag", "one").map_err(|e| e.to_string())?;
    let first = put_object_with_metadata(
        handle,
        ctx,
        k.clone(),
        b"same",
        Preconditions::none(),
        ContentProperties::default(),
        first_meta,
    )
    .await
    .map_err(|e| err_detail(&e))?;

    let mut second_meta = UserMetadata::new();
    second_meta
        .insert("tag", "two")
        .map_err(|e| e.to_string())?;
    let second = put_object_with_metadata(
        handle,
        ctx,
        k,
        b"same",
        Preconditions::none(),
        ContentProperties::default(),
        second_meta,
    )
    .await
    .map_err(|e| err_detail(&e))?;

    require(
        first.revision != second.revision,
        "metadata-only update reused the revision",
    )?;
    Ok(())
}

async fn check_ordering_and_pagination(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
) -> Result<(), String> {
    // Insert out of order; deletion keeps the namespace tidy for other checks.
    let keys = ["page/c", "page/a", "page/e", "page/b", "page/d"];
    for k in keys {
        put_object(handle, ctx, key(k), k.as_bytes(), Preconditions::none())
            .await
            .map_err(|e| err_detail(&e))?;
    }

    let mut collected: Vec<String> = Vec::new();
    let mut cursor: Option<ListCursor> = None;
    let mut guard = 0;
    loop {
        guard += 1;
        require(guard < 100, "pagination did not terminate")?;
        let mut req = ListRequest::new(limit(2))
            .with_prefix("page/")
            .map_err(|e| e.to_string())?;
        if let Some(c) = cursor.clone() {
            req = req.with_cursor(c).map_err(|e| e.to_string())?;
        }
        let page = handle
            .read
            .list(ctx, req)
            .await
            .map_err(|e| err_detail(&e))?;
        for o in &page.objects {
            collected.push(o.key.to_string());
        }
        match page.next_cursor {
            Some(c) if page.is_truncated => cursor = Some(c),
            _ => break,
        }
    }

    let mut expected: Vec<String> = keys.iter().map(|s| (*s).to_string()).collect();
    expected.sort();
    require(
        collected == expected,
        format!("paged listing mismatch: got {collected:?}, want {expected:?}"),
    )?;

    for k in keys {
        let _ = handle
            .write
            .as_ref()
            .expect("writable")
            .delete(
                ctx,
                crate::domain::DeleteRequest {
                    key: key(k),
                    preconditions: Preconditions::none(),
                },
            )
            .await;
    }
    Ok(())
}

async fn check_delimiter_folding(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
) -> Result<(), String> {
    let keys = ["fold/a/1", "fold/a/2", "fold/b", "fold/c/1"];
    for k in keys {
        put_object(handle, ctx, key(k), b"x", Preconditions::none())
            .await
            .map_err(|e| err_detail(&e))?;
    }

    let req = ListRequest::new(limit(100))
        .with_prefix("fold/")
        .map_err(|e| e.to_string())?
        .with_delimiter('/');
    let page = handle
        .read
        .list(ctx, req)
        .await
        .map_err(|e| err_detail(&e))?;
    require(
        page.common_prefixes == vec!["fold/a/".to_string(), "fold/c/".to_string()],
        format!("unexpected common prefixes: {:?}", page.common_prefixes),
    )?;
    require(
        page.objects.len() == 1 && page.objects[0].key.as_str() == "fold/b",
        "unexpected objects under delimiter listing",
    )?;

    for k in keys {
        let _ = handle
            .write
            .as_ref()
            .expect("writable")
            .delete(
                ctx,
                crate::domain::DeleteRequest {
                    key: key(k),
                    preconditions: Preconditions::none(),
                },
            )
            .await;
    }
    Ok(())
}

async fn check_list_absent_key(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
) -> Result<(), String> {
    let req = ListRequest::new(limit(10))
        .with_prefix("definitely/absent/")
        .map_err(|e| e.to_string())?;
    let page = handle
        .read
        .list(ctx, req)
        .await
        .map_err(|e| err_detail(&e))?;
    require(
        page.objects.is_empty() && page.common_prefixes.is_empty() && !page.is_truncated,
        "listing a nonexistent prefix returned entries",
    )?;
    Ok(())
}

async fn check_conditional_writes(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
) -> Result<(), String> {
    let k = key("conditional/object");

    // If-None-Match: * succeeds when absent.
    let created = put_object(
        handle,
        ctx,
        k.clone(),
        b"first",
        Preconditions::must_not_exist(),
    )
    .await
    .map_err(|e| err_detail(&e))?;

    // If-None-Match: * fails when present.
    let conflict = put_object(
        handle,
        ctx,
        k.clone(),
        b"second",
        Preconditions::must_not_exist(),
    )
    .await;
    require(
        matches!(conflict, Err(ref e) if e.kind() == DriverErrorKind::PreconditionFailed),
        "If-None-Match: * did not reject an existing object",
    )?;

    // If-Match with the wrong tag fails.
    let wrong = crate::domain::EntityTag::new("deadbeef").map_err(|e| e.to_string())?;
    let mismatch = put_object(
        handle,
        ctx,
        k.clone(),
        b"third",
        Preconditions::if_match(wrong),
    )
    .await;
    require(
        matches!(mismatch, Err(ref e) if e.kind() == DriverErrorKind::PreconditionFailed),
        "If-Match did not reject a mismatched tag",
    )?;

    // If-Match with the right tag succeeds.
    let tag = created.etag.ok_or("driver returned no ETag")?;
    put_object(handle, ctx, k, b"fourth", Preconditions::if_match(tag))
        .await
        .map_err(|e| err_detail(&e))?;
    Ok(())
}

async fn check_namespace_isolation(
    driver: &dyn StorageDriver,
    primary: &NamespaceId,
    secondary: &NamespaceId,
) -> Result<(), String> {
    let ctx = ctx("isolation");
    let a = driver
        .open_namespace(&ctx, primary)
        .await
        .map_err(|e| err_detail(&e))?;
    let b = driver
        .open_namespace(&ctx, secondary)
        .await
        .map_err(|e| err_detail(&e))?;
    require(a.capabilities.read, "primary namespace is not readable")?;

    let k = key("shared/name.txt");
    put_object(&a, &ctx, k.clone(), b"from-a", Preconditions::none())
        .await
        .map_err(|e| err_detail(&e))?;

    // The same key must not be visible in the other namespace.
    let b_stat = b.read.stat(&ctx, &k).await;
    require(
        matches!(b_stat, Err(ref e) if e.kind() == DriverErrorKind::NotFound),
        "same key leaked between namespaces",
    )?;

    put_object(&b, &ctx, k.clone(), b"from-b", Preconditions::none())
        .await
        .map_err(|e| err_detail(&e))?;

    // Each namespace must observe its own content. Revisions are opaque and may
    // legitimately collide between namespaces, so content is the real check.
    let a_bytes = read_key(&a, &ctx, &k).await?;
    let b_bytes = read_key(&b, &ctx, &k).await?;
    require(a_bytes == b"from-a", "primary namespace content changed")?;
    require(b_bytes == b"from-b", "secondary namespace content changed")?;
    Ok(())
}

/// Reads a key back using its current revision and returns the bytes.
async fn read_key(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
    k: &ObjectKey,
) -> Result<Vec<u8>, String> {
    let stat = handle.read.stat(ctx, k).await.map_err(|e| err_detail(&e))?;
    let read = handle
        .read
        .read(
            ctx,
            crate::driver::ReadRequest {
                key: k.clone(),
                expected_revision: stat.revision.clone(),
                range: None,
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    read_to_end(read.stream).await.map_err(|e| err_detail(&e))
}

async fn check_commit_recovery(handle: &NamespaceHandle) -> Result<(), String> {
    let ctx = ctx("recovery");
    let recovery: &dyn CommitRecovery = handle.recovery.as_deref().expect("recovery declared");

    // A missing record is not proof of final non-commit.
    let unknown = recovery
        .query(&ctx, &WriteId::from_raw("never-seen"))
        .await
        .map_err(|e| err_detail(&e))?;
    require(
        unknown == CommitState::Unknown,
        format!("missing record produced {unknown:?}, not Unknown"),
    )?;

    // Abort must reach the final not-committed state, and a late commit of the
    // fenced attempt must be refused.
    let k = key("recovery/fenced");
    let write_id = new_write_id();
    let mut staged = handle
        .write
        .as_ref()
        .expect("writable")
        .begin_write(
            &ctx,
            BeginWrite {
                key: k.clone(),
                expected_length: Some(1),
                preconditions: Preconditions::none(),
                write_id: write_id.clone(),
                content: ContentProperties::default(),
                user_metadata: UserMetadata::new(),
                checksum_algorithm: None,
            },
        )
        .await
        .map_err(|e| err_detail(&e))?;
    staged
        .write_chunk(&ctx, Bytes::from_static(b"x"))
        .await
        .map_err(|e| err_detail(&e))?;
    staged.abort(&ctx).await.map_err(|e| err_detail(&e))?;

    let state = recovery
        .query(&ctx, &write_id)
        .await
        .map_err(|e| err_detail(&e))?;
    require(
        state == CommitState::NotCommittedFinal,
        format!("aborted attempt reported {state:?}, not NotCommittedFinal"),
    )?;

    let stat = handle.read.stat(&ctx, &k).await;
    require(
        matches!(stat, Err(ref e) if e.kind() == DriverErrorKind::NotFound),
        "aborted attempt left an object behind",
    )?;
    Ok(())
}

async fn list_page(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
    prefix_key: &ObjectKey,
    cursor: Option<ListCursor>,
) -> Result<crate::domain::ListPage, String> {
    let mut req = ListRequest::new(limit(100))
        .with_prefix(prefix_key.as_str())
        .map_err(|e| e.to_string())?;
    if let Some(c) = cursor {
        req = req.with_cursor(c).map_err(|e| e.to_string())?;
    }
    handle.read.list(ctx, req).await.map_err(|e| err_detail(&e))
}
