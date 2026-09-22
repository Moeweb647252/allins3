//! Fault injection.
//!
//! [`FaultDriverFactory`] wraps any [`DriverFactory`] and decorates the driver,
//! its namespace handles and its staged writes, so tests can force the failure
//! and lost-acknowledgement windows described in `docs/implementation-spec.md`
//! §5.1 and §9.2 without the wrapped driver knowing.
//!
//! Commit recovery is deliberately **not** wrapped: tests must observe the real
//! underlying [`CommitState`](crate::domain::CommitState).

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;

use crate::domain::{
    BeginWrite, CommitAttributes, DeleteRequest, DeleteResult, DriverContext, DriverError,
    DriverInstanceId, ListPage, ListRequest, NamespaceId, ObjectKey, ObjectMetadata,
};
use crate::driver::{
    CommitRecovery, DriverDescriptor, DriverFactory, DriverHealth, DriverKind, MultipartNamespace,
    NamespaceHandle, NativeCopy, ReadNamespace, ReadRequest, ReadResult, SecretResolver,
    StagedWrite, StorageDriver, WriteNamespace,
};

/// How a commit should misbehave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitFault {
    /// No fault.
    #[default]
    None,
    /// The underlying commit succeeds but the acknowledgement is lost; the
    /// wrapper reports an unknown outcome.
    LostAck,
    /// The attempt is fenced (never publishes) and then reported as an unknown
    /// outcome, so recovery must resolve it to `NotCommittedFinal`.
    FencedBeforePublish,
}

/// Which operations should fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FaultPlan {
    /// `stat` fails.
    pub fail_stat: bool,
    /// `read` fails.
    pub fail_read: bool,
    /// `list` fails.
    pub fail_list: bool,
    /// `begin_write` fails.
    pub fail_begin_write: bool,
    /// `delete` fails.
    pub fail_delete: bool,
    /// Commit misbehaviour.
    pub commit: CommitFault,
    /// The namespace answers no commit-recovery queries, so an unresolved
    /// commit outcome stays unknown instead of being reconcilable.
    pub hide_recovery: bool,
}

impl FaultPlan {
    /// A plan with no faults.
    pub fn none() -> Self {
        Self::default()
    }

    /// A plan whose commits lose their acknowledgement.
    pub fn lost_ack() -> Self {
        Self {
            commit: CommitFault::LostAck,
            ..Self::default()
        }
    }

    /// A plan whose commits are fenced before publish.
    pub fn fenced() -> Self {
        Self {
            commit: CommitFault::FencedBeforePublish,
            ..Self::default()
        }
    }
}

fn injected(op: &str) -> DriverError {
    DriverError::temporarily_unavailable(format!("injected fault: {op}"))
}

/// A factory that wraps an inner factory and injects faults.
pub struct FaultDriverFactory {
    inner: Arc<dyn DriverFactory>,
    plan: FaultPlan,
}

impl FaultDriverFactory {
    /// Wraps `inner` with the given fault plan.
    pub fn new(inner: Arc<dyn DriverFactory>, plan: FaultPlan) -> Self {
        Self { inner, plan }
    }
}

#[async_trait]
impl DriverFactory for FaultDriverFactory {
    fn kind(&self) -> DriverKind {
        self.inner.kind()
    }

    async fn validate(
        &self,
        ctx: &DriverContext,
        secrets: &dyn SecretResolver,
    ) -> Result<(), DriverError> {
        self.inner.validate(ctx, secrets).await
    }

    async fn create(
        &self,
        ctx: &DriverContext,
        instance_id: &DriverInstanceId,
        secrets: &dyn SecretResolver,
    ) -> Result<Arc<dyn StorageDriver>, DriverError> {
        let inner = self.inner.create(ctx, instance_id, secrets).await?;
        Ok(Arc::new(FaultDriver {
            inner,
            plan: self.plan,
            fenced_once: Arc::new(AtomicBool::new(false)),
        }))
    }
}

struct FaultDriver {
    inner: Arc<dyn StorageDriver>,
    plan: FaultPlan,
    /// Whether the one-shot fence has already been consumed.
    fenced_once: Arc<AtomicBool>,
}

#[async_trait]
impl StorageDriver for FaultDriver {
    fn descriptor(&self) -> DriverDescriptor {
        self.inner.descriptor()
    }

    async fn health(&self, ctx: &DriverContext) -> Result<DriverHealth, DriverError> {
        self.inner.health(ctx).await
    }

    async fn shutdown(&self, ctx: &DriverContext) -> Result<(), DriverError> {
        self.inner.shutdown(ctx).await
    }

    async fn open_namespace(
        &self,
        ctx: &DriverContext,
        namespace: &NamespaceId,
    ) -> Result<NamespaceHandle, DriverError> {
        let inner = self.inner.open_namespace(ctx, namespace).await?;
        let read = Arc::new(FaultRead {
            inner: inner.read.clone(),
            plan: self.plan,
        });
        let write = inner.write.clone().map(|w| {
            Arc::new(FaultWrite {
                inner: w,
                plan: self.plan,
                fenced_once: self.fenced_once.clone(),
            }) as Arc<dyn WriteNamespace>
        });
        // Recovery is observed unwrapped so tests see the real state, except
        // when the plan asks for a driver that cannot answer recovery queries at
        // all — then the capability declaration must agree with the handle.
        let mut capabilities = inner.capabilities.clone();
        let recovery = if self.plan.hide_recovery {
            capabilities.recovery = false;
            None
        } else {
            inner.recovery.clone()
        };
        Ok(NamespaceHandle {
            capabilities,
            read,
            write,
            multipart: inner.multipart.clone(),
            native_copy: inner.native_copy.clone(),
            recovery,
        })
    }
}

struct FaultRead {
    inner: Arc<dyn ReadNamespace>,
    plan: FaultPlan,
}

#[async_trait]
impl ReadNamespace for FaultRead {
    async fn stat(
        &self,
        ctx: &DriverContext,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, DriverError> {
        if self.plan.fail_stat {
            return Err(injected("stat"));
        }
        self.inner.stat(ctx, key).await
    }

    async fn read(&self, ctx: &DriverContext, req: ReadRequest) -> Result<ReadResult, DriverError> {
        if self.plan.fail_read {
            return Err(injected("read"));
        }
        self.inner.read(ctx, req).await
    }

    async fn list(&self, ctx: &DriverContext, req: ListRequest) -> Result<ListPage, DriverError> {
        if self.plan.fail_list {
            return Err(injected("list"));
        }
        self.inner.list(ctx, req).await
    }
}

struct FaultWrite {
    inner: Arc<dyn WriteNamespace>,
    plan: FaultPlan,
    fenced_once: Arc<AtomicBool>,
}

#[async_trait]
impl WriteNamespace for FaultWrite {
    async fn begin_write(
        &self,
        ctx: &DriverContext,
        req: BeginWrite,
    ) -> Result<Box<dyn StagedWrite>, DriverError> {
        if self.plan.fail_begin_write {
            return Err(injected("begin_write"));
        }
        let inner = self.inner.begin_write(ctx, req).await?;
        Ok(Box::new(FaultStagedWrite {
            inner,
            plan: self.plan,
            fenced_once: self.fenced_once.clone(),
        }))
    }

    async fn delete(
        &self,
        ctx: &DriverContext,
        req: DeleteRequest,
    ) -> Result<DeleteResult, DriverError> {
        if self.plan.fail_delete {
            return Err(injected("delete"));
        }
        self.inner.delete(ctx, req).await
    }
}

struct FaultStagedWrite {
    inner: Box<dyn StagedWrite>,
    plan: FaultPlan,
    fenced_once: Arc<AtomicBool>,
}

#[async_trait]
impl StagedWrite for FaultStagedWrite {
    async fn write_chunk(&mut self, ctx: &DriverContext, bytes: Bytes) -> Result<(), DriverError> {
        self.inner.write_chunk(ctx, bytes).await
    }

    async fn commit(
        self: Box<Self>,
        ctx: &DriverContext,
        attrs: CommitAttributes,
    ) -> Result<ObjectMetadata, DriverError> {
        let Self {
            inner,
            plan,
            fenced_once,
        } = *self;
        match plan.commit {
            CommitFault::None => inner.commit(ctx, attrs).await,
            CommitFault::LostAck => match inner.commit(ctx, attrs).await {
                Ok(_) => Err(DriverError::commit_outcome_unknown(
                    "injected: commit acknowledgement lost",
                )),
                Err(e) => Err(e),
            },
            CommitFault::FencedBeforePublish => {
                // Only the first attempt is fenced, modelling "the original
                // attempt was fenced; a later, correctly-governed replacement
                // succeeds". Recovery must resolve the first to
                // `NotCommittedFinal`.
                if fenced_once.swap(true, Ordering::SeqCst) {
                    return inner.commit(ctx, attrs).await;
                }
                inner.abort(ctx).await?;
                Err(DriverError::commit_outcome_unknown(
                    "injected: attempt fenced before publish",
                ))
            }
        }
    }

    async fn abort(self: Box<Self>, ctx: &DriverContext) -> Result<(), DriverError> {
        self.inner.abort(ctx).await
    }
}

// Keep the unused imports meaningful: the wrapper intentionally does not
// intercept multipart or native copy, but must preserve them.
#[allow(dead_code)]
fn _passthrough(
    _: Option<Arc<dyn MultipartNamespace>>,
    _: Option<Arc<dyn NativeCopy>>,
    _: Option<Arc<dyn CommitRecovery>>,
    _: NonZeroU32,
) {
}
