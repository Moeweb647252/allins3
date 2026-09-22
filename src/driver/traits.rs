//! The object-safe driver interfaces.
//!
//! Every public async trait is annotated with `#[async_trait]` and returns
//! `Result<_, DriverError>`. Shared interfaces are used through `Arc<dyn _>`
//! for dynamic dispatch; a staged write is owned and consumed through
//! `Box<dyn StagedWrite>`. All futures are `Send` (the default
//! `#[async_trait]` behaviour); `DriverContext` is `Send + Sync`.
//!
//! Data always flows as [`Bytes`] chunks or a
//! `Stream<Item = Result<Bytes, DriverError>>`; an entire object is never
//! required as a `Vec<u8>`.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

use crate::domain::{
    BeginWrite, CommitAttributes, DeleteRequest, DeleteResult, DriverContext, DriverError,
    ListPage, ListRequest, NamespaceId, ObjectKey, ObjectMetadata, ObjectRevision, ResolvedRange,
};

use super::capabilities::NamespaceCapabilities;
use super::descriptor::{DriverDescriptor, DriverHealth};

/// A back-pressured stream of object bytes.
pub type ObjectByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, DriverError>> + Send>>;

/// A read pinned to one revision.
///
/// `expected_revision` is mandatory. A driver must return data and metadata
/// that both belong to this revision; it must not silently fall back to the
/// current object when the revision is unreadable.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    /// The object key.
    pub key: ObjectKey,
    /// The revision captured by an earlier `stat`.
    pub expected_revision: ObjectRevision,
    /// An already-resolved byte range, if the caller requested one.
    pub range: Option<ResolvedRange>,
}

/// The result of a read: metadata and data for the same revision.
pub struct ReadResult {
    /// Metadata for the requested revision.
    pub metadata: ObjectMetadata,
    /// The object bytes, honouring the requested range.
    pub stream: ObjectByteStream,
}

impl std::fmt::Debug for ReadResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadResult")
            .field("metadata", &self.metadata)
            .field("stream", &"<byte stream>")
            .finish()
    }
}

/// A namespace opened for reading.
#[async_trait]
pub trait ReadNamespace: Send + Sync {
    /// Returns complete metadata and the current revision for a key.
    async fn stat(
        &self,
        ctx: &DriverContext,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, DriverError>;

    /// Reads an exact revision, optionally within a resolved range.
    async fn read(&self, ctx: &DriverContext, req: ReadRequest) -> Result<ReadResult, DriverError>;

    /// Returns one ordered page of objects.
    async fn list(&self, ctx: &DriverContext, req: ListRequest) -> Result<ListPage, DriverError>;
}

/// A namespace opened for writing.
#[async_trait]
pub trait WriteNamespace: Send + Sync {
    /// Starts a hidden staged write. Nothing is visible until commit.
    async fn begin_write(
        &self,
        ctx: &DriverContext,
        req: BeginWrite,
    ) -> Result<Box<dyn StagedWrite>, DriverError>;

    /// Atomically deletes a key, honouring preconditions.
    async fn delete(
        &self,
        ctx: &DriverContext,
        req: DeleteRequest,
    ) -> Result<DeleteResult, DriverError>;
}

/// One staged write, owned exclusively by the caller.
///
/// Dropping the value is not a guaranteed cleanup: callers must `abort`
/// explicitly, and drivers must register temporary resources so they can be
/// reclaimed after a crash (spec §5.1).
#[async_trait]
pub trait StagedWrite: Send {
    /// Appends one chunk. Back-pressure is applied by the caller.
    async fn write_chunk(&mut self, ctx: &DriverContext, bytes: Bytes) -> Result<(), DriverError>;

    /// Publishes the complete object and its metadata as one revision.
    async fn commit(
        self: Box<Self>,
        ctx: &DriverContext,
        attrs: CommitAttributes,
    ) -> Result<ObjectMetadata, DriverError>;

    /// Discards the staged data.
    async fn abort(self: Box<Self>, ctx: &DriverContext) -> Result<(), DriverError>;
}

/// An opened namespace plus the optional interfaces it supports.
///
/// `capabilities` and the presence of the optional handles must agree: a
/// declared capability with no handle (or vice versa) is a driver bug.
pub struct NamespaceHandle {
    /// Declared capabilities and limits for this namespace.
    pub capabilities: NamespaceCapabilities,
    /// Read interface; always present.
    pub read: Arc<dyn ReadNamespace>,
    /// Write interface; present when the namespace is writable.
    pub write: Option<Arc<dyn WriteNamespace>>,
    /// Native multipart interface; present when multipart is native.
    pub multipart: Option<Arc<dyn super::extensions::MultipartNamespace>>,
    /// Native copy interface; present when same-domain copy is native.
    pub native_copy: Option<Arc<dyn super::extensions::NativeCopy>>,
    /// Commit-outcome query interface; present when durable write ids are
    /// tracked.
    pub recovery: Option<Arc<dyn super::extensions::CommitRecovery>>,
}

impl std::fmt::Debug for NamespaceHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceHandle")
            .field("capabilities", &self.capabilities)
            .field("writable", &self.write.is_some())
            .field("multipart", &self.multipart.is_some())
            .field("native_copy", &self.native_copy.is_some())
            .field("recovery", &self.recovery.is_some())
            .finish()
    }
}

impl NamespaceHandle {
    /// Checks that the optional handles and the capability declarations agree.
    pub fn is_self_consistent(&self) -> bool {
        let caps = &self.capabilities;
        self.write.is_some() == caps.write
            && self.multipart.is_some() == caps.multipart
            && self.native_copy.is_some() == caps.native_copy
            && self.recovery.is_some() == caps.recovery
    }
}

/// A storage backend instance (one configured connection/resource set).
#[async_trait]
pub trait StorageDriver: Send + Sync {
    /// Static description of this instance.
    fn descriptor(&self) -> DriverDescriptor;

    /// Probes health without modifying user objects.
    async fn health(&self, ctx: &DriverContext) -> Result<DriverHealth, DriverError>;

    /// Stops internal work and reports any unresolved state.
    async fn shutdown(&self, ctx: &DriverContext) -> Result<(), DriverError>;

    /// Opens a namespace carrying its isolation space.
    async fn open_namespace(
        &self,
        ctx: &DriverContext,
        namespace: &NamespaceId,
    ) -> Result<NamespaceHandle, DriverError>;
}
