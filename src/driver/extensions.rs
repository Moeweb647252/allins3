//! Optional driver extension interfaces.
//!
//! These are the interfaces a driver exposes beyond core read/write. Each one
//! uses domain types only and never becomes an unconstrained SDK pass-through:
//! inputs and outputs are validated domain values, not raw backend objects or
//! arbitrary URLs.

use std::num::NonZeroU32;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::domain::{
    Checksum, ChecksumAlgorithm, ChecksumType, CommitState, ContentProperties, DriverContext,
    DriverError, EntityTag, NamespaceId, ObjectKey, ObjectMetadata, ObjectRevision, PartNumber,
    Preconditions, UserMetadata, WriteId,
};

use super::traits::StagedWrite;

// --- Native copy -----------------------------------------------------------

/// A registry-resolved source reference: a namespace, a key and a fixed
/// revision. Arbitrary URLs are never accepted.
#[derive(Debug, Clone)]
pub struct CopySource {
    /// The source namespace, which must belong to this driver domain.
    pub namespace: NamespaceId,
    /// The source key.
    pub key: ObjectKey,
    /// The exact source revision to copy, already condition-checked.
    pub revision: ObjectRevision,
}

/// How source metadata is handled on copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataDirective {
    /// Keep the source metadata.
    Copy,
    /// Use the metadata supplied in the request.
    Replace,
}

/// A native copy request.
#[derive(Debug, Clone)]
pub struct CopyRequest {
    /// The resolved source.
    pub source: CopySource,
    /// The destination key.
    pub destination: ObjectKey,
    /// Atomic preconditions on the destination.
    pub preconditions: Preconditions,
    /// Whether to copy or replace metadata.
    pub metadata_directive: MetadataDirective,
    /// Replacement content properties, used with [`MetadataDirective::Replace`].
    pub content: ContentProperties,
    /// Replacement user metadata, used with [`MetadataDirective::Replace`].
    pub user_metadata: UserMetadata,
    /// The attempt identifier, for recovery.
    pub write_id: WriteId,
}

/// Same-domain native copy. The result has atomic publish semantics.
#[async_trait]
pub trait NativeCopy: Send + Sync {
    /// Copies within a driver's native copy domain.
    async fn copy(
        &self,
        ctx: &DriverContext,
        req: CopyRequest,
    ) -> Result<ObjectMetadata, DriverError>;
}

// --- Commit recovery -------------------------------------------------------

/// Observes the outcome of an earlier write attempt.
///
/// `query` must never cancel or declare an attempt failed merely because no
/// record was found. Returning [`CommitState::NotCommittedFinal`] requires the
/// driver to prove the original attempt and all its in-flight calls lost the
/// ability to publish.
#[async_trait]
pub trait CommitRecovery: Send + Sync {
    /// Queries the persisted state of a write attempt.
    async fn query(
        &self,
        ctx: &DriverContext,
        write_id: &WriteId,
    ) -> Result<CommitState, DriverError>;
}

// --- Multipart -------------------------------------------------------------

/// An unpredictable multipart upload identifier.
///
/// It must not be an executable backend path, and every operation must check
/// its namespace binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UploadId(String);

impl UploadId {
    /// Wraps an existing upload id (for recovery of persisted sessions).
    pub fn from_raw(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrows the id.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Generates a new unpredictable upload id.
    pub fn generate() -> Self {
        Self(WriteId::generate().to_string())
    }
}

/// An opaque handle bound to a namespace and target key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadHandle {
    /// The upload identifier.
    pub upload_id: UploadId,
    /// The target key.
    pub key: ObjectKey,
}

/// Parameters for starting a native multipart upload.
#[derive(Debug, Clone)]
pub struct CreateUploadRequest {
    /// The target key. The existing target must not be modified.
    pub key: ObjectKey,
    /// Content properties for the finished object.
    pub content: ContentProperties,
    /// User metadata for the finished object.
    pub user_metadata: UserMetadata,
    /// The checksum the upload runs under, when it was created with one.
    ///
    /// The scheme is fixed for the upload's lifetime, so the driver must digest
    /// every part under it (and report those digests from `list_parts`) and
    /// publish the finished object with the same algorithm and coverage.
    pub checksum: Option<MultipartChecksumScheme>,
}

/// One part of a multipart upload.
#[derive(Debug, Clone)]
pub struct PartInfo {
    /// The part number.
    pub part_number: PartNumber,
    /// The part size in bytes.
    pub size: u64,
    /// The part ETag, when known.
    pub etag: Option<EntityTag>,
    /// The part checksum, when stored.
    pub checksum: Option<Checksum>,
    /// The part's last modification time.
    pub last_modified: SystemTime,
}

/// A client-specified part entry in a completion manifest.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    /// The part number as sent by the client.
    pub part_number: PartNumber,
    /// The client-supplied ETag to verify against the stored part.
    pub etag: Option<EntityTag>,
}

/// One page of parts.
#[derive(Debug, Clone)]
pub struct PartPage {
    /// Parts in ascending part-number order.
    pub parts: Vec<PartInfo>,
    /// The next part-number marker, absent when complete.
    pub next_marker: Option<PartNumber>,
    /// Whether more parts follow.
    pub is_truncated: bool,
}

/// Summary of an in-progress multipart upload.
#[derive(Debug, Clone)]
pub struct MultipartUpload {
    /// The upload identifier.
    pub upload_id: UploadId,
    /// The target key.
    pub key: ObjectKey,
    /// When the upload was initiated.
    pub initiated: SystemTime,
}

/// The checksum a multipart completion must compute and publish.
///
/// The service fixes the algorithm and the coverage when the upload is created
/// (or when a completion request names one for an upload created without a
/// scheme); the driver has the part bytes, so it computes the digest and
/// returns it in the published metadata. The service validates the returned
/// value against anything the client declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartChecksumScheme {
    /// The digest algorithm every part and the finished object use.
    pub algorithm: ChecksumAlgorithm,
    /// Whether the digest covers the assembled object or the ordered part
    /// digests.
    pub checksum_type: ChecksumType,
}

/// One page of in-progress uploads.
#[derive(Debug, Clone)]
pub struct UploadPage {
    /// Uploads in S3 marker order.
    pub uploads: Vec<MultipartUpload>,
    /// The next key marker.
    pub next_key_marker: Option<ObjectKey>,
    /// The next upload-id marker.
    pub next_upload_id_marker: Option<UploadId>,
    /// Whether more uploads follow.
    pub is_truncated: bool,
}

/// Native multipart support for one namespace.
#[async_trait]
pub trait MultipartNamespace: Send + Sync {
    /// Starts an upload without modifying any existing target object.
    async fn create_upload(
        &self,
        ctx: &DriverContext,
        req: CreateUploadRequest,
    ) -> Result<UploadHandle, DriverError>;

    /// Starts one part. The staged part follows the same
    /// write/commit/abort contract as a staged write; a failed replacement must
    /// leave the previous valid part in place.
    async fn begin_part(
        &self,
        ctx: &DriverContext,
        upload: &UploadHandle,
        part_number: PartNumber,
        expected_length: Option<u64>,
    ) -> Result<Box<dyn StagedWrite>, DriverError>;

    /// Lists valid parts for this namespace's upload.
    async fn list_parts(
        &self,
        ctx: &DriverContext,
        upload: &UploadHandle,
        marker: Option<PartNumber>,
        limit: NonZeroU32,
    ) -> Result<PartPage, DriverError>;

    /// Lists in-progress uploads using S3 marker semantics.
    async fn list_uploads(
        &self,
        ctx: &DriverContext,
        namespace: &NamespaceId,
        key_marker: Option<ObjectKey>,
        upload_id_marker: Option<UploadId>,
        limit: NonZeroU32,
    ) -> Result<UploadPage, DriverError>;

    /// Atomically completes an upload from an exact manifest. Nothing is
    /// published before final validation.
    ///
    /// `parts` is the client's fixed manifest. `scheme` is the checksum the
    /// finished object must be published under, when the upload has one; the
    /// driver computes that digest from the parts it holds and returns it in the
    /// metadata. `expected` are object-level checksums the client declared: the
    /// driver must verify the assembled object against them and fail *without
    /// publishing* when one does not match, so a rejected completion never
    /// consumes the upload. The published ETag is the driver's; a driver
    /// completing a multipart upload publishes the multipart `md5(parts)-<count>`
    /// form.
    async fn complete(
        &self,
        ctx: &DriverContext,
        upload: &UploadHandle,
        parts: Vec<CompletedPart>,
        scheme: Option<MultipartChecksumScheme>,
        expected: Vec<Checksum>,
        preconditions: Preconditions,
    ) -> Result<ObjectMetadata, DriverError>;

    /// Aborts an upload, arbitrating with in-flight parts and completion.
    async fn abort(&self, ctx: &DriverContext, upload: &UploadHandle) -> Result<(), DriverError>;
}
