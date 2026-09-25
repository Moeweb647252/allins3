//! The object service: S3 parameter semantics, capability selection, the
//! condition/range/revision read flow, and the write pipeline.
//!
//! The service owns no protocol concerns: it never sees HTTP, XML or a
//! signature. It is the only layer that decides *which* driver capability to
//! use and how to fall back.

mod admission;
pub mod error;
mod handles;
pub mod multipart;
mod staging;
mod tokens;

pub(crate) use admission::OperationSlot;

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;

use crate::auth::AccessPolicy;
use crate::catalog::{BucketBinding, BucketCatalog};
use crate::domain::ChecksumType;
use crate::domain::{
    BeginWrite, Checksum, ChecksumAlgorithm, CommitAttributes, ContentProperties, DeleteRequest,
    DeleteResult, DriverContext, DriverError, DriverErrorKind, EntityTag, ListCursor, ListRequest,
    ObjectKey, ObjectMetadata, Preconditions, RangeResolution, RangeSpec, ResolvedRange,
    UserMetadata, WriteId,
};
use crate::driver::{
    CopyRequest, CopySource, MetadataDirective, ObjectByteStream, ReadRequest, StorageDriver,
};
use crate::integrity::StreamingDigest;

use admission::Admission;
pub use error::ServiceError;
use handles::{CopyRoute, NamespaceHandles};
use tokens::{TokenRecord, TokenStore};

/// How many times the read flow re-runs `stat → conditions → range → read`
/// after a revision becomes unreadable.
const MAX_READ_ATTEMPTS: u32 = 3;

/// HTTP-style read conditions.
#[derive(Debug, Clone, Default)]
pub struct ReadConditions {
    /// `If-Match` tags.
    pub if_match: Option<Vec<EntityTag>>,
    /// `If-None-Match` tags.
    pub if_none_match: Option<Vec<EntityTag>>,
    /// `If-Modified-Since`.
    pub if_modified_since: Option<SystemTime>,
    /// `If-Unmodified-Since`.
    pub if_unmodified_since: Option<SystemTime>,
}

/// The result of evaluating read conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionOutcome {
    /// Serve the object.
    Proceed,
    /// `304 Not Modified`.
    NotModified,
    /// `412 Precondition Failed`.
    PreconditionFailed,
}

impl ReadConditions {
    /// Evaluates conditions against a revision's metadata.
    ///
    /// Order follows RFC 9110: `If-Match`, `If-Unmodified-Since`,
    /// `If-None-Match`, `If-Modified-Since`; `If-None-Match` takes precedence
    /// over `If-Modified-Since`.
    pub fn evaluate(&self, meta: &ObjectMetadata) -> ConditionOutcome {
        if let Some(tags) = &self.if_match
            && !tags.iter().any(|t| Some(t) == meta.etag.as_ref())
        {
            return ConditionOutcome::PreconditionFailed;
        }
        if let Some(since) = self.if_unmodified_since
            && meta.last_modified > since
        {
            return ConditionOutcome::PreconditionFailed;
        }
        if let Some(tags) = &self.if_none_match
            && tags.iter().any(|t| Some(t) == meta.etag.as_ref())
        {
            return ConditionOutcome::NotModified;
        }
        if let Some(since) = self.if_modified_since
            && meta.last_modified <= since
        {
            return ConditionOutcome::NotModified;
        }
        ConditionOutcome::Proceed
    }

    /// Whether any condition is present.
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.if_modified_since.is_none()
            && self.if_unmodified_since.is_none()
    }
}

/// The checksum a client declared in a request trailer.
///
/// A trailer value only exists once the body has been consumed, so the service
/// asks for it after the stream ends and verifies it exactly like a checksum
/// declared in a header. The protocol library parses trailers but deliberately
/// leaves their verification to the server (spec §6.3).
#[derive(Clone)]
pub struct TrailerChecksum {
    algorithm: ChecksumAlgorithm,
    value: Arc<dyn Fn() -> Option<String> + Send + Sync>,
}

impl std::fmt::Debug for TrailerChecksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrailerChecksum")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

impl TrailerChecksum {
    /// Wraps the algorithm named by the request and a reader for its value.
    pub fn new(
        algorithm: ChecksumAlgorithm,
        value: impl Fn() -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            algorithm,
            value: Arc::new(value),
        }
    }

    /// The algorithm the request's trailer declaration named.
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    /// The decoded trailer value; `None` if the client never sent it.
    pub fn value(&self) -> Option<Vec<u8>> {
        crate::integrity::decode_base64(&(self.value)()?).ok()
    }
}

/// Parameters for a staged write.
#[derive(Debug, Clone, Default)]
pub struct PutParams {
    /// Expected decoded length, when the request declared one.
    pub expected_length: Option<u64>,
    /// `Content-MD5` (16 raw bytes), when supplied.
    pub content_md5: Option<Vec<u8>>,
    /// A checksum declared by the client (`x-amz-checksum-*`), if any.
    pub declared_checksum: Option<(ChecksumAlgorithm, Vec<u8>)>,
    /// A checksum algorithm the client asked the service to compute.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// A checksum the client said it would send in a request trailer.
    pub trailer_checksum: Option<TrailerChecksum>,
    /// HTTP content properties.
    pub content: ContentProperties,
    /// User metadata.
    pub user_metadata: UserMetadata,
    /// Atomic preconditions.
    pub preconditions: Preconditions,
}

/// A `HEAD` result.
#[derive(Debug, Clone)]
pub struct HeadResult {
    /// The object metadata.
    pub metadata: ObjectMetadata,
    /// The resolved range, if the request asked for a partial head.
    pub range: Option<ResolvedRange>,
}

/// A `GET` result.
pub struct GetResult {
    /// The object metadata.
    pub metadata: ObjectMetadata,
    /// The resolved range, if the request should be served as `206`.
    pub range: Option<ResolvedRange>,
    /// The object bytes.
    pub stream: ObjectByteStream,
}

impl std::fmt::Debug for GetResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetResult")
            .field("metadata", &self.metadata)
            .field("range", &self.range)
            .field("stream", &"<byte stream>")
            .finish()
    }
}

/// A `ListObjectsV2` result.
#[derive(Debug, Clone)]
pub struct ListV2Result {
    /// Objects in ascending key order.
    pub objects: Vec<ObjectMetadata>,
    /// Folded common prefixes.
    pub common_prefixes: Vec<String>,
    /// The opaque continuation token to send back, if truncated.
    pub next_token: Option<String>,
    /// Whether more results follow.
    pub is_truncated: bool,
    /// Whether the requested prefix itself was restricted by the policy.
    pub effective_prefix: Option<String>,
}

/// A `CopyObject` request.
#[derive(Debug, Clone)]
pub struct CopyParams {
    /// Source bucket.
    pub source_bucket: String,
    /// Source key.
    pub source_key: ObjectKey,
    /// Source read conditions.
    pub source_conditions: ReadConditions,
    /// Destination key.
    pub destination_key: ObjectKey,
    /// `COPY` or `REPLACE`.
    pub directive: MetadataDirective,
    /// Replacement content properties (with `REPLACE`).
    pub content: ContentProperties,
    /// Replacement user metadata (with `REPLACE`).
    pub user_metadata: UserMetadata,
    /// Destination preconditions.
    pub preconditions: Preconditions,
}

/// A batch delete item.
#[derive(Debug, Clone)]
pub(crate) struct DeleteItem {
    /// The key to delete.
    pub key: ObjectKey,
}

/// One result of a batch delete.
///
/// A key that was already absent is reported as deleted, so there is no
/// "removed or not" field here: S3 answers `Deleted` for a no-op delete and
/// reserves the error slot for a real failure.
#[derive(Debug, Clone)]
pub(crate) struct DeleteItemResult {
    /// The key.
    pub key: ObjectKey,
    /// The error code, when the item failed.
    pub error: Option<(String, String)>,
}

struct ServiceInner {
    handles: NamespaceHandles,
    catalog: BucketCatalog,
    limits: crate::config::Limits,
    tokens: TokenStore,
    admission: Admission,
    access: AccessPolicy,
    multipart: multipart::Multipart,
}

/// The object service.
#[derive(Clone)]
pub struct ObjectService {
    inner: std::sync::Arc<ServiceInner>,
}

impl ObjectService {
    /// Creates a service over a catalog and a set of driver instances.
    ///
    /// `production` is the deployment mode from configuration
    /// (`development = false`). It is a policy the service must apply at the
    /// moment it opens a namespace, because only the opened namespace knows
    /// whether it can back persistent writes (ADR-0006 §4).
    pub fn new(
        catalog: BucketCatalog,
        drivers: HashMap<String, std::sync::Arc<dyn StorageDriver>>,
        limits: crate::config::Limits,
        access: AccessPolicy,
        production: bool,
    ) -> Self {
        let staging_quota = limits.staging_quota_bytes;
        let token_ttl = limits.list_token_ttl;
        let admission = Admission::new(
            limits.max_concurrent_requests,
            limits.max_concurrent_requests_per_driver,
        );
        Self {
            inner: std::sync::Arc::new(ServiceInner {
                handles: NamespaceHandles::new(drivers, production),
                catalog,
                limits,
                // The token lifetime is a deployment policy owned by the token
                // store, not recomputed at each issue site (spec §4.3).
                tokens: TokenStore::new(token_ttl),
                admission,
                access,
                multipart: multipart::Multipart::new(staging_quota),
            }),
        }
    }

    /// The limits.
    pub(crate) fn limits(&self) -> &crate::config::Limits {
        &self.inner.limits
    }

    /// Authorizes an action against the configured policy.
    pub(crate) fn authorize(
        &self,
        principal: Option<&str>,
        action: crate::auth::Action,
        bucket: &str,
        key: Option<&str>,
    ) -> bool {
        self.inner.access.authorize(principal, action, bucket, key)
    }

    /// Opens and capability-checks every configured bucket at startup.
    pub(crate) async fn warmup(&self, ctx: &DriverContext) -> Result<(), ServiceError> {
        for binding in self.inner.catalog.iter() {
            self.inner.handles.get(binding, ctx).await?;
        }
        Ok(())
    }

    /// Looks up a binding, mapping an absent bucket to `NoSuchBucket`.
    pub(crate) fn binding(&self, bucket: &str) -> Result<BucketBinding, ServiceError> {
        self.inner
            .catalog
            .get(bucket)
            .cloned()
            .ok_or(ServiceError::NoSuchBucket)
    }

    /// Resolves a binding together with the slots the operation needs
    /// (spec §9.3): one in the server-wide budget, one in its driver's.
    ///
    /// Every operation that reaches a driver, or that consumes a request body
    /// (`PutObject`, `UploadPart`, `CompleteMultipartUpload`), resolves its
    /// binding through this call, so the server never holds more than
    /// `max_concurrent_requests` operations at once and one driver instance
    /// never serves more than `max_concurrent_requests_per_driver` of them,
    /// however many buckets it backs. Saturation is answered with `SlowDown`
    /// rather than queued, keeping each queue bounded at zero, and a graceful
    /// drain waits for every slot to come back rather than exiting under an
    /// operation. A multi-step operation (`CopyObject`, `UploadPartCopy`)
    /// admits once here at its own entry and performs its steps through the
    /// non-admitting `*_in` helpers, so no operation holds two shares of the
    /// same scope: a second claim on one driver instance would be refused by
    /// the operation's own limit — at a limit of one, forever. Operations that
    /// only consult the catalog (`HeadBucket`, `ListBuckets`, creating, listing
    /// or aborting an upload) use [`ObjectService::binding`] and take no slot.
    pub(crate) fn binding_slot(
        &self,
        bucket: &str,
    ) -> Result<(BucketBinding, OperationSlot), ServiceError> {
        let binding = self.binding(bucket)?;
        let slot = self.operation_slot(&binding)?;
        Ok((binding, slot))
    }

    /// Takes an operation's slots for an already resolved binding.
    pub(crate) fn operation_slot(
        &self,
        binding: &BucketBinding,
    ) -> Result<OperationSlot, ServiceError> {
        self.inner.admission.slot(binding)
    }

    /// Waits until no operation is in flight (used by graceful shutdown).
    pub async fn drain(&self) {
        self.inner.admission.drain().await;
    }

    fn require_writable(&self, binding: &BucketBinding) -> Result<(), ServiceError> {
        if binding.readonly {
            return Err(ServiceError::AccessDenied);
        }
        Ok(())
    }

    // --- Bucket operations -------------------------------------------------

    /// `HeadBucket`.
    pub(crate) fn head_bucket(&self, bucket: &str) -> Result<BucketBinding, ServiceError> {
        self.binding(bucket)
    }

    /// `ListBuckets`, filtered by the caller's read permission.
    pub(crate) fn list_buckets(&self, principal: Option<&str>) -> Vec<BucketBinding> {
        self.inner
            .catalog
            .iter()
            .filter(|b| {
                self.inner.access.authorize(
                    principal,
                    crate::auth::Action::List,
                    b.bucket.as_str(),
                    None,
                ) || self.inner.access.authorize(
                    principal,
                    crate::auth::Action::Read,
                    b.bucket.as_str(),
                    None,
                )
            })
            .cloned()
            .collect()
    }

    // --- Read path ---------------------------------------------------------

    /// `HeadObject`.
    pub async fn head_object(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        conditions: &ReadConditions,
        range: Option<RangeSpec>,
    ) -> Result<HeadResult, ServiceError> {
        let (binding, _slot) = self.binding_slot(bucket)?;
        let handle = self.inner.handles.get(&binding, ctx).await?;

        for _ in 0..MAX_READ_ATTEMPTS {
            let meta = match handle.read.stat(ctx, key).await {
                Ok(meta) => meta,
                Err(e) if e.kind() == DriverErrorKind::RevisionUnavailable => continue,
                Err(e) => return Err(ServiceError::from_driver(&e, true)),
            };
            match conditions.evaluate(&meta) {
                ConditionOutcome::PreconditionFailed => {
                    return Err(ServiceError::PreconditionFailed);
                }
                ConditionOutcome::NotModified => return Err(ServiceError::NotModified),
                ConditionOutcome::Proceed => {}
            }
            // Conditions passed: only now consider the range.
            let resolved = match range {
                None => None,
                Some(spec) => match spec.resolve(meta.size) {
                    RangeResolution::Full => None,
                    RangeResolution::Partial(r) => Some(r),
                    RangeResolution::Unsatisfiable => return Err(ServiceError::InvalidRange),
                },
            };
            return Ok(HeadResult {
                metadata: meta,
                range: resolved,
            });
        }
        Err(ServiceError::ServiceUnavailable)
    }

    /// `GetObject`.
    pub async fn get_object(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        conditions: &ReadConditions,
        range: Option<RangeSpec>,
    ) -> Result<GetResult, ServiceError> {
        let (binding, _slot) = self.binding_slot(bucket)?;
        self.get_object_in(&binding, ctx, key, conditions, range)
            .await
    }

    /// `GetObject` against an already resolved binding, without admitting.
    ///
    /// The internal steps of a multi-step operation (a copy) use this: the
    /// operation holds one slot for its whole span, and a second claim on the
    /// same scope would stall it at a limit of one.
    async fn get_object_in(
        &self,
        binding: &BucketBinding,
        ctx: &DriverContext,
        key: &ObjectKey,
        conditions: &ReadConditions,
        range: Option<RangeSpec>,
    ) -> Result<GetResult, ServiceError> {
        let handle = self.inner.handles.get(binding, ctx).await?;

        for _ in 0..MAX_READ_ATTEMPTS {
            let meta = match handle.read.stat(ctx, key).await {
                Ok(meta) => meta,
                Err(e) if e.kind() == DriverErrorKind::RevisionUnavailable => continue,
                Err(e) => return Err(ServiceError::from_driver(&e, true)),
            };
            match conditions.evaluate(&meta) {
                ConditionOutcome::PreconditionFailed => {
                    return Err(ServiceError::PreconditionFailed);
                }
                ConditionOutcome::NotModified => return Err(ServiceError::NotModified),
                ConditionOutcome::Proceed => {}
            }
            let resolved = match range {
                None => None,
                Some(spec) => match spec.resolve(meta.size) {
                    RangeResolution::Full => None,
                    RangeResolution::Partial(r) => Some(r),
                    RangeResolution::Unsatisfiable => return Err(ServiceError::InvalidRange),
                },
            };

            let expected_revision = meta.revision.clone();
            match handle
                .read
                .read(
                    ctx,
                    ReadRequest {
                        key: key.clone(),
                        expected_revision: expected_revision.clone(),
                        range: resolved,
                    },
                )
                .await
            {
                Ok(read) => {
                    return Ok(GetResult {
                        metadata: read.metadata,
                        range: resolved,
                        stream: read.stream,
                    });
                }
                Err(e) if e.kind() == DriverErrorKind::RevisionUnavailable => {
                    // The object changed after `stat`; re-run the whole flow.
                    continue;
                }
                Err(e) => return Err(ServiceError::from_driver(&e, true)),
            }
        }
        Err(ServiceError::ServiceUnavailable)
    }

    // --- Write path --------------------------------------------------------

    /// `PutObject`. Streams the body into a staged write and commits.
    pub async fn put_object(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        body: ObjectByteStream,
        params: PutParams,
    ) -> Result<ObjectMetadata, ServiceError> {
        let (binding, _slot) = self.binding_slot(bucket)?;
        self.put_object_in(&binding, ctx, key, body, params).await
    }

    /// `PutObject` against an already resolved binding, without admitting.
    ///
    /// See [`ObjectService::get_object_in`]: the admitting happens once, at the
    /// entry point of the operation that owns the whole span.
    async fn put_object_in(
        &self,
        binding: &BucketBinding,
        ctx: &DriverContext,
        key: &ObjectKey,
        mut body: ObjectByteStream,
        params: PutParams,
    ) -> Result<ObjectMetadata, ServiceError> {
        self.require_writable(binding)?;
        let handle = self.inner.handles.get(binding, ctx).await?;
        let write = handle
            .writer()
            .ok_or_else(|| ServiceError::NotImplemented("bucket is not writable".into()))?;

        let max_single = self.inner.limits.max_single_write_size;
        let max_object = self.inner.limits.max_object_size.unwrap_or(u64::MAX);
        let effective_max = max_single.min(max_object);

        let mut staged = write
            .begin_write(
                ctx,
                BeginWrite {
                    key: key.clone(),
                    expected_length: params.expected_length,
                    preconditions: params.preconditions.clone(),
                    write_id: WriteId::generate(),
                    content: params.content.clone(),
                    user_metadata: params.user_metadata.clone(),
                    checksum_algorithm: params.checksum_algorithm,
                },
            )
            .await
            .map_err(|e| ServiceError::from_driver(&e, true))?; // Stream the checksum the client declared even when it did not set
        // `x-amz-sdk-checksum-algorithm`, so a declared value can be verified
        // without buffering the whole object.
        let stream_algorithm = params
            .checksum_algorithm
            .or_else(|| params.declared_checksum.as_ref().map(|(alg, _)| *alg))
            .or_else(|| params.trailer_checksum.as_ref().map(|t| t.algorithm()));
        let mut md5 = StreamingDigest::new(ChecksumAlgorithm::Md5);
        let mut extra = stream_algorithm.map(StreamingDigest::new);
        let mut length: u64 = 0;

        loop {
            // A cancelled or timed-out request must stop consuming the body and
            // must never publish what it received so far.
            if ctx.is_done() {
                let _ = staged.abort(ctx).await;
                return Err(ServiceError::InvalidRequest(
                    "the request was cancelled before the object was fully received".into(),
                ));
            }
            let chunk = match next_chunk(&mut body).await {
                None => break,
                Some(Ok(bytes)) => bytes,
                Some(Err(e)) => {
                    let _ = staged.abort(ctx).await;
                    return Err(ServiceError::from_driver(&e, true));
                }
            };
            if chunk.is_empty() {
                continue;
            }
            length += chunk.len() as u64;
            if length > effective_max {
                let _ = staged.abort(ctx).await;
                return Err(ServiceError::EntityTooLarge(format!(
                    "the object exceeds the configured maximum size of {effective_max} bytes"
                )));
            }
            if let Some(expected) = params.expected_length
                && length > expected
            {
                let _ = staged.abort(ctx).await;
                return Err(ServiceError::BadDigest(
                    "the object exceeded the declared content length".into(),
                ));
            }
            md5.update(&chunk);
            if let Some(hasher) = extra.as_mut() {
                hasher.update(&chunk);
            }
            if let Err(e) = staged.write_chunk(ctx, chunk).await {
                let _ = staged.abort(ctx).await;
                return Err(ServiceError::from_driver(&e, true));
            }
        }

        if let Some(expected) = params.expected_length
            && length != expected
        {
            let _ = staged.abort(ctx).await;
            return Err(ServiceError::BadDigest(
                "the object length did not match the declared content length".into(),
            ));
        }

        let md5_digest = md5.finalize();

        // `Content-MD5` verification.
        if let Some(expected) = &params.content_md5
            && expected.as_slice() != md5_digest.as_ref()
        {
            let _ = staged.abort(ctx).await;
            return Err(ServiceError::BadDigest(
                "the Content-MD5 you specified did not match what we received".into(),
            ));
        }

        // A checksum declared in a request trailer is only readable now that the
        // body has been consumed. A declaration whose value never arrives is a
        // mismatch too: the algorithm and the actual value must agree (spec
        // §6.3), and nothing is published when they do not.
        let trailer_declared = match &params.trailer_checksum {
            None => None,
            Some(trailer) => match trailer.value() {
                Some(value) => Some((trailer.algorithm(), value)),
                None => {
                    let _ = staged.abort(ctx).await;
                    return Err(ServiceError::BadDigest(
                        "the checksum you declared in a trailer was not sent".into(),
                    ));
                }
            },
        };
        let declared_checksum = params.declared_checksum.clone().or(trailer_declared);

        // Client-declared checksum verification.
        let mut checksums = Vec::new();
        if let Some((algorithm, expected)) = &declared_checksum {
            let actual = if *algorithm == ChecksumAlgorithm::Md5 {
                md5_digest.clone()
            } else if Some(*algorithm) == stream_algorithm {
                extra
                    .take()
                    .map(StreamingDigest::finalize)
                    .unwrap_or_else(|| crate::integrity::compute_checksum(*algorithm, &[]))
            } else {
                // A declared checksum we did not stream: recompute is impossible
                // without buffering, so reject rather than accept blindly.
                let _ = staged.abort(ctx).await;
                return Err(ServiceError::NotImplemented(format!(
                    "checksum algorithm `{}` is not supported for this request",
                    algorithm.as_str()
                )));
            };
            if actual.as_ref() != expected.as_slice() {
                let _ = staged.abort(ctx).await;
                return Err(ServiceError::BadDigest(
                    "the checksum you specified did not match what we received".into(),
                ));
            }
            if let Ok(c) = Checksum::new(*algorithm, ChecksumType::FullObject, actual) {
                checksums.push(c);
            }
        } else if let (Some(algorithm), Some(hasher)) = (params.checksum_algorithm, extra) {
            let actual = hasher.finalize();
            if let Ok(c) = Checksum::new(algorithm, ChecksumType::FullObject, actual) {
                checksums.push(c);
            }
        }

        let resolved_etag =
            EntityTag::new(crate::integrity::hex(&md5_digest)).expect("md5 hex is a valid tag");

        match staged
            .commit(
                ctx,
                CommitAttributes {
                    length,
                    etag: Some(resolved_etag),
                    checksums,
                },
            )
            .await
        {
            Ok(meta) => Ok(meta),
            Err(e) => Err(ServiceError::from_driver(&e, true)),
        }
    }

    /// `DeleteObject`.
    pub(crate) async fn delete_object(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        preconditions: Preconditions,
    ) -> Result<DeleteResult, ServiceError> {
        let (binding, _slot) = self.binding_slot(bucket)?;
        self.require_writable(&binding)?;
        let handle = self.inner.handles.get(&binding, ctx).await?;
        let write = handle
            .writer()
            .ok_or_else(|| ServiceError::NotImplemented("bucket is not writable".into()))?;
        write
            .delete(
                ctx,
                DeleteRequest {
                    key: key.clone(),
                    preconditions,
                },
            )
            .await
            .map_err(|e| ServiceError::from_driver(&e, true))
    }

    /// `DeleteObjects`.
    ///
    /// Results are per item; the whole batch is never reported as a success if
    /// any item failed.
    pub(crate) async fn delete_objects(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        items: &[DeleteItem],
    ) -> Result<Vec<DeleteItemResult>, ServiceError> {
        let (binding, _slot) = self.binding_slot(bucket)?;
        self.require_writable(&binding)?;
        if items.len() > 1000 {
            return Err(ServiceError::MalformedXml(
                "DeleteObjects accepts at most 1000 keys per request".into(),
            ));
        }
        let handle = self.inner.handles.get(&binding, ctx).await?;
        let write = handle
            .writer()
            .ok_or_else(|| ServiceError::NotImplemented("bucket is not writable".into()))?;

        let mut results = Vec::with_capacity(items.len());
        for item in items {
            match write
                .delete(
                    ctx,
                    DeleteRequest {
                        key: item.key.clone(),
                        preconditions: Preconditions::none(),
                    },
                )
                .await
            {
                // A delete that removed nothing is still a success: S3 answers
                // `Deleted` for a key that was already absent.
                Ok(_) => results.push(DeleteItemResult {
                    key: item.key.clone(),
                    error: None,
                }),
                Err(e) => {
                    let service = ServiceError::from_driver(&e, true);
                    results.push(DeleteItemResult {
                        key: item.key.clone(),
                        error: Some((service.code().to_string(), service.message())),
                    });
                }
            }
        }
        Ok(results)
    }

    // --- Listing -----------------------------------------------------------

    /// `ListObjectsV2`.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_objects_v2(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        principal: Option<&str>,
        prefix: Option<String>,
        delimiter: Option<char>,
        start_after: Option<ObjectKey>,
        continuation_token: Option<String>,
        encoding: Option<String>,
        max_keys: Option<u32>,
    ) -> Result<ListV2Result, ServiceError> {
        let (binding, _slot) = self.binding_slot(bucket)?;

        // Constrain the requested prefix by the principal's list scope.
        let policy_prefix = self
            .inner
            .access
            .list_prefix(principal, bucket)
            .unwrap_or_default();
        let requested = prefix.unwrap_or_default();
        let effective_prefix = if policy_prefix.is_empty() || requested.starts_with(&policy_prefix)
        {
            requested.clone()
        } else if policy_prefix.starts_with(&requested) {
            policy_prefix.clone()
        } else {
            return Ok(ListV2Result {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                next_token: None,
                is_truncated: false,
                effective_prefix: Some(policy_prefix),
            });
        };

        // Resolve a continuation token, if given.
        let (cursor, token_prefix, token_delimiter, token_encoding) = match &continuation_token {
            None => (None, effective_prefix.clone(), delimiter, encoding),
            Some(token) => {
                let record = self.inner.tokens.resolve(token).ok_or_else(|| {
                    ServiceError::InvalidArgument("invalid continuation token".into())
                })?;
                if record.bucket != bucket
                    || record.generation != binding.generation
                    || record.principal.as_deref() != principal
                    || record.prefix.as_deref() != Some(effective_prefix.as_str())
                    || record.delimiter != delimiter
                    || record.encoding != encoding
                {
                    return Err(ServiceError::InvalidArgument(
                        "continuation token does not match this request".into(),
                    ));
                }
                (record.cursor, effective_prefix.clone(), delimiter, encoding)
            }
        };

        let page_limit = max_keys
            .unwrap_or(self.inner.limits.max_list_keys)
            .clamp(1, self.inner.limits.max_list_keys);

        let handle = self.inner.handles.get(&binding, ctx).await?;
        let mut request =
            ListRequest::new(NonZeroU32::new(page_limit).expect("page limit is non-zero"))
                .with_prefix(token_prefix.clone())
                .map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;
        if let Some(delimiter) = token_delimiter {
            request = request.with_delimiter(delimiter);
        }
        if let Some(cursor) = cursor {
            request = request
                .with_cursor(ListCursor::new(cursor))
                .map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;
        } else if let Some(start_after) = start_after {
            request = request
                .with_start_after(start_after)
                .map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;
        }

        let page = handle
            .read
            .list(ctx, request)
            .await
            .map_err(|e| ServiceError::from_driver(&e, false))?;

        let next_token = if page.is_truncated {
            page.next_cursor.as_ref().map(|cursor| {
                self.inner.tokens.issue(TokenRecord {
                    bucket: bucket.to_string(),
                    prefix: Some(token_prefix.clone()),
                    delimiter: token_delimiter,
                    encoding: token_encoding,
                    cursor: Some(cursor.as_str().to_string()),
                    principal: principal.map(str::to_string),
                    generation: binding.generation,
                })
            })
        } else {
            None
        };

        Ok(ListV2Result {
            objects: page.objects,
            common_prefixes: page.common_prefixes,
            next_token,
            is_truncated: page.is_truncated,
            effective_prefix: Some(effective_prefix),
        })
    }

    // --- Copy --------------------------------------------------------------

    /// `CopyObject`.
    ///
    /// A copy inside one namespace is published either natively (when the driver
    /// supports it) or by streaming the checked source revision into a staged
    /// write. Both routes produce the same observable object; which one ran is
    /// decided here, so no caller can silently pick the weaker one.
    pub async fn copy_object(
        &self,
        ctx: &DriverContext,
        destination_bucket: &str,
        params: CopyParams,
    ) -> Result<ObjectMetadata, ServiceError> {
        // One slot admits the whole copy. Its source read and its destination
        // write must not each claim one of their own: a copy that reads and
        // writes through one driver holding two shares of it would be rejected
        // by its own limit — at a limit of one, forever.
        let (binding, _slot) = self.binding_slot(destination_bucket)?;
        self.require_writable(&binding)?;

        // Stable read of the source at a checked revision.
        let source_binding = self.binding(&params.source_bucket)?;
        let source = self
            .get_object_in(
                &source_binding,
                ctx,
                &params.source_key,
                &params.source_conditions,
                None,
            )
            .await?;

        let (content, user_metadata) = match params.directive {
            MetadataDirective::Copy => (
                source.metadata.content.clone(),
                source.metadata.user_metadata.clone(),
            ),
            MetadataDirective::Replace => (params.content.clone(), params.user_metadata.clone()),
        };

        // Native copy is only valid for a same-namespace copy, and only when
        // the plan for this namespace selected the native route: the decision
        // is read from the plan rather than re-derived from the handle.
        if params.source_bucket == destination_bucket {
            let namespace = self.inner.handles.get(&binding, ctx).await?;
            if let CopyRoute::Native(native) = namespace.copy_route() {
                let request = CopyRequest {
                    source: CopySource {
                        namespace: source_binding.namespace.clone(),
                        key: params.source_key.clone(),
                        revision: source.metadata.revision.clone(),
                    },
                    destination: params.destination_key.clone(),
                    preconditions: params.preconditions.clone(),
                    metadata_directive: params.directive,
                    content: content.clone(),
                    user_metadata: user_metadata.clone(),
                    write_id: WriteId::generate(),
                };
                match native.copy(ctx, request).await {
                    Ok(meta) => return Ok(meta),
                    // The source moved on, so fall back to the already checked
                    // revision rather than re-reading a different one.
                    Err(e) if e.kind() == DriverErrorKind::RevisionUnavailable => {}
                    Err(e) => return Err(ServiceError::from_driver(&e, true)),
                }
            }
        }

        let put = PutParams {
            expected_length: Some(source.metadata.size),
            content_md5: None,
            declared_checksum: None,
            checksum_algorithm: source.metadata.checksums.first().map(|c| c.algorithm()),
            trailer_checksum: None,
            content,
            user_metadata,
            preconditions: params.preconditions.clone(),
        };

        self.put_object_in(&binding, ctx, &params.destination_key, source.stream, put)
            .await
    }

    // --- Recovery ----------------------------------------------------------
}

async fn next_chunk(stream: &mut ObjectByteStream) -> Option<Result<Bytes, DriverError>> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ObjectRevision, RequestId};
    use std::time::Duration;

    fn meta(etag: &str, last_modified_secs: u64) -> ObjectMetadata {
        ObjectMetadata {
            key: ObjectKey::parse("k").unwrap(),
            size: 1,
            etag: Some(EntityTag::new(etag).unwrap()),
            last_modified: SystemTime::UNIX_EPOCH + Duration::from_secs(last_modified_secs),
            content: ContentProperties::default(),
            user_metadata: UserMetadata::new(),
            checksums: Vec::new(),
            revision: ObjectRevision::new("r1"),
        }
    }

    #[test]
    fn if_match_mismatch_is_412() {
        let cond = ReadConditions {
            if_match: Some(vec![EntityTag::new("other").unwrap()]),
            ..Default::default()
        };
        assert_eq!(
            cond.evaluate(&meta("abc", 100)),
            ConditionOutcome::PreconditionFailed
        );
    }

    #[test]
    fn if_none_match_hit_is_304() {
        let cond = ReadConditions {
            if_none_match: Some(vec![EntityTag::new("abc").unwrap()]),
            ..Default::default()
        };
        assert_eq!(
            cond.evaluate(&meta("abc", 100)),
            ConditionOutcome::NotModified
        );
    }

    #[test]
    fn if_match_wins_over_if_none_match() {
        let cond = ReadConditions {
            if_match: Some(vec![EntityTag::new("nope").unwrap()]),
            if_none_match: Some(vec![EntityTag::new("abc").unwrap()]),
            ..Default::default()
        };
        assert_eq!(
            cond.evaluate(&meta("abc", 100)),
            ConditionOutcome::PreconditionFailed
        );
    }

    #[test]
    fn if_modified_since() {
        let cond = ReadConditions {
            if_modified_since: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(200)),
            ..Default::default()
        };
        assert_eq!(
            cond.evaluate(&meta("abc", 100)),
            ConditionOutcome::NotModified
        );
    }

    #[test]
    fn if_unmodified_since() {
        let cond = ReadConditions {
            if_unmodified_since: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(50)),
            ..Default::default()
        };
        assert_eq!(
            cond.evaluate(&meta("abc", 100)),
            ConditionOutcome::PreconditionFailed
        );
    }

    #[test]
    fn service_error_context() {
        let _ = RequestId::new("x");
    }
}
