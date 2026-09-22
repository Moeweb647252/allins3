//! The object service: S3 parameter semantics, capability selection, the
//! condition/range/revision read flow, and the write pipeline.
//!
//! The service owns no protocol concerns: it never sees HTTP, XML or a
//! signature. It is the only layer that decides *which* driver capability to
//! use and how to fall back.

pub mod error;
pub mod multipart;

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;

use crate::auth::AccessPolicy;
use crate::catalog::{BucketBinding, BucketCatalog};
use crate::domain::{
    BeginWrite, Checksum, ChecksumAlgorithm, CommitAttributes, ContentProperties, DeleteRequest,
    DeleteResult, DriverContext, DriverError, DriverErrorKind, EntityTag, ListCursor, ListRequest,
    ObjectKey, ObjectMetadata, Preconditions, RangeResolution, RangeSpec, ResolvedRange,
    UserMetadata, WriteId,
};
use crate::domain::{ChecksumType, CommitState};
use crate::driver::{
    CommitRecovery, CopyRequest, CopySource, MetadataDirective, NamespaceHandle, ObjectByteStream,
    ReadRequest, StorageDriver, validate_profile,
};
use crate::integrity::StreamingDigest;

pub use error::ServiceError;

/// How many times the read flow re-runs `stat → conditions → range → read`
/// after a revision becomes unreadable.
const MAX_READ_ATTEMPTS: u32 = 3;

/// Server-side continuation-token lifetime.
const TOKEN_TTL: Duration = Duration::from_secs(3600);

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
pub struct DeleteItem {
    /// The key to delete.
    pub key: ObjectKey,
}

/// One result of a batch delete.
#[derive(Debug, Clone)]
pub struct DeleteItemResult {
    /// The key.
    pub key: ObjectKey,
    /// Whether it was removed.
    pub deleted: bool,
    /// The error code, when the item failed.
    pub error: Option<(String, String)>,
}

/// A continuation-token record.
struct TokenRecord {
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<char>,
    encoding: Option<String>,
    cursor: Option<String>,
    principal: Option<String>,
    generation: u64,
    expires: Instant,
}

/// Server-side continuation-token store.
///
/// Tokens are opaque random handles, so they cannot be forged or used to leak
/// backend cursors. Each token is bound to bucket, binding generation, prefix,
/// delimiter, encoding and authorization scope, and every use re-authorizes.
#[derive(Default)]
struct TokenStore {
    records: Mutex<HashMap<String, TokenRecord>>,
}

impl TokenStore {
    fn issue(&self, record: TokenRecord) -> String {
        let mut counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let id = format!(
            "tok-{:016x}-{}",
            {
                counter = counter.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
                counter
            },
            WriteId::generate()
        );
        let mut guard = self.records.lock().expect("token store poisoned");
        guard.retain(|_, r| r.expires > Instant::now());
        guard.insert(id.clone(), record);
        id
    }

    fn resolve(&self, token: &str) -> Option<ResolvedToken> {
        let mut guard = self.records.lock().expect("token store poisoned");
        let record = guard.get(token)?;
        if record.expires <= Instant::now() {
            guard.remove(token);
            return None;
        }
        Some(ResolvedToken {
            bucket: record.bucket.clone(),
            prefix: record.prefix.clone(),
            delimiter: record.delimiter,
            encoding: record.encoding.clone(),
            cursor: record.cursor.clone(),
            principal: record.principal.clone(),
            generation: record.generation,
        })
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A validated continuation token.
pub struct ResolvedToken {
    /// Bound bucket.
    pub bucket: String,
    /// Bound prefix.
    pub prefix: Option<String>,
    /// Bound delimiter.
    pub delimiter: Option<char>,
    /// Bound encoding type.
    pub encoding: Option<String>,
    /// The driver cursor to resume from.
    pub cursor: Option<String>,
    /// The principal that issued it.
    pub principal: Option<String>,
    /// The binding generation it was issued under.
    pub generation: u64,
}

struct ServiceInner {
    drivers: HashMap<String, std::sync::Arc<dyn StorageDriver>>,
    catalog: BucketCatalog,
    limits: crate::config::Limits,
    namespace_cache: Mutex<HashMap<String, std::sync::Arc<NamespaceHandle>>>,
    tokens: TokenStore,
    access: AccessPolicy,
    multipart: Mutex<multipart::MultipartState>,
}

/// The object service.
#[derive(Clone)]
pub struct ObjectService {
    inner: std::sync::Arc<ServiceInner>,
}

impl ObjectService {
    /// Creates a service over a catalog and a set of driver instances.
    pub fn new(
        catalog: BucketCatalog,
        drivers: HashMap<String, std::sync::Arc<dyn StorageDriver>>,
        limits: crate::config::Limits,
        access: AccessPolicy,
    ) -> Self {
        let staging_quota = limits.staging_quota_bytes;
        Self {
            inner: std::sync::Arc::new(ServiceInner {
                drivers,
                catalog,
                limits,
                namespace_cache: Mutex::new(HashMap::new()),
                tokens: TokenStore::default(),
                access,
                multipart: Mutex::new(multipart::MultipartState::with_quota(staging_quota)),
            }),
        }
    }

    /// The catalog.
    pub fn catalog(&self) -> &BucketCatalog {
        &self.inner.catalog
    }

    /// The limits.
    pub fn limits(&self) -> &crate::config::Limits {
        &self.inner.limits
    }

    /// Authorizes an action against the configured policy.
    pub fn authorize(
        &self,
        principal: Option<&str>,
        action: crate::auth::Action,
        bucket: &str,
        key: Option<&str>,
    ) -> bool {
        self.inner.access.authorize(principal, action, bucket, key)
    }

    /// Opens and capability-checks every configured bucket at startup.
    pub async fn warmup(&self, ctx: &DriverContext) -> Result<(), ServiceError> {
        for binding in self.inner.catalog.iter() {
            self.namespace_for(binding, ctx).await?;
        }
        Ok(())
    }

    /// Looks up a binding, mapping an absent bucket to `NoSuchBucket`.
    pub fn binding(&self, bucket: &str) -> Result<BucketBinding, ServiceError> {
        self.inner
            .catalog
            .get(bucket)
            .cloned()
            .ok_or(ServiceError::NoSuchBucket)
    }

    async fn namespace_for(
        &self,
        binding: &BucketBinding,
        ctx: &DriverContext,
    ) -> Result<std::sync::Arc<NamespaceHandle>, ServiceError> {
        let cache_key = format!("{}#{}", binding.bucket, binding.generation);
        if let Some(handle) = self
            .inner
            .namespace_cache
            .lock()
            .expect("namespace cache poisoned")
            .get(&cache_key)
            .cloned()
        {
            return Ok(handle);
        }

        let driver = self
            .inner
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

        validate_profile(binding.profile, &handle.capabilities).map_err(|e| {
            ServiceError::Internal(format!(
                "bucket `{}` capability mismatch: {e}",
                binding.bucket
            ))
        })?;

        let handle = std::sync::Arc::new(handle);
        self.inner
            .namespace_cache
            .lock()
            .expect("namespace cache poisoned")
            .insert(cache_key, handle.clone());
        Ok(handle)
    }

    fn require_writable(&self, binding: &BucketBinding) -> Result<(), ServiceError> {
        if binding.readonly {
            return Err(ServiceError::AccessDenied);
        }
        Ok(())
    }

    // --- Bucket operations -------------------------------------------------

    /// `HeadBucket`.
    pub fn head_bucket(&self, bucket: &str) -> Result<BucketBinding, ServiceError> {
        self.binding(bucket)
    }

    /// `ListBuckets`, filtered by the caller's read permission.
    pub fn list_buckets(&self, principal: Option<&str>) -> Vec<BucketBinding> {
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
        let binding = self.binding(bucket)?;
        let handle = self.namespace_for(&binding, ctx).await?;

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
        let binding = self.binding(bucket)?;
        let handle = self.namespace_for(&binding, ctx).await?;

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
        mut body: ObjectByteStream,
        params: PutParams,
    ) -> Result<ObjectMetadata, ServiceError> {
        let binding = self.binding(bucket)?;
        self.require_writable(&binding)?;
        let handle = self.namespace_for(&binding, ctx).await?;
        let write = handle
            .write
            .as_ref()
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
    pub async fn delete_object(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        preconditions: Preconditions,
    ) -> Result<DeleteResult, ServiceError> {
        let binding = self.binding(bucket)?;
        self.require_writable(&binding)?;
        let handle = self.namespace_for(&binding, ctx).await?;
        let write = handle
            .write
            .as_ref()
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
    pub async fn delete_objects(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        items: &[DeleteItem],
    ) -> Result<Vec<DeleteItemResult>, ServiceError> {
        let binding = self.binding(bucket)?;
        self.require_writable(&binding)?;
        if items.len() > 1000 {
            return Err(ServiceError::MalformedXml(
                "DeleteObjects accepts at most 1000 keys per request".into(),
            ));
        }
        let handle = self.namespace_for(&binding, ctx).await?;
        let write = handle
            .write
            .as_ref()
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
                Ok(result) => results.push(DeleteItemResult {
                    key: item.key.clone(),
                    deleted: result.deleted,
                    error: None,
                }),
                Err(e) => {
                    let service = ServiceError::from_driver(&e, true);
                    results.push(DeleteItemResult {
                        key: item.key.clone(),
                        deleted: false,
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
        let binding = self.binding(bucket)?;

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

        let handle = self.namespace_for(&binding, ctx).await?;
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
                    expires: Instant::now() + TOKEN_TTL,
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
        let binding = self.binding(destination_bucket)?;
        self.require_writable(&binding)?;

        // Stable read of the source at a checked revision.
        let source = self
            .get_object(
                ctx,
                &params.source_bucket,
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

        // Native copy is only valid for a same-namespace copy.
        if params.source_bucket == destination_bucket {
            let handle = self.namespace_for(&binding, ctx).await?;
            if let Some(native) = &handle.native_copy {
                let source_binding = self.binding(&params.source_bucket)?;
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

        self.put_object(
            ctx,
            destination_bucket,
            &params.destination_key,
            source.stream,
            put,
        )
        .await
    }

    // --- Recovery ----------------------------------------------------------

    /// Queries the commit outcome of a write attempt, when the bucket's driver
    /// supports recovery.
    pub async fn query_commit(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        write_id: &WriteId,
    ) -> Result<CommitState, ServiceError> {
        let binding = self.binding(bucket)?;
        let handle = self.namespace_for(&binding, ctx).await?;
        let recovery: &dyn CommitRecovery = handle.recovery.as_deref().ok_or_else(|| {
            ServiceError::NotImplemented("commit recovery is not supported".into())
        })?;
        recovery
            .query(ctx, write_id)
            .await
            .map_err(|e| ServiceError::from_driver(&e, true))
    }

    /// The backing driver for a bucket, used for health reporting.
    pub fn driver_for(&self, binding: &BucketBinding) -> Option<std::sync::Arc<dyn StorageDriver>> {
        self.inner.drivers.get(binding.driver.as_str()).cloned()
    }
}

async fn next_chunk(stream: &mut ObjectByteStream) -> Option<Result<Bytes, DriverError>> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ObjectRevision, RequestId};

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
    fn token_store_roundtrip_and_binding() {
        let store = TokenStore::default();
        let token = store.issue(TokenRecord {
            bucket: "b".into(),
            prefix: Some("p/".into()),
            delimiter: Some('/'),
            encoding: None,
            cursor: Some("i:5".into()),
            principal: Some("alice".into()),
            generation: 7,
            expires: Instant::now() + TOKEN_TTL,
        });
        let resolved = store.resolve(&token).expect("token resolves");
        assert_eq!(resolved.bucket, "b");
        assert_eq!(resolved.generation, 7);
        assert_eq!(resolved.cursor.as_deref(), Some("i:5"));
        assert!(store.resolve("tok-forged").is_none());
    }

    #[test]
    fn expired_token_is_rejected() {
        let store = TokenStore::default();
        let token = store.issue(TokenRecord {
            bucket: "b".into(),
            prefix: None,
            delimiter: None,
            encoding: None,
            cursor: None,
            principal: None,
            generation: 1,
            expires: Instant::now(),
        });
        assert!(store.resolve(&token).is_none());
    }

    #[test]
    fn service_error_context() {
        let _ = RequestId::new("x");
    }
}
