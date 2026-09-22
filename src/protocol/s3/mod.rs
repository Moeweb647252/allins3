//! The S3 protocol adapter, built on the `s3s` candidate library.
//!
//! Responsibilities: routing (via `s3s`), XML, HTTP headers, the signature's
//! raw input and S3 error encoding. It must not contain backend path rules or
//! storage-product branches (`ADR-0007`).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use hyper::header::HeaderValue;
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::dto::{
    AbortMultipartUploadOutput, Bucket, BucketLocationConstraint, CommonPrefix,
    CompleteMultipartUploadOutput, CopyObjectOutput, CopyObjectResult, CopyPartResult,
    CreateMultipartUploadOutput, DeleteObjectOutput, DeleteObjectsOutput, DeletedObject,
    Error as S3DtoError, GetBucketLocationOutput, GetObjectOutput, HeadBucketOutput,
    HeadObjectOutput, ListBucketsOutput, ListMultipartUploadsOutput, ListObjectsV2Output,
    ListPartsOutput, Metadata as S3Metadata, MultipartUpload as S3MultipartUpload,
    Object as S3Object, Owner, Part as S3Part, PutObjectOutput, StreamingBlob, Timestamp,
    UploadPartCopyOutput, UploadPartOutput,
};
use s3s::stream::{ByteStream, RemainingLength};
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, StdError};

use crate::auth::{AccessPolicy, Action, CredentialStore};
use crate::domain::{
    ContentProperties, DriverContext, DriverError, EntityTag, Preconditions, RangeSpec, RequestId,
    UserMetadata,
};
use crate::driver::{MetadataDirective, ObjectByteStream};
use crate::integrity::base64;
use crate::service::{CopyParams, ObjectService, ReadConditions, ServiceError};

/// The S3 service implementation.
#[derive(Clone)]
pub struct S3Handler {
    service: ObjectService,
    credentials: Arc<CredentialStore>,
    limiter: PrincipalLimiter,
}

impl S3Handler {
    /// Creates a handler over the object service.
    pub fn new(
        service: ObjectService,
        credentials: Arc<CredentialStore>,
        limiter: PrincipalLimiter,
    ) -> Self {
        Self {
            service,
            credentials,
            limiter,
        }
    }

    /// Resolves the authenticated principal from the request credentials.
    fn principal_of<T>(&self, req: &S3Request<T>) -> Option<String> {
        req.credentials
            .as_ref()
            .and_then(|c| self.credentials.get(&c.access_key))
            .map(|c| c.principal.clone())
    }

    /// Builds the request scope and admits the request under the per-principal
    /// concurrency limit. One principal must not be able to occupy every
    /// connection of the server.
    fn scope<T>(&self, req: &S3Request<T>) -> S3Result<RequestScope> {
        let mut scope = scope();
        scope.principal = self.principal_of(req);
        scope.permit = self
            .limiter
            .acquire(scope.principal.as_deref())
            .map_err(|e| map_error(e, &scope.request_id))?;
        Ok(scope)
    }
}

/// Per-principal concurrency admission.
///
/// The global limit (a listener-level semaphore) bounds total work; this bounds
/// how much of it one principal may hold at once. A saturated principal is
/// rejected with `503 SlowDown` instead of queueing behind it.
#[derive(Clone)]
pub struct PrincipalLimiter {
    per_principal: usize,
    permits: Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Semaphore>>>>,
}

impl PrincipalLimiter {
    /// Creates a limiter allowing `per_principal` concurrent operations per
    /// principal; `0` disables the limit.
    pub fn new(per_principal: usize) -> Self {
        Self {
            per_principal,
            permits: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    fn acquire(
        &self,
        principal: Option<&str>,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, ServiceError> {
        if self.per_principal == 0 {
            return Ok(None);
        }
        let key = principal.unwrap_or("<unauthenticated>").to_string();
        let semaphore = {
            let mut permits = self.permits.lock().expect("principal limiter poisoned");
            permits
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(self.per_principal)))
                .clone()
        };
        semaphore
            .try_acquire_owned()
            .map(Some)
            .map_err(|_| ServiceError::SlowDown)
    }
}

/// A per-request context derived from the adapter.
struct RequestScope {
    ctx: DriverContext,
    request_id: String,
    principal: Option<String>,
    /// Held for the whole request so the principal's slot is released when the
    /// operation ends, however it ends.
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

fn scope() -> RequestScope {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let request_id = format!("req-{n:016x}");
    RequestScope {
        ctx: DriverContext::new(RequestId::new(request_id.clone())),
        request_id,
        principal: None,
        permit: None,
    }
}

fn attach_request_id<T>(mut resp: S3Response<T>, id: &str) -> S3Response<T> {
    if let Ok(value) = HeaderValue::from_str(id) {
        resp.headers.insert("x-amz-request-id", value);
    }
    resp
}

fn map_error(err: ServiceError, request_id: &str) -> S3Error {
    let code: S3ErrorCode = err
        .code()
        .parse()
        .unwrap_or_else(|_| S3ErrorCode::InternalError);
    let mut out = S3Error::with_message(code, err.message());
    out.set_request_id(request_id.to_string());
    out
}

// --- DTO conversion helpers ------------------------------------------------

fn to_timestamp(time: std::time::SystemTime) -> Timestamp {
    Timestamp::from(time)
}

fn to_system_time(ts: &Timestamp) -> std::time::SystemTime {
    std::time::SystemTime::from(time::OffsetDateTime::from(ts.clone()))
}

fn to_content_properties(input: &S3ContentInput) -> ContentProperties {
    ContentProperties {
        content_type: input.content_type.clone(),
        content_encoding: input.content_encoding.clone(),
        content_language: input.content_language.clone(),
        content_disposition: input.content_disposition.clone(),
        cache_control: input.cache_control.clone(),
        expires: None,
    }
}

/// The subset of content headers shared by `PutObject`/`CopyObject`.
struct S3ContentInput {
    content_type: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_disposition: Option<String>,
    cache_control: Option<String>,
}

/// Converts request metadata, rejecting anything over the metadata limits.
///
/// Silently dropping an over-limit entry would publish an object whose metadata
/// differs from what the client sent.
fn to_user_metadata(
    metadata: &Option<S3Metadata>,
    request_id: &str,
) -> Result<UserMetadata, S3Error> {
    let mut out = UserMetadata::new();
    if let Some(map) = metadata {
        for (k, v) in map {
            out.insert(k.clone(), v.clone()).map_err(|e| {
                map_error(
                    ServiceError::InvalidRequest(format!("invalid user metadata: {e}")),
                    request_id,
                )
            })?;
        }
    }
    Ok(out)
}

fn apply_metadata(meta: &crate::domain::ObjectMetadata, out: &mut CommonObjectOutput) {
    out.content_length = Some(meta.size as i64);
    out.e_tag = meta
        .etag
        .as_ref()
        .map(|t| s3s::dto::ETag::Strong(t.as_str().to_string()));
    out.last_modified = Some(to_timestamp(meta.last_modified));
    let mut metadata = std::collections::HashMap::new();
    for (k, v) in meta.user_metadata.iter() {
        metadata.insert(k.to_string(), v.to_string());
    }
    if !metadata.is_empty() {
        out.metadata = Some(metadata);
    }
    out.content_type = meta.content.content_type.clone();
    out.content_encoding = meta.content.content_encoding.clone();
    out.content_language = meta.content.content_language.clone();
    out.content_disposition = meta.content.content_disposition.clone();
    out.cache_control = meta.content.cache_control.clone();

    for checksum in &meta.checksums {
        let value = base64(checksum.value());
        match checksum.algorithm() {
            crate::domain::ChecksumAlgorithm::Md5 => out.checksum_md5 = Some(value),
            crate::domain::ChecksumAlgorithm::Crc32 => out.checksum_crc32 = Some(value),
            crate::domain::ChecksumAlgorithm::Crc32c => out.checksum_crc32c = Some(value),
            crate::domain::ChecksumAlgorithm::Crc64Nvme => out.checksum_crc64nvme = Some(value),
            crate::domain::ChecksumAlgorithm::Sha1 => out.checksum_sha1 = Some(value),
            crate::domain::ChecksumAlgorithm::Sha256 => out.checksum_sha256 = Some(value),
        }
    }
}

struct CommonObjectOutput {
    content_length: Option<i64>,
    e_tag: Option<s3s::dto::ETag>,
    last_modified: Option<Timestamp>,
    metadata: Option<S3Metadata>,
    content_type: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_disposition: Option<String>,
    cache_control: Option<String>,
    checksum_md5: Option<String>,
    checksum_crc32: Option<String>,
    checksum_crc32c: Option<String>,
    checksum_crc64nvme: Option<String>,
    checksum_sha1: Option<String>,
    checksum_sha256: Option<String>,
}

impl CommonObjectOutput {
    fn new() -> Self {
        Self {
            content_length: None,
            e_tag: None,
            last_modified: None,
            metadata: None,
            content_type: None,
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            checksum_md5: None,
            checksum_crc32: None,
            checksum_crc32c: None,
            checksum_crc64nvme: None,
            checksum_sha1: None,
            checksum_sha256: None,
        }
    }
}

fn read_conditions(
    if_match: &Option<s3s::dto::ETagCondition>,
    if_none_match: &Option<s3s::dto::ETagCondition>,
    if_modified_since: &Option<Timestamp>,
    if_unmodified_since: &Option<Timestamp>,
) -> ReadConditions {
    ReadConditions {
        if_match: single_tag(if_match),
        if_none_match: single_tag(if_none_match),
        if_modified_since: if_modified_since.as_ref().map(to_system_time),
        if_unmodified_since: if_unmodified_since.as_ref().map(to_system_time),
    }
}

fn single_tag(condition: &Option<s3s::dto::ETagCondition>) -> Option<Vec<EntityTag>> {
    match condition {
        None | Some(s3s::dto::ETagCondition::Any) => None,
        Some(s3s::dto::ETagCondition::ETag(tag)) => EntityTag::new(tag.value().to_string())
            .ok()
            .map(|t| vec![t]),
    }
}

/// Converts S3 write preconditions, rejecting forms the first phase does not
/// implement atomically rather than silently ignoring them.
fn write_preconditions(
    if_match: &Option<s3s::dto::ETagCondition>,
    if_none_match: &Option<s3s::dto::ETagCondition>,
) -> Result<Preconditions, ServiceError> {
    let mut preconditions = Preconditions::none();
    match if_match {
        None => {}
        Some(s3s::dto::ETagCondition::ETag(tag)) => {
            let entity = EntityTag::new(tag.value().to_string())
                .map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;
            preconditions.if_match = Some(entity);
        }
        Some(s3s::dto::ETagCondition::Any) => {
            return Err(ServiceError::NotImplemented(
                "`If-Match: *` is not supported for this operation".into(),
            ));
        }
    }
    match if_none_match {
        None => {}
        Some(s3s::dto::ETagCondition::Any) => preconditions.if_none_match = true,
        Some(s3s::dto::ETagCondition::ETag(_)) => {
            return Err(ServiceError::NotImplemented(
                "`If-None-Match` with a specific ETag is not supported for this operation".into(),
            ));
        }
    }
    Ok(preconditions)
}

fn to_range(range: &Option<s3s::dto::Range>) -> Option<RangeSpec> {
    range.map(|r| match r {
        s3s::dto::Range::Int { first, last } => match last {
            Some(last) => RangeSpec::Closed {
                start: first,
                end: last,
            },
            None => RangeSpec::Open { start: first },
        },
        s3s::dto::Range::Suffix { length } => RangeSpec::Suffix { suffix: length },
    })
}

// --- Byte-stream bridging --------------------------------------------------

/// Bridges a driver byte stream into the protocol library's blob type.
struct OutputStream {
    inner: ObjectByteStream,
}

impl futures_core::Stream for OutputStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e) as StdError))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl ByteStream for OutputStream {
    fn remaining_length(&self) -> RemainingLength {
        RemainingLength::unknown()
    }
}

fn to_blob(stream: ObjectByteStream) -> StreamingBlob {
    StreamingBlob::new(OutputStream { inner: stream })
}

/// Bridges the protocol library's request body into a driver byte stream.
struct InputStream {
    inner: StreamingBlob,
}

impl futures_core::Stream for InputStream {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(DriverError::internal(format!(
                "request body error: {e}"
            ))))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A stream that yields nothing.
struct EmptyStream;

impl futures_core::Stream for EmptyStream {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

fn body_stream(body: Option<StreamingBlob>) -> ObjectByteStream {
    match body {
        Some(blob) => Box::pin(InputStream { inner: blob }),
        None => Box::pin(EmptyStream),
    }
}

// --- Authentication / authorization ---------------------------------------

/// Static, config-backed credential lookup.
#[derive(Clone)]
pub struct CredentialAuth {
    store: Arc<CredentialStore>,
}

impl CredentialAuth {
    /// Creates the auth provider.
    pub fn new(store: Arc<CredentialStore>) -> Self {
        Self { store }
    }

    /// Maps an access key to its principal.
    pub fn principal(&self, access_key: &str) -> Option<String> {
        self.store.get(access_key).map(|c| c.principal.clone())
    }
}

#[async_trait]
impl S3Auth for CredentialAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.store.get(access_key) {
            Some(credential) => Ok(SecretKey::from(credential.secret.expose())),
            None => Err(s3s::s3_error!(InvalidAccessKeyId)),
        }
    }
}

/// Principal/action/bucket/prefix authorization.
#[derive(Clone)]
pub struct GrantAccess {
    policy: Arc<AccessPolicy>,
    credentials: Arc<CredentialStore>,
}

impl GrantAccess {
    /// Creates the access provider.
    pub fn new(policy: Arc<AccessPolicy>, credentials: Arc<CredentialStore>) -> Self {
        Self {
            policy,
            credentials,
        }
    }
}

fn action_for_operation(name: &str) -> Option<Action> {
    match name {
        "ListBuckets" => Some(Action::List),
        "HeadBucket" | "GetBucketLocation" | "ListObjectsV2" | "ListObjects" => Some(Action::List),
        "HeadObject" | "GetObject" | "ListParts" | "ListMultipartUploads" => Some(Action::Read),
        "PutObject"
        | "CopyObject"
        | "CreateMultipartUpload"
        | "UploadPart"
        | "UploadPartCopy"
        | "CompleteMultipartUpload"
        | "AbortMultipartUpload" => Some(Action::Write),
        "DeleteObject" | "DeleteObjects" => Some(Action::Delete),
        _ => None,
    }
}

#[async_trait]
impl S3Access for GrantAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let op = cx.s3_op().name();
        // Unknown/unimplemented operations fall through to their handler's own
        // `NotImplemented` error; they are not authorized here.
        let Some(action) = action_for_operation(op) else {
            return Ok(());
        };

        let principal = cx
            .credentials()
            .and_then(|c| self.credentials.get(&c.access_key))
            .map(|c| c.principal.clone());

        // Account-level listing is allowed for any authenticated principal; the
        // result is filtered to the buckets they can see.
        if op == "ListBuckets" {
            return if principal.is_some() {
                Ok(())
            } else {
                Err(s3s::s3_error!(AccessDenied))
            };
        }

        let (bucket, key) = match cx.s3_path().as_object() {
            Some((bucket, key)) => (bucket.to_string(), Some(key.to_string())),
            None => match cx.s3_path().as_bucket() {
                Some(bucket) => (bucket.to_string(), None),
                None => return Err(s3s::s3_error!(AccessDenied)),
            },
        };

        if self
            .policy
            .authorize(principal.as_deref(), action, &bucket, key.as_deref())
        {
            Ok(())
        } else {
            Err(s3s::s3_error!(AccessDenied))
        }
    }
}

// --- The S3 operations -----------------------------------------------------

#[async_trait]
impl S3 for S3Handler {
    async fn list_buckets(
        &self,
        req: S3Request<s3s::dto::ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let scope = self.scope(&req)?;
        let buckets = self
            .service
            .list_buckets(scope.principal.as_deref())
            .into_iter()
            .map(|binding| Bucket {
                name: Some(binding.bucket.to_string()),
                creation_date: None,
                bucket_region: Some(binding.region),
                bucket_arn: None,
            })
            .collect::<Vec<_>>();

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(Owner {
                id: Some("allins3".to_string()),
                display_name: Some("allins3".to_string()),
            }),
            continuation_token: None,
            prefix: None,
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn head_bucket(
        &self,
        req: S3Request<s3s::dto::HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let scope = self.scope(&req)?;
        let binding = self
            .service
            .head_bucket(req.input.bucket.as_str())
            .map_err(|e| map_error(e, &scope.request_id))?;
        let output = HeadBucketOutput {
            bucket_region: Some(binding.region),
            bucket_arn: None,
            bucket_location_name: None,
            bucket_location_type: None,
            access_point_alias: None,
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<s3s::dto::GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let scope = self.scope(&req)?;
        let binding = self
            .service
            .head_bucket(req.input.bucket.as_str())
            .map_err(|e| map_error(e, &scope.request_id))?;
        let output = GetBucketLocationOutput {
            location_constraint: Some(BucketLocationConstraint::from(binding.region)),
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn head_object(
        &self,
        req: S3Request<s3s::dto::HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let scope = self.scope(&req)?;
        let key = parse_key(&req.input.key, &scope.request_id)?;
        let conditions = read_conditions(
            &req.input.if_match,
            &req.input.if_none_match,
            &req.input.if_modified_since,
            &req.input.if_unmodified_since,
        );
        let result = self
            .service
            .head_object(
                &scope.ctx,
                req.input.bucket.as_str(),
                &key,
                &conditions,
                to_range(&req.input.range),
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let mut common = CommonObjectOutput::new();
        apply_metadata(&result.metadata, &mut common);
        let output = HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            content_length: common.content_length,
            content_range: result.range.as_ref().map(|r| r.content_range()),
            e_tag: common.e_tag,
            last_modified: common.last_modified,
            metadata: common.metadata,
            content_type: common.content_type,
            content_encoding: common.content_encoding,
            content_language: common.content_language,
            content_disposition: common.content_disposition,
            cache_control: common.cache_control,
            checksum_md5: common.checksum_md5,
            checksum_crc32: common.checksum_crc32,
            checksum_crc32c: common.checksum_crc32c,
            checksum_crc64nvme: common.checksum_crc64nvme,
            checksum_sha1: common.checksum_sha1,
            checksum_sha256: common.checksum_sha256,
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn get_object(
        &self,
        req: S3Request<s3s::dto::GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let scope = self.scope(&req)?;
        let key = parse_key(&req.input.key, &scope.request_id)?;

        // Reject explicitly unsupported parameters rather than ignoring them.
        if req.input.version_id.is_some() {
            return Err(map_error(
                ServiceError::NotImplemented("object versioning is not supported".into()),
                &scope.request_id,
            ));
        }

        let conditions = read_conditions(
            &req.input.if_match,
            &req.input.if_none_match,
            &req.input.if_modified_since,
            &req.input.if_unmodified_since,
        );
        let result = self
            .service
            .get_object(
                &scope.ctx,
                req.input.bucket.as_str(),
                &key,
                &conditions,
                to_range(&req.input.range),
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let mut common = CommonObjectOutput::new();
        apply_metadata(&result.metadata, &mut common);
        // A partial response advertises the length of the range, not of the object;
        // otherwise the body would contradict `Content-Length`.
        if let Some(range) = &result.range {
            common.content_length = Some(range.len() as i64);
            // Whole-object checksums do not describe a byte range; publishing them
            // would make clients fail validation on a partial response.
            common.checksum_md5 = None;
            common.checksum_crc32 = None;
            common.checksum_crc32c = None;
            common.checksum_crc64nvme = None;
            common.checksum_sha1 = None;
            common.checksum_sha256 = None;
        }
        let output = GetObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            body: Some(to_blob(result.stream)),
            content_length: common.content_length,
            content_range: result.range.as_ref().map(|r| r.content_range()),
            e_tag: common.e_tag,
            last_modified: common.last_modified,
            metadata: common.metadata,
            content_type: common.content_type,
            content_encoding: common.content_encoding,
            content_language: common.content_language,
            content_disposition: common.content_disposition,
            cache_control: common.cache_control,
            checksum_md5: common.checksum_md5,
            checksum_crc32: common.checksum_crc32,
            checksum_crc32c: common.checksum_crc32c,
            checksum_crc64nvme: common.checksum_crc64nvme,
            checksum_sha1: common.checksum_sha1,
            checksum_sha256: common.checksum_sha256,
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn put_object(
        &self,
        req: S3Request<s3s::dto::PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;

        // Explicitly unsupported parameters must fail, not be ignored.
        if input.server_side_encryption.is_some()
            || input.sse_customer_algorithm.is_some()
            || input.ssekms_key_id.is_some()
            || input.acl.is_some()
            || input.tagging.is_some()
        {
            return Err(map_error(
                ServiceError::NotImplemented(
                    "encryption, ACL and tagging parameters are not supported".into(),
                ),
                &scope.request_id,
            ));
        }
        let key = parse_key(&input.key, &scope.request_id)?;
        let content_props = to_content_properties(&S3ContentInput {
            content_type: input.content_type.clone(),
            content_encoding: input.content_encoding.clone(),
            content_language: input.content_language.clone(),
            content_disposition: input.content_disposition.clone(),
            cache_control: input.cache_control.clone(),
        });
        let user_metadata = to_user_metadata(&input.metadata, &scope.request_id)?;
        let preconditions = write_preconditions(&input.if_match, &input.if_none_match)
            .map_err(|e| map_error(e, &scope.request_id))?;

        let content_md5 = match &input.content_md5 {
            Some(value) => Some(decode_base64(value).map_err(|e| {
                map_error(
                    ServiceError::InvalidArgument(format!("invalid Content-MD5: {e}")),
                    &scope.request_id,
                )
            })?),
            None => None,
        };

        let (checksum_algorithm, declared_checksum) = parse_checksums(&input, &scope.request_id)?;

        let params = crate::service::PutParams {
            expected_length: input.content_length.map(|n| n as u64),
            content_md5,
            declared_checksum,
            checksum_algorithm,
            content: content_props,
            user_metadata,
            preconditions,
        };

        let meta = self
            .service
            .put_object(
                &scope.ctx,
                input.bucket.as_str(),
                &key,
                body_stream(input.body),
                params,
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let mut common = CommonObjectOutput::new();
        apply_metadata(&meta, &mut common);
        let output = PutObjectOutput {
            e_tag: common.e_tag,
            checksum_md5: common.checksum_md5,
            checksum_crc32: common.checksum_crc32,
            checksum_crc32c: common.checksum_crc32c,
            checksum_crc64nvme: common.checksum_crc64nvme,
            checksum_sha1: common.checksum_sha1,
            checksum_sha256: common.checksum_sha256,
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn delete_object(
        &self,
        req: S3Request<s3s::dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;
        if input.version_id.is_some() {
            return Err(map_error(
                ServiceError::NotImplemented("object versioning is not supported".into()),
                &scope.request_id,
            ));
        }
        let key = parse_key(&input.key, &scope.request_id)?;
        let preconditions = write_preconditions(&input.if_match, &None)
            .map_err(|e| map_error(e, &scope.request_id))?;
        self.service
            .delete_object(&scope.ctx, input.bucket.as_str(), &key, preconditions)
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;
        Ok(attach_request_id(
            S3Response::new(DeleteObjectOutput::default()),
            &scope.request_id,
        ))
    }

    async fn delete_objects(
        &self,
        req: S3Request<s3s::dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;

        let mut items = Vec::with_capacity(input.delete.objects.len());
        for object in &input.delete.objects {
            let key = parse_key(&object.key, &scope.request_id)?;
            items.push(crate::service::DeleteItem { key });
        }

        let quiet = input.delete.quiet.unwrap_or(false);
        let results = self
            .service
            .delete_objects(&scope.ctx, input.bucket.as_str(), &items)
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for result in results {
            match result.error {
                Some((code, message)) => errors.push(S3DtoError {
                    code: Some(code),
                    message: Some(message),
                    key: Some(result.key.to_string()),
                    version_id: None,
                }),
                None if !quiet => deleted.push(DeletedObject {
                    key: Some(result.key.to_string()),
                    delete_marker: None,
                    delete_marker_version_id: None,
                    version_id: None,
                }),
                None => {}
            }
        }

        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            request_charged: None,
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<s3s::dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let scope = self.scope(&req)?;
        let input = req.input;

        let delimiter = input.delimiter.as_ref().and_then(|d| d.chars().next());
        let start_after = match &input.start_after {
            Some(value) => Some(parse_key(value, &scope.request_id)?),
            None => None,
        };
        let encoding = input.encoding_type.as_ref().map(|e| e.as_str().to_string());
        if let Some(encoding) = &encoding
            && encoding != "url"
        {
            return Err(map_error(
                ServiceError::InvalidArgument(format!("unsupported encoding-type `{encoding}`")),
                &scope.request_id,
            ));
        }

        let result = self
            .service
            .list_objects_v2(
                &scope.ctx,
                input.bucket.as_str(),
                scope.principal.as_deref(),
                input.prefix.clone(),
                delimiter,
                start_after,
                input.continuation_token.clone(),
                encoding.clone(),
                input.max_keys.map(|m| m.max(0) as u32),
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let encode = |value: &str| {
            if encoding.as_deref() == Some("url") {
                url_encode_key(value)
            } else {
                value.to_string()
            }
        };

        let contents = result
            .objects
            .into_iter()
            .map(|meta| S3Object {
                key: Some(encode(meta.key.as_str())),
                size: Some(meta.size as i64),
                e_tag: meta
                    .etag
                    .as_ref()
                    .map(|t| s3s::dto::ETag::Strong(t.as_str().to_string())),
                last_modified: Some(to_timestamp(meta.last_modified)),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let common_prefixes = result
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode(&prefix)),
            })
            .collect::<Vec<_>>();

        let key_count = (contents.len() + common_prefixes.len()) as i32;
        let output = ListObjectsV2Output {
            name: Some(input.bucket.clone()),
            prefix: Some(input.prefix.unwrap_or_default()),
            delimiter: input.delimiter.clone(),
            encoding_type: input.encoding_type.clone(),
            max_keys: input.max_keys,
            key_count: Some(key_count),
            is_truncated: Some(result.is_truncated),
            continuation_token: input.continuation_token.clone(),
            next_continuation_token: result.next_token.clone(),
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            start_after: input.start_after.clone(),
            request_charged: None,
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn copy_object(
        &self,
        req: S3Request<s3s::dto::CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;

        let (source_bucket, source_key) = match &input.copy_source {
            s3s::dto::CopySource::Bucket { bucket, key, .. } => {
                (bucket.to_string(), parse_key(key, &scope.request_id)?)
            }
            _ => {
                return Err(map_error(
                    ServiceError::NotImplemented(
                        "only `bucket/key` copy sources are supported".into(),
                    ),
                    &scope.request_id,
                ));
            }
        };

        // The destination was authorized by the access layer; the source must
        // be authorized separately.
        if !self.service.authorize(
            scope.principal.as_deref(),
            Action::Read,
            &source_bucket,
            Some(source_key.as_str()),
        ) {
            return Err(map_error(ServiceError::AccessDenied, &scope.request_id));
        }

        let source_conditions = ReadConditions {
            if_match: single_tag(&input.copy_source_if_match),
            if_none_match: single_tag(&input.copy_source_if_none_match),
            if_modified_since: input
                .copy_source_if_modified_since
                .as_ref()
                .map(to_system_time),
            if_unmodified_since: input
                .copy_source_if_unmodified_since
                .as_ref()
                .map(to_system_time),
        };
        let preconditions = write_preconditions(&input.if_match, &input.if_none_match)
            .map_err(|e| map_error(e, &scope.request_id))?;

        let directive = match &input.metadata_directive {
            Some(d) if d.as_str() == "REPLACE" => MetadataDirective::Replace,
            _ => MetadataDirective::Copy,
        };
        let content = to_content_properties(&S3ContentInput {
            content_type: input.content_type.clone(),
            content_encoding: input.content_encoding.clone(),
            content_language: input.content_language.clone(),
            content_disposition: input.content_disposition.clone(),
            cache_control: input.cache_control.clone(),
        });
        let user_metadata = to_user_metadata(&input.metadata, &scope.request_id)?;
        let destination_key = parse_key(&input.key, &scope.request_id)?;

        let meta = self
            .service
            .copy_object(
                &scope.ctx,
                input.bucket.as_str(),
                CopyParams {
                    source_bucket,
                    source_key,
                    source_conditions,
                    destination_key,
                    directive,
                    content,
                    user_metadata,
                    preconditions,
                },
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let output = CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: meta
                    .etag
                    .as_ref()
                    .map(|t| s3s::dto::ETag::Strong(t.as_str().to_string())),
                last_modified: Some(to_timestamp(meta.last_modified)),
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }
    // --- Multipart operations ---------------------------------------------

    async fn create_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;
        if input.server_side_encryption.is_some()
            || input.sse_customer_algorithm.is_some()
            || input.ssekms_key_id.is_some()
            || input.acl.is_some()
            || input.tagging.is_some()
        {
            return Err(map_error(
                ServiceError::NotImplemented(
                    "encryption, ACL and tagging parameters are not supported".into(),
                ),
                &scope.request_id,
            ));
        }
        let key = parse_key(&input.key, &scope.request_id)?;
        let content = to_content_properties(&S3ContentInput {
            content_type: input.content_type.clone(),
            content_encoding: input.content_encoding.clone(),
            content_language: input.content_language.clone(),
            content_disposition: input.content_disposition.clone(),
            cache_control: input.cache_control.clone(),
        });
        let session = self
            .service
            .create_multipart(
                input.bucket.as_str(),
                &key,
                scope.principal.as_deref(),
                content,
                to_user_metadata(&input.metadata, &scope.request_id)?,
            )
            .map_err(|e| map_error(e, &scope.request_id))?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(input.bucket.clone()),
            key: Some(input.key.clone()),
            upload_id: Some(session.upload_id.as_str().to_string()),
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn upload_part(
        &self,
        req: S3Request<s3s::dto::UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;
        let key = parse_key(&input.key, &scope.request_id)?;
        let part_number = part_number(&input.part_number, &scope.request_id)?;

        let summary = self
            .service
            .upload_part(
                &scope.ctx,
                input.bucket.as_str(),
                &key,
                &input.upload_id,
                part_number,
                body_stream(input.body),
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let output = UploadPartOutput {
            e_tag: Some(s3s::dto::ETag::Strong(summary.etag.as_str().to_string())),
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<s3s::dto::UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;
        let key = parse_key(&input.key, &scope.request_id)?;
        let part = part_number(&input.part_number, &scope.request_id)?;

        let (source_bucket, source_key) = match &input.copy_source {
            s3s::dto::CopySource::Bucket { bucket, key, .. } => {
                (bucket.to_string(), parse_key(key, &scope.request_id)?)
            }
            _ => {
                return Err(map_error(
                    ServiceError::NotImplemented(
                        "only `bucket/key` copy sources are supported".into(),
                    ),
                    &scope.request_id,
                ));
            }
        };
        if !self.service.authorize(
            scope.principal.as_deref(),
            Action::Read,
            &source_bucket,
            Some(source_key.as_str()),
        ) {
            return Err(map_error(ServiceError::AccessDenied, &scope.request_id));
        }

        let range = input
            .copy_source_range
            .as_ref()
            .map(|value| parse_copy_range(value))
            .transpose()
            .map_err(|e| map_error(e, &scope.request_id))?;

        let (summary, source_meta) = self
            .service
            .upload_part_copy(
                &scope.ctx,
                crate::service::multipart::UploadPartCopyRequest {
                    bucket: input.bucket.as_str().to_string(),
                    key,
                    upload_id: input.upload_id.clone(),
                    part_number: part,
                    source_bucket,
                    source_key,
                    range,
                },
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let output = UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(s3s::dto::ETag::Strong(summary.etag.as_str().to_string())),
                last_modified: Some(to_timestamp(source_meta.last_modified)),
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn list_parts(
        &self,
        req: S3Request<s3s::dto::ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;
        let key = parse_key(&input.key, &scope.request_id)?;
        let marker = input.part_number_marker.map(|m| m.clamp(0, 10_000) as u16);
        let limit = input.max_parts.unwrap_or(1000).clamp(1, 1000) as u32;

        let page = self
            .service
            .list_parts(input.bucket.as_str(), &key, &input.upload_id, marker, limit)
            .map_err(|e| map_error(e, &scope.request_id))?;

        let parts = page
            .parts
            .into_iter()
            .map(|p| S3Part {
                part_number: Some(p.number as i32),
                size: Some(p.size as i64),
                e_tag: Some(s3s::dto::ETag::Strong(p.etag.as_str().to_string())),
                last_modified: Some(to_timestamp(p.last_modified)),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let output = ListPartsOutput {
            bucket: Some(input.bucket.clone()),
            key: Some(input.key.clone()),
            upload_id: Some(input.upload_id.clone()),
            part_number_marker: input.part_number_marker,
            next_part_number_marker: page.next_marker.map(|m| m as i32),
            max_parts: input.max_parts,
            is_truncated: Some(page.is_truncated),
            parts: Some(parts),
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<s3s::dto::ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;
        let limit = input.max_uploads.unwrap_or(1000).clamp(1, 1000) as u32;

        let page = self
            .service
            .list_uploads(
                input.bucket.as_str(),
                input.prefix.as_deref(),
                input.key_marker.as_deref(),
                input.upload_id_marker.as_deref(),
                limit,
            )
            .map_err(|e| map_error(e, &scope.request_id))?;

        let uploads = page
            .uploads
            .into_iter()
            .map(|session| S3MultipartUpload {
                key: Some(session.key.to_string()),
                upload_id: Some(session.upload_id.as_str().to_string()),
                initiated: Some(to_timestamp(session.initiated)),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let output = ListMultipartUploadsOutput {
            bucket: Some(input.bucket.clone()),
            prefix: input.prefix.clone(),
            delimiter: input.delimiter.clone(),
            key_marker: input.key_marker.clone(),
            upload_id_marker: input.upload_id_marker.clone(),
            next_key_marker: page.next_key_marker.as_ref().map(|k| k.to_string()),
            next_upload_id_marker: page
                .next_upload_id_marker
                .as_ref()
                .map(|u| u.as_str().to_string()),
            is_truncated: Some(page.is_truncated),
            uploads: Some(uploads),
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;
        let key = parse_key(&input.key, &scope.request_id)?;

        let manifest = match &input.multipart_upload {
            Some(completed) => completed.parts.clone().unwrap_or_default(),
            None => Vec::new(),
        };
        let mut requested = Vec::with_capacity(manifest.len());
        for part in &manifest {
            let number = part.part_number.ok_or_else(|| {
                map_error(
                    ServiceError::MalformedXml(
                        "a completed part is missing its part number".into(),
                    ),
                    &scope.request_id,
                )
            })?;
            let number = part_number(&number, &scope.request_id)?;
            let etag = match &part.e_tag {
                Some(tag) => Some(EntityTag::new(tag.value().to_string()).map_err(|e| {
                    map_error(
                        ServiceError::InvalidArgument(e.to_string()),
                        &scope.request_id,
                    )
                })?),
                None => None,
            };
            requested.push((number, etag));
        }

        let preconditions = write_preconditions(&input.if_match, &input.if_none_match)
            .map_err(|e| map_error(e, &scope.request_id))?;

        let meta = self
            .service
            .complete_multipart(
                &scope.ctx,
                input.bucket.as_str(),
                &key,
                &input.upload_id,
                &requested,
                preconditions,
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(input.bucket.clone()),
            key: Some(input.key.clone()),
            location: Some(format!("/{}/{}", input.bucket, input.key)),
            e_tag: meta
                .etag
                .as_ref()
                .map(|t| s3s::dto::ETag::Strong(t.as_str().to_string())),
            ..Default::default()
        };
        Ok(attach_request_id(
            S3Response::new(output),
            &scope.request_id,
        ))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<s3s::dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let scope = self.scope(&req)?;
        let input = req.input;
        let key = parse_key(&input.key, &scope.request_id)?;
        self.service
            .abort_multipart(input.bucket.as_str(), &key, &input.upload_id)
            .map_err(|e| map_error(e, &scope.request_id))?;
        Ok(attach_request_id(
            S3Response::new(AbortMultipartUploadOutput::default()),
            &scope.request_id,
        ))
    }
}

fn part_number(value: &i32, request_id: &str) -> Result<u16, S3Error> {
    if *value < 1 || *value > 10_000 {
        return Err(map_error(
            ServiceError::InvalidArgument(format!("part number {value} is outside 1..=10000")),
            request_id,
        ));
    }
    Ok(*value as u16)
}

fn parse_copy_range(value: &str) -> Result<RangeSpec, ServiceError> {
    let spec =
        RangeSpec::parse_header(value).map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;
    if matches!(spec, RangeSpec::Suffix { .. }) {
        return Err(ServiceError::InvalidArgument(
            "suffix ranges are not supported for UploadPartCopy".into(),
        ));
    }
    Ok(spec)
}

fn parse_key(raw: &str, request_id: &str) -> Result<crate::domain::ObjectKey, S3Error> {
    crate::domain::ObjectKey::parse(raw.to_string())
        .map_err(|e| map_error(ServiceError::InvalidArgument(e.to_string()), request_id))
}

fn decode_base64(value: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|e| e.to_string())
}

/// A checksum algorithm together with the raw value the client declared for it.
type DeclaredChecksum = (crate::domain::ChecksumAlgorithm, Vec<u8>);

/// Extracts a requested checksum algorithm and any client-declared value.
fn parse_checksums(
    input: &s3s::dto::PutObjectInput,
    request_id: &str,
) -> S3Result<(
    Option<crate::domain::ChecksumAlgorithm>,
    Option<DeclaredChecksum>,
)> {
    use crate::domain::ChecksumAlgorithm as Alg;

    let algorithm = input
        .checksum_algorithm
        .as_ref()
        .map(|a| checksum_algorithm_from(a.as_str(), request_id))
        .transpose()?;

    if input.checksum_sha512.is_some()
        || input.checksum_xxhash128.is_some()
        || input.checksum_xxhash3.is_some()
        || input.checksum_xxhash64.is_some()
    {
        return Err(map_error(
            ServiceError::NotImplemented("unsupported checksum algorithm".into()),
            request_id,
        ));
    }

    let declared = [
        (Alg::Crc32, input.checksum_crc32.as_ref()),
        (Alg::Crc32c, input.checksum_crc32c.as_ref()),
        (Alg::Crc64Nvme, input.checksum_crc64nvme.as_ref()),
        (Alg::Sha1, input.checksum_sha1.as_ref()),
        (Alg::Sha256, input.checksum_sha256.as_ref()),
    ]
    .into_iter()
    .find_map(|(alg, value)| value.map(|v| (alg, v)));

    let declared = match declared {
        Some((alg, value)) => {
            let bytes = decode_base64(value).map_err(|e| {
                map_error(
                    ServiceError::InvalidArgument(format!("invalid checksum: {e}")),
                    request_id,
                )
            })?;
            Some((alg, bytes))
        }
        None => None,
    };

    Ok((algorithm, declared))
}

fn checksum_algorithm_from(
    value: &str,
    request_id: &str,
) -> S3Result<crate::domain::ChecksumAlgorithm> {
    use crate::domain::ChecksumAlgorithm as Alg;
    match value {
        "CRC32" => Ok(Alg::Crc32),
        "CRC32C" => Ok(Alg::Crc32c),
        "CRC64NVME" => Ok(Alg::Crc64Nvme),
        "SHA1" => Ok(Alg::Sha1),
        "SHA256" => Ok(Alg::Sha256),
        other => Err(map_error(
            ServiceError::NotImplemented(format!("unsupported checksum algorithm `{other}`")),
            request_id,
        )),
    }
}

/// Percent-encodes a key for `encoding-type=url`.
fn url_encode_key(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Builds the `s3s` service, wiring auth and access.
pub fn build_service(
    handler: S3Handler,
    credentials: Arc<CredentialStore>,
    policy: Arc<AccessPolicy>,
    config: s3s::config::S3Config,
) -> s3s::service::S3Service {
    let mut builder = s3s::service::S3ServiceBuilder::new(handler);
    let provider = s3s::config::StaticConfigProvider::new(Arc::new(config));
    builder.set_config(Arc::new(provider));
    builder.set_auth(CredentialAuth::new(credentials.clone()));
    builder.set_access(GrantAccess::new(policy, credentials));
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::dto::ETagCondition;

    #[test]
    fn url_encoding_preserves_unreserved_only() {
        assert_eq!(url_encode_key("a/b c"), "a%2Fb%20c");
        assert_eq!(url_encode_key("a~b"), "a~b");
    }

    #[test]
    fn range_conversion() {
        assert_eq!(
            to_range(&Some(s3s::dto::Range::Int {
                first: 0,
                last: Some(5)
            })),
            Some(RangeSpec::Closed { start: 0, end: 5 })
        );
        assert_eq!(
            to_range(&Some(s3s::dto::Range::Int {
                first: 5,
                last: None
            })),
            Some(RangeSpec::Open { start: 5 })
        );
        assert_eq!(
            to_range(&Some(s3s::dto::Range::Suffix { length: 3 })),
            Some(RangeSpec::Suffix { suffix: 3 })
        );
    }

    #[test]
    fn write_preconditions_reject_unsupported_forms() {
        let any = Some(ETagCondition::Any);
        assert!(write_preconditions(&None, &any).is_ok());
        assert!(write_preconditions(&any, &None).is_err());
        let tag = Some(ETagCondition::ETag(s3s::dto::ETag::Strong("abc".into())));
        assert!(write_preconditions(&None, &tag).is_err());
        assert!(write_preconditions(&tag, &None).is_ok());
    }

    #[test]
    fn per_principal_admission_is_bounded_and_released() {
        let limiter = PrincipalLimiter::new(2);

        let first = limiter.acquire(Some("alice")).unwrap().expect("first");
        let second = limiter.acquire(Some("alice")).unwrap().expect("second");
        // A third concurrent request from the same principal is rejected with a
        // retryable `SlowDown` rather than queueing behind the first two.
        assert!(matches!(
            limiter.acquire(Some("alice")),
            Err(ServiceError::SlowDown)
        ));
        assert_eq!(ServiceError::SlowDown.code(), "SlowDown");

        // Other principals and unauthenticated callers have their own budgets.
        let bob = limiter.acquire(Some("bob")).unwrap().expect("bob");
        let anonymous = limiter.acquire(None).unwrap().expect("anonymous");

        // Releasing a slot makes it available again.
        drop(first);
        let third = limiter.acquire(Some("alice")).unwrap().expect("third");
        drop((second, third, bob, anonymous));
        assert!(limiter.acquire(Some("alice")).unwrap().is_some());
    }

    #[test]
    fn a_zero_limit_disables_admission_control() {
        let limiter = PrincipalLimiter::new(0);
        for _ in 0..64 {
            assert!(limiter.acquire(Some("alice")).unwrap().is_none());
        }
    }

    #[test]
    fn action_mapping() {
        assert_eq!(action_for_operation("GetObject"), Some(Action::Read));
        assert_eq!(action_for_operation("DeleteObjects"), Some(Action::Delete));
        assert_eq!(action_for_operation("PutObject"), Some(Action::Write));
        assert_eq!(action_for_operation("Unknown"), None);
    }
}
