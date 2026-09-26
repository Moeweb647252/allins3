//! The S3 protocol adapter, built on the `s3s` candidate library.
//!
//! Responsibilities: routing (via `s3s`), XML, HTTP headers, the signature's
//! raw input and S3 error encoding. It must not contain backend path rules or
//! storage-product branches (`ADR-0007`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use hyper::header::HeaderValue;
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::dto::{
    AbortMultipartUploadOutput, Bucket, BucketLocationConstraint, CommonPrefix,
    CompleteMultipartUploadOutput, CopyObjectOutput, CopyObjectResult, CopyPartResult,
    CreateMultipartUploadOutput, DeleteObjectOutput, DeleteObjectsOutput, DeletedObject,
    Error as S3DtoError, GetBucketLocationOutput, GetObjectOutput, HeadBucketOutput,
    HeadObjectOutput, ListBucketsOutput, ListMultipartUploadsOutput, ListObjectsV2Output,
    ListPartsOutput, MultipartUpload as S3MultipartUpload, Object as S3Object, Owner,
    Part as S3Part, PutObjectOutput, UploadPartCopyOutput, UploadPartOutput,
};
use s3s::{S3, S3Request, S3Response, S3Result};

mod body;
mod checksum;
mod convert;

use body::{body_stream, to_blob};
use checksum::*;
use convert::*;

// --- The adapter facade ----------------------------------------------------
//
// `ADR-0007` keeps `s3s` inside this module. The composition root builds the
// service, its config and its bodies through these re-exports rather than
// naming the library, so replacing the candidate stays an adapter change.
pub use s3s::config::S3Config;
pub use s3s::{Body as S3Body, HttpError, HttpResponse, S3Error, S3ErrorCode};

/// The built adapter service the server drives.
pub type S3Service = s3s::service::S3Service;

/// The adapter configuration derived from the resolved limits.
///
/// Every default this library ships is pinned explicitly here, so a dependency
/// bump that changes one fails a test rather than silently changing behaviour
/// (`ADR-0007` §3).
pub fn adapter_config(limits: &crate::config::Limits) -> S3Config {
    let mut config = S3Config::default();
    // SigV4 only (spec §6.1); the candidate defaults to off, assert it anyway.
    config.enable_sig_v2 = false;
    // Preserve significant key bytes (`a//b` != `a/b`).
    config.normalize_forward_slash_path = false;
    // Only accept the `s3` service scope.
    config.sig_v4_allowed_services = vec!["s3".to_string()];
    config.put_object_max_size = Some(limits.max_single_write_size);
    config.xml_max_body_size = limits.max_xml_bytes;
    config
}

use crate::auth::{AccessPolicy, Action, CredentialStore};
use crate::domain::{ChecksumAlgorithm, DriverContext, EntityTag, RangeSpec, RequestId};
use crate::driver::MetadataDirective;
use crate::integrity::base64;
use crate::service::multipart::CompletionPart;
use crate::service::{CopyParams, ObjectService, ReadConditions, ServiceError};

/// Sets the `x-amz-checksum-*` field of a part-level response DTO.
macro_rules! set_part_checksum {
    ($out:expr, $checksum:expr) => {
        if let Some(checksum) = $checksum {
            let value = base64(&checksum.value);
            match checksum.algorithm {
                ChecksumAlgorithm::Md5 => $out.checksum_md5 = Some(value),
                ChecksumAlgorithm::Crc32 => $out.checksum_crc32 = Some(value),
                ChecksumAlgorithm::Crc32c => $out.checksum_crc32c = Some(value),
                ChecksumAlgorithm::Crc64Nvme => $out.checksum_crc64nvme = Some(value),
                ChecksumAlgorithm::Sha1 => $out.checksum_sha1 = Some(value),
                ChecksumAlgorithm::Sha256 => $out.checksum_sha256 = Some(value),
            }
        }
    };
}

/// The S3 service implementation.
#[derive(Clone)]
pub struct S3Handler {
    service: ObjectService,
    credentials: Arc<CredentialStore>,
    limiter: PrincipalLimiter,
    /// Mints the `x-amz-request-id` of each request. Owned by the handler, not a
    /// process-wide static, so one service cannot advance another's ids and a
    /// test observes the sequence it started.
    request_counter: Arc<AtomicU64>,
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
            request_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A fresh request scope: a unique id and a request context, before any
    /// admission decision is made.
    fn request_scope(&self) -> RequestScope {
        let n = self.request_counter.fetch_add(1, Ordering::Relaxed);
        let request_id = format!("req-{n:016x}");
        RequestScope {
            ctx: DriverContext::new(RequestId::new(request_id.clone())),
            request_id,
            principal: None,
            permit: None,
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
        let mut scope = self.request_scope();
        // The backend operation deadline is the total deadline (spec §9.3): a
        // driver call must not outlive the request that started it, so the
        // request context carries the same bound the server enforces around the
        // whole call. There is no separate knob because no backend call is ever
        // allowed to run longer than its request.
        if let Some(deadline) = self.service.limits().request_timeout {
            scope.ctx = scope.ctx.with_timeout(deadline);
        }
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
            let mut permits = crate::sync::lock(&self.permits);
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

fn attach_request_id<T>(mut resp: S3Response<T>, id: &str) -> S3Response<T> {
    if let Ok(value) = HeaderValue::from_str(id) {
        resp.headers.insert("x-amz-request-id", value);
    }
    resp
}

fn map_error(err: ServiceError, request_id: &str) -> S3Error {
    let shape = err.describe();
    // `describe` is the single source of truth for the code name, and
    // `every_service_error_code_is_a_known_s3_code` proves each one parses, so
    // this fallback is unreachable rather than a silent degradation.
    let code: S3ErrorCode = shape
        .code
        .parse()
        .unwrap_or_else(|_| S3ErrorCode::InternalError);
    let mut out = S3Error::with_message(code, shape.message);
    out.set_request_id(request_id.to_string());
    out
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
            expires: common.expires,
            checksum_md5: common.checksums.md5,
            checksum_crc32: common.checksums.crc32,
            checksum_crc32c: common.checksums.crc32c,
            checksum_crc64nvme: common.checksums.crc64nvme,
            checksum_sha1: common.checksums.sha1,
            checksum_sha256: common.checksums.sha256,
            checksum_type: common.checksums.checksum_type,
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
            common.checksums = ChecksumHeaders::default();
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
            expires: common.expires,
            checksum_md5: common.checksums.md5,
            checksum_crc32: common.checksums.crc32,
            checksum_crc32c: common.checksums.crc32c,
            checksum_crc64nvme: common.checksums.crc64nvme,
            checksum_sha1: common.checksums.sha1,
            checksum_sha256: common.checksums.sha256,
            checksum_type: common.checksums.checksum_type,
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
        let trailer_checksum = request_trailer_checksum(&req, &scope.request_id)?;
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
            expires: input.expires.clone(),
        })
        .map_err(|e| map_error(e, &scope.request_id))?;
        let user_metadata = to_user_metadata(&input.metadata, &scope.request_id)?;
        let preconditions = write_preconditions(&input.if_match, &input.if_none_match)
            .map_err(|e| map_error(e, &scope.request_id))?;

        let content_md5 = match &input.content_md5 {
            Some(value) => Some(crate::integrity::decode_base64(value).map_err(|e| {
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
            trailer_checksum,
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
            checksum_md5: common.checksums.md5,
            checksum_crc32: common.checksums.crc32,
            checksum_crc32c: common.checksums.crc32c,
            checksum_crc64nvme: common.checksums.crc64nvme,
            checksum_sha1: common.checksums.sha1,
            checksum_sha256: common.checksums.sha256,
            checksum_type: common.checksums.checksum_type,
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

        // A result that cannot be written as XML 1.0 must not be silently
        // broken; the client is asked for `encoding-type=url` instead.
        require_encodable(
            encoding.as_deref(),
            result
                .objects
                .iter()
                .map(|meta| meta.key.as_str().to_string())
                .chain(result.common_prefixes.iter().cloned())
                .chain(std::iter::once(input.prefix.clone().unwrap_or_default())),
            &scope.request_id,
        )?;

        let encode = |value: &str| encode_value(value, &encoding);

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
            expires: input.expires.clone(),
        })
        .map_err(|e| map_error(e, &scope.request_id))?;
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
            expires: input.expires.clone(),
        })
        .map_err(|e| map_error(e, &scope.request_id))?;
        let checksum = checksum_scheme(
            input.checksum_algorithm.as_ref(),
            input.checksum_type.as_ref(),
            &scope.request_id,
        )?;
        let session = self
            .service
            .create_multipart(
                &scope.ctx,
                input.bucket.as_str(),
                &key,
                scope.principal.as_deref(),
                content,
                to_user_metadata(&input.metadata, &scope.request_id)?,
                checksum,
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(input.bucket.clone()),
            key: Some(input.key.clone()),
            upload_id: Some(session.upload_id.as_str().to_string()),
            checksum_algorithm: checksum.map(|scheme| {
                s3s::dto::ChecksumAlgorithm::from_static(checksum_algorithm_wire(scheme.algorithm))
            }),
            checksum_type: checksum
                .map(|scheme| s3s::dto::ChecksumType::from_static(scheme.checksum_type.as_str())),
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
        let trailer = request_trailer_checksum(&req, &scope.request_id)?;
        let input = req.input;
        let key = parse_key(&input.key, &scope.request_id)?;
        let part_number = part_number(&input.part_number, &scope.request_id)?;
        let algorithm = input
            .checksum_algorithm
            .as_ref()
            .map(|value| checksum_algorithm_from(value.as_str(), &scope.request_id))
            .transpose()?;
        let declared = declared_checksum_header(
            &part_checksum_fields(
                input.checksum_crc32.as_deref(),
                input.checksum_crc32c.as_deref(),
                input.checksum_crc64nvme.as_deref(),
                input.checksum_sha1.as_deref(),
                input.checksum_sha256.as_deref(),
            ),
            &[
                input.checksum_sha512.as_deref(),
                input.checksum_xxhash128.as_deref(),
                input.checksum_xxhash3.as_deref(),
                input.checksum_xxhash64.as_deref(),
            ],
            &scope.request_id,
        )?
        .map(|(algorithm, value)| decode_checksum(algorithm, value, &scope.request_id))
        .transpose()?;

        let summary = self
            .service
            .upload_part(
                &scope.ctx,
                input.bucket.as_str(),
                &key,
                &input.upload_id,
                part_number,
                crate::service::multipart::PartChecksums {
                    algorithm,
                    declared,
                    trailer,
                },
                body_stream(input.body),
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let mut output = UploadPartOutput {
            e_tag: Some(s3s::dto::ETag::Strong(summary.etag.as_str().to_string())),
            ..Default::default()
        };
        set_part_checksum!(output, summary.checksum.as_ref());
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

        let mut part_result = CopyPartResult {
            e_tag: Some(s3s::dto::ETag::Strong(summary.etag.as_str().to_string())),
            last_modified: Some(to_timestamp(source_meta.last_modified)),
            ..Default::default()
        };
        set_part_checksum!(part_result, summary.checksum.as_ref());
        let output = UploadPartCopyOutput {
            copy_part_result: Some(part_result),
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
        // `ListParts` echoes the key back; it must be representable as XML 1.0
        // just like a listing result (spec §4.2).
        require_encodable(
            None,
            std::iter::once(key.as_str().to_string()),
            &scope.request_id,
        )?;
        let marker = input.part_number_marker.map(|m| m.clamp(0, 10_000) as u16);
        let limit = input.max_parts.unwrap_or(1000).clamp(1, 1000) as u32;

        let page = self
            .service
            .list_parts(
                &scope.ctx,
                input.bucket.as_str(),
                &key,
                &input.upload_id,
                marker,
                limit,
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let parts = page
            .parts
            .into_iter()
            .map(|p| {
                let mut part = S3Part {
                    part_number: Some(p.number as i32),
                    size: Some(p.size as i64),
                    e_tag: Some(s3s::dto::ETag::Strong(p.etag.as_str().to_string())),
                    last_modified: Some(to_timestamp(p.last_modified)),
                    ..Default::default()
                };
                set_part_checksum!(part, p.checksum.as_ref());
                part
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
        let encoding = input.encoding_type.as_ref().map(|e| e.as_str().to_string());
        if let Some(encoding) = &encoding
            && encoding != "url"
        {
            return Err(map_error(
                ServiceError::InvalidArgument(format!("unsupported encoding-type `{encoding}`")),
                &scope.request_id,
            ));
        }

        let page = self
            .service
            .list_uploads(
                &scope.ctx,
                input.bucket.as_str(),
                input.prefix.as_deref(),
                input.key_marker.as_deref(),
                input.upload_id_marker.as_deref(),
                limit,
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        require_encodable(
            encoding.as_deref(),
            page.uploads
                .iter()
                .map(|session| session.key.to_string())
                .chain(std::iter::once(input.prefix.clone().unwrap_or_default())),
            &scope.request_id,
        )?;

        let uploads = page
            .uploads
            .into_iter()
            .map(|session| S3MultipartUpload {
                key: Some(encode_value(session.key.as_str(), &encoding)),
                upload_id: Some(session.upload_id.as_str().to_string()),
                initiated: Some(to_timestamp(session.initiated)),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let output = ListMultipartUploadsOutput {
            bucket: Some(input.bucket.clone()),
            prefix: input.prefix.clone(),
            delimiter: input.delimiter.clone(),
            encoding_type: input.encoding_type.clone(),
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
            let checksum = declared_checksum_header(
                &part_checksum_fields(
                    part.checksum_crc32.as_deref(),
                    part.checksum_crc32c.as_deref(),
                    part.checksum_crc64nvme.as_deref(),
                    part.checksum_sha1.as_deref(),
                    part.checksum_sha256.as_deref(),
                ),
                &[
                    part.checksum_sha512.as_deref(),
                    part.checksum_xxhash128.as_deref(),
                    part.checksum_xxhash3.as_deref(),
                    part.checksum_xxhash64.as_deref(),
                ],
                &scope.request_id,
            )?
            .map(|(algorithm, value)| decode_checksum(algorithm, value, &scope.request_id))
            .transpose()?;
            requested.push(CompletionPart {
                number,
                etag,
                checksum,
            });
        }

        let declared = complete_checksum(&input, &scope.request_id)?;
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
                declared,
                preconditions,
            )
            .await
            .map_err(|e| map_error(e, &scope.request_id))?;

        let headers = ChecksumHeaders::from_checksums(&meta.checksums);
        let output = CompleteMultipartUploadOutput {
            bucket: Some(input.bucket.clone()),
            key: Some(input.key.clone()),
            location: Some(format!("/{}/{}", input.bucket, input.key)),
            e_tag: meta
                .etag
                .as_ref()
                .map(|t| s3s::dto::ETag::Strong(t.as_str().to_string())),
            checksum_crc32: headers.crc32,
            checksum_crc32c: headers.crc32c,
            checksum_crc64nvme: headers.crc64nvme,
            checksum_sha1: headers.sha1,
            checksum_sha256: headers.sha256,
            checksum_type: headers.checksum_type,
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
            .abort_multipart(&scope.ctx, input.bucket.as_str(), &key, &input.upload_id)
            .await
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
    fn every_service_error_code_is_a_known_s3_code() {
        // `map_error` parses the code name the service produced; without this
        // check a typo or a renamed code would silently degrade every response
        // of that variant to `InternalError`.
        for error in ServiceError::ALL {
            assert!(
                error.code().parse::<S3ErrorCode>().is_ok(),
                "`{}` is not a code the adapter understands",
                error.code()
            );
        }
    }

    #[test]
    fn http_dates_round_trip() {
        let raw = "Wed, 21 Oct 2015 07:28:00 GMT";
        let parsed = parse_http_date(raw).expect("a valid IMF-fixdate");
        assert_eq!(format_http_date(parsed), raw);
        // Anything the service cannot store is a request error, not a silently
        // dropped header (spec §5.4).
        assert!(parse_http_date("2015-10-21T07:28:00Z").is_none());
        assert!(parse_http_date("Wed, 32 Oct 2015 07:28:00 GMT").is_none());
        assert!(parse_http_date("Wed, 21 Oct 2015 07:28:00 UTC").is_none());
    }

    #[test]
    fn xml_1_0_invalid_keys_require_url_encoding() {
        assert!(is_xml_1_0_text("a/\u{1F600}\nb"));
        assert!(!is_xml_1_0_text("a\u{1}b"));
        assert!(require_encodable(None, ["a/b".to_string()], "req").is_ok());
        assert!(require_encodable(None, ["a\u{1}b".to_string()], "req").is_err());
        assert!(require_encodable(Some("url"), ["a\u{1}b".to_string()], "req").is_ok());
    }

    #[test]
    fn an_idle_request_body_is_a_client_error() {
        // A client that stops sending must see a client error, never a server
        // fault counted against the server (spec §9.1, §9.3).
        let error: s3s::StdError = Box::new(crate::timeouts::BodyTimeoutError::Idle);
        let mapped = super::body::classify_request_body_error(error);
        assert_eq!(
            ServiceError::from_driver(&mapped, true).code(),
            "InvalidRequest"
        );
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
