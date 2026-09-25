//! The S3 protocol adapter, built on the `s3s` candidate library.
//!
//! Responsibilities: routing (via `s3s`), XML, HTTP headers, the signature's
//! raw input and S3 error encoding. It must not contain backend path rules or
//! storage-product branches (`ADR-0007`).

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
use s3s::{S3, S3Request, S3Response, S3Result, StdError};

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
use crate::domain::{
    ChecksumAlgorithm, ChecksumType, ContentProperties, DriverContext, DriverError, EntityTag,
    Preconditions, RangeSpec, RequestId, UserMetadata,
};
use crate::driver::{MetadataDirective, ObjectByteStream};
use crate::integrity::base64;
use crate::service::multipart::{CompletionPart, DeclaredCompletionChecksum, MultipartChecksum};
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

// --- DTO conversion helpers ------------------------------------------------

fn to_timestamp(time: std::time::SystemTime) -> Timestamp {
    Timestamp::from(time)
}

fn to_system_time(ts: &Timestamp) -> std::time::SystemTime {
    std::time::SystemTime::from(time::OffsetDateTime::from(ts.clone()))
}

fn to_content_properties(input: &S3ContentInput) -> Result<ContentProperties, ServiceError> {
    // `Expires` is an object property that must survive the round trip (spec
    // §5.4). A value the service cannot store is rejected rather than dropped:
    // silently publishing an object without it would change what the client
    // asked for.
    let expires = match input.expires.as_deref() {
        None => None,
        Some(raw) => Some(parse_http_date(raw).ok_or_else(|| {
            ServiceError::InvalidArgument(
                "`Expires` must be an HTTP date in IMF-fixdate form \
                 (e.g. `Wed, 21 Oct 2015 07:28:00 GMT`)"
                    .into(),
            )
        })?),
    };
    Ok(ContentProperties {
        content_type: input.content_type.clone(),
        content_encoding: input.content_encoding.clone(),
        content_language: input.content_language.clone(),
        content_disposition: input.content_disposition.clone(),
        cache_control: input.cache_control.clone(),
        expires,
    })
}

/// The subset of content headers shared by `PutObject`/`CopyObject`.
struct S3ContentInput {
    content_type: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_disposition: Option<String>,
    cache_control: Option<String>,
    expires: Option<String>,
}

/// Formats a `SystemTime` as an RFC 7231 IMF-fixdate.
///
/// Only the components are used, so this needs no formatting feature: the shape
/// is fixed (`Sun, 06 Nov 1994 08:49:37 GMT`).
fn format_http_date(time: std::time::SystemTime) -> String {
    use time::{Month, OffsetDateTime, Weekday};
    let value = OffsetDateTime::from(time);
    let weekday = match value.weekday() {
        Weekday::Monday => "Mon",
        Weekday::Tuesday => "Tue",
        Weekday::Wednesday => "Wed",
        Weekday::Thursday => "Thu",
        Weekday::Friday => "Fri",
        Weekday::Saturday => "Sat",
        Weekday::Sunday => "Sun",
    };
    let month = match value.month() {
        Month::January => "Jan",
        Month::February => "Feb",
        Month::March => "Mar",
        Month::April => "Apr",
        Month::May => "May",
        Month::June => "Jun",
        Month::July => "Jul",
        Month::August => "Aug",
        Month::September => "Sep",
        Month::October => "Oct",
        Month::November => "Nov",
        Month::December => "Dec",
    };
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        weekday,
        value.day(),
        month,
        value.year(),
        value.hour(),
        value.minute(),
        value.second(),
    )
}

/// Parses the IMF-fixdate form of an HTTP date, the only form S3 clients send.
fn parse_http_date(value: &str) -> Option<std::time::SystemTime> {
    let (weekday, rest) = value.trim().split_once(',')?;
    if !matches!(
        weekday,
        "Mon" | "Tue" | "Wed" | "Thu" | "Fri" | "Sat" | "Sun"
    ) {
        return None;
    }
    let mut fields = rest.trim_start().split(' ');
    let day: u8 = fields.next()?.parse().ok()?;
    let month = match fields.next()? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i32 = fields.next()?.parse().ok()?;
    let mut clock = fields.next()?.split(':');
    let hour: u8 = clock.next()?.parse().ok()?;
    let minute: u8 = clock.next()?.parse().ok()?;
    let second: u8 = clock.next()?.parse().ok()?;
    if clock.next().is_some() || fields.next()? != "GMT" || fields.next().is_some() {
        return None;
    }
    use time::{Date, Month, PrimitiveDateTime, Time};
    let date = Date::from_calendar_date(year, Month::try_from(month).ok()?, day).ok()?;
    let time = Time::from_hms(hour, minute, second).ok()?;
    Some(std::time::SystemTime::from(
        PrimitiveDateTime::new(date, time).assume_utc(),
    ))
}

/// Whether `value` can appear as XML 1.0 character data.
///
/// XML 1.0 excludes most C0 control characters. A key containing one cannot be
/// serialised without producing a document XML parsers reject, so such a
/// listing asks the client for `encoding-type=url` instead (spec §4.2).
fn is_xml_1_0_text(value: &str) -> bool {
    value.chars().all(|c| {
        matches!(c, '\u{9}' | '\u{A}' | '\u{D}')
            || ('\u{20}'..='\u{D7FF}').contains(&c)
            || ('\u{E000}'..='\u{FFFD}').contains(&c)
            || ('\u{10000}'..='\u{10FFFF}').contains(&c)
    })
}

/// Rejects a listing whose result could not be represented in XML 1.0.
///
/// The alternative — emitting a broken document or rewriting the key — is
/// explicitly refused by spec §4.2.
fn require_encodable(
    encoding: Option<&str>,
    values: impl IntoIterator<Item = String>,
    request_id: &str,
) -> S3Result<()> {
    if encoding.is_some() {
        return Ok(());
    }
    if values.into_iter().any(|value| !is_xml_1_0_text(&value)) {
        return Err(map_error(
            ServiceError::InvalidRequest(
                "the listing contains characters that XML 1.0 cannot represent; \
                 retry with `encoding-type=url`"
                    .into(),
            ),
            request_id,
        ));
    }
    Ok(())
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
    out.expires = meta.content.expires.map(format_http_date);

    out.checksums = ChecksumHeaders::from_checksums(&meta.checksums);
}

/// The wire form of an object's checksums (spec §6.3).
///
/// A composite checksum is `base64(digest)-<partcount>`; a full-object one has
/// no suffix. The coverage is published alongside so a client can tell them
/// apart instead of guessing from the algorithm.
#[derive(Default)]
struct ChecksumHeaders {
    md5: Option<String>,
    crc32: Option<String>,
    crc32c: Option<String>,
    crc64nvme: Option<String>,
    sha1: Option<String>,
    sha256: Option<String>,
    checksum_type: Option<s3s::dto::ChecksumType>,
}

impl ChecksumHeaders {
    fn from_checksums(checksums: &[crate::domain::Checksum]) -> Self {
        use crate::domain::ChecksumAlgorithm as Alg;
        let mut headers = Self::default();
        for checksum in checksums {
            let mut value = base64(checksum.value());
            if let Some(count) = checksum.part_count() {
                value = format!("{value}-{count}");
            }
            match checksum.algorithm() {
                Alg::Md5 => headers.md5 = Some(value),
                Alg::Crc32 => headers.crc32 = Some(value),
                Alg::Crc32c => headers.crc32c = Some(value),
                Alg::Crc64Nvme => headers.crc64nvme = Some(value),
                Alg::Sha1 => headers.sha1 = Some(value),
                Alg::Sha256 => headers.sha256 = Some(value),
            }
            headers.checksum_type = Some(s3s::dto::ChecksumType::from_static(
                checksum.checksum_type().as_str(),
            ));
        }
        headers
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
    expires: Option<String>,
    checksums: ChecksumHeaders,
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
            expires: None,
            checksums: ChecksumHeaders::default(),
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
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(classify_request_body_error(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Wraps a body-bridge error so it can be kept as an internal cause.
///
/// [`DriverError::with_cause`] takes a sized error; the bridge yields a boxed
/// one, and this keeps the library's text out of the client's response while
/// leaving it reachable through `Error::source`.
struct BodyBridgeError(StdError);

impl std::fmt::Debug for BodyBridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for BodyBridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for BodyBridgeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Classifies a failure of the request body itself.
///
/// The bytes never reached a driver, so the outcome is a client-visible one and
/// must not be reported as a server fault (spec §9.1): a signed chunk or trailer
/// that does not verify is the corresponding signature error, framing the server
/// cannot parse is an invalid request, and a body over the configured limit is
/// too large. The library's own error is kept as the internal cause — never as
/// the client's message, which must not leak backend detail (spec §10).
fn classify_request_body_error(error: StdError) -> DriverError {
    // A body that went idle is the client's fault, not a server fault: it must
    // never be reported as an internal error (spec §9.1, §9.3).
    if crate::timeouts::is_body_idle(error.as_ref()) {
        return DriverError::cancelled(
            "the request body was idle for longer than the configured body idle timeout",
        )
        .with_cause(BodyBridgeError(error));
    }
    if let Some(chunked) =
        error.downcast_ref::<s3s::stream::aws_chunked_stream::AwsChunkedStreamError>()
    {
        return match chunked.to_s3_error_code() {
            s3s::S3ErrorCode::SignatureDoesNotMatch => DriverError::payload_signature_mismatch(
                "the signature of at least one aws-chunked chunk or trailer did not match",
            )
            .with_cause(BodyBridgeError(error)),
            s3s::S3ErrorCode::EntityTooLarge => DriverError::resource_exhausted(
                "a chunk of the request body exceeds the configured maximum chunk size",
            )
            .with_cause(BodyBridgeError(error)),
            s3s::S3ErrorCode::InternalError => {
                DriverError::internal("the request body could not be read")
                    .with_cause(BodyBridgeError(error))
            }
            _ => DriverError::invalid_argument("the request body is not valid aws-chunked framing")
                .with_cause(BodyBridgeError(error)),
        };
    }
    if let Some(payload) = error.downcast_ref::<s3s::stream::upload_stream::UploadStreamError>() {
        return match payload.to_s3_error_code() {
            s3s::S3ErrorCode::BadDigest => DriverError::payload_digest_mismatch(
                "the body you sent did not match the digest you declared for it",
            )
            .with_cause(BodyBridgeError(error)),
            s3s::S3ErrorCode::InternalError => {
                DriverError::internal("the request body could not be read")
                    .with_cause(BodyBridgeError(error))
            }
            _ => DriverError::invalid_argument(
                "the request body did not match the length you declared for it",
            )
            .with_cause(BodyBridgeError(error)),
        };
    }
    if error.downcast_ref::<s3s::BodySizeLimitExceeded>().is_some() {
        return DriverError::resource_exhausted(
            "the request body exceeds the configured maximum size",
        )
        .with_cause(BodyBridgeError(error));
    }
    // Anything else is a transport or server-side failure: keep it internal and
    // keep the library's text out of the client's response.
    DriverError::internal("the request body could not be read").with_cause(BodyBridgeError(error))
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
                input.bucket.as_str(),
                &key,
                scope.principal.as_deref(),
                content,
                to_user_metadata(&input.metadata, &scope.request_id)?,
                checksum,
            )
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
            .list_parts(input.bucket.as_str(), &key, &input.upload_id, marker, limit)
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
                input.bucket.as_str(),
                input.prefix.as_deref(),
                input.key_marker.as_deref(),
                input.upload_id_marker.as_deref(),
                limit,
            )
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
    crate::integrity::decode_base64(value)
}

/// The checksum a request declares it will send in a trailer.
///
/// `s3s` parses `x-amz-trailer` and exposes the trailing headers, but leaves
/// their verification to the handler, so the service is given a reader and
/// re-checks the digest once the body has been consumed (spec §6.3). A request
/// that declares a trailer must deliver it: a body that never produces one (no
/// trailing-headers handle, or no value) is reported as an absent declaration,
/// not skipped.
fn request_trailer_checksum<T>(
    request: &S3Request<T>,
    request_id: &str,
) -> S3Result<Option<crate::service::TrailerChecksum>> {
    let Some(name) = declared_trailer_checksum(&request.headers) else {
        return Ok(None);
    };
    let handle = request.trailing_headers.clone();
    let Some(algorithm) = checksum_algorithm_wire_name(&name) else {
        return Err(map_error(
            ServiceError::NotImplemented(format!("unsupported checksum trailer `{name}`")),
            request_id,
        ));
    };
    let header = match hyper::header::HeaderName::from_bytes(name.as_bytes()) {
        Ok(header) => header,
        Err(_) => {
            return Err(map_error(
                ServiceError::InvalidArgument(format!("invalid trailer name `{name}`")),
                request_id,
            ));
        }
    };
    Ok(Some(crate::service::TrailerChecksum::new(
        algorithm,
        move || {
            handle.as_ref()?.read(|headers| {
                headers
                    .get(&header)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
            })?
        },
    )))
}

/// The `x-amz-checksum-*` name listed in `x-amz-trailer`, if any.
fn declared_trailer_checksum(headers: &hyper::HeaderMap) -> Option<String> {
    let value = headers.get("x-amz-trailer")?.to_str().ok()?;
    value
        .split(',')
        .map(str::trim)
        .find(|name| name.to_ascii_lowercase().starts_with("x-amz-checksum-"))
        .map(|name| name.to_ascii_lowercase())
}

/// The algorithm a `x-amz-checksum-*` header name selects.
fn checksum_algorithm_wire_name(name: &str) -> Option<ChecksumAlgorithm> {
    match name.strip_prefix("x-amz-checksum-")? {
        "crc32" => Some(ChecksumAlgorithm::Crc32),
        "crc32c" => Some(ChecksumAlgorithm::Crc32c),
        "crc64nvme" => Some(ChecksumAlgorithm::Crc64Nvme),
        "sha1" => Some(ChecksumAlgorithm::Sha1),
        "sha256" => Some(ChecksumAlgorithm::Sha256),
        _ => None,
    }
}

/// A checksum algorithm together with the raw value the client declared for it.
type DeclaredChecksum = (ChecksumAlgorithm, Vec<u8>);

/// The `x-amz-checksum-*` header fields shared by part-level requests, in the
/// order they are checked.
fn part_checksum_fields<'a>(
    crc32: Option<&'a str>,
    crc32c: Option<&'a str>,
    crc64nvme: Option<&'a str>,
    sha1: Option<&'a str>,
    sha256: Option<&'a str>,
) -> [(ChecksumAlgorithm, Option<&'a str>); 5] {
    [
        (ChecksumAlgorithm::Crc32, crc32),
        (ChecksumAlgorithm::Crc32c, crc32c),
        (ChecksumAlgorithm::Crc64Nvme, crc64nvme),
        (ChecksumAlgorithm::Sha1, sha1),
        (ChecksumAlgorithm::Sha256, sha256),
    ]
}

/// Finds the one checksum the client declared, rejecting algorithms this server
/// does not implement rather than silently ignoring them.
fn declared_checksum_header<'a>(
    declared: &[(ChecksumAlgorithm, Option<&'a str>)],
    unsupported: &[Option<&'a str>],
    request_id: &str,
) -> S3Result<Option<(ChecksumAlgorithm, &'a str)>> {
    if unsupported.iter().any(Option::is_some) {
        return Err(map_error(
            ServiceError::NotImplemented("unsupported checksum algorithm".into()),
            request_id,
        ));
    }
    Ok(declared
        .iter()
        .find_map(|(algorithm, value)| value.map(|value| (*algorithm, value))))
}

/// Decodes a declared checksum value (standard base64, no part-count suffix).
fn decode_checksum(
    algorithm: ChecksumAlgorithm,
    value: &str,
    request_id: &str,
) -> S3Result<DeclaredChecksum> {
    let bytes = decode_base64(value).map_err(|e| {
        map_error(
            ServiceError::InvalidArgument(format!("invalid checksum: {e}")),
            request_id,
        )
    })?;
    Ok((algorithm, bytes))
}

/// Builds the multipart checksum scheme a create request asked for.
fn checksum_scheme(
    algorithm: Option<&s3s::dto::ChecksumAlgorithm>,
    checksum_type: Option<&s3s::dto::ChecksumType>,
    request_id: &str,
) -> S3Result<Option<MultipartChecksum>> {
    let Some(algorithm) = algorithm else {
        if checksum_type.is_some() {
            return Err(map_error(
                ServiceError::InvalidArgument(
                    "a checksum type requires a checksum algorithm".into(),
                ),
                request_id,
            ));
        }
        return Ok(None);
    };
    let algorithm = checksum_algorithm_from(algorithm.as_str(), request_id)?;
    let checksum_type = match checksum_type {
        Some(value) => checksum_type_from(value.as_str(), request_id)?,
        None => algorithm.default_multipart_type(),
    };
    MultipartChecksum::new(algorithm, checksum_type)
        .map(Some)
        .map_err(|e| map_error(e, request_id))
}

/// The whole-object checksum a completion request declared.
///
/// A composite value arrives as `base64(digest)-<partcount>`, so the count is
/// split off before the digest is decoded; the digest is what gets compared.
fn complete_checksum(
    input: &s3s::dto::CompleteMultipartUploadInput,
    request_id: &str,
) -> S3Result<Option<DeclaredCompletionChecksum>> {
    let found = declared_checksum_header(
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
        request_id,
    )?;
    let checksum_type = input
        .checksum_type
        .as_ref()
        .map(|value| checksum_type_from(value.as_str(), request_id))
        .transpose()?;
    // A request may declare only the coverage; that constrains the checksum the
    // service must produce without naming a value to compare against.
    let (raw_algorithm, raw_value) = match found {
        Some(found) => found,
        None => {
            return Ok(
                checksum_type.map(|checksum_type| DeclaredCompletionChecksum {
                    algorithm: None,
                    value: None,
                    checksum_type: Some(checksum_type),
                    part_count: None,
                }),
            );
        }
    };
    let (value, part_count) = split_part_count(raw_value);
    let (algorithm, value) = decode_checksum(raw_algorithm, value, request_id)?;
    Ok(Some(DeclaredCompletionChecksum {
        algorithm: Some(algorithm),
        value: Some(value),
        checksum_type,
        part_count,
    }))
}

/// Splits the `-<partcount>` suffix off a composite checksum value.
fn split_part_count(value: &str) -> (&str, Option<u32>) {
    match value.rsplit_once('-') {
        Some((digest, count)) => match count.parse::<u32>() {
            Ok(count) => (digest, Some(count)),
            Err(_) => (value, None),
        },
        None => (value, None),
    }
}

/// The wire name of a checksum algorithm.
fn checksum_algorithm_wire(algorithm: ChecksumAlgorithm) -> &'static str {
    match algorithm {
        ChecksumAlgorithm::Md5 => "MD5",
        ChecksumAlgorithm::Crc32 => "CRC32",
        ChecksumAlgorithm::Crc32c => "CRC32C",
        ChecksumAlgorithm::Crc64Nvme => "CRC64NVME",
        ChecksumAlgorithm::Sha1 => "SHA1",
        ChecksumAlgorithm::Sha256 => "SHA256",
    }
}

fn checksum_type_from(value: &str, request_id: &str) -> S3Result<ChecksumType> {
    match value {
        "COMPOSITE" => Ok(ChecksumType::Composite),
        "FULL_OBJECT" => Ok(ChecksumType::FullObject),
        other => Err(map_error(
            ServiceError::InvalidArgument(format!("unsupported checksum type `{other}`")),
            request_id,
        )),
    }
}

/// Extracts a requested checksum algorithm and any client-declared value.
fn parse_checksums(
    input: &s3s::dto::PutObjectInput,
    request_id: &str,
) -> S3Result<(
    Option<crate::domain::ChecksumAlgorithm>,
    Option<DeclaredChecksum>,
)> {
    let algorithm = input
        .checksum_algorithm
        .as_ref()
        .map(|a| checksum_algorithm_from(a.as_str(), request_id))
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
        request_id,
    )?
    .map(|(algorithm, value)| decode_checksum(algorithm, value, request_id))
    .transpose()?;

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

/// Encodes a listing value for the response's requested `encoding-type`.
fn encode_value(value: &str, encoding: &Option<String>) -> String {
    if encoding.as_deref() == Some("url") {
        url_encode_key(value)
    } else {
        value.to_string()
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
        let error: StdError = Box::new(crate::timeouts::BodyTimeoutError::Idle);
        let mapped = classify_request_body_error(error);
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
