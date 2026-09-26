//! Byte-stream bridging between the driver interface and the protocol library.
//!
//! The driver speaks [`ObjectByteStream`] (boxed, `DriverError`), the library
//! speaks `StreamingBlob` (boxed, `StdError`). The two adapters here are the
//! only place that translation happens, and [`classify_request_body_error`]
//! keeps a request-body failure a *client-visible* one rather than a server
//! fault (spec §9.1).

use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use bytes::Bytes;
use s3s::StdError;
use s3s::dto::StreamingBlob;
use s3s::stream::{ByteStream, RemainingLength};

use crate::domain::DriverError;
use crate::driver::ObjectByteStream;

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

pub(super) fn to_blob(stream: ObjectByteStream) -> StreamingBlob {
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
pub(super) fn classify_request_body_error(error: StdError) -> DriverError {
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

pub(super) fn body_stream(body: Option<StreamingBlob>) -> ObjectByteStream {
    match body {
        Some(blob) => Box::pin(InputStream { inner: blob }),
        None => Box::pin(EmptyStream),
    }
}
