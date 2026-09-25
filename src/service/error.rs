//! Service-level errors.
//!
//! Drivers return [`DriverError`](crate::domain::DriverError); the service
//! translates those into these outcomes, and the protocol layer maps them to S3
//! error responses (spec §9.1). The service never returns an HTTP status.

use crate::domain::DriverError;

/// A failed object-service operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServiceError {
    /// The logical bucket is not configured.
    #[error("bucket does not exist")]
    NoSuchBucket,
    /// The key does not exist.
    #[error("key does not exist")]
    NoSuchKey,
    /// The caller is not permitted to perform the operation.
    #[error("access denied")]
    AccessDenied,
    /// A precondition was not satisfied (`412`).
    #[error("precondition failed")]
    PreconditionFailed,
    /// A signature over the request payload did not verify (`403`).
    #[error("payload signature mismatch")]
    SignatureMismatch,
    /// A conditional read observed an unchanged object (`304`).
    #[error("not modified")]
    NotModified,
    /// A concurrent conditional mutation lost the race (`409`).
    #[error("conditional request conflict")]
    ConditionalConflict,
    /// The requested range is not satisfiable (`416`).
    #[error("invalid range")]
    InvalidRange,
    /// The request arguments are invalid (`400 InvalidArgument`).
    #[error("{0}")]
    InvalidArgument(String),
    /// The request is malformed (`400 InvalidRequest`).
    #[error("{0}")]
    InvalidRequest(String),
    /// The request XML is malformed (`400 MalformedXML`).
    #[error("{0}")]
    MalformedXml(String),
    /// A requested feature is not supported (`501`).
    #[error("{0}")]
    NotImplemented(String),
    /// An integrity check failed (`400 BadDigest`).
    #[error("{0}")]
    BadDigest(String),
    /// The object or part exceeds the configured or declared size
    /// (`400 EntityTooLarge`: the S3 status for this code, see the compatibility
    /// matrix's error mapping).
    #[error("{0}")]
    EntityTooLarge(String),
    /// A revision could not be re-read within budget (`503`).
    #[error("the object changed repeatedly while being read")]
    ServiceUnavailable,
    /// The backend is throttling or temporarily unavailable (`503`).
    #[error("temporarily unavailable")]
    SlowDown,
    /// An internal invariant failed (`500`).
    #[error("{0}")]
    Internal(String),
}

/// The wire form of a [`ServiceError`]: its S3 code name and its client-safe
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorShape {
    /// The S3 error code name (`NoSuchKey`, `SlowDown`, …).
    pub code: &'static str,
    /// A client-safe message.
    pub message: String,
}

impl ServiceError {
    /// The variants, so the protocol layer can check that every code it can
    /// produce is a code the adapter understands.
    pub const ALL: [Self; 17] = [
        Self::NoSuchBucket,
        Self::NoSuchKey,
        Self::AccessDenied,
        Self::PreconditionFailed,
        Self::SignatureMismatch,
        Self::NotModified,
        Self::ConditionalConflict,
        Self::InvalidRange,
        Self::InvalidArgument(String::new()),
        Self::InvalidRequest(String::new()),
        Self::MalformedXml(String::new()),
        Self::NotImplemented(String::new()),
        Self::BadDigest(String::new()),
        Self::EntityTooLarge(String::new()),
        Self::ServiceUnavailable,
        Self::SlowDown,
        Self::Internal(String::new()),
    ];

    /// The error's code and message, decided together.
    ///
    /// One match rather than two parallel exhaustive matches over the same
    /// variants: a variant added to only one of them would still compile, and
    /// the two would silently disagree about what the client is told.
    pub fn describe(&self) -> ErrorShape {
        use std::borrow::Cow;
        let (code, message): (&'static str, Cow<'_, str>) = match self {
            Self::NoSuchBucket => (
                "NoSuchBucket",
                Cow::Borrowed("The specified bucket does not exist"),
            ),
            Self::NoSuchKey => (
                "NoSuchKey",
                Cow::Borrowed("The specified key does not exist"),
            ),
            Self::AccessDenied => ("AccessDenied", Cow::Borrowed("Access Denied")),
            Self::PreconditionFailed => (
                "PreconditionFailed",
                Cow::Borrowed("At least one of the pre-conditions you specified did not hold"),
            ),
            Self::SignatureMismatch => (
                "SignatureDoesNotMatch",
                Cow::Borrowed(
                    "The request signature we calculated does not match the signature you \
                     provided. Check your key and signing method",
                ),
            ),
            Self::NotModified => ("NotModified", Cow::Borrowed("Not Modified")),
            Self::ConditionalConflict => (
                "ConditionalRequestConflict",
                Cow::Borrowed("The conditional request conflicted with a concurrent change"),
            ),
            Self::InvalidRange => (
                "InvalidRange",
                Cow::Borrowed("The requested range is not satisfiable"),
            ),
            Self::InvalidArgument(m) => ("InvalidArgument", Cow::Borrowed(m)),
            Self::InvalidRequest(m) => ("InvalidRequest", Cow::Borrowed(m)),
            Self::MalformedXml(m) => ("MalformedXML", Cow::Borrowed(m)),
            Self::NotImplemented(m) => ("NotImplemented", Cow::Borrowed(m)),
            Self::BadDigest(m) => ("BadDigest", Cow::Borrowed(m)),
            Self::EntityTooLarge(m) => ("EntityTooLarge", Cow::Borrowed(m)),
            Self::ServiceUnavailable => (
                "ServiceUnavailable",
                Cow::Borrowed(
                    "The object changed too many times while the request was being served",
                ),
            ),
            Self::SlowDown => ("SlowDown", Cow::Borrowed("Please reduce your request rate")),
            Self::Internal(m) => ("InternalError", Cow::Borrowed(m)),
        };
        ErrorShape {
            code,
            message: message.into_owned(),
        }
    }

    /// The S3 error code name.
    pub fn code(&self) -> &'static str {
        self.describe().code
    }

    /// A client-safe message.
    pub fn message(&self) -> String {
        self.describe().message
    }

    /// Maps a driver error onto a service error, preserving commit certainty in
    /// the message where relevant.
    pub fn from_driver(err: &DriverError, key_absent_is_no_such_key: bool) -> Self {
        use crate::domain::{CommitState, DriverErrorKind};

        match err.kind() {
            DriverErrorKind::NotFound => {
                if key_absent_is_no_such_key {
                    Self::NoSuchKey
                } else {
                    Self::NoSuchBucket
                }
            }
            DriverErrorKind::PreconditionFailed => Self::PreconditionFailed,
            DriverErrorKind::PayloadSignatureMismatch => Self::SignatureMismatch,
            DriverErrorKind::PayloadDigestMismatch => Self::BadDigest(
                "the body you sent did not match the digest you declared for it".to_string(),
            ),
            DriverErrorKind::ConditionalConflict => Self::ConditionalConflict,
            DriverErrorKind::AccessDenied => Self::AccessDenied,
            DriverErrorKind::InvalidArgument => Self::InvalidArgument(err.message().to_string()),
            DriverErrorKind::RevisionUnavailable => Self::ServiceUnavailable,
            DriverErrorKind::NotSupported => Self::NotImplemented(err.message().to_string()),
            DriverErrorKind::ResourceExhausted => Self::SlowDown,
            DriverErrorKind::Throttled => Self::SlowDown,
            DriverErrorKind::TemporarilyUnavailable => Self::SlowDown,
            DriverErrorKind::Internal => {
                if matches!(err.commit_state(), Some(CommitState::Unknown)) {
                    Self::Internal("the outcome of the write could not be determined".to_string())
                } else {
                    Self::Internal(err.message().to_string())
                }
            }
            // A cancelled request is not a server fault, and it must never be
            // reported as a completed write.
            DriverErrorKind::Cancelled => {
                Self::InvalidRequest("the request was cancelled before it completed".to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{CommitState, DriverError};

    #[test]
    fn unknown_commit_is_internal_not_silent_retry() {
        let err = DriverError::commit_outcome_unknown("lost");
        let mapped = ServiceError::from_driver(&err, true);
        assert_eq!(mapped.code(), "InternalError");
        assert!(mapped.message().contains("outcome"));
    }

    #[test]
    fn revision_unavailable_maps_to_503_not_412() {
        let err = DriverError::revision_unavailable("stale");
        assert_eq!(
            ServiceError::from_driver(&err, true),
            ServiceError::ServiceUnavailable
        );
    }

    #[test]
    fn not_found_maps_by_context() {
        let err = DriverError::not_found("x");
        assert_eq!(
            ServiceError::from_driver(&err, true),
            ServiceError::NoSuchKey
        );
        assert_eq!(
            ServiceError::from_driver(&err, false),
            ServiceError::NoSuchBucket
        );
    }

    #[test]
    fn commit_state_unknown_is_not_reported_as_committed() {
        let err = DriverError::internal("boom").with_commit_state(CommitState::Unknown);
        let mapped = ServiceError::from_driver(&err, true);
        assert_eq!(mapped.code(), "InternalError");
    }
}
