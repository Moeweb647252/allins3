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
    /// The object or part exceeds the configured or declared size (`413`).
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

impl ServiceError {
    /// The S3 error code name.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::AccessDenied => "AccessDenied",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::NotModified => "NotModified",
            Self::ConditionalConflict => "ConditionalRequestConflict",
            Self::InvalidRange => "InvalidRange",
            Self::InvalidArgument(_) => "InvalidArgument",
            Self::InvalidRequest(_) => "InvalidRequest",
            Self::MalformedXml(_) => "MalformedXML",
            Self::NotImplemented(_) => "NotImplemented",
            Self::BadDigest(_) => "BadDigest",
            Self::EntityTooLarge(_) => "EntityTooLarge",
            Self::ServiceUnavailable => "ServiceUnavailable",
            Self::SlowDown => "SlowDown",
            Self::Internal(_) => "InternalError",
        }
    }

    /// A client-safe message.
    pub fn message(&self) -> String {
        match self {
            Self::NoSuchBucket => "The specified bucket does not exist".to_string(),
            Self::NoSuchKey => "The specified key does not exist".to_string(),
            Self::AccessDenied => "Access Denied".to_string(),
            Self::PreconditionFailed => {
                "At least one of the pre-conditions you specified did not hold".to_string()
            }
            Self::NotModified => "Not Modified".to_string(),
            Self::ConditionalConflict => {
                "The conditional request conflicted with a concurrent change".to_string()
            }
            Self::InvalidRange => "The requested range is not satisfiable".to_string(),
            Self::InvalidArgument(m)
            | Self::InvalidRequest(m)
            | Self::MalformedXml(m)
            | Self::NotImplemented(m)
            | Self::BadDigest(m)
            | Self::EntityTooLarge(m)
            | Self::Internal(m) => m.clone(),
            Self::ServiceUnavailable => {
                "The object changed too many times while the request was being served".to_string()
            }
            Self::SlowDown => "Please reduce your request rate".to_string(),
        }
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
