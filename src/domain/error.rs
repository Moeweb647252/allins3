//! The driver error model.
//!
//! Drivers never return HTTP status codes. They return a stable
//! [`DriverErrorKind`], an optional [`CommitState`] for mutating operations, a
//! short message that is safe to surface, an optional internal cause, and a
//! [`RetryHint`]. The protocol layer combines the kind with the operation and
//! the caller's permissions to choose the final S3 error (spec §9.1).

use std::error::Error as StdError;
use std::fmt;

/// Stable classification of a driver failure.
///
/// Backend SDK errors must be translated into one of these kinds; SDK types
/// never cross the driver boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DriverErrorKind {
    /// The requested object, namespace or bucket does not exist.
    NotFound,
    /// A precondition (`If-Match` / `If-None-Match`) was not satisfied.
    PreconditionFailed,
    /// A concurrent conditional operation was rejected; the caller should not
    /// blindly retry with different semantics.
    ConditionalConflict,
    /// The caller is not permitted to perform the operation.
    AccessDenied,
    /// The request arguments were invalid.
    InvalidArgument,
    /// The revision captured by an earlier `stat` can no longer be read. The
    /// service layer may re-run the whole condition + range flow within budget;
    /// this must never be surfaced as a client precondition failure.
    RevisionUnavailable,
    /// The driver cannot satisfy the request with the declared capabilities.
    NotSupported,
    /// A local resource limit (quota, buffer, connection) was hit.
    ResourceExhausted,
    /// The backend is throttling the request.
    Throttled,
    /// The backend is temporarily unavailable. Availability is not a capability.
    TemporarilyUnavailable,
    /// An internal invariant failed.
    Internal,
    /// The operation was cancelled and did not complete.
    Cancelled,
}

impl DriverErrorKind {
    /// A short, stable, machine-friendly label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::PreconditionFailed => "precondition_failed",
            Self::ConditionalConflict => "conditional_conflict",
            Self::AccessDenied => "access_denied",
            Self::InvalidArgument => "invalid_argument",
            Self::RevisionUnavailable => "revision_unavailable",
            Self::NotSupported => "not_supported",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Throttled => "throttled",
            Self::TemporarilyUnavailable => "temporarily_unavailable",
            Self::Internal => "internal",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the error is an availability failure rather than a capability
    /// loss. Availability failures are transient and never change a namespace's
    /// declared capabilities.
    pub fn is_transient(self) -> bool {
        matches!(self, Self::Throttled | Self::TemporarilyUnavailable)
    }
}

/// Determinism of a mutating attempt, as observed by the driver.
///
/// This is a four-state model; a lone `retryable: bool` cannot express it
/// (spec §9.2). A response that is merely missing does not imply
/// [`CommitState::NotCommittedFinal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommitState {
    /// The attempt published; later overwrites or deletes do not change this
    /// historical result. The original response may be replayed, but the data
    /// must not be published again.
    Committed,
    /// The attempt never published and can never publish later, because it has
    /// terminated or been fenced at the actual publish point. Irreversible.
    NotCommittedFinal,
    /// The attempt is still running or may still complete publishing. Wait or
    /// query; do not start a replacement commit.
    InFlight,
    /// Success or final non-commit cannot be proven, including missing,
    /// expired or failed status lookups. Keep a pending record; do not start a
    /// replacement commit.
    Unknown,
}

impl CommitState {
    /// Whether a replacement attempt may be created under this state.
    pub fn permits_replacement_attempt(self) -> bool {
        matches!(self, Self::NotCommittedFinal)
    }

    /// Whether the original response can be restored without publishing again.
    pub fn is_recoverable_success(self) -> bool {
        matches!(self, Self::Committed)
    }
}

/// Advice on whether and how the caller may retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetryHint {
    /// Do not retry.
    Never,
    /// Safe to retry the operation with a fresh attempt.
    Transient,
    /// Retry only after reconciling the commit outcome via
    /// [`CommitRecovery`](crate::driver::CommitRecovery).
    RequiresRecoveryQuery,
}

/// An error returned by a driver.
///
/// `message` is a short summary that is safe to include in internal logs and,
/// after protocol mapping, in some client responses. Backend details belong in
/// `cause`, which is never serialised to a client.
pub struct DriverError {
    kind: DriverErrorKind,
    message: String,
    commit_state: Option<CommitState>,
    retry: RetryHint,
    cause: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl DriverError {
    /// Builds an error. Commit state defaults to `None`, retry hint to
    /// [`RetryHint::Never`].
    pub fn new(kind: DriverErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            commit_state: None,
            retry: RetryHint::Never,
            cause: None,
        }
    }

    /// Attaches a commit state.
    pub fn with_commit_state(mut self, state: CommitState) -> Self {
        self.commit_state = Some(state);
        self
    }

    /// Attaches a retry hint.
    pub fn with_retry(mut self, retry: RetryHint) -> Self {
        self.retry = retry;
        self
    }

    /// Attaches an internal cause.
    pub fn with_cause(mut self, cause: impl StdError + Send + Sync + 'static) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    /// The error kind.
    pub fn kind(&self) -> DriverErrorKind {
        self.kind
    }

    /// A short, safe message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The observed commit state, if applicable.
    pub fn commit_state(&self) -> Option<CommitState> {
        self.commit_state
    }

    /// The retry hint.
    pub fn retry_hint(&self) -> RetryHint {
        self.retry
    }

    /// The commit state, inferred from the error kind when the driver did not
    /// attach one explicitly.
    ///
    /// A driver must attach the observed state; this fallback only classifies
    /// errors that provably mean "nothing was published" as
    /// [`CommitState::NotCommittedFinal`] and stays [`CommitState::Unknown`]
    /// otherwise, so an unknown outcome is never mistaken for a final failure.
    pub fn commit_state_or_infer(&self) -> CommitState {
        if let Some(state) = self.commit_state {
            return state;
        }
        match self.kind {
            DriverErrorKind::PreconditionFailed
            | DriverErrorKind::ConditionalConflict
            | DriverErrorKind::AccessDenied
            | DriverErrorKind::InvalidArgument
            | DriverErrorKind::NotSupported
            | DriverErrorKind::NotFound
            | DriverErrorKind::ResourceExhausted
            | DriverErrorKind::Cancelled => CommitState::NotCommittedFinal,
            DriverErrorKind::RevisionUnavailable
            | DriverErrorKind::Throttled
            | DriverErrorKind::TemporarilyUnavailable
            | DriverErrorKind::Internal => CommitState::Unknown,
        }
    }

    /// Whether the caller may retry the operation.
    pub fn is_retryable(&self) -> bool {
        !matches!(self.retry, RetryHint::Never)
    }

    // --- Common constructors -------------------------------------------------

    /// The object, namespace or bucket was not found.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::NotFound, message)
    }

    /// A precondition was not satisfied.
    pub fn precondition_failed(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::PreconditionFailed, message)
    }

    /// A concurrent conditional operation lost the race.
    pub fn conditional_conflict(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::ConditionalConflict, message)
    }

    /// The caller is not permitted to perform the operation.
    pub fn access_denied(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::AccessDenied, message)
    }

    /// The arguments were invalid.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::InvalidArgument, message)
    }

    /// The checked revision can no longer be read.
    pub fn revision_unavailable(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::RevisionUnavailable, message)
    }

    /// The driver does not support the requested operation.
    pub fn not_supported(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::NotSupported, message)
    }

    /// A local resource limit was hit.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::ResourceExhausted, message)
    }

    /// The backend is throttling.
    pub fn throttled(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::Throttled, message).with_retry(RetryHint::Transient)
    }

    /// The backend is temporarily unavailable.
    pub fn temporarily_unavailable(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::TemporarilyUnavailable, message).with_retry(RetryHint::Transient)
    }

    /// An internal invariant failed.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::Internal, message)
    }

    /// The operation was cancelled and did not complete.
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::Cancelled, message)
    }

    /// A mutating operation whose commit outcome is unknown.
    ///
    /// This is the only correct way to report a lost commit acknowledgement.
    /// Callers must reconcile via [`CommitRecovery`](crate::driver::CommitRecovery)
    /// and must not start a replacement commit.
    pub fn commit_outcome_unknown(message: impl Into<String>) -> Self {
        Self::new(DriverErrorKind::Internal, message)
            .with_commit_state(CommitState::Unknown)
            .with_retry(RetryHint::RequiresRecoveryQuery)
    }
}

impl fmt::Debug for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately excludes the internal cause, which may contain backend
        // detail that must not leak into logs by accident.
        f.debug_struct("DriverError")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .field("commit_state", &self.commit_state)
            .field("retry", &self.retry)
            .field("has_cause", &self.cause.is_some())
            .finish()
    }
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl StdError for DriverError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.cause
            .as_ref()
            .map(|c| c.as_ref() as &(dyn StdError + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_outcome_is_not_retryable_directly() {
        let e = DriverError::commit_outcome_unknown("lost ack");
        assert_eq!(e.commit_state(), Some(CommitState::Unknown));
        assert_eq!(e.retry_hint(), RetryHint::RequiresRecoveryQuery);
        assert!(e.is_retryable());
        assert!(!CommitState::Unknown.permits_replacement_attempt());
    }

    #[test]
    fn only_final_non_commit_permits_replacement() {
        assert!(CommitState::NotCommittedFinal.permits_replacement_attempt());
        for s in [
            CommitState::Committed,
            CommitState::InFlight,
            CommitState::Unknown,
        ] {
            assert!(!s.permits_replacement_attempt());
        }
    }

    #[test]
    fn committed_is_recoverable() {
        assert!(CommitState::Committed.is_recoverable_success());
        assert!(!CommitState::InFlight.is_recoverable_success());
    }

    #[test]
    fn debug_hides_cause_detail() {
        #[derive(Debug)]
        struct SecretErr;
        impl fmt::Display for SecretErr {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("secret-backend-detail")
            }
        }
        impl StdError for SecretErr {}

        let e = DriverError::internal("backend failed").with_cause(SecretErr);
        let rendered = format!("{e:?}");
        assert!(!rendered.contains("secret-backend-detail"));
        assert!(rendered.contains("has_cause: true"));
        // The cause is still reachable through `Error::source`.
        assert!(e.source().is_some());
    }

    #[test]
    fn transient_kinds_are_marked() {
        assert!(DriverErrorKind::Throttled.is_transient());
        assert!(!DriverErrorKind::NotSupported.is_transient());
    }
}
