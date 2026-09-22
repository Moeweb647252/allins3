//! Per-request driver context.
//!
//! The context carries request identity, a deadline, a cancellation signal and
//! trace metadata. It never carries client credentials and is safe to hand to a
//! driver.

use std::fmt;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

/// A per-request identifier, included in error responses and logs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(String);

impl RequestId {
    /// Wraps a caller-supplied request id.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrows the id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Opaque trace correlation metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// Distributed trace id.
    pub trace_id: Option<String>,
    /// Current span id.
    pub span_id: Option<String>,
}

/// Context passed to every driver operation.
///
/// `DriverContext` is `Send + Sync + Clone`; it is cloned cheaply and shared
/// across the awaits of a single request.
#[derive(Debug, Clone)]
pub struct DriverContext {
    request_id: RequestId,
    deadline: Option<Instant>,
    cancel: CancellationToken,
    trace: TraceContext,
}

impl DriverContext {
    /// Creates a context with no deadline and a fresh cancellation token.
    pub fn new(request_id: RequestId) -> Self {
        Self {
            request_id,
            deadline: None,
            cancel: CancellationToken::new(),
            trace: TraceContext::default(),
        }
    }

    /// Sets an absolute deadline.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Sets a deadline relative to now.
    pub fn with_timeout(self, timeout: Duration) -> Self {
        self.with_deadline(Instant::now() + timeout)
    }

    /// Uses the supplied cancellation token.
    pub fn with_cancellation(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// Sets trace metadata.
    pub fn with_trace(mut self, trace: TraceContext) -> Self {
        self.trace = trace;
        self
    }

    /// The request id.
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// The absolute deadline, if any.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// A cloneable cancellation token, usable from background tasks.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Whether the request has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Whether the deadline has passed.
    pub fn is_expired(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() >= d)
    }

    /// Whether the operation should stop: cancelled or past its deadline.
    pub fn is_done(&self) -> bool {
        self.is_cancelled() || self.is_expired()
    }

    /// Remaining time budget, if a deadline is set.
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
    }

    /// Trace metadata.
    pub fn trace(&self) -> &TraceContext {
        &self.trace
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_observable() {
        let ctx = DriverContext::new(RequestId::new("req-1"));
        assert!(!ctx.is_cancelled());
        ctx.cancellation_token().cancel();
        assert!(ctx.is_cancelled());
        assert!(ctx.is_done());
    }

    #[test]
    fn deadline_expires() {
        let ctx = DriverContext::new(RequestId::new("req-2")).with_timeout(Duration::ZERO);
        assert!(ctx.is_expired());
        assert!(ctx.is_done());
    }

    #[test]
    fn remaining_is_bounded() {
        let ctx = DriverContext::new(RequestId::new("req-3")).with_timeout(Duration::from_secs(60));
        assert!(ctx.remaining().unwrap() <= Duration::from_secs(60));
    }

    #[test]
    fn context_is_send_sync_clone() {
        fn assert_bounds<T: Send + Sync + Clone>() {}
        assert_bounds::<DriverContext>();
    }
}
