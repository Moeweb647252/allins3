//! Request timeouts (spec §9.3).
//!
//! §9.3 requires four separate bounds — the header timeout, the body idle
//! timeout, the backend operation deadline and the total deadline — so that one
//! short total timeout cannot cut off every large object. Each bound has exactly
//! one owner:
//!
//! | bound | owner | config value |
//! | --- | --- | --- |
//! | header timeout | the HTTP/1 connection builder (`server::run`) | `header_timeout_secs` |
//! | body idle timeout | [`IdleTimeoutBody`], wrapped around each request body | `body_idle_timeout_secs` |
//! | total deadline | `HttpService::call`, around the whole S3 call | `request_timeout_secs` (0 disables) |
//! | backend operation deadline | the total deadline: a driver call cannot outlive the request that started it, so the request context carries it (`S3Handler::scope`) | `request_timeout_secs` |
//!
//! The body idle timeout is the one that must not be a connection-wide timer: a
//! large *download* leaves the read side idle for as long as the client takes,
//! so only a bound that wraps the request *body* distinguishes "the client
//! stopped sending" from "the client is still reading".

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use tokio::time::{Instant, Sleep};

/// A request body that fails when no frame arrives within the idle timeout.
///
/// Wrapping the body rather than the connection keeps the bound on what §9.3
/// names: it starts when the body is first polled, is pushed back by every
/// frame, and never applies to a response being written to a slow reader.
pub struct IdleTimeoutBody<B> {
    inner: B,
    idle: Option<Duration>,
    sleep: Option<Pin<Box<Sleep>>>,
    idled: Arc<AtomicBool>,
}

impl<B> IdleTimeoutBody<B> {
    /// Wraps `inner`, failing after `idle` without a frame. `None` disables.
    ///
    /// `idled` is raised when the bound fires. Control bodies (XML) are read by
    /// the protocol library itself, which reports a body error as an internal
    /// error; the server watches this flag so the client still sees a client
    /// error (spec §9.1).
    pub fn new(inner: B, idle: Option<Duration>, idled: Arc<AtomicBool>) -> Self {
        Self {
            sleep: idle.map(|idle| Box::pin(tokio::time::sleep(idle))),
            idle,
            inner,
            idled,
        }
    }
}

impl<B> Body for IdleTimeoutBody<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Data = Bytes;
    type Error = BodyTimeoutError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let (Some(sleep), Some(idle)) = (this.sleep.as_mut(), this.idle) {
                    sleep.as_mut().reset(Instant::now() + idle);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(BodyTimeoutError::Transport(Box::new(error)))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => match this.sleep.as_mut() {
                None => Poll::Pending,
                Some(sleep) => {
                    if sleep.as_mut().poll(cx).is_ready() {
                        this.idled.store(true, Ordering::Relaxed);
                        Poll::Ready(Some(Err(BodyTimeoutError::Idle)))
                    } else {
                        Poll::Pending
                    }
                }
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Why a request body was abandoned before it ended.
#[derive(Debug, thiserror::Error)]
pub enum BodyTimeoutError {
    /// No frame arrived within the configured body idle timeout.
    #[error("the request body was idle for longer than the configured body idle timeout")]
    Idle,
    /// The transport failed while the body was being read.
    #[error("the request body could not be read")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Whether an error chain reports an idle request body.
///
/// The protocol library wraps a body error before it reaches the service, so the
/// marker is looked for anywhere in the source chain rather than only at the
/// top.
pub fn is_body_idle(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(timeout) = error.downcast_ref::<BodyTimeoutError>() {
            return matches!(timeout, BodyTimeoutError::Idle);
        }
        current = error.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    use http_body_util::BodyExt;

    fn observed() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    /// A body that never yields, standing in for a client that stopped sending.
    struct Never;

    impl Body for Never {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            Poll::Pending
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_body_times_out() {
        let idled = observed();
        let mut body = IdleTimeoutBody::new(Never, Some(Duration::from_secs(5)), idled.clone());
        let frame = tokio::time::timeout(Duration::from_secs(60), body.frame())
            .await
            .expect("the idle timeout fires well before the guard");
        assert!(matches!(frame, Some(Err(BodyTimeoutError::Idle))));
        assert!(idled.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn no_configured_idle_timeout_leaves_the_body_pending() {
        let mut body = IdleTimeoutBody::new(Never, None, observed());
        assert!(futures_util::poll!(body.frame()).is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn frames_pass_through_and_reset_the_deadline() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"xyz"));
        let mut body = IdleTimeoutBody::new(inner, Some(Duration::from_secs(5)), observed());
        let frame = body
            .frame()
            .await
            .expect("a frame")
            .expect("the body is readable");
        assert_eq!(
            frame.into_data().expect("data frame"),
            Bytes::from_static(b"xyz")
        );
        // The wrapped body ended, so the idle timer never fires.
        assert!(body.frame().await.is_none());
    }
}
