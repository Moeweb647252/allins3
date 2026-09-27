//! A bounded executor for driver work that must not run on the async runtime.
//!
//! `spec §10` requires blocking I/O to run on a restricted blocking executor:
//! a driver that wraps a synchronous SDK must not park a runtime worker thread.
//! The limit is a deployment policy (`limits.max_blocking_operations`) and the
//! executor has exactly one owner — the composition root that builds driver
//! instances — so two drivers cannot each invent their own thread budget.
//!
//! A limit of `0` disables the service-owned bound: the executor still moves
//! work off the runtime threads, but admits however much the backend submits and
//! leaves any cap to the driver. That is recorded in the driver guide, because
//! it changes what a synchronous backend may assume.

use std::fmt;
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::domain::DriverError;

/// A cloneable handle to a shared, bounded blocking executor.
#[derive(Clone)]
pub struct BlockingExecutor {
    limit: usize,
    permits: Option<Arc<Semaphore>>,
}

impl BlockingExecutor {
    /// An executor admitting at most `limit` blocking operations at once.
    ///
    /// `0` means unbounded: work still leaves the runtime threads, but the
    /// executor imposes no ceiling of its own.
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            permits: (limit > 0).then(|| Arc::new(Semaphore::new(limit))),
        }
    }

    /// An executor with no service-owned ceiling.
    pub fn unbounded() -> Self {
        Self::new(0)
    }

    /// The configured ceiling (`0` when unbounded).
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Runs `work` on the blocking pool, waiting for a permit when bounded.
    ///
    /// The future is `Send`; the closure runs on a blocking thread and its
    /// result is returned to the caller. A panicking closure is reported as an
    /// internal error rather than unwinding into the request path.
    pub async fn run<F, T>(&self, work: F) -> Result<T, DriverError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let _permit = match &self.permits {
            Some(permits) => Some(
                permits
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| DriverError::internal("blocking executor was closed"))?,
            ),
            None => None,
        };
        tokio::task::spawn_blocking(work)
            .await
            .map_err(|e| DriverError::internal(format!("blocking task failed: {e}")))
    }
}

impl fmt::Debug for BlockingExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingExecutor")
            .field("limit", &self.limit)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_work_and_returns_its_value() {
        let executor = BlockingExecutor::new(2);
        let value = executor.run(|| 6 * 7).await.expect("ran");
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn a_bounded_executor_serialises_beyond_its_limit() {
        // A limit of one must admit one blocking operation at a time.
        let executor = BlockingExecutor::new(1);
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..4 {
            let executor = executor.clone();
            let counter = counter.clone();
            let peak = peak.clone();
            tasks.push(tokio::spawn(async move {
                executor
                    .run(move || {
                        let now = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        peak.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(20));
                        counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    })
                    .await
                    .expect("ran");
            }));
        }
        for task in tasks {
            task.await.expect("join");
        }
        assert_eq!(peak.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn limit_is_reported() {
        assert_eq!(BlockingExecutor::new(8).limit(), 8);
        assert_eq!(BlockingExecutor::unbounded().limit(), 0);
    }
}
