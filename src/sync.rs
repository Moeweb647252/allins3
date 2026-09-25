//! Poison-tolerant locking.
//!
//! A poisoned `std::sync::Mutex` means a thread panicked while holding it. The
//! data behind the locks in this crate is a cache, a counter map, an issued-token
//! table or a test-driver namespace: a panic in one request must not turn every
//! later request into a panic of its own, so these guards are recovered rather
//! than unwrapped (spec §10: no `panic`/`unwrap`/`expect` on a request path).
//!
//! This is deliberately *not* a general-purpose helper for locks that guard a
//! broken invariant: those keep their explicit error path.

/// Locks `mutex`, recovering the guard if a previous holder panicked.
pub(crate) fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
