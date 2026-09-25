//! Concurrency admission (spec §9.3).
//!
//! Bounds how much work the server holds at once, and nothing else: it does not
//! authorize, does not look anything up in the catalog and never calls a
//! driver. Two budgets live here, and they answer different questions:
//!
//! - the **global** budget — how much work the server as a whole may hold, so no
//!   amount of traffic can make it hold more than it has budget for;
//! - the **per-driver** budget — how much work any one driver instance may be
//!   occupied by, however many buckets it backs, so a slow backend cannot
//!   consume every connection the server has.
//!
//! Saturation is *rejected* (`503 SlowDown`), never queued: queueing would move
//! the backlog into the server instead of bounding it, so every waiting set here
//! is exactly zero. The one place that does wait is [`Admission::drain`], and it
//! waits for slots to come *back*, not to become free.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::ServiceError;
use crate::catalog::BucketBinding;
use crate::domain::DriverInstanceId;
use crate::sync;

/// The resolved admission budgets of one service.
pub(crate) struct Admission {
    global: GlobalLimiter,
    per_driver: DriverLimiter,
}

impl Admission {
    /// Budgets from the resolved limits: `max_concurrent` server-wide and
    /// `per_driver` per driver instance (`0` disables the per-driver budget).
    pub(crate) fn new(max_concurrent: usize, per_driver: usize) -> Self {
        Self {
            global: GlobalLimiter::new(max_concurrent),
            per_driver: DriverLimiter::new(per_driver),
        }
    }

    /// Takes the slots one operation needs.
    ///
    /// A multi-step operation admits once here at its own entry and performs its
    /// steps without admitting again, so no operation holds two shares of the
    /// same budget: a second claim on one driver instance would be refused by
    /// the operation's own limit — at a limit of one, forever.
    pub(crate) fn slot(&self, binding: &BucketBinding) -> Result<OperationSlot, ServiceError> {
        Ok(OperationSlot {
            _global: self.global.acquire()?,
            _driver: self.per_driver.acquire(&binding.driver)?,
        })
    }

    /// Waits until no operation is in flight (used by graceful shutdown).
    pub(crate) async fn drain(&self) {
        self.global.drain().await;
    }
}

/// Server-wide admission.
struct GlobalLimiter {
    max: usize,
    slots: Arc<Semaphore>,
}

impl GlobalLimiter {
    fn new(max: usize) -> Self {
        // A zero would admit nothing at all, so it means "one" here, the same
        // way config resolution clamps it.
        let max = max.max(1);
        Self {
            max,
            slots: Arc::new(Semaphore::new(max)),
        }
    }

    fn acquire(&self) -> Result<OwnedSemaphorePermit, ServiceError> {
        self.slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ServiceError::SlowDown)
    }

    /// Waits until no operation is in flight, then takes every slot so none can
    /// start while the caller finishes.
    async fn drain(&self) {
        let permits = self.slots.clone().acquire_many_owned(self.max as u32).await;
        drop(permits);
    }
}

/// Per-driver-instance admission.
#[derive(Default)]
struct DriverLimiter {
    per_driver: usize,
    slots: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl DriverLimiter {
    fn new(per_driver: usize) -> Self {
        Self {
            per_driver,
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// Takes one slot for `driver`; `0` disables the limit.
    fn acquire(
        &self,
        driver: &DriverInstanceId,
    ) -> Result<Option<OwnedSemaphorePermit>, ServiceError> {
        if self.per_driver == 0 {
            return Ok(None);
        }
        let semaphore = {
            let mut slots = sync::lock(&self.slots);
            slots
                .entry(driver.as_str().to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(self.per_driver)))
                .clone()
        };
        semaphore
            .try_acquire_owned()
            .map(Some)
            .map_err(|_| ServiceError::SlowDown)
    }
}

/// One operation's slots, held for as long as the operation that took them runs.
/// Dropping the guard releases them, however the operation ends.
pub(crate) struct OperationSlot {
    _global: OwnedSemaphorePermit,
    _driver: Option<OwnedSemaphorePermit>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::domain::{BucketId, NamespaceId};
    use crate::driver::Profile;

    fn binding(driver: &str) -> BucketBinding {
        BucketBinding {
            bucket: BucketId::parse("bucket-a").expect("bucket id"),
            driver: DriverInstanceId::parse(driver).expect("instance id"),
            namespace: NamespaceId::parse("ns-a").expect("namespace"),
            generation: 1,
            profile: Profile::CoreWrite,
            readonly: false,
            region: "us-east-1".into(),
        }
    }

    /// The global budget admits exactly its size and refuses the next caller.
    #[test]
    fn a_global_limit_admits_its_budget_and_refuses_the_next() {
        let admission = Admission::new(2, 0);
        let first = admission.slot(&binding("inst-1")).expect("first");
        let second = admission.slot(&binding("inst-1")).expect("second");
        assert!(matches!(
            admission.slot(&binding("inst-1")),
            Err(ServiceError::SlowDown)
        ));

        // A returned slot is reusable, and saturation never queued.
        drop(first);
        assert!(admission.slot(&binding("inst-1")).is_ok());
        drop(second);
    }

    /// The per-driver budget bounds one instance without touching another.
    #[test]
    fn a_per_driver_limit_bounds_one_instance_only() {
        let admission = Admission::new(8, 1);
        let _busy = admission
            .slot(&binding("inst-1"))
            .expect("first for inst-1");
        assert!(
            matches!(
                admission.slot(&binding("inst-1")),
                Err(ServiceError::SlowDown)
            ),
            "the same instance must not take a second share"
        );
        // Another instance has its own share.
        let _other = admission
            .slot(&binding("inst-2"))
            .expect("inst-2 has its own");
    }

    /// `0` disables the per-driver budget rather than admitting nothing.
    #[test]
    fn a_zero_per_driver_limit_is_disabled() {
        let admission = Admission::new(8, 0);
        let _a = admission.slot(&binding("inst-1")).expect("first");
        let _b = admission.slot(&binding("inst-1")).expect("second");
    }

    /// Drain waits for every slot to come back before it returns.
    #[tokio::test]
    async fn drain_waits_for_slots_to_come_back() {
        let admission = Admission::new(1, 0);
        let held = admission.slot(&binding("inst-1")).expect("the only slot");

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), admission.drain())
                .await
                .is_err(),
            "drain must not return while an operation is in flight"
        );

        drop(held);
        tokio::time::timeout(std::time::Duration::from_secs(5), admission.drain())
            .await
            .expect("drain returns once the last slot is back");
        // And the budget is usable again after draining.
        assert!(admission.slot(&binding("inst-1")).is_ok());
    }
}
