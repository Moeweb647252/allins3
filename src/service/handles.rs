//! Namespace handles: from a bucket binding to a usable driver namespace.
//!
//! This module owns the driver instance table and the handle cache, and nothing
//! else does. A caller hands it a [`BucketBinding`] and gets back a handle that
//! is already open and already capability-checked for that binding's profile;
//! it never learns that a driver table or a cache key exists.
//!
//! Two rules from ADR-0004 are load-bearing here:
//!
//! - the handle is pinned to the binding's **generation** (ADR-0004 §5), so a
//!   generation is never silently swapped under a request, a page token or an
//!   upload. The cache key is `bucket#generation` for exactly that reason;
//! - authorization is *not* this module's business (ADR-0004 §8): a handle says
//!   which namespace a binding reaches, never who may reach it.
//!
//! A binding whose profile the namespace cannot serve fails closed here — it is
//! a configuration fault, not a client error — so a capability mismatch is
//! reported as `Internal` rather than served as a broken success.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::catalog::BucketBinding;
use crate::domain::DriverContext;
use crate::driver::{NamespaceHandle, StorageDriver, validate_profile};
use crate::sync;

use super::ServiceError;

/// The driver instances of one service and the namespace handles opened from
/// them.
pub(crate) struct NamespaceHandles {
    drivers: HashMap<String, Arc<dyn StorageDriver>>,
    cache: Mutex<HashMap<String, Arc<NamespaceHandle>>>,
}

impl NamespaceHandles {
    /// A table over the resolved driver instances, keyed by instance id.
    pub(crate) fn new(drivers: HashMap<String, Arc<dyn StorageDriver>>) -> Self {
        Self {
            drivers,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// The open, capability-checked handle for `binding`.
    ///
    /// Opening is done once per binding generation and then cached; a caller
    /// that asks again gets the same handle rather than a second backend
    /// connection.
    pub(crate) async fn get(
        &self,
        binding: &BucketBinding,
        ctx: &DriverContext,
    ) -> Result<Arc<NamespaceHandle>, ServiceError> {
        let cache_key = format!("{}#{}", binding.bucket, binding.generation);
        if let Some(handle) = sync::lock(&self.cache).get(&cache_key).cloned() {
            return Ok(handle);
        }

        let driver = self
            .drivers
            .get(binding.driver.as_str())
            .cloned()
            .ok_or_else(|| {
                ServiceError::Internal(format!("driver `{}` is not registered", binding.driver))
            })?;
        let handle = driver
            .open_namespace(ctx, &binding.namespace)
            .await
            .map_err(|e| ServiceError::from_driver(&e, false))?;

        validate_profile(binding.profile, &handle.capabilities).map_err(|e| {
            ServiceError::Internal(format!(
                "bucket `{}` capability mismatch: {e}",
                binding.bucket
            ))
        })?;

        let handle = Arc::new(handle);
        sync::lock(&self.cache).insert(cache_key, handle.clone());
        Ok(handle)
    }
}
