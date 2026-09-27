//! The 139 cloud-drive storage backend, read-only.
//!
//! This adapter exposes a folder of a personal 139 (china mobile) cloud drive
//! as an S3-compatible object namespace: files become objects whose keys are
//! their paths relative to the configured folder, and nested folders become key
//! prefixes. It is a *real* backend, not a test double, but deliberately a
//! read-only one — see `docs/adr/0017-read-only-yun139-driver.md` for why
//! (no hidden staging, no atomic publish, no conditional write, and a
//! namespace that other clients also write).
//!
//! ## Private configuration
//!
//! The core passes the opaque `options` table through untouched; this factory
//! owns its schema and rejects anything it does not understand. The session
//! credentials are supplied here, in the deployment file, by operator choice —
//! keep that file out of version control.
//!
//! ```toml
//! [[drivers]]
//! id = "instance-yun139"
//! kind = "yun139"
//! options = { phone = "198...", auth_token = "…", cookie = "…", folder_id = "Ft…" }
//! ```
//!
//! ## Where the API lives
//!
//! The signed drive API is `https://personal-kd-njs.yun.139.com/hcy`; reads go
//! through it (`file/list`, `file/getDownloadUrl`) and the bytes themselves come
//! from a short-lived presigned URL on the backing S3-compatible store. See
//! `docs/yun139-api.md` for the reverse-engineered contract.

mod client;
mod namespace;
mod options;
mod sign;
mod stamp;

use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::{DriverContext, DriverError, DriverErrorKind, NamespaceId};
use crate::driver::{
    BlockingExecutor, DriverDescriptor, DriverFactory, DriverHealth, DriverKind, FactoryConfig,
    NamespaceHandle, SecretResolver, StorageDriver,
};

use client::Yun139Client;
use namespace::Yun139Namespace;
use options::Yun139Options;

/// The kind name configuration uses.
pub(crate) use options::KIND;

/// Factory for [`Yun139Driver`] instances.
pub struct Yun139DriverFactory {
    kind: DriverKind,
}

impl Yun139DriverFactory {
    /// A factory registered under `kind`.
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: DriverKind::new(kind),
        }
    }
}

#[async_trait]
impl DriverFactory for Yun139DriverFactory {
    fn kind(&self) -> DriverKind {
        self.kind.clone()
    }

    async fn validate(
        &self,
        _ctx: &DriverContext,
        config: &FactoryConfig,
        _secrets: &dyn SecretResolver,
    ) -> Result<(), DriverError> {
        // Private-schema validation only: parsing performs no I/O, so
        // `check-config` can reject a bad deployment without a network call.
        Yun139Options::parse(&config.options).map(|_| ())
    }

    async fn create(
        &self,
        _ctx: &DriverContext,
        config: &FactoryConfig,
        _secrets: &dyn SecretResolver,
    ) -> Result<Arc<dyn StorageDriver>, DriverError> {
        let options = Yun139Options::parse(&config.options)?;
        let client = Yun139Client::new(&options)?;
        Ok(Arc::new(Yun139Driver {
            descriptor: DriverDescriptor {
                kind: self.kind.clone(),
                instance_id: config.instance_id.clone(),
                display_name: "139 cloud drive (read-only)".into(),
                version: Some(env!("CARGO_PKG_VERSION").into()),
            },
            client,
            options: Arc::new(options),
            blocking: config.blocking.clone(),
        }))
    }
}

/// The read-only 139 driver.
pub struct Yun139Driver {
    descriptor: DriverDescriptor,
    client: Arc<Yun139Client>,
    options: Arc<Yun139Options>,
    blocking: BlockingExecutor,
}

#[async_trait]
impl StorageDriver for Yun139Driver {
    fn descriptor(&self) -> DriverDescriptor {
        self.descriptor.clone()
    }

    async fn health(&self, ctx: &DriverContext) -> Result<DriverHealth, DriverError> {
        check_ctx(ctx)?;
        let Some(folder) = self.options.probe_folder() else {
            return Ok(DriverHealth::Unhealthy {
                reason: "no drive folder is configured".into(),
            });
        };
        // A single cheap, read-only listing: it proves the session is accepted
        // and the configured folder is reachable without touching any object.
        match self.client.probe(folder).await {
            Ok(()) => Ok(DriverHealth::Healthy),
            Err(e) => Ok(DriverHealth::Unhealthy {
                reason: e.message().to_string(),
            }),
        }
    }

    async fn shutdown(&self, _ctx: &DriverContext) -> Result<(), DriverError> {
        // Reads hold no durable state and the HTTP client is dropped with the
        // driver; nothing to flush.
        Ok(())
    }

    async fn open_namespace(
        &self,
        ctx: &DriverContext,
        namespace: &NamespaceId,
    ) -> Result<NamespaceHandle, DriverError> {
        check_ctx(ctx)?;
        let folder_id = self.options.folder_for(namespace.as_str())?.to_string();

        let read: Arc<dyn crate::driver::ReadNamespace> = Arc::new(Yun139Namespace {
            client: self.client.clone(),
            blocking: self.blocking.clone(),
            folder_id,
            max_objects: self.options.max_objects,
        });

        let handle = NamespaceHandle {
            capabilities: self.options.capabilities(),
            read,
            write: None,
            multipart: None,
            native_copy: None,
            recovery: None,
        };
        debug_assert!(
            handle.is_self_consistent(),
            "capabilities and handles diverged"
        );
        Ok(handle)
    }
}

/// Rejects work whose request context has already finished.
pub(crate) fn check_ctx(ctx: &DriverContext) -> Result<(), DriverError> {
    if ctx.is_done() {
        let kind = if ctx.is_cancelled() {
            DriverErrorKind::Cancelled
        } else {
            DriverErrorKind::TemporarilyUnavailable
        };
        return Err(DriverError::new(kind, "request cancelled or past deadline"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_factory_reports_its_kind() {
        let factory = Yun139DriverFactory::new("yun139");
        assert_eq!(factory.kind().as_str(), "yun139");
        assert_eq!(KIND, "yun139");
    }
}
