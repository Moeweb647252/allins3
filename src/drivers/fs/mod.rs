//! The local filesystem storage backend.
//!
//! This is the first *real* backend: it survives a process restart, so a
//! namespace it serves can satisfy the production-write admission test
//! (`is_production_write_capable`). Its durability claim is deliberately
//! narrow — one node, one disk, and this process as the sole writer — which is
//! recorded in `docs/adr/0015-local-filesystem-production-driver.md`.
//!
//! ## Private configuration
//!
//! The core passes the opaque `options` table through untouched; this factory
//! owns its schema and rejects anything it does not understand, so a typo in a
//! deployment file fails `check-config` rather than being silently ignored:
//!
//! ```toml
//! [[drivers]]
//! id = "instance-fs"
//! kind = "fs"
//! options = { root = "/var/lib/allins3" }
//! ```
//!
//! ## On-disk layout
//!
//! ```text
//! <root>/ns-<hex namespace>/objects/<hex key>     one object: header + metadata + bytes
//! <root>/ns-<hex namespace>/staging/<hex write-id> one in-progress staged write
//! <root>/ns-<hex namespace>/recovery/<hex write-id> one commit-outcome record
//! ```
//!
//! An object is a single file so that publish is one atomic `rename`: writing
//! metadata and content as two files would leave a crash window in which one
//! landed and the other did not, which is exactly the split `ADR-0002` forbids.
//! The file is `u64` little-endian metadata length, then the metadata as TOML,
//! then the raw object bytes.

mod meta;
mod namespace;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::domain::{DriverContext, DriverError, NamespaceId};
use crate::driver::{
    BlockingExecutor, ConditionalSupport, Consistency, DriverDescriptor, DriverFactory,
    DriverHealth, DriverKind, FactoryConfig, NamespaceCapabilities, NamespaceHandle, Persistence,
    RangeSupport, SecretResolver, StorageDriver,
};

use namespace::{FsNamespace, FsRecovery};

/// The kind name production deployments configure.
pub(crate) const KIND: &str = "fs";

/// Private driver options, validated by this factory.
#[derive(Debug, Clone)]
pub(crate) struct FsOptions {
    /// Root directory holding every namespace of this instance.
    pub(crate) root: PathBuf,
    /// Declared maximum object size, if the deployment sets one.
    pub(crate) max_object_size: Option<u64>,
    /// Declared maximum size of a single non-multipart write.
    pub(crate) max_single_write_size: Option<u64>,
}

impl FsOptions {
    /// Parses and validates the private schema.
    ///
    /// Unknown keys are rejected: accepting them would let a deployment believe
    /// it configured something the driver never reads.
    pub(crate) fn parse(options: &toml::Table) -> Result<Self, DriverError> {
        for key in options.keys() {
            if !matches!(
                key.as_str(),
                "root" | "max_object_size" | "max_single_write_size"
            ) {
                return Err(DriverError::invalid_argument(format!(
                    "unknown fs option `{key}` (known: root, max_object_size, max_single_write_size)"
                )));
            }
        }
        let root = options
            .get("root")
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| DriverError::invalid_argument("fs option `root` is required"))?;
        let max_object_size = optional_size(options, "max_object_size")?;
        let max_single_write_size = optional_size(options, "max_single_write_size")?;
        Ok(Self {
            root: PathBuf::from(root),
            max_object_size,
            max_single_write_size,
        })
    }

    /// The declared capabilities of a namespace on this backend.
    pub(crate) fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            read: true,
            list: true,
            write: true,
            // The coordinator covers multipart over this backend's staged
            // writes, so the backend itself does not declare it native.
            multipart: false,
            // Emulated: the service reads the source and stages a write.
            native_copy: false,
            recovery: true,
            range: RangeSupport::Native,
            conditional: ConditionalSupport::Native,
            consistency: Consistency::Strong,
            // A directory that survives process exit. Whether it survives a
            // machine failure is a deployment property, recorded with the
            // single-node claim rather than assumed here.
            persistence: Persistence::Durable,
            limits: crate::driver::DriverLimits {
                max_object_size: self.max_object_size,
                max_single_write_size: self.max_single_write_size,
                // The S3 protocol bounds for parts the coordinator enforces;
                // declared here because the driver's staged writes back them.
                max_multipart_parts: Some(10_000),
                min_part_size: Some(5 * 1024 * 1024),
                max_metadata_count: Some(crate::domain::MAX_METADATA_COUNT as u32),
                max_metadata_bytes: Some(crate::domain::MAX_METADATA_BYTES as u64),
            },
            // One process owns the directory, so an in-process lock makes the
            // conditional operations atomic. External writers would invalidate
            // that, which is why this is declared rather than assumed.
            exclusive_writer: true,
        }
    }
}

fn optional_size(options: &toml::Table, key: &str) -> Result<Option<u64>, DriverError> {
    match options.get(key) {
        None => Ok(None),
        Some(value) => {
            let number = value.as_integer().ok_or_else(|| {
                DriverError::invalid_argument(format!("fs option `{key}` must be an integer"))
            })?;
            if number <= 0 {
                return Err(DriverError::invalid_argument(format!(
                    "fs option `{key}` must be positive"
                )));
            }
            Ok(Some(number as u64))
        }
    }
}

/// Factory for [`FsDriver`] instances.
pub struct FsDriverFactory {
    kind: DriverKind,
}

impl FsDriverFactory {
    /// A factory registered under `kind`.
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: DriverKind::new(kind),
        }
    }
}

#[async_trait]
impl DriverFactory for FsDriverFactory {
    fn kind(&self) -> DriverKind {
        self.kind.clone()
    }

    async fn validate(
        &self,
        _ctx: &DriverContext,
        config: &FactoryConfig,
        _secrets: &dyn SecretResolver,
    ) -> Result<(), DriverError> {
        // Private-schema validation only: no user data, and no directory is
        // created. `check-config` relies on this being side-effect free.
        FsOptions::parse(&config.options).map(|_| ())
    }

    async fn create(
        &self,
        _ctx: &DriverContext,
        config: &FactoryConfig,
        _secrets: &dyn SecretResolver,
    ) -> Result<Arc<dyn StorageDriver>, DriverError> {
        let options = FsOptions::parse(&config.options)?;
        tokio::fs::create_dir_all(&options.root)
            .await
            .map_err(|e| io_error("create the driver root", &options.root, e))?;

        Ok(Arc::new(FsDriver {
            descriptor: DriverDescriptor {
                kind: self.kind.clone(),
                instance_id: config.instance_id.clone(),
                display_name: "local filesystem".into(),
                version: Some(env!("CARGO_PKG_VERSION").into()),
            },
            options,
            blocking: config.blocking.clone(),
            namespace_locks: Mutex::new(HashMap::new()),
            revision_seq: Arc::new(AtomicU64::new(0)),
        }))
    }
}

/// The local filesystem driver.
pub struct FsDriver {
    descriptor: DriverDescriptor,
    options: FsOptions,
    blocking: BlockingExecutor,
    /// One lock per namespace, shared by every handle of that namespace, so
    /// conditional commits and deletes are serialised within this process.
    namespace_locks: Mutex<HashMap<NamespaceId, Arc<tokio::sync::Mutex<()>>>>,
    /// Revision counter, shared by every namespace of this instance so two
    /// revisions minted in the same nanosecond still differ.
    revision_seq: Arc<AtomicU64>,
}

impl FsDriver {
    fn lock_for(&self, namespace: &NamespaceId) -> Arc<tokio::sync::Mutex<()>> {
        crate::sync::lock(&self.namespace_locks)
            .entry(namespace.clone())
            .or_default()
            .clone()
    }
}

#[async_trait]
impl StorageDriver for FsDriver {
    fn descriptor(&self) -> DriverDescriptor {
        self.descriptor.clone()
    }

    async fn health(&self, ctx: &DriverContext) -> Result<DriverHealth, DriverError> {
        check_ctx(ctx)?;
        match tokio::fs::metadata(&self.options.root).await {
            Ok(meta) if meta.is_dir() => Ok(DriverHealth::Healthy),
            Ok(_) => Ok(DriverHealth::Unhealthy {
                reason: format!(
                    "driver root `{}` is not a directory",
                    self.options.root.display()
                ),
            }),
            Err(e) => Ok(DriverHealth::Unhealthy {
                reason: format!(
                    "driver root `{}` is unreachable: {e}",
                    self.options.root.display()
                ),
            }),
        }
    }

    async fn shutdown(&self, _ctx: &DriverContext) -> Result<(), DriverError> {
        // Every write is fsynced before its rename returns, so there is no
        // buffered state to flush here; the declaration lives on the type so a
        // future queue can be drained without changing the interface.
        Ok(())
    }

    async fn open_namespace(
        &self,
        ctx: &DriverContext,
        namespace: &NamespaceId,
    ) -> Result<NamespaceHandle, DriverError> {
        check_ctx(ctx)?;
        let layout = namespace::Layout::new(&self.options.root, namespace);
        for dir in [
            layout.objects_dir(),
            layout.staging_dir(),
            layout.recovery_dir(),
        ] {
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| io_error("create a namespace directory", &dir, e))?;
        }

        let capabilities = self.options.capabilities();
        let shared = Arc::new(namespace::NamespaceShared {
            layout,
            max_object_size: self.options.max_object_size,
            max_single_write_size: self.options.max_single_write_size,
            blocking: self.blocking.clone(),
            lock: self.lock_for(namespace),
            revision_seq: self.revision_seq.clone(),
        });

        let read: Arc<dyn crate::driver::ReadNamespace> = Arc::new(FsNamespace {
            shared: shared.clone(),
        });
        let write: Option<Arc<dyn crate::driver::WriteNamespace>> = Some(Arc::new(FsNamespace {
            shared: shared.clone(),
        }));
        let recovery: Option<Arc<dyn crate::driver::CommitRecovery>> =
            Some(Arc::new(FsRecovery { shared }));

        let handle = NamespaceHandle {
            capabilities,
            read,
            write,
            multipart: None,
            native_copy: None,
            recovery,
        };
        debug_assert!(
            handle.is_self_consistent(),
            "capabilities and handles diverged"
        );
        Ok(handle)
    }
}

fn check_ctx(ctx: &DriverContext) -> Result<(), DriverError> {
    if ctx.is_done() {
        let kind = if ctx.is_cancelled() {
            crate::domain::DriverErrorKind::Cancelled
        } else {
            crate::domain::DriverErrorKind::TemporarilyUnavailable
        };
        return Err(DriverError::new(kind, "request cancelled or past deadline"));
    }
    Ok(())
}

/// Maps a filesystem error onto the stable driver kinds.
///
/// The path is not included in the caller-visible message — it would leak the
/// backend layout — but the underlying error travels as the internal cause.
fn io_error(action: &str, path: &Path, source: std::io::Error) -> DriverError {
    let internal = || format!("{action} at `{}` failed", path.display());
    match source.kind() {
        std::io::ErrorKind::NotFound => DriverError::not_found(internal()),
        std::io::ErrorKind::PermissionDenied => {
            DriverError::access_denied(internal()).with_cause(source)
        }
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted => {
            DriverError::temporarily_unavailable(internal()).with_cause(source)
        }
        std::io::ErrorKind::StorageFull => {
            DriverError::resource_exhausted(internal()).with_cause(source)
        }
        _ => DriverError::internal(internal()).with_cause(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(table: &str) -> Result<FsOptions, DriverError> {
        let table: toml::Table = toml::from_str(table).expect("valid toml");
        FsOptions::parse(&table)
    }

    #[test]
    fn requires_the_root_option() {
        assert!(parse("max_object_size = 10").is_err());
        assert_eq!(
            parse("root = \"/tmp/allins3\"").expect("valid").root,
            PathBuf::from("/tmp/allins3")
        );
    }

    #[test]
    fn rejects_unknown_private_options() {
        // A deployment must not believe it configured something the driver
        // never reads.
        assert!(parse("root = \"/tmp/a\"\ntypo = 1").is_err());
    }

    #[test]
    fn rejects_non_positive_sizes() {
        assert!(parse("root = \"/tmp/a\"\nmax_object_size = 0").is_err());
        assert!(parse("root = \"/tmp/a\"\nmax_object_size = \"big\"").is_err());
        assert_eq!(
            parse("root = \"/tmp/a\"\nmax_object_size = 1024")
                .expect("valid")
                .max_object_size,
            Some(1024)
        );
    }

    #[test]
    fn declares_a_production_write_capable_namespace() {
        let capabilities = parse("root = \"/tmp/a\"").expect("valid").capabilities();
        assert!(capabilities.is_production_write_capable());
        assert_eq!(capabilities.persistence, Persistence::Durable);
        assert_eq!(capabilities.consistency, Consistency::Strong);
        assert!(capabilities.exclusive_writer);
        assert!(capabilities.recovery);
        assert_eq!(capabilities.range, RangeSupport::Native);
        assert_eq!(capabilities.conditional, ConditionalSupport::Native);
    }
}
