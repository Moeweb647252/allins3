//! Storage driver abstraction.
//!
//! This module defines the interfaces a storage backend implements. It knows
//! nothing about HTTP, S3 XML or signature verification: adding a new driver
//! must never require changing the protocol layer (`INV-01`).
//!
//! See `docs/implementation-spec.md` §2, §3.

mod capabilities;
mod descriptor;
mod extensions;
mod factory;
mod registry;
mod traits;

pub use capabilities::{
    CapabilityError, CapabilityPlan, ConditionalSupport, Consistency, DriverLimits, FeatureSupport,
    NamespaceCapabilities, Persistence, PlannedFeature, Profile, ProfileError, RangeSupport, plan,
    support_for, validate_profile,
};
pub use descriptor::{DriverDescriptor, DriverHealth, DriverKind};
pub use extensions::{
    CommitRecovery, CompletedPart, CopyRequest, CopySource, MetadataDirective, MultipartNamespace,
    MultipartUpload, NativeCopy, PartInfo, UploadHandle, UploadId,
};
pub use factory::{DriverFactory, FactoryConfig, Secret, SecretResolver};
pub use registry::{DriverRegistry, RegistryError};
pub use traits::{
    NamespaceHandle, ObjectByteStream, ReadNamespace, ReadRequest, ReadResult, StagedWrite,
    StorageDriver, WriteNamespace,
};
