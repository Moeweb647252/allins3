//! Storage driver abstraction.
//!
//! This module defines the interfaces a storage backend implements. It knows
//! nothing about HTTP, S3 XML or signature verification: adding a new driver
//! must never require changing the protocol layer (`INV-01`).
//!
//! See `docs/implementation-spec.md` §2, §3.

pub(crate) mod builtins;
mod capabilities;
mod descriptor;
mod extensions;
mod factory;
mod registry;
mod traits;

pub use capabilities::{
    CapabilityError, CapabilityPlan, ConditionalSupport, Consistency, DriverLimits, FeatureSupport,
    NamespaceCapabilities, Persistence, PlannedFeature, Profile, ProfileError, RangeSupport,
    features_for_profile, plan, support_for, validate_profile,
};
pub use descriptor::{DriverDescriptor, DriverHealth, DriverKind};
pub use extensions::{
    CommitRecovery, CompletedPart, CopyRequest, CopySource, CreateUploadRequest, MetadataDirective,
    MultipartChecksumScheme, MultipartNamespace, MultipartUpload, NativeCopy, PartInfo, PartPage,
    UploadHandle, UploadId, UploadPage,
};
pub use factory::{DriverFactory, FactoryConfig, Secret, SecretResolver};
pub use registry::{DriverRegistry, RegistryError};
pub use traits::{
    NamespaceHandle, ObjectByteStream, ReadNamespace, ReadRequest, ReadResult, StagedWrite,
    StorageDriver, WriteNamespace,
};
